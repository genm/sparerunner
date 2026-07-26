package store

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/genm/tewake/internal/domain"
)

func TestControllerWALAtomicReplayAndEpoch(t *testing.T) {
	ctx := context.Background()
	store := openController(t, "controller.db")
	defer store.Close()
	if epoch, err := store.AdvanceEpoch(ctx); err != nil || epoch != 1 {
		t.Fatalf("epoch = %d, %v", epoch, err)
	}
	var journal string
	if err := store.db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("journal mode = %q, %v", journal, err)
	}
	var foreignKeys int
	if err := store.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("foreign keys = %d, %v", foreignKeys, err)
	}
	assignment := testAssignment(101, "execution-1", "node-1", 0)
	got, replay, err := store.Assign(ctx, assignment)
	if err != nil || replay || got != assignment.Execution {
		t.Fatalf("first assignment = (%+v, %t, %v)", got, replay, err)
	}
	got, replay, err = store.Assign(ctx, assignment)
	if err != nil || !replay || got != assignment.Execution {
		t.Fatalf("exact replay = (%+v, %t, %v)", got, replay, err)
	}
	assignment.MessageDigest = domain.PayloadDigest([]byte("different"))
	if _, _, err := store.Assign(ctx, assignment); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("digest mismatch = %v", err)
	}
	assertCount(t, store.db, "SELECT count(*) FROM processed_messages", 1)
	assertCount(t, store.db, "SELECT count(*) FROM slot_reservations", 1)
	assertCount(t, store.db, "SELECT count(*) FROM executions", 1)
}

func TestAssignmentCanonicalIdentityAndDatabaseInvariants(t *testing.T) {
	ctx := context.Background()
	store := openController(t, "controller.db")
	defer store.Close()
	invalid := testAssignment(1, "execution-1", "node-1", 0)
	invalid.ScaleSetID = 0
	if _, _, err := store.Assign(ctx, invalid); err == nil {
		t.Fatal("zero scale set ID accepted")
	}
	invalid = testAssignment(1, "execution-1", "node-1", 0)
	invalid.MessageDigest = strings.ToUpper(invalid.MessageDigest)
	if _, _, err := store.Assign(ctx, invalid); err == nil {
		t.Fatal("uppercase digest accepted")
	}
	invalid = testAssignment(1, "execution-1", "node-1", 0)
	invalid.Execution.State = domain.ExecutionPending
	if _, _, err := store.Assign(ctx, invalid); err == nil {
		t.Fatal("non-reserved desired execution accepted")
	}
	if _, err := store.db.Exec(`INSERT INTO slot_reservations(node_id, slot_index, target_id, execution_id) VALUES ('node-direct', 0, 'target-direct', 'execution-direct')`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES ('execution-direct', 'target-direct', 'node-direct', 0, 'running', 1)`); err == nil {
		t.Fatal("direct invalid execution state bypassed CHECK")
	}
	if _, err := store.db.Exec(`INSERT INTO processed_messages(scale_set_id, message_id, message_digest, execution_id, created_at_unix_nano) VALUES (0, 1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'execution-direct', 1)`); err == nil {
		t.Fatal("zero numeric message identity bypassed CHECK")
	}
	columns := tableColumns(t, store.db, "processed_messages")
	if strings.Join(columns, ",") != "scale_set_id,message_id,message_digest,execution_id,created_at_unix_nano" {
		t.Fatalf("processed_messages has redundant drift columns: %v", columns)
	}
}

func TestAssignmentRollbackAndConcurrentSlotConstraint(t *testing.T) {
	ctx := context.Background()
	store := openController(t, "controller.db")
	defer store.Close()
	if _, _, err := store.Assign(ctx, testAssignment(1, "execution-1", "node-1", 0)); err != nil {
		t.Fatal(err)
	}
	// A duplicate execution ID fails after attempting a second reservation; the
	// transaction must leave that concrete slot untouched.
	if _, _, err := store.Assign(ctx, testAssignment(2, "execution-1", "node-1", 1)); !errors.Is(err, ErrActiveExecution) {
		t.Fatalf("rollback error = %v", err)
	}
	assertCount(t, store.db, "SELECT count(*) FROM slot_reservations WHERE node_id='node-1' AND slot_index=1", 0)

	path := filepath.Join(privateTestDir(t), "concurrent.db")
	first, second := openControllerPath(t, path), openControllerPath(t, path)
	defer first.Close()
	defer second.Close()
	var successes int
	var group sync.WaitGroup
	var lock sync.Mutex
	for index, candidate := range []*ControllerStore{first, second} {
		group.Add(1)
		go func(index int, candidate *ControllerStore) {
			defer group.Done()
			_, _, err := candidate.Assign(ctx, testAssignment(MessageID(index+1), "concurrent-"+string(rune('a'+index)), "node-1", 0))
			if err == nil {
				lock.Lock()
				successes++
				lock.Unlock()
				return
			}
			if !errors.Is(err, ErrSlotAlreadyReserved) && !errors.Is(err, ErrActiveExecution) {
				t.Errorf("concurrent assignment %d: %v", index, err)
			}
		}(index, candidate)
	}
	group.Wait()
	if successes != 1 {
		t.Fatalf("concurrent successes = %d", successes)
	}
}

func TestMigrationRecoveryPreservesOriginalAndRejectsUnknownVersions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller.db")
	failed, err := OpenController(ctx, path, Options{MigrationHook: func(string, int) error { return errors.New("injected") }})
	if failed == nil || !errors.Is(err, ErrRecoveryMode) {
		t.Fatalf("migration failure = (%v, %v)", failed, err)
	}
	if _, err := failed.AdvanceEpoch(ctx); !errors.Is(err, ErrRecoveryMode) {
		t.Fatalf("migration recovery mutation = %v", err)
	}
	assertCount(t, failed.db, "SELECT count(*) FROM sqlite_master WHERE type='table'", 0)
	_ = failed.Close()
	recovered := openControllerPath(t, path)
	_ = recovered.Close()

	mutateVersions(t, path, "DELETE FROM schema_migrations; INSERT INTO schema_migrations(version, applied_at_unix_nano) VALUES (2, 1)")
	degraded, err := OpenController(ctx, path, Options{})
	if degraded == nil || !errors.Is(err, ErrRecoveryMode) {
		t.Fatalf("future version open = (%v, %v)", degraded, err)
	}
	if _, err := degraded.AdvanceEpoch(ctx); !errors.Is(err, ErrRecoveryMode) {
		t.Fatalf("future version write = %v", err)
	}
	_ = degraded.Close()
}

func TestInjectedPendingMigrationPreservesExistingDataAndVersion(t *testing.T) {
	ctx := context.Background()
	db := openRawTestDatabase(t)
	defer db.Close()
	for _, statement := range []string{
		"CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at_unix_nano INTEGER NOT NULL)",
		"INSERT INTO schema_migrations(version, applied_at_unix_nano) VALUES (1, 1)",
		"CREATE TABLE preserved(value TEXT NOT NULL)",
		"INSERT INTO preserved(value) VALUES ('unchanged')",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	migrations := fstestMapFS(map[string]string{
		"migrations/controller/001_base.sql":    "SELECT 1;",
		"migrations/controller/002_pending.sql": "CREATE TABLE must_rollback(value TEXT NOT NULL);",
	})
	err := applyMigrations(ctx, db, "controller", migrations, time.Now, func(string, int) error { return errors.New("injected") })
	if err == nil {
		t.Fatal("pending migration unexpectedly succeeded")
	}
	var value string
	if err := db.QueryRow("SELECT value FROM preserved").Scan(&value); err != nil || value != "unchanged" {
		t.Fatalf("preserved content = %q, %v", value, err)
	}
	assertCount(t, db, "SELECT count(*) FROM schema_migrations WHERE version=1", 1)
	assertCount(t, db, "SELECT count(*) FROM schema_migrations WHERE version=2", 0)
	assertCount(t, db, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='must_rollback'", 0)
}

func TestWrongRoleOpenIsRecoveryAndFatalOpenIsNil(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "agent.db")
	agent, err := OpenAgent(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_ = agent.Close()
	controller, err := OpenController(ctx, path, Options{})
	if controller == nil || !errors.Is(err, ErrRecoveryMode) || !errors.Is(err, ErrWrongRole) {
		t.Fatalf("wrong role open = (%v, %v)", controller, err)
	}
	if _, err := controller.AdvanceEpoch(ctx); !errors.Is(err, ErrRecoveryMode) {
		t.Fatalf("wrong-role mutation = %v", err)
	}
	_ = controller.Close()
	if store, err := OpenController(ctx, "", Options{}); store != nil || err == nil {
		t.Fatalf("fatal insecure parent open = (%v, %v)", store, err)
	}
}

func TestAgentJournalAllowlistedTombstoneAndSecretCanaryRejection(t *testing.T) {
	ctx := context.Background()
	store, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	command := domain.Command{ID: "command-1", ControllerEpoch: 1, ExecutionID: "execution-1", ExpectedState: domain.ExecutionReserved, PayloadDigest: domain.PayloadDigest([]byte("only-digest"))}
	if replay, err := store.RecordCommand(ctx, command); err != nil || replay {
		t.Fatalf("initial command = (%t, %v)", replay, err)
	}
	if replay, err := store.RecordCommand(ctx, command); err != nil || !replay {
		t.Fatalf("exact replay = (%t, %v)", replay, err)
	}
	command.PayloadDigest = domain.PayloadDigest([]byte("changed"))
	if _, err := store.RecordCommand(ctx, command); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed command = %v", err)
	}
	if err := store.RecordObservation(ctx, Observation{ExecutionID: "execution-1", State: domain.ExecutionRunning}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCleanupTombstone(ctx, CleanupTombstone{ExecutionID: "execution-1", FailureCode: CleanupProcessResidue}); err != nil {
		t.Fatal(err)
	}
	canary := CleanupFailureCode("jit-plaintext-join-secret-node-private-key.example.test")
	if err := store.RecordCleanupTombstone(ctx, CleanupTombstone{ExecutionID: "execution-2", FailureCode: canary}); err == nil {
		t.Fatal("secret-like raw tombstone code accepted")
	}
	if _, err := store.db.Exec(`INSERT INTO execution_observations(execution_id, state, observed_at_unix_nano) VALUES ('bad', 'unknown', 1)`); err == nil {
		t.Fatal("invalid observation bypassed CHECK")
	}
	contents, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), string(canary)) {
		t.Fatal("rejected secret canary reached journal")
	}
}

func TestPrivatePathsURIBackupAndRestoreGuards(t *testing.T) {
	ctx := context.Background()
	dir := privateTestDir(t)
	specialDir := filepath.Join(dir, "space ?# directory")
	if err := os.Mkdir(specialDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(specialDir, "controller ?#.db")
	store := openControllerPath(t, path)
	defer store.Close()
	if _, _, err := store.Assign(ctx, testAssignment(1, "execution-1", "node-1", 0)); err != nil {
		t.Fatal(err)
	}
	dsn, err := sqliteDSN(path, true)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(dsn)
	if err != nil || uri.RawQuery != "mode=ro" || strings.Contains(uri.EscapedPath(), "%3F") == false || strings.Contains(uri.EscapedPath(), "%23") == false {
		t.Fatalf("unsafe SQLite URI %q: %v", dsn, err)
	}
	windowsDSN, err := sqliteDSN(`C:\\tewake\\data ?#.db`, false)
	if err != nil || strings.Contains(windowsDSN, "?") || strings.Contains(windowsDSN, "#") {
		t.Fatalf("Windows-style path escaped unsafely: %q, %v", windowsDSN, err)
	}
	backup := filepath.Join(specialDir, "backup.db")
	if err := store.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if got := tableColumns(t, store.db, "processed_messages"); strings.Join(got, ",") != "scale_set_id,message_id,message_digest,execution_id,created_at_unix_nano" {
		t.Fatalf("backup source schema is not secret allowlisted: %v", got)
	}
	if err := store.Backup(ctx, backup); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("backup overwrite = %v", err)
	}
	restored := filepath.Join(specialDir, "restored.db")
	if err := RestoreController(ctx, restored, backup); err != nil {
		t.Fatal(err)
	}
	if err := RestoreController(ctx, restored, backup); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("restore overwrite = %v", err)
	}
	if runtime.GOOS != "windows" {
		symlink := filepath.Join(specialDir, "backup-link.db")
		if err := os.Symlink(backup, symlink); err != nil {
			t.Fatal(err)
		}
		if err := RestoreController(ctx, filepath.Join(specialDir, "from-link.db"), symlink); !errors.Is(err, ErrInsecurePath) {
			t.Fatalf("symlink backup accepted: %v", err)
		}
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(specialDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if opened, err := OpenController(ctx, filepath.Join(specialDir, "unsafe.db"), Options{}); opened != nil || !errors.Is(err, ErrInsecurePath) {
			t.Fatalf("shared directory accepted: (%v, %v)", opened, err)
		}
	}
}

func TestRestoreBadAndWrongRolePreservesDestination(t *testing.T) {
	ctx := context.Background()
	dir := privateTestDir(t)
	destination := filepath.Join(dir, "destination.db")
	destinationStore := openControllerPath(t, destination)
	if _, err := destinationStore.AdvanceEpoch(ctx); err != nil {
		t.Fatal(err)
	}
	_ = destinationStore.Close()
	bad := filepath.Join(dir, "bad.backup")
	if err := os.WriteFile(bad, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RestoreController(ctx, destination, bad); err == nil {
		t.Fatal("bad restore succeeded")
	}
	assertEpoch(t, destination, 2)
	agent, err := OpenAgent(ctx, filepath.Join(dir, "agent.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	agentBackup := filepath.Join(dir, "agent.backup")
	if err := agent.Backup(ctx, agentBackup); err != nil {
		t.Fatal(err)
	}
	_ = agent.Close()
	if err := RestoreController(ctx, destination, agentBackup); !errors.Is(err, ErrWrongRole) {
		t.Fatalf("wrong role restore = %v", err)
	}
	assertEpoch(t, destination, 3)
}

func openController(t *testing.T, name string) *ControllerStore {
	t.Helper()
	return openControllerPath(t, filepath.Join(privateTestDir(t), name))
}

func openControllerPath(t *testing.T, path string) *ControllerStore {
	t.Helper()
	store, err := OpenController(context.Background(), path, Options{Now: func() time.Time { return time.Unix(100, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func privateTestDir(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func testAssignment(messageID MessageID, executionID, nodeID string, slot int) Assignment {
	return Assignment{ScaleSetID: 11, MessageID: messageID, MessageDigest: domain.PayloadDigest([]byte("message-" + string(rune(messageID)))), Execution: domain.ExecutionSnapshot{ID: domain.ExecutionID(executionID), TargetID: "target-1", Slot: domain.SlotKey{NodeID: domain.NodeID(nodeID), Index: slot}, State: domain.ExecutionReserved}}
}

func mutateVersions(t *testing.T, path, statement string) {
	t.Helper()
	dsn, err := sqliteDSN(path, false)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(sqliteDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}

func openRawTestDatabase(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(privateTestDir(t), "raw.db")
	if _, err := prepareDatabasePath(path); err != nil {
		t.Fatal(err)
	}
	dsn, err := sqliteDSN(path, false)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(sqliteDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func fstestMapFS(files map[string]string) fs.FS {
	result := make(fstest.MapFS, len(files))
	for name, body := range files {
		result[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return result
}

func tableColumns(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := []string{}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	return columns
}

func assertCount(t *testing.T, db *sql.DB, query string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("count %q = %d, want %d", query, got, want)
	}
}

func assertEpoch(t *testing.T, path string, want domain.ControllerEpoch) {
	t.Helper()
	store := openControllerPath(t, path)
	defer store.Close()
	got, err := store.AdvanceEpoch(context.Background())
	if err != nil || got != want {
		t.Fatalf("epoch = %d, %v; want %d", got, err, want)
	}
}
