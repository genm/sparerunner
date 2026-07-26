package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
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
	var busyTimeout int
	if err := store.db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout); err != nil || busyTimeout != sqliteBusyTimeoutMilliseconds {
		t.Fatalf("busy timeout = %d, %v", busyTimeout, err)
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
	invalid = testAssignment(1, "execution-1", "node-1", 0)
	invalid.ScaleSetID = ScaleSetID(maxSQLiteInteger + 1)
	if _, _, err := store.Assign(ctx, invalid); err == nil {
		t.Fatal("out-of-range scale set ID accepted")
	}
	invalid = testAssignment(MessageID(maxSQLiteInteger+1), "execution-1", "node-1", 0)
	if _, _, err := store.Assign(ctx, invalid); err == nil {
		t.Fatal("out-of-range message ID accepted")
	}
	boundary := testAssignment(MessageID(maxSQLiteInteger), "execution-max-id", "node-max-id", 0)
	boundary.ScaleSetID = ScaleSetID(maxSQLiteInteger)
	if _, replayed, err := store.Assign(ctx, boundary); err != nil || replayed {
		t.Fatalf("maximum signed SQLite IDs = (%t, %v)", replayed, err)
	}
	if _, err := store.db.Exec(`INSERT INTO slot_reservations(node_id, slot_index, target_id, execution_id) VALUES ('node-direct', 0, 'target-direct', 'execution-direct')`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES ('execution-direct', 'target-direct', 'node-direct', 0, 'running', 1)`); err == nil {
		t.Fatal("direct invalid execution state bypassed CHECK")
	}
	if _, err := store.db.Exec(`INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES ('different-execution', 'target-direct', 'node-direct', 0, 'reserved', 1)`); err == nil {
		t.Fatal("execution-to-reservation identity mismatch bypassed composite foreign key")
	}
	if _, err := store.db.Exec(`INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES ('execution-direct', 'different-target', 'node-direct', 0, 'reserved', 1)`); err == nil {
		t.Fatal("execution-to-reservation target mismatch bypassed composite foreign key")
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
	start := make(chan struct{})
	for index, candidate := range []*ControllerStore{first, second} {
		group.Add(1)
		go func(index int, candidate *ControllerStore) {
			defer group.Done()
			<-start
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
	close(start)
	group.Wait()
	if successes != 1 {
		t.Fatalf("concurrent successes = %d", successes)
	}

	replayPath := filepath.Join(privateTestDir(t), "concurrent-replay.db")
	replayFirst, replaySecond := openControllerPath(t, replayPath), openControllerPath(t, replayPath)
	defer replayFirst.Close()
	defer replaySecond.Close()
	replayAssignment := testAssignment(91, "replayed-execution", "node-replay", 0)
	start = make(chan struct{})
	results := make(chan bool, 2)
	for _, candidate := range []*ControllerStore{replayFirst, replaySecond} {
		group.Add(1)
		go func(candidate *ControllerStore) {
			defer group.Done()
			<-start
			_, replayed, err := candidate.Assign(ctx, replayAssignment)
			if err != nil {
				t.Errorf("concurrent replay assignment: %v", err)
				return
			}
			results <- replayed
		}(candidate)
	}
	close(start)
	group.Wait()
	close(results)
	var initial, replayed int
	for replay := range results {
		if replay {
			replayed++
		} else {
			initial++
		}
	}
	if initial != 1 || replayed != 1 {
		t.Fatalf("concurrent replay results initial=%d replayed=%d", initial, replayed)
	}
}

func TestControllerSnapshotSurvivesRestartAndPreventsDuplicateAssignment(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller.db")
	store := openControllerPath(t, path)
	empty, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if empty.ControllerEpoch != 0 || len(empty.Reservations) != 0 || len(empty.Executions) != 0 || len(empty.ProcessedMessages) != 0 {
		t.Fatalf("non-empty initial snapshot: %+v", empty)
	}
	if _, err := store.AdvanceEpoch(ctx); err != nil {
		t.Fatal(err)
	}
	assignment := testAssignment(44, "restart-execution", "restart-node", 2)
	if _, replayed, err := store.Assign(ctx, assignment); err != nil || replayed {
		t.Fatalf("initial assignment = (%t, %v)", replayed, err)
	}
	before, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openControllerPath(t, path)
	defer reopened.Close()
	after, err := reopened.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("restart snapshot changed\nbefore=%+v\nafter=%+v", before, after)
	}
	got, replayed, err := reopened.Assign(ctx, assignment)
	if err != nil || !replayed || got != assignment.Execution {
		t.Fatalf("restart replay = (%+v, %t, %v)", got, replayed, err)
	}
	assertCount(t, reopened.db, "SELECT count(*) FROM processed_messages", 1)
	assertCount(t, reopened.db, "SELECT count(*) FROM slot_reservations", 1)
	assertCount(t, reopened.db, "SELECT count(*) FROM executions", 1)
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

func TestAgentEpochFenceAndSnapshotSurviveRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "agent.db")
	store, err := OpenAgent(ctx, path, Options{Now: func() time.Time { return time.Unix(200, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if empty.MaxControllerEpoch != 0 || len(empty.Commands) != 0 || len(empty.Observations) != 0 || len(empty.CleanupTombstones) != 0 {
		t.Fatalf("non-empty initial agent snapshot: %+v", empty)
	}
	command := domain.Command{
		ID:              "command-epoch-3",
		ControllerEpoch: 3,
		ExecutionID:     "execution-1",
		ExpectedState:   domain.ExecutionReserved,
		PayloadDigest:   domain.PayloadDigest([]byte("command-epoch-3")),
	}
	if replayed, err := store.RecordCommand(ctx, command); err != nil || replayed {
		t.Fatalf("epoch 3 command = (%t, %v)", replayed, err)
	}
	if err := store.RecordObservation(ctx, Observation{ExecutionID: command.ExecutionID, State: domain.ExecutionRunning}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCleanupTombstone(ctx, CleanupTombstone{ExecutionID: command.ExecutionID, FailureCode: CleanupVerificationFailed}); err != nil {
		t.Fatal(err)
	}
	before, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.MaxControllerEpoch != 3 {
		t.Fatalf("max controller epoch = %d", before.MaxControllerEpoch)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenAgent(ctx, path, Options{Now: func() time.Time { return time.Unix(200, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("agent restart snapshot changed\nbefore=%+v\nafter=%+v", before, after)
	}
	if replayed, err := reopened.RecordCommand(ctx, command); err != nil || !replayed {
		t.Fatalf("exact old command replay = (%t, %v)", replayed, err)
	}
	stale := command
	stale.ID = "fresh-stale-command"
	stale.ControllerEpoch = 2
	stale.PayloadDigest = domain.PayloadDigest([]byte("fresh-stale-command"))
	if _, err := reopened.RecordCommand(ctx, stale); !errors.Is(err, ErrStaleControllerEpoch) {
		t.Fatalf("fresh stale command = %v", err)
	}
	sameEpoch := command
	sameEpoch.ID = "same-epoch-command"
	sameEpoch.PayloadDigest = domain.PayloadDigest([]byte("same-epoch-command"))
	if replayed, err := reopened.RecordCommand(ctx, sameEpoch); err != nil || replayed {
		t.Fatalf("same epoch command = (%t, %v)", replayed, err)
	}
	higher := command
	higher.ID = "higher-epoch-command"
	higher.ControllerEpoch = 4
	higher.PayloadDigest = domain.PayloadDigest([]byte("higher-epoch-command"))
	if replayed, err := reopened.RecordCommand(ctx, higher); err != nil || replayed {
		t.Fatalf("higher epoch command = (%t, %v)", replayed, err)
	}
	final, err := reopened.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if final.MaxControllerEpoch != 4 || len(final.Commands) != 3 {
		t.Fatalf("final agent snapshot = %+v", final)
	}
}

func TestPrivatePathsURIBackupAndRestoreGuards(t *testing.T) {
	ctx := context.Background()
	dir := privateTestDir(t)
	specialDir := filepath.Join(dir, "space #% directory")
	if err := os.Mkdir(specialDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(specialDir, "controller #%25.db")
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
	absPath, absErr := filepath.Abs(path)
	if err != nil || absErr != nil || uri.Scheme != "file" || uri.Host != "" || uri.RawQuery != "mode=ro" || uri.Path != sqliteURIPath(absPath) || !strings.Contains(uri.EscapedPath(), "%23") || !strings.Contains(uri.EscapedPath(), "%25") || !strings.Contains(uri.EscapedPath(), "%20") {
		t.Fatalf("unsafe SQLite URI %q: %v", dsn, err)
	}
	writeDSN, err := sqliteDSN(path, false)
	if err != nil {
		t.Fatal(err)
	}
	writeURI, err := url.Parse(writeDSN)
	if err != nil || writeURI.Host != "" || writeURI.Query().Get("_txlock") != "immediate" {
		t.Fatalf("invalid writable SQLite URI %q: %v", writeDSN, err)
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
	assertNoTemporaryStoreFiles(t, dir)
}

func TestSchemaObjectsAndForeignKeysFailClosedOnOpen(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{
			name:      "extra secret table",
			statement: `CREATE TABLE secrets(value TEXT NOT NULL); INSERT INTO secrets(value) VALUES ('secret-canary.example.test')`,
		},
		{
			name:      "extra secret metadata",
			statement: `INSERT INTO store_metadata(key, value) VALUES ('private_key', 'secret-canary.example.test')`,
		},
		{
			name:      "extra view",
			statement: `CREATE VIEW execution_view AS SELECT id FROM executions`,
		},
		{
			name:      "extra trigger",
			statement: `CREATE TRIGGER execution_trigger AFTER INSERT ON executions BEGIN SELECT 1; END`,
		},
		{
			name:      "extra index",
			statement: `CREATE INDEX execution_target_index ON executions(target_id)`,
		},
		{
			name:      "constraint drift",
			statement: `ALTER TABLE executions ADD COLUMN leaked_material TEXT`,
		},
		{
			name: "foreign key violation",
			statement: `
				PRAGMA foreign_keys=OFF;
				INSERT INTO slot_reservations(node_id, slot_index, target_id, execution_id)
					VALUES ('node-corrupt', 0, 'target-a', 'execution-corrupt');
				INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano)
					VALUES ('execution-corrupt', 'target-b', 'node-corrupt', 0, 'reserved', 1);`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(privateTestDir(t), "controller.db")
			store := openControllerPath(t, path)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			execRaw(t, path, test.statement)
			degraded, err := OpenController(context.Background(), path, Options{})
			if degraded == nil || !errors.Is(err, ErrRecoveryMode) {
				t.Fatalf("drifted database open = (%v, %v)", degraded, err)
			}
			if readyErr := degraded.Ready(); !errors.Is(readyErr, ErrRecoveryMode) {
				t.Fatalf("drifted database readiness = %v", readyErr)
			}
			_ = degraded.Close()
		})
	}
}

func TestSecretCanaryBackupIsRejectedWithoutCreatingDestination(t *testing.T) {
	ctx := context.Background()
	dir := privateTestDir(t)
	store := openControllerPath(t, filepath.Join(dir, "controller.db"))
	backup := filepath.Join(dir, "controller.backup")
	if err := store.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	execRaw(t, backup, `CREATE TABLE secrets(value TEXT NOT NULL); INSERT INTO secrets(value) VALUES ('secret-canary.example.test')`)
	var canary string
	queryRaw(t, backup, `SELECT value FROM secrets`).Scan(&canary)
	if canary != "secret-canary.example.test" {
		t.Fatalf("secret canary fixture = %q", canary)
	}
	destination := filepath.Join(dir, "restored.db")
	if err := RestoreController(ctx, destination, backup); !errors.Is(err, ErrCorruptBackup) {
		t.Fatalf("secret-bearing backup restore = %v", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected restore created destination: %v", err)
	}
	assertNoTemporaryStoreFiles(t, dir)
}

func TestCanceledBackupPreservesSourceAndCleansTemporaryFile(t *testing.T) {
	dir := privateTestDir(t)
	store := openControllerPath(t, filepath.Join(dir, "controller.db"))
	defer store.Close()
	if _, _, err := store.Assign(context.Background(), testAssignment(7, "execution-cancel", "node-cancel", 0)); err != nil {
		t.Fatal(err)
	}
	before, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	destination := filepath.Join(dir, "canceled.backup")
	if err := store.Backup(ctx, destination); err == nil {
		t.Fatal("canceled backup succeeded")
	}
	after, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("canceled backup mutated source\nbefore=%+v\nafter=%+v", before, after)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled backup created destination: %v", err)
	}
	assertNoTemporaryStoreFiles(t, dir)
}

func TestAtomicPublishNeverClobbersConcurrentWinner(t *testing.T) {
	dir := privateTestDir(t)
	destination := filepath.Join(dir, "winner.db")
	const contenders = 16
	type result struct {
		value string
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, contenders)
	var group sync.WaitGroup
	for index := 0; index < contenders; index++ {
		value := strings.Repeat(string(rune('a'+index)), index+1)
		temporary := filepath.Join(dir, ".candidate-"+string(rune('a'+index)))
		if err := os.WriteFile(temporary, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := syncFile(temporary); err != nil {
			t.Fatal(err)
		}
		group.Add(1)
		go func(temporary, value string) {
			defer group.Done()
			<-start
			results <- result{value: value, err: publishNoReplace(temporary, destination)}
		}(temporary, value)
	}
	close(start)
	group.Wait()
	close(results)
	var winner string
	var successes int
	for result := range results {
		if result.err == nil {
			successes++
			winner = result.value
			continue
		}
		if !errors.Is(result.err, ErrDestinationExists) {
			t.Errorf("publish contention error = %v", result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("publish successes = %d", successes)
	}
	content, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != winner {
		t.Fatalf("published content = %q, winner = %q", content, winner)
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".candidate-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("publish left temporary files: %v", matches)
	}
}

func TestConcurrentBackupDoesNotClobberWinner(t *testing.T) {
	ctx := context.Background()
	dir := privateTestDir(t)
	sources := make([]*ControllerStore, 2)
	for index := range sources {
		sources[index] = openControllerPath(t, filepath.Join(dir, "source-"+string(rune('a'+index))+".db"))
		for advances := 0; advances <= index; advances++ {
			if _, err := sources[index].AdvanceEpoch(ctx); err != nil {
				t.Fatal(err)
			}
		}
		defer sources[index].Close()
	}
	destination := filepath.Join(dir, "winner.backup")
	start := make(chan struct{})
	results := make(chan error, len(sources))
	var group sync.WaitGroup
	for _, source := range sources {
		group.Add(1)
		go func(source *ControllerStore) {
			defer group.Done()
			<-start
			results <- source.Backup(ctx, destination)
		}(source)
	}
	close(start)
	group.Wait()
	close(results)
	var successes int
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrDestinationExists) {
			t.Errorf("backup contention error = %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("backup successes = %d", successes)
	}
	restoredPath := filepath.Join(dir, "restored.db")
	if err := RestoreController(ctx, restoredPath, destination); err != nil {
		t.Fatal(err)
	}
	restored := openControllerPath(t, restoredPath)
	snapshot, err := restored.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ControllerEpoch != 1 && snapshot.ControllerEpoch != 2 {
		t.Fatalf("backup winner epoch = %d", snapshot.ControllerEpoch)
	}
	_ = restored.Close()
	assertNoTemporaryStoreFiles(t, dir)
}

func TestConcurrentRestoreDoesNotClobberAndPreservesSources(t *testing.T) {
	ctx := context.Background()
	dir := privateTestDir(t)
	backups := make([]string, 2)
	before := make([][]byte, 2)
	for index := range backups {
		source := openControllerPath(t, filepath.Join(dir, "source-"+string(rune('a'+index))+".db"))
		for advances := 0; advances <= index; advances++ {
			if _, err := source.AdvanceEpoch(ctx); err != nil {
				t.Fatal(err)
			}
		}
		backups[index] = filepath.Join(dir, "backup-"+string(rune('a'+index))+".db")
		if err := source.Backup(ctx, backups[index]); err != nil {
			t.Fatal(err)
		}
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
		var err error
		before[index], err = os.ReadFile(backups[index])
		if err != nil {
			t.Fatal(err)
		}
	}
	destination := filepath.Join(dir, "restored.db")
	start := make(chan struct{})
	results := make(chan error, len(backups))
	var group sync.WaitGroup
	for _, backup := range backups {
		group.Add(1)
		go func(backup string) {
			defer group.Done()
			<-start
			results <- RestoreController(ctx, destination, backup)
		}(backup)
	}
	close(start)
	group.Wait()
	close(results)
	var successes int
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrDestinationExists) {
			t.Errorf("restore contention error = %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("restore successes = %d", successes)
	}
	restored := openControllerPath(t, destination)
	snapshot, err := restored.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ControllerEpoch != 1 && snapshot.ControllerEpoch != 2 {
		t.Fatalf("restored winner epoch = %d", snapshot.ControllerEpoch)
	}
	_ = restored.Close()
	for index, backup := range backups {
		after, err := os.ReadFile(backup)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(after, before[index]) {
			t.Fatalf("restore mutated source backup %d", index)
		}
	}
	assertNoTemporaryStoreFiles(t, dir)
}

func TestDirectoryDurabilityContract(t *testing.T) {
	dir := privateTestDir(t)
	if runtime.GOOS == "windows" && directorySyncSupported {
		t.Fatal("Windows unexpectedly claims portable directory fsync support")
	}
	if runtime.GOOS != "windows" && !directorySyncSupported {
		t.Fatal("Unix build disabled directory fsync")
	}
	if err := syncDirectory(dir); err != nil {
		t.Fatal(err)
	}
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

func execRaw(t *testing.T, path, statement string) {
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
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}

func queryRaw(t *testing.T, path, query string) *sql.Row {
	t.Helper()
	dsn, err := sqliteDSN(path, true)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(sqliteDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	return db.QueryRow(query)
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

func assertNoTemporaryStoreFiles(t *testing.T, directory string) {
	t.Helper()
	for _, pattern := range []string{".tewake-backup-*", ".tewake-restore-*"} {
		matches, err := filepath.Glob(filepath.Join(directory, pattern))
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) != 0 {
			t.Fatalf("temporary store files remain: %v", matches)
		}
	}
}
