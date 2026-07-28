package store

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/genm/sparerunner/internal/domain"
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
	if _, err := store.db.Exec(`INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES ('execution-direct', 'target-direct', 'node-direct', 0, 'unknown', 1)`); err == nil {
		t.Fatal("direct invalid execution state bypassed CHECK")
	}
	if _, err := store.db.Exec(`INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES ('execution-direct', 'target-direct', 'node-direct', 0, 'reserved', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO slot_reservations(node_id, slot_index, target_id, execution_id) VALUES ('node-direct', 0, 'target-direct', 'execution-direct')`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES ('different-execution', 'target-direct', 'node-direct', 1, 'reserved', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO slot_reservations(node_id, slot_index, target_id, execution_id) VALUES ('different-node', 1, 'target-direct', 'different-execution')`); err == nil {
		t.Fatal("reservation-to-execution node mismatch bypassed composite foreign key")
	}
	if _, err := store.db.Exec(`INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES ('different-target-execution', 'target-direct', 'node-direct', 2, 'reserved', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO slot_reservations(node_id, slot_index, target_id, execution_id) VALUES ('node-direct', 2, 'different-target', 'different-target-execution')`); err == nil {
		t.Fatal("reservation-to-execution target mismatch bypassed composite foreign key")
	}
	if _, err := store.db.Exec(`INSERT INTO processed_messages(scale_set_id, message_id, message_digest, execution_id, created_at_unix_nano) VALUES (0, 1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'execution-direct', 1)`); err == nil {
		t.Fatal("zero numeric message identity bypassed CHECK")
	}
	columns := tableColumns(t, store.db, "processed_messages")
	if strings.Join(columns, ",") != "scale_set_id,message_id,message_digest,execution_id,created_at_unix_nano" {
		t.Fatalf("processed_messages has redundant drift columns: %v", columns)
	}
}

func TestActiveExecutionReservationCannotBeDeleted(t *testing.T) {
	ctx := context.Background()
	store := openController(t, "controller.db")
	defer store.Close()

	const executionID = "execution-active-lease"
	if _, replayed, err := store.Assign(ctx, testAssignment(92, executionID, "node-active-lease", 0)); err != nil || replayed {
		t.Fatalf("assign active execution = (%t, %v)", replayed, err)
	}
	if _, err := store.db.Exec(`DELETE FROM slot_reservations WHERE execution_id = ?`, executionID); err == nil {
		t.Fatal("active execution reservation was deleted")
	}
	assertCount(t, store.db, `SELECT count(*) FROM slot_reservations WHERE execution_id = 'execution-active-lease'`, 1)
}

func TestExecutionReservationDataInvariantFailsClosedOnOpen(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{
			name: "active execution without reservation",
			statement: `
				UPDATE executions SET state = 'released' WHERE id = 'execution-reservation-invariant';
				DELETE FROM slot_reservations WHERE execution_id = 'execution-reservation-invariant';
				UPDATE executions SET state = 'running' WHERE id = 'execution-reservation-invariant';`,
		},
		{
			name:      "terminal execution with reservation",
			statement: `UPDATE executions SET state = 'released' WHERE id = 'execution-reservation-invariant';`,
		},
		{
			name:      "quarantined execution with reservation",
			statement: `UPDATE executions SET state = 'quarantined' WHERE id = 'execution-reservation-invariant';`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(privateTestDir(t), "controller.db")
			controller := openControllerPath(t, path)
			if _, replayed, err := controller.Assign(ctx, testAssignment(
				93,
				"execution-reservation-invariant",
				"node-reservation-invariant",
				0,
			)); err != nil || replayed {
				t.Fatalf("assign execution = (%t, %v)", replayed, err)
			}
			if err := controller.Close(); err != nil {
				t.Fatal(err)
			}

			execRaw(t, path, test.statement)
			degraded, err := OpenController(ctx, path, Options{})
			if degraded == nil || !errors.Is(err, ErrRecoveryMode) || !errors.Is(err, ErrCorruptBackup) {
				t.Fatalf("invalid reservation database open = (%v, %v)", degraded, err)
			}
			if readyErr := degraded.Ready(); !errors.Is(readyErr, ErrRecoveryMode) {
				t.Fatalf("invalid reservation database readiness = %v", readyErr)
			}
			_ = degraded.Close()
		})
	}
}

func TestUnpickedRequeueIntentDataInvariantFailsClosedOnOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller-intent-invariant.db")
	controller := openControllerPath(t, path)
	fixture := prepareGitHubStartedTerminalRequeueFixture(
		t,
		controller,
		8,
		96,
		1601,
		2601,
		domain.ExecutionReleased,
		domain.ExecutionErrorNone,
	)
	fresh := githubQueueMessageForTest(
		fixture.current.ScaleSetID,
		1602,
		fixture.current.ClaimKey,
	)
	fresh.Jobs[0].ExecutionID = "execution-intent-proposed"
	commit, err := controller.CommitGitHubQueueMessage(
		ctx,
		fresh,
		fixture.binding,
	)
	if err != nil || commit.RequeueIntent == nil {
		_ = controller.Close()
		t.Fatalf("create valid requeue intent = (%#v, %v)", commit, err)
	}
	if _, err := controller.db.ExecContext(ctx, `INSERT INTO executions(
			id, target_id, node_id, slot_index, state, created_at_unix_nano
		) VALUES (
			'execution-unrelated-terminal', 'target-unrelated',
			'node-unrelated', 0, 'released', ?
		)`,
		time.Unix(200, 0).UnixNano(),
	); err != nil {
		_ = controller.Close()
		t.Fatal(err)
	}
	if _, err := controller.db.ExecContext(ctx, `UPDATE
			github_unpicked_requeue_intents
		SET old_execution_id = 'execution-unrelated-terminal'
		WHERE scale_set_id = ? AND claim_key = ?`,
		fixture.current.ScaleSetID,
		fixture.current.ClaimKey,
	); err != nil {
		_ = controller.Close()
		t.Fatal(err)
	}
	if err := validateForeignKeys(ctx, controller.db); err != nil {
		_ = controller.Close()
		t.Fatalf("corruption fixture must retain valid foreign keys: %v", err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}

	degraded, err := OpenController(ctx, path, Options{})
	if degraded == nil ||
		!errors.Is(err, ErrRecoveryMode) ||
		!errors.Is(err, ErrCorruptBackup) {
		t.Fatalf("invalid intent database open = (%v, %v)", degraded, err)
	}
	if readyErr := degraded.Ready(); !errors.Is(readyErr, ErrRecoveryMode) {
		t.Fatalf("invalid intent database readiness = %v", readyErr)
	}
	_ = degraded.Close()
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

func TestControllerSnapshotSurvivesProcessKillAndPreventsDuplicateAssignment(t *testing.T) {
	const helperEnvironment = "SPARERUNNER_CONTROLLER_CRASH_PATH"
	if path := os.Getenv(helperEnvironment); path != "" {
		store, err := OpenController(context.Background(), path, Options{Now: func() time.Time { return time.Unix(100, 0) }})
		if err != nil {
			crashHelperExit("open controller: %v", err)
		}
		if _, err := store.db.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
			crashHelperExit("disable WAL auto-checkpoint: %v", err)
		}
		if epoch, err := store.AdvanceEpoch(context.Background()); err != nil || epoch != 1 {
			crashHelperExit("advance epoch = %d, %v", epoch, err)
		}
		assignment := testAssignment(45, "crash-execution", "crash-node", 3)
		if _, replayed, err := store.Assign(context.Background(), assignment); err != nil || replayed {
			crashHelperExit("assign = %t, %v", replayed, err)
		}
		fmt.Fprintln(os.Stdout, "crash-ready")
		select {}
	}

	path := filepath.Join(privateTestDir(t), "controller.db")
	runAndKillStoreHelper(t, "TestControllerSnapshotSurvivesProcessKillAndPreventsDuplicateAssignment", helperEnvironment, path)
	walInfo, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatalf("committed WAL missing after process kill: %v", err)
	}
	if walInfo.Size() <= 32 {
		t.Fatalf("committed WAL is unexpectedly empty: %d bytes", walInfo.Size())
	}

	reopened := openControllerPath(t, path)
	defer reopened.Close()
	snapshot, err := reopened.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assignment := testAssignment(45, "crash-execution", "crash-node", 3)
	if snapshot.ControllerEpoch != 1 ||
		len(snapshot.Reservations) != 1 ||
		len(snapshot.Executions) != 1 ||
		len(snapshot.ProcessedMessages) != 1 ||
		snapshot.Executions[0] != assignment.Execution {
		t.Fatalf("recovered crash snapshot = %+v", snapshot)
	}
	got, replayed, err := reopened.Assign(context.Background(), assignment)
	if err != nil || !replayed || got != assignment.Execution {
		t.Fatalf("crash replay = (%+v, %t, %v)", got, replayed, err)
	}
	assertCount(t, reopened.db, "SELECT count(*) FROM processed_messages", 1)
	assertCount(t, reopened.db, "SELECT count(*) FROM slot_reservations", 1)
	assertCount(t, reopened.db, "SELECT count(*) FROM executions", 1)
}

func TestMigrationTransactionRollsBackAfterProcessKill(t *testing.T) {
	const helperEnvironment = "SPARERUNNER_MIGRATION_CRASH_PATH"
	if path := os.Getenv(helperEnvironment); path != "" {
		_, err := OpenController(context.Background(), path, Options{
			Now: func() time.Time { return time.Unix(100, 0) },
			MigrationHook: func(string, int) error {
				fmt.Fprintln(os.Stdout, "crash-ready")
				select {}
			},
		})
		crashHelperExit("migration returned before process kill: %v", err)
	}

	path := filepath.Join(privateTestDir(t), "controller.db")
	runAndKillStoreHelper(t, "TestMigrationTransactionRollsBackAfterProcessKill", helperEnvironment, path)
	dsn, err := sqliteDSN(path, true)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(sqliteDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var objectCount int
	if err := db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").Scan(&objectCount); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if objectCount != 0 {
		t.Fatalf("killed migration exposed %d partially committed schema objects", objectCount)
	}

	recovered := openControllerPath(t, path)
	defer recovered.Close()
	snapshot, err := recovered.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ControllerEpoch != 0 ||
		len(snapshot.Reservations) != 0 ||
		len(snapshot.Executions) != 0 ||
		len(snapshot.ProcessedMessages) != 0 {
		t.Fatalf("recovered migration snapshot = %+v", snapshot)
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

func TestAgentRuntimeMigrationPreservesHistoricalAssignmentAndSeedsNodeAuthority(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller-v2.db")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
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
	db.SetMaxOpenConns(1)
	if err := configure(ctx, db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	migrations, err := loadMigrations("controller", controllerMigrations)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := applyLoadedMigrations(ctx, db, "controller", migrations[:2], func() time.Time {
		return time.Unix(100, 0)
	}, nil); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	nodeID := enrollmentNodeID(6)
	if _, err := db.Exec(`INSERT INTO enrolled_nodes(
		node_id, current_serial, credential_epoch, not_before_unix_nano,
		not_after_unix_nano, revoked
	) VALUES (?, 'a6', 1, 1, 2, 0)`, nodeID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO slot_reservations(
		node_id, slot_index, target_id, execution_id
	) VALUES (?, 0, 'target-v2', 'execution-v2')`, nodeID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO executions(
		id, target_id, node_id, slot_index, state, created_at_unix_nano
	) VALUES ('execution-v2', 'target-v2', ?, 0, 'reserved', 1)`, nodeID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO processed_messages(
		scale_set_id, message_id, message_digest, execution_id, created_at_unix_nano
	) VALUES (1, 1, ?, 'execution-v2', 1)`, digestForTest("v2-message")); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded := openControllerPath(t, path)
	defer upgraded.Close()
	snapshot, err := upgraded.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Nodes) != 1 ||
		snapshot.Nodes[0] != (NodeAdministration{NodeID: domain.NodeID(nodeID), State: domain.NodeActive}) ||
		len(snapshot.Reservations) != 1 ||
		len(snapshot.Executions) != 1 ||
		snapshot.Executions[0].ID != "execution-v2" {
		t.Fatalf("upgraded controller snapshot = %+v", snapshot)
	}
	if err := validateForeignKeys(ctx, upgraded.db); err != nil {
		t.Fatal(err)
	}
}

func TestQuarantinedTerminalLeaseMigrationUpgradesVersionSixAuthority(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller-v6-quarantined.db")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
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
	db.SetMaxOpenConns(1)
	if err := configure(ctx, db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	migrations, err := loadMigrations("controller", controllerMigrations)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := applyLoadedMigrations(
		ctx,
		db,
		"controller",
		migrations[:6],
		func() time.Time { return time.Unix(100, 0) },
		nil,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	const nodeID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := db.Exec(`INSERT INTO enrolled_nodes(
		node_id, current_serial, credential_epoch, not_before_unix_nano,
		not_after_unix_nano, revoked
	) VALUES (?, 'a6', 1, 1, 2, 0)`, nodeID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE node_administrative_states
		SET administrative_state = 'quarantined' WHERE node_id = ?`, nodeID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO executions(
		id, target_id, node_id, slot_index, state, created_at_unix_nano
	) VALUES ('execution-v6-quarantined', 'target-v6', ?, 0, 'quarantined', 1)`,
		nodeID,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO slot_reservations(
		node_id, slot_index, target_id, execution_id
	) VALUES (?, 0, 'target-v6', 'execution-v6-quarantined')`, nodeID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded := openControllerPath(t, path)
	defer upgraded.Close()
	if err := upgraded.Ready(); err != nil {
		t.Fatal(err)
	}
	assertCount(
		t,
		upgraded.db,
		`SELECT count(*) FROM slot_reservations
		 WHERE execution_id = 'execution-v6-quarantined'`,
		0,
	)
	assertNodeAdministrativeState(
		t,
		upgraded,
		nodeID,
		domain.NodeQuarantined,
	)
	snapshot, err := upgraded.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Controller.Executions) != 1 ||
		snapshot.Controller.Executions[0].State != domain.ExecutionQuarantined ||
		len(snapshot.Controller.Reservations) != 0 {
		t.Fatalf("upgraded quarantined authority = %#v", snapshot.Controller)
	}
}

func TestUnpickedRequeueMigrationUpgradesVersionEightAuthority(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller-v8-unpicked-requeue.db")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
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
	db.SetMaxOpenConns(1)
	if err := configure(ctx, db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	migrations, err := loadMigrations("controller", controllerMigrations)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if len(migrations) < 9 {
		_ = db.Close()
		t.Fatalf("controller migrations = %d, want at least 9", len(migrations))
	}
	if err := applyLoadedMigrations(
		ctx,
		db,
		"controller",
		migrations[:8],
		func() time.Time { return time.Unix(100, 0) },
		nil,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	const (
		scaleSetID      ScaleSetID = 93
		messageID       MessageID  = 1221
		claimKey        int64      = 2321
		executionID                = "execution-v8-migration"
		controllerEpoch            = domain.ControllerEpoch(1)
	)
	attempt := GitHubJITAttempt{
		ScaleSetID:      scaleSetID,
		ClaimKey:        claimKey,
		Attempt:         1,
		ControllerEpoch: controllerEpoch,
		RunnerName:      "sparerunner-v8-migration",
		State:           GitHubJITGenerationAmbiguous,
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO executions(
			id, target_id, node_id, slot_index, state, created_at_unix_nano
		) VALUES (?, 'target-v8-migration', 'node-v8-migration', 0,
			'released', ?)`,
		executionID,
		time.Unix(101, 0).UnixNano(),
	); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_queue_messages(
			scale_set_id, message_id, message_digest, committed_at_unix_nano
		) VALUES (?, ?, ?, ?)`,
		scaleSetID,
		messageID,
		digestForTest("v8-migration-message"),
		time.Unix(101, 0).UnixNano(),
	); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_job_claims(
			scale_set_id, runner_request_id, source_message_id, execution_id,
			state, current_jit_attempt, created_at_unix_nano,
			updated_at_unix_nano
		) VALUES (?, ?, ?, ?, 'reconciliation_required', 1, ?, ?)`,
		scaleSetID,
		claimKey,
		messageID,
		executionID,
		time.Unix(101, 0).UnixNano(),
		time.Unix(101, 0).UnixNano(),
	); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_jit_attempts(
			scale_set_id, runner_request_id, attempt, controller_epoch,
			runner_name, state, runner_id, jit_digest, start_command_id,
			created_at_unix_nano, updated_at_unix_nano
		) VALUES (?, ?, 1, ?, ?, 'generation_ambiguous', NULL, NULL, '', ?, ?)`,
		scaleSetID,
		claimKey,
		controllerEpoch,
		attempt.RunnerName,
		time.Unix(101, 0).UnixNano(),
		time.Unix(101, 0).UnixNano(),
	); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	snapshotDigest := digestForTest("v8-migration-snapshot")
	if _, err := db.ExecContext(ctx, `INSERT INTO github_jit_snapshot_authority(
			scale_set_id, runner_request_id, attempt, snapshot_digest,
			controller_epoch, decision, updated_at_unix_nano,
			github_session_generation
		) VALUES (?, ?, ?, ?, ?, 'generation_absence_pending', ?, 7)`,
		scaleSetID,
		claimKey,
		attempt.Attempt,
		snapshotDigest,
		controllerEpoch,
		time.Unix(102, 0).UnixNano(),
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded := openControllerPath(t, path)
	defer upgraded.Close()
	var (
		gotDigest     string
		gotEpoch      domain.ControllerEpoch
		gotDecision   string
		gotGeneration uint64
	)
	if err := upgraded.db.QueryRowContext(ctx, `SELECT snapshot_digest,
			controller_epoch, decision, github_session_generation
		FROM github_jit_snapshot_authority
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		scaleSetID,
		claimKey,
		attempt.Attempt,
	).Scan(
		&gotDigest,
		&gotEpoch,
		&gotDecision,
		&gotGeneration,
	); err != nil {
		t.Fatal(err)
	}
	if gotDigest != snapshotDigest ||
		gotEpoch != controllerEpoch ||
		gotDecision != "generation_absence_pending" ||
		gotGeneration != 7 {
		t.Fatalf(
			"upgraded v8 authority = digest %q epoch %d decision %q generation %d",
			gotDigest,
			gotEpoch,
			gotDecision,
			gotGeneration,
		)
	}
	assertCount(
		t,
		upgraded.db,
		`SELECT count(*) FROM sqlite_schema
			WHERE type = 'table' AND name = 'github_unpicked_requeue_intents'`,
		1,
	)
	if _, err := upgraded.db.ExecContext(ctx, `UPDATE github_jit_snapshot_authority
		SET decision = 'unpicked_requeue_removal_issued'
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		scaleSetID,
		claimKey,
		attempt.Attempt,
	); err != nil {
		t.Fatalf("new v9 authority decision rejected = %v", err)
	}
	if _, err := upgraded.db.ExecContext(ctx, `UPDATE github_jit_snapshot_authority
		SET decision = 'unknown'
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		scaleSetID,
		claimKey,
		attempt.Attempt,
	); err == nil {
		t.Fatal("v9 authority CHECK accepted unknown decision")
	}
	if err := validateForeignKeys(ctx, upgraded.db); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleRequestMigrationPreservesVersionNineIntentAndForeignKeys(
	t *testing.T,
) {
	ctx := context.Background()
	path := filepath.Join(
		privateTestDir(t),
		"controller-v9-lifecycle-request.db",
	)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
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
	db.SetMaxOpenConns(1)
	if err := configure(ctx, db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	migrations, err := loadMigrations("controller", controllerMigrations)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if len(migrations) < 10 {
		_ = db.Close()
		t.Fatalf("controller migrations = %d, want at least 10", len(migrations))
	}
	if err := applyLoadedMigrations(
		ctx,
		db,
		"controller",
		migrations[:9],
		func() time.Time { return time.Unix(100, 0) },
		nil,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	const (
		scaleSetID     ScaleSetID = 109
		claimKey       int64      = 4109
		oldMessageID   MessageID  = 2101
		freshMessageID MessageID  = 2102
		oldExecutionID            = "v9-lifecycle-old-execution"
		replacementID             = "v9-lifecycle-replacement"
		runnerName                = "sparerunner-v9-lifecycle-runner"
	)
	now := time.Unix(101, 0).UnixNano()
	for _, message := range []struct {
		id     MessageID
		digest string
	}{
		{oldMessageID, digestForTest("v9-lifecycle-old-message")},
		{freshMessageID, digestForTest("v9-lifecycle-fresh-message")},
	} {
		if _, err := db.ExecContext(ctx, `INSERT INTO github_queue_messages(
				scale_set_id, message_id, message_digest, committed_at_unix_nano
			) VALUES (?, ?, ?, ?)`,
			scaleSetID,
			message.id,
			message.digest,
			now,
		); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO github_message_jobs(
			scale_set_id, message_id, event_index, event_type,
			runner_request_id, runner_id, runner_name, result,
			repository_name, owner_name, job_id, workflow_run_id
		) VALUES (?, ?, 0, 'JobAvailable', ?, 0, '', '', '', '', '', 0)`,
		scaleSetID,
		freshMessageID,
		claimKey,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO executions(
			id, target_id, node_id, slot_index, state, created_at_unix_nano
		) VALUES (?, 'target-v9', 'node-v9', 0, 'released', ?)`,
		oldExecutionID,
		now,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO github_job_claims(
			scale_set_id, runner_request_id, source_message_id, execution_id,
			state, current_jit_attempt, created_at_unix_nano,
			updated_at_unix_nano
		) VALUES (?, ?, ?, ?, 'reconciliation_required', 1, ?, ?)`,
		scaleSetID,
		claimKey,
		oldMessageID,
		oldExecutionID,
		now,
		now,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO github_jit_attempts(
			scale_set_id, runner_request_id, attempt, controller_epoch,
			runner_name, state, runner_id, jit_digest, start_command_id,
			created_at_unix_nano, updated_at_unix_nano
		) VALUES (?, ?, 1, 1, ?, 'started', 91, ?, 'v9-start', ?, ?)`,
		scaleSetID,
		claimKey,
		runnerName,
		digestForTest("v9-lifecycle-jit"),
		now,
		now,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO github_acquire_attempts(
			scale_set_id, runner_request_id, attempt, evidence_message_id,
			controller_epoch, state, created_at_unix_nano,
			updated_at_unix_nano
		) VALUES (?, ?, 1, ?, 1, 'acquired', ?, ?)`,
		scaleSetID,
		claimKey,
		oldMessageID,
		now,
		now,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO github_unpicked_requeue_intents(
			scale_set_id, runner_request_id, jit_attempt, old_execution_id,
			replacement_execution_id, source_message_id, source_event_index,
			controller_epoch, created_at_unix_nano, updated_at_unix_nano
		) VALUES (?, ?, 1, ?, ?, ?, 0, 1, ?, ?)`,
		scaleSetID,
		claimKey,
		oldExecutionID,
		replacementID,
		freshMessageID,
		now,
		now,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded := openControllerPath(t, path)
	defer upgraded.Close()
	assertCount(t, upgraded.db, "SELECT count(*) FROM github_message_jobs", 1)
	assertCount(
		t,
		upgraded.db,
		"SELECT count(*) FROM github_unpicked_requeue_intents",
		1,
	)
	if err := validateForeignKeys(ctx, upgraded.db); err != nil {
		t.Fatal(err)
	}
	zeroLifecycle := GitHubQueueMessage{
		ScaleSetID: scaleSetID,
		MessageID:  2103,
		Digest:     digestForTest("v10-zero-request-lifecycle"),
		Jobs: []GitHubJobEvent{{
			Type:            GitHubJobStarted,
			RunnerRequestID: 0,
			RunnerID:        91,
			RunnerName:      runnerName,
		}},
	}
	if _, err := upgraded.CommitGitHubQueueMessage(
		ctx,
		zeroLifecycle,
		SingleSlotBinding{
			TargetID: "target-v9",
			NodeID:   "node-v9",
			Slot:     0,
		},
	); err != nil {
		t.Fatalf("zero-request lifecycle commit = %v", err)
	}
	replayed, err := upgraded.CommitGitHubQueueMessage(
		ctx,
		zeroLifecycle,
		SingleSlotBinding{
			TargetID: "target-v9",
			NodeID:   "node-v9",
			Slot:     0,
		},
	)
	if err != nil || !replayed.Replayed {
		t.Fatalf("zero-request lifecycle replay = (%#v, %v)", replayed, err)
	}
	assertCount(
		t,
		upgraded.db,
		`SELECT count(*) FROM github_message_jobs
			WHERE runner_request_id = 0 AND event_type = 'JobStarted'`,
		1,
	)
	if _, err := upgraded.db.ExecContext(ctx, `INSERT INTO github_message_jobs(
			scale_set_id, message_id, event_index, event_type,
			runner_request_id, runner_id, runner_name, result,
			repository_name, owner_name, job_id, workflow_run_id
		) VALUES (?, ?, 1, 'JobAvailable', 0, 0, '', '', '', '', '', 0)`,
		scaleSetID,
		zeroLifecycle.MessageID,
	); err == nil {
		t.Fatal("v10 lifecycle schema accepted zero-request JobAvailable")
	}
	if _, err := upgraded.db.ExecContext(ctx, `INSERT INTO github_jit_attempts(
			scale_set_id, claim_key, attempt, controller_epoch,
			runner_name, state, runner_id, jit_digest, start_command_id,
			created_at_unix_nano, updated_at_unix_nano
		) VALUES (?, ?, 2, 1, 'duplicate-runner-name', 'generated', 91, ?,
			'duplicate-start', ?, ?)`,
		scaleSetID,
		claimKey,
		digestForTest("v10-duplicate-runner"),
		now,
		now,
	); err == nil {
		t.Fatal("v10 lifecycle schema accepted duplicate provider runner identity")
	}
}

func TestBrowserHandoffAuditMigrationPreservesVersionElevenRowsAndGuards(
	t *testing.T,
) {
	ctx := context.Background()
	db := openRawTestDatabase(t)
	defer db.Close()
	migrations, err := loadMigrations("controller", controllerMigrations)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) < 12 {
		t.Fatalf("controller migrations = %d, want at least 12", len(migrations))
	}
	if err := applyLoadedMigrations(
		ctx,
		db,
		"controller",
		migrations[:11],
		func() time.Time { return time.Unix(100, 0) },
		nil,
	); err != nil {
		t.Fatal(err)
	}
	const requestID = "req_0123456789abcdef0123456789abcdef"
	if _, err := db.ExecContext(ctx, `INSERT INTO management_audit_events(
			sequence, occurred_at_unix_nano, actor, action, outcome,
			resource_kind, resource_id, error_code, request_id, revision
		) VALUES (1, 1, 'single_admin', 'authentication_succeeded',
			'succeeded', 'controller', '', '', ?, 11)`, requestID); err != nil {
		t.Fatal(err)
	}

	if err := applyLoadedMigrations(
		ctx,
		db,
		"controller",
		migrations,
		func() time.Time { return time.Unix(101, 0) },
		nil,
	); err != nil {
		t.Fatal(err)
	}
	var action string
	var revision uint64
	if err := db.QueryRowContext(
		ctx,
		`SELECT action, revision FROM management_audit_events WHERE sequence = 1`,
	).Scan(&action, &revision); err != nil {
		t.Fatal(err)
	}
	if action != "authentication_succeeded" || revision != 11 {
		t.Fatalf("preserved audit = (%q, %d)", action, revision)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO management_audit_events(
			sequence, occurred_at_unix_nano, actor, action, outcome,
			resource_kind, resource_id, error_code, request_id, revision
		) VALUES (2, 2, 'single_admin', 'browser_handoff_authorized',
			'succeeded', 'controller', '', '', ?, 12)`, requestID); err != nil {
		t.Fatalf("new browser handoff audit rejected: %v", err)
	}
	if _, err := db.ExecContext(
		ctx,
		`UPDATE management_audit_events SET revision = 13 WHERE sequence = 1`,
	); err == nil {
		t.Fatal("upgraded audit row was mutable")
	}
	if _, err := db.ExecContext(
		ctx,
		`DELETE FROM management_audit_events WHERE sequence = 1`,
	); err == nil {
		t.Fatal("upgraded audit row was deletable")
	}
	for _, column := range tableColumns(t, db, "management_audit_events") {
		switch column {
		case "code", "claim_digest", "claim_secret", "session", "csrf":
			t.Fatalf("browser credential column migrated into audit table: %q", column)
		}
	}
}

func TestInjectedPendingMigrationPreservesExistingDataAndVersion(t *testing.T) {
	ctx := context.Background()
	db := openRawTestDatabase(t)
	defer db.Close()
	base := `
		CREATE TABLE store_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO store_metadata(key, value) VALUES ('role', 'controller'), ('controller_epoch', '0');
		CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at_unix_nano INTEGER NOT NULL);
		CREATE TABLE preserved(value TEXT NOT NULL);
		INSERT INTO preserved(value) VALUES ('unchanged');`
	initialMigrations := fstestMapFS(map[string]string{
		"migrations/controller/001_base.sql": base,
	})
	if err := applyMigrations(ctx, db, "controller", initialMigrations, time.Now, nil); err != nil {
		t.Fatal(err)
	}
	migrations := fstestMapFS(map[string]string{
		"migrations/controller/001_base.sql":    base,
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

func TestUnownedDatabaseIsRejectedWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir := privateTestDir(t)
	path := filepath.Join(dir, "unrelated.db")
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
	if _, err := db.Exec(`CREATE TABLE unrelated(value TEXT NOT NULL); INSERT INTO unrelated(value) VALUES ('preserve-me')`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	degraded, err := OpenController(ctx, path, Options{})
	if degraded == nil || !errors.Is(err, ErrRecoveryMode) || !errors.Is(err, ErrUnownedDatabase) {
		t.Fatalf("unowned database open = (%v, %v)", degraded, err)
	}
	if readyErr := degraded.Ready(); !errors.Is(readyErr, ErrRecoveryMode) {
		t.Fatalf("unowned database readiness = %v", readyErr)
	}
	if err := degraded.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("rejected open changed unrelated database bytes")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rejected open left SQLite sidecar %q: %v", suffix, err)
		}
	}
	var value string
	if err := queryRaw(t, path, "SELECT value FROM unrelated").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "preserve-me" {
		t.Fatalf("unrelated data = %q", value)
	}
	assertRawCount(t, path, "SELECT count(*) FROM sqlite_schema WHERE name IN ('store_metadata', 'schema_migrations')", 0)
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
	if replay, err := store.LookupCommand(ctx, command); err != nil || replay {
		t.Fatalf("missing command lookup = (%t, %v)", replay, err)
	}
	if replay, err := store.RecordCommand(ctx, command); err != nil || replay {
		t.Fatalf("initial command = (%t, %v)", replay, err)
	}
	if replay, err := store.LookupCommand(ctx, command); err != nil || !replay {
		t.Fatalf("exact command lookup = (%t, %v)", replay, err)
	}
	if replay, err := store.RecordCommand(ctx, command); err != nil || !replay {
		t.Fatalf("exact replay = (%t, %v)", replay, err)
	}
	command.PayloadDigest = domain.PayloadDigest([]byte("changed"))
	if _, err := store.LookupCommand(ctx, command); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed command lookup = %v", err)
	}
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
	outOfRange := command
	outOfRange.ID = "out-of-range-epoch"
	outOfRange.ControllerEpoch = domain.ControllerEpoch(maxSQLiteInteger + 1)
	outOfRange.PayloadDigest = domain.PayloadDigest([]byte("out-of-range-epoch"))
	if _, err := reopened.RecordCommand(ctx, outOfRange); err == nil {
		t.Fatal("out-of-range controller epoch accepted")
	}
	final, err := reopened.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if final.MaxControllerEpoch != 4 || len(final.Commands) != 3 {
		t.Fatalf("final agent snapshot = %+v", final)
	}
}

func TestAgentObservationOrderingSurvivesWallClockRegressionAndRejectsStateRegression(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(300, 0)
	store, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent-monotonic.db"), Options{
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	executionID := domain.ExecutionID("execution-monotonic")
	if err := store.RecordObservation(ctx, Observation{
		ExecutionID: executionID,
		State:       domain.ExecutionPreparing,
	}); err != nil {
		t.Fatal(err)
	}
	first, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}

	now = time.Unix(100, 0)
	if err := store.RecordObservation(ctx, Observation{
		ExecutionID: executionID,
		State:       domain.ExecutionRunning,
	}); err != nil {
		t.Fatalf("forward observation after wall-clock regression = %v", err)
	}
	second, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Observations) != 1 || len(second.Observations) != 1 ||
		second.Observations[0].ObservedAtUnixNano <= first.Observations[0].ObservedAtUnixNano {
		t.Fatalf("observation ordering regressed: first=%+v second=%+v", first.Observations, second.Observations)
	}
	if err := store.RecordObservation(ctx, Observation{
		ExecutionID: executionID,
		State:       domain.ExecutionPreparing,
	}); err == nil {
		t.Fatal("Agent journal accepted a lifecycle state regression")
	}

	tombstoneID := domain.ExecutionID("cleanup-monotonic")
	if err := store.RecordCleanupTombstone(ctx, CleanupTombstone{
		ExecutionID: tombstoneID,
		FailureCode: CleanupProcessResidue,
	}); err != nil {
		t.Fatal(err)
	}
	beforeReplay, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now = time.Unix(50, 0)
	if err := store.RecordCleanupTombstone(ctx, CleanupTombstone{
		ExecutionID: tombstoneID,
		FailureCode: CleanupProcessResidue,
	}); err != nil {
		t.Fatalf("exact tombstone replay after wall-clock regression = %v", err)
	}
	afterReplay, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeReplay.CleanupTombstones) != 1 || len(afterReplay.CleanupTombstones) != 1 ||
		afterReplay.CleanupTombstones[0].RecordedAtUnixNano <= beforeReplay.CleanupTombstones[0].RecordedAtUnixNano {
		t.Fatalf("tombstone ordering regressed: before=%+v after=%+v",
			beforeReplay.CleanupTombstones, afterReplay.CleanupTombstones)
	}
	if err := store.RecordCleanupTombstone(ctx, CleanupTombstone{
		ExecutionID: tombstoneID,
		FailureCode: CleanupWorkspaceRemoval,
	}); err == nil {
		t.Fatal("cleanup tombstone classification changed after persistence")
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
			name:      "epoch outside signed range",
			statement: `UPDATE store_metadata SET value = '9223372036854775808' WHERE key = 'controller_epoch'`,
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

func TestControllerAgentRuntimeColumnAllowlistRejectsSecretCanaryColumns(t *testing.T) {
	tables := []string{
		"node_administrative_states",
		"agent_commands",
		"agent_session_snapshots",
		"agent_snapshot_commands",
		"agent_snapshot_observations",
		"agent_snapshot_cleanup_tombstones",
		"agent_execution_updates",
		"agent_snapshot_authority",
		"agent_current_snapshot_commands",
		"agent_current_snapshot_observations",
		"agent_current_snapshot_tombstones",
		"reconciliation_agent_commands",
		"github_session_demand",
		"github_queue_messages",
		"github_message_jobs",
		"github_job_claims",
		"github_jit_attempts",
		"github_acquire_attempts",
		"github_jit_snapshot_authority",
		"github_unpicked_requeue_intents",
		"runner_profile_update_policies",
		"github_target_runtime_bindings",
		"github_runner_release_state",
		"github_scale_set_session_health",
		"management_configuration_state",
		"management_node_configurations",
		"management_runner_profiles",
		"management_github_targets",
		"management_audit_events",
	}
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			path := filepath.Join(privateTestDir(t), "controller.db")
			controller := openControllerPath(t, path)
			if err := controller.Close(); err != nil {
				t.Fatal(err)
			}
			execRaw(t, path, fmt.Sprintf(
				`ALTER TABLE %s ADD COLUMN jit_config TEXT NOT NULL DEFAULT 'jit-secret-canary.example.test'`,
				table,
			))
			dsn, err := sqliteDSN(path, true)
			if err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open(sqliteDriver, dsn)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateColumnAllowlist(context.Background(), db, "controller"); !errors.Is(err, ErrCorruptBackup) {
				_ = db.Close()
				t.Fatalf("secret column allowlist error = %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			degraded, err := OpenController(context.Background(), path, Options{})
			if degraded == nil || !errors.Is(err, ErrRecoveryMode) {
				t.Fatalf("secret-column database open = (%v, %v)", degraded, err)
			}
			_ = degraded.Close()
		})
	}
}

func TestPageCorruptionEntersRecoveryBeforeMigration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller.db")
	store := openControllerPath(t, path)
	if _, _, err := store.Assign(ctx, testAssignment(81, "corrupt-page-execution", "corrupt-page-node", 0)); err != nil {
		t.Fatal(err)
	}
	var pageSize, rootPage int64
	if err := store.db.QueryRow("PRAGMA page_size").Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT rootpage FROM sqlite_schema WHERE type='table' AND name='processed_messages'`).Scan(&rootPage); err != nil {
		t.Fatal(err)
	}
	if pageSize <= 0 || rootPage <= 1 {
		t.Fatalf("invalid corruption target page_size=%d rootpage=%d", pageSize, rootPage)
	}
	if _, err := store.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, (rootPage-1)*pageSize); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	degraded, err := OpenController(ctx, path, Options{})
	if degraded == nil || !errors.Is(err, ErrRecoveryMode) || !errors.Is(err, ErrCorruptBackup) {
		t.Fatalf("page-corrupt database open = (%v, %v)", degraded, err)
	}
	if readyErr := degraded.Ready(); !errors.Is(readyErr, ErrRecoveryMode) {
		t.Fatalf("page-corrupt readiness = %v", readyErr)
	}
	_ = degraded.Close()
}

func TestSnapshotsAndWritesRejectInvalidPersistedRanges(t *testing.T) {
	ctx := context.Background()
	t.Run("controller reservation", func(t *testing.T) {
		store := openController(t, "reservation.db")
		defer store.Close()
		if _, err := store.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec(`INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES ('execution-invalid', 'target-1', '', 0, 'reserved', 1)`); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec(`INSERT INTO slot_reservations(node_id, slot_index, target_id, execution_id) VALUES ('', 0, 'target-1', 'execution-invalid')`); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Snapshot(ctx); err == nil {
			t.Fatal("invalid reservation appeared in typed snapshot")
		}
	})
	t.Run("controller timestamp", func(t *testing.T) {
		store := openController(t, "controller-time.db")
		defer store.Close()
		assignment := testAssignment(82, "invalid-time-execution", "invalid-time-node", 0)
		if _, _, err := store.Assign(ctx, assignment); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec(`UPDATE executions SET created_at_unix_nano=0 WHERE id=?`, assignment.Execution.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Snapshot(ctx); err == nil {
			t.Fatal("invalid execution timestamp appeared in typed snapshot")
		}
	})
	t.Run("agent timestamp", func(t *testing.T) {
		store, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent-time.db"), Options{Now: func() time.Time { return time.Unix(100, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if err := store.RecordObservation(ctx, Observation{ExecutionID: "execution-1", State: domain.ExecutionRunning}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec(`UPDATE execution_observations SET observed_at_unix_nano=0 WHERE execution_id='execution-1'`); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Snapshot(ctx); err == nil {
			t.Fatal("invalid observation timestamp appeared in typed snapshot")
		}
	})
	t.Run("write clock", func(t *testing.T) {
		path := filepath.Join(privateTestDir(t), "invalid-clock.db")
		initial := openControllerPath(t, path)
		if err := initial.Close(); err != nil {
			t.Fatal(err)
		}
		store, err := OpenController(ctx, path, Options{Now: func() time.Time { return time.Unix(-1, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if _, _, err := store.Assign(ctx, testAssignment(83, "invalid-clock-execution", "invalid-clock-node", 0)); err == nil {
			t.Fatal("out-of-range store clock accepted")
		}
		assertCount(t, store.db, "SELECT count(*) FROM executions", 0)
	})
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

func runAndKillStoreHelper(t *testing.T, testName, environment, path string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+testName+"$")
	command.Env = append(os.Environ(), environment+"="+path)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		waitErr := command.Wait()
		t.Fatalf("crash helper did not become ready: scan=%v wait=%v stderr=%s", scanner.Err(), waitErr, stderr.String())
	}
	if line := scanner.Text(); line != "crash-ready" {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("unexpected crash helper signal %q; stderr=%s", line, stderr.String())
	}
	if err := command.Process.Kill(); err != nil {
		_ = command.Wait()
		t.Fatalf("kill crash helper: %v; stderr=%s", err, stderr.String())
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed crash helper exited successfully")
	}
	if ctx.Err() != nil {
		t.Fatalf("crash helper timed out: %v; stderr=%s", ctx.Err(), stderr.String())
	}
}

func crashHelperExit(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(97)
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
	if err := rows.Err(); err != nil {
		t.Fatal(err)
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

func assertRawCount(t *testing.T, path, query string, want int) {
	t.Helper()
	var got int
	if err := queryRaw(t, path, query).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("count = %d, want %d", got, want)
	}
}

func assertNoTemporaryStoreFiles(t *testing.T, directory string) {
	t.Helper()
	for _, pattern := range []string{".sparerunner-backup-*", ".sparerunner-restore-*"} {
		matches, err := filepath.Glob(filepath.Join(directory, pattern))
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) != 0 {
			t.Fatalf("temporary store files remain: %v", matches)
		}
	}
}
