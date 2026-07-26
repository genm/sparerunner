package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
)

func TestControllerOpenEpochAndAtomicAssignmentReplay(t *testing.T) {
	ctx := context.Background()
	store := openController(t, "controller.db")
	defer store.Close()
	if first, err := store.AdvanceEpoch(ctx); err != nil || first != 1 {
		t.Fatalf("first epoch = %d, %v", first, err)
	}
	if second, err := store.AdvanceEpoch(ctx); err != nil || second != 2 {
		t.Fatalf("second epoch = %d, %v", second, err)
	}
	var journalMode string
	if err := store.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil || journalMode != "wal" {
		t.Fatalf("journal mode = %q, %v; want WAL", journalMode, err)
	}
	var foreignKeys int
	if err := store.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("foreign keys = %d, %v; want enabled", foreignKeys, err)
	}
	assignment := testAssignment("message-1", "digest-1", "execution-1", "node-1", 0)
	got, replay, err := store.Assign(ctx, assignment)
	if err != nil || replay || got != assignment.Execution {
		t.Fatalf("first assignment = (%+v, %t, %v)", got, replay, err)
	}
	got, replay, err = store.Assign(ctx, assignment)
	if err != nil || !replay || got != assignment.Execution {
		t.Fatalf("exact replay = (%+v, %t, %v)", got, replay, err)
	}
	assignment.MessageDigest = "different-digest"
	_, _, err = store.Assign(ctx, assignment)
	if !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("different message digest error = %v, want ErrReplayMismatch", err)
	}
	assertCount(t, store.db, "SELECT count(*) FROM processed_messages", 1)
	assertCount(t, store.db, "SELECT count(*) FROM slot_reservations", 1)
	assertCount(t, store.db, "SELECT count(*) FROM executions", 1)
}

func TestAssignmentFailureRollsBackReservation(t *testing.T) {
	ctx := context.Background()
	store := openController(t, "controller.db")
	defer store.Close()
	if _, _, err := store.Assign(ctx, testAssignment("message-1", "digest-1", "execution-1", "node-1", 0)); err != nil {
		t.Fatal(err)
	}
	// The repeated execution ID causes the execution insert to fail after the
	// reservation insert; the second physical slot must remain unclaimed.
	_, _, err := store.Assign(ctx, testAssignment("message-2", "digest-2", "execution-1", "node-1", 1))
	if !errors.Is(err, ErrActiveExecution) {
		t.Fatalf("failed assignment = %v, want ErrActiveExecution", err)
	}
	assertCount(t, store.db, "SELECT count(*) FROM slot_reservations", 1)
	var secondSlot int
	if err := store.db.QueryRow("SELECT count(*) FROM slot_reservations WHERE node_id='node-1' AND slot_index=1").Scan(&secondSlot); err != nil {
		t.Fatal(err)
	}
	if secondSlot != 0 {
		t.Fatalf("rolled-back slot reservation count = %d, want 0", secondSlot)
	}
}

func TestConcurrentSameSlotAllowsExactlyOneAssignment(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "controller.db")
	first := openControllerPath(t, path)
	second := openControllerPath(t, path)
	defer first.Close()
	defer second.Close()
	stores := []*ControllerStore{first, second}
	var successes int
	var wg sync.WaitGroup
	var lock sync.Mutex
	for index, candidate := range stores {
		wg.Add(1)
		go func(index int, candidate *ControllerStore) {
			defer wg.Done()
			_, _, err := candidate.Assign(ctx, testAssignment("message-"+string(rune('a'+index)), "digest", "execution-"+string(rune('a'+index)), "node-1", 0))
			if err == nil {
				lock.Lock()
				successes++
				lock.Unlock()
				return
			}
			if !errors.Is(err, ErrSlotAlreadyReserved) && !errors.Is(err, ErrActiveExecution) {
				t.Errorf("assignment %d error = %v", index, err)
			}
		}(index, candidate)
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("successful concurrent assignments = %d, want 1", successes)
	}
	assertCount(t, first.db, "SELECT count(*) FROM slot_reservations", 1)
}

func TestMigrationFailureRollsBackAndBlocksMutations(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "controller.db")
	failed, err := OpenController(ctx, path, Options{MigrationHook: func(role string, version int) error {
		return errors.New("injected migration interruption")
	}})
	if err == nil || failed == nil {
		t.Fatalf("migration failure = (%v, %v), want nonnil store and error", failed, err)
	}
	defer failed.Close()
	if err := failed.Ready(); !errors.Is(err, ErrRecoveryMode) {
		t.Fatalf("ready after migration failure = %v", err)
	}
	if _, err := failed.AdvanceEpoch(ctx); !errors.Is(err, ErrRecoveryMode) {
		t.Fatalf("mutation in recovery = %v", err)
	}
	assertCount(t, failed.db, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='store_metadata'", 0)
	if err := failed.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := openControllerPath(t, path)
	defer recovered.Close()
	if _, err := recovered.AdvanceEpoch(ctx); err != nil {
		t.Fatalf("open after rolled back migration: %v", err)
	}
}

func TestAgentJournalRejectsChangedCommandAndPersistsObservations(t *testing.T) {
	ctx := context.Background()
	store, err := OpenAgent(ctx, filepath.Join(t.TempDir(), "agent.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	command := domain.Command{ID: "command-1", ControllerEpoch: 1, ExecutionID: "execution-1", ExpectedState: domain.ExecutionReserved, PayloadDigest: domain.PayloadDigest([]byte("only-digest-is-stored"))}
	if replay, err := store.RecordCommand(ctx, command); err != nil || replay {
		t.Fatalf("initial command = (%t, %v)", replay, err)
	}
	if replay, err := store.RecordCommand(ctx, command); err != nil || !replay {
		t.Fatalf("exact command replay = (%t, %v)", replay, err)
	}
	command.PayloadDigest = domain.PayloadDigest([]byte("changed"))
	if _, err := store.RecordCommand(ctx, command); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed command payload = %v", err)
	}
	if err := store.RecordObservation(ctx, Observation{ExecutionID: "execution-1", State: domain.ExecutionRunning}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCleanupTombstone(ctx, CleanupTombstone{ExecutionID: "execution-1", Reason: "cleanup_failed"}); err != nil {
		t.Fatal(err)
	}
	assertCount(t, store.db, "SELECT count(*) FROM execution_observations", 1)
	assertCount(t, store.db, "SELECT count(*) FROM cleanup_tombstones", 1)
}

func TestBackupRestoreAndSecretCanaryExclusion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	controller := openControllerPath(t, filepath.Join(dir, "controller.db"))
	defer controller.Close()
	if _, _, err := controller.Assign(ctx, testAssignment("message-1", "digest-1", "execution-1", "node-1", 0)); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(dir, "controller.backup.db")
	if err := controller.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	canary := "jit-plaintext-join-secret-node-private-key.example.test"
	for _, path := range []string{filepath.Join(dir, "controller.db"), backup} {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(contents), canary) {
			t.Fatalf("secret canary found in %s", path)
		}
	}
	restored := filepath.Join(dir, "restored.db")
	if err := RestoreController(ctx, restored, backup); err != nil {
		t.Fatal(err)
	}
	opened := openControllerPath(t, restored)
	defer opened.Close()
	assertCount(t, opened.db, "SELECT count(*) FROM executions", 1)
}

func TestBadOrWrongRoleRestorePreservesDestination(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	destination := filepath.Join(dir, "destination.db")
	dest := openControllerPath(t, destination)
	if _, err := dest.AdvanceEpoch(ctx); err != nil {
		t.Fatal(err)
	}
	if err := dest.Close(); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(dir, "bad.backup")
	if err := os.WriteFile(bad, []byte("not a sqlite backup"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RestoreController(ctx, destination, bad); err == nil {
		t.Fatal("corrupt restore unexpectedly succeeded")
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
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	if err := RestoreController(ctx, destination, agentBackup); !errors.Is(err, ErrWrongRole) {
		t.Fatalf("wrong-role restore error = %v", err)
	}
	assertEpoch(t, destination, 3)
}

func openController(t *testing.T, name string) *ControllerStore {
	t.Helper()
	return openControllerPath(t, filepath.Join(t.TempDir(), name))
}

func openControllerPath(t *testing.T, path string) *ControllerStore {
	t.Helper()
	store, err := OpenController(context.Background(), path, Options{Now: func() time.Time { return time.Unix(100, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testAssignment(messageID, digest, executionID, nodeID string, slot int) Assignment {
	return Assignment{ScaleSetID: "scale-set-1", MessageID: messageID, MessageDigest: digest, Execution: domain.ExecutionSnapshot{ID: domain.ExecutionID(executionID), TargetID: "target-1", Slot: domain.SlotKey{NodeID: domain.NodeID(nodeID), Index: slot}, State: domain.ExecutionPending}}
}

func assertCount(t *testing.T, db *sql.DB, query string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("count query %q = %d, want %d", query, got, want)
	}
}

func assertEpoch(t *testing.T, path string, want domain.ControllerEpoch) {
	t.Helper()
	store := openControllerPath(t, path)
	defer store.Close()
	got, err := store.AdvanceEpoch(context.Background())
	if err != nil || got != want {
		t.Fatalf("epoch after failed restore = %d, %v; want %d", got, err, want)
	}
}
