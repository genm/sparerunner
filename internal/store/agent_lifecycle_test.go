package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/domain"
)

func TestExecutionLifecycleCommitIsAtomicMonotonicAndExactlyReplayable(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(300, 0)
	agent, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent-lifecycle.db"), Options{
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()

	initial := testExecutionLifecycleCommit(
		"a",
		"execution-lifecycle",
		domain.ExecutionCleanupFailed,
		domain.ExecutionErrorCleanup,
		CleanupProcessResidue,
	)
	pending, created, err := agent.CommitExecutionLifecycle(ctx, initial)
	if err != nil || !created {
		t.Fatalf("initial lifecycle commit = (%#v, %t, %v)", pending, created, err)
	}
	snapshot, err := agent.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Observations) != 1 ||
		snapshot.Observations[0].State != domain.ExecutionCleanupFailed ||
		len(snapshot.CleanupTombstones) != 1 ||
		snapshot.CleanupTombstones[0].FailureCode != CleanupProcessResidue {
		t.Fatalf("initial lifecycle snapshot = %+v", snapshot)
	}
	firstObservationTime := snapshot.Observations[0].ObservedAtUnixNano
	firstTombstoneTime := snapshot.CleanupTombstones[0].RecordedAtUnixNano

	now = time.Unix(100, 0)
	replayed, created, err := agent.CommitExecutionLifecycle(ctx, initial)
	if err != nil || created || replayed != pending {
		t.Fatalf("exact lifecycle replay = (%#v, %t, %v)", replayed, created, err)
	}
	snapshot, err = agent.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Observations[0].ObservedAtUnixNano != firstObservationTime ||
		snapshot.CleanupTombstones[0].RecordedAtUnixNano != firstTombstoneTime {
		t.Fatalf("exact replay rewrote lifecycle timestamps: %+v", snapshot)
	}

	quarantined := testExecutionLifecycleCommit(
		"b",
		"execution-lifecycle",
		domain.ExecutionQuarantined,
		domain.ExecutionErrorQuarantined,
		CleanupProcessResidue,
	)
	if _, created, err := agent.CommitExecutionLifecycle(ctx, quarantined); err != nil || !created {
		t.Fatalf("quarantine lifecycle commit = (%t, %v)", created, err)
	}
	advanced, err := agent.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if advanced.Observations[0].State != domain.ExecutionQuarantined ||
		advanced.Observations[0].ObservedAtUnixNano <= firstObservationTime ||
		advanced.CleanupTombstones[0].RecordedAtUnixNano <= firstTombstoneTime {
		t.Fatalf("clock rollback regressed lifecycle ordering: %+v", advanced)
	}

	// An old exact transport replay must remain exact without regressing the
	// newer local reconciliation observation.
	if got, created, err := agent.CommitExecutionLifecycle(ctx, initial); err != nil || created || got != pending {
		t.Fatalf("advanced exact replay = (%#v, %t, %v)", got, created, err)
	}
	afterOldReplay, err := agent.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterOldReplay, advanced) {
		t.Fatalf("old exact replay changed advanced evidence\nbefore=%+v\nafter=%+v", advanced, afterOldReplay)
	}

	collision := initial
	collision.Update.Replayed = true
	if _, _, err := agent.CommitExecutionLifecycle(ctx, collision); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("message identity collision = %v", err)
	}
	changedTombstone := initial
	changedTombstone.CleanupTombstone = &CleanupTombstone{
		ExecutionID: initial.Update.ExecutionID,
		FailureCode: CleanupWorkspaceRemoval,
	}
	if _, _, err := agent.CommitExecutionLifecycle(ctx, changedTombstone); err == nil ||
		!strings.Contains(err.Error(), "classification is immutable") {
		t.Fatalf("mutable cleanup classification = %v", err)
	}
}

func TestExecutionLifecycleCommitRejectsInvalidCompositeWithoutPersistence(t *testing.T) {
	ctx := context.Background()
	agent, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent-lifecycle-invalid.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()

	valid := testExecutionLifecycleCommit(
		"c",
		"execution-invalid",
		domain.ExecutionRunning,
		domain.ExecutionErrorNone,
		"",
	)
	tests := []struct {
		name   string
		mutate func(*ExecutionLifecycleCommit)
	}{
		{
			name: "message ID",
			mutate: func(commit *ExecutionLifecycleCommit) {
				commit.MessageID = "jit-secret.example.test"
			},
		},
		{
			name: "execution identity",
			mutate: func(commit *ExecutionLifecycleCommit) {
				commit.Observation.ExecutionID = "different-execution"
			},
		},
		{
			name: "state identity",
			mutate: func(commit *ExecutionLifecycleCommit) {
				commit.Observation.State = domain.ExecutionPreparing
			},
		},
		{
			name: "missing cleanup tombstone",
			mutate: func(commit *ExecutionLifecycleCommit) {
				commit.Update.State = domain.ExecutionCleanupFailed
				commit.Update.ErrorCode = domain.ExecutionErrorCleanup
				commit.Observation.State = domain.ExecutionCleanupFailed
			},
		},
		{
			name: "unexpected cleanup tombstone",
			mutate: func(commit *ExecutionLifecycleCommit) {
				commit.CleanupTombstone = &CleanupTombstone{
					ExecutionID: commit.Update.ExecutionID,
					FailureCode: CleanupVerificationFailed,
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			if _, _, err := agent.CommitExecutionLifecycle(ctx, candidate); err == nil {
				t.Fatal("invalid lifecycle commit succeeded")
			}
		})
	}
	assertCount(t, agent.db, "SELECT count(*) FROM execution_observations", 0)
	assertCount(t, agent.db, "SELECT count(*) FROM cleanup_tombstones", 0)
	assertCount(t, agent.db, "SELECT count(*) FROM execution_update_outbox", 0)
}

func TestExecutionLifecycleTransactionFaultRollsBackAllEvidence(t *testing.T) {
	ctx := context.Background()
	agent, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent-lifecycle-fault.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	if _, err := agent.db.Exec(`CREATE TEMP TRIGGER fail_lifecycle_outbox
		BEFORE INSERT ON execution_update_outbox
		BEGIN
			SELECT RAISE(ABORT, 'injected lifecycle outbox fault');
		END`); err != nil {
		t.Fatal(err)
	}

	commit := testExecutionLifecycleCommit(
		"d",
		"execution-fault",
		domain.ExecutionCleanupFailed,
		domain.ExecutionErrorCleanup,
		CleanupVerificationFailed,
	)
	if _, _, err := agent.CommitExecutionLifecycle(ctx, commit); err == nil ||
		!strings.Contains(err.Error(), "injected lifecycle outbox fault") {
		t.Fatalf("injected transaction fault = %v", err)
	}
	assertCount(t, agent.db, "SELECT count(*) FROM execution_observations", 0)
	assertCount(t, agent.db, "SELECT count(*) FROM cleanup_tombstones", 0)
	assertCount(t, agent.db, "SELECT count(*) FROM execution_update_outbox", 0)
}

func TestExecutionLifecycleCommitAmbiguityRequiresCompleteDurableEvidence(t *testing.T) {
	t.Run("committed tuple is recovered as exact replay", func(t *testing.T) {
		ctx := context.Background()
		agent, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent-lifecycle-committed.db"), Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer agent.Close()
		commit := testExecutionLifecycleCommit(
			"e",
			"execution-committed",
			domain.ExecutionCleanupFailed,
			domain.ExecutionErrorCleanup,
			CleanupWorkspaceRemoval,
		)
		pending, created, err := agent.commitExecutionLifecycle(ctx, commit, func(tx *sql.Tx) error {
			if err := tx.Commit(); err != nil {
				return err
			}
			return errors.New("injected lost commit response")
		})
		if err != nil || created || pending.Update != commit.Update {
			t.Fatalf("durably committed ambiguous result = (%#v, %t, %v)", pending, created, err)
		}
		assertCount(t, agent.db, "SELECT count(*) FROM execution_observations", 1)
		assertCount(t, agent.db, "SELECT count(*) FROM cleanup_tombstones", 1)
		assertCount(t, agent.db, "SELECT count(*) FROM execution_update_outbox", 1)
	})

	t.Run("rolled back tuple fails closed", func(t *testing.T) {
		ctx := context.Background()
		agent, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent-lifecycle-rolled-back.db"), Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer agent.Close()
		commit := testExecutionLifecycleCommit(
			"f",
			"execution-rolled-back",
			domain.ExecutionRunning,
			domain.ExecutionErrorNone,
			"",
		)
		_, created, err := agent.commitExecutionLifecycle(ctx, commit, func(tx *sql.Tx) error {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return rollbackErr
			}
			return errors.New("injected lost rollback response")
		})
		if !errors.Is(err, ErrLifecycleCommitAmbiguous) || created {
			t.Fatalf("rolled-back ambiguous result = (%t, %v)", created, err)
		}
		assertCount(t, agent.db, "SELECT count(*) FROM execution_observations", 0)
		assertCount(t, agent.db, "SELECT count(*) FROM cleanup_tombstones", 0)
		assertCount(t, agent.db, "SELECT count(*) FROM execution_update_outbox", 0)
	})
}

func testExecutionLifecycleCommit(
	messageDigit string,
	executionID domain.ExecutionID,
	state domain.ExecutionState,
	errorCode domain.ExecutionErrorCode,
	cleanupCode CleanupFailureCode,
) ExecutionLifecycleCommit {
	commit := ExecutionLifecycleCommit{
		Observation: Observation{
			ExecutionID: executionID,
			State:       state,
		},
		MessageID: strings.Repeat(messageDigit, 64),
		Update: ExecutionUpdateRecord{
			NodeID:      "node-lifecycle",
			CommandID:   domain.CommandID("command-" + string(executionID)),
			ExecutionID: executionID,
			State:       state,
			ErrorCode:   errorCode,
		},
	}
	if cleanupCode != "" {
		commit.CleanupTombstone = &CleanupTombstone{
			ExecutionID: executionID,
			FailureCode: cleanupCode,
		}
	}
	return commit
}
