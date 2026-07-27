package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/genm/tewake/internal/domain"
)

var ErrLifecycleCommitAmbiguous = errors.New("agent lifecycle commit outcome is ambiguous")

// ExecutionLifecycleCommit is the complete, non-secret Agent evidence published
// for one execution update. The observation, optional cleanup tombstone, and
// transport outbox row are committed together so a restart cannot expose a
// Controller-visible update without its local reconciliation evidence.
type ExecutionLifecycleCommit struct {
	Observation      Observation
	CleanupTombstone *CleanupTombstone
	MessageID        string
	Update           ExecutionUpdateRecord
	// Target attributes this execution to the GitHub scope that produced it. It
	// travels in the same transaction as the observation so a crash can never
	// leave a locally visible execution whose owner-facing scope is unknown
	// while its observation already exists. It is set only by the commands that
	// carry target identity; a nil value leaves any existing attribution intact.
	Target *ExecutionTarget
}

func (commit ExecutionLifecycleCommit) validate() error {
	if err := validateObservation(commit.Observation); err != nil {
		return err
	}
	if !isLowerSHA256(commit.MessageID) {
		return errors.New("execution lifecycle message ID is invalid")
	}
	if err := commit.Update.Validate(); err != nil {
		return err
	}
	if commit.Update.ExecutionID != commit.Observation.ExecutionID {
		return errors.New("execution lifecycle identities do not match")
	}
	if commit.Update.State != commit.Observation.State {
		return errors.New("execution lifecycle states do not match")
	}

	requiresTombstone := commit.Update.State == domain.ExecutionCleanupFailed ||
		commit.Update.State == domain.ExecutionQuarantined
	if requiresTombstone != (commit.CleanupTombstone != nil) {
		return errors.New("cleanup failure lifecycle requires exactly one cleanup tombstone")
	}
	if commit.CleanupTombstone != nil {
		if err := validateCleanupTombstone(*commit.CleanupTombstone); err != nil {
			return err
		}
		if commit.CleanupTombstone.ExecutionID != commit.Update.ExecutionID {
			return errors.New("cleanup tombstone execution identity does not match lifecycle update")
		}
	}
	if commit.Target != nil {
		if err := commit.Target.validate(); err != nil {
			return err
		}
		if commit.Target.ExecutionID != commit.Update.ExecutionID {
			return errors.New("execution target identity does not match lifecycle update")
		}
	}
	return nil
}

// CommitExecutionLifecycle atomically persists the Agent observation, optional
// cleanup tombstone, and pending execution update. An exact MessageID replay is
// idempotent. If SQLite reports a commit error, the method returns success only
// when every required durable fact can be re-read with the exact update identity;
// otherwise callers receive ErrLifecycleCommitAmbiguous and must reconcile before
// publishing or admitting more work.
func (s *AgentStore) CommitExecutionLifecycle(
	ctx context.Context,
	commit ExecutionLifecycleCommit,
) (PendingExecutionUpdate, bool, error) {
	return s.commitExecutionLifecycle(ctx, commit, func(tx *sql.Tx) error {
		return tx.Commit()
	})
}

type lifecycleCommitter func(*sql.Tx) error

func (s *AgentStore) commitExecutionLifecycle(
	ctx context.Context,
	commit ExecutionLifecycleCommit,
	commitTx lifecycleCommitter,
) (PendingExecutionUpdate, bool, error) {
	if err := s.requireReady(); err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	if err := commit.validate(); err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	if commitTx == nil {
		return PendingExecutionUpdate{}, false, errors.New("execution lifecycle committer is required")
	}
	nowUnixNano, err := storeUnixNano(s.now())
	if err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	defer tx.Rollback()

	pending, replayed, err := applyExecutionLifecycle(ctx, tx, commit, nowUnixNano)
	if err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	if err := commitTx(tx); err != nil {
		observed, exact, observeErr := s.observeExecutionLifecycleCommit(ctx, commit)
		if observeErr == nil && exact {
			// The full durable tuple proves that the operation reached SQLite.
			// Report it as a replay because the caller cannot safely distinguish
			// which attempt created the row after an ambiguous Commit result.
			return observed, false, nil
		}
		if observeErr != nil {
			return PendingExecutionUpdate{}, false, fmt.Errorf(
				"%w: commit: %v; observe: %v",
				ErrLifecycleCommitAmbiguous,
				err,
				observeErr,
			)
		}
		return PendingExecutionUpdate{}, false, fmt.Errorf("%w: %v", ErrLifecycleCommitAmbiguous, err)
	}
	return pending, !replayed, nil
}

func applyExecutionLifecycle(
	ctx context.Context,
	tx *sql.Tx,
	commit ExecutionLifecycleCommit,
	nowUnixNano int64,
) (PendingExecutionUpdate, bool, error) {
	pending, found, err := loadPendingExecutionUpdate(ctx, tx, commit.MessageID)
	if err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	if found && pending.Update != commit.Update {
		return PendingExecutionUpdate{}, false, fmt.Errorf(
			"%w: execution update message identity",
			ErrReplayMismatch,
		)
	}

	// An exact outbox replay may arrive after a later lifecycle update. Preserve
	// that newer Agent observation instead of regressing it to the replayed state.
	if _, err := recordObservationTx(
		ctx,
		tx,
		commit.Observation,
		nowUnixNano,
		!found,
		found,
	); err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	if commit.Target != nil {
		// Attribution is immutable for the life of one execution: the first
		// command that carried it wins, so a later replay cannot silently
		// re-attribute work already shown to the owner.
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO execution_targets (execution_id, target_id, scope, scope_kind)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(execution_id) DO NOTHING`,
			string(commit.Target.ExecutionID),
			string(commit.Target.TargetID),
			commit.Target.Scope,
			string(commit.Target.ScopeKind),
		); err != nil {
			return PendingExecutionUpdate{}, false, err
		}
	}
	if commit.CleanupTombstone != nil {
		if _, err := recordCleanupTombstoneTx(
			ctx,
			tx,
			*commit.CleanupTombstone,
			nowUnixNano,
			!found,
		); err != nil {
			return PendingExecutionUpdate{}, false, err
		}
	}
	if found {
		return pending, true, nil
	}
	pending, err = insertExecutionUpdateTx(ctx, tx, commit.MessageID, commit.Update)
	if err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	return pending, false, nil
}

func (s *AgentStore) observeExecutionLifecycleCommit(
	ctx context.Context,
	commit ExecutionLifecycleCommit,
) (PendingExecutionUpdate, bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	defer tx.Rollback()

	pending, found, err := loadPendingExecutionUpdate(ctx, tx, commit.MessageID)
	if err != nil || !found {
		return PendingExecutionUpdate{}, false, err
	}
	if pending.Update != commit.Update {
		return PendingExecutionUpdate{}, false, nil
	}
	observation, found, err := loadObservation(ctx, tx, commit.Observation.ExecutionID)
	if err != nil || !found {
		return PendingExecutionUpdate{}, false, err
	}
	if observation.State != commit.Observation.State &&
		!domain.CanReachExecutionState(commit.Observation.State, observation.State) {
		return PendingExecutionUpdate{}, false, nil
	}
	if commit.CleanupTombstone != nil {
		tombstone, found, err := loadCleanupTombstone(
			ctx,
			tx,
			commit.CleanupTombstone.ExecutionID,
		)
		if err != nil || !found {
			return PendingExecutionUpdate{}, false, err
		}
		if tombstone.FailureCode != commit.CleanupTombstone.FailureCode {
			return PendingExecutionUpdate{}, false, nil
		}
	}
	if err := tx.Commit(); err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	return pending, true, nil
}
