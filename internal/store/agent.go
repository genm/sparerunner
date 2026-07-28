package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/genm/sparerunner/internal/domain"
)

type Observation struct {
	ExecutionID domain.ExecutionID
	State       domain.ExecutionState
}

type CleanupTombstone struct {
	ExecutionID domain.ExecutionID
	FailureCode CleanupFailureCode
}

type ObservationSnapshot struct {
	ExecutionID        domain.ExecutionID
	State              domain.ExecutionState
	ObservedAtUnixNano int64
}

type CleanupTombstoneSnapshot struct {
	ExecutionID        domain.ExecutionID
	FailureCode        CleanupFailureCode
	RecordedAtUnixNano int64
}

type AgentSnapshot struct {
	MaxControllerEpoch domain.ControllerEpoch
	Commands           []domain.Command
	Observations       []ObservationSnapshot
	CleanupTombstones  []CleanupTombstoneSnapshot
}

// CleanupFailureCode remains a store-facing alias for the domain-owned closed
// classification.
type CleanupFailureCode = domain.CleanupFailureCode

const (
	CleanupVerificationFailed = domain.CleanupVerificationFailed
	CleanupProcessResidue     = domain.CleanupProcessResidue
	CleanupWorkspaceRemoval   = domain.CleanupWorkspaceRemoval
)

// LookupCommand distinguishes an exact, previously accepted command from a new
// command without mutating the epoch fence. Agent command admission uses this
// before expected-state validation so an exact replay can resume an
// Accept-before-ACK crash, while a command rejected by state validation never
// gains a durable replay bypass.
func (s *AgentStore) LookupCommand(ctx context.Context, command domain.Command) (bool, error) {
	if err := s.requireReady(); err != nil {
		return false, err
	}
	if err := command.Validate(); err != nil {
		return false, err
	}
	if uint64(command.ControllerEpoch) > maxSQLiteInteger {
		return false, errors.New("controller epoch exceeds SQLite's signed INTEGER range")
	}
	return lookupCommand(ctx, s.db, command)
}

func (s *AgentStore) RecordCommand(ctx context.Context, command domain.Command) (bool, error) {
	if err := s.requireReady(); err != nil {
		return false, err
	}
	if err := command.Validate(); err != nil {
		return false, err
	}
	if uint64(command.ControllerEpoch) > maxSQLiteInteger {
		return false, errors.New("controller epoch exceeds SQLite's signed INTEGER range")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	replayed, err := lookupCommand(ctx, tx, command)
	if err != nil {
		return false, err
	}
	if replayed {
		return true, nil
	}

	maxEpoch, err := readUintMetadata(ctx, tx, "max_controller_epoch")
	if err != nil {
		return false, err
	}
	if uint64(command.ControllerEpoch) < maxEpoch {
		return false, fmt.Errorf("%w: got %d, accepted %d", ErrStaleControllerEpoch, command.ControllerEpoch, maxEpoch)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO command_replays(command_id, controller_epoch, execution_id, expected_state, payload_digest) VALUES (?, ?, ?, ?, ?)`, command.ID, command.ControllerEpoch, command.ExecutionID, command.ExpectedState, command.PayloadDigest); err != nil {
		return false, err
	}
	if uint64(command.ControllerEpoch) > maxEpoch {
		if _, err := tx.ExecContext(ctx, `UPDATE store_metadata SET value = ? WHERE key = 'max_controller_epoch'`, command.ControllerEpoch); err != nil {
			return false, err
		}
	}
	return false, tx.Commit()
}

func lookupCommand(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, command domain.Command) (bool, error) {
	var existing domain.Command
	err := queryer.QueryRowContext(ctx, `SELECT command_id, controller_epoch, execution_id, expected_state, payload_digest FROM command_replays WHERE command_id = ?`, command.ID).Scan(
		&existing.ID,
		&existing.ControllerEpoch,
		&existing.ExecutionID,
		&existing.ExpectedState,
		&existing.PayloadDigest,
	)
	switch {
	case err == nil && existing == command:
		return true, nil
	case err == nil && existing.PayloadDigest != command.PayloadDigest:
		return false, fmt.Errorf("%w: command payload digest", ErrReplayMismatch)
	case err == nil:
		return false, fmt.Errorf("%w: command identity", ErrReplayMismatch)
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, err
	}
}

func (s *AgentStore) RecordObservation(ctx context.Context, observation Observation) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if err := validateObservation(observation); err != nil {
		return err
	}
	observedAt, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := recordObservationTx(ctx, tx, observation, observedAt, true, false); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *AgentStore) RecordCleanupTombstone(ctx context.Context, tombstone CleanupTombstone) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if err := validateCleanupTombstone(tombstone); err != nil {
		return err
	}
	recordedAt, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := recordCleanupTombstoneTx(ctx, tx, tombstone, recordedAt, true); err != nil {
		return err
	}
	return tx.Commit()
}

func validateObservation(observation Observation) error {
	if observation.ExecutionID == "" {
		return errors.New("observation requires execution ID")
	}
	return observation.State.Validate("observation.state")
}

func recordObservationTx(
	ctx context.Context,
	tx *sql.Tx,
	observation Observation,
	observedAt int64,
	touchEqual bool,
	allowAdvanced bool,
) (ObservationSnapshot, error) {
	previous, found, err := loadObservation(ctx, tx, observation.ExecutionID)
	if err != nil {
		return ObservationSnapshot{}, err
	}
	if found {
		switch {
		case previous.State == observation.State && !touchEqual:
			return previous, nil
		case !domain.CanReachExecutionState(previous.State, observation.State):
			if allowAdvanced && domain.CanReachExecutionState(observation.State, previous.State) {
				return previous, nil
			}
			return ObservationSnapshot{}, errors.New("observation state cannot regress")
		}
		observedAt, err = monotonicAgentTimestamp(observedAt, previous.ObservedAtUnixNano, "observation")
		if err != nil {
			return ObservationSnapshot{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO execution_observations(
		execution_id, state, observed_at_unix_nano
	) VALUES (?, ?, ?)
	ON CONFLICT(execution_id) DO UPDATE SET
		state=excluded.state,
		observed_at_unix_nano=excluded.observed_at_unix_nano`,
		observation.ExecutionID, observation.State, observedAt); err != nil {
		return ObservationSnapshot{}, err
	}
	return ObservationSnapshot{
		ExecutionID:        observation.ExecutionID,
		State:              observation.State,
		ObservedAtUnixNano: observedAt,
	}, nil
}

func loadObservation(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, executionID domain.ExecutionID) (ObservationSnapshot, bool, error) {
	var observation ObservationSnapshot
	err := queryer.QueryRowContext(ctx, `SELECT execution_id, state, observed_at_unix_nano
		FROM execution_observations WHERE execution_id = ?`, executionID).
		Scan(&observation.ExecutionID, &observation.State, &observation.ObservedAtUnixNano)
	if errors.Is(err, sql.ErrNoRows) {
		return ObservationSnapshot{}, false, nil
	}
	if err != nil {
		return ObservationSnapshot{}, false, err
	}
	if observation.ExecutionID == "" || observation.ObservedAtUnixNano <= 0 ||
		observation.State.Validate("observation.state") != nil {
		return ObservationSnapshot{}, false, errors.New("stored observation failed validation")
	}
	return observation, true, nil
}

func validateCleanupTombstone(tombstone CleanupTombstone) error {
	if tombstone.ExecutionID == "" {
		return errors.New("cleanup tombstone requires execution ID")
	}
	return tombstone.FailureCode.Validate("cleanup_tombstone.failure_code")
}

func recordCleanupTombstoneTx(
	ctx context.Context,
	tx *sql.Tx,
	tombstone CleanupTombstone,
	recordedAt int64,
	touchEqual bool,
) (CleanupTombstoneSnapshot, error) {
	previous, found, err := loadCleanupTombstone(ctx, tx, tombstone.ExecutionID)
	if err != nil {
		return CleanupTombstoneSnapshot{}, err
	}
	if found {
		if previous.FailureCode != tombstone.FailureCode {
			return CleanupTombstoneSnapshot{}, errors.New("cleanup tombstone classification is immutable")
		}
		if !touchEqual {
			return previous, nil
		}
		recordedAt, err = monotonicAgentTimestamp(recordedAt, previous.RecordedAtUnixNano, "cleanup tombstone")
		if err != nil {
			return CleanupTombstoneSnapshot{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cleanup_tombstones(
		execution_id, failure_code, recorded_at_unix_nano
	) VALUES (?, ?, ?)
	ON CONFLICT(execution_id) DO UPDATE SET
		failure_code=excluded.failure_code,
		recorded_at_unix_nano=excluded.recorded_at_unix_nano`,
		tombstone.ExecutionID, tombstone.FailureCode, recordedAt); err != nil {
		return CleanupTombstoneSnapshot{}, err
	}
	return CleanupTombstoneSnapshot{
		ExecutionID:        tombstone.ExecutionID,
		FailureCode:        tombstone.FailureCode,
		RecordedAtUnixNano: recordedAt,
	}, nil
}

func loadCleanupTombstone(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, executionID domain.ExecutionID) (CleanupTombstoneSnapshot, bool, error) {
	var tombstone CleanupTombstoneSnapshot
	err := queryer.QueryRowContext(ctx, `SELECT execution_id, failure_code, recorded_at_unix_nano
		FROM cleanup_tombstones WHERE execution_id = ?`, executionID).
		Scan(&tombstone.ExecutionID, &tombstone.FailureCode, &tombstone.RecordedAtUnixNano)
	if errors.Is(err, sql.ErrNoRows) {
		return CleanupTombstoneSnapshot{}, false, nil
	}
	if err != nil {
		return CleanupTombstoneSnapshot{}, false, err
	}
	if tombstone.ExecutionID == "" || tombstone.RecordedAtUnixNano <= 0 ||
		tombstone.FailureCode.Validate("cleanup_tombstone.failure_code") != nil {
		return CleanupTombstoneSnapshot{}, false, errors.New("stored cleanup tombstone failed validation")
	}
	return tombstone, true, nil
}

func monotonicAgentTimestamp(candidate, previous int64, subject string) (int64, error) {
	if candidate > previous {
		return candidate, nil
	}
	if previous == int64(^uint64(0)>>1) {
		return 0, fmt.Errorf("%s timestamp is exhausted", subject)
	}
	return previous + 1, nil
}

// Snapshot returns only typed replay and cleanup observations. Command payloads,
// node private keys, JIT configuration, and raw cleanup errors are not persisted
// and therefore cannot be exposed by this restart boundary.
func (s *AgentStore) Snapshot(ctx context.Context) (AgentSnapshot, error) {
	if err := s.requireReady(); err != nil {
		return AgentSnapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AgentSnapshot{}, err
	}
	defer tx.Rollback()
	rawEpoch, err := readUintMetadata(ctx, tx, "max_controller_epoch")
	if err != nil {
		return AgentSnapshot{}, err
	}
	result := AgentSnapshot{MaxControllerEpoch: domain.ControllerEpoch(rawEpoch)}

	rows, err := tx.QueryContext(ctx, `SELECT command_id, controller_epoch, execution_id, expected_state, payload_digest FROM command_replays ORDER BY command_id`)
	if err != nil {
		return AgentSnapshot{}, err
	}
	for rows.Next() {
		var command domain.Command
		if err := rows.Scan(&command.ID, &command.ControllerEpoch, &command.ExecutionID, &command.ExpectedState, &command.PayloadDigest); err != nil {
			rows.Close()
			return AgentSnapshot{}, err
		}
		if err := command.Validate(); err != nil {
			rows.Close()
			return AgentSnapshot{}, err
		}
		result.Commands = append(result.Commands, command)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return AgentSnapshot{}, err
	}
	if err := rows.Close(); err != nil {
		return AgentSnapshot{}, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT execution_id, state, observed_at_unix_nano FROM execution_observations ORDER BY execution_id`)
	if err != nil {
		return AgentSnapshot{}, err
	}
	for rows.Next() {
		var observation ObservationSnapshot
		if err := rows.Scan(&observation.ExecutionID, &observation.State, &observation.ObservedAtUnixNano); err != nil {
			rows.Close()
			return AgentSnapshot{}, err
		}
		if observation.ExecutionID == "" || observation.ObservedAtUnixNano <= 0 {
			rows.Close()
			return AgentSnapshot{}, errors.New("stored observation failed validation")
		}
		if err := observation.State.Validate("observation.state"); err != nil {
			rows.Close()
			return AgentSnapshot{}, err
		}
		result.Observations = append(result.Observations, observation)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return AgentSnapshot{}, err
	}
	if err := rows.Close(); err != nil {
		return AgentSnapshot{}, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT execution_id, failure_code, recorded_at_unix_nano FROM cleanup_tombstones ORDER BY execution_id`)
	if err != nil {
		return AgentSnapshot{}, err
	}
	for rows.Next() {
		var tombstone CleanupTombstoneSnapshot
		if err := rows.Scan(&tombstone.ExecutionID, &tombstone.FailureCode, &tombstone.RecordedAtUnixNano); err != nil {
			rows.Close()
			return AgentSnapshot{}, err
		}
		if tombstone.ExecutionID == "" || tombstone.RecordedAtUnixNano <= 0 {
			rows.Close()
			return AgentSnapshot{}, errors.New("stored cleanup tombstone failed validation")
		}
		if err := tombstone.FailureCode.Validate("cleanup_tombstone.failure_code"); err != nil {
			rows.Close()
			return AgentSnapshot{}, err
		}
		result.CleanupTombstones = append(result.CleanupTombstones, tombstone)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return AgentSnapshot{}, err
	}
	if err := rows.Close(); err != nil {
		return AgentSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AgentSnapshot{}, err
	}
	return result, nil
}

func (s *AgentStore) Backup(ctx context.Context, destination string) error {
	return s.backup(ctx, destination)
}
func RestoreAgent(ctx context.Context, destination, backup string) error {
	return restore(ctx, destination, backup, "agent")
}
