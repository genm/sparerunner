package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/genm/tewake/internal/domain"
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

// CleanupFailureCode is intentionally closed: raw cleanup errors can contain JIT
// material, tokens, paths, or runner output and must never enter the journal.
type CleanupFailureCode string

const (
	CleanupVerificationFailed CleanupFailureCode = "cleanup_verification_failed"
	CleanupProcessResidue     CleanupFailureCode = "process_residue"
	CleanupWorkspaceRemoval   CleanupFailureCode = "workspace_removal_failed"
)

func (c CleanupFailureCode) Validate() error {
	switch c {
	case CleanupVerificationFailed, CleanupProcessResidue, CleanupWorkspaceRemoval:
		return nil
	default:
		return errors.New("cleanup tombstone failure code is not allowlisted")
	}
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

	var existing domain.Command
	readErr := tx.QueryRowContext(ctx, `SELECT command_id, controller_epoch, execution_id, expected_state, payload_digest FROM command_replays WHERE command_id = ?`, command.ID).Scan(&existing.ID, &existing.ControllerEpoch, &existing.ExecutionID, &existing.ExpectedState, &existing.PayloadDigest)
	switch {
	case readErr == nil && existing == command:
		return true, nil
	case readErr == nil && existing.PayloadDigest != command.PayloadDigest:
		return false, fmt.Errorf("%w: command payload digest", ErrReplayMismatch)
	case readErr == nil:
		return false, fmt.Errorf("%w: command identity", ErrReplayMismatch)
	case !errors.Is(readErr, sql.ErrNoRows):
		return false, readErr
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

func (s *AgentStore) RecordObservation(ctx context.Context, observation Observation) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if observation.ExecutionID == "" {
		return errors.New("observation requires execution ID")
	}
	if err := observation.State.Validate("observation.state"); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO execution_observations(execution_id, state, observed_at_unix_nano) VALUES (?, ?, ?) ON CONFLICT(execution_id) DO UPDATE SET state=excluded.state, observed_at_unix_nano=excluded.observed_at_unix_nano`, observation.ExecutionID, observation.State, s.now().UnixNano())
	return err
}

func (s *AgentStore) RecordCleanupTombstone(ctx context.Context, tombstone CleanupTombstone) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if tombstone.ExecutionID == "" {
		return errors.New("cleanup tombstone requires execution ID")
	}
	if err := tombstone.FailureCode.Validate(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO cleanup_tombstones(execution_id, failure_code, recorded_at_unix_nano) VALUES (?, ?, ?) ON CONFLICT(execution_id) DO UPDATE SET failure_code=excluded.failure_code, recorded_at_unix_nano=excluded.recorded_at_unix_nano`, tombstone.ExecutionID, tombstone.FailureCode, s.now().UnixNano())
	return err
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
		if observation.ExecutionID == "" {
			rows.Close()
			return AgentSnapshot{}, errors.New("stored observation requires execution ID")
		}
		if err := observation.State.Validate("observation.state"); err != nil {
			rows.Close()
			return AgentSnapshot{}, err
		}
		result.Observations = append(result.Observations, observation)
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
		if tombstone.ExecutionID == "" {
			rows.Close()
			return AgentSnapshot{}, errors.New("stored cleanup tombstone requires execution ID")
		}
		if err := tombstone.FailureCode.Validate(); err != nil {
			rows.Close()
			return AgentSnapshot{}, err
		}
		result.CleanupTombstones = append(result.CleanupTombstones, tombstone)
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
