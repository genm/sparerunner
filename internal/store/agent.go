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
	Reason      string
}

func (s *AgentStore) RecordCommand(ctx context.Context, command domain.Command) (bool, error) {
	if err := s.requireReady(); err != nil {
		return false, err
	}
	if err := command.Validate(); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO command_replays(command_id, controller_epoch, execution_id, expected_state, payload_digest) VALUES (?, ?, ?, ?, ?)`, command.ID, command.ControllerEpoch, command.ExecutionID, command.ExpectedState, command.PayloadDigest)
	if err == nil {
		return false, tx.Commit()
	}
	var existing domain.Command
	readErr := tx.QueryRowContext(ctx, `SELECT command_id, controller_epoch, execution_id, expected_state, payload_digest FROM command_replays WHERE command_id = ?`, command.ID).Scan(&existing.ID, &existing.ControllerEpoch, &existing.ExecutionID, &existing.ExpectedState, &existing.PayloadDigest)
	if readErr != nil {
		return false, err
	}
	if existing == command {
		return true, nil
	}
	if existing.PayloadDigest != command.PayloadDigest {
		return false, fmt.Errorf("%w: command payload digest", ErrReplayMismatch)
	}
	return false, fmt.Errorf("%w: command identity", ErrReplayMismatch)
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
	if tombstone.ExecutionID == "" || tombstone.Reason == "" {
		return errors.New("cleanup tombstone requires execution ID and reason")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO cleanup_tombstones(execution_id, reason, recorded_at_unix_nano) VALUES (?, ?, ?) ON CONFLICT(execution_id) DO UPDATE SET reason=excluded.reason, recorded_at_unix_nano=excluded.recorded_at_unix_nano`, tombstone.ExecutionID, tombstone.Reason, s.now().UnixNano())
	return err
}

func (s *AgentStore) Backup(ctx context.Context, destination string) error {
	return s.backup(ctx, destination)
}
func RestoreAgent(ctx context.Context, destination, backup string) error {
	return restore(ctx, destination, backup, "agent")
}
