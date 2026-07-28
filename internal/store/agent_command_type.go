package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/genm/sparerunner/internal/domain"
)

// AcceptedAgentCommand is the non-secret local admission record used only for
// restart reconciliation. The authenticated payload remains represented by the
// digest in Command.
type AcceptedAgentCommand struct {
	Type    domain.CommandType
	Command domain.Command
}

func (accepted AcceptedAgentCommand) validate() error {
	if err := accepted.Type.Validate("accepted_command.type"); err != nil {
		return err
	}
	if err := accepted.Command.Validate(); err != nil {
		return err
	}
	switch accepted.Type {
	case domain.CommandPrepare:
		if accepted.Command.ExpectedState != domain.ExecutionReserved {
			return errors.New("accepted prepare command must expect reserved")
		}
	case domain.CommandStart:
		if accepted.Command.ExpectedState != domain.ExecutionPreparing {
			return errors.New("accepted start command must expect preparing")
		}
	case domain.CommandCancel:
		switch accepted.Command.ExpectedState {
		case domain.ExecutionPreparing, domain.ExecutionRunning, domain.ExecutionCleaning:
		default:
			return errors.New("accepted cancel command must expect an active execution")
		}
	}
	return nil
}

// LookupTypedCommand accepts only an exact command-and-type replay. A legacy
// command row without a type cannot authorize replay of a secret-bearing Start.
func (s *AgentStore) LookupTypedCommand(
	ctx context.Context,
	accepted AcceptedAgentCommand,
) (bool, error) {
	if err := s.requireReady(); err != nil {
		return false, err
	}
	if err := accepted.validate(); err != nil {
		return false, err
	}
	return lookupTypedCommand(ctx, s.db, accepted)
}

// RecordTypedCommand atomically records replay identity, command type, and the
// maximum accepted Controller epoch before the transport can acknowledge it.
func (s *AgentStore) RecordTypedCommand(
	ctx context.Context,
	accepted AcceptedAgentCommand,
) (bool, error) {
	if err := s.requireReady(); err != nil {
		return false, err
	}
	if err := accepted.validate(); err != nil {
		return false, err
	}
	if uint64(accepted.Command.ControllerEpoch) > maxSQLiteInteger {
		return false, errors.New("controller epoch exceeds SQLite's signed INTEGER range")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	replayed, err := lookupTypedCommand(ctx, tx, accepted)
	if err != nil {
		return false, err
	}
	if replayed {
		return true, nil
	}
	// A command identity already present without its exact type is ambiguous.
	// Never retrofit it into a replay authority.
	if commandExists, err := commandReplayExists(ctx, tx, accepted.Command.ID); err != nil {
		return false, err
	} else if commandExists {
		return false, fmt.Errorf("%w: accepted command type is absent", ErrReplayMismatch)
	}
	maxEpoch, err := readUintMetadata(ctx, tx, "max_controller_epoch")
	if err != nil {
		return false, err
	}
	if uint64(accepted.Command.ControllerEpoch) < maxEpoch {
		return false, fmt.Errorf(
			"%w: got %d, accepted %d",
			ErrStaleControllerEpoch,
			accepted.Command.ControllerEpoch,
			maxEpoch,
		)
	}
	command := accepted.Command
	if _, err := tx.ExecContext(ctx, `INSERT INTO command_replays(
		command_id, controller_epoch, execution_id, expected_state, payload_digest
	) VALUES (?, ?, ?, ?, ?)`, command.ID, command.ControllerEpoch,
		command.ExecutionID, command.ExpectedState, command.PayloadDigest); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO accepted_command_types(
		command_id, command_type
	) VALUES (?, ?)`, command.ID, accepted.Type); err != nil {
		return false, err
	}
	if uint64(command.ControllerEpoch) > maxEpoch {
		if _, err := tx.ExecContext(ctx, `UPDATE store_metadata
			SET value = ? WHERE key = 'max_controller_epoch'`, command.ControllerEpoch); err != nil {
			return false, err
		}
	}
	return false, tx.Commit()
}

func lookupTypedCommand(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	accepted AcceptedAgentCommand,
) (bool, error) {
	var existing AcceptedAgentCommand
	err := queryer.QueryRowContext(ctx, `SELECT t.command_type, c.command_id,
		c.controller_epoch, c.execution_id, c.expected_state, c.payload_digest
		FROM command_replays c
		JOIN accepted_command_types t ON t.command_id = c.command_id
		WHERE c.command_id = ?`, accepted.Command.ID).Scan(
		&existing.Type,
		&existing.Command.ID,
		&existing.Command.ControllerEpoch,
		&existing.Command.ExecutionID,
		&existing.Command.ExpectedState,
		&existing.Command.PayloadDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := existing.validate(); err != nil {
		return false, fmt.Errorf("stored accepted command is invalid: %w", err)
	}
	if existing != accepted {
		return false, fmt.Errorf("%w: accepted command identity", ErrReplayMismatch)
	}
	return true, nil
}

func commandReplayExists(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	commandID domain.CommandID,
) (bool, error) {
	var present int
	err := queryer.QueryRowContext(ctx, `SELECT 1 FROM command_replays WHERE command_id = ?`, commandID).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil && present == 1, err
}

// AcceptedAgentCommands returns only typed, validated recovery authorities.
func (s *AgentStore) AcceptedAgentCommands(ctx context.Context) ([]AcceptedAgentCommand, error) {
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT t.command_type, c.command_id,
		c.controller_epoch, c.execution_id, c.expected_state, c.payload_digest
		FROM command_replays c
		JOIN accepted_command_types t ON t.command_id = c.command_id
		ORDER BY c.execution_id, c.command_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var commands []AcceptedAgentCommand
	for rows.Next() {
		var accepted AcceptedAgentCommand
		if err := rows.Scan(
			&accepted.Type,
			&accepted.Command.ID,
			&accepted.Command.ControllerEpoch,
			&accepted.Command.ExecutionID,
			&accepted.Command.ExpectedState,
			&accepted.Command.PayloadDigest,
		); err != nil {
			return nil, err
		}
		if err := accepted.validate(); err != nil {
			return nil, fmt.Errorf("stored accepted command is invalid: %w", err)
		}
		commands = append(commands, accepted)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return commands, nil
}
