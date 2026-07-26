package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/genm/tewake/internal/domain"
)

var ErrExecutionUpdateNotFound = errors.New("store execution update is not pending")

// ExecutionUpdateRecord is the storage-owned classified lifecycle shape. It is
// deliberately independent of the WebSocket protocol DTO.
type ExecutionUpdateRecord struct {
	NodeID      domain.NodeID
	CommandID   domain.CommandID
	ExecutionID domain.ExecutionID
	State       domain.ExecutionState
	Replayed    bool
	ErrorCode   domain.ExecutionErrorCode
}

func (update ExecutionUpdateRecord) Validate() error {
	if update.NodeID == "" || update.CommandID == "" || update.ExecutionID == "" {
		return errors.New("execution update identity is incomplete")
	}
	return domain.ValidateExecutionResult(update.State, update.ErrorCode, "execution_update")
}

// PendingExecutionUpdate is the typed, non-secret unit retained until the
// Controller acknowledges that its durable consumer accepted the update.
type PendingExecutionUpdate struct {
	Sequence  int64
	MessageID string
	Update    ExecutionUpdateRecord
}

func (s *AgentStore) QueueExecutionUpdate(
	ctx context.Context,
	messageID string,
	update ExecutionUpdateRecord,
) (PendingExecutionUpdate, bool, error) {
	if err := s.requireReady(); err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	if !isLowerSHA256(messageID) || update.Validate() != nil {
		return PendingExecutionUpdate{}, false, errors.New("execution update outbox input is invalid")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	defer tx.Rollback()

	existing, found, err := loadPendingExecutionUpdate(ctx, tx, messageID)
	if err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	if found {
		if existing.Update != update {
			return PendingExecutionUpdate{}, false, fmt.Errorf("%w: execution update message identity", ErrReplayMismatch)
		}
		return existing, false, nil
	}
	pending, err := insertExecutionUpdateTx(ctx, tx, messageID, update)
	if err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	if err := tx.Commit(); err != nil {
		// Resolve an ambiguous commit without inventing success. An exact row is
		// durable evidence that this write reached the outbox.
		observed, observedFound, observeErr := loadPendingExecutionUpdate(ctx, s.db, messageID)
		if observeErr == nil && observedFound && observed.Update == update {
			return observed, false, nil
		}
		return PendingExecutionUpdate{}, false, err
	}
	return pending, true, nil
}

func insertExecutionUpdateTx(
	ctx context.Context,
	tx *sql.Tx,
	messageID string,
	update ExecutionUpdateRecord,
) (PendingExecutionUpdate, error) {
	result, err := tx.ExecContext(ctx, `INSERT INTO execution_update_outbox (
		message_id, node_id, command_id, execution_id, state, replayed, error_code
	) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		messageID,
		update.NodeID,
		update.CommandID,
		update.ExecutionID,
		update.State,
		update.Replayed,
		update.ErrorCode,
	)
	if err != nil {
		return PendingExecutionUpdate{}, err
	}
	sequence, err := result.LastInsertId()
	if err != nil || sequence <= 0 {
		return PendingExecutionUpdate{}, errors.New("execution update outbox sequence is invalid")
	}
	return PendingExecutionUpdate{Sequence: sequence, MessageID: messageID, Update: update}, nil
}

func (s *AgentStore) PendingExecutionUpdates(ctx context.Context) ([]PendingExecutionUpdate, error) {
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT sequence, message_id, node_id, command_id, execution_id, state, replayed, error_code
		FROM execution_update_outbox ORDER BY sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []PendingExecutionUpdate
	for rows.Next() {
		item, err := scanPendingExecutionUpdate(rows)
		if err != nil {
			return nil, err
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pending, nil
}

func (s *AgentStore) AcknowledgeExecutionUpdate(ctx context.Context, messageID string) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if !isLowerSHA256(messageID) {
		return ErrExecutionUpdateNotFound
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pending, found, err := loadPendingExecutionUpdate(ctx, tx, messageID)
	if err != nil {
		return err
	}
	if !found {
		return ErrExecutionUpdateNotFound
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM execution_update_outbox WHERE message_id = ?`, messageID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrExecutionUpdateNotFound
	}
	if pending.Update.State == domain.ExecutionReleased ||
		pending.Update.State == domain.ExecutionFailed {
		if err := pruneAcknowledgedTerminalExecution(ctx, tx, pending.Update.ExecutionID); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		_, found, observeErr := loadPendingExecutionUpdate(ctx, s.db, messageID)
		if observeErr == nil && !found {
			return nil
		}
		return err
	}
	return nil
}

func pruneAcknowledgedTerminalExecution(
	ctx context.Context,
	tx *sql.Tx,
	executionID domain.ExecutionID,
) error {
	// An older update must be delivered before the terminal update because the
	// Agent sends this outbox in sequence order. If evidence contradicts that
	// ordering, retain the complete journal and fail closed.
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM execution_update_outbox
		WHERE execution_id = ?`, executionID).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return errors.New("terminal execution acknowledgement preceded an older outbox update")
	}
	var tombstones int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM cleanup_tombstones
		WHERE execution_id = ?`, executionID).Scan(&tombstones); err != nil {
		return err
	}
	if tombstones != 0 {
		return errors.New("clean terminal execution retained cleanup failure evidence")
	}
	for _, statement := range []string{
		`DELETE FROM command_replays WHERE execution_id = ?`,
		`DELETE FROM execution_observations WHERE execution_id = ?`,
		`DELETE FROM runner_journal_records WHERE execution_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, statement, executionID); err != nil {
			return err
		}
	}
	return nil
}

type pendingExecutionUpdateScanner interface {
	Scan(...any) error
}

func scanPendingExecutionUpdate(scanner pendingExecutionUpdateScanner) (PendingExecutionUpdate, error) {
	var pending PendingExecutionUpdate
	var replayed int64
	err := scanner.Scan(
		&pending.Sequence,
		&pending.MessageID,
		&pending.Update.NodeID,
		&pending.Update.CommandID,
		&pending.Update.ExecutionID,
		&pending.Update.State,
		&replayed,
		&pending.Update.ErrorCode,
	)
	if err != nil {
		return PendingExecutionUpdate{}, err
	}
	pending.Update.Replayed = replayed == 1
	if pending.Sequence <= 0 || !isLowerSHA256(pending.MessageID) || (replayed != 0 && replayed != 1) || pending.Update.Validate() != nil {
		return PendingExecutionUpdate{}, errors.New("stored execution update failed validation")
	}
	return pending, nil
}

func loadPendingExecutionUpdate(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, messageID string) (PendingExecutionUpdate, bool, error) {
	row := queryer.QueryRowContext(ctx, `SELECT sequence, message_id, node_id, command_id, execution_id, state, replayed, error_code
		FROM execution_update_outbox WHERE message_id = ?`, messageID)
	pending, err := scanPendingExecutionUpdate(row)
	if errors.Is(err, sql.ErrNoRows) {
		return PendingExecutionUpdate{}, false, nil
	}
	if err != nil {
		return PendingExecutionUpdate{}, false, err
	}
	return pending, true, nil
}
