package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/genm/tewake/internal/domain"
	sqlite "modernc.org/sqlite"
)

type Assignment struct {
	ScaleSetID    ScaleSetID
	MessageID     MessageID
	MessageDigest string
	Execution     domain.ExecutionSnapshot
}

// ScaleSetID and MessageID are canonical positive GitHub numeric identities.
// Accepting strings here could make `1` and `01` distinct deduplication keys.
type ScaleSetID uint64
type MessageID uint64

type SlotReservation struct {
	Slot  domain.SlotKey
	Owner domain.SlotOwner
}

type ProcessedMessage struct {
	ScaleSetID        ScaleSetID
	MessageID         MessageID
	MessageDigest     string
	ExecutionID       domain.ExecutionID
	CreatedAtUnixNano int64
}

type ControllerSnapshot struct {
	ControllerEpoch   domain.ControllerEpoch
	Reservations      []SlotReservation
	Executions        []domain.ExecutionSnapshot
	ProcessedMessages []ProcessedMessage
}

func (a Assignment) Validate() error {
	if a.ScaleSetID == 0 || a.MessageID == 0 || uint64(a.ScaleSetID) > maxSQLiteInteger || uint64(a.MessageID) > maxSQLiteInteger {
		return errors.New("assignment requires positive scale set and message IDs within SQLite's signed INTEGER range")
	}
	if !isLowerSHA256(a.MessageDigest) {
		return errors.New("assignment requires a lowercase SHA-256 message digest")
	}
	if err := a.Execution.Validate(); err != nil {
		return err
	}
	if a.Execution.State != domain.ExecutionReserved {
		return errors.New("assigned execution state must be reserved")
	}
	return nil
}

func isLowerSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

// Assign atomically records the durable GitHub message, its concrete slot claim,
// and desired execution before callers acknowledge GitHub's message.
func (s *ControllerStore) Assign(ctx context.Context, assignment Assignment) (domain.ExecutionSnapshot, bool, error) {
	if err := s.requireReady(); err != nil {
		return domain.ExecutionSnapshot{}, false, err
	}
	if err := assignment.Validate(); err != nil {
		return domain.ExecutionSnapshot{}, false, err
	}
	// BEGIN IMMEDIATE serializes competing writer handles before replay reads.
	// The outer budget covers one busy_timeout plus a retry, remains cancellable,
	// and turns abnormal prolonged contention into a stable store error.
	retryCtx, cancel := context.WithTimeout(ctx, 2*time.Duration(sqliteBusyTimeoutMilliseconds)*time.Millisecond)
	defer cancel()
	for attempt := 0; ; attempt++ {
		snapshot, replay, err := s.assignOnce(retryCtx, assignment)
		if err == nil || !isBusy(err) {
			return snapshot, replay, err
		}
		select {
		case <-retryCtx.Done():
			if ctx.Err() != nil {
				return domain.ExecutionSnapshot{}, false, ctx.Err()
			}
			return domain.ExecutionSnapshot{}, false, fmt.Errorf("%w: %v", ErrStoreBusy, err)
		case <-time.After(min(time.Duration(attempt+1)*25*time.Millisecond, 250*time.Millisecond)):
		}
	}
}

func (s *ControllerStore) assignOnce(ctx context.Context, assignment Assignment) (domain.ExecutionSnapshot, bool, error) {
	createdAt := s.now().UnixNano()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return domain.ExecutionSnapshot{}, false, err
	}
	defer tx.Rollback()
	if existing, found, err := findMessage(ctx, tx, assignment); err != nil {
		return domain.ExecutionSnapshot{}, false, err
	} else if found {
		return existing, true, nil
	}
	e := assignment.Execution
	if _, err := tx.ExecContext(ctx, `INSERT INTO slot_reservations(node_id, slot_index, target_id, execution_id) VALUES (?, ?, ?, ?)`, e.Slot.NodeID, e.Slot.Index, e.TargetID, e.ID); err != nil {
		return domain.ExecutionSnapshot{}, false, mapAssignmentError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES (?, ?, ?, ?, ?, ?)`, e.ID, e.TargetID, e.Slot.NodeID, e.Slot.Index, e.State, createdAt); err != nil {
		return domain.ExecutionSnapshot{}, false, mapAssignmentError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO processed_messages(scale_set_id, message_id, message_digest, execution_id, created_at_unix_nano) VALUES (?, ?, ?, ?, ?)`, assignment.ScaleSetID, assignment.MessageID, assignment.MessageDigest, e.ID, createdAt); err != nil {
		return domain.ExecutionSnapshot{}, false, mapAssignmentError(err)
	}
	if err := tx.Commit(); err != nil {
		return domain.ExecutionSnapshot{}, false, err
	}
	return e, false, nil
}

func isBusy(err error) bool {
	var sqliteError *sqlite.Error
	if errors.As(err, &sqliteError) {
		// SQLite extended result codes preserve the primary code in the low byte.
		return sqliteError.Code()&0xff == 5
	}
	return strings.Contains(strings.ToLower(err.Error()), "database is locked") || strings.Contains(strings.ToLower(err.Error()), "sqlite_busy")
}

func findMessage(ctx context.Context, tx *sql.Tx, assignment Assignment) (domain.ExecutionSnapshot, bool, error) {
	var digest, targetID, executionID, nodeID, state string
	var slotIndex int
	err := tx.QueryRowContext(ctx, `SELECT m.message_digest, e.target_id, e.id, e.node_id, e.slot_index, e.state FROM processed_messages m JOIN executions e ON e.id = m.execution_id WHERE m.scale_set_id = ? AND m.message_id = ?`, assignment.ScaleSetID, assignment.MessageID).Scan(&digest, &targetID, &executionID, &nodeID, &slotIndex, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ExecutionSnapshot{}, false, nil
	}
	if err != nil {
		return domain.ExecutionSnapshot{}, false, err
	}
	existing := domain.ExecutionSnapshot{ID: domain.ExecutionID(executionID), TargetID: domain.TargetID(targetID), Slot: domain.SlotKey{NodeID: domain.NodeID(nodeID), Index: slotIndex}, State: domain.ExecutionState(state)}
	if digest != assignment.MessageDigest || existing != assignment.Execution {
		return domain.ExecutionSnapshot{}, false, fmt.Errorf("%w: scale set message %d", ErrReplayMismatch, assignment.MessageID)
	}
	return existing, true, nil
}

func mapAssignmentError(err error) error {
	message := err.Error()
	switch {
	case strings.Contains(message, "slot_reservations.node_id, slot_reservations.slot_index"):
		return fmt.Errorf("%w: %v", ErrSlotAlreadyReserved, err)
	case strings.Contains(message, "slot_reservations.execution_id"), strings.Contains(message, "active_execution_per_slot"), strings.Contains(message, "executions.id"):
		return fmt.Errorf("%w: %v", ErrActiveExecution, err)
	default:
		return err
	}
}

func (s *ControllerStore) AdvanceEpoch(ctx context.Context) (domain.ControllerEpoch, error) {
	if err := s.requireReady(); err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	raw, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return 0, err
	}
	next, err := domain.NextControllerEpoch(domain.ControllerEpoch(raw))
	if err != nil {
		return 0, err
	}
	if uint64(next) > maxSQLiteInteger {
		return 0, errors.New("controller epoch exceeds SQLite's signed INTEGER range")
	}
	if _, err := tx.ExecContext(ctx, "UPDATE store_metadata SET value = ? WHERE key = 'controller_epoch'", next); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

// Snapshot returns the complete non-secret desired-state and replay view needed
// to reconcile a restarted controller. All rows are read in one transaction and
// returned in stable identity order.
func (s *ControllerStore) Snapshot(ctx context.Context) (ControllerSnapshot, error) {
	if err := s.requireReady(); err != nil {
		return ControllerSnapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ControllerSnapshot{}, err
	}
	defer tx.Rollback()
	rawEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return ControllerSnapshot{}, err
	}
	result := ControllerSnapshot{ControllerEpoch: domain.ControllerEpoch(rawEpoch)}

	rows, err := tx.QueryContext(ctx, `SELECT node_id, slot_index, target_id, execution_id FROM slot_reservations ORDER BY node_id, slot_index`)
	if err != nil {
		return ControllerSnapshot{}, err
	}
	for rows.Next() {
		var nodeID, targetID, executionID string
		var slotIndex int
		if err := rows.Scan(&nodeID, &slotIndex, &targetID, &executionID); err != nil {
			rows.Close()
			return ControllerSnapshot{}, err
		}
		result.Reservations = append(result.Reservations, SlotReservation{
			Slot:  domain.SlotKey{NodeID: domain.NodeID(nodeID), Index: slotIndex},
			Owner: domain.SlotOwner{TargetID: domain.TargetID(targetID), ExecutionID: domain.ExecutionID(executionID)},
		})
	}
	if err := rows.Close(); err != nil {
		return ControllerSnapshot{}, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT id, target_id, node_id, slot_index, state FROM executions ORDER BY id`)
	if err != nil {
		return ControllerSnapshot{}, err
	}
	for rows.Next() {
		var id, targetID, nodeID, state string
		var slotIndex int
		if err := rows.Scan(&id, &targetID, &nodeID, &slotIndex, &state); err != nil {
			rows.Close()
			return ControllerSnapshot{}, err
		}
		execution := domain.ExecutionSnapshot{
			ID:       domain.ExecutionID(id),
			TargetID: domain.TargetID(targetID),
			Slot:     domain.SlotKey{NodeID: domain.NodeID(nodeID), Index: slotIndex},
			State:    domain.ExecutionState(state),
		}
		if err := execution.Validate(); err != nil {
			rows.Close()
			return ControllerSnapshot{}, err
		}
		result.Executions = append(result.Executions, execution)
	}
	if err := rows.Close(); err != nil {
		return ControllerSnapshot{}, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT scale_set_id, message_id, message_digest, execution_id, created_at_unix_nano FROM processed_messages ORDER BY scale_set_id, message_id`)
	if err != nil {
		return ControllerSnapshot{}, err
	}
	for rows.Next() {
		var message ProcessedMessage
		if err := rows.Scan(&message.ScaleSetID, &message.MessageID, &message.MessageDigest, &message.ExecutionID, &message.CreatedAtUnixNano); err != nil {
			rows.Close()
			return ControllerSnapshot{}, err
		}
		if message.ScaleSetID == 0 || message.MessageID == 0 || !isLowerSHA256(message.MessageDigest) || message.ExecutionID == "" {
			rows.Close()
			return ControllerSnapshot{}, errors.New("stored processed message failed validation")
		}
		result.ProcessedMessages = append(result.ProcessedMessages, message)
	}
	if err := rows.Close(); err != nil {
		return ControllerSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return ControllerSnapshot{}, err
	}
	return result, nil
}

func (s *ControllerStore) Backup(ctx context.Context, destination string) error {
	return s.backup(ctx, destination)
}
func RestoreController(ctx context.Context, destination, backup string) error {
	return restore(ctx, destination, backup, "controller")
}
