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

type NodeAdministration struct {
	NodeID domain.NodeID
	State  domain.NodeAdministrativeState
}

type ControllerSnapshot struct {
	ControllerEpoch   domain.ControllerEpoch
	Nodes             []NodeAdministration
	Reservations      []SlotReservation
	Executions        []domain.ExecutionSnapshot
	ProcessedMessages []ProcessedMessage
}

// RestartNodeTopology is the non-secret node authority needed to rebuild the
// Controller projection after a process restart. PlatformObserved distinguishes
// a node that has never completed an authenticated Agent snapshot from one whose
// last-known platform can be used as immutable topology. The last-known ready
// bit remains evidence only; restart admission still begins at capacity zero.
type RestartNodeTopology struct {
	NodeID                     domain.NodeID
	CertificateSerial          string
	CredentialEpoch            uint64
	AdministrativeState        domain.NodeAdministrativeState
	MaxRunners                 int
	PlatformObserved           bool
	OS                         domain.OperatingSystem
	Architecture               domain.Architecture
	LastRunnerVersion          string
	LastNativeRunnerReady      bool
	LastAgentControllerEpoch   domain.ControllerEpoch
	LastSnapshotReceivedAtNano int64
}

// GitHubReconciliationFence is durable provider ambiguity that must be resolved
// before the corresponding slot can be advertised again. Attempt contains only
// runner identity and digests; the JIT body is never persisted or returned.
type GitHubReconciliationFence struct {
	Claim   GitHubJobClaim
	Attempt *GitHubJITAttempt
}

// ControllerRestartSnapshot is one transactionally consistent, secret-free
// restart read model. It deliberately excludes enrollment secrets, GitHub App
// credentials, raw command payloads, JIT configuration, and Agent private keys.
type ControllerRestartSnapshot struct {
	Controller     ControllerSnapshot
	NodeTopology   []RestartNodeTopology
	IssuedCommands []IssuedAgentCommand
	GitHubFences   []GitHubReconciliationFence
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
	createdAt, err := storeUnixNano(s.now())
	if err != nil {
		return domain.ExecutionSnapshot{}, false, err
	}
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO executions(id, target_id, node_id, slot_index, state, created_at_unix_nano) VALUES (?, ?, ?, ?, ?, ?)`, e.ID, e.TargetID, e.Slot.NodeID, e.Slot.Index, e.State, createdAt); err != nil {
		return domain.ExecutionSnapshot{}, false, mapAssignmentError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO slot_reservations(node_id, slot_index, target_id, execution_id) VALUES (?, ?, ?, ?)`, e.Slot.NodeID, e.Slot.Index, e.TargetID, e.ID); err != nil {
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
	// Desired state may have advanced after the message was first committed.
	// Replay identity is the immutable message digest plus execution/slot
	// ownership, so comparing the caller's original Reserved state here would
	// reject a legitimate GitHub redelivery after the Agent started.
	if digest != assignment.MessageDigest ||
		existing.ID != assignment.Execution.ID ||
		existing.TargetID != assignment.Execution.TargetID ||
		existing.Slot != assignment.Execution.Slot {
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
	// The epoch starts at zero after init. Initial join codes are minted for
	// epoch one so the first serve can retain them. Every later process epoch
	// invalidates still-unused codes while preserving issued replay rows.
	if raw > 0 {
		if _, err := tx.ExecContext(ctx, "DELETE FROM enrollment_tokens"); err != nil {
			return 0, err
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE store_metadata SET value = ? WHERE key = 'controller_epoch'", next); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

// EnrollmentEpoch returns the epoch used to mint join codes. Before the first
// controller serve, epoch one is reserved while durable controller_epoch remains
// zero; AdvanceEpoch activates that same epoch.
func (s *ControllerStore) EnrollmentEpoch(ctx context.Context) (domain.ControllerEpoch, error) {
	if err := s.requireReady(); err != nil {
		return 0, err
	}
	raw, err := readUintMetadata(ctx, s.db, "controller_epoch")
	if err != nil {
		return 0, err
	}
	if raw == 0 {
		return 1, nil
	}
	if raw > maxSQLiteInteger {
		return 0, errors.New("controller epoch exceeds SQLite's signed INTEGER range")
	}
	return domain.ControllerEpoch(raw), nil
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
	result, err := readControllerSnapshot(ctx, tx)
	if err != nil {
		return ControllerSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return ControllerSnapshot{}, err
	}
	return result, nil
}

func readControllerSnapshot(ctx context.Context, tx *sql.Tx) (ControllerSnapshot, error) {
	rawEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return ControllerSnapshot{}, err
	}
	result := ControllerSnapshot{ControllerEpoch: domain.ControllerEpoch(rawEpoch)}

	rows, err := tx.QueryContext(ctx, `SELECT n.node_id, a.administrative_state, n.revoked
		FROM enrolled_nodes n
		LEFT JOIN node_administrative_states a ON a.node_id = n.node_id
		ORDER BY n.node_id`)
	if err != nil {
		return ControllerSnapshot{}, err
	}
	for rows.Next() {
		var node NodeAdministration
		var state sql.NullString
		var revoked int
		if err := rows.Scan(&node.NodeID, &state, &revoked); err != nil {
			rows.Close()
			return ControllerSnapshot{}, err
		}
		if node.NodeID == "" || !state.Valid {
			rows.Close()
			return ControllerSnapshot{}, errors.New("stored node administrative authority failed validation")
		}
		node.State = domain.NodeAdministrativeState(state.String)
		if err := node.State.Validate("controller_snapshot.node.administrative_state"); err != nil {
			rows.Close()
			return ControllerSnapshot{}, err
		}
		if (revoked != 0) != (node.State == domain.NodeRevoked) {
			rows.Close()
			return ControllerSnapshot{}, errors.New("stored node revocation authority is inconsistent")
		}
		result.Nodes = append(result.Nodes, node)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ControllerSnapshot{}, err
	}
	if err := rows.Close(); err != nil {
		return ControllerSnapshot{}, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT node_id, slot_index, target_id, execution_id FROM slot_reservations ORDER BY node_id, slot_index`)
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
		reservation := result.Reservations[len(result.Reservations)-1]
		if reservation.Slot.NodeID == "" || reservation.Slot.Index < 0 || reservation.Owner.ExecutionID == "" {
			rows.Close()
			return ControllerSnapshot{}, errors.New("stored slot reservation failed validation")
		}
		if err := reservation.Owner.Validate(); err != nil {
			rows.Close()
			return ControllerSnapshot{}, err
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ControllerSnapshot{}, err
	}
	if err := rows.Close(); err != nil {
		return ControllerSnapshot{}, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT id, target_id, node_id, slot_index, state, created_at_unix_nano FROM executions ORDER BY id`)
	if err != nil {
		return ControllerSnapshot{}, err
	}
	for rows.Next() {
		var id, targetID, nodeID, state string
		var slotIndex int
		var createdAt int64
		if err := rows.Scan(&id, &targetID, &nodeID, &slotIndex, &state, &createdAt); err != nil {
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
		if createdAt <= 0 {
			rows.Close()
			return ControllerSnapshot{}, errors.New("stored execution timestamp failed validation")
		}
		result.Executions = append(result.Executions, execution)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ControllerSnapshot{}, err
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
		if message.ScaleSetID == 0 || message.MessageID == 0 || !isLowerSHA256(message.MessageDigest) || message.ExecutionID == "" || message.CreatedAtUnixNano <= 0 {
			rows.Close()
			return ControllerSnapshot{}, errors.New("stored processed message failed validation")
		}
		result.ProcessedMessages = append(result.ProcessedMessages, message)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ControllerSnapshot{}, err
	}
	if err := rows.Close(); err != nil {
		return ControllerSnapshot{}, err
	}
	return result, nil
}

// RestartSnapshot reads Controller desired state, node topology, exact issued
// command identity, and provider ambiguity under one SQLite read transaction.
// Callers must advance the process epoch before this read and reject a mismatch.
func (s *ControllerStore) RestartSnapshot(ctx context.Context) (ControllerRestartSnapshot, error) {
	if err := s.requireReady(); err != nil {
		return ControllerRestartSnapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ControllerRestartSnapshot{}, err
	}
	defer tx.Rollback()

	controller, err := readControllerSnapshot(ctx, tx)
	if err != nil {
		return ControllerRestartSnapshot{}, err
	}
	nodes, err := readRestartNodeTopology(ctx, tx)
	if err != nil {
		return ControllerRestartSnapshot{}, err
	}
	commands, err := readRestartIssuedCommands(ctx, tx, controller.ControllerEpoch)
	if err != nil {
		return ControllerRestartSnapshot{}, err
	}
	fences, err := readGitHubReconciliationFences(ctx, tx, controller.ControllerEpoch)
	if err != nil {
		return ControllerRestartSnapshot{}, err
	}
	result := ControllerRestartSnapshot{
		Controller:     controller,
		NodeTopology:   nodes,
		IssuedCommands: commands,
		GitHubFences:   fences,
	}
	if err := tx.Commit(); err != nil {
		return ControllerRestartSnapshot{}, err
	}
	return result, nil
}

func readRestartNodeTopology(ctx context.Context, tx *sql.Tx) ([]RestartNodeTopology, error) {
	rows, err := tx.QueryContext(ctx, `SELECT
		n.node_id, n.current_serial, n.credential_epoch, n.revoked,
		a.administrative_state,
		s.operating_system, s.architecture, s.runner_version,
		s.native_runner_ready,
		s.max_controller_epoch, s.received_at_unix_nano
		FROM enrolled_nodes n
		JOIN node_administrative_states a ON a.node_id = n.node_id
		LEFT JOIN agent_session_snapshots s ON s.node_id = n.node_id
		ORDER BY n.node_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []RestartNodeTopology
	for rows.Next() {
		var node RestartNodeTopology
		var revoked int
		var operatingSystem, architecture, runnerVersion sql.NullString
		var nativeRunnerReady, maxControllerEpoch, receivedAt sql.NullInt64
		if err := rows.Scan(
			&node.NodeID,
			&node.CertificateSerial,
			&node.CredentialEpoch,
			&revoked,
			&node.AdministrativeState,
			&operatingSystem,
			&architecture,
			&runnerVersion,
			&nativeRunnerReady,
			&maxControllerEpoch,
			&receivedAt,
		); err != nil {
			return nil, err
		}
		if node.NodeID == "" || node.CertificateSerial == "" || node.CredentialEpoch == 0 {
			return nil, errors.New("stored restart node topology failed validation")
		}
		if err := node.AdministrativeState.Validate("restart_snapshot.node.administrative_state"); err != nil {
			return nil, err
		}
		if (revoked != 0) != (node.AdministrativeState == domain.NodeRevoked) {
			return nil, errors.New("stored restart node revocation authority is inconsistent")
		}
		// Current storage owns one slot per enrolled host. A later settings
		// migration can replace this default without changing the read contract.
		node.MaxRunners = domain.DefaultMaxRunners
		platformColumns := []bool{
			operatingSystem.Valid,
			architecture.Valid,
			runnerVersion.Valid,
			nativeRunnerReady.Valid,
			maxControllerEpoch.Valid,
			receivedAt.Valid,
		}
		for _, valid := range platformColumns[1:] {
			if valid != platformColumns[0] {
				return nil, errors.New("stored restart node platform evidence is incomplete")
			}
		}
		if operatingSystem.Valid {
			node.PlatformObserved = true
			node.OS = domain.OperatingSystem(operatingSystem.String)
			node.Architecture = domain.Architecture(architecture.String)
			node.LastRunnerVersion = runnerVersion.String
			if err := node.OS.Validate("restart_snapshot.node.os"); err != nil {
				return nil, err
			}
			if err := node.Architecture.Validate("restart_snapshot.node.architecture"); err != nil {
				return nil, err
			}
			if node.LastRunnerVersion != "" &&
				(strings.TrimSpace(node.LastRunnerVersion) != node.LastRunnerVersion ||
					len(node.LastRunnerVersion) > 64) {
				return nil, errors.New("stored restart node runner version failed validation")
			}
			if nativeRunnerReady.Int64 < 0 || nativeRunnerReady.Int64 > 1 ||
				maxControllerEpoch.Int64 < 0 || receivedAt.Int64 <= 0 {
				return nil, errors.New("stored restart node observation failed validation")
			}
			node.LastNativeRunnerReady = nativeRunnerReady.Int64 == 1
			node.LastAgentControllerEpoch = domain.ControllerEpoch(maxControllerEpoch.Int64)
			node.LastSnapshotReceivedAtNano = receivedAt.Int64
		}
		result = append(result, node)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func readRestartIssuedCommands(
	ctx context.Context,
	tx *sql.Tx,
	controllerEpoch domain.ControllerEpoch,
) ([]IssuedAgentCommand, error) {
	rows, err := tx.QueryContext(ctx, `SELECT node_id, command_type, command_id,
		controller_epoch, execution_id, expected_state, payload_digest
		FROM agent_commands
		ORDER BY node_id, controller_epoch, command_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []IssuedAgentCommand
	for rows.Next() {
		var issued IssuedAgentCommand
		if err := rows.Scan(
			&issued.NodeID,
			&issued.Type,
			&issued.Command.ID,
			&issued.Command.ControllerEpoch,
			&issued.Command.ExecutionID,
			&issued.Command.ExpectedState,
			&issued.Command.PayloadDigest,
		); err != nil {
			return nil, err
		}
		if err := validateIssuedAgentCommand(issued); err != nil {
			return nil, fmt.Errorf("stored restart command failed validation: %w", err)
		}
		if issued.Command.ControllerEpoch > controllerEpoch {
			return nil, errors.New("stored restart command belongs to a future controller epoch")
		}
		result = append(result, issued)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *ControllerStore) Backup(ctx context.Context, destination string) error {
	return s.backup(ctx, destination)
}
func RestoreController(ctx context.Context, destination, backup string) error {
	return restore(ctx, destination, backup, "controller")
}
