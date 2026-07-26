package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/genm/tewake/internal/domain"
)

// IssuedAgentCommand is the non-secret Controller authority committed before a
// command is written to an Agent session. PayloadDigest covers the authenticated
// command type and exact payload; the payload body is never accepted here.
type IssuedAgentCommand struct {
	NodeID  domain.NodeID
	Type    domain.CommandType
	Command domain.Command
}

type NodeAgentSnapshot struct {
	NodeID            domain.NodeID
	OS                domain.OperatingSystem
	Architecture      domain.Architecture
	NativeRunnerReady bool
	Journal           AgentSnapshot
}

// AgentExecutionUpdate is the persistence-safe form of one durable Agent outbox
// event. MessageID is the envelope/outbox identity and PayloadDigest binds the
// exact typed update without retaining its JSON representation.
type AgentExecutionUpdate struct {
	NodeID        domain.NodeID
	MessageID     string
	CommandID     domain.CommandID
	ExecutionID   domain.ExecutionID
	State         domain.ExecutionState
	Replayed      bool
	ErrorCode     domain.ExecutionErrorCode
	PayloadDigest string
}

func (s *ControllerStore) CommitAgentCommand(ctx context.Context, issued IssuedAgentCommand) (bool, error) {
	if err := s.requireReady(); err != nil {
		return false, err
	}
	if err := validateIssuedAgentCommand(issued); err != nil {
		return false, err
	}
	issuedAt, err := storeUnixNano(s.now())
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	existing, found, err := loadIssuedAgentCommand(ctx, tx, issued.Command.ID)
	if err != nil {
		return false, err
	}
	if found {
		if existing != issued {
			return false, fmt.Errorf("%w: agent command %s", ErrReplayMismatch, issued.Command.ID)
		}
		return true, nil
	}
	if err := requireActiveEnrolledNode(ctx, tx, issued.NodeID); err != nil {
		return false, err
	}
	currentEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return false, err
	}
	if uint64(issued.Command.ControllerEpoch) != currentEpoch {
		return false, fmt.Errorf("%w: command epoch", ErrStaleControllerEpoch)
	}
	var nodeID string
	var currentState domain.ExecutionState
	if err := tx.QueryRowContext(ctx, `SELECT node_id, state FROM executions WHERE id = ?`, issued.Command.ExecutionID).Scan(&nodeID, &currentState); err != nil {
		return false, err
	}
	if domain.NodeID(nodeID) != issued.NodeID || currentState != issued.Command.ExpectedState {
		return false, errors.New("agent command execution precondition is stale")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO agent_commands(
		command_id, node_id, command_type, controller_epoch, execution_id,
		expected_state, payload_digest, issued_at_unix_nano
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		issued.Command.ID, issued.NodeID, issued.Type, issued.Command.ControllerEpoch,
		issued.Command.ExecutionID, issued.Command.ExpectedState,
		issued.Command.PayloadDigest, issuedAt); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return false, nil
}

func validateIssuedAgentCommand(issued IssuedAgentCommand) error {
	if issued.NodeID == "" {
		return errors.New("agent command requires node ID")
	}
	if err := issued.Type.Validate("agent_command.type"); err != nil {
		return err
	}
	if err := issued.Command.Validate(); err != nil {
		return err
	}
	switch issued.Type {
	case domain.CommandPrepare:
		if issued.Command.ExpectedState != domain.ExecutionReserved {
			return errors.New("prepare command must expect reserved")
		}
	case domain.CommandStart:
		if issued.Command.ExpectedState != domain.ExecutionPreparing {
			return errors.New("start command must expect preparing")
		}
	case domain.CommandCancel:
		switch issued.Command.ExpectedState {
		case domain.ExecutionPreparing, domain.ExecutionRunning, domain.ExecutionCleaning:
		default:
			return errors.New("cancel command must expect an active Agent state")
		}
	}
	return nil
}

type controllerAgentQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadIssuedAgentCommand(ctx context.Context, queryer controllerAgentQueryer, commandID domain.CommandID) (IssuedAgentCommand, bool, error) {
	var issued IssuedAgentCommand
	err := queryer.QueryRowContext(ctx, `SELECT node_id, command_type, command_id,
		controller_epoch, execution_id, expected_state, payload_digest
		FROM agent_commands WHERE command_id = ?`, commandID).Scan(
		&issued.NodeID, &issued.Type, &issued.Command.ID,
		&issued.Command.ControllerEpoch, &issued.Command.ExecutionID,
		&issued.Command.ExpectedState, &issued.Command.PayloadDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return IssuedAgentCommand{}, false, nil
	}
	if err != nil {
		return IssuedAgentCommand{}, false, err
	}
	if err := validateIssuedAgentCommand(issued); err != nil {
		return IssuedAgentCommand{}, false, fmt.Errorf("stored agent command is invalid: %w", err)
	}
	return issued, true, nil
}

func (s *ControllerStore) RecordAgentSnapshot(ctx context.Context, snapshot NodeAgentSnapshot) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if err := validateNodeAgentSnapshot(snapshot); err != nil {
		return err
	}
	receivedAt, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := requireKnownEnrolledNode(ctx, tx, snapshot.NodeID); err != nil {
		return err
	}
	controllerEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return err
	}
	if uint64(snapshot.Journal.MaxControllerEpoch) > controllerEpoch {
		return errors.New("agent snapshot reports an unknown future controller epoch")
	}
	var previousEpoch uint64
	previousErr := tx.QueryRowContext(ctx, `SELECT max_controller_epoch FROM agent_session_snapshots WHERE node_id = ?`, snapshot.NodeID).Scan(&previousEpoch)
	if previousErr != nil && !errors.Is(previousErr, sql.ErrNoRows) {
		return previousErr
	}
	if previousErr == nil && uint64(snapshot.Journal.MaxControllerEpoch) < previousEpoch {
		return errors.New("agent snapshot controller epoch regressed")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO agent_session_snapshots(
		node_id, operating_system, architecture, native_runner_ready,
		max_controller_epoch, received_at_unix_nano
	) VALUES (?, ?, ?, ?, ?, ?)
	ON CONFLICT(node_id) DO UPDATE SET
		operating_system=excluded.operating_system,
		architecture=excluded.architecture,
		native_runner_ready=excluded.native_runner_ready,
		max_controller_epoch=excluded.max_controller_epoch,
		received_at_unix_nano=excluded.received_at_unix_nano`,
		snapshot.NodeID, snapshot.OS, snapshot.Architecture, snapshot.NativeRunnerReady,
		snapshot.Journal.MaxControllerEpoch, receivedAt); err != nil {
		return err
	}
	for _, command := range snapshot.Journal.Commands {
		if err := recordSnapshotCommand(ctx, tx, snapshot.NodeID, command); err != nil {
			return err
		}
	}
	for _, observation := range snapshot.Journal.Observations {
		if err := recordSnapshotObservation(ctx, tx, snapshot.NodeID, observation, receivedAt); err != nil {
			return err
		}
	}
	for _, tombstone := range snapshot.Journal.CleanupTombstones {
		if err := recordSnapshotTombstone(ctx, tx, snapshot.NodeID, tombstone, receivedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func validateNodeAgentSnapshot(snapshot NodeAgentSnapshot) error {
	if snapshot.NodeID == "" {
		return errors.New("agent snapshot requires node ID")
	}
	if err := snapshot.OS.Validate("agent_snapshot.os"); err != nil {
		return err
	}
	if err := snapshot.Architecture.Validate("agent_snapshot.architecture"); err != nil {
		return err
	}
	commandIDs := make(map[domain.CommandID]struct{}, len(snapshot.Journal.Commands))
	for _, command := range snapshot.Journal.Commands {
		if err := command.Validate(); err != nil {
			return err
		}
		if command.ControllerEpoch > snapshot.Journal.MaxControllerEpoch {
			return errors.New("agent snapshot command exceeds its maximum controller epoch")
		}
		if _, duplicate := commandIDs[command.ID]; duplicate {
			return errors.New("agent snapshot repeats a command ID")
		}
		commandIDs[command.ID] = struct{}{}
	}
	observationIDs := make(map[domain.ExecutionID]struct{}, len(snapshot.Journal.Observations))
	for _, observation := range snapshot.Journal.Observations {
		if observation.ExecutionID == "" || observation.ObservedAtUnixNano <= 0 {
			return errors.New("agent snapshot observation is invalid")
		}
		if err := observation.State.Validate("agent_snapshot.observation.state"); err != nil {
			return err
		}
		if _, duplicate := observationIDs[observation.ExecutionID]; duplicate {
			return errors.New("agent snapshot repeats an execution observation")
		}
		observationIDs[observation.ExecutionID] = struct{}{}
	}
	tombstoneIDs := make(map[domain.ExecutionID]struct{}, len(snapshot.Journal.CleanupTombstones))
	for _, tombstone := range snapshot.Journal.CleanupTombstones {
		if tombstone.ExecutionID == "" || tombstone.RecordedAtUnixNano <= 0 {
			return errors.New("agent snapshot cleanup tombstone is invalid")
		}
		if err := tombstone.FailureCode.Validate("agent_snapshot.cleanup_tombstone.failure_code"); err != nil {
			return err
		}
		if _, duplicate := tombstoneIDs[tombstone.ExecutionID]; duplicate {
			return errors.New("agent snapshot repeats a cleanup tombstone")
		}
		tombstoneIDs[tombstone.ExecutionID] = struct{}{}
	}
	return nil
}

func recordSnapshotCommand(ctx context.Context, tx *sql.Tx, nodeID domain.NodeID, command domain.Command) error {
	issued, issuedFound, err := loadIssuedAgentCommand(ctx, tx, command.ID)
	if err != nil {
		return err
	}
	if !issuedFound {
		return errors.New("agent snapshot command was not issued by this controller")
	}
	if issued.NodeID != nodeID || issued.Command != command {
		return fmt.Errorf("%w: agent snapshot command %s differs from controller authority", ErrReplayMismatch, command.ID)
	}
	var existing domain.Command
	err = tx.QueryRowContext(ctx, `SELECT command_id, controller_epoch, execution_id,
		expected_state, payload_digest FROM agent_snapshot_commands
		WHERE node_id = ? AND command_id = ?`, nodeID, command.ID).Scan(
		&existing.ID, &existing.ControllerEpoch, &existing.ExecutionID,
		&existing.ExpectedState, &existing.PayloadDigest)
	if err == nil {
		if existing != command {
			return fmt.Errorf("%w: agent snapshot command %s", ErrReplayMismatch, command.ID)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO agent_snapshot_commands(
		node_id, command_id, controller_epoch, execution_id, expected_state, payload_digest
	) VALUES (?, ?, ?, ?, ?, ?)`, nodeID, command.ID, command.ControllerEpoch,
		command.ExecutionID, command.ExpectedState, command.PayloadDigest)
	return err
}

func recordSnapshotObservation(ctx context.Context, tx *sql.Tx, nodeID domain.NodeID, observation ObservationSnapshot, receivedAt int64) error {
	if err := requireExecutionOwnedByNode(ctx, tx, nodeID, observation.ExecutionID); err != nil {
		return err
	}
	var previousState domain.ExecutionState
	var previousObservedAt int64
	err := tx.QueryRowContext(ctx, `SELECT state, agent_observed_at_unix_nano
		FROM agent_snapshot_observations WHERE node_id = ? AND execution_id = ?`,
		nodeID, observation.ExecutionID).Scan(&previousState, &previousObservedAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && observation.ObservedAtUnixNano < previousObservedAt {
		return errors.New("agent snapshot observation timestamp regressed")
	}
	if err == nil && observation.ObservedAtUnixNano == previousObservedAt && observation.State != previousState {
		return errors.New("agent snapshot changed an observation at the same timestamp")
	}
	if err == nil && !domain.CanReachExecutionState(previousState, observation.State) {
		return errors.New("agent snapshot observation state regressed")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO agent_snapshot_observations(
		node_id, execution_id, state, agent_observed_at_unix_nano, received_at_unix_nano
	) VALUES (?, ?, ?, ?, ?)
	ON CONFLICT(node_id, execution_id) DO UPDATE SET
		state=excluded.state,
		agent_observed_at_unix_nano=excluded.agent_observed_at_unix_nano,
		received_at_unix_nano=excluded.received_at_unix_nano`,
		nodeID, observation.ExecutionID, observation.State,
		observation.ObservedAtUnixNano, receivedAt)
	if err != nil {
		return err
	}
	if observation.State == domain.ExecutionCleanupFailed || observation.State == domain.ExecutionQuarantined {
		return quarantineNode(ctx, tx, nodeID)
	}
	return nil
}

func recordSnapshotTombstone(ctx context.Context, tx *sql.Tx, nodeID domain.NodeID, tombstone CleanupTombstoneSnapshot, receivedAt int64) error {
	if err := requireExecutionOwnedByNode(ctx, tx, nodeID, tombstone.ExecutionID); err != nil {
		return err
	}
	var previousCode domain.CleanupFailureCode
	var previousRecordedAt int64
	err := tx.QueryRowContext(ctx, `SELECT failure_code, agent_recorded_at_unix_nano
		FROM agent_snapshot_cleanup_tombstones WHERE node_id = ? AND execution_id = ?`,
		nodeID, tombstone.ExecutionID).Scan(&previousCode, &previousRecordedAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && tombstone.RecordedAtUnixNano < previousRecordedAt {
		return errors.New("agent cleanup tombstone timestamp regressed")
	}
	if err == nil && tombstone.FailureCode != previousCode {
		return errors.New("agent cleanup tombstone classification changed")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO agent_snapshot_cleanup_tombstones(
		node_id, execution_id, failure_code, agent_recorded_at_unix_nano, received_at_unix_nano
	) VALUES (?, ?, ?, ?, ?)
	ON CONFLICT(node_id, execution_id) DO UPDATE SET
		failure_code=excluded.failure_code,
		agent_recorded_at_unix_nano=excluded.agent_recorded_at_unix_nano,
		received_at_unix_nano=excluded.received_at_unix_nano`,
		nodeID, tombstone.ExecutionID, tombstone.FailureCode,
		tombstone.RecordedAtUnixNano, receivedAt)
	if err != nil {
		return err
	}
	return quarantineNode(ctx, tx, nodeID)
}

func (s *ControllerStore) RecordAgentExecutionUpdate(ctx context.Context, update AgentExecutionUpdate) (bool, error) {
	if err := s.requireReady(); err != nil {
		return false, err
	}
	if err := validateAgentExecutionUpdate(update); err != nil {
		return false, err
	}
	receivedAt, err := storeUnixNano(s.now())
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	existing, found, err := loadAgentExecutionUpdate(ctx, tx, update.NodeID, update.MessageID)
	if err != nil {
		return false, err
	}
	if found {
		if existing != update {
			return false, fmt.Errorf("%w: agent execution update %s", ErrReplayMismatch, update.MessageID)
		}
		return true, nil
	}
	if err := requireKnownEnrolledNode(ctx, tx, update.NodeID); err != nil {
		return false, err
	}
	issued, found, err := loadIssuedAgentCommand(ctx, tx, update.CommandID)
	if err != nil {
		return false, err
	}
	if !found || issued.NodeID != update.NodeID || issued.Command.ExecutionID != update.ExecutionID {
		return false, errors.New("agent execution update does not match an issued command")
	}
	if !agentCommandMayReport(issued.Type, update.State) {
		return false, errors.New("agent execution update state does not match its command type")
	}
	execution, err := loadControllerExecution(ctx, tx, update.ExecutionID)
	if err != nil {
		return false, err
	}
	if execution.Slot.NodeID != update.NodeID {
		return false, errors.New("agent execution update node does not own the desired execution")
	}
	if execution.State != update.State {
		// Agent outbox delivery and reconnect snapshots may omit intermediate
		// observations. Accept only a state reachable through the domain-owned
		// transition graph; a regression or impossible jump still fails closed.
		if !domain.CanReachExecutionState(execution.State, update.State) {
			return false, errors.New("agent execution update state cannot advance desired execution")
		}
		result, err := tx.ExecContext(ctx, `UPDATE executions SET state = ? WHERE id = ? AND state = ?`,
			update.State, update.ExecutionID, execution.State)
		if err != nil {
			return false, err
		}
		affected, err := result.RowsAffected()
		if err != nil || affected != 1 {
			return false, errors.New("agent execution update lost its desired-state compare-and-swap")
		}
	}
	replayed := 0
	if update.Replayed {
		replayed = 1
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO agent_execution_updates(
		node_id, message_id, command_id, execution_id, state, replayed,
		error_code, payload_digest, received_at_unix_nano
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, update.NodeID, update.MessageID,
		update.CommandID, update.ExecutionID, update.State, replayed,
		update.ErrorCode, update.PayloadDigest, receivedAt); err != nil {
		return false, err
	}
	switch update.State {
	case domain.ExecutionReleased, domain.ExecutionFailed:
		// An Agent terminal success or classified command failure is accepted
		// only after its local runtime cleanup boundary has completed. Retain
		// the execution history while atomically returning its active slot.
		result, err := tx.ExecContext(ctx, `DELETE FROM slot_reservations WHERE execution_id = ?`, update.ExecutionID)
		if err != nil {
			return false, err
		}
		affected, err := result.RowsAffected()
		if err != nil || affected != 1 {
			return false, errors.New("agent terminal execution did not release exactly one slot reservation")
		}
	case domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
		// Cleanup uncertainty owns both the slot and the node until an explicit
		// operator/reconciliation flow proves the machine is safe again.
		if err := quarantineNode(ctx, tx, update.NodeID); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return false, nil
}

func agentCommandMayReport(commandType domain.CommandType, state domain.ExecutionState) bool {
	switch commandType {
	case domain.CommandPrepare:
		// An exact Prepare replay reports the execution's current observation,
		// which may have advanced after the original Prepare ACK was lost.
		return domain.CanReachExecutionState(domain.ExecutionPreparing, state)
	case domain.CommandStart:
		switch state {
		case domain.ExecutionRunning, domain.ExecutionCleaning, domain.ExecutionReleased,
			domain.ExecutionFailed, domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
			return true
		}
	case domain.CommandCancel:
		switch state {
		case domain.ExecutionCleaning, domain.ExecutionReleased,
			domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
			return true
		}
	}
	return false
}

func validateAgentExecutionUpdate(update AgentExecutionUpdate) error {
	if update.NodeID == "" || update.MessageID == "" || update.CommandID == "" || update.ExecutionID == "" || !isLowerSHA256(update.PayloadDigest) {
		return errors.New("agent execution update identity is invalid")
	}
	if err := update.State.Validate("agent_execution_update.state"); err != nil {
		return err
	}
	if err := update.ErrorCode.Validate("agent_execution_update.error_code"); err != nil {
		return err
	}
	switch update.State {
	case domain.ExecutionFailed, domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
		if update.ErrorCode == domain.ExecutionErrorNone {
			return errors.New("failed agent execution update requires a classified error")
		}
	default:
		if update.ErrorCode != domain.ExecutionErrorNone {
			return errors.New("non-failed agent execution update cannot carry an error")
		}
	}
	return nil
}

func loadAgentExecutionUpdate(ctx context.Context, queryer controllerAgentQueryer, nodeID domain.NodeID, messageID string) (AgentExecutionUpdate, bool, error) {
	var update AgentExecutionUpdate
	var replayed int
	err := queryer.QueryRowContext(ctx, `SELECT node_id, message_id, command_id,
		execution_id, state, replayed, error_code, payload_digest
		FROM agent_execution_updates WHERE node_id = ? AND message_id = ?`,
		nodeID, messageID).Scan(&update.NodeID, &update.MessageID, &update.CommandID,
		&update.ExecutionID, &update.State, &replayed, &update.ErrorCode,
		&update.PayloadDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentExecutionUpdate{}, false, nil
	}
	if err != nil {
		return AgentExecutionUpdate{}, false, err
	}
	if replayed < 0 || replayed > 1 {
		return AgentExecutionUpdate{}, false, errors.New("stored agent execution update replay flag is invalid")
	}
	update.Replayed = replayed == 1
	if err := validateAgentExecutionUpdate(update); err != nil {
		return AgentExecutionUpdate{}, false, fmt.Errorf("stored agent execution update is invalid: %w", err)
	}
	return update, true, nil
}

func loadControllerExecution(ctx context.Context, queryer controllerAgentQueryer, executionID domain.ExecutionID) (domain.ExecutionSnapshot, error) {
	var result domain.ExecutionSnapshot
	err := queryer.QueryRowContext(ctx, `SELECT id, target_id, node_id, slot_index, state
		FROM executions WHERE id = ?`, executionID).Scan(&result.ID, &result.TargetID,
		&result.Slot.NodeID, &result.Slot.Index, &result.State)
	if err != nil {
		return domain.ExecutionSnapshot{}, err
	}
	if err := result.Validate(); err != nil {
		return domain.ExecutionSnapshot{}, err
	}
	return result, nil
}

func requireActiveEnrolledNode(ctx context.Context, queryer controllerAgentQueryer, nodeID domain.NodeID) error {
	var revoked int
	var state domain.NodeAdministrativeState
	if err := queryer.QueryRowContext(ctx, `SELECT n.revoked, a.administrative_state
		FROM enrolled_nodes n
		JOIN node_administrative_states a ON a.node_id = n.node_id
		WHERE n.node_id = ?`, nodeID).Scan(&revoked, &state); err != nil {
		return err
	}
	if revoked != 0 {
		return errors.New("agent node credential is revoked")
	}
	if state != domain.NodeActive {
		return fmt.Errorf("agent node is not active: %s", state)
	}
	return nil
}

func requireKnownEnrolledNode(ctx context.Context, queryer controllerAgentQueryer, nodeID domain.NodeID) error {
	var revoked int
	var state domain.NodeAdministrativeState
	if err := queryer.QueryRowContext(ctx, `SELECT n.revoked, a.administrative_state
		FROM enrolled_nodes n
		JOIN node_administrative_states a ON a.node_id = n.node_id
		WHERE n.node_id = ?`, nodeID).Scan(&revoked, &state); err != nil {
		return err
	}
	if revoked != 0 || state == domain.NodeRevoked {
		return errors.New("agent node credential is revoked")
	}
	if err := state.Validate("enrolled_node.administrative_state"); err != nil {
		return err
	}
	return nil
}

func requireExecutionOwnedByNode(
	ctx context.Context,
	queryer controllerAgentQueryer,
	nodeID domain.NodeID,
	executionID domain.ExecutionID,
) error {
	var owner domain.NodeID
	if err := queryer.QueryRowContext(ctx,
		`SELECT node_id FROM executions WHERE id = ?`,
		executionID,
	).Scan(&owner); err != nil {
		return err
	}
	if owner != nodeID {
		return errors.New("agent snapshot execution is owned by another node")
	}
	return nil
}

func quarantineNode(ctx context.Context, tx *sql.Tx, nodeID domain.NodeID) error {
	result, err := tx.ExecContext(ctx, `UPDATE node_administrative_states
		SET administrative_state = ?
		WHERE node_id = ? AND administrative_state != ?`,
		domain.NodeQuarantined, nodeID, domain.NodeRevoked)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return errors.New("agent node cannot be quarantined")
	}
	return nil
}
