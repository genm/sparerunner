package reconcile

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/scheduler"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

// ControllerAuthority is the exact durable startup boundary. AdvanceEpoch must
// commit before Snapshot is read; a mismatch is never treated as an empty fleet.
type ControllerAuthority interface {
	AdvanceEpoch(context.Context) (domain.ControllerEpoch, error)
	Snapshot(context.Context) (store.ControllerSnapshot, error)
}

type NodeDefinition struct {
	Node                 domain.Node
	PlatformUnknown      bool
	AvailableMemoryBytes uint64
	CachedRunnerPackages []string
	RunnerVersionPolicy  domain.RunnerVersionPolicy
	RunnerUpdate         RunnerUpdateStatus
}

// NodeConfiguration is the live, operator-controlled subset of node topology.
// Credential, platform, observation, and cleanup authority remain untouched.
type NodeConfiguration struct {
	NodeID      domain.NodeID
	DisplayName string
	MaxRunners  int
}

type IssuedCommand struct {
	NodeID  domain.NodeID
	Type    domain.CommandType
	Command domain.Command
}

// GitHubFence represents a durable provider-side ambiguity. The reservation
// remains owned, but it is suppressed from advertised capacity until a later
// Agent/GitHub observation is durably reconciled.
type GitHubFence struct {
	ExecutionID     domain.ExecutionID
	ScaleSetID      store.ScaleSetID
	RunnerRequestID int64
	ClaimState      store.GitHubClaimState
	Attempt         *store.GitHubJITAttempt
}

type Config struct {
	Nodes        []NodeDefinition
	Commands     []IssuedCommand
	GitHubFences []GitHubFence
	Now          func() time.Time
}

type NodePhase string

const (
	NodeOffline     NodePhase = "offline"
	NodeReconciling NodePhase = "reconciling"
	NodeReady       NodePhase = "ready"
	NodeDraining    NodePhase = "draining"
	NodeQuarantined NodePhase = "quarantined"
	NodeRevoked     NodePhase = "revoked"
	NodeDegraded    NodePhase = "degraded"
)

type NodeReason string

const (
	ReasonNone                    NodeReason = ""
	ReasonAgentOffline            NodeReason = "agent_offline"
	ReasonSnapshotMismatch        NodeReason = "snapshot_mismatch"
	ReasonNativeRunnerUnavailable NodeReason = "native_runner_unavailable"
	ReasonRunnerUpdateUnavailable NodeReason = "runner_update_unavailable"
	ReasonAdministrativeDrain     NodeReason = "administrative_drain"
	ReasonCleanupUncertain        NodeReason = "cleanup_uncertain"
	ReasonCredentialRevoked       NodeReason = "credential_revoked"
)

type NodeStatus struct {
	NodeID       domain.NodeID
	Phase        NodePhase
	Reason       NodeReason
	RunnerUpdate RunnerUpdateStatus
}

type ActionKind string

const (
	ActionReplayCommand             ActionKind = "replay_command"
	ActionIssuePrepare              ActionKind = "issue_prepare"
	ActionAdoptObservation          ActionKind = "adopt_observation"
	ActionInspectAndDestroy         ActionKind = "inspect_and_destroy"
	ActionPersistQuarantine         ActionKind = "persist_quarantine"
	ActionObserveGitHubClaim        ActionKind = "observe_github_claim"
	ActionObserveGitHubRunner       ActionKind = "observe_github_runner"
	ActionConfirmAgentStartAccepted ActionKind = "confirm_agent_start_accepted"
	ActionAwaitAgentObservation     ActionKind = "await_agent_observation"
	ActionFailDesired               ActionKind = "fail_desired"
)

// Action contains only durable identities and safe classifications. Command
// payloads, JIT configuration, filesystem paths, and raw cleanup errors are
// deliberately absent.
type Action struct {
	Kind            ActionKind
	NodeID          domain.NodeID
	ExecutionID     domain.ExecutionID
	CommandID       domain.CommandID
	ControllerEpoch domain.ControllerEpoch
	ExpectedState   domain.ExecutionState
	ObservedState   domain.ExecutionState
	ObservedAtNano  int64
	SnapshotDigest  string
}

type NodeResult struct {
	Scheduler              scheduler.NodeSnapshot
	Status                 NodeStatus
	Actions                []Action
	SuppressedReservations []scheduler.RestoredReservation
}

type FleetSnapshot struct {
	Epoch                  domain.ControllerEpoch
	Nodes                  []scheduler.NodeSnapshot
	Reservations           []scheduler.RestoredReservation
	SuppressedReservations []scheduler.RestoredReservation
	Statuses               []NodeStatus
	Actions                []Action
}

// NodeAdmission is the production capacity/recovery gate derived from one
// atomic reconciliation projection. New capacity is stricter than recovery:
// recovery may execute an already-durable action while the node is
// reconciling, but neither path is allowed for offline, stale, draining,
// quarantined, or revoked nodes.
type NodeAdmission struct {
	Node              scheduler.NodeSnapshot
	Status            NodeStatus
	Actions           []Action
	AllowsNewCapacity bool
	AllowsRecovery    bool
	RecheckAt         time.Time
	Change            <-chan struct{}
}

type nodeRuntime struct {
	scheduler     scheduler.NodeSnapshot
	status        NodeStatus
	runnerPolicy  domain.RunnerVersionPolicy
	platformKnown bool
	actions       []Action
	snapshot      transport.AgentSnapshot
	seen          bool
}

// Controller is an in-memory projection of three authorities: Controller
// desired state, Agent observations, and provider ambiguity fences. It never
// mutates SQLite or an Agent runtime; typed Actions name the explicit owning
// boundary that must be applied before a later snapshot can clear suppression.
type Controller struct {
	applyMu sync.Mutex
	mu      sync.RWMutex

	epoch              domain.ControllerEpoch
	nodes              map[domain.NodeID]nodeRuntime
	executions         map[domain.ExecutionID]domain.ExecutionSnapshot
	executionIDs       []domain.ExecutionID
	reservations       map[domain.ExecutionID]scheduler.RestoredReservation
	commands           map[domain.CommandID]IssuedCommand
	commandByExecution map[domain.ExecutionID][]IssuedCommand
	fences             map[domain.ExecutionID]GitHubFence
	// clearedFences retains only inactive high-water tokens. Without that
	// tombstone, a delayed Apply could resurrect an older ambiguity after its
	// exact clear already completed in this Controller process.
	clearedFences               map[domain.ExecutionID]GitHubFence
	now                         func() time.Time
	change                      chan struct{}
	managementProjectionHealthy bool
}

// Start establishes a new process epoch before reading desired state. No
// Controller is returned if either durable operation fails.
func Start(ctx context.Context, authority ControllerAuthority, config Config) (*Controller, error) {
	if authority == nil {
		return nil, invalid("controller_authority_required", "authority", "must not be nil")
	}
	epoch, err := authority.AdvanceEpoch(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := authority.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.ControllerEpoch != epoch {
		return nil, invalid("controller_epoch_mismatch", "controller_snapshot.controller_epoch", "does not match the committed startup epoch")
	}
	return Restore(epoch, snapshot, config)
}

// Restore constructs the deterministic projection when the caller already owns
// the process epoch (for example, an existing application startup transaction).
// Every node starts offline and every durable reservation starts suppressed.
func Restore(
	epoch domain.ControllerEpoch,
	snapshot store.ControllerSnapshot,
	config Config,
) (*Controller, error) {
	if err := epoch.Validate(); err != nil {
		return nil, err
	}
	if snapshot.ControllerEpoch != epoch {
		return nil, invalid("controller_epoch_mismatch", "controller_snapshot.controller_epoch", "does not match the supplied process epoch")
	}
	controller := &Controller{
		epoch:                       epoch,
		nodes:                       make(map[domain.NodeID]nodeRuntime, len(config.Nodes)),
		executions:                  make(map[domain.ExecutionID]domain.ExecutionSnapshot, len(snapshot.Executions)),
		reservations:                make(map[domain.ExecutionID]scheduler.RestoredReservation, len(snapshot.Reservations)),
		commands:                    make(map[domain.CommandID]IssuedCommand, len(config.Commands)),
		commandByExecution:          make(map[domain.ExecutionID][]IssuedCommand),
		fences:                      make(map[domain.ExecutionID]GitHubFence, len(config.GitHubFences)),
		clearedFences:               make(map[domain.ExecutionID]GitHubFence),
		now:                         config.Now,
		change:                      make(chan struct{}),
		managementProjectionHealthy: true,
	}
	if controller.now == nil {
		controller.now = time.Now
	}
	if err := controller.restoreNodes(snapshot.Nodes, config.Nodes); err != nil {
		return nil, err
	}
	if err := controller.restoreDesired(snapshot.Executions, snapshot.Reservations); err != nil {
		return nil, err
	}
	if err := controller.restoreCommands(config.Commands); err != nil {
		return nil, err
	}
	if err := controller.restoreFences(config.GitHubFences); err != nil {
		return nil, err
	}
	return controller, nil
}

// RestoreRestart converts the store-owned, secret-free restart read model into
// the in-memory projection. Platform evidence is topology only: all nodes still
// begin offline and reconciled capacity remains zero until a fresh Agent
// snapshot is committed and accepted in the new process epoch.
func RestoreRestart(
	snapshot store.ControllerRestartSnapshot,
	now func() time.Time,
) (*Controller, error) {
	config := Config{Now: now}
	config.Nodes = make([]NodeDefinition, 0, len(snapshot.NodeTopology))
	for _, topology := range snapshot.NodeTopology {
		definition, err := restartNodeDefinition(topology)
		if err != nil {
			return nil, err
		}
		config.Nodes = append(config.Nodes, definition)
	}
	config.Commands = make([]IssuedCommand, len(snapshot.IssuedCommands))
	for index, issued := range snapshot.IssuedCommands {
		config.Commands[index] = IssuedCommand{
			NodeID:  issued.NodeID,
			Type:    issued.Type,
			Command: issued.Command,
		}
	}
	config.GitHubFences = make([]GitHubFence, len(snapshot.GitHubFences))
	for index, persisted := range snapshot.GitHubFences {
		config.GitHubFences[index] = GitHubFence{
			ExecutionID:     persisted.Claim.Execution.ID,
			ScaleSetID:      persisted.Claim.ScaleSetID,
			RunnerRequestID: persisted.Claim.RunnerRequestID,
			ClaimState:      persisted.Claim.State,
			Attempt:         persisted.Attempt,
		}
	}
	return Restore(
		snapshot.Controller.ControllerEpoch,
		snapshot.Controller,
		config,
	)
}

func restartNodeDefinition(topology store.RestartNodeTopology) (NodeDefinition, error) {
	if topology.NodeID == "" ||
		topology.DisplayName == "" ||
		topology.CertificateSerial == "" ||
		topology.CredentialEpoch == 0 ||
		topology.MaxRunners < 1 {
		return NodeDefinition{}, invalid("invalid_restart_node_topology", "restart_snapshot.node_topology", "contains incomplete node authority")
	}
	node := domain.Node{
		ID:                  topology.NodeID,
		DisplayName:         topology.DisplayName,
		CertificateSerial:   topology.CertificateSerial,
		CredentialEpoch:     topology.CredentialEpoch,
		MaxRunners:          topology.MaxRunners,
		AdministrativeState: topology.AdministrativeState,
		ObservedState:       domain.NodeOffline,
	}
	if topology.PlatformObserved {
		node.OS = topology.OS
		node.Architecture = topology.Architecture
	}
	return NodeDefinition{
		Node:                node,
		PlatformUnknown:     !topology.PlatformObserved,
		RunnerVersionPolicy: domain.RunnerVersionAutoUpdate,
		RunnerUpdate:        ManagedRunnerUpdate(),
	}, nil
}

// EnsureRestartNode adds a newly enrolled node only from a fresh store-owned
// restart topology read. It is idempotent for the exact same durable identity
// and never infers administrative or credential authority from Agent payloads.
func (controller *Controller) EnsureRestartNode(
	topology store.RestartNodeTopology,
) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	definition, err := restartNodeDefinition(topology)
	if err != nil {
		return err
	}
	if err := validateNodeDefinition(definition); err != nil {
		return err
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if current, exists := controller.nodes[topology.NodeID]; exists {
		node := current.scheduler.Node
		if node.CertificateSerial != topology.CertificateSerial ||
			node.DisplayName != topology.DisplayName ||
			node.CredentialEpoch != topology.CredentialEpoch ||
			node.MaxRunners != topology.MaxRunners ||
			node.AdministrativeState != topology.AdministrativeState ||
			(current.platformKnown &&
				(!topology.PlatformObserved ||
					node.OS != topology.OS ||
					node.Architecture != topology.Architecture)) {
			return invalid("restart_node_authority_mismatch", "restart_snapshot.node_topology", "changed existing node authority")
		}
		return nil
	}
	controller.nodes[topology.NodeID] = runtimeForNodeDefinition(
		definition,
		topology.AdministrativeState,
	)
	controller.signalChangeLocked()
	return nil
}

func (controller *Controller) restoreNodes(
	administration []store.NodeAdministration,
	definitions []NodeDefinition,
) error {
	adminByNode := make(map[domain.NodeID]domain.NodeAdministrativeState, len(administration))
	for _, node := range administration {
		if strings.TrimSpace(string(node.NodeID)) == "" {
			return invalid("invalid_node_authority", "controller_snapshot.nodes", "contains an empty node ID")
		}
		if err := node.State.Validate("controller_snapshot.node.state"); err != nil {
			return err
		}
		if _, duplicate := adminByNode[node.NodeID]; duplicate {
			return invalid("duplicate_node_authority", "controller_snapshot.nodes", "contains a duplicate node ID")
		}
		adminByNode[node.NodeID] = node.State
	}
	for _, definition := range definitions {
		if err := validateNodeDefinition(definition); err != nil {
			return err
		}
		administrativeState, exists := adminByNode[definition.Node.ID]
		if !exists {
			return invalid("node_definition_not_enrolled", "nodes", "contains a node absent from durable Controller authority")
		}
		if _, duplicate := controller.nodes[definition.Node.ID]; duplicate {
			return invalid("duplicate_node_definition", "nodes", "contains a duplicate node ID")
		}
		controller.nodes[definition.Node.ID] = runtimeForNodeDefinition(
			definition,
			administrativeState,
		)
		delete(adminByNode, definition.Node.ID)
	}
	if len(adminByNode) != 0 {
		return invalid("missing_node_definition", "nodes", "does not describe every enrolled Controller node")
	}
	return nil
}

func validateNodeDefinition(definition NodeDefinition) error {
	if definition.PlatformUnknown {
		if strings.TrimSpace(string(definition.Node.ID)) == "" ||
			strings.TrimSpace(definition.Node.DisplayName) == "" ||
			definition.Node.MaxRunners < 1 ||
			definition.Node.OS != "" ||
			definition.Node.Architecture != "" {
			return invalid("invalid_unknown_node_topology", "nodes", "unknown platform definitions require node identity, display name, slots, and no inferred platform")
		}
		if err := definition.Node.AdministrativeState.Validate("node.administrative_state"); err != nil {
			return err
		}
		if err := definition.Node.ObservedState.Validate("node.observed_state"); err != nil {
			return err
		}
	} else if err := definition.Node.Validate(); err != nil {
		return err
	}
	if err := definition.RunnerUpdate.Validate(); err != nil {
		return err
	}
	if !definition.RunnerUpdate.MatchesPolicy(definition.RunnerVersionPolicy) {
		return invalid("runner_update_policy_mismatch", "nodes.runner_update", "does not match the configured runner version policy")
	}
	for _, packageID := range definition.CachedRunnerPackages {
		if strings.TrimSpace(packageID) != packageID || packageID == "" {
			return invalid("invalid_cached_runner_package", "nodes.cached_runner_packages", "must contain non-empty canonical identifiers")
		}
	}
	return nil
}

func runtimeForNodeDefinition(
	definition NodeDefinition,
	administrativeState domain.NodeAdministrativeState,
) nodeRuntime {
	node := definition.Node
	node.AdministrativeState = administrativeState
	node.ObservedState = domain.NodeOffline
	return nodeRuntime{
		scheduler: scheduler.NodeSnapshot{
			Node:                 node,
			AvailableMemoryBytes: definition.AvailableMemoryBytes,
			CachedRunnerPackages: append([]string(nil), definition.CachedRunnerPackages...),
		},
		status: NodeStatus{
			NodeID:       node.ID,
			Phase:        NodeOffline,
			Reason:       ReasonAgentOffline,
			RunnerUpdate: definition.RunnerUpdate,
		},
		runnerPolicy:  definition.RunnerVersionPolicy,
		platformKnown: !definition.PlatformUnknown,
	}
}

func (controller *Controller) restoreDesired(
	executions []domain.ExecutionSnapshot,
	reservations []store.SlotReservation,
) error {
	slotOwners := make(map[domain.SlotKey]domain.ExecutionID, len(reservations))
	for _, reservation := range reservations {
		if reservation.Slot.NodeID == "" || reservation.Slot.Index < 0 ||
			reservation.Owner.ExecutionID == "" {
			return invalid("invalid_slot_reservation", "controller_snapshot.reservations", "contains an incomplete reservation")
		}
		if err := reservation.Owner.Validate(); err != nil {
			return err
		}
		node, exists := controller.nodes[reservation.Slot.NodeID]
		if !exists || reservation.Slot.Index >= node.scheduler.Node.MaxRunners {
			return invalid("reservation_slot_not_configured", "controller_snapshot.reservations", "references a slot outside configured node topology")
		}
		if _, duplicate := slotOwners[reservation.Slot]; duplicate {
			return invalid("duplicate_slot_reservation", "controller_snapshot.reservations", "contains duplicate concrete slot ownership")
		}
		if _, duplicate := controller.reservations[reservation.Owner.ExecutionID]; duplicate {
			return invalid("duplicate_execution_reservation", "controller_snapshot.reservations", "contains duplicate execution ownership")
		}
		slotOwners[reservation.Slot] = reservation.Owner.ExecutionID
		controller.reservations[reservation.Owner.ExecutionID] = scheduler.RestoredReservation{
			TargetID:    reservation.Owner.TargetID,
			Slot:        reservation.Slot,
			ExecutionID: reservation.Owner.ExecutionID,
		}
	}
	for _, execution := range executions {
		if err := execution.Validate(); err != nil {
			return err
		}
		if _, exists := controller.nodes[execution.Slot.NodeID]; !exists {
			return invalid("execution_node_not_configured", "controller_snapshot.executions", "references an unknown node")
		}
		if _, duplicate := controller.executions[execution.ID]; duplicate {
			return invalid("duplicate_execution", "controller_snapshot.executions", "contains a duplicate execution ID")
		}
		controller.executions[execution.ID] = execution
		controller.executionIDs = append(controller.executionIDs, execution.ID)

		reservation, reserved := controller.reservations[execution.ID]
		if terminalExecution(execution.State) {
			if reserved {
				return invalid("terminal_execution_reserved", "controller_snapshot.reservations", "terminal executions must not retain slot ownership")
			}
			continue
		}
		if !reserved || reservation.TargetID != execution.TargetID || reservation.Slot != execution.Slot {
			return invalid("execution_reservation_mismatch", "controller_snapshot.reservations", "every non-terminal execution requires one exact reservation")
		}
	}
	for executionID := range controller.reservations {
		if _, exists := controller.executions[executionID]; !exists {
			return invalid("reservation_execution_missing", "controller_snapshot.reservations", "references an execution absent from desired state")
		}
	}
	sort.Slice(controller.executionIDs, func(i, j int) bool {
		return controller.executionIDs[i] < controller.executionIDs[j]
	})
	return nil
}

func (controller *Controller) restoreCommands(commands []IssuedCommand) error {
	for _, issued := range commands {
		if issued.NodeID == "" {
			return invalid("invalid_issued_command", "commands.node_id", "must not be empty")
		}
		if err := issued.Type.Validate("commands.type"); err != nil {
			return err
		}
		if err := issued.Command.Validate(); err != nil {
			return err
		}
		if issued.Command.ControllerEpoch > controller.epoch {
			return invalid("future_command_epoch", "commands.controller_epoch", "cannot exceed the active Controller epoch")
		}
		execution, exists := controller.executions[issued.Command.ExecutionID]
		if !exists || execution.Slot.NodeID != issued.NodeID {
			return invalid("command_execution_mismatch", "commands.execution_id", "does not reference an execution owned by the command node")
		}
		if !commandTypeMatchesExpectedState(issued.Type, issued.Command.ExpectedState) {
			return invalid("command_expected_state_mismatch", "commands.expected_state", "does not match the command type")
		}
		if _, duplicate := controller.commands[issued.Command.ID]; duplicate {
			return invalid("duplicate_command", "commands.command_id", "must be globally unique")
		}
		controller.commands[issued.Command.ID] = issued
		controller.commandByExecution[issued.Command.ExecutionID] = append(
			controller.commandByExecution[issued.Command.ExecutionID], issued)
	}
	for executionID := range controller.commandByExecution {
		sort.Slice(controller.commandByExecution[executionID], func(i, j int) bool {
			left := controller.commandByExecution[executionID][i]
			right := controller.commandByExecution[executionID][j]
			if left.Command.ControllerEpoch != right.Command.ControllerEpoch {
				return left.Command.ControllerEpoch > right.Command.ControllerEpoch
			}
			return left.Command.ID < right.Command.ID
		})
	}
	return nil
}

func (controller *Controller) restoreFences(fences []GitHubFence) error {
	for _, fence := range fences {
		_, exists := controller.executions[fence.ExecutionID]
		if !exists {
			return invalid("github_fence_execution_mismatch", "github_fences.execution_id", "must reference durable desired execution")
		}
		if _, duplicate := controller.fences[fence.ExecutionID]; duplicate {
			return invalid("duplicate_github_fence", "github_fences.execution_id", "must occur at most once")
		}
		if err := validateGitHubFence(fence); err != nil {
			return err
		}
		fence = cloneGitHubFence(fence)
		controller.fences[fence.ExecutionID] = fence
	}
	return nil
}

func validateGitHubFence(fence GitHubFence) error {
	if fence.ExecutionID == "" || fence.ScaleSetID == 0 ||
		fence.RunnerRequestID <= 0 {
		return invalid("invalid_github_fence", "github_fences.claim_identity", "requires execution, scale-set, and runner-request identity")
	}
	switch fence.ClaimState {
	case store.GitHubClaimAcquireAmbiguous:
		if fence.Attempt != nil {
			return invalid("invalid_github_fence", "github_fences.attempt", "acquire ambiguity must not claim a JIT attempt")
		}
		return nil
	case store.GitHubClaimJITIntent,
		store.GitHubClaimJITGenerationAmbiguous,
		store.GitHubClaimJITGenerated,
		store.GitHubClaimStartDispatching,
		store.GitHubClaimStartAmbiguous,
		store.GitHubClaimReconciliationRequired:
		if fence.Attempt == nil || fence.Attempt.Attempt < 1 ||
			fence.Attempt.ControllerEpoch == 0 || fence.Attempt.RunnerName == "" {
			return invalid("invalid_github_fence", "github_fences.attempt", "JIT ambiguity requires durable attempt identity")
		}
		if fence.Attempt.ScaleSetID != fence.ScaleSetID ||
			fence.Attempt.RunnerRequestID != fence.RunnerRequestID {
			return invalid("github_fence_claim_mismatch", "github_fences.attempt", "does not belong to the durable claim identity")
		}
		if !githubFenceStatesMatch(fence.ClaimState, fence.Attempt.State) {
			return invalid("github_fence_state_mismatch", "github_fences.attempt.state", "does not match the durable claim state")
		}
		if (fence.ClaimState == store.GitHubClaimStartDispatching ||
			fence.ClaimState == store.GitHubClaimStartAmbiguous) &&
			fence.Attempt.StartCommandID == "" {
			return invalid("invalid_github_fence", "github_fences.attempt.start_command_id", "start ambiguity requires exact command identity")
		}
		return nil
	default:
		return invalid("invalid_github_fence", "github_fences.claim_state", "does not represent a recovery-only GitHub state")
	}
}

func githubFenceStatesMatch(
	claim store.GitHubClaimState,
	attempt store.GitHubJITAttemptState,
) bool {
	switch claim {
	case store.GitHubClaimJITIntent:
		return attempt == store.GitHubJITIntent
	case store.GitHubClaimJITGenerationAmbiguous:
		return attempt == store.GitHubJITGenerationAmbiguous
	case store.GitHubClaimJITGenerated:
		return attempt == store.GitHubJITGenerated
	case store.GitHubClaimStartDispatching:
		return attempt == store.GitHubJITStartDispatching
	case store.GitHubClaimStartAmbiguous:
		return attempt == store.GitHubJITStartAmbiguous
	case store.GitHubClaimReconciliationRequired:
		return attempt == store.GitHubJITStarted ||
			attempt == store.GitHubJITAgentAccepted ||
			attempt == store.GitHubJITRemovalPending
	default:
		return false
	}
}

func commandTypeMatchesExpectedState(commandType domain.CommandType, state domain.ExecutionState) bool {
	switch commandType {
	case domain.CommandPrepare:
		return state == domain.ExecutionReserved
	case domain.CommandStart:
		return state == domain.ExecutionPreparing
	case domain.CommandCancel:
		return state == domain.ExecutionPreparing ||
			state == domain.ExecutionRunning ||
			state == domain.ExecutionCleaning
	default:
		return false
	}
}

func (controller *Controller) Epoch() domain.ControllerEpoch {
	if controller == nil {
		return 0
	}
	controller.mu.RLock()
	defer controller.mu.RUnlock()
	return controller.epoch
}

// Change returns a process-local invalidation signal for the current
// reconciliation projection. The channel closes whenever any node admission or
// desired-state projection changes; callers must obtain the next channel after
// it closes.
func (controller *Controller) Change() <-chan struct{} {
	if controller == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	controller.mu.RLock()
	defer controller.mu.RUnlock()
	return controller.change
}

// ManagementProjectionHealthy reports whether the in-memory operator
// projection is still known to match durable SQLite authority. A post-commit
// projection failure makes this process unhealthy until restart; silently
// re-enabling from a partial update could advertise stale capacity.
func (controller *Controller) ManagementProjectionHealthy() bool {
	if controller == nil {
		return false
	}
	controller.mu.RLock()
	defer controller.mu.RUnlock()
	return controller.managementProjectionHealthy
}

// MarkManagementProjectionUnavailable globally suppresses new capacity after a
// durable management mutation cannot be reflected in memory. Recovery actions
// for already-owned executions remain available; a clean Controller restart
// restores the complete projection from SQLite.
func (controller *Controller) MarkManagementProjectionUnavailable() {
	if controller == nil {
		return
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if !controller.managementProjectionHealthy {
		return
	}
	controller.managementProjectionHealthy = false
	controller.signalChangeLocked()
}

func (controller *Controller) signalChangeLocked() {
	close(controller.change)
	controller.change = make(chan struct{})
}

// ReconcileAgentSnapshot must be called only after the authenticated snapshot
// consumer has durably accepted the same snapshot. Rejected input leaves the
// prior last-known node projection untouched.
func (controller *Controller) ReconcileAgentSnapshot(
	snapshot transport.AgentSnapshot,
) (NodeResult, error) {
	if controller == nil {
		return NodeResult{}, invalid("controller_unavailable", "controller", "is nil")
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	return controller.reconcileAgentSnapshot(snapshot)
}

func (controller *Controller) reconcileAgentSnapshot(
	snapshot transport.AgentSnapshot,
) (NodeResult, error) {
	return controller.reconcileAgentSnapshotAt(snapshot, controller.now())
}

func (controller *Controller) reconcileAgentSnapshotAt(
	snapshot transport.AgentSnapshot,
	now time.Time,
) (NodeResult, error) {
	if err := snapshot.Validate(); err != nil {
		return NodeResult{}, invalid("invalid_agent_snapshot", "agent_snapshot", "failed typed validation")
	}
	snapshotDigest, err := transport.AgentSnapshotDigest(snapshot)
	if err != nil {
		return NodeResult{}, invalid("invalid_agent_snapshot_digest", "agent_snapshot", "failed canonical digest")
	}
	if now.IsZero() {
		return NodeResult{}, invalid("runner_update_clock_unavailable", "clock", "returned a zero time")
	}

	controller.mu.Lock()
	defer controller.mu.Unlock()
	current, exists := controller.nodes[snapshot.NodeID]
	if !exists {
		return NodeResult{}, invalid("node_not_found", "agent_snapshot.node_id", "does not identify a configured node")
	}
	if snapshot.MaxControllerEpoch > controller.epoch {
		return NodeResult{}, invalid("future_agent_epoch", "agent_snapshot.max_controller_epoch", "exceeds the active Controller epoch")
	}
	if current.platformKnown &&
		(snapshot.OS != current.scheduler.Node.OS ||
			snapshot.Arch != current.scheduler.Node.Architecture) {
		return NodeResult{}, invalid("node_platform_mismatch", "agent_snapshot", "does not match immutable node platform authority")
	}
	reportedCommands := make(map[domain.CommandID]domain.Command, len(snapshot.Commands))
	for _, command := range snapshot.Commands {
		issued, exists := controller.commands[command.ID]
		if !exists || issued.NodeID != snapshot.NodeID || issued.Command != command {
			return NodeResult{}, invalid("agent_command_authority_mismatch", "agent_snapshot.commands", "contains a command not issued exactly by this Controller")
		}
		reportedCommands[command.ID] = command
	}

	candidate := cloneNodeRuntime(current)
	if !candidate.platformKnown {
		candidate.scheduler.Node.OS = snapshot.OS
		candidate.scheduler.Node.Architecture = snapshot.Arch
		candidate.platformKnown = true
	}
	candidate.snapshot = cloneAgentSnapshot(snapshot)
	candidate.seen = true
	candidate.actions = nil
	candidate.scheduler.Node.ObservedState = domain.NodeOnline
	candidate.scheduler.NativeReady = snapshot.NativeRunnerReady
	// A nil ExcludedTargets is "no change reported" and must keep whatever was
	// last adopted; only a present set replaces it. Degradation paths below zero
	// NativeReady but deliberately leave this alone: excluded stays excluded.
	if snapshot.ExcludedTargets != nil {
		candidate.scheduler.ExcludedTargets = append(
			[]domain.TargetID(nil), snapshot.ExcludedTargets...)
	}
	candidate.scheduler.Reconciled = true
	candidate.scheduler.ActiveExecutions = activeObservationIDs(snapshot.Observations)
	if len(candidate.scheduler.ActiveExecutions) > candidate.scheduler.Node.MaxRunners {
		return NodeResult{}, invalid("active_runners_exceed_node_maximum", "agent_snapshot.observations", "contains more active runtimes than node.maxRunners")
	}

	observations := make(map[domain.ExecutionID]transport.AgentExecutionObservation, len(snapshot.Observations))
	for _, observation := range snapshot.Observations {
		observations[observation.ExecutionID] = observation
	}
	blocked := false
	quarantined := false
	suppress := make(map[domain.ExecutionID]struct{})
	tombstones := make(map[domain.ExecutionID]struct{}, len(snapshot.CleanupTombstones))
	for _, tombstone := range snapshot.CleanupTombstones {
		quarantined = true
		suppress[tombstone.ExecutionID] = struct{}{}
		tombstones[tombstone.ExecutionID] = struct{}{}
		appendUniqueAction(&candidate.actions, Action{
			Kind:        ActionPersistQuarantine,
			NodeID:      snapshot.NodeID,
			ExecutionID: tombstone.ExecutionID,
		})
	}

	for _, observation := range snapshot.Observations {
		if _, known := controller.executions[observation.ExecutionID]; known {
			continue
		}
		if localRuntimeMayExist(observation.State) {
			blocked = true
			appendUniqueAction(&candidate.actions, Action{
				Kind:           ActionInspectAndDestroy,
				NodeID:         snapshot.NodeID,
				ExecutionID:    observation.ExecutionID,
				ObservedState:  observation.State,
				ObservedAtNano: observation.ObservedAtUnixNano,
			})
		}
	}

	for _, executionID := range controller.executionIDs {
		desired := controller.executions[executionID]
		if desired.Slot.NodeID != snapshot.NodeID {
			continue
		}
		observation, observed := observations[executionID]
		fence, ambiguous := controller.fences[executionID]
		if terminalExecution(desired.State) {
			if ambiguous {
				appendUniqueAction(&candidate.actions, githubFenceAction(
					snapshot.NodeID, fence, reportedCommands))
			}
			if observed && localRuntimeMayExist(observation.State) {
				blocked = true
				appendUniqueAction(&candidate.actions, Action{
					Kind:           ActionInspectAndDestroy,
					NodeID:         snapshot.NodeID,
					ExecutionID:    executionID,
					ExpectedState:  desired.State,
					ObservedState:  observation.State,
					ObservedAtNano: observation.ObservedAtUnixNano,
				})
			}
			continue
		}
		if _, cleanupUncertain := tombstones[executionID]; cleanupUncertain {
			continue
		}

		if ambiguous {
			suppress[executionID] = struct{}{}
			appendUniqueAction(&candidate.actions, githubFenceAction(
				snapshot.NodeID, fence, reportedCommands))
		}
		if observed {
			switch observation.State {
			case domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
				quarantined = true
				suppress[executionID] = struct{}{}
				appendUniqueAction(&candidate.actions, Action{
					Kind:        ActionPersistQuarantine,
					NodeID:      snapshot.NodeID,
					ExecutionID: executionID,
				})
				continue
			}
			switch {
			case observation.State == desired.State:
				if desired.State == domain.ExecutionPending ||
					desired.State == domain.ExecutionReserved {
					blocked = true
					suppress[executionID] = struct{}{}
					appendUniqueAction(&candidate.actions, Action{
						Kind:           ActionInspectAndDestroy,
						NodeID:         snapshot.NodeID,
						ExecutionID:    executionID,
						ExpectedState:  desired.State,
						ObservedState:  observation.State,
						ObservedAtNano: observation.ObservedAtUnixNano,
					})
				}
			case domain.CanReachExecutionState(desired.State, observation.State):
				// The Agent journal is observed authority. Persisting the
				// adoption remains explicit, while exact reservation ownership
				// prevents another runtime from using this slot.
				suppress[executionID] = struct{}{}
				appendUniqueAction(&candidate.actions, Action{
					Kind:           ActionAdoptObservation,
					NodeID:         snapshot.NodeID,
					ExecutionID:    executionID,
					ExpectedState:  desired.State,
					ObservedState:  observation.State,
					ObservedAtNano: observation.ObservedAtUnixNano,
				})
			case domain.CanReachExecutionState(observation.State, desired.State):
				suppress[executionID] = struct{}{}
				if ambiguous {
					// Provider ambiguity is the stronger fence. Replaying a
					// command here could regenerate JIT or race a runtime whose
					// provider-side existence has not been established.
					blocked = true
				} else {
					action, safeReplay := controller.recoveryAction(
						snapshot.NodeID, desired)
					appendUniqueAction(&candidate.actions, action)
					blocked = blocked || !safeReplay
				}
			default:
				blocked = true
				suppress[executionID] = struct{}{}
				appendUniqueAction(&candidate.actions, Action{
					Kind:           ActionInspectAndDestroy,
					NodeID:         snapshot.NodeID,
					ExecutionID:    executionID,
					ExpectedState:  desired.State,
					ObservedState:  observation.State,
					ObservedAtNano: observation.ObservedAtUnixNano,
				})
			}
			continue
		}

		suppress[executionID] = struct{}{}
		if ambiguous {
			// A fresh Agent snapshot without the execution is necessary but not
			// sufficient evidence: observe/remove the GitHub runner first.
			continue
		}
		switch desired.State {
		case domain.ExecutionReserved, domain.ExecutionPreparing:
			action, safeReplay := controller.recoveryAction(
				snapshot.NodeID, desired)
			appendUniqueAction(&candidate.actions, action)
			blocked = blocked || !safeReplay
		case domain.ExecutionCleanupFailed:
			quarantined = true
			appendUniqueAction(&candidate.actions, Action{
				Kind:        ActionPersistQuarantine,
				NodeID:      snapshot.NodeID,
				ExecutionID: executionID,
			})
		case domain.ExecutionRunning, domain.ExecutionCleaning:
			blocked = true
			appendUniqueAction(&candidate.actions, Action{
				Kind:          ActionInspectAndDestroy,
				NodeID:        snapshot.NodeID,
				ExecutionID:   executionID,
				ExpectedState: desired.State,
			})
		default:
			blocked = true
			appendUniqueAction(&candidate.actions, Action{
				Kind:          ActionFailDesired,
				NodeID:        snapshot.NodeID,
				ExecutionID:   executionID,
				ExpectedState: desired.State,
			})
		}
	}

	if quarantined {
		candidate.scheduler.Node.AdministrativeState = domain.NodeQuarantined
		candidate.scheduler.Reconciled = false
		candidate.scheduler.NativeReady = false
		candidate.status.Phase = NodeQuarantined
		candidate.status.Reason = ReasonCleanupUncertain
	} else if candidate.scheduler.Node.AdministrativeState == domain.NodeRevoked {
		candidate.scheduler.Reconciled = false
		candidate.scheduler.NativeReady = false
		candidate.status.Phase = NodeRevoked
		candidate.status.Reason = ReasonCredentialRevoked
	} else if !candidate.status.RunnerUpdate.AllowsAdmissionAt(now) {
		candidate.scheduler.Node.ObservedState = domain.NodeStale
		candidate.scheduler.Reconciled = false
		candidate.scheduler.NativeReady = false
		candidate.status.Phase = NodeDegraded
		candidate.status.Reason = ReasonRunnerUpdateUnavailable
	} else if !snapshot.NativeRunnerReady {
		candidate.scheduler.Node.ObservedState = domain.NodeStale
		candidate.scheduler.Reconciled = false
		candidate.scheduler.NativeReady = false
		candidate.status.Phase = NodeDegraded
		candidate.status.Reason = ReasonNativeRunnerUnavailable
	} else if blocked {
		candidate.scheduler.Node.ObservedState = domain.NodeReconciling
		candidate.scheduler.Reconciled = false
		candidate.scheduler.NativeReady = false
		candidate.status.Phase = NodeReconciling
		candidate.status.Reason = ReasonSnapshotMismatch
	} else if candidate.scheduler.Node.AdministrativeState == domain.NodeDraining {
		candidate.status.Phase = NodeDraining
		candidate.status.Reason = ReasonAdministrativeDrain
	} else {
		candidate.status.Phase = NodeReady
		candidate.status.Reason = ReasonNone
		if len(candidate.actions) > 0 {
			candidate.status.Phase = NodeReconciling
		}
	}

	for index := range candidate.actions {
		candidate.actions[index].SnapshotDigest = snapshotDigest
	}
	sortActions(candidate.actions)
	controller.nodes[snapshot.NodeID] = candidate
	controller.signalChangeLocked()
	return controller.nodeResultLocked(snapshot.NodeID, suppress, now), nil
}

func (controller *Controller) recoveryAction(
	nodeID domain.NodeID,
	execution domain.ExecutionSnapshot,
) (Action, bool) {
	commands := controller.commandByExecution[execution.ID]
	for _, issued := range commands {
		// Prepare is secret-free and exact-replay safe. Start carries one-shot
		// JIT material that is intentionally absent after restart, so its durable
		// identity may be confirmed from Agent authority but never replayed.
		if issued.Type != domain.CommandPrepare {
			continue
		}
		if issued.Command.ExpectedState != execution.State &&
			!(issued.Type == domain.CommandPrepare &&
				execution.State == domain.ExecutionPreparing &&
				issued.Command.ExpectedState == domain.ExecutionReserved) {
			continue
		}
		// Replay the exact durable identity. If the fresh Agent journal contains
		// it this resumes accepted work; if absent, no second command identity is
		// introduced.
		return Action{
			Kind:            ActionReplayCommand,
			NodeID:          nodeID,
			ExecutionID:     execution.ID,
			CommandID:       issued.Command.ID,
			ControllerEpoch: issued.Command.ControllerEpoch,
			ExpectedState:   execution.State,
		}, true
	}
	if execution.State == domain.ExecutionReserved {
		return Action{
			Kind:            ActionIssuePrepare,
			NodeID:          nodeID,
			ExecutionID:     execution.ID,
			ControllerEpoch: controller.epoch,
			ExpectedState:   execution.State,
		}, true
	}
	return Action{
		Kind:          ActionInspectAndDestroy,
		NodeID:        nodeID,
		ExecutionID:   execution.ID,
		ExpectedState: execution.State,
	}, false
}

func githubFenceAction(
	nodeID domain.NodeID,
	fence GitHubFence,
	reported map[domain.CommandID]domain.Command,
) Action {
	action := Action{
		NodeID:      nodeID,
		ExecutionID: fence.ExecutionID,
	}
	if fence.ClaimState == store.GitHubClaimAcquireAmbiguous {
		action.Kind = ActionObserveGitHubClaim
		return action
	}
	if fence.Attempt != nil {
		switch fence.Attempt.State {
		case store.GitHubJITAgentAccepted, store.GitHubJITStarted:
			action.Kind = ActionAwaitAgentObservation
			action.CommandID = fence.Attempt.StartCommandID
			action.ControllerEpoch = fence.Attempt.ControllerEpoch
			return action
		case store.GitHubJITRemovalPending:
			action.Kind = ActionObserveGitHubRunner
			action.CommandID = fence.Attempt.StartCommandID
			action.ControllerEpoch = fence.Attempt.ControllerEpoch
			return action
		}
	}
	if fence.Attempt != nil && fence.Attempt.StartCommandID != "" {
		if _, accepted := reported[fence.Attempt.StartCommandID]; accepted {
			action.Kind = ActionConfirmAgentStartAccepted
			action.CommandID = fence.Attempt.StartCommandID
			action.ControllerEpoch = fence.Attempt.ControllerEpoch
			return action
		}
	}
	action.Kind = ActionObserveGitHubRunner
	if fence.Attempt != nil {
		action.CommandID = fence.Attempt.StartCommandID
		action.ControllerEpoch = fence.Attempt.ControllerEpoch
	}
	return action
}

// Disconnect changes only Controller observation. It emits no cancel or
// destroy action and preserves the last-known active execution list so a local
// Agent remains the runtime and cleanup authority.
func (controller *Controller) Disconnect(nodeID domain.NodeID) (NodeResult, error) {
	if controller == nil {
		return NodeResult{}, invalid("controller_unavailable", "controller", "is nil")
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	now := controller.now()
	controller.mu.Lock()
	defer controller.mu.Unlock()
	node, exists := controller.nodes[nodeID]
	if !exists {
		return NodeResult{}, invalid("node_not_found", "node_id", "does not identify a configured node")
	}
	node.scheduler.Node.ObservedState = domain.NodeOffline
	node.scheduler.Reconciled = false
	node.scheduler.NativeReady = false
	switch node.scheduler.Node.AdministrativeState {
	case domain.NodeQuarantined:
		node.status.Phase = NodeQuarantined
		node.status.Reason = ReasonCleanupUncertain
	case domain.NodeRevoked:
		node.status.Phase = NodeRevoked
		node.status.Reason = ReasonCredentialRevoked
	case domain.NodeDraining:
		node.status.Phase = NodeDraining
		node.status.Reason = ReasonAgentOffline
	default:
		node.status.Phase = NodeOffline
		node.status.Reason = ReasonAgentOffline
	}
	node.actions = nil
	controller.nodes[nodeID] = node
	controller.signalChangeLocked()
	return controller.nodeResultLocked(
		nodeID, controller.allReservationIDsForNode(nodeID), now), nil
}

// HandleAgentDisconnect structurally satisfies the application broker's
// optional lifecycle consumer without importing the composition package.
func (controller *Controller) HandleAgentDisconnect(
	ctx context.Context,
	nodeID domain.NodeID,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := controller.Disconnect(nodeID)
	return err
}

// SetAdministrativeState applies explicit operator authority. Clearing
// quarantine requires prior successful cleanup remediation and always requires
// a fresh Agent snapshot before capacity can return.
func (controller *Controller) SetAdministrativeState(
	nodeID domain.NodeID,
	next domain.NodeAdministrativeState,
	cleanupRemediated bool,
) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if err := next.Validate("node.administrative_state"); err != nil {
		return err
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	defer controller.mu.Unlock()
	node, exists := controller.nodes[nodeID]
	if !exists {
		return invalid("node_not_found", "node_id", "does not identify a configured node")
	}
	current := node.scheduler.Node.AdministrativeState
	if current == domain.NodeRevoked && next != domain.NodeRevoked {
		return invalid("revoked_node_terminal", "node.administrative_state", "revoked nodes cannot be reactivated")
	}
	if current == domain.NodeQuarantined && next == domain.NodeActive && !cleanupRemediated {
		return invalid("cleanup_remediation_required", "node.administrative_state", "quarantine requires explicit successful cleanup remediation")
	}
	node.scheduler.Node.AdministrativeState = next
	node.scheduler.Reconciled = false
	node.scheduler.NativeReady = false
	node.scheduler.Node.ObservedState = domain.NodeReconciling
	node.actions = nil
	node.seen = false
	switch next {
	case domain.NodeQuarantined:
		node.status.Phase = NodeQuarantined
		node.status.Reason = ReasonCleanupUncertain
	case domain.NodeRevoked:
		node.status.Phase = NodeRevoked
		node.status.Reason = ReasonCredentialRevoked
	case domain.NodeDraining:
		node.status.Phase = NodeDraining
		node.status.Reason = ReasonAdministrativeDrain
	default:
		node.status.Phase = NodeReconciling
		node.status.Reason = ReasonSnapshotMismatch
	}
	controller.nodes[nodeID] = node
	controller.signalChangeLocked()
	return nil
}

// ApplyNodeConfigurations replaces the complete operator-controlled node
// configuration atomically in the in-memory projection after the store commits
// the same revision. A shrink that would strand a concrete reservation is
// rejected without mutating any node.
func (controller *Controller) ApplyNodeConfigurations(
	configurations []NodeConfiguration,
) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	defer controller.mu.Unlock()

	if len(configurations) != len(controller.nodes) {
		return invalid(
			"node_configuration_set_mismatch",
			"nodes",
			"must describe every configured node exactly once",
		)
	}
	byNode := make(map[domain.NodeID]NodeConfiguration, len(configurations))
	for _, configuration := range configurations {
		if strings.TrimSpace(string(configuration.NodeID)) == "" ||
			strings.TrimSpace(configuration.DisplayName) == "" ||
			configuration.MaxRunners < 1 {
			return invalid(
				"invalid_node_configuration",
				"nodes",
				"contains an invalid node configuration",
			)
		}
		if _, duplicate := byNode[configuration.NodeID]; duplicate {
			return invalid(
				"duplicate_node_configuration",
				"nodes",
				"contains a duplicate node ID",
			)
		}
		if _, exists := controller.nodes[configuration.NodeID]; !exists {
			return invalid(
				"node_configuration_set_mismatch",
				"nodes",
				"contains a node outside the current projection",
			)
		}
		byNode[configuration.NodeID] = configuration
	}
	for _, reservation := range controller.reservations {
		configuration := byNode[reservation.Slot.NodeID]
		if reservation.Slot.Index >= configuration.MaxRunners {
			return invalid(
				"node_capacity_below_reservation",
				"nodes.max_runners",
				"would strand an existing concrete slot reservation",
			)
		}
	}

	changed := false
	for nodeID, configuration := range byNode {
		node := controller.nodes[nodeID]
		if node.scheduler.Node.DisplayName == configuration.DisplayName &&
			node.scheduler.Node.MaxRunners == configuration.MaxRunners {
			continue
		}
		node.scheduler.Node.DisplayName = configuration.DisplayName
		node.scheduler.Node.MaxRunners = configuration.MaxRunners
		controller.nodes[nodeID] = node
		changed = true
	}
	if changed {
		controller.signalChangeLocked()
	}
	return nil
}

// SetRunnerUpdateStatus applies a newly observed release state without
// restarting the Controller. Degradation is immediate; recovery still requires
// a fresh Agent snapshot so package readiness is re-observed after an update.
func (controller *Controller) SetRunnerUpdateStatus(
	nodeID domain.NodeID,
	status RunnerUpdateStatus,
) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if err := status.Validate(); err != nil {
		return err
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	defer controller.mu.Unlock()
	node, exists := controller.nodes[nodeID]
	if !exists {
		return invalid("node_not_found", "node_id", "does not identify a configured node")
	}
	if !status.MatchesPolicy(node.runnerPolicy) {
		return invalid("runner_update_policy_mismatch", "runner_update", "does not match the configured runner version policy")
	}
	node.status.RunnerUpdate = status
	node.scheduler.Reconciled = false
	node.scheduler.NativeReady = false
	if node.scheduler.Node.ObservedState != domain.NodeOffline {
		node.scheduler.Node.ObservedState = domain.NodeStale
		node.status.Phase = NodeDegraded
		node.status.Reason = ReasonRunnerUpdateUnavailable
	}
	node.seen = false
	controller.nodes[nodeID] = node
	controller.signalChangeLocked()
	return nil
}

func (controller *Controller) FleetSnapshot() FleetSnapshot {
	if controller == nil {
		return FleetSnapshot{}
	}
	now := controller.now()
	controller.mu.RLock()
	defer controller.mu.RUnlock()
	result := FleetSnapshot{Epoch: controller.epoch}
	nodeIDs := make([]domain.NodeID, 0, len(controller.nodes))
	for nodeID := range controller.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })
	for _, nodeID := range nodeIDs {
		node := controller.nodes[nodeID]
		schedulerNode, status := effectiveNode(node, now)
		if node.platformKnown {
			result.Nodes = append(result.Nodes, schedulerNode)
		}
		result.Statuses = append(result.Statuses, status)
		result.Actions = append(result.Actions, cloneActions(node.actions)...)
	}
	result.Reservations = controller.sortedReservationsLocked()
	for _, reservation := range result.Reservations {
		node := controller.nodes[reservation.Slot.NodeID]
		schedulerNode, _ := effectiveNode(node, now)
		if !schedulerNode.Reconciled || !schedulerNode.NativeReady ||
			schedulerNode.Node.AdministrativeState != domain.NodeActive ||
			controller.fences[reservation.ExecutionID].ExecutionID != "" ||
			actionSuppressesExecution(node.actions, reservation.ExecutionID) {
			result.SuppressedReservations = append(result.SuppressedReservations, reservation)
		}
	}
	sortActions(result.Actions)
	return result
}

func (controller *Controller) Admission(
	nodeID domain.NodeID,
) (NodeAdmission, error) {
	if controller == nil {
		return NodeAdmission{}, invalid("controller_unavailable", "controller", "is nil")
	}
	now := controller.now()
	if now.IsZero() {
		return NodeAdmission{}, invalid("runner_update_clock_unavailable", "clock", "returned a zero time")
	}
	controller.mu.RLock()
	defer controller.mu.RUnlock()
	node, exists := controller.nodes[nodeID]
	if !exists {
		return NodeAdmission{}, invalid("node_not_found", "node_id", "does not identify a configured node")
	}
	schedulerNode, status := effectiveNode(node, now)
	transportReady := node.seen &&
		schedulerNode.Node.ObservedState == domain.NodeOnline &&
		schedulerNode.Node.AdministrativeState == domain.NodeActive &&
		schedulerNode.NativeReady &&
		status.RunnerUpdate.AllowsAdmissionAt(now)
	result := NodeAdmission{
		Node:           schedulerNode,
		Status:         status,
		Actions:        cloneActions(node.actions),
		AllowsRecovery: transportReady,
		Change:         controller.change,
	}
	result.AllowsNewCapacity = transportReady &&
		controller.managementProjectionHealthy &&
		schedulerNode.Reconciled &&
		status.Phase == NodeReady &&
		len(result.Actions) == 0 &&
		!controller.nodeHasGitHubFenceLocked(nodeID)
	if status.RunnerUpdate.AllowsAdmissionAt(now) {
		result.RecheckAt = status.RunnerUpdate.FreshUntil
		if status.RunnerUpdate.State == RunnerUpdateDue &&
			status.RunnerUpdate.Deadline.Before(result.RecheckAt) {
			result.RecheckAt = status.RunnerUpdate.Deadline
		}
	}
	return result, nil
}

func (controller *Controller) nodeHasGitHubFenceLocked(
	nodeID domain.NodeID,
) bool {
	for executionID := range controller.fences {
		execution, found := controller.executions[executionID]
		if found && execution.Slot.NodeID == nodeID {
			return true
		}
	}
	return false
}

// ApplyGitHubClaim mirrors one already-committed Controller claim into the
// process projection. A store commit always precedes this call. Exact replay is
// idempotent; any different owner for the same execution or slot fails closed.
func (controller *Controller) ApplyGitHubClaim(claim store.GitHubJobClaim) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if claim.ScaleSetID == 0 || claim.RunnerRequestID <= 0 ||
		claim.SourceMessageID == 0 || claim.State == "" {
		return invalid("invalid_github_claim", "github_claim", "contains incomplete durable identity")
	}
	if err := claim.Execution.Validate(); err != nil {
		return err
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	node, exists := controller.nodes[claim.Execution.Slot.NodeID]
	if !exists || claim.Execution.Slot.Index < 0 ||
		claim.Execution.Slot.Index >= node.scheduler.Node.MaxRunners {
		controller.mu.Unlock()
		return invalid("github_claim_slot_mismatch", "github_claim.execution.slot", "does not reference configured node capacity")
	}
	changed := false
	if existing, found := controller.executions[claim.Execution.ID]; found {
		if existing != claim.Execution {
			controller.mu.Unlock()
			return invalid("github_claim_execution_mismatch", "github_claim.execution", "differs from the retained execution")
		}
	} else {
		for executionID, reservation := range controller.reservations {
			if reservation.Slot == claim.Execution.Slot &&
				executionID != claim.Execution.ID {
				controller.mu.Unlock()
				return invalid("github_claim_slot_conflict", "github_claim.execution.slot", "is already owned by another execution")
			}
		}
		controller.executions[claim.Execution.ID] = claim.Execution
		controller.executionIDs = append(controller.executionIDs, claim.Execution.ID)
		sort.Slice(controller.executionIDs, func(i, j int) bool {
			return controller.executionIDs[i] < controller.executionIDs[j]
		})
		if !terminalExecution(claim.Execution.State) {
			controller.reservations[claim.Execution.ID] = scheduler.RestoredReservation{
				TargetID:    claim.Execution.TargetID,
				Slot:        claim.Execution.Slot,
				ExecutionID: claim.Execution.ID,
			}
		}
		changed = true
	}
	snapshot := cloneAgentSnapshot(node.snapshot)
	seen := node.seen
	if changed {
		controller.signalChangeLocked()
	}
	controller.mu.Unlock()
	if seen {
		_, err := controller.reconcileAgentSnapshot(snapshot)
		return err
	}
	return nil
}

// ApplyIssuedCommand mirrors exact non-secret command authority only after the
// Controller store has committed it. It never reconstructs a payload or JIT
// configuration.
func (controller *Controller) ApplyIssuedCommand(issued IssuedCommand) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if issued.NodeID == "" || issued.Type.Validate("issued_command.type") != nil {
		return invalid("invalid_issued_command", "issued_command", "contains incomplete identity")
	}
	if err := issued.Command.Validate(); err != nil {
		return err
	}
	if !commandTypeMatchesExpectedState(issued.Type, issued.Command.ExpectedState) {
		return invalid("command_expected_state_mismatch", "issued_command.expected_state", "does not match the command type")
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	execution, exists := controller.executions[issued.Command.ExecutionID]
	node := controller.nodes[issued.NodeID]
	if !exists || node.scheduler.Node.ID == "" ||
		execution.Slot.NodeID != issued.NodeID {
		controller.mu.Unlock()
		return invalid("command_execution_mismatch", "issued_command.execution_id", "does not belong to the command node")
	}
	if existing, found := controller.commands[issued.Command.ID]; found {
		controller.mu.Unlock()
		if existing != issued {
			return invalid("duplicate_command", "issued_command.command_id", "was reused with different authority")
		}
		return nil
	}
	controller.commands[issued.Command.ID] = issued
	controller.commandByExecution[issued.Command.ExecutionID] = append(
		controller.commandByExecution[issued.Command.ExecutionID], issued)
	sort.Slice(controller.commandByExecution[issued.Command.ExecutionID], func(i, j int) bool {
		left := controller.commandByExecution[issued.Command.ExecutionID][i]
		right := controller.commandByExecution[issued.Command.ExecutionID][j]
		if left.Command.ControllerEpoch != right.Command.ControllerEpoch {
			return left.Command.ControllerEpoch > right.Command.ControllerEpoch
		}
		return left.Command.ID < right.Command.ID
	})
	snapshot := cloneAgentSnapshot(node.snapshot)
	seen := node.seen
	controller.signalChangeLocked()
	controller.mu.Unlock()
	if seen {
		_, err := controller.reconcileAgentSnapshot(snapshot)
		return err
	}
	return nil
}

// ApplyExecutionUpdate advances the process projection after the store has
// committed the same Agent outbox event. The update is also folded into the
// retained Agent snapshot so subsequent reconciliation cannot forget a live
// runtime merely because the original connection remains open.
func (controller *Controller) ApplyExecutionUpdate(
	update transport.ExecutionUpdate,
) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if err := update.Validate(); err != nil {
		return err
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	now := controller.now()
	if now.IsZero() {
		return invalid("runner_update_clock_unavailable", "clock", "returned a zero time")
	}
	controller.mu.Lock()
	execution, exists := controller.executions[update.ExecutionID]
	currentNode, nodeExists := controller.nodes[update.NodeID]
	if !exists || !nodeExists || execution.Slot.NodeID != update.NodeID {
		controller.mu.Unlock()
		return invalid("execution_update_authority_mismatch", "execution_update", "does not belong to the retained node execution")
	}
	issued, issuedExists := controller.commands[update.CommandID]
	if !issuedExists || issued.NodeID != update.NodeID ||
		issued.Command.ExecutionID != update.ExecutionID {
		controller.mu.Unlock()
		return invalid("execution_update_command_mismatch", "execution_update.command_id", "does not reference exact Controller command authority")
	}
	if execution.State != update.State {
		if !domain.CanReachExecutionState(execution.State, update.State) {
			controller.mu.Unlock()
			return invalid("execution_update_regressed", "execution_update.state", "cannot advance the retained execution")
		}
	}

	// Build a detached candidate and finish every fail-closed check before
	// changing the live projection. The durable store already committed this
	// event, but a projection error must still leave one coherent last-known
	// in-memory state for the caller to recover or restart from.
	node := cloneNodeRuntime(currentNode)
	commandSeen := false
	for _, command := range node.snapshot.Commands {
		if command.ID == issued.Command.ID {
			if command != issued.Command {
				controller.mu.Unlock()
				return invalid("agent_command_authority_mismatch", "execution_update.command_id", "conflicts with retained Agent evidence")
			}
			commandSeen = true
			break
		}
	}
	if !commandSeen {
		node.snapshot.Commands = append(node.snapshot.Commands, issued.Command)
		sort.Slice(node.snapshot.Commands, func(i, j int) bool {
			return node.snapshot.Commands[i].ID < node.snapshot.Commands[j].ID
		})
	}
	if issued.Command.ControllerEpoch > node.snapshot.MaxControllerEpoch {
		node.snapshot.MaxControllerEpoch = issued.Command.ControllerEpoch
	}
	observedAt := now.UnixNano()
	observationFound := false
	for index := range node.snapshot.Observations {
		observation := node.snapshot.Observations[index]
		if observation.ExecutionID != update.ExecutionID {
			continue
		}
		nextObservedAt, timestampErr := monotonicObservationTimestamp(
			observedAt,
			observation.ObservedAtUnixNano,
		)
		if timestampErr != nil {
			controller.mu.Unlock()
			return timestampErr
		}
		observedAt = nextObservedAt
		node.snapshot.Observations[index].State = update.State
		node.snapshot.Observations[index].ObservedAtUnixNano = observedAt
		observationFound = true
		break
	}
	if !observationFound {
		node.snapshot.Observations = append(
			node.snapshot.Observations,
			transport.AgentExecutionObservation{
				ExecutionID:        update.ExecutionID,
				State:              update.State,
				ObservedAtUnixNano: observedAt,
			},
		)
	}
	if node.seen {
		if err := controller.validateFoldedAgentSnapshotLocked(node.snapshot); err != nil {
			controller.mu.Unlock()
			return err
		}
	}

	execution.State = update.State
	controller.executions[update.ExecutionID] = execution
	switch update.State {
	case domain.ExecutionReleased, domain.ExecutionFailed:
		delete(controller.reservations, update.ExecutionID)
	case domain.ExecutionCleanupFailed:
		node.scheduler.Node.AdministrativeState = domain.NodeQuarantined
	case domain.ExecutionQuarantined:
		delete(controller.reservations, update.ExecutionID)
		node.scheduler.Node.AdministrativeState = domain.NodeQuarantined
	}
	controller.nodes[update.NodeID] = node
	snapshot := cloneAgentSnapshot(node.snapshot)
	seen := node.seen
	controller.signalChangeLocked()
	controller.mu.Unlock()
	if seen {
		_, err := controller.reconcileAgentSnapshotAt(snapshot, now)
		return err
	}
	return nil
}

// ApplyAgentReadiness changes only lease-backed scheduling liveness on the
// newest retained process snapshot. It deliberately preserves commands,
// observations, and tombstones folded in by execution updates after the last
// full Agent journal commit.
func (controller *Controller) ApplyAgentReadiness(
	nodeID domain.NodeID,
	ready bool,
) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if nodeID == "" {
		return invalid("node_not_found", "agent_readiness.node_id", "is empty")
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	node, exists := controller.nodes[nodeID]
	if !exists || !node.seen {
		controller.mu.Unlock()
		return invalid("node_not_found", "agent_readiness.node_id", "does not identify an active reconciled node")
	}
	node.snapshot.NativeRunnerReady = ready
	controller.nodes[nodeID] = node
	snapshot := cloneAgentSnapshot(node.snapshot)
	controller.signalChangeLocked()
	controller.mu.Unlock()
	_, err := controller.reconcileAgentSnapshot(snapshot)
	return err
}

// ApplyNodeOwnerState mirrors a durably adopted node-owner availability change
// into the retained process snapshot without replaying the Agent journal. It is
// the mid-session counterpart of the snapshot-carried adoption, so a heartbeat
// exclusion takes effect on placement without waiting for a reconnect.
//
// An empty intent and a nil exclusion set each mean "no change reported".
func (controller *Controller) ApplyNodeOwnerState(
	nodeID domain.NodeID,
	intent domain.AvailabilityIntent,
	exclusions []domain.TargetID,
) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if nodeID == "" {
		return invalid("node_not_found", "node_owner_state.node_id", "is empty")
	}
	if intent != "" && intent.Validate("node_owner_state.availability_intent") != nil {
		return invalid("invalid_availability_intent", "node_owner_state.availability_intent", "is not a known intent")
	}
	for _, targetID := range exclusions {
		if strings.TrimSpace(string(targetID)) == "" {
			return invalid("invalid_excluded_target", "node_owner_state.excluded_targets", "must not contain an empty target identifier")
		}
	}
	if intent == "" && exclusions == nil {
		return nil
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	node, exists := controller.nodes[nodeID]
	if !exists || !node.seen {
		controller.mu.Unlock()
		return invalid("node_not_found", "node_owner_state.node_id", "does not identify an active reconciled node")
	}
	if intent != "" {
		node.snapshot.AvailabilityIntent = intent
	}
	if exclusions != nil {
		node.snapshot.ExcludedTargets = append([]domain.TargetID{}, exclusions...)
	}
	controller.nodes[nodeID] = node
	snapshot := cloneAgentSnapshot(node.snapshot)
	controller.signalChangeLocked()
	controller.mu.Unlock()
	_, err := controller.reconcileAgentSnapshot(snapshot)
	return err
}

// ApplyDesiredExecution mirrors an exact store CAS into the process
// projection. It is used for snapshot adoption; no Agent or provider side
// effect is performed here.
func (controller *Controller) ApplyDesiredExecution(
	next domain.ExecutionSnapshot,
) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if err := next.Validate(); err != nil {
		return err
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	current, exists := controller.executions[next.ID]
	if !exists || current.TargetID != next.TargetID ||
		current.Slot != next.Slot {
		controller.mu.Unlock()
		return invalid("execution_authority_mismatch", "execution", "does not match retained desired identity")
	}
	if current.State != next.State &&
		!domain.CanReachExecutionState(current.State, next.State) {
		controller.mu.Unlock()
		return invalid("execution_update_regressed", "execution.state", "cannot advance retained desired state")
	}
	node := controller.nodes[next.Slot.NodeID]
	if node.scheduler.Node.ID == "" {
		controller.mu.Unlock()
		return invalid("node_not_found", "execution.slot.node_id", "does not identify a retained node")
	}
	controller.executions[next.ID] = next
	if terminalExecution(next.State) {
		delete(controller.reservations, next.ID)
	}
	snapshot := cloneAgentSnapshot(node.snapshot)
	seen := node.seen
	controller.signalChangeLocked()
	controller.mu.Unlock()
	if seen {
		_, err := controller.reconcileAgentSnapshot(snapshot)
		return err
	}
	return nil
}

// ApplyReconciliationExecutionUpdate records local teardown evidence without
// regressing an already-terminal desired execution. The store has already
// verified that the command was a recovery-only Cancel.
func (controller *Controller) ApplyReconciliationExecutionUpdate(
	update transport.ExecutionUpdate,
) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if err := update.Validate(); err != nil {
		return err
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	now := controller.now()
	if now.IsZero() {
		return invalid("runner_update_clock_unavailable", "clock", "returned a zero time")
	}
	controller.mu.Lock()
	execution, exists := controller.executions[update.ExecutionID]
	currentNode, nodeExists := controller.nodes[update.NodeID]
	if !exists || !nodeExists || execution.Slot.NodeID != update.NodeID ||
		!terminalExecution(execution.State) {
		controller.mu.Unlock()
		return invalid("reconciliation_update_authority_mismatch", "execution_update", "does not belong to a terminal desired execution")
	}
	issued, issuedExists := controller.commands[update.CommandID]
	if !issuedExists || issued.NodeID != update.NodeID ||
		issued.Type != domain.CommandCancel ||
		issued.Command.ExecutionID != update.ExecutionID {
		controller.mu.Unlock()
		return invalid("execution_update_command_mismatch", "execution_update.command_id", "does not reference exact reconciliation Cancel authority")
	}
	switch update.State {
	case domain.ExecutionCleaning, domain.ExecutionReleased,
		domain.ExecutionFailed, domain.ExecutionCleanupFailed,
		domain.ExecutionQuarantined:
	default:
		controller.mu.Unlock()
		return invalid("invalid_reconciliation_update", "execution_update.state", "does not report teardown state")
	}

	// Keep all candidate mutations detached until command authority, state
	// monotonicity, and timestamp monotonicity have been validated.
	node := cloneNodeRuntime(currentNode)
	commandSeen := false
	for _, command := range node.snapshot.Commands {
		if command.ID != issued.Command.ID {
			continue
		}
		if command != issued.Command {
			controller.mu.Unlock()
			return invalid("agent_command_authority_mismatch", "execution_update.command_id", "conflicts with retained Agent evidence")
		}
		commandSeen = true
		break
	}
	if !commandSeen {
		node.snapshot.Commands = append(node.snapshot.Commands, issued.Command)
		sort.Slice(node.snapshot.Commands, func(i, j int) bool {
			return node.snapshot.Commands[i].ID < node.snapshot.Commands[j].ID
		})
	}
	if issued.Command.ControllerEpoch > node.snapshot.MaxControllerEpoch {
		node.snapshot.MaxControllerEpoch = issued.Command.ControllerEpoch
	}
	observedAt := now.UnixNano()
	observationFound := false
	for index := range node.snapshot.Observations {
		observation := node.snapshot.Observations[index]
		if observation.ExecutionID != update.ExecutionID {
			continue
		}
		if observation.State != update.State &&
			!domain.CanReachExecutionState(observation.State, update.State) {
			controller.mu.Unlock()
			return invalid("agent_observation_regressed", "execution_update.state", "cannot advance retained Agent observation")
		}
		nextObservedAt, timestampErr := monotonicObservationTimestamp(
			observedAt,
			observation.ObservedAtUnixNano,
		)
		if timestampErr != nil {
			controller.mu.Unlock()
			return timestampErr
		}
		observedAt = nextObservedAt
		node.snapshot.Observations[index].State = update.State
		node.snapshot.Observations[index].ObservedAtUnixNano = observedAt
		observationFound = true
		break
	}
	if !observationFound {
		node.snapshot.Observations = append(
			node.snapshot.Observations,
			transport.AgentExecutionObservation{
				ExecutionID:        update.ExecutionID,
				State:              update.State,
				ObservedAtUnixNano: observedAt,
			},
		)
	}
	if node.seen {
		if err := controller.validateFoldedAgentSnapshotLocked(node.snapshot); err != nil {
			controller.mu.Unlock()
			return err
		}
	}
	if update.State == domain.ExecutionCleanupFailed ||
		update.State == domain.ExecutionQuarantined {
		node.scheduler.Node.AdministrativeState = domain.NodeQuarantined
	}
	controller.nodes[update.NodeID] = node
	snapshot := cloneAgentSnapshot(node.snapshot)
	seen := node.seen
	controller.signalChangeLocked()
	controller.mu.Unlock()
	if seen {
		_, err := controller.reconcileAgentSnapshotAt(snapshot, now)
		return err
	}
	return nil
}

func monotonicObservationTimestamp(now, previous int64) (int64, error) {
	if now > previous {
		return now, nil
	}
	if previous == math.MaxInt64 {
		return 0, invalid(
			"agent_observation_timestamp_exhausted",
			"execution_update.observed_at",
			"cannot advance the retained Agent observation timestamp",
		)
	}
	return previous + 1, nil
}

// validateFoldedAgentSnapshotLocked mirrors every rejecting precondition in
// reconcileAgentSnapshotAt. Callers can therefore commit a detached candidate
// knowing that the subsequent projection fold cannot fail after partial state
// has become visible.
func (controller *Controller) validateFoldedAgentSnapshotLocked(
	snapshot transport.AgentSnapshot,
) error {
	if err := snapshot.Validate(); err != nil {
		return invalid("invalid_agent_snapshot", "agent_snapshot", "failed typed validation")
	}
	if _, err := transport.AgentSnapshotDigest(snapshot); err != nil {
		return invalid("invalid_agent_snapshot_digest", "agent_snapshot", "failed canonical digest")
	}
	node, exists := controller.nodes[snapshot.NodeID]
	if !exists {
		return invalid("node_not_found", "agent_snapshot.node_id", "does not identify a configured node")
	}
	if snapshot.MaxControllerEpoch > controller.epoch {
		return invalid("future_agent_epoch", "agent_snapshot.max_controller_epoch", "exceeds the active Controller epoch")
	}
	if node.platformKnown &&
		(snapshot.OS != node.scheduler.Node.OS ||
			snapshot.Arch != node.scheduler.Node.Architecture) {
		return invalid("node_platform_mismatch", "agent_snapshot", "does not match immutable node platform authority")
	}
	for _, command := range snapshot.Commands {
		issued, exists := controller.commands[command.ID]
		if !exists || issued.NodeID != snapshot.NodeID ||
			issued.Command != command {
			return invalid("agent_command_authority_mismatch", "agent_snapshot.commands", "contains a command not issued exactly by this Controller")
		}
	}
	if len(activeObservationIDs(snapshot.Observations)) >
		node.scheduler.Node.MaxRunners {
		return invalid("active_runners_exceed_node_maximum", "agent_snapshot.observations", "contains more active runtimes than node.maxRunners")
	}
	return nil
}

// ApplyGitHubFence mirrors a durable provider fence into the process
// projection. Exact active replay is idempotent. A different token must be a
// valid durable state-machine advance; delayed or conflicting writers fail
// closed and leave the newer projection untouched.
func (controller *Controller) ApplyGitHubFence(fence GitHubFence) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if err := validateGitHubFence(fence); err != nil {
		return err
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	execution, exists := controller.executions[fence.ExecutionID]
	node := controller.nodes[execution.Slot.NodeID]
	if !exists || node.scheduler.Node.ID == "" {
		controller.mu.Unlock()
		return invalid("github_fence_execution_mismatch", "github_fence.execution_id", "does not reference retained desired state")
	}
	if current, found := controller.fences[fence.ExecutionID]; found {
		if githubFencesEqual(current, fence) {
			controller.mu.Unlock()
			return nil
		}
		if !githubFenceCanAdvance(current, fence) {
			controller.mu.Unlock()
			return invalid("github_fence_update_regressed", "github_fence", "does not advance the retained durable fence authority")
		}
	} else if cleared, found := controller.clearedFences[fence.ExecutionID]; found {
		if !githubFenceCanAdvance(cleared, fence) {
			controller.mu.Unlock()
			return invalid("github_fence_update_regressed", "github_fence", "does not advance the retained durable fence authority")
		}
	}
	fence = cloneGitHubFence(fence)
	controller.fences[fence.ExecutionID] = fence
	delete(controller.clearedFences, fence.ExecutionID)
	snapshot := cloneAgentSnapshot(node.snapshot)
	seen := node.seen
	controller.signalChangeLocked()
	controller.mu.Unlock()
	if seen {
		_, err := controller.reconcileAgentSnapshot(snapshot)
		return err
	}
	return nil
}

// ClearGitHubFence removes only the exact full token the caller proved durable.
// Retaining that token as an inactive high-water mark makes exact clear replay
// idempotent while preventing stale clears and delayed Apply resurrection.
func (controller *Controller) ClearGitHubFence(expected GitHubFence) error {
	if controller == nil {
		return invalid("controller_unavailable", "controller", "is nil")
	}
	if err := validateGitHubFence(expected); err != nil {
		return err
	}
	controller.applyMu.Lock()
	defer controller.applyMu.Unlock()
	controller.mu.Lock()
	execution, exists := controller.executions[expected.ExecutionID]
	node := controller.nodes[execution.Slot.NodeID]
	if !exists || node.scheduler.Node.ID == "" {
		controller.mu.Unlock()
		return invalid("github_fence_execution_mismatch", "github_fence.execution_id", "does not reference retained desired state")
	}
	current, found := controller.fences[expected.ExecutionID]
	if !found {
		cleared, clearedBefore := controller.clearedFences[expected.ExecutionID]
		controller.mu.Unlock()
		if !clearedBefore {
			return invalid("github_fence_clear_mismatch", "github_fence", "does not match any active or exactly-cleared durable fence token")
		}
		if !githubFencesEqual(cleared, expected) {
			return invalid("github_fence_clear_mismatch", "github_fence", "does not match the full retained durable fence token")
		}
		return nil
	}
	if !githubFencesEqual(current, expected) {
		controller.mu.Unlock()
		return invalid("github_fence_clear_mismatch", "github_fence", "does not match the full retained durable fence token")
	}
	delete(controller.fences, expected.ExecutionID)
	controller.clearedFences[expected.ExecutionID] = cloneGitHubFence(current)
	snapshot := cloneAgentSnapshot(node.snapshot)
	seen := node.seen
	controller.signalChangeLocked()
	controller.mu.Unlock()
	if seen {
		_, err := controller.reconcileAgentSnapshot(snapshot)
		return err
	}
	return nil
}

func cloneGitHubFence(fence GitHubFence) GitHubFence {
	if fence.Attempt != nil {
		attempt := *fence.Attempt
		fence.Attempt = &attempt
	}
	return fence
}

func githubFencesEqual(left, right GitHubFence) bool {
	return left.ExecutionID == right.ExecutionID &&
		left.ScaleSetID == right.ScaleSetID &&
		left.RunnerRequestID == right.RunnerRequestID &&
		left.ClaimState == right.ClaimState &&
		githubAttemptsEqual(left.Attempt, right.Attempt)
}

func githubFenceCanAdvance(current, next GitHubFence) bool {
	if current.ExecutionID != next.ExecutionID ||
		current.ScaleSetID != next.ScaleSetID ||
		current.RunnerRequestID != next.RunnerRequestID {
		return false
	}
	if current.Attempt == nil || next.Attempt == nil {
		return current.Attempt == nil && next.Attempt != nil
	}
	if next.Attempt.Attempt != current.Attempt.Attempt {
		// A later durable retry is created only after a later Controller epoch
		// reconciles the preceding attempt.
		return next.Attempt.Attempt > current.Attempt.Attempt &&
			next.Attempt.ControllerEpoch > current.Attempt.ControllerEpoch
	}
	if next.Attempt.ControllerEpoch != current.Attempt.ControllerEpoch ||
		next.Attempt.RunnerName != current.Attempt.RunnerName ||
		!githubJITFenceStateCanAdvance(current.Attempt.State, next.Attempt.State) {
		return false
	}
	// Provider material may be filled as the same attempt advances, but an
	// already-durable value is immutable identity and can never be replaced.
	return (current.Attempt.RunnerID == 0 ||
		current.Attempt.RunnerID == next.Attempt.RunnerID) &&
		(current.Attempt.JITDigest == "" ||
			current.Attempt.JITDigest == next.Attempt.JITDigest) &&
		(current.Attempt.StartCommandID == "" ||
			current.Attempt.StartCommandID == next.Attempt.StartCommandID)
}

func githubJITFenceStateCanAdvance(
	current store.GitHubJITAttemptState,
	next store.GitHubJITAttemptState,
) bool {
	switch current {
	case store.GitHubJITIntent:
		return next == store.GitHubJITGenerationAmbiguous ||
			next == store.GitHubJITGenerated ||
			next == store.GitHubJITStartDispatching ||
			next == store.GitHubJITStartAmbiguous ||
			next == store.GitHubJITAgentAccepted ||
			next == store.GitHubJITStarted ||
			next == store.GitHubJITRemovalPending
	case store.GitHubJITGenerationAmbiguous:
		return next == store.GitHubJITRemovalPending
	case store.GitHubJITGenerated:
		return next == store.GitHubJITStartDispatching ||
			next == store.GitHubJITStartAmbiguous ||
			next == store.GitHubJITAgentAccepted ||
			next == store.GitHubJITStarted ||
			next == store.GitHubJITRemovalPending
	case store.GitHubJITStartDispatching:
		return next == store.GitHubJITStartAmbiguous ||
			next == store.GitHubJITAgentAccepted ||
			next == store.GitHubJITStarted ||
			next == store.GitHubJITRemovalPending
	case store.GitHubJITStartAmbiguous:
		return next == store.GitHubJITAgentAccepted ||
			next == store.GitHubJITStarted ||
			next == store.GitHubJITRemovalPending
	case store.GitHubJITAgentAccepted:
		// Agent startup recovery can prove that Start was accepted while also
		// proving the one-shot JIT never reached a runner process. The owning
		// store then advances the exact provider identity to removal_pending;
		// the process projection must preserve that same monotonic recovery
		// path instead of stranding the durable fence.
		return next == store.GitHubJITStarted ||
			next == store.GitHubJITRemovalPending
	case store.GitHubJITStarted:
		// A fresh JobAvailable message without exact pickup evidence turns a
		// terminal local execution into an explicit provider-cleanup intent.
		// Only that exact attempt may advance to removal_pending.
		return next == store.GitHubJITRemovalPending
	default:
		return false
	}
}

func githubAttemptsEqual(left, right *store.GitHubJITAttempt) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func (controller *Controller) nodeResultLocked(
	nodeID domain.NodeID,
	explicitSuppression map[domain.ExecutionID]struct{},
	now time.Time,
) NodeResult {
	node := controller.nodes[nodeID]
	schedulerNode, status := effectiveNode(node, now)
	result := NodeResult{
		Scheduler: schedulerNode,
		Status:    status,
		Actions:   cloneActions(node.actions),
	}
	for _, reservation := range controller.sortedReservationsLocked() {
		if reservation.Slot.NodeID != nodeID {
			continue
		}
		_, explicitlySuppressed := explicitSuppression[reservation.ExecutionID]
		if explicitlySuppressed ||
			!schedulerNode.Reconciled ||
			!schedulerNode.NativeReady ||
			schedulerNode.Node.AdministrativeState != domain.NodeActive ||
			controller.fences[reservation.ExecutionID].ExecutionID != "" ||
			actionSuppressesExecution(node.actions, reservation.ExecutionID) {
			result.SuppressedReservations = append(result.SuppressedReservations, reservation)
		}
	}
	return result
}

func effectiveNode(node nodeRuntime, now time.Time) (scheduler.NodeSnapshot, NodeStatus) {
	schedulerNode := cloneSchedulerNode(node.scheduler)
	status := node.status
	if schedulerNode.Node.ObservedState != domain.NodeOffline &&
		!status.RunnerUpdate.AllowsAdmissionAt(now) {
		schedulerNode.Node.ObservedState = domain.NodeStale
		schedulerNode.Reconciled = false
		schedulerNode.NativeReady = false
		if status.Phase != NodeQuarantined && status.Phase != NodeRevoked {
			status.Phase = NodeDegraded
			status.Reason = ReasonRunnerUpdateUnavailable
		}
	}
	return schedulerNode, status
}

func (controller *Controller) allReservationIDsForNode(
	nodeID domain.NodeID,
) map[domain.ExecutionID]struct{} {
	result := make(map[domain.ExecutionID]struct{})
	for executionID, reservation := range controller.reservations {
		if reservation.Slot.NodeID == nodeID {
			result[executionID] = struct{}{}
		}
	}
	return result
}

func (controller *Controller) sortedReservationsLocked() []scheduler.RestoredReservation {
	result := make([]scheduler.RestoredReservation, 0, len(controller.reservations))
	for _, reservation := range controller.reservations {
		result = append(result, reservation)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Slot.NodeID != result[j].Slot.NodeID {
			return result[i].Slot.NodeID < result[j].Slot.NodeID
		}
		if result[i].Slot.Index != result[j].Slot.Index {
			return result[i].Slot.Index < result[j].Slot.Index
		}
		return result[i].ExecutionID < result[j].ExecutionID
	})
	return result
}

func actionSuppressesExecution(actions []Action, executionID domain.ExecutionID) bool {
	for _, action := range actions {
		if action.ExecutionID == executionID {
			return true
		}
	}
	return false
}

func appendUniqueAction(actions *[]Action, action Action) {
	for _, existing := range *actions {
		if existing == action {
			return
		}
	}
	*actions = append(*actions, action)
}

func sortActions(actions []Action) {
	sort.Slice(actions, func(i, j int) bool {
		if actions[i].NodeID != actions[j].NodeID {
			return actions[i].NodeID < actions[j].NodeID
		}
		if actions[i].ExecutionID != actions[j].ExecutionID {
			return actions[i].ExecutionID < actions[j].ExecutionID
		}
		if actions[i].Kind != actions[j].Kind {
			return actions[i].Kind < actions[j].Kind
		}
		return actions[i].CommandID < actions[j].CommandID
	})
}

func activeObservationIDs(
	observations []transport.AgentExecutionObservation,
) []domain.ExecutionID {
	result := make([]domain.ExecutionID, 0, len(observations))
	for _, observation := range observations {
		if localRuntimeMayExist(observation.State) {
			result = append(result, observation.ExecutionID)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func localRuntimeMayExist(state domain.ExecutionState) bool {
	switch state {
	case domain.ExecutionPreparing,
		domain.ExecutionRunning,
		domain.ExecutionCleaning,
		domain.ExecutionCleanupFailed:
		return true
	default:
		return false
	}
}

func terminalExecution(state domain.ExecutionState) bool {
	switch state {
	case domain.ExecutionReleased, domain.ExecutionFailed, domain.ExecutionQuarantined:
		return true
	default:
		return false
	}
}

func cloneNodeRuntime(node nodeRuntime) nodeRuntime {
	node.scheduler = cloneSchedulerNode(node.scheduler)
	node.actions = cloneActions(node.actions)
	node.snapshot = cloneAgentSnapshot(node.snapshot)
	return node
}

func cloneSchedulerNode(node scheduler.NodeSnapshot) scheduler.NodeSnapshot {
	node.ActiveExecutions = append([]domain.ExecutionID(nil), node.ActiveExecutions...)
	node.CachedRunnerPackages = append([]string(nil), node.CachedRunnerPackages...)
	node.ExcludedTargets = append([]domain.TargetID(nil), node.ExcludedTargets...)
	return node
}

func cloneActions(actions []Action) []Action {
	return append([]Action(nil), actions...)
}

func cloneAgentSnapshot(snapshot transport.AgentSnapshot) transport.AgentSnapshot {
	snapshot.Commands = append([]domain.Command(nil), snapshot.Commands...)
	// Preserves nil, which distinguishes "no change reported" from an empty set.
	snapshot.ExcludedTargets = append(
		[]domain.TargetID(nil), snapshot.ExcludedTargets...)
	snapshot.Observations = append(
		[]transport.AgentExecutionObservation(nil), snapshot.Observations...)
	snapshot.CleanupTombstones = append(
		[]transport.AgentCleanupTombstone(nil), snapshot.CleanupTombstones...)
	return snapshot
}

func (controller *Controller) String() string {
	if controller == nil {
		return "reconcile-controller{nil}"
	}
	controller.mu.RLock()
	defer controller.mu.RUnlock()
	return fmt.Sprintf("reconcile-controller{epoch:%d,nodes:%d,executions:%d}",
		controller.epoch, len(controller.nodes), len(controller.executions))
}
