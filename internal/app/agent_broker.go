package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/enroll"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/transport"
)

const DefaultAgentReadinessLease = 4 * time.Second

var (
	ErrAgentOffline                    = errors.New("agent is offline")
	ErrAgentDisconnected               = errors.New("agent disconnected")
	ErrAgentSessionReplaced            = errors.New("agent session was replaced")
	ErrAgentProtocol                   = errors.New("agent protocol violation")
	ErrAgentBrokerClosed               = errors.New("agent broker is closed")
	ErrAgentCommandConsumerRequired    = errors.New("agent command consumer is required")
	ErrAgentSnapshotConsumerRequired   = errors.New("agent snapshot consumer is required")
	ErrAgentReadinessConsumerRequired  = errors.New("agent readiness consumer is required")
	ErrExecutionUpdateConsumerRequired = errors.New("execution update consumer is required")
	ErrAgentOwnerStateConsumerRequired = errors.New("agent owner state consumer is required")
	ErrAgentCommandCommit              = errors.New("agent command commit failed")
	ErrAgentSnapshotCommit             = errors.New("agent snapshot commit failed")
	ErrExecutionUpdateCommit           = errors.New("execution update commit failed")
	ErrAgentDisconnectCommit           = errors.New("agent disconnect commit failed")
	ErrAgentReconciliationStale        = errors.New("agent reconciliation snapshot changed")
)

// AgentCommandRecord is the non-secret, durable identity committed before a
// command reaches the network. PayloadDigest covers kind + NUL + the exact
// encoded payload; the raw payload and JIT configuration never cross this
// boundary.
type AgentCommandRecord struct {
	NodeID         domain.NodeID
	Kind           transport.MessageType
	Metadata       transport.CommandMetadata
	PayloadDigest  [sha256.Size]byte
	Reconciliation bool
	ReplayOnly     bool
	SnapshotDigest string
}

type AgentCommandConsumer interface {
	// HandleAgentCommand must exact-deduplicate CommandID and commit before
	// returning nil. Different node actors may call it concurrently.
	HandleAgentCommand(context.Context, AgentCommandRecord) error
}

type AgentCommandConsumerFunc func(context.Context, AgentCommandRecord) error

func (consumer AgentCommandConsumerFunc) HandleAgentCommand(ctx context.Context, command AgentCommandRecord) error {
	if consumer == nil {
		return ErrAgentCommandConsumerRequired
	}
	return consumer(ctx, command)
}

// AgentSnapshotConsumer owns durable reconciliation of the complete Agent
// journal snapshot. A session is not acknowledged or activated until it accepts
// the snapshot.
type AgentSnapshotConsumer interface {
	// HandleAgentSnapshot must durably reconcile the full typed journal before
	// returning nil. Different node handshakes may call it concurrently.
	HandleAgentSnapshot(context.Context, AgentSnapshot) error
}

type AgentSnapshotConsumerFunc func(context.Context, AgentSnapshot) error

func (consumer AgentSnapshotConsumerFunc) HandleAgentSnapshot(ctx context.Context, snapshot AgentSnapshot) error {
	if consumer == nil {
		return ErrAgentSnapshotConsumerRequired
	}
	return consumer(ctx, snapshot)
}

// AgentReadinessConsumer owns the durable readiness-only compare-and-swap.
// Readiness is lease-backed liveness and must not be persisted by replaying an
// older full Agent journal snapshot.
type AgentReadinessConsumer interface {
	HandleAgentReadiness(context.Context, domain.NodeID, string, bool) error
}

type AgentReadinessConsumerFunc func(context.Context, domain.NodeID, string, bool) error

func (consumer AgentReadinessConsumerFunc) HandleAgentReadiness(
	ctx context.Context,
	nodeID domain.NodeID,
	snapshotDigest string,
	ready bool,
) error {
	if consumer == nil {
		return ErrAgentReadinessConsumerRequired
	}
	return consumer(ctx, nodeID, snapshotDigest, ready)
}

// ExecutionUpdateConsumer is the scheduler/reconciler-owned durable boundary.
// The envelope message ID participates in exact deduplication. The broker
// acknowledges an update only after this callback accepts it. Different node
// actors may call the consumer concurrently.
type AgentExecutionUpdateRecord struct {
	MessageID     string
	Update        transport.ExecutionUpdate
	PayloadDigest [sha256.Size]byte
}

type ExecutionUpdateConsumer interface {
	// HandleExecutionUpdate must exact-deduplicate (NodeID, MessageID), including
	// PayloadDigest, and commit the lifecycle observation before returning nil.
	HandleExecutionUpdate(context.Context, AgentExecutionUpdateRecord) error
}

type ExecutionUpdateConsumerFunc func(context.Context, AgentExecutionUpdateRecord) error

func (consumer ExecutionUpdateConsumerFunc) HandleExecutionUpdate(ctx context.Context, update AgentExecutionUpdateRecord) error {
	if consumer == nil {
		return ErrExecutionUpdateConsumerRequired
	}
	return consumer(ctx, update)
}

// AgentDisconnectRecord binds an offline transition to the exact full Agent
// journal that activated the disappearing authenticated session. The digest
// contains no JIT material or credential.
type AgentDisconnectRecord struct {
	NodeID         domain.NodeID
	SnapshotDigest string
}

// AgentDisconnectConsumer owns the durable readiness revocation and last-known
// Controller projection transition after the current authenticated session
// disappears. It is optional because enrollment-only and test brokers do not
// advertise scheduler capacity.
type AgentDisconnectConsumer interface {
	// HandleAgentDisconnect runs outside the per-node lifecycle lock and must
	// honor context cancellation. The broker lifetime owns the context so
	// Controller shutdown can interrupt a stalled local projection.
	HandleAgentDisconnect(context.Context, AgentDisconnectRecord) error
}

type AgentDisconnectConsumerFunc func(context.Context, AgentDisconnectRecord) error

func (consumer AgentDisconnectConsumerFunc) HandleAgentDisconnect(
	ctx context.Context,
	record AgentDisconnectRecord,
) error {
	if consumer == nil {
		return nil
	}
	return consumer(ctx, record)
}

// Command, snapshot, readiness, and execution-update consumers are mandatory
// for a work-bearing production session; Disconnects is optional for brokers
// without a scheduler projection. A nil mandatory owner fails closed before
// its corresponding ACK or command write. Consumers are commit boundaries and
// must not synchronously call this broker's Send methods.
//
// Eligibility is deliberately not mandatory: it is informational display data,
// not a correctness or security boundary, so its absence degrades a heartbeat
// ack to carrying no eligible-target list rather than failing the session.
//
// OwnerState is optional in the same sense as Disconnects — a broker without a
// durable projection has nothing to adopt into — but it is not best-effort: a
// heartbeat that reports an owner change with no consumer configured fails
// closed rather than silently dropping the change.
type AgentConsumers struct {
	Commands         AgentCommandConsumer
	Snapshot         AgentSnapshotConsumer
	Readiness        AgentReadinessConsumer
	ExecutionUpdates ExecutionUpdateConsumer
	Disconnects      AgentDisconnectConsumer
	Eligibility      AgentEligibilityConsumer
	OwnerState       AgentOwnerStateConsumer
}

// AgentOwnerStateRecord binds one mid-session node-owner availability change to
// the exact full Agent journal that activated this session. Intent "" and a nil
// ExcludedTargets each mean "no change reported"; a non-nil ExcludedTargets is
// the authoritative full set, including an empty one.
type AgentOwnerStateRecord struct {
	NodeID             domain.NodeID
	SnapshotDigest     string
	AvailabilityIntent domain.AvailabilityIntent
	ExcludedTargets    []domain.TargetID
}

// AgentOwnerStateConsumer owns the durable adoption of node-owner availability
// state reported on a heartbeat. It runs under the per-node lifecycle lock
// before the acknowledgement, so a failed adoption fails the heartbeat instead
// of leaving the owner's change unrecorded but acknowledged.
type AgentOwnerStateConsumer interface {
	HandleAgentOwnerState(context.Context, AgentOwnerStateRecord) error
}

type AgentOwnerStateConsumerFunc func(context.Context, AgentOwnerStateRecord) error

func (consumer AgentOwnerStateConsumerFunc) HandleAgentOwnerState(
	ctx context.Context,
	record AgentOwnerStateRecord,
) error {
	if consumer == nil {
		return ErrAgentOwnerStateConsumerRequired
	}
	return consumer(ctx, record)
}

// AgentEligibilityConsumer reports which configured GitHub Targets currently
// match a node's platform. It is read-only display data derived from
// configuration, never a scheduling decision or a capacity guarantee. The node
// identity is required because each entry also reports whether the controller
// has durably adopted that Target as excluded for this node.
type AgentEligibilityConsumer interface {
	EligibleTargets(
		ctx context.Context,
		nodeID domain.NodeID,
		os domain.OperatingSystem,
		architecture domain.Architecture,
	) ([]transport.EligibleTarget, error)
}

type AgentEligibilityConsumerFunc func(
	context.Context, domain.NodeID, domain.OperatingSystem, domain.Architecture,
) ([]transport.EligibleTarget, error)

func (consumer AgentEligibilityConsumerFunc) EligibleTargets(
	ctx context.Context,
	nodeID domain.NodeID,
	os domain.OperatingSystem,
	architecture domain.Architecture,
) ([]transport.EligibleTarget, error) {
	if consumer == nil {
		return nil, nil
	}
	return consumer(ctx, nodeID, os, architecture)
}

// AgentSnapshot is the authenticated, non-secret journal evidence used to
// reconcile and activate a node session.
type AgentSnapshot = transport.AgentSnapshot

// AgentBroker owns one active bidirectional session actor per authenticated
// node. It never stores command payloads or JIT configuration.
type AgentBroker struct {
	mu                    sync.RWMutex
	lifetimeContext       context.Context
	cancelLifetime        context.CancelFunc
	epoch                 domain.ControllerEpoch
	consumers             AgentConsumers
	readinessLease        time.Duration
	sessions              map[domain.NodeID]*agentSessionActor
	offlineChanges        map[domain.NodeID]brokerReadinessChange
	disconnectErrors      map[domain.NodeID]error
	lifecycleLocks        sync.Map
	commandSequences      map[domain.NodeID]uint64
	commandsInFlight      map[domain.NodeID]*agentSessionActor
	snapshotCaptures      map[domain.NodeID]agentSnapshotCapture
	disconnectProjections map[domain.NodeID]*agentSessionActor
	// sessionGeneration allocates connection incarnations; projectedGeneration
	// is the committed high-water mark that prevents a delayed older handshake
	// from becoming authoritative after a replacement disconnects.
	sessionGeneration   map[domain.NodeID]uint64
	projectedGeneration map[domain.NodeID]uint64
	closed              bool
}

type brokerReadinessChange struct {
	context context.Context
	cancel  context.CancelFunc
}

// agentSnapshotCapture binds the interval that starts before Hello is
// acknowledged (and therefore before the Agent builds its snapshot) to the
// command sequence observed at that point. A snapshot whose capture interval
// overlaps any command dispatch is rejected before it can replace durable
// current-journal authority.
type agentSnapshotCapture struct {
	actor              *agentSessionActor
	commandSequence    uint64
	commandWasInFlight bool
}

func NewAgentBroker(epoch domain.ControllerEpoch, consumers AgentConsumers) *AgentBroker {
	return NewAgentBrokerWithOptions(epoch, consumers, AgentBrokerOptions{})
}

type AgentBrokerOptions struct {
	ReadinessLease time.Duration
}

func NewAgentBrokerWithOptions(
	epoch domain.ControllerEpoch,
	consumers AgentConsumers,
	options AgentBrokerOptions,
) *AgentBroker {
	if options.ReadinessLease <= 0 {
		options.ReadinessLease = DefaultAgentReadinessLease
	}
	lifetimeContext, cancelLifetime := context.WithCancel(context.Background())
	return &AgentBroker{
		lifetimeContext:       lifetimeContext,
		cancelLifetime:        cancelLifetime,
		epoch:                 epoch,
		consumers:             consumers,
		readinessLease:        options.ReadinessLease,
		sessions:              make(map[domain.NodeID]*agentSessionActor),
		offlineChanges:        make(map[domain.NodeID]brokerReadinessChange),
		disconnectErrors:      make(map[domain.NodeID]error),
		commandSequences:      make(map[domain.NodeID]uint64),
		commandsInFlight:      make(map[domain.NodeID]*agentSessionActor),
		snapshotCaptures:      make(map[domain.NodeID]agentSnapshotCapture),
		disconnectProjections: make(map[domain.NodeID]*agentSessionActor),
		sessionGeneration:     make(map[domain.NodeID]uint64),
		projectedGeneration:   make(map[domain.NodeID]uint64),
	}
}

func (broker *AgentBroker) String() string {
	return fmt.Sprintf("agent-broker{connected:%d,commands:redacted}", broker.ConnectedCount())
}

func (broker *AgentBroker) GoString() string     { return broker.String() }
func (broker *AgentBroker) LogValue() slog.Value { return slog.StringValue(broker.String()) }
func (broker *AgentBroker) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Connected int `json:"connected"`
	}{Connected: broker.ConnectedCount()})
}

func (broker *AgentBroker) ConnectedCount() int {
	if broker == nil {
		return 0
	}
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	connected := 0
	for _, actor := range broker.sessions {
		actor.stateMu.Lock()
		ready := actor.readyAcked && !actor.terminated
		actor.stateMu.Unlock()
		if ready {
			connected++
		}
	}
	return connected
}

func (broker *AgentBroker) Snapshot(nodeID domain.NodeID) (AgentSnapshot, bool) {
	snapshot, online, _ := broker.Readiness(nodeID)
	return snapshot, online
}

// DisconnectError exposes an optional durable disconnect failure without disguising the
// session as connected. A later successful disconnect or a replacement session
// activation clears the retained error.
func (broker *AgentBroker) DisconnectError(nodeID domain.NodeID) (error, bool) {
	if broker == nil {
		return nil, false
	}
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	err, found := broker.disconnectErrors[nodeID]
	return err, found
}

// Readiness returns one atomic view of the current Agent evidence and a
// state-incarnation context. The context is canceled whenever readiness
// changes, its lease expires, or the authenticated session disconnects.
func (broker *AgentBroker) Readiness(
	nodeID domain.NodeID,
) (AgentSnapshot, bool, context.Context) {
	if broker == nil {
		changed, cancel := context.WithCancel(context.Background())
		cancel()
		return AgentSnapshot{}, false, changed
	}
	broker.mu.Lock()
	if broker.closed {
		broker.mu.Unlock()
		changed, cancel := context.WithCancel(context.Background())
		cancel()
		return AgentSnapshot{}, false, changed
	}
	actor, ok := broker.sessions[nodeID]
	if ok {
		actor.stateMu.Lock()
		ready := actor.readyAcked && !actor.terminated
		if ready {
			snapshot := cloneAgentSnapshot(actor.snapshot)
			changed := actor.readinessContext
			actor.stateMu.Unlock()
			broker.mu.Unlock()
			return snapshot, true, changed
		}
		actor.stateMu.Unlock()
	}
	change, found := broker.offlineChanges[nodeID]
	if !found {
		change.context, change.cancel = context.WithCancel(context.Background())
		broker.offlineChanges[nodeID] = change
	}
	broker.mu.Unlock()
	return AgentSnapshot{}, false, change.context
}

// SendPrepare asks the Agent to materialize the non-secret runner package and
// runtime boundary. Admission of the later JIT-bearing start remains the
// durable coordinator's responsibility; broker memory is not desired state.
func (broker *AgentBroker) SendPrepare(
	ctx context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	disableUpdate bool,
) (transport.ExecutionUpdate, error) {
	actor, err := broker.commandSession(nodeID, metadata)
	if err != nil {
		return transport.ExecutionUpdate{}, err
	}
	if broker.consumers.Commands == nil {
		return transport.ExecutionUpdate{}, ErrAgentCommandConsumerRequired
	}
	if broker.consumers.ExecutionUpdates == nil {
		return transport.ExecutionUpdate{}, ErrExecutionUpdateConsumerRequired
	}
	return actor.sendCommand(ctx, transport.MessagePrepare, metadata, func() (json.RawMessage, error) {
		return transport.EncodePrepareCommandPayload(
			metadata,
			runner.OfficialRunnerVersion,
			disableUpdate,
		)
	})
}

// SendStart writes the secret-bearing payload directly to the active session.
// The temporary JSON buffer is wiped immediately after the synchronous write;
// no broker queue, pending record, or callback receives the JIT body.
//
// A disconnect after Write is ambiguous: the Agent may already have committed
// and started the command. The broker never retries or regenerates JIT material.
// The coordinator must reconcile the Agent command/outbox snapshot before it
// creates another JIT configuration for this execution.
func (broker *AgentBroker) SendStart(
	ctx context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	disableUpdate bool,
	jitConfig runner.JITConfig,
) (transport.ExecutionUpdate, error) {
	if jitConfig == nil {
		return transport.ExecutionUpdate{}, transport.ErrCommandSecret
	}
	actor, err := broker.commandSession(nodeID, metadata)
	if err != nil {
		return transport.ExecutionUpdate{}, err
	}
	if broker.consumers.Commands == nil {
		return transport.ExecutionUpdate{}, ErrAgentCommandConsumerRequired
	}
	if broker.consumers.ExecutionUpdates == nil {
		return transport.ExecutionUpdate{}, ErrExecutionUpdateConsumerRequired
	}
	return actor.sendCommand(ctx, transport.MessageStart, metadata, func() (json.RawMessage, error) {
		var payload json.RawMessage
		delivered := false
		deliverErr := jitConfig.Deliver(func(value string) error {
			if delivered || value == "" || domain.PayloadDigest([]byte(value)) != jitConfig.Digest() {
				return transport.ErrCommandSecret
			}
			delivered = true
			encoded, encodeErr := transport.EncodeStartCommandPayload(
				metadata,
				runner.OfficialRunnerVersion,
				disableUpdate,
				value,
			)
			if encodeErr != nil {
				return encodeErr
			}
			payload = encoded
			return nil
		})
		jitConfig = nil
		if deliverErr != nil || !delivered || len(payload) == 0 {
			clear(payload)
			return nil, transport.ErrCommandSecret
		}
		return payload, nil
	})
}

func (broker *AgentBroker) SendCancel(
	ctx context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
) (transport.ExecutionUpdate, error) {
	actor, err := broker.commandSession(nodeID, metadata)
	if err != nil {
		return transport.ExecutionUpdate{}, err
	}
	if broker.consumers.Commands == nil {
		return transport.ExecutionUpdate{}, ErrAgentCommandConsumerRequired
	}
	if broker.consumers.ExecutionUpdates == nil {
		return transport.ExecutionUpdate{}, ErrExecutionUpdateConsumerRequired
	}
	return actor.sendCommand(ctx, transport.MessageCancel, metadata, func() (json.RawMessage, error) {
		return transport.EncodeCancelCommandPayload(metadata)
	})
}

// ReplayPrepare replays a previously committed secret-free Prepare identity.
// A prior Controller epoch is accepted only at this broker boundary; the
// durable command consumer still requires an exact existing record, so this
// path cannot introduce new old-epoch authority.
func (broker *AgentBroker) ReplayPrepare(
	ctx context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	disableUpdate bool,
	snapshotDigest string,
) (transport.ExecutionUpdate, error) {
	actor, err := broker.recoveryCommandSession(nodeID, metadata)
	if err != nil {
		return transport.ExecutionUpdate{}, err
	}
	if broker.consumers.Commands == nil {
		return transport.ExecutionUpdate{}, ErrAgentCommandConsumerRequired
	}
	if broker.consumers.ExecutionUpdates == nil {
		return transport.ExecutionUpdate{}, ErrExecutionUpdateConsumerRequired
	}
	return actor.sendReplayCommand(
		ctx,
		transport.MessagePrepare,
		metadata,
		snapshotDigest,
		func() (json.RawMessage, error) {
			return transport.EncodePrepareCommandPayload(
				metadata,
				runner.OfficialRunnerVersion,
				disableUpdate,
			)
		},
	)
}

// SendReconciliationCancel commits a recovery-only Cancel authority before it
// reaches the Agent. It is used when desired state is already terminal but a
// fresh Agent snapshot proves that the exact local runtime still exists.
func (broker *AgentBroker) SendReconciliationCancel(
	ctx context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	snapshotDigest string,
) (transport.ExecutionUpdate, error) {
	actor, err := broker.commandSession(nodeID, metadata)
	if err != nil {
		return transport.ExecutionUpdate{}, err
	}
	if broker.consumers.Commands == nil {
		return transport.ExecutionUpdate{}, ErrAgentCommandConsumerRequired
	}
	if broker.consumers.ExecutionUpdates == nil {
		return transport.ExecutionUpdate{}, ErrExecutionUpdateConsumerRequired
	}
	return actor.sendReconciliationCommand(
		ctx,
		transport.MessageCancel,
		metadata,
		snapshotDigest,
		func() (json.RawMessage, error) {
			return transport.EncodeCancelCommandPayload(metadata)
		},
	)
}

func (broker *AgentBroker) commandSession(nodeID domain.NodeID, metadata transport.CommandMetadata) (*agentSessionActor, error) {
	if broker == nil {
		return nil, ErrAgentBrokerClosed
	}
	if nodeID == "" || metadata.ControllerEpoch != broker.epoch {
		return nil, ErrAgentProtocol
	}
	actor, ok := broker.session(nodeID)
	if !ok {
		return nil, ErrAgentOffline
	}
	return actor, nil
}

func (broker *AgentBroker) recoveryCommandSession(
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
) (*agentSessionActor, error) {
	if broker == nil {
		return nil, ErrAgentBrokerClosed
	}
	if nodeID == "" || metadata.ControllerEpoch == 0 ||
		metadata.ControllerEpoch > broker.epoch {
		return nil, ErrAgentProtocol
	}
	actor, ok := broker.session(nodeID)
	if !ok {
		return nil, ErrAgentOffline
	}
	return actor, nil
}

func (broker *AgentBroker) session(nodeID domain.NodeID) (*agentSessionActor, bool) {
	if broker == nil {
		return nil, false
	}
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	if broker.closed {
		return nil, false
	}
	actor, ok := broker.sessions[nodeID]
	if !ok {
		return nil, false
	}
	actor.stateMu.Lock()
	ready := actor.readyAcked && !actor.terminated
	actor.stateMu.Unlock()
	if !ready {
		return nil, false
	}
	return actor, true
}

func (broker *AgentBroker) activate(actor *agentSessionActor) error {
	broker.mu.Lock()
	if broker.closed {
		broker.mu.Unlock()
		return ErrAgentBrokerClosed
	}
	if actor.generation == 0 ||
		broker.projectedGeneration[actor.nodeID] != actor.generation {
		broker.mu.Unlock()
		return ErrAgentSessionReplaced
	}
	previous := broker.sessions[actor.nodeID]
	broker.sessions[actor.nodeID] = actor
	broker.mu.Unlock()
	if previous != nil && previous != actor {
		previous.terminate(ErrAgentSessionReplaced)
	}
	return nil
}

func (broker *AgentBroker) markReady(actor *agentSessionActor) error {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed ||
		broker.sessions[actor.nodeID] != actor ||
		broker.projectedGeneration[actor.nodeID] != actor.generation {
		return ErrAgentSessionReplaced
	}
	actor.stateMu.Lock()
	if actor.terminated {
		actor.stateMu.Unlock()
		return actor.terminalError()
	}
	actor.readyAcked = true
	actor.readyOnce.Do(func() { close(actor.ready) })
	actor.stateMu.Unlock()
	if offline, found := broker.offlineChanges[actor.nodeID]; found {
		offline.cancel()
		delete(broker.offlineChanges, actor.nodeID)
	}
	delete(broker.disconnectErrors, actor.nodeID)
	return nil
}

func (broker *AgentBroker) deactivate(actor *agentSessionActor) {
	lifecycle := broker.nodeLifecycleLock(actor.nodeID)
	lifecycle.Lock()
	projectDisconnect := broker.deactivateUnderLifecycle(actor)
	lifecycle.Unlock()
	if projectDisconnect {
		broker.commitDisconnectProjection(actor)
	}
}

func (broker *AgentBroker) deactivateUnderLifecycle(
	actor *agentSessionActor,
) bool {
	deactivated := false
	broker.mu.Lock()
	if broker.sessions[actor.nodeID] == actor {
		delete(broker.sessions, actor.nodeID)
		deactivated = true
		if !broker.closed {
			changeContext, cancel := context.WithCancel(context.Background())
			broker.offlineChanges[actor.nodeID] = brokerReadinessChange{
				context: changeContext,
				cancel:  cancel,
			}
		}
	}
	broker.mu.Unlock()
	if !deactivated || broker.consumers.Disconnects == nil {
		return false
	}
	return broker.beginDisconnectProjectionUnderLifecycle(actor)
}

func (broker *AgentBroker) beginDisconnectProjectionUnderLifecycle(
	actor *agentSessionActor,
) bool {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if existing := broker.disconnectProjections[actor.nodeID]; existing != nil {
		// An overlapping projection would make it impossible to attribute the
		// completion to one exact journal. Keep the node offline and surface the
		// invariant failure instead of replacing the pending authority.
		broker.disconnectErrors[actor.nodeID] = ErrAgentDisconnectCommit
		return false
	}
	broker.disconnectProjections[actor.nodeID] = actor
	return true
}

func (broker *AgentBroker) commitDisconnectProjection(
	actor *agentSessionActor,
) {
	actor.stateMu.Lock()
	snapshot := cloneAgentSnapshot(actor.snapshot)
	actor.stateMu.Unlock()
	snapshotDigest, digestErr := transport.AgentSnapshotDigest(snapshot)
	var commitErr error
	if digestErr != nil {
		commitErr = ErrAgentDisconnectCommit
	} else {
		commitErr = broker.consumers.Disconnects.HandleAgentDisconnect(
			broker.lifetimeContext,
			AgentDisconnectRecord{
				NodeID:         actor.nodeID,
				SnapshotDigest: snapshotDigest,
			},
		)
	}

	lifecycle := broker.nodeLifecycleLock(actor.nodeID)
	lifecycle.Lock()
	defer lifecycle.Unlock()
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.disconnectProjections[actor.nodeID] != actor {
		return
	}
	delete(broker.disconnectProjections, actor.nodeID)
	if commitErr != nil {
		// Consumer errors are not retained verbatim because future diagnostics
		// must not leak implementation detail or credential-bearing context.
		broker.disconnectErrors[actor.nodeID] = ErrAgentDisconnectCommit
	} else {
		delete(broker.disconnectErrors, actor.nodeID)
	}
}

func (broker *AgentBroker) nodeLifecycleLock(nodeID domain.NodeID) *sync.Mutex {
	candidate := &sync.Mutex{}
	actual, _ := broker.lifecycleLocks.LoadOrStore(nodeID, candidate)
	return actual.(*sync.Mutex)
}

func (broker *AgentBroker) allocateSessionGeneration(
	nodeID domain.NodeID,
) (uint64, error) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return 0, ErrAgentBrokerClosed
	}
	current := broker.sessionGeneration[nodeID]
	if current == ^uint64(0) {
		return 0, ErrAgentProtocol
	}
	current++
	broker.sessionGeneration[nodeID] = current
	return current, nil
}

func (broker *AgentBroker) mayProject(actor *agentSessionActor) error {
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	if broker.closed {
		return ErrAgentBrokerClosed
	}
	capture, capturing := broker.snapshotCaptures[actor.nodeID]
	if !capturing || capture.actor != actor ||
		capture.commandWasInFlight ||
		broker.commandsInFlight[actor.nodeID] != nil ||
		capture.commandSequence != broker.commandSequences[actor.nodeID] ||
		actor.generation <= broker.projectedGeneration[actor.nodeID] {
		return ErrAgentSessionReplaced
	}
	return nil
}

func (broker *AgentBroker) recordProjection(actor *agentSessionActor) error {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return ErrAgentBrokerClosed
	}
	capture, capturing := broker.snapshotCaptures[actor.nodeID]
	if !capturing || capture.actor != actor ||
		capture.commandWasInFlight ||
		broker.commandsInFlight[actor.nodeID] != nil ||
		capture.commandSequence != broker.commandSequences[actor.nodeID] ||
		actor.generation <= broker.projectedGeneration[actor.nodeID] {
		return ErrAgentSessionReplaced
	}
	broker.projectedGeneration[actor.nodeID] = actor.generation
	return nil
}

func (broker *AgentBroker) currentProjectedActor(actor *agentSessionActor) bool {
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	if broker.closed ||
		broker.sessions[actor.nodeID] != actor ||
		broker.projectedGeneration[actor.nodeID] != actor.generation {
		return false
	}
	actor.stateMu.Lock()
	defer actor.stateMu.Unlock()
	return actor.readyAcked && !actor.terminated
}

func (broker *AgentBroker) beginSnapshotCapture(actor *agentSessionActor) error {
	lifecycle := broker.nodeLifecycleLock(actor.nodeID)
	lifecycle.Lock()
	defer lifecycle.Unlock()

	broker.mu.Lock()
	if broker.closed {
		broker.mu.Unlock()
		return ErrAgentBrokerClosed
	}
	if broker.disconnectProjections[actor.nodeID] != nil {
		broker.mu.Unlock()
		return ErrAgentDisconnectCommit
	}
	if actor.generation <= broker.projectedGeneration[actor.nodeID] {
		broker.mu.Unlock()
		return ErrAgentSessionReplaced
	}
	previous := broker.snapshotCaptures[actor.nodeID].actor
	if previous != nil && previous.generation >= actor.generation {
		broker.mu.Unlock()
		return ErrAgentSessionReplaced
	}
	broker.snapshotCaptures[actor.nodeID] = agentSnapshotCapture{
		actor:              actor,
		commandSequence:    broker.commandSequences[actor.nodeID],
		commandWasInFlight: broker.commandsInFlight[actor.nodeID] != nil,
	}
	broker.mu.Unlock()

	if previous != nil && previous != actor {
		previous.terminate(ErrAgentSessionReplaced)
	}
	return nil
}

func (broker *AgentBroker) endSnapshotCapture(actor *agentSessionActor) {
	lifecycle := broker.nodeLifecycleLock(actor.nodeID)
	lifecycle.Lock()
	defer lifecycle.Unlock()
	broker.endSnapshotCaptureUnderLifecycle(actor)
}

func (broker *AgentBroker) endSnapshotCaptureUnderLifecycle(
	actor *agentSessionActor,
) {
	broker.mu.Lock()
	if capture, found := broker.snapshotCaptures[actor.nodeID]; found &&
		capture.actor == actor {
		delete(broker.snapshotCaptures, actor.nodeID)
	}
	broker.mu.Unlock()
}

func (broker *AgentBroker) beginCommandDispatch(
	actor *agentSessionActor,
) error {
	lifecycle := broker.nodeLifecycleLock(actor.nodeID)
	lifecycle.Lock()
	defer lifecycle.Unlock()

	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed ||
		broker.sessions[actor.nodeID] != actor ||
		broker.projectedGeneration[actor.nodeID] != actor.generation {
		return ErrAgentSessionReplaced
	}
	actor.stateMu.Lock()
	ready := actor.readyAcked && !actor.terminated
	actor.stateMu.Unlock()
	if !ready {
		return ErrAgentSessionReplaced
	}
	if broker.commandsInFlight[actor.nodeID] != nil {
		return ErrAgentProtocol
	}
	if broker.commandSequences[actor.nodeID] == ^uint64(0) {
		return ErrAgentProtocol
	}
	broker.commandSequences[actor.nodeID]++
	broker.commandsInFlight[actor.nodeID] = actor
	return nil
}

func (broker *AgentBroker) endCommandDispatch(actor *agentSessionActor) {
	lifecycle := broker.nodeLifecycleLock(actor.nodeID)
	lifecycle.Lock()
	defer lifecycle.Unlock()

	broker.mu.Lock()
	if broker.commandsInFlight[actor.nodeID] == actor {
		delete(broker.commandsInFlight, actor.nodeID)
	}
	broker.mu.Unlock()
}

func (broker *AgentBroker) Close() {
	if broker == nil {
		return
	}
	broker.mu.Lock()
	if broker.closed {
		broker.mu.Unlock()
		return
	}
	broker.closed = true
	cancelLifetime := broker.cancelLifetime
	actors := make([]*agentSessionActor, 0, len(broker.sessions))
	for _, actor := range broker.sessions {
		actors = append(actors, actor)
	}
	for nodeID, offline := range broker.offlineChanges {
		offline.cancel()
		delete(broker.offlineChanges, nodeID)
	}
	clear(broker.disconnectErrors)
	clear(broker.commandsInFlight)
	clear(broker.snapshotCaptures)
	broker.mu.Unlock()
	if cancelLifetime != nil {
		cancelLifetime()
	}
	for _, actor := range actors {
		actor.terminate(ErrAgentBrokerClosed)
	}
}

type authenticatedAgentSession interface {
	Credential() enroll.Credential
	Read(context.Context) (transport.Envelope, error)
	Write(context.Context, transport.Envelope) error
}

func (broker *AgentBroker) serveSession(ctx context.Context, session authenticatedAgentSession) error {
	if broker == nil || session == nil {
		return ErrAgentBrokerClosed
	}
	credential := session.Credential()
	if credential.NodeID == "" {
		return ErrAgentProtocol
	}
	generation, err := broker.allocateSessionGeneration(domain.NodeID(credential.NodeID))
	if err != nil {
		return err
	}
	actorContext, cancel := context.WithCancel(ctx)
	actor := &agentSessionActor{
		broker:        broker,
		session:       session,
		nodeID:        domain.NodeID(credential.NodeID),
		generation:    generation,
		ctx:           actorContext,
		cancel:        cancel,
		ready:         make(chan struct{}),
		done:          make(chan struct{}),
		writeGate:     make(chan struct{}, 1),
		commandGate:   make(chan struct{}, 1),
		knownCommands: make(map[domain.CommandID]domain.ExecutionID),
	}
	actor.writeGate <- struct{}{}
	actor.commandGate <- struct{}{}
	return actor.run()
}

type pendingAgentCommand struct {
	commandID   domain.CommandID
	executionID domain.ExecutionID
	context     context.Context
	acked       bool
	result      chan agentCommandResult
}

type agentCommandResult struct {
	update transport.ExecutionUpdate
	err    error
}

type agentSessionActor struct {
	broker     *AgentBroker
	session    authenticatedAgentSession
	nodeID     domain.NodeID
	generation uint64
	ctx        context.Context
	cancel     context.CancelFunc

	ready       chan struct{}
	readyOnce   sync.Once
	done        chan struct{}
	terminal    sync.Once
	terminalMu  sync.Mutex
	terminalErr error

	writeGate   chan struct{}
	commandGate chan struct{}

	stateMu             sync.Mutex
	terminated          bool
	readyAcked          bool
	pending             *pendingAgentCommand
	knownCommands       map[domain.CommandID]domain.ExecutionID
	snapshot            AgentSnapshot
	readinessContext    context.Context
	readinessCancel     context.CancelFunc
	readinessTimer      *time.Timer
	readinessGeneration uint64
}

func (actor *agentSessionActor) run() error {
	defer actor.broker.deactivate(actor)
	defer actor.broker.endSnapshotCapture(actor)
	defer actor.terminate(ErrAgentDisconnected)

	if err := actor.readHello(); err != nil {
		actor.terminate(err)
		return actor.terminalError()
	}
	if err := actor.readSnapshotAndActivate(); err != nil {
		actor.terminate(err)
		return actor.terminalError()
	}

	for {
		envelope, err := actor.session.Read(actor.ctx)
		if err != nil {
			actor.terminate(classifyAgentSessionError(err))
			return actor.terminalError()
		}
		switch envelope.Type {
		case transport.MessageHeartbeat:
			if err := actor.handleHeartbeat(envelope); err != nil {
				actor.terminate(err)
				return actor.terminalError()
			}
		case transport.MessageAck:
			if err := actor.handleCommandAck(envelope); err != nil {
				actor.terminate(err)
				return actor.terminalError()
			}
		case transport.MessageExecutionUpdate:
			if err := actor.handleExecutionUpdate(envelope); err != nil {
				actor.terminate(err)
				return actor.terminalError()
			}
		default:
			actor.terminate(ErrAgentProtocol)
			return actor.terminalError()
		}
	}
}

func (actor *agentSessionActor) readHello() error {
	envelope, err := actor.session.Read(actor.ctx)
	if err != nil {
		return classifyAgentSessionError(err)
	}
	if envelope.Type != transport.MessageHello {
		return ErrAgentProtocol
	}
	var payload struct {
		NodeID string `json:"nodeId"`
	}
	if err := decodeStrictJSON(envelope.Payload, &payload); err != nil || domain.NodeID(payload.NodeID) != actor.nodeID {
		return ErrAgentProtocol
	}
	// The Agent builds its snapshot only after this ACK. Record the command
	// sequence first so a command accepted by the old session during the capture
	// interval makes the replacement snapshot stale before any durable commit.
	if err := actor.broker.beginSnapshotCapture(actor); err != nil {
		return err
	}
	return actor.acknowledge(envelope.MessageID)
}

func (actor *agentSessionActor) readSnapshotAndActivate() error {
	envelope, err := actor.session.Read(actor.ctx)
	if err != nil {
		return classifyAgentSessionError(err)
	}
	receivedAt := time.Now()
	if envelope.Type != transport.MessageSnapshot {
		return ErrAgentProtocol
	}
	snapshot, err := transport.DecodeAgentSnapshot(envelope.Payload)
	if err != nil || snapshot.NodeID != actor.nodeID {
		return ErrAgentProtocol
	}
	if actor.broker.consumers.Snapshot == nil {
		return ErrAgentSnapshotConsumerRequired
	}
	lifecycle := actor.broker.nodeLifecycleLock(actor.nodeID)
	lifecycle.Lock()
	if err := actor.broker.mayProject(actor); err != nil {
		lifecycle.Unlock()
		return err
	}
	if err := actor.broker.consumers.Snapshot.HandleAgentSnapshot(actor.ctx, cloneAgentSnapshot(snapshot)); err != nil {
		lifecycle.Unlock()
		return ErrAgentSnapshotCommit
	}
	actor.stateMu.Lock()
	actor.snapshot = cloneAgentSnapshot(snapshot)
	actor.replaceReadinessStateLocked(snapshot.NativeRunnerReady, receivedAt)
	for _, command := range snapshot.Commands {
		actor.knownCommands[command.ID] = command.ExecutionID
	}
	actor.stateMu.Unlock()
	if err := actor.broker.recordProjection(actor); err != nil {
		projectDisconnect := false
		if actor.broker.consumers.Disconnects != nil {
			projectDisconnect = actor.broker.beginDisconnectProjectionUnderLifecycle(actor)
		}
		actor.broker.endSnapshotCaptureUnderLifecycle(actor)
		lifecycle.Unlock()
		if projectDisconnect {
			actor.broker.commitDisconnectProjection(actor)
		}
		return err
	}
	if err := actor.broker.activate(actor); err != nil {
		projectDisconnect := false
		if actor.broker.consumers.Disconnects != nil {
			projectDisconnect = actor.broker.beginDisconnectProjectionUnderLifecycle(actor)
		}
		actor.broker.endSnapshotCaptureUnderLifecycle(actor)
		lifecycle.Unlock()
		if projectDisconnect {
			actor.broker.commitDisconnectProjection(actor)
		}
		return err
	}
	// Durable projection and replacement activation are the linearization
	// point. Protocol I/O must not retain the per-node lifecycle lock: a stalled
	// peer can then be superseded, which cancels this actor's write.
	actor.broker.endSnapshotCaptureUnderLifecycle(actor)
	lifecycle.Unlock()
	if err := actor.acknowledge(envelope.MessageID); err != nil {
		actor.broker.deactivate(actor)
		return err
	}
	if err := actor.broker.markReady(actor); err != nil {
		actor.broker.deactivate(actor)
		return err
	}
	return nil
}

func cloneAgentSnapshot(snapshot AgentSnapshot) AgentSnapshot {
	snapshot.Commands = append([]domain.Command(nil), snapshot.Commands...)
	snapshot.Observations = append([]transport.AgentExecutionObservation(nil), snapshot.Observations...)
	snapshot.CleanupTombstones = append([]transport.AgentCleanupTombstone(nil), snapshot.CleanupTombstones...)
	// The pointer distinguishes "no change reported" (nil) from an empty set;
	// clone the pointee so a later caller mutation cannot leak in.
	if snapshot.ExcludedTargets != nil {
		set := append([]domain.TargetID{}, *snapshot.ExcludedTargets...)
		snapshot.ExcludedTargets = &set
	}
	return snapshot
}

func (actor *agentSessionActor) handleHeartbeat(envelope transport.Envelope) error {
	receivedAt := time.Now()
	heartbeat, err := transport.DecodeAgentHeartbeat(envelope.Payload)
	if err != nil || heartbeat.NodeID != actor.nodeID {
		return ErrAgentProtocol
	}

	lifecycle := actor.broker.nodeLifecycleLock(actor.nodeID)
	lifecycle.Lock()
	if !actor.broker.currentProjectedActor(actor) {
		lifecycle.Unlock()
		return ErrAgentSessionReplaced
	}
	actor.stateMu.Lock()
	current := actor.snapshot.NativeRunnerReady
	if current != heartbeat.NativeRunnerReady {
		snapshot := cloneAgentSnapshot(actor.snapshot)
		actor.stateMu.Unlock()
		if actor.broker.consumers.Readiness == nil {
			lifecycle.Unlock()
			return ErrAgentReadinessConsumerRequired
		}
		snapshotDigest, err := transport.AgentSnapshotDigest(snapshot)
		if err != nil {
			lifecycle.Unlock()
			return ErrAgentProtocol
		}
		// A heartbeat can only advance liveness for the exact full journal that
		// activated this actor. It never replays that journal over newer
		// execution-update projection state.
		if err := actor.broker.consumers.Readiness.HandleAgentReadiness(
			actor.ctx, actor.nodeID, snapshotDigest,
			heartbeat.NativeRunnerReady,
		); err != nil {
			lifecycle.Unlock()
			return ErrAgentSnapshotCommit
		}
		actor.stateMu.Lock()
		actor.snapshot.NativeRunnerReady = heartbeat.NativeRunnerReady
		actor.replaceReadinessStateLocked(heartbeat.NativeRunnerReady, receivedAt)
	} else if heartbeat.NativeRunnerReady {
		// A healthy renewal extends only the lease. Keeping the same context
		// lets an in-flight GitHub long poll continue without churn.
		actor.armReadinessLeaseLocked(receivedAt)
	}
	actor.stateMu.Unlock()
	if err := actor.adoptOwnerStateUnderLifecycle(heartbeat); err != nil {
		lifecycle.Unlock()
		return err
	}
	lifecycle.Unlock()
	return actor.acknowledgeHeartbeat(envelope.MessageID)
}

// adoptOwnerStateUnderLifecycle commits a mid-session node-owner availability
// change before the heartbeat is acknowledged. Excluding is subtractive and is
// already locally effective on the node; adopting it here is what makes the
// controller stop advertising that Target's capacity. A failed commit fails the
// heartbeat rather than acknowledging a change that was never recorded.
//
// The caller holds the per-node lifecycle lock and has already proven this actor
// is the currently projected session.
func (actor *agentSessionActor) adoptOwnerStateUnderLifecycle(
	heartbeat transport.AgentHeartbeat,
) error {
	actor.stateMu.Lock()
	intent := heartbeat.AvailabilityIntent
	if intent == actor.snapshot.AvailabilityIntent {
		intent = ""
	}
	var exclusions []domain.TargetID
	// A nil heartbeat field is "no change reported". A present set that already
	// matches the last-known one is a steady-state repeat, not a change. A nil
	// last-known set is "this session never reported one", which is not the same
	// as an empty set: adopt so a stale durable row cannot outlive the session
	// that stopped reporting it.
	if heartbeat.ExcludedTargets != nil &&
		(actor.snapshot.ExcludedTargets == nil ||
			!sameTargetIDSet(*actor.snapshot.ExcludedTargets, *heartbeat.ExcludedTargets)) {
		exclusions = append([]domain.TargetID{}, *heartbeat.ExcludedTargets...)
	}
	if intent == "" && exclusions == nil {
		actor.stateMu.Unlock()
		return nil
	}
	snapshot := cloneAgentSnapshot(actor.snapshot)
	actor.stateMu.Unlock()

	if actor.broker.consumers.OwnerState == nil {
		return ErrAgentOwnerStateConsumerRequired
	}
	snapshotDigest, err := transport.AgentSnapshotDigest(snapshot)
	if err != nil {
		return ErrAgentProtocol
	}
	if err := actor.broker.consumers.OwnerState.HandleAgentOwnerState(
		actor.ctx,
		AgentOwnerStateRecord{
			NodeID:             actor.nodeID,
			SnapshotDigest:     snapshotDigest,
			AvailabilityIntent: intent,
			ExcludedTargets:    exclusions,
		},
	); err != nil {
		return ErrAgentSnapshotCommit
	}
	actor.stateMu.Lock()
	if intent != "" {
		actor.snapshot.AvailabilityIntent = intent
	}
	if exclusions != nil {
		set := append([]domain.TargetID{}, exclusions...)
		actor.snapshot.ExcludedTargets = &set
	}
	actor.stateMu.Unlock()
	return nil
}

// sameTargetIDSet compares two exclusion sets by membership. Both sides are
// duplicate-free by transport validation, so equal length plus containment is a
// set equality test and does not depend on the order the owner edited them in.
func sameTargetIDSet(left, right []domain.TargetID) bool {
	if len(left) != len(right) {
		return false
	}
	members := make(map[domain.TargetID]struct{}, len(left))
	for _, targetID := range left {
		members[targetID] = struct{}{}
	}
	for _, targetID := range right {
		if _, found := members[targetID]; !found {
			return false
		}
	}
	return true
}

// acknowledgeHeartbeat piggybacks the current eligible-target list onto the
// heartbeat's own acknowledgement rather than a separate message type, so a
// desktop client's view of "which org/repo scopes could route here" refreshes
// at the same 1 Hz cadence as liveness with no additional protocol surface. A
// failed or absent eligibility lookup still acknowledges the heartbeat; it
// only leaves the desktop-visible list unrefreshed until the next tick.
func (actor *agentSessionActor) acknowledgeHeartbeat(messageID string) error {
	// A *slice, not a slice, carries the field: nil means "lookup failed or
	// absent, keep the Agent's previously known list"; a non-nil pointer to an
	// empty slice means "a successful lookup confirmed zero eligible targets".
	// A plain slice with omitempty cannot express that second case, since Go's
	// JSON encoder treats nil and empty slices identically.
	var eligible *[]transport.EligibleTarget
	if actor.broker.consumers.Eligibility != nil {
		actor.stateMu.Lock()
		nodeOS, nodeArch := actor.snapshot.OS, actor.snapshot.Arch
		actor.stateMu.Unlock()
		if targets, err := actor.broker.consumers.Eligibility.EligibleTargets(
			actor.ctx, actor.nodeID, nodeOS, nodeArch,
		); err == nil {
			if targets == nil {
				targets = []transport.EligibleTarget{}
			}
			eligible = &targets
		}
	}
	payload, err := json.Marshal(struct {
		MessageID       string                      `json:"messageId"`
		EligibleTargets *[]transport.EligibleTarget `json:"eligibleTargets,omitempty"`
	}{MessageID: messageID, EligibleTargets: eligible})
	if err != nil {
		return ErrAgentProtocol
	}
	ackID, err := randomMessageID()
	if err != nil {
		return ErrAgentProtocol
	}
	if err := actor.write(actor.ctx, transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       ackID,
		Type:            transport.MessageAck,
		Payload:         payload,
	}); err != nil {
		return classifyAgentSessionError(err)
	}
	return nil
}

func (actor *agentSessionActor) replaceReadinessStateLocked(ready bool, receivedAt time.Time) {
	if actor.readinessCancel != nil {
		actor.readinessCancel()
	}
	if actor.readinessTimer != nil {
		actor.readinessTimer.Stop()
		actor.readinessTimer = nil
	}
	actor.readinessGeneration++
	actor.readinessContext, actor.readinessCancel = context.WithCancel(context.Background())
	if ready {
		actor.armReadinessLeaseLocked(receivedAt)
	}
}

func (actor *agentSessionActor) armReadinessLeaseLocked(receivedAt time.Time) {
	if actor.readinessTimer != nil {
		actor.readinessTimer.Stop()
	}
	actor.readinessGeneration++
	generation := actor.readinessGeneration
	remaining := time.Until(receivedAt.Add(actor.broker.readinessLease))
	if remaining <= 0 {
		remaining = time.Nanosecond
	}
	actor.readinessTimer = time.AfterFunc(remaining, func() {
		actor.expireReadiness(generation)
	})
}

func (actor *agentSessionActor) expireReadiness(generation uint64) {
	lifecycle := actor.broker.nodeLifecycleLock(actor.nodeID)
	lifecycle.Lock()
	if !actor.broker.currentProjectedActor(actor) {
		lifecycle.Unlock()
		return
	}
	actor.stateMu.Lock()
	if actor.terminated || generation != actor.readinessGeneration ||
		!actor.snapshot.NativeRunnerReady {
		actor.stateMu.Unlock()
		lifecycle.Unlock()
		return
	}
	snapshot := cloneAgentSnapshot(actor.snapshot)
	snapshotDigest, digestErr := transport.AgentSnapshotDigest(snapshot)
	// Expiry must revoke advertised capacity at receive time, even when the
	// durable observer is temporarily slow. The failed commit then terminates
	// the session so the node cannot silently regain capacity.
	actor.snapshot.NativeRunnerReady = false
	actor.replaceReadinessStateLocked(false, time.Now())
	consumer := actor.broker.consumers.Readiness
	actor.stateMu.Unlock()
	if digestErr != nil || consumer == nil ||
		consumer.HandleAgentReadiness(
			actor.ctx, actor.nodeID, snapshotDigest, false,
		) != nil {
		lifecycle.Unlock()
		actor.terminate(ErrAgentSnapshotCommit)
		return
	}
	lifecycle.Unlock()
}

func (actor *agentSessionActor) handleCommandAck(envelope transport.Envelope) error {
	var payload struct {
		MessageID string `json:"messageId"`
	}
	if err := decodeStrictJSON(envelope.Payload, &payload); err != nil {
		return ErrAgentProtocol
	}
	actor.stateMu.Lock()
	defer actor.stateMu.Unlock()
	if actor.pending == nil || payload.MessageID != string(actor.pending.commandID) || actor.pending.acked {
		return ErrAgentProtocol
	}
	actor.pending.acked = true
	return nil
}

func (actor *agentSessionActor) handleExecutionUpdate(envelope transport.Envelope) error {
	update, err := transport.DecodeExecutionUpdate(envelope.Payload)
	if err != nil || update.NodeID != actor.nodeID {
		return ErrAgentProtocol
	}

	actor.stateMu.Lock()
	pending := actor.pending
	knownExecutionID, known := actor.knownCommands[update.CommandID]
	isInitial := pending != nil && pending.commandID == update.CommandID
	if isInitial {
		if !pending.acked || update.ExecutionID != pending.executionID {
			actor.stateMu.Unlock()
			return ErrAgentProtocol
		}
	} else if !known || update.ExecutionID != knownExecutionID {
		actor.stateMu.Unlock()
		return ErrAgentProtocol
	}
	actor.stateMu.Unlock()

	if actor.broker.consumers.ExecutionUpdates == nil {
		return ErrExecutionUpdateConsumerRequired
	}
	consumerContext := actor.ctx
	if isInitial {
		consumerContext = pending.context
	}
	if err := actor.broker.consumers.ExecutionUpdates.HandleExecutionUpdate(consumerContext, AgentExecutionUpdateRecord{
		MessageID:     envelope.MessageID,
		Update:        update,
		PayloadDigest: transport.PayloadDigest(transport.MessageExecutionUpdate, envelope.Payload),
	}); err != nil {
		return ErrExecutionUpdateCommit
	}
	if err := actor.acknowledge(envelope.MessageID); err != nil {
		return err
	}

	actor.stateMu.Lock()
	if isInitial && actor.pending != pending {
		actor.stateMu.Unlock()
		return ErrAgentProtocol
	}
	switch update.State {
	case domain.ExecutionReleased, domain.ExecutionFailed:
		// Durable consumer commit and Agent ACK are both complete at this point.
		// Forget every command for the finished execution so a long-lived
		// session is bounded by active or cleanup-uncertain work, not job count.
		for commandID, executionID := range actor.knownCommands {
			if executionID == update.ExecutionID {
				delete(actor.knownCommands, commandID)
			}
		}
	default:
		// CleanupFailed and Quarantined deliberately remain known until
		// reconciliation proves that their runtime boundary is safe.
		actor.knownCommands[update.CommandID] = update.ExecutionID
	}
	if isInitial {
		actor.pending = nil
	}
	actor.stateMu.Unlock()
	if isInitial {
		pending.result <- agentCommandResult{update: update}
	}
	return nil
}

func (actor *agentSessionActor) acknowledge(messageID string) error {
	payload, err := json.Marshal(struct {
		MessageID string `json:"messageId"`
	}{MessageID: messageID})
	if err != nil {
		return ErrAgentProtocol
	}
	ackID, err := randomMessageID()
	if err != nil {
		return ErrAgentProtocol
	}
	if err := actor.write(actor.ctx, transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       ackID,
		Type:            transport.MessageAck,
		Payload:         payload,
	}); err != nil {
		return classifyAgentSessionError(err)
	}
	return nil
}

func (actor *agentSessionActor) sendCommand(
	ctx context.Context,
	messageType transport.MessageType,
	metadata transport.CommandMetadata,
	encode func() (json.RawMessage, error),
) (transport.ExecutionUpdate, error) {
	return actor.sendCommandWithAuthority(
		ctx, messageType, metadata, false, false, "", encode)
}

func (actor *agentSessionActor) sendReplayCommand(
	ctx context.Context,
	messageType transport.MessageType,
	metadata transport.CommandMetadata,
	snapshotDigest string,
	encode func() (json.RawMessage, error),
) (transport.ExecutionUpdate, error) {
	return actor.sendCommandWithAuthority(
		ctx, messageType, metadata, false, true, snapshotDigest, encode)
}

func (actor *agentSessionActor) sendReconciliationCommand(
	ctx context.Context,
	messageType transport.MessageType,
	metadata transport.CommandMetadata,
	snapshotDigest string,
	encode func() (json.RawMessage, error),
) (transport.ExecutionUpdate, error) {
	return actor.sendCommandWithAuthority(
		ctx, messageType, metadata, true, false, snapshotDigest, encode)
}

func (actor *agentSessionActor) sendCommandWithAuthority(
	ctx context.Context,
	messageType transport.MessageType,
	metadata transport.CommandMetadata,
	reconciliation bool,
	replayOnly bool,
	snapshotDigest string,
	encode func() (json.RawMessage, error),
) (transport.ExecutionUpdate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := actor.acquire(ctx, actor.commandGate); err != nil {
		return transport.ExecutionUpdate{}, err
	}
	defer actor.release(actor.commandGate)

	select {
	case <-actor.done:
		return transport.ExecutionUpdate{}, actor.terminalError()
	default:
	}
	select {
	case <-actor.ready:
	case <-actor.done:
		return transport.ExecutionUpdate{}, actor.terminalError()
	case <-ctx.Done():
		return transport.ExecutionUpdate{}, ctx.Err()
	}
	if err := actor.broker.beginCommandDispatch(actor); err != nil {
		return transport.ExecutionUpdate{}, err
	}
	defer actor.broker.endCommandDispatch(actor)
	if snapshotDigest != "" {
		actor.stateMu.Lock()
		currentSnapshot := cloneAgentSnapshot(actor.snapshot)
		actor.stateMu.Unlock()
		currentDigest, err := transport.AgentSnapshotDigest(currentSnapshot)
		if err != nil || currentDigest != snapshotDigest {
			return transport.ExecutionUpdate{}, ErrAgentReconciliationStale
		}
	}

	actor.stateMu.Lock()
	if actor.terminated || actor.pending != nil {
		actor.stateMu.Unlock()
		if actor.terminated {
			return transport.ExecutionUpdate{}, actor.terminalError()
		}
		actor.terminate(ErrAgentProtocol)
		return transport.ExecutionUpdate{}, ErrAgentProtocol
	}
	pending := &pendingAgentCommand{
		commandID:   metadata.CommandID,
		executionID: metadata.ExecutionID,
		context:     ctx,
		result:      make(chan agentCommandResult, 1),
	}
	actor.pending = pending
	actor.stateMu.Unlock()

	payload, err := encode()
	if err != nil {
		actor.clearPending(pending)
		return transport.ExecutionUpdate{}, err
	}
	payloadCleared := false
	clearPayload := func() {
		if payloadCleared {
			return
		}
		clear(payload)
		payload = nil
		payloadCleared = true
	}
	defer clearPayload()
	digest := transport.PayloadDigest(messageType, payload)
	if actor.broker.consumers.Commands == nil {
		actor.clearPending(pending)
		return transport.ExecutionUpdate{}, ErrAgentCommandConsumerRequired
	}
	if err := actor.broker.consumers.Commands.HandleAgentCommand(ctx, AgentCommandRecord{
		NodeID:         actor.nodeID,
		Kind:           messageType,
		Metadata:       metadata,
		PayloadDigest:  digest,
		Reconciliation: reconciliation,
		ReplayOnly:     replayOnly,
		SnapshotDigest: snapshotDigest,
	}); err != nil {
		actor.clearPending(pending)
		return transport.ExecutionUpdate{}, ErrAgentCommandCommit
	}
	envelope := transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       string(metadata.CommandID),
		Type:            messageType,
		Payload:         payload,
	}
	writeErr := actor.write(ctx, envelope)
	clearPayload()
	envelope.Payload = nil
	if writeErr != nil {
		actor.terminate(classifyAgentSessionError(writeErr))
		return transport.ExecutionUpdate{}, actor.terminalError()
	}

	select {
	case result := <-pending.result:
		return result.update, result.err
	case <-actor.done:
		return transport.ExecutionUpdate{}, actor.terminalError()
	case <-ctx.Done():
		// Once a command may have reached the Agent, abandoning only the caller
		// would make a later ACK ambiguous. Tear down the session and reconcile.
		actor.terminate(ctx.Err())
		return transport.ExecutionUpdate{}, ctx.Err()
	}
}

func (actor *agentSessionActor) clearPending(pending *pendingAgentCommand) {
	actor.stateMu.Lock()
	if actor.pending == pending {
		actor.pending = nil
	}
	actor.stateMu.Unlock()
}

func (actor *agentSessionActor) write(ctx context.Context, envelope transport.Envelope) error {
	if err := actor.acquire(ctx, actor.writeGate); err != nil {
		return err
	}
	defer actor.release(actor.writeGate)
	writeContext, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(actor.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()
	return actor.session.Write(writeContext, envelope)
}

func (actor *agentSessionActor) acquire(ctx context.Context, gate chan struct{}) error {
	select {
	case <-actor.done:
		return actor.terminalError()
	default:
	}
	select {
	case <-gate:
		select {
		case <-actor.done:
			actor.release(gate)
			return actor.terminalError()
		default:
			return nil
		}
	case <-actor.done:
		return actor.terminalError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*agentSessionActor) release(gate chan struct{}) {
	gate <- struct{}{}
}

func (actor *agentSessionActor) terminate(err error) {
	if err == nil {
		err = ErrAgentDisconnected
	}
	actor.terminal.Do(func() {
		actor.terminalMu.Lock()
		actor.terminalErr = err
		actor.terminalMu.Unlock()
		actor.cancel()

		actor.stateMu.Lock()
		actor.terminated = true
		if actor.readinessTimer != nil {
			actor.readinessTimer.Stop()
			actor.readinessTimer = nil
		}
		if actor.readinessCancel != nil {
			actor.readinessCancel()
		}
		pending := actor.pending
		actor.pending = nil
		actor.stateMu.Unlock()
		if pending != nil {
			pending.result <- agentCommandResult{err: err}
		}
		close(actor.done)
	})
}

func (actor *agentSessionActor) terminalError() error {
	actor.terminalMu.Lock()
	defer actor.terminalMu.Unlock()
	if actor.terminalErr == nil {
		return ErrAgentDisconnected
	}
	return actor.terminalErr
}

func classifyAgentSessionError(err error) error {
	if err == nil {
		return nil
	}
	if _, _, rejected := transport.AgentSessionRejection(err); rejected {
		return err
	}
	if errors.Is(err, transport.ErrProtocolVersion) ||
		errors.Is(err, transport.ErrInvalidEnvelope) ||
		errors.Is(err, transport.ErrUnsupportedType) {
		return errors.Join(ErrAgentProtocol, err)
	}
	return ErrAgentDisconnected
}
