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
	ErrExecutionUpdateConsumerRequired = errors.New("execution update consumer is required")
	ErrAgentCommandCommit              = errors.New("agent command commit failed")
	ErrAgentSnapshotCommit             = errors.New("agent snapshot commit failed")
	ErrExecutionUpdateCommit           = errors.New("execution update commit failed")
)

// AgentCommandRecord is the non-secret, durable identity committed before a
// command reaches the network. PayloadDigest covers kind + NUL + the exact
// encoded payload; the raw payload and JIT configuration never cross this
// boundary.
type AgentCommandRecord struct {
	NodeID        domain.NodeID
	Kind          transport.MessageType
	Metadata      transport.CommandMetadata
	PayloadDigest [sha256.Size]byte
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

// AgentConsumers are all mandatory for a work-bearing production session.
// A nil owner fails closed before its corresponding ACK or command write.
// Consumers are commit boundaries and must not synchronously call this broker's
// Send methods; schedule any follow-up only after the callback returns.
type AgentConsumers struct {
	Commands         AgentCommandConsumer
	Snapshot         AgentSnapshotConsumer
	ExecutionUpdates ExecutionUpdateConsumer
}

// AgentSnapshot is the authenticated, non-secret journal evidence used to
// reconcile and activate a node session.
type AgentSnapshot = transport.AgentSnapshot

// AgentBroker owns one active bidirectional session actor per authenticated
// node. It never stores command payloads or JIT configuration.
type AgentBroker struct {
	mu             sync.RWMutex
	epoch          domain.ControllerEpoch
	consumers      AgentConsumers
	readinessLease time.Duration
	sessions       map[domain.NodeID]*agentSessionActor
	offlineChanges map[domain.NodeID]brokerReadinessChange
	closed         bool
}

type brokerReadinessChange struct {
	context context.Context
	cancel  context.CancelFunc
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
	return &AgentBroker{
		epoch:          epoch,
		consumers:      consumers,
		readinessLease: options.ReadinessLease,
		sessions:       make(map[domain.NodeID]*agentSessionActor),
		offlineChanges: make(map[domain.NodeID]brokerReadinessChange),
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
	return len(broker.sessions)
}

func (broker *AgentBroker) Snapshot(nodeID domain.NodeID) (AgentSnapshot, bool) {
	snapshot, online, _ := broker.Readiness(nodeID)
	return snapshot, online
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
	if !ok {
		change, found := broker.offlineChanges[nodeID]
		if !found {
			change.context, change.cancel = context.WithCancel(context.Background())
			broker.offlineChanges[nodeID] = change
		}
		broker.mu.Unlock()
		return AgentSnapshot{}, false, change.context
	}
	broker.mu.Unlock()
	actor.stateMu.Lock()
	defer actor.stateMu.Unlock()
	return cloneAgentSnapshot(actor.snapshot), !actor.terminated, actor.readinessContext
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
	return actor, ok
}

func (broker *AgentBroker) activate(actor *agentSessionActor) error {
	broker.mu.Lock()
	if broker.closed {
		broker.mu.Unlock()
		return ErrAgentBrokerClosed
	}
	previous := broker.sessions[actor.nodeID]
	broker.sessions[actor.nodeID] = actor
	if offline, found := broker.offlineChanges[actor.nodeID]; found {
		offline.cancel()
		delete(broker.offlineChanges, actor.nodeID)
	}
	broker.mu.Unlock()
	if previous != nil && previous != actor {
		previous.terminate(ErrAgentSessionReplaced)
	}
	return nil
}

func (broker *AgentBroker) deactivate(actor *agentSessionActor) {
	broker.mu.Lock()
	if broker.sessions[actor.nodeID] == actor {
		delete(broker.sessions, actor.nodeID)
		if !broker.closed {
			changeContext, cancel := context.WithCancel(context.Background())
			broker.offlineChanges[actor.nodeID] = brokerReadinessChange{
				context: changeContext,
				cancel:  cancel,
			}
		}
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
	actors := make([]*agentSessionActor, 0, len(broker.sessions))
	for _, actor := range broker.sessions {
		actors = append(actors, actor)
	}
	clear(broker.sessions)
	for nodeID, offline := range broker.offlineChanges {
		offline.cancel()
		delete(broker.offlineChanges, nodeID)
	}
	broker.mu.Unlock()
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
	actorContext, cancel := context.WithCancel(ctx)
	actor := &agentSessionActor{
		broker:        broker,
		session:       session,
		nodeID:        domain.NodeID(credential.NodeID),
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
	broker  *AgentBroker
	session authenticatedAgentSession
	nodeID  domain.NodeID
	ctx     context.Context
	cancel  context.CancelFunc

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
	defer actor.terminate(ErrAgentDisconnected)

	if err := actor.readHello(); err != nil {
		actor.terminate(err)
		return actor.terminalError()
	}
	if err := actor.readSnapshot(); err != nil {
		actor.terminate(err)
		return actor.terminalError()
	}
	if err := actor.broker.activate(actor); err != nil {
		actor.terminate(err)
		return actor.terminalError()
	}
	actor.readyOnce.Do(func() { close(actor.ready) })

	for {
		envelope, err := actor.session.Read(actor.ctx)
		if err != nil {
			actor.terminate(ErrAgentDisconnected)
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
		return ErrAgentDisconnected
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
	return actor.acknowledge(envelope.MessageID)
}

func (actor *agentSessionActor) readSnapshot() error {
	envelope, err := actor.session.Read(actor.ctx)
	if err != nil {
		return ErrAgentDisconnected
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
	if err := actor.broker.consumers.Snapshot.HandleAgentSnapshot(actor.ctx, cloneAgentSnapshot(snapshot)); err != nil {
		return ErrAgentSnapshotCommit
	}
	actor.stateMu.Lock()
	actor.snapshot = cloneAgentSnapshot(snapshot)
	actor.replaceReadinessStateLocked(snapshot.NativeRunnerReady, receivedAt)
	for _, command := range snapshot.Commands {
		actor.knownCommands[command.ID] = command.ExecutionID
	}
	actor.stateMu.Unlock()
	return actor.acknowledge(envelope.MessageID)
}

func cloneAgentSnapshot(snapshot AgentSnapshot) AgentSnapshot {
	snapshot.Commands = append([]domain.Command(nil), snapshot.Commands...)
	snapshot.Observations = append([]transport.AgentExecutionObservation(nil), snapshot.Observations...)
	snapshot.CleanupTombstones = append([]transport.AgentCleanupTombstone(nil), snapshot.CleanupTombstones...)
	return snapshot
}

func (actor *agentSessionActor) handleHeartbeat(envelope transport.Envelope) error {
	receivedAt := time.Now()
	heartbeat, err := transport.DecodeAgentHeartbeat(envelope.Payload)
	if err != nil || heartbeat.NodeID != actor.nodeID {
		return ErrAgentProtocol
	}

	actor.stateMu.Lock()
	current := actor.snapshot.NativeRunnerReady
	if current != heartbeat.NativeRunnerReady {
		snapshot := cloneAgentSnapshot(actor.snapshot)
		snapshot.NativeRunnerReady = heartbeat.NativeRunnerReady
		if actor.broker.consumers.Snapshot == nil {
			actor.stateMu.Unlock()
			return ErrAgentSnapshotConsumerRequired
		}
		// This is the same durable reconciliation boundary as activation. An
		// Agent never receives a readiness ACK before the changed state commits.
		if err := actor.broker.consumers.Snapshot.HandleAgentSnapshot(
			actor.ctx,
			cloneAgentSnapshot(snapshot),
		); err != nil {
			actor.stateMu.Unlock()
			return ErrAgentSnapshotCommit
		}
		actor.snapshot = snapshot
		actor.replaceReadinessStateLocked(heartbeat.NativeRunnerReady, receivedAt)
	} else if heartbeat.NativeRunnerReady {
		// A healthy renewal extends only the lease. Keeping the same context
		// lets an in-flight GitHub long poll continue without churn.
		actor.armReadinessLeaseLocked(receivedAt)
	}
	actor.stateMu.Unlock()
	return actor.acknowledge(envelope.MessageID)
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
	actor.stateMu.Lock()
	if actor.terminated || generation != actor.readinessGeneration ||
		!actor.snapshot.NativeRunnerReady {
		actor.stateMu.Unlock()
		return
	}
	snapshot := cloneAgentSnapshot(actor.snapshot)
	snapshot.NativeRunnerReady = false
	// Expiry must revoke advertised capacity at receive time, even when the
	// durable observer is temporarily slow. The failed commit then terminates
	// the session so the node cannot silently regain capacity.
	actor.snapshot = snapshot
	actor.replaceReadinessStateLocked(false, time.Now())
	consumer := actor.broker.consumers.Snapshot
	if consumer == nil || consumer.HandleAgentSnapshot(actor.ctx, cloneAgentSnapshot(snapshot)) != nil {
		actor.stateMu.Unlock()
		actor.terminate(ErrAgentSnapshotCommit)
		return
	}
	actor.stateMu.Unlock()
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
		return ErrAgentDisconnected
	}
	return nil
}

func (actor *agentSessionActor) sendCommand(
	ctx context.Context,
	messageType transport.MessageType,
	metadata transport.CommandMetadata,
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
		NodeID:        actor.nodeID,
		Kind:          messageType,
		Metadata:      metadata,
		PayloadDigest: digest,
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
		actor.terminate(ErrAgentDisconnected)
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
