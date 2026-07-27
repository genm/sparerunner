package app

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/enroll"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/transport"
)

type fakeAgentSessionRead struct {
	envelope transport.Envelope
	err      error
}

type brokerJITConfig string

func (config brokerJITConfig) Digest() string {
	return domain.PayloadDigest([]byte(config))
}

func (config brokerJITConfig) Deliver(deliver func(string) error) error {
	if deliver == nil {
		return transport.ErrCommandSecret
	}
	return deliver(string(config))
}

type fakeAgentSession struct {
	credential enroll.Credential
	reads      chan fakeAgentSessionRead
	writes     chan transport.Envelope

	writeBlockMu sync.Mutex
	writeBlock   <-chan struct{}
	writeErr     error
	writeEntered chan struct{}
	activeWrites atomic.Int32
	maxWrites    atomic.Int32
}

func newFakeAgentSession(nodeID string) *fakeAgentSession {
	return &fakeAgentSession{
		credential:   enroll.Credential{NodeID: nodeID},
		reads:        make(chan fakeAgentSessionRead, 32),
		writes:       make(chan transport.Envelope, 32),
		writeEntered: make(chan struct{}, 32),
	}
}

func (session *fakeAgentSession) Credential() enroll.Credential {
	return session.credential
}

func (session *fakeAgentSession) Read(ctx context.Context) (transport.Envelope, error) {
	select {
	case read := <-session.reads:
		return read.envelope, read.err
	case <-ctx.Done():
		return transport.Envelope{}, ctx.Err()
	}
}

func (session *fakeAgentSession) Write(ctx context.Context, envelope transport.Envelope) error {
	active := session.activeWrites.Add(1)
	defer session.activeWrites.Add(-1)
	for {
		maximum := session.maxWrites.Load()
		if active <= maximum || session.maxWrites.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case session.writeEntered <- struct{}{}:
	default:
	}

	session.writeBlockMu.Lock()
	block := session.writeBlock
	session.writeBlockMu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	session.writeBlockMu.Lock()
	writeErr := session.writeErr
	session.writeErr = nil
	session.writeBlockMu.Unlock()
	if writeErr != nil {
		return writeErr
	}
	payload := append(json.RawMessage(nil), envelope.Payload...)
	envelope.Payload = payload
	select {
	case session.writes <- envelope:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (session *fakeAgentSession) setWriteBlock(block <-chan struct{}) {
	session.writeBlockMu.Lock()
	session.writeBlock = block
	session.writeBlockMu.Unlock()
}

func (session *fakeAgentSession) failNextWrite(err error) {
	session.writeBlockMu.Lock()
	session.writeErr = err
	session.writeBlockMu.Unlock()
}

func (session *fakeAgentSession) send(envelope transport.Envelope) {
	session.reads <- fakeAgentSessionRead{envelope: envelope}
}

func (session *fakeAgentSession) disconnect() {
	session.reads <- fakeAgentSessionRead{err: errors.New("peer disappeared")}
}

type recordingUpdateConsumer struct {
	mu         sync.Mutex
	updates    []transport.ExecutionUpdate
	messageIDs []string
	records    []AgentExecutionUpdateRecord
	signal     chan transport.ExecutionUpdate
	err        error
}

type recordingCommandConsumer struct {
	mu      sync.Mutex
	records []AgentCommandRecord
	err     error
}

type recordingSnapshotConsumer struct {
	mu        sync.Mutex
	snapshots []AgentSnapshot
	entered   chan int
	blockCall int
	release   <-chan struct{}
}

type recordingDisconnectConsumer struct {
	mu      sync.Mutex
	records []AgentDisconnectRecord
	err     error
	entered chan struct{}
	release <-chan struct{}
}

func (consumer *recordingDisconnectConsumer) HandleAgentDisconnect(
	ctx context.Context,
	record AgentDisconnectRecord,
) error {
	consumer.mu.Lock()
	consumer.records = append(consumer.records, record)
	err := consumer.err
	entered := consumer.entered
	release := consumer.release
	consumer.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (consumer *recordingDisconnectConsumer) count() int {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return len(consumer.records)
}

func (consumer *recordingDisconnectConsumer) record(index int) AgentDisconnectRecord {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return consumer.records[index]
}

func (consumer *recordingSnapshotConsumer) HandleAgentSnapshot(
	ctx context.Context,
	snapshot AgentSnapshot,
) error {
	consumer.mu.Lock()
	consumer.snapshots = append(consumer.snapshots, cloneAgentSnapshot(snapshot))
	call := len(consumer.snapshots)
	block := call == consumer.blockCall
	entered := consumer.entered
	release := consumer.release
	consumer.mu.Unlock()
	if entered != nil {
		select {
		case entered <- call:
		default:
		}
	}
	if block {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (consumer *recordingSnapshotConsumer) HandleAgentReadiness(
	ctx context.Context,
	_ domain.NodeID,
	_ string,
	ready bool,
) error {
	consumer.mu.Lock()
	snapshot := AgentSnapshot{}
	if len(consumer.snapshots) > 0 {
		snapshot = cloneAgentSnapshot(consumer.snapshots[len(consumer.snapshots)-1])
	}
	snapshot.NativeRunnerReady = ready
	consumer.snapshots = append(consumer.snapshots, snapshot)
	call := len(consumer.snapshots)
	block := call == consumer.blockCall
	entered := consumer.entered
	release := consumer.release
	consumer.mu.Unlock()
	if entered != nil {
		select {
		case entered <- call:
		default:
		}
	}
	if block {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (consumer *recordingSnapshotConsumer) last() AgentSnapshot {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if len(consumer.snapshots) == 0 {
		return AgentSnapshot{}
	}
	return cloneAgentSnapshot(consumer.snapshots[len(consumer.snapshots)-1])
}

func (consumer *recordingSnapshotConsumer) count() int {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return len(consumer.snapshots)
}

type exactDedupUpdateConsumer struct {
	mu        sync.Mutex
	seen      map[string]AgentExecutionUpdateRecord
	attempts  int
	committed int
}

func (consumer *exactDedupUpdateConsumer) HandleExecutionUpdate(_ context.Context, update AgentExecutionUpdateRecord) error {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	consumer.attempts++
	existing, found := consumer.seen[update.MessageID]
	if found {
		if existing != update {
			return errors.New("message ID payload mismatch")
		}
		return nil
	}
	consumer.seen[update.MessageID] = update
	consumer.committed++
	return nil
}

func (consumer *recordingCommandConsumer) HandleAgentCommand(_ context.Context, command AgentCommandRecord) error {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	consumer.records = append(consumer.records, command)
	return consumer.err
}

func (consumer *recordingCommandConsumer) record(index int) AgentCommandRecord {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return consumer.records[index]
}

func (consumer *recordingCommandConsumer) recordCount() int {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return len(consumer.records)
}

func newRecordingUpdateConsumer() *recordingUpdateConsumer {
	return &recordingUpdateConsumer{signal: make(chan transport.ExecutionUpdate, 32)}
}

func (consumer *recordingUpdateConsumer) HandleExecutionUpdate(_ context.Context, update AgentExecutionUpdateRecord) error {
	consumer.mu.Lock()
	consumer.updates = append(consumer.updates, update.Update)
	consumer.messageIDs = append(consumer.messageIDs, update.MessageID)
	consumer.records = append(consumer.records, update)
	err := consumer.err
	consumer.mu.Unlock()
	if err == nil {
		consumer.signal <- update.Update
	}
	return err
}

func (consumer *recordingUpdateConsumer) record(index int) AgentExecutionUpdateRecord {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return consumer.records[index]
}

func acceptingAgentConsumers(updateConsumer ExecutionUpdateConsumer) AgentConsumers {
	return AgentConsumers{
		Commands: AgentCommandConsumerFunc(func(context.Context, AgentCommandRecord) error {
			return nil
		}),
		Snapshot: AgentSnapshotConsumerFunc(func(context.Context, AgentSnapshot) error {
			return nil
		}),
		Readiness: AgentReadinessConsumerFunc(func(
			context.Context, domain.NodeID, string, bool,
		) error {
			return nil
		}),
		ExecutionUpdates: updateConsumer,
	}
}

func (consumer *recordingUpdateConsumer) count() int {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return len(consumer.updates)
}

func brokerEnvelope(t *testing.T, messageID string, messageType transport.MessageType, body any) transport.Envelope {
	t.Helper()
	if messageType == transport.MessageSnapshot {
		if snapshot, ok := body.(AgentSnapshot); ok && snapshot.RunnerVersion == "" {
			snapshot.RunnerVersion = runner.OfficialRunnerVersion
			body = snapshot
		}
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       messageID,
		Type:            messageType,
		Payload:         payload,
	}
}

func brokerMetadata(commandID, executionID string) transport.CommandMetadata {
	return transport.CommandMetadata{
		CommandID:       domain.CommandID(commandID),
		ControllerEpoch: 1,
		ExecutionID:     domain.ExecutionID(executionID),
		ExpectedState:   domain.ExecutionReserved,
		Target:          transport.CommandTarget{TargetID: "target-1", Scope: "owner/repo", ScopeKind: domain.TargetRepository},
	}
}

func brokerStartMetadata(commandID, executionID string) transport.CommandMetadata {
	metadata := brokerMetadata(commandID, executionID)
	metadata.ExpectedState = domain.ExecutionPreparing
	return metadata
}

func brokerCancelMetadata(commandID, executionID string) transport.CommandMetadata {
	metadata := brokerMetadata(commandID, executionID)
	metadata.ExpectedState = domain.ExecutionRunning
	return metadata
}

func startReadyBrokerSession(
	t *testing.T,
	broker *AgentBroker,
	nodeID string,
) (*fakeAgentSession, <-chan error) {
	t.Helper()
	return startReadyBrokerSessionWithSnapshot(t, broker, AgentSnapshot{
		NodeID: domain.NodeID(nodeID),
		OS:     "linux",
		Arch:   "amd64",
	})
}

func startReadyBrokerSessionWithSnapshot(
	t *testing.T,
	broker *AgentBroker,
	snapshot AgentSnapshot,
) (*fakeAgentSession, <-chan error) {
	t.Helper()
	nodeID := string(snapshot.NodeID)
	session := newFakeAgentSession(nodeID)
	result := make(chan error, 1)
	go func() {
		result <- broker.serveSession(context.Background(), session)
	}()
	session.send(brokerEnvelope(t, "hello-1", transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: nodeID}))
	assertBrokerAck(t, session, "hello-1")
	session.send(brokerEnvelope(t, "snapshot-1", transport.MessageSnapshot, snapshot))
	assertBrokerAck(t, session, "snapshot-1")
	eventuallyBroker(t, func() bool { return broker.ConnectedCount() == 1 })
	return session, result
}

func assertBrokerAck(t *testing.T, session *fakeAgentSession, wantMessageID string) {
	t.Helper()
	envelope := receiveBrokerWrite(t, session)
	if envelope.Type != transport.MessageAck {
		t.Fatalf("message type = %s, want ack", envelope.Type)
	}
	var payload struct {
		MessageID string `json:"messageId"`
	}
	if err := decodeStrictJSON(envelope.Payload, &payload); err != nil || payload.MessageID != wantMessageID {
		t.Fatalf("ack payload = %q, error = %v", envelope.Payload, err)
	}
}

func receiveBrokerWrite(t *testing.T, session *fakeAgentSession) transport.Envelope {
	t.Helper()
	select {
	case envelope := <-session.writes:
		return envelope
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for broker write")
		return transport.Envelope{}
	}
}

func eventuallyBroker(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("broker condition was not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

func sendCommandAck(t *testing.T, session *fakeAgentSession, envelopeID, commandID string) {
	t.Helper()
	session.send(brokerEnvelope(t, envelopeID, transport.MessageAck, struct {
		MessageID string `json:"messageId"`
	}{MessageID: commandID}))
}

func sendExecutionUpdate(
	t *testing.T,
	session *fakeAgentSession,
	envelopeID string,
	update transport.ExecutionUpdate,
) {
	t.Helper()
	payload, err := transport.EncodeExecutionUpdate(update)
	if err != nil {
		t.Fatal(err)
	}
	session.send(transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       envelopeID,
		Type:            transport.MessageExecutionUpdate,
		Payload:         payload,
	})
}

func TestAgentBrokerStartAndAsynchronousLifecycleUpdates(t *testing.T) {
	consumer := newRecordingUpdateConsumer()
	commandConsumer := &recordingCommandConsumer{}
	consumers := acceptingAgentConsumers(consumer)
	consumers.Commands = commandConsumer
	broker := NewAgentBroker(1, consumers)
	session, serveResult := startReadyBrokerSession(t, broker, "node-1")
	prepareMetadata := brokerMetadata("command-prepare", "execution-1")
	const canary = "jit-canary.example.test"

	type commandResult struct {
		update transport.ExecutionUpdate
		err    error
	}
	prepareResult := make(chan commandResult, 1)
	go func() {
		update, err := broker.SendPrepare(context.Background(), "node-1", prepareMetadata, true)
		prepareResult <- commandResult{update: update, err: err}
	}()
	prepare := receiveBrokerWrite(t, session)
	if prepare.Type != transport.MessagePrepare || prepare.MessageID != string(prepareMetadata.CommandID) ||
		bytes.Contains(prepare.Payload, []byte(canary)) {
		t.Fatalf("prepare command = %#v", prepare)
	}
	prepareRecord := commandConsumer.record(0)
	if prepareRecord.NodeID != "node-1" || prepareRecord.Kind != transport.MessagePrepare ||
		prepareRecord.Metadata != prepareMetadata ||
		prepareRecord.PayloadDigest != transport.PayloadDigest(transport.MessagePrepare, prepare.Payload) {
		t.Fatalf("prepare durable record = %#v", prepareRecord)
	}
	decodedPrepare, err := transport.DecodePrepareCommand(prepare.Payload)
	if err != nil || decodedPrepare.ReplayIdentity(prepare.Payload).PayloadDigest != hex.EncodeToString(prepareRecord.PayloadDigest[:]) {
		t.Fatalf("prepare replay identity mismatch: %v", err)
	}
	sendCommandAck(t, session, "agent-ack-prepare", string(prepareMetadata.CommandID))
	preparing := transport.ExecutionUpdate{
		NodeID:      "node-1",
		CommandID:   prepareMetadata.CommandID,
		ExecutionID: prepareMetadata.ExecutionID,
		State:       domain.ExecutionPreparing,
	}
	sendExecutionUpdate(t, session, "update-preparing", preparing)
	assertBrokerAck(t, session, "update-preparing")
	prepared := <-prepareResult
	if prepared.err != nil || prepared.update != preparing {
		t.Fatalf("prepare result = %#v, %v", prepared.update, prepared.err)
	}

	metadata := brokerStartMetadata("command-start", "execution-1")
	result := make(chan commandResult, 1)
	go func() {
		update, err := broker.SendStart(context.Background(), "node-1", metadata, true, brokerJITConfig(canary))
		result <- commandResult{update: update, err: err}
	}()

	command := receiveBrokerWrite(t, session)
	if command.Type != transport.MessageStart || command.MessageID != string(metadata.CommandID) ||
		!bytes.Contains(command.Payload, []byte(canary)) {
		t.Fatalf("start command = %#v", command)
	}
	startRecord := commandConsumer.record(1)
	if startRecord.NodeID != "node-1" || startRecord.Kind != transport.MessageStart ||
		startRecord.Metadata != metadata ||
		startRecord.PayloadDigest != transport.PayloadDigest(transport.MessageStart, command.Payload) {
		t.Fatalf("start durable record = %#v", startRecord)
	}
	decodedStart, err := transport.DecodeStartCommand(command.Payload)
	if err != nil || decodedStart.ReplayIdentity(command.Payload).PayloadDigest != hex.EncodeToString(startRecord.PayloadDigest[:]) {
		t.Fatalf("start replay identity mismatch: %v", err)
	}
	decodedStart.Discard()
	sendCommandAck(t, session, "agent-ack-1", string(metadata.CommandID))
	running := transport.ExecutionUpdate{
		NodeID:      "node-1",
		CommandID:   metadata.CommandID,
		ExecutionID: metadata.ExecutionID,
		State:       domain.ExecutionRunning,
	}
	sendExecutionUpdate(t, session, "update-running", running)
	assertBrokerAck(t, session, "update-running")
	runningPayload, err := transport.EncodeExecutionUpdate(running)
	if err != nil {
		t.Fatal(err)
	}
	runningRecord := consumer.record(1)
	if runningRecord.MessageID != "update-running" || runningRecord.Update != running ||
		runningRecord.PayloadDigest != transport.PayloadDigest(transport.MessageExecutionUpdate, runningPayload) {
		t.Fatalf("running durable record = %#v", runningRecord)
	}
	got := <-result
	if got.err != nil || got.update != running {
		t.Fatalf("start result = %#v, %v", got.update, got.err)
	}

	for index, state := range []domain.ExecutionState{domain.ExecutionCleaning, domain.ExecutionReleased} {
		update := running
		update.State = state
		messageID := fmt.Sprintf("update-followup-%d", index)
		sendExecutionUpdate(t, session, messageID, update)
		assertBrokerAck(t, session, messageID)
	}
	eventuallyBroker(t, func() bool { return consumer.count() == 4 })
	consumer.mu.Lock()
	messageIDs := append([]string(nil), consumer.messageIDs...)
	consumer.mu.Unlock()
	if fmt.Sprint(messageIDs) != "[update-preparing update-running update-followup-0 update-followup-1]" {
		t.Fatalf("durably consumed update message IDs = %v", messageIDs)
	}

	clear(command.Payload)
	command.Payload = nil
	for _, rendered := range []string{
		fmt.Sprintf("%#v", broker),
		string(mustJSON(t, broker)),
		slog.AnyValue(broker).String(),
		fmt.Sprintf("%#v", prepareRecord),
		fmt.Sprintf("%#v", startRecord),
	} {
		if bytes.Contains([]byte(rendered), []byte(canary)) {
			t.Fatalf("broker rendering retained JIT canary: %q", rendered)
		}
	}

	session.disconnect()
	if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("serve result = %v", err)
	}
}

func TestAgentBrokerForgetsFinishedExecutionCommandsOnlyAfterCommitAndAck(t *testing.T) {
	tests := []struct {
		name      string
		state     domain.ExecutionState
		errorCode domain.ExecutionErrorCode
		wantCount int
	}{
		{
			name:      "released",
			state:     domain.ExecutionReleased,
			errorCode: domain.ExecutionErrorNone,
			wantCount: 1,
		},
		{
			name:      "failed",
			state:     domain.ExecutionFailed,
			errorCode: domain.ExecutionErrorStart,
			wantCount: 1,
		},
		{
			name:      "cleanup failed",
			state:     domain.ExecutionCleanupFailed,
			errorCode: domain.ExecutionErrorCleanup,
			wantCount: 3,
		},
		{
			name:      "quarantined",
			state:     domain.ExecutionQuarantined,
			errorCode: domain.ExecutionErrorQuarantined,
			wantCount: 3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := newRecordingUpdateConsumer()
			broker := NewAgentBroker(1, acceptingAgentConsumers(consumer))
			session, serveResult := startReadyBrokerSessionWithSnapshot(
				t, broker, brokerKnownCommandsSnapshot())
			actor, ok := broker.session("node-1")
			if !ok {
				t.Fatal("ready Agent actor not found")
			}

			sendExecutionUpdate(t, session, "terminal-update", transport.ExecutionUpdate{
				NodeID:      "node-1",
				CommandID:   "command-start",
				ExecutionID: "execution-finished",
				State:       test.state,
				ErrorCode:   test.errorCode,
			})
			assertBrokerAck(t, session, "terminal-update")

			var known map[domain.CommandID]domain.ExecutionID
			eventuallyBroker(t, func() bool {
				known = brokerKnownCommands(actor)
				return len(known) == test.wantCount
			})
			if known["command-unrelated"] != "execution-active" {
				t.Fatalf("unrelated command was removed: %#v", known)
			}
			_, prepareKnown := known["command-prepare"]
			_, startKnown := known["command-start"]
			wantFinishedKnown := test.state == domain.ExecutionCleanupFailed ||
				test.state == domain.ExecutionQuarantined
			if prepareKnown != wantFinishedKnown || startKnown != wantFinishedKnown {
				t.Fatalf("finished execution commands retained=(%t,%t), want %t: %#v",
					prepareKnown, startKnown, wantFinishedKnown, known)
			}
			if consumer.count() != 1 {
				t.Fatalf("durable consumer commits = %d, want 1", consumer.count())
			}

			session.disconnect()
			if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
				t.Fatalf("serve result = %v", err)
			}
		})
	}
}

func TestAgentBrokerRetainsFinishedExecutionCommandsWhenAckFails(t *testing.T) {
	for _, test := range []struct {
		name      string
		state     domain.ExecutionState
		errorCode domain.ExecutionErrorCode
	}{
		{name: "released", state: domain.ExecutionReleased},
		{name: "failed", state: domain.ExecutionFailed, errorCode: domain.ExecutionErrorStart},
	} {
		t.Run(test.name, func(t *testing.T) {
			consumer := newRecordingUpdateConsumer()
			broker := NewAgentBroker(1, acceptingAgentConsumers(consumer))
			session, serveResult := startReadyBrokerSessionWithSnapshot(
				t, broker, brokerKnownCommandsSnapshot())
			actor, ok := broker.session("node-1")
			if !ok {
				t.Fatal("ready Agent actor not found")
			}
			session.failNextWrite(errors.New("ack write failed"))

			sendExecutionUpdate(t, session, "terminal-ack-failure", transport.ExecutionUpdate{
				NodeID:      "node-1",
				CommandID:   "command-start",
				ExecutionID: "execution-finished",
				State:       test.state,
				ErrorCode:   test.errorCode,
			})
			if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
				t.Fatalf("serve result = %v", err)
			}
			if consumer.count() != 1 {
				t.Fatalf("durable consumer commits = %d, want 1", consumer.count())
			}
			assertFinishedExecutionCommandsRetained(t, actor)
			select {
			case envelope := <-session.writes:
				t.Fatalf("failed ACK reached peer: %#v", envelope)
			case <-time.After(20 * time.Millisecond):
			}
		})
	}
}

func TestAgentBrokerRetainsFinishedExecutionCommandsWhenConsumerFails(t *testing.T) {
	for _, test := range []struct {
		name      string
		state     domain.ExecutionState
		errorCode domain.ExecutionErrorCode
	}{
		{name: "released", state: domain.ExecutionReleased},
		{name: "failed", state: domain.ExecutionFailed, errorCode: domain.ExecutionErrorStart},
	} {
		t.Run(test.name, func(t *testing.T) {
			consumer := newRecordingUpdateConsumer()
			consumer.err = errors.New("durable consumer failed")
			broker := NewAgentBroker(1, acceptingAgentConsumers(consumer))
			session, serveResult := startReadyBrokerSessionWithSnapshot(
				t, broker, brokerKnownCommandsSnapshot())
			actor, ok := broker.session("node-1")
			if !ok {
				t.Fatal("ready Agent actor not found")
			}

			sendExecutionUpdate(t, session, "terminal-consumer-failure", transport.ExecutionUpdate{
				NodeID:      "node-1",
				CommandID:   "command-start",
				ExecutionID: "execution-finished",
				State:       test.state,
				ErrorCode:   test.errorCode,
			})
			if err := <-serveResult; !errors.Is(err, ErrExecutionUpdateCommit) {
				t.Fatalf("serve result = %v", err)
			}
			if consumer.count() != 1 {
				t.Fatalf("durable consumer attempts = %d, want 1", consumer.count())
			}
			assertFinishedExecutionCommandsRetained(t, actor)
			select {
			case envelope := <-session.writes:
				t.Fatalf("rejected update was acknowledged: %#v", envelope)
			case <-time.After(20 * time.Millisecond):
			}
		})
	}
}

func brokerKnownCommandsSnapshot() AgentSnapshot {
	return AgentSnapshot{
		NodeID:             "node-1",
		OS:                 "linux",
		Arch:               "amd64",
		MaxControllerEpoch: 1,
		Commands: []domain.Command{
			{
				ID:              "command-prepare",
				ControllerEpoch: 1,
				ExecutionID:     "execution-finished",
				ExpectedState:   domain.ExecutionReserved,
				PayloadDigest:   domain.PayloadDigest([]byte("command-prepare")),
			},
			{
				ID:              "command-start",
				ControllerEpoch: 1,
				ExecutionID:     "execution-finished",
				ExpectedState:   domain.ExecutionPreparing,
				PayloadDigest:   domain.PayloadDigest([]byte("command-start")),
			},
			{
				ID:              "command-unrelated",
				ControllerEpoch: 1,
				ExecutionID:     "execution-active",
				ExpectedState:   domain.ExecutionRunning,
				PayloadDigest:   domain.PayloadDigest([]byte("command-unrelated")),
			},
		},
	}
}

func brokerKnownCommands(actor *agentSessionActor) map[domain.CommandID]domain.ExecutionID {
	actor.stateMu.Lock()
	defer actor.stateMu.Unlock()
	known := make(map[domain.CommandID]domain.ExecutionID, len(actor.knownCommands))
	for commandID, executionID := range actor.knownCommands {
		known[commandID] = executionID
	}
	return known
}

func assertFinishedExecutionCommandsRetained(t *testing.T, actor *agentSessionActor) {
	t.Helper()
	known := brokerKnownCommands(actor)
	if len(known) != 3 ||
		known["command-prepare"] != "execution-finished" ||
		known["command-start"] != "execution-finished" ||
		known["command-unrelated"] != "execution-active" {
		t.Fatalf("known commands changed before commit and ACK completed: %#v", known)
	}
}

func TestAgentBrokerRejectsMismatchedCommandAck(t *testing.T) {
	consumer := newRecordingUpdateConsumer()
	broker := NewAgentBroker(1, acceptingAgentConsumers(consumer))
	session, serveResult := startReadyBrokerSession(t, broker, "node-1")
	metadata := brokerCancelMetadata("command-mismatch", "execution-1")
	result := make(chan error, 1)
	go func() {
		_, err := broker.SendCancel(context.Background(), "node-1", metadata)
		result <- err
	}()
	_ = receiveBrokerWrite(t, session)
	sendCommandAck(t, session, "agent-ack-wrong", "another-command")
	if err := <-serveResult; !errors.Is(err, ErrAgentProtocol) {
		t.Fatalf("serve result = %v", err)
	}
	if err := <-result; !errors.Is(err, ErrAgentProtocol) {
		t.Fatalf("command result = %v", err)
	}
	if consumer.count() != 0 {
		t.Fatalf("consumer received %d invalid updates", consumer.count())
	}
}

func TestAgentBrokerRejectsUnknownAndMismatchedMessages(t *testing.T) {
	tests := []struct {
		name   string
		attack func(*testing.T, *fakeAgentSession, transport.CommandMetadata)
	}{
		{
			name: "unknown acknowledgement",
			attack: func(t *testing.T, session *fakeAgentSession, _ transport.CommandMetadata) {
				sendCommandAck(t, session, "unknown-ack", "never-sent")
			},
		},
		{
			name: "mismatched execution update",
			attack: func(t *testing.T, session *fakeAgentSession, metadata transport.CommandMetadata) {
				sendCommandAck(t, session, "valid-command-ack", string(metadata.CommandID))
				sendExecutionUpdate(t, session, "bad-update", transport.ExecutionUpdate{
					NodeID:      "node-1",
					CommandID:   metadata.CommandID,
					ExecutionID: "another-execution",
					State:       domain.ExecutionRunning,
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := newRecordingUpdateConsumer()
			broker := NewAgentBroker(1, acceptingAgentConsumers(consumer))
			session, serveResult := startReadyBrokerSession(t, broker, "node-1")
			metadata := brokerCancelMetadata("command-attack", "execution-attack")
			var commandResult chan error
			if test.name == "mismatched execution update" {
				commandResult = make(chan error, 1)
				go func() {
					_, err := broker.SendCancel(context.Background(), "node-1", metadata)
					commandResult <- err
				}()
				_ = receiveBrokerWrite(t, session)
			}
			test.attack(t, session, metadata)
			if err := <-serveResult; !errors.Is(err, ErrAgentProtocol) {
				t.Fatalf("serve result = %v", err)
			}
			if commandResult != nil {
				if err := <-commandResult; !errors.Is(err, ErrAgentProtocol) {
					t.Fatalf("command result = %v", err)
				}
			}
			if consumer.count() != 0 {
				t.Fatalf("consumer received %d invalid updates", consumer.count())
			}
		})
	}
}

func TestAgentBrokerReconcilesAcceptedSnapshotCommandAndExactOutboxReplay(t *testing.T) {
	consumer := &exactDedupUpdateConsumer{seen: make(map[string]AgentExecutionUpdateRecord)}
	broker := NewAgentBroker(1, acceptingAgentConsumers(consumer))
	command := domain.Command{
		ID:              "accepted-command",
		ControllerEpoch: 1,
		ExecutionID:     "execution-accepted",
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("accepted-command-payload")),
	}
	session, serveResult := startReadyBrokerSessionWithSnapshot(t, broker, AgentSnapshot{
		NodeID:             "node-1",
		OS:                 "linux",
		Arch:               "amd64",
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{command},
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        command.ExecutionID,
			State:              domain.ExecutionRunning,
			ObservedAtUnixNano: 1,
		}},
	})
	update := transport.ExecutionUpdate{
		NodeID:      "node-1",
		CommandID:   command.ID,
		ExecutionID: command.ExecutionID,
		State:       domain.ExecutionRunning,
	}
	canonicalPayload, err := transport.EncodeExecutionUpdate(update)
	if err != nil {
		t.Fatal(err)
	}
	exactPayload := append(append(json.RawMessage(nil), canonicalPayload...), ' ', '\n')
	for range 2 {
		session.send(transport.Envelope{
			ProtocolVersion: transport.ProtocolVersion,
			MessageID:       "outbox-message-1",
			Type:            transport.MessageExecutionUpdate,
			Payload:         append(json.RawMessage(nil), exactPayload...),
		})
		assertBrokerAck(t, session, "outbox-message-1")
	}
	consumer.mu.Lock()
	attempts, committed := consumer.attempts, consumer.committed
	committedRecord := consumer.seen["outbox-message-1"]
	consumer.mu.Unlock()
	if attempts != 2 || committed != 1 {
		t.Fatalf("outbox dedupe attempts=%d committed=%d", attempts, committed)
	}
	if committedRecord.PayloadDigest != transport.PayloadDigest(transport.MessageExecutionUpdate, exactPayload) ||
		committedRecord.PayloadDigest == transport.PayloadDigest(transport.MessageExecutionUpdate, canonicalPayload) {
		t.Fatalf("outbox payload digest did not preserve exact wire identity: %x", committedRecord.PayloadDigest)
	}

	mismatched := update
	mismatched.State = domain.ExecutionCleaning
	sendExecutionUpdate(t, session, "outbox-message-1", mismatched)
	if err := <-serveResult; !errors.Is(err, ErrExecutionUpdateCommit) {
		t.Fatalf("mismatched outbox replay result = %v", err)
	}
	select {
	case envelope := <-session.writes:
		t.Fatalf("mismatched outbox replay was acknowledged: %#v", envelope)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestAgentBrokerFailsClosedWhenDurableConsumerRejectsDuplicateLifecycleUpdate(t *testing.T) {
	consumer := newRecordingUpdateConsumer()
	broker := NewAgentBroker(1, acceptingAgentConsumers(consumer))
	session, serveResult := startReadyBrokerSession(t, broker, "node-1")
	metadata := brokerCancelMetadata("command-duplicate-update", "execution-1")
	commandResult := make(chan error, 1)
	go func() {
		_, err := broker.SendCancel(context.Background(), "node-1", metadata)
		commandResult <- err
	}()
	_ = receiveBrokerWrite(t, session)
	sendCommandAck(t, session, "duplicate-update-command-ack", string(metadata.CommandID))
	update := transport.ExecutionUpdate{
		NodeID:      "node-1",
		CommandID:   metadata.CommandID,
		ExecutionID: metadata.ExecutionID,
		State:       domain.ExecutionCleaning,
	}
	sendExecutionUpdate(t, session, "first-update-envelope", update)
	assertBrokerAck(t, session, "first-update-envelope")
	if err := <-commandResult; err != nil {
		t.Fatalf("command result = %v", err)
	}
	consumer.mu.Lock()
	consumer.err = errors.New("duplicate lifecycle transition")
	consumer.mu.Unlock()
	sendExecutionUpdate(t, session, "duplicate-update-envelope", update)
	if err := <-serveResult; !errors.Is(err, ErrExecutionUpdateCommit) {
		t.Fatalf("duplicate update result = %v", err)
	}
	select {
	case envelope := <-session.writes:
		t.Fatalf("duplicate update was acknowledged: %#v", envelope)
	case <-time.After(20 * time.Millisecond):
	}
	if consumer.count() != 2 {
		t.Fatalf("consumer attempts = %d, want 2", consumer.count())
	}
}

func TestAgentBrokerDoesNotAcknowledgeConsumerFailure(t *testing.T) {
	consumer := newRecordingUpdateConsumer()
	consumer.err = errors.New("durable mutation failed")
	broker := NewAgentBroker(1, acceptingAgentConsumers(consumer))
	session, serveResult := startReadyBrokerSession(t, broker, "node-1")
	metadata := brokerCancelMetadata("command-consumer-failure", "execution-1")
	commandResult := make(chan error, 1)
	go func() {
		_, err := broker.SendCancel(context.Background(), "node-1", metadata)
		commandResult <- err
	}()
	_ = receiveBrokerWrite(t, session)
	sendCommandAck(t, session, "consumer-command-ack", string(metadata.CommandID))
	sendExecutionUpdate(t, session, "consumer-update", transport.ExecutionUpdate{
		NodeID:      "node-1",
		CommandID:   metadata.CommandID,
		ExecutionID: metadata.ExecutionID,
		State:       domain.ExecutionCleaning,
	})
	if err := <-serveResult; !errors.Is(err, ErrExecutionUpdateCommit) {
		t.Fatalf("consumer failure session result = %v", err)
	}
	if err := <-commandResult; !errors.Is(err, ErrExecutionUpdateCommit) {
		t.Fatalf("consumer failure command result = %v", err)
	}
	select {
	case envelope := <-session.writes:
		t.Fatalf("rejected update was acknowledged: %#v", envelope)
	case <-time.After(20 * time.Millisecond):
	}
	if consumer.count() != 1 {
		t.Fatalf("consumer attempts = %d, want 1", consumer.count())
	}
}

func TestAgentBrokerCommitsCommandDigestBeforeWriteAndFailsClosed(t *testing.T) {
	updates := newRecordingUpdateConsumer()
	commands := &recordingCommandConsumer{err: errors.New("controller store unavailable")}
	consumers := acceptingAgentConsumers(updates)
	consumers.Commands = commands
	broker := NewAgentBroker(1, consumers)
	session, serveResult := startReadyBrokerSession(t, broker, "node-1")
	const canary = "command-commit-jit-canary.example.test"
	metadata := brokerStartMetadata("command-store-failure", "execution-1")
	if _, err := broker.SendStart(context.Background(), "node-1", metadata, true, brokerJITConfig(canary)); !errors.Is(err, ErrAgentCommandCommit) {
		t.Fatalf("command commit failure = %v", err)
	}
	record := commands.record(0)
	if record.NodeID != "node-1" || record.Kind != transport.MessageStart || record.Metadata != metadata {
		t.Fatalf("command record = %#v", record)
	}
	expectedPayload, err := transport.EncodeStartCommandPayload(metadata, runner.OfficialRunnerVersion, true, canary)
	if err != nil {
		t.Fatal(err)
	}
	expectedDigest := transport.PayloadDigest(transport.MessageStart, expectedPayload)
	clear(expectedPayload)
	if record.PayloadDigest != expectedDigest {
		t.Fatalf("command payload digest = %x, want %x", record.PayloadDigest, expectedDigest)
	}
	select {
	case envelope := <-session.writes:
		t.Fatalf("uncommitted command reached network: %#v", envelope)
	case <-time.After(20 * time.Millisecond):
	}
	if rendered := fmt.Sprintf("%#v\n%#v", broker, record); bytes.Contains([]byte(rendered), []byte(canary)) {
		t.Fatalf("failed command commit retained JIT: %q", rendered)
	}
	replacement, replacementResult := startReadyBrokerSession(t, broker, "node-1")
	if err := <-serveResult; !errors.Is(err, ErrAgentSessionReplaced) {
		t.Fatalf("session result after fresh snapshot = %v", err)
	}
	replacement.disconnect()
	if err := <-replacementResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("replacement session result = %v", err)
	}
}

func TestAgentBrokerRequiresDurableSnapshotBeforeAckAndActivation(t *testing.T) {
	tests := []struct {
		name      string
		consumer  AgentSnapshotConsumer
		wantError error
	}{
		{name: "missing consumer", wantError: ErrAgentSnapshotConsumerRequired},
		{
			name: "consumer failure",
			consumer: AgentSnapshotConsumerFunc(func(context.Context, AgentSnapshot) error {
				return errors.New("snapshot store unavailable")
			}),
			wantError: ErrAgentSnapshotCommit,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
			consumers.Snapshot = test.consumer
			broker := NewAgentBroker(1, consumers)
			session := newFakeAgentSession("node-1")
			serveResult := make(chan error, 1)
			go func() {
				serveResult <- broker.serveSession(context.Background(), session)
			}()
			session.send(brokerEnvelope(t, "snapshot-gate-hello", transport.MessageHello, struct {
				NodeID string `json:"nodeId"`
			}{NodeID: "node-1"}))
			assertBrokerAck(t, session, "snapshot-gate-hello")
			session.send(brokerEnvelope(t, "snapshot-gate-snapshot", transport.MessageSnapshot, AgentSnapshot{
				NodeID: "node-1",
				OS:     "linux",
				Arch:   "amd64",
			}))
			if err := <-serveResult; !errors.Is(err, test.wantError) {
				t.Fatalf("snapshot gate result = %v", err)
			}
			if broker.ConnectedCount() != 0 {
				t.Fatalf("uncommitted snapshot activated node: %d", broker.ConnectedCount())
			}
			select {
			case envelope := <-session.writes:
				t.Fatalf("uncommitted snapshot was acknowledged: %#v", envelope)
			case <-time.After(20 * time.Millisecond):
			}
		})
	}
}

func TestAgentBrokerDisconnectAndContextTimeoutFailPendingCommands(t *testing.T) {
	t.Run("disconnect", func(t *testing.T) {
		broker := NewAgentBroker(1, acceptingAgentConsumers(newRecordingUpdateConsumer()))
		session, serveResult := startReadyBrokerSession(t, broker, "node-1")
		result := make(chan error, 1)
		go func() {
			_, err := broker.SendCancel(context.Background(), "node-1", brokerCancelMetadata("command-disconnect", "execution-1"))
			result <- err
		}()
		_ = receiveBrokerWrite(t, session)
		session.disconnect()
		if err := <-result; !errors.Is(err, ErrAgentDisconnected) {
			t.Fatalf("command result = %v", err)
		}
		if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
			t.Fatalf("serve result = %v", err)
		}
	})

	t.Run("start write is never retried after ambiguous disconnect", func(t *testing.T) {
		broker := NewAgentBroker(1, acceptingAgentConsumers(newRecordingUpdateConsumer()))
		session, serveResult := startReadyBrokerSession(t, broker, "node-1")
		const canary = "ambiguous-jit-canary.example.test"
		result := make(chan error, 1)
		go func() {
			_, err := broker.SendStart(
				context.Background(),
				"node-1",
				brokerStartMetadata("command-ambiguous", "execution-1"),
				true,
				brokerJITConfig(canary),
			)
			result <- err
		}()
		command := receiveBrokerWrite(t, session)
		if command.Type != transport.MessageStart || !bytes.Contains(command.Payload, []byte(canary)) {
			t.Fatalf("ambiguous start command = %#v", command)
		}
		clear(command.Payload)
		command.Payload = nil
		rendered := fmt.Sprintf("%#v", broker)
		if bytes.Contains([]byte(rendered), []byte(canary)) {
			t.Fatalf("broker retained JIT after network write: %q", rendered)
		}
		session.disconnect()
		if err := <-result; !errors.Is(err, ErrAgentDisconnected) {
			t.Fatalf("ambiguous start result = %v", err)
		}
		if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
			t.Fatalf("ambiguous session result = %v", err)
		}
		select {
		case retried := <-session.writes:
			t.Fatalf("ambiguous JIT command was retried: %#v", retried)
		case <-time.After(20 * time.Millisecond):
		}
	})

	t.Run("timeout closes ambiguous session", func(t *testing.T) {
		broker := NewAgentBroker(1, acceptingAgentConsumers(newRecordingUpdateConsumer()))
		session, serveResult := startReadyBrokerSession(t, broker, "node-1")
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := broker.SendCancel(ctx, "node-1", brokerCancelMetadata("command-timeout", "execution-1"))
			result <- err
		}()
		_ = receiveBrokerWrite(t, session)
		if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("command result = %v", err)
		}
		if err := <-serveResult; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("serve result = %v", err)
		}
		eventuallyBroker(t, func() bool { return broker.ConnectedCount() == 0 })
		replacement, replacementResult := startReadyBrokerSession(
			t,
			broker,
			"node-1",
		)
		replacement.disconnect()
		if err := <-replacementResult; !errors.Is(err, ErrAgentDisconnected) {
			t.Fatalf("replacement session result = %v", err)
		}
	})
}

func TestAgentBrokerSerializesCommandAndHeartbeatWriters(t *testing.T) {
	consumer := newRecordingUpdateConsumer()
	broker := NewAgentBroker(1, acceptingAgentConsumers(consumer))
	session, serveResult := startReadyBrokerSession(t, broker, "node-1")
	for {
		select {
		case <-session.writeEntered:
		default:
			goto writeSignalsDrained
		}
	}
writeSignalsDrained:
	block := make(chan struct{})
	session.setWriteBlock(block)
	metadata := brokerCancelMetadata("command-concurrent", "execution-1")
	result := make(chan error, 1)
	go func() {
		_, err := broker.SendCancel(context.Background(), "node-1", metadata)
		result <- err
	}()
	select {
	case <-session.writeEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("command writer did not enter")
	}
	session.send(brokerEnvelope(t, "heartbeat-concurrent", transport.MessageHeartbeat, struct {
		NodeID            string `json:"nodeId"`
		NativeRunnerReady bool   `json:"nativeRunnerReady"`
	}{NodeID: "node-1", NativeRunnerReady: false}))
	time.Sleep(10 * time.Millisecond)
	if maximum := session.maxWrites.Load(); maximum != 1 {
		t.Fatalf("concurrent writes before release = %d", maximum)
	}
	close(block)
	command := receiveBrokerWrite(t, session)
	if command.Type != transport.MessageCancel {
		t.Fatalf("first write type = %s", command.Type)
	}
	assertBrokerAck(t, session, "heartbeat-concurrent")
	session.setWriteBlock(nil)
	sendCommandAck(t, session, "command-ack", string(metadata.CommandID))
	update := transport.ExecutionUpdate{
		NodeID:      "node-1",
		CommandID:   metadata.CommandID,
		ExecutionID: metadata.ExecutionID,
		State:       domain.ExecutionCleaning,
	}
	sendExecutionUpdate(t, session, "command-update", update)
	assertBrokerAck(t, session, "command-update")
	if err := <-result; err != nil {
		t.Fatalf("command result = %v", err)
	}
	if maximum := session.maxWrites.Load(); maximum != 1 {
		t.Fatalf("maximum concurrent writes = %d", maximum)
	}
	session.disconnect()
	_ = <-serveResult
}

func TestAgentBrokerCommitsReadinessChangeBeforeHeartbeatAck(t *testing.T) {
	release := make(chan struct{})
	snapshots := &recordingSnapshotConsumer{
		entered:   make(chan int, 4),
		blockCall: 2,
		release:   release,
	}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Snapshot = snapshots
	consumers.Readiness = snapshots
	broker := NewAgentBrokerWithOptions(1, consumers, AgentBrokerOptions{
		ReadinessLease: time.Second,
	})
	session, serveResult := startReadyBrokerSessionWithSnapshot(t, broker, AgentSnapshot{
		NodeID:            "node-1",
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		NativeRunnerReady: true,
	})
	_, _, changed := broker.Readiness("node-1")
	session.send(brokerEnvelope(t, "heartbeat-false", transport.MessageHeartbeat, transport.AgentHeartbeat{
		NodeID:            "node-1",
		NativeRunnerReady: false,
	}))
	select {
	case call := <-snapshots.entered:
		if call == 1 {
			call = <-snapshots.entered
		}
		if call != 2 {
			t.Fatalf("snapshot commit call = %d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("readiness snapshot did not reach durable consumer")
	}
	select {
	case envelope := <-session.writes:
		t.Fatalf("heartbeat acknowledged before durable readiness commit: %#v", envelope)
	default:
	}
	close(release)
	assertBrokerAck(t, session, "heartbeat-false")
	select {
	case <-changed.Done():
	case <-time.After(time.Second):
		t.Fatal("readiness change context was not canceled")
	}
	snapshot, online, _ := broker.Readiness("node-1")
	if !online || snapshot.NativeRunnerReady || snapshots.last().NativeRunnerReady {
		t.Fatalf("readiness after false heartbeat = online:%t memory:%t durable:%t",
			online, snapshot.NativeRunnerReady, snapshots.last().NativeRunnerReady)
	}
	session.disconnect()
	if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("serve result = %v", err)
	}
}

func TestAgentBrokerHeartbeatNeverReplaysAnOlderFullJournal(t *testing.T) {
	fullSnapshots := make(chan AgentSnapshot, 2)
	type readinessRecord struct {
		nodeID domain.NodeID
		digest string
		ready  bool
	}
	readinessChanges := make(chan readinessRecord, 1)
	updates := newRecordingUpdateConsumer()
	consumers := acceptingAgentConsumers(updates)
	consumers.Snapshot = AgentSnapshotConsumerFunc(func(
		_ context.Context,
		snapshot AgentSnapshot,
	) error {
		fullSnapshots <- cloneAgentSnapshot(snapshot)
		return nil
	})
	consumers.Readiness = AgentReadinessConsumerFunc(func(
		_ context.Context,
		nodeID domain.NodeID,
		digest string,
		ready bool,
	) error {
		readinessChanges <- readinessRecord{
			nodeID: nodeID,
			digest: digest,
			ready:  ready,
		}
		return nil
	})
	broker := NewAgentBrokerWithOptions(1, consumers, AgentBrokerOptions{
		ReadinessLease: time.Second,
	})
	command := domain.Command{
		ID:              "start-readiness-journal",
		ControllerEpoch: 1,
		ExecutionID:     "execution-readiness-journal",
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start-readiness-journal")),
	}
	initial := AgentSnapshot{
		NodeID:             "node-1",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		RunnerVersion:      runner.OfficialRunnerVersion,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{command},
	}
	session, serveResult := startReadyBrokerSessionWithSnapshot(t, broker, initial)
	select {
	case accepted := <-fullSnapshots:
		if accepted.NativeRunnerReady != initial.NativeRunnerReady ||
			len(accepted.Commands) != 1 || accepted.Commands[0] != command {
			t.Fatalf("accepted full snapshot = %#v", accepted)
		}
	case <-time.After(time.Second):
		t.Fatal("initial full snapshot was not committed")
	}

	sendExecutionUpdate(t, session, "running-readiness-journal", transport.ExecutionUpdate{
		NodeID:      initial.NodeID,
		CommandID:   command.ID,
		ExecutionID: command.ExecutionID,
		State:       domain.ExecutionRunning,
	})
	assertBrokerAck(t, session, "running-readiness-journal")
	eventuallyBroker(t, func() bool { return updates.count() == 1 })

	session.send(brokerEnvelope(t, "heartbeat-readiness-journal", transport.MessageHeartbeat, transport.AgentHeartbeat{
		NodeID:            initial.NodeID,
		NativeRunnerReady: false,
	}))
	assertBrokerAck(t, session, "heartbeat-readiness-journal")
	expectedDigest, err := transport.AgentSnapshotDigest(initial)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case change := <-readinessChanges:
		if change.nodeID != initial.NodeID || change.digest != expectedDigest ||
			change.ready {
			t.Fatalf("readiness-only authority = %#v", change)
		}
	case <-time.After(time.Second):
		t.Fatal("readiness-only change was not committed")
	}
	select {
	case replayed := <-fullSnapshots:
		t.Fatalf("heartbeat replayed stale full journal: %#v", replayed)
	default:
	}

	session.disconnect()
	if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("serve result = %v", err)
	}
}

func TestAgentBrokerReadinessLeaseExpiresWithoutDroppingOnlineSnapshot(t *testing.T) {
	snapshots := &recordingSnapshotConsumer{}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Snapshot = snapshots
	consumers.Readiness = snapshots
	broker := NewAgentBrokerWithOptions(1, consumers, AgentBrokerOptions{
		ReadinessLease: 30 * time.Millisecond,
	})
	session, serveResult := startReadyBrokerSessionWithSnapshot(t, broker, AgentSnapshot{
		NodeID:            "node-1",
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		NativeRunnerReady: true,
	})
	_, _, changed := broker.Readiness("node-1")
	select {
	case <-changed.Done():
	case <-time.After(time.Second):
		t.Fatal("readiness lease did not expire")
	}
	snapshot, online, _ := broker.Readiness("node-1")
	if !online || snapshot.NativeRunnerReady {
		t.Fatalf("expired readiness = online:%t ready:%t", online, snapshot.NativeRunnerReady)
	}
	if snapshots.last().NativeRunnerReady {
		t.Fatal("expired readiness was not durably observed")
	}
	session.disconnect()
	if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("serve result = %v", err)
	}
}

func TestAgentBrokerHealthyHeartbeatRenewsLeaseWithoutCancelingLongPollContext(t *testing.T) {
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	broker := NewAgentBrokerWithOptions(1, consumers, AgentBrokerOptions{
		ReadinessLease: 100 * time.Millisecond,
	})
	session, serveResult := startReadyBrokerSessionWithSnapshot(t, broker, AgentSnapshot{
		NodeID:            "node-1",
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		NativeRunnerReady: true,
	})
	_, _, changed := broker.Readiness("node-1")
	time.Sleep(60 * time.Millisecond)
	session.send(brokerEnvelope(t, "heartbeat-renew", transport.MessageHeartbeat, transport.AgentHeartbeat{
		NodeID:            "node-1",
		NativeRunnerReady: true,
	}))
	assertBrokerAck(t, session, "heartbeat-renew")
	time.Sleep(60 * time.Millisecond)
	select {
	case <-changed.Done():
		t.Fatal("healthy renewal canceled the readiness context or retained the old deadline")
	default:
	}
	snapshot, online, renewedContext := broker.Readiness("node-1")
	if !online || !snapshot.NativeRunnerReady || renewedContext != changed {
		t.Fatalf("renewed readiness = online:%t ready:%t same-context:%t",
			online, snapshot.NativeRunnerReady, renewedContext == changed)
	}
	select {
	case <-changed.Done():
	case <-time.After(time.Second):
		t.Fatal("renewed readiness lease did not eventually expire")
	}
	session.disconnect()
	if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("serve result = %v", err)
	}
}

func TestAgentBrokerOfflineReadinessContextWakesOnReconnectWithoutBusyCancellation(t *testing.T) {
	broker := NewAgentBroker(1, acceptingAgentConsumers(newRecordingUpdateConsumer()))
	_, online, reconnect := broker.Readiness("node-1")
	if online {
		t.Fatal("never-connected node reported online")
	}
	select {
	case <-reconnect.Done():
		t.Fatal("offline readiness context was already canceled")
	default:
	}

	session, serveResult := startReadyBrokerSession(t, broker, "node-1")
	select {
	case <-reconnect.Done():
	case <-time.After(time.Second):
		t.Fatal("node connection did not wake offline readiness waiter")
	}
	session.disconnect()
	if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("serve result = %v", err)
	}
	_, online, nextReconnect := broker.Readiness("node-1")
	if online {
		t.Fatal("disconnected node reported online")
	}
	select {
	case <-nextReconnect.Done():
		t.Fatal("post-disconnect readiness context would cause a busy retry loop")
	default:
	}
}

func TestAgentBrokerRejectsStaleReplacementSnapshotCapturedAcrossCommand(t *testing.T) {
	broker := NewAgentBroker(1, acceptingAgentConsumers(newRecordingUpdateConsumer()))
	first, firstResult := startReadyBrokerSession(t, broker, "node-1")
	cancelMetadata := brokerCancelMetadata("command-replaced", "execution-1")
	commandResult := make(chan error, 1)
	go func() {
		_, err := broker.SendCancel(context.Background(), "node-1", cancelMetadata)
		commandResult <- err
	}()
	cancelCommand := receiveBrokerWrite(t, first)
	if cancelCommand.Type != transport.MessageCancel ||
		cancelCommand.MessageID != string(cancelMetadata.CommandID) {
		t.Fatalf("cancel command = %#v", cancelCommand)
	}

	partial := newFakeAgentSession("node-1")
	partialResult := make(chan error, 1)
	go func() { partialResult <- broker.serveSession(context.Background(), partial) }()
	partial.send(brokerEnvelope(t, "partial-hello", transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: "node-1"}))
	assertBrokerAck(t, partial, "partial-hello")
	if broker.ConnectedCount() != 1 {
		t.Fatalf("partial handshake changed connected count: %d", broker.ConnectedCount())
	}

	second := newFakeAgentSession("node-1")
	secondResult := make(chan error, 1)
	go func() { secondResult <- broker.serveSession(context.Background(), second) }()
	second.send(brokerEnvelope(t, "second-hello", transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: "node-1"}))
	assertBrokerAck(t, second, "second-hello")
	second.send(brokerEnvelope(t, "second-snapshot", transport.MessageSnapshot, AgentSnapshot{
		NodeID: "node-1",
		OS:     "linux",
		Arch:   "amd64",
	}))
	select {
	case envelope := <-second.writes:
		t.Fatalf("stale replacement snapshot was acknowledged: %#v", envelope)
	case err := <-secondResult:
		if !errors.Is(err, ErrAgentSessionReplaced) {
			t.Fatalf("stale replacement result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stale replacement snapshot was not rejected")
	}
	if broker.ConnectedCount() != 1 {
		t.Fatalf("stale replacement changed connected count: %d", broker.ConnectedCount())
	}

	sendCommandAck(t, first, "cancel-ack", string(cancelMetadata.CommandID))
	sendExecutionUpdate(t, first, "cancel-cleaning", transport.ExecutionUpdate{
		NodeID:      "node-1",
		CommandID:   cancelMetadata.CommandID,
		ExecutionID: cancelMetadata.ExecutionID,
		State:       domain.ExecutionCleaning,
	})
	assertBrokerAck(t, first, "cancel-cleaning")
	if err := <-commandResult; err != nil {
		t.Fatalf("old-session command result = %v", err)
	}

	third, thirdResult := startReadyBrokerSession(t, broker, "node-1")
	if err := <-firstResult; !errors.Is(err, ErrAgentSessionReplaced) {
		t.Fatalf("first session result = %v", err)
	}
	if err := <-partialResult; !errors.Is(err, ErrAgentSessionReplaced) {
		t.Fatalf("partial session result = %v", err)
	}
	if snapshot, ok := broker.Snapshot("node-1"); !ok || snapshot.Arch != "amd64" {
		t.Fatalf("active snapshot = %#v, %t", snapshot, ok)
	}

	third.disconnect()
	if err := <-thirdResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("third session result = %v", err)
	}
}

func TestAgentBrokerRejectsReplacementCaptureOverlappingEveryCommandAuthority(
	t *testing.T,
) {
	type commandOutcome struct {
		update transport.ExecutionUpdate
		err    error
	}
	tests := []struct {
		name      string
		kind      transport.MessageType
		metadata  transport.CommandMetadata
		wantState domain.ExecutionState
		send      func(
			context.Context,
			*AgentBroker,
			transport.CommandMetadata,
			string,
		) (transport.ExecutionUpdate, error)
	}{
		{
			name:      "prepare",
			kind:      transport.MessagePrepare,
			metadata:  brokerMetadata("command-1", "execution-1"),
			wantState: domain.ExecutionPreparing,
			send: func(
				ctx context.Context,
				broker *AgentBroker,
				metadata transport.CommandMetadata,
				_ string,
			) (transport.ExecutionUpdate, error) {
				return broker.SendPrepare(ctx, "node-1", metadata, true)
			},
		},
		{
			name:      "start",
			kind:      transport.MessageStart,
			metadata:  brokerStartMetadata("command-1", "execution-1"),
			wantState: domain.ExecutionRunning,
			send: func(
				ctx context.Context,
				broker *AgentBroker,
				metadata transport.CommandMetadata,
				_ string,
			) (transport.ExecutionUpdate, error) {
				return broker.SendStart(
					ctx,
					"node-1",
					metadata,
					true,
					brokerJITConfig("jit-overlap.example.test"),
				)
			},
		},
		{
			name:      "cancel",
			kind:      transport.MessageCancel,
			metadata:  brokerCancelMetadata("command-1", "execution-1"),
			wantState: domain.ExecutionCleaning,
			send: func(
				ctx context.Context,
				broker *AgentBroker,
				metadata transport.CommandMetadata,
				_ string,
			) (transport.ExecutionUpdate, error) {
				return broker.SendCancel(ctx, "node-1", metadata)
			},
		},
		{
			name:      "replay prepare",
			kind:      transport.MessagePrepare,
			metadata:  brokerMetadata("command-1", "execution-1"),
			wantState: domain.ExecutionPreparing,
			send: func(
				ctx context.Context,
				broker *AgentBroker,
				metadata transport.CommandMetadata,
				snapshotDigest string,
			) (transport.ExecutionUpdate, error) {
				return broker.ReplayPrepare(
					ctx,
					"node-1",
					metadata,
					true,
					snapshotDigest,
				)
			},
		},
		{
			name:      "reconciliation cancel",
			kind:      transport.MessageCancel,
			metadata:  brokerCancelMetadata("command-1", "execution-1"),
			wantState: domain.ExecutionCleaning,
			send: func(
				ctx context.Context,
				broker *AgentBroker,
				metadata transport.CommandMetadata,
				snapshotDigest string,
			) (transport.ExecutionUpdate, error) {
				return broker.SendReconciliationCancel(
					ctx,
					"node-1",
					metadata,
					snapshotDigest,
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshots := &recordingSnapshotConsumer{}
			consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
			consumers.Snapshot = snapshots
			broker := NewAgentBroker(1, consumers)
			capturedSnapshot := AgentSnapshot{
				NodeID:            "node-1",
				OS:                domain.OSLinux,
				Arch:              domain.ArchAMD64,
				RunnerVersion:     runner.OfficialRunnerVersion,
				NativeRunnerReady: true,
			}
			snapshotDigest, err := transport.AgentSnapshotDigest(capturedSnapshot)
			if err != nil {
				t.Fatal(err)
			}
			first, firstResult := startReadyBrokerSessionWithSnapshot(
				t,
				broker,
				capturedSnapshot,
			)
			if snapshots.count() != 1 {
				t.Fatalf("initial snapshot commits = %d, want 1", snapshots.count())
			}

			replacement := newFakeAgentSession("node-1")
			replacementResult := make(chan error, 1)
			go func() {
				replacementResult <- broker.serveSession(
					context.Background(),
					replacement,
				)
			}()
			replacement.send(brokerEnvelope(
				t,
				"replacement-hello",
				transport.MessageHello,
				struct {
					NodeID string `json:"nodeId"`
				}{NodeID: "node-1"},
			))
			assertBrokerAck(t, replacement, "replacement-hello")

			commandResult := make(chan commandOutcome, 1)
			go func() {
				update, sendErr := test.send(
					context.Background(),
					broker,
					test.metadata,
					snapshotDigest,
				)
				commandResult <- commandOutcome{update: update, err: sendErr}
			}()
			command := receiveBrokerWrite(t, first)
			if command.Type != test.kind ||
				command.MessageID != string(test.metadata.CommandID) {
				t.Fatalf("command = %#v, want %s/%s",
					command, test.kind, test.metadata.CommandID)
			}

			// The replacement built this snapshot from the Hello ACK baseline,
			// before the old actor accepted the command above.
			replacement.send(brokerEnvelope(
				t,
				"replacement-snapshot",
				transport.MessageSnapshot,
				capturedSnapshot,
			))
			select {
			case envelope := <-replacement.writes:
				t.Fatalf("stale replacement snapshot was acknowledged: %#v", envelope)
			case serveErr := <-replacementResult:
				if !errors.Is(serveErr, ErrAgentSessionReplaced) {
					t.Fatalf("replacement result = %v", serveErr)
				}
			case <-time.After(time.Second):
				t.Fatal("replacement snapshot was not rejected")
			}
			if snapshots.count() != 1 {
				t.Fatalf(
					"stale snapshot crossed durable consumer: commits = %d",
					snapshots.count(),
				)
			}
			if broker.ConnectedCount() != 1 {
				t.Fatalf(
					"stale replacement changed connected count: %d",
					broker.ConnectedCount(),
				)
			}

			sendCommandAck(
				t,
				first,
				"command-ack",
				string(test.metadata.CommandID),
			)
			sendExecutionUpdate(
				t,
				first,
				"command-update",
				transport.ExecutionUpdate{
					NodeID:      "node-1",
					CommandID:   test.metadata.CommandID,
					ExecutionID: test.metadata.ExecutionID,
					State:       test.wantState,
				},
			)
			assertBrokerAck(t, first, "command-update")
			outcome := <-commandResult
			if outcome.err != nil || outcome.update.State != test.wantState {
				t.Fatalf(
					"command outcome = (%#v, %v), want state %s",
					outcome.update,
					outcome.err,
					test.wantState,
				)
			}

			first.disconnect()
			if serveErr := <-firstResult; !errors.Is(serveErr, ErrAgentDisconnected) {
				t.Fatalf("first session result = %v", serveErr)
			}
		})
	}
}

func TestAgentBrokerSnapshotCommitLinearizesBeforeOldActorCommandDispatch(
	t *testing.T,
) {
	releaseSnapshot := make(chan struct{})
	snapshots := &recordingSnapshotConsumer{
		entered:   make(chan int, 4),
		blockCall: 2,
		release:   releaseSnapshot,
	}
	commands := &recordingCommandConsumer{}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Snapshot = snapshots
	consumers.Commands = commands
	broker := NewAgentBroker(1, consumers)
	first, firstResult := startReadyBrokerSession(t, broker, "node-1")
	if call := <-snapshots.entered; call != 1 {
		t.Fatalf("initial snapshot call = %d, want 1", call)
	}
	firstActor, ok := broker.session("node-1")
	if !ok {
		t.Fatal("first actor is not ready")
	}

	replacement := newFakeAgentSession("node-1")
	replacementResult := make(chan error, 1)
	go func() {
		replacementResult <- broker.serveSession(context.Background(), replacement)
	}()
	replacement.send(brokerEnvelope(t, "replacement-hello", transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: "node-1"}))
	assertBrokerAck(t, replacement, "replacement-hello")
	replacement.send(brokerEnvelope(t, "replacement-snapshot", transport.MessageSnapshot, AgentSnapshot{
		NodeID: "node-1",
		OS:     domain.OSLinux,
		Arch:   domain.ArchAMD64,
	}))
	if call := <-snapshots.entered; call != 2 {
		t.Fatalf("replacement snapshot call = %d, want 2", call)
	}

	metadata := brokerStartMetadata("old-start", "execution-1")
	commandResult := make(chan error, 1)
	go func() {
		_, sendErr := firstActor.sendCommand(
			context.Background(),
			transport.MessageStart,
			metadata,
			func() (json.RawMessage, error) {
				return transport.EncodeStartCommandPayload(
					metadata,
					runner.OfficialRunnerVersion,
					true,
					"jit-linearization.example.test",
				)
			},
		)
		commandResult <- sendErr
	}()
	// Holding commandGate proves this call passed the actor ready/done checks.
	// The replacement snapshot already owns the lifecycle lock, so activation
	// deterministically linearizes before command authority can be committed.
	eventuallyBroker(t, func() bool { return len(firstActor.commandGate) == 0 })
	select {
	case envelope := <-first.writes:
		t.Fatalf("old actor command crossed blocked snapshot commit: %#v", envelope)
	case sendErr := <-commandResult:
		t.Fatalf("old actor command returned before snapshot commit: %v", sendErr)
	default:
	}

	close(releaseSnapshot)
	assertBrokerAck(t, replacement, "replacement-snapshot")
	if sendErr := <-commandResult; !errors.Is(sendErr, ErrAgentSessionReplaced) {
		t.Fatalf("old actor command result = %v", sendErr)
	}
	if commands.recordCount() != 0 {
		t.Fatalf("old actor command crossed durable consumer: %d records", commands.recordCount())
	}
	if serveErr := <-firstResult; !errors.Is(serveErr, ErrAgentSessionReplaced) {
		t.Fatalf("first session result = %v", serveErr)
	}

	replacement.disconnect()
	if serveErr := <-replacementResult; !errors.Is(serveErr, ErrAgentDisconnected) {
		t.Fatalf("replacement session result = %v", serveErr)
	}
}

func TestAgentBrokerDisconnectConsumerRunsOnlyForRemovedCurrentSession(t *testing.T) {
	disconnects := &recordingDisconnectConsumer{}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Disconnects = disconnects
	broker := NewAgentBroker(1, consumers)

	first, firstResult := startReadyBrokerSession(t, broker, "node-1")
	second, secondResult := startReadyBrokerSession(t, broker, "node-1")
	if err := <-firstResult; !errors.Is(err, ErrAgentSessionReplaced) {
		t.Fatalf("replaced session result = %v", err)
	}
	if disconnects.count() != 0 {
		t.Fatal("replacement actor termination emitted an offline transition")
	}
	first.disconnect()
	if disconnects.count() != 0 {
		t.Fatal("already replaced actor emitted a later offline transition")
	}

	second.disconnect()
	if err := <-secondResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("current session result = %v", err)
	}
	if disconnects.count() != 1 {
		t.Fatalf("disconnect calls = %d, want 1", disconnects.count())
	}
	wantDigest, err := transport.AgentSnapshotDigest(AgentSnapshot{
		NodeID:        "node-1",
		OS:            domain.OSLinux,
		Arch:          domain.ArchAMD64,
		RunnerVersion: runner.OfficialRunnerVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if record := disconnects.record(0); record.NodeID != "node-1" ||
		record.SnapshotDigest != wantDigest {
		t.Fatalf("disconnect authority = %#v, want node-1/%s", record, wantDigest)
	}
	if err, found := broker.DisconnectError("node-1"); found || err != nil {
		t.Fatalf("successful disconnect retained error = (%v, %t)", err, found)
	}
}

func TestAgentBrokerDisconnectBeforeSnapshotActivationHasNoDurableAuthorityToRevoke(
	t *testing.T,
) {
	disconnects := &recordingDisconnectConsumer{}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Disconnects = disconnects
	broker := NewAgentBroker(1, consumers)
	session := newFakeAgentSession("node-1")
	result := make(chan error, 1)
	go func() { result <- broker.serveSession(context.Background(), session) }()

	session.send(brokerEnvelope(t, "pre-snapshot-hello", transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: "node-1"}))
	assertBrokerAck(t, session, "pre-snapshot-hello")
	session.disconnect()
	if err := <-result; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("pre-snapshot session result = %v", err)
	}
	if disconnects.count() != 0 {
		t.Fatalf(
			"pre-snapshot session revoked %d durable authorities",
			disconnects.count(),
		)
	}
}

func TestAgentBrokerDisconnectConsumerFailureIsObservableAndDoesNotRestoreSession(t *testing.T) {
	want := errors.New("offline projection unavailable")
	disconnects := &recordingDisconnectConsumer{err: want}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Disconnects = disconnects
	broker := NewAgentBroker(1, consumers)

	session, serveResult := startReadyBrokerSession(t, broker, "node-1")
	session.disconnect()
	if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("session result = %v", err)
	}
	if broker.ConnectedCount() != 0 {
		t.Fatal("disconnect consumer failure rolled back session cleanup")
	}
	if _, online := broker.Snapshot("node-1"); online {
		t.Fatal("disconnect consumer failure synthesized an online session")
	}
	if err, found := broker.DisconnectError("node-1"); !found || !errors.Is(err, ErrAgentDisconnectCommit) {
		t.Fatalf("observable disconnect error = (%v, %t)", err, found)
	}
}

func TestAgentBrokerRejectsReconnectWhileOfflineProjectionIsPending(t *testing.T) {
	release := make(chan struct{})
	disconnects := &recordingDisconnectConsumer{
		entered: make(chan struct{}, 1),
		release: release,
	}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Disconnects = disconnects
	broker := NewAgentBroker(1, consumers)

	first, firstResult := startReadyBrokerSession(t, broker, "node-1")
	first.disconnect()
	select {
	case <-disconnects.entered:
	case <-time.After(time.Second):
		t.Fatal("disconnect projection did not start")
	}

	second := newFakeAgentSession("node-1")
	secondResult := make(chan error, 1)
	go func() { secondResult <- broker.serveSession(context.Background(), second) }()
	second.send(brokerEnvelope(t, "reconnect-hello", transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: "node-1"}))
	select {
	case envelope := <-second.writes:
		t.Fatalf("reconnect handshake advanced before offline projection: %#v", envelope)
	case err := <-secondResult:
		if !errors.Is(err, ErrAgentDisconnectCommit) {
			t.Fatalf("pending-projection reconnect result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending offline projection blocked reconnect instead of failing closed")
	}

	close(release)
	if err := <-firstResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("first session result = %v", err)
	}
	if disconnects.count() != 1 {
		t.Fatalf("offline projections = %d, want 1", disconnects.count())
	}

	third, thirdResult := startReadyBrokerSession(t, broker, "node-1")
	if err, found := broker.DisconnectError("node-1"); found || err != nil {
		t.Fatalf("successful retry retained disconnect error = (%v, %t)", err, found)
	}
	third.disconnect()
	if err := <-thirdResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("third session result = %v", err)
	}
}

func TestAgentBrokerCloseCancelsPendingOfflineProjection(t *testing.T) {
	disconnects := &recordingDisconnectConsumer{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Disconnects = disconnects
	broker := NewAgentBroker(1, consumers)

	session, serveResult := startReadyBrokerSession(t, broker, "node-1")
	session.disconnect()
	select {
	case <-disconnects.entered:
	case <-time.After(time.Second):
		t.Fatal("disconnect projection did not start")
	}

	broker.Close()
	select {
	case err := <-serveResult:
		if !errors.Is(err, ErrAgentDisconnected) {
			t.Fatalf("serve result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("broker close did not cancel the pending disconnect projection")
	}
	if err, found := broker.DisconnectError("node-1"); !found ||
		!errors.Is(err, ErrAgentDisconnectCommit) {
		t.Fatalf("canceled disconnect projection = (%v, %t)", err, found)
	}
}

func TestAgentBrokerSnapshotActivationIsInvisibleUntilAckAndRollsBackOnAckFailure(t *testing.T) {
	t.Run("command and readiness remain unavailable while snapshot ACK is blocked", func(t *testing.T) {
		releaseACK := make(chan struct{})
		snapshots := &recordingSnapshotConsumer{
			entered: make(chan int, 4),
		}
		consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
		consumers.Snapshot = snapshots
		broker := NewAgentBroker(1, consumers)
		session := newFakeAgentSession("node-1")
		result := make(chan error, 1)
		go func() { result <- broker.serveSession(context.Background(), session) }()

		session.send(brokerEnvelope(t, "blocked-ack-hello", transport.MessageHello, struct {
			NodeID string `json:"nodeId"`
		}{NodeID: "node-1"}))
		assertBrokerAck(t, session, "blocked-ack-hello")
		for {
			select {
			case <-session.writeEntered:
			default:
				goto writesDrained
			}
		}
	writesDrained:
		session.setWriteBlock(releaseACK)
		session.send(brokerEnvelope(t, "blocked-ack-snapshot", transport.MessageSnapshot, AgentSnapshot{
			NodeID:            "node-1",
			OS:                domain.OSLinux,
			Arch:              domain.ArchAMD64,
			NativeRunnerReady: true,
		}))
		select {
		case call := <-snapshots.entered:
			if call != 1 {
				t.Fatalf("snapshot commit call = %d", call)
			}
		case <-time.After(time.Second):
			t.Fatal("snapshot did not reach commit boundary")
		}
		select {
		case <-session.writeEntered:
		case <-time.After(time.Second):
			t.Fatal("snapshot ACK did not reach blocked write")
		}

		if broker.ConnectedCount() != 0 {
			t.Fatal("snapshot session became connected before its ACK completed")
		}
		if _, online := broker.Snapshot("node-1"); online {
			t.Fatal("snapshot readiness became visible before its ACK completed")
		}
		if _, err := broker.commandSession("node-1", brokerCancelMetadata("before-ack", "execution-1")); !errors.Is(err, ErrAgentOffline) {
			t.Fatalf("command session before snapshot ACK = %v", err)
		}

		close(releaseACK)
		assertBrokerAck(t, session, "blocked-ack-snapshot")
		eventuallyBroker(t, func() bool { return broker.ConnectedCount() == 1 })
		session.disconnect()
		if err := <-result; !errors.Is(err, ErrAgentDisconnected) {
			t.Fatalf("session result = %v", err)
		}
	})

	t.Run("failed snapshot ACK projects the committed session offline", func(t *testing.T) {
		snapshots := &recordingSnapshotConsumer{}
		disconnects := &recordingDisconnectConsumer{}
		consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
		consumers.Snapshot = snapshots
		consumers.Disconnects = disconnects
		broker := NewAgentBroker(1, consumers)
		session := newFakeAgentSession("node-1")
		result := make(chan error, 1)
		go func() { result <- broker.serveSession(context.Background(), session) }()

		session.send(brokerEnvelope(t, "failed-ack-hello", transport.MessageHello, struct {
			NodeID string `json:"nodeId"`
		}{NodeID: "node-1"}))
		assertBrokerAck(t, session, "failed-ack-hello")
		session.failNextWrite(errors.New("snapshot ACK write failed"))
		session.send(brokerEnvelope(t, "failed-ack-snapshot", transport.MessageSnapshot, AgentSnapshot{
			NodeID:            "node-1",
			OS:                domain.OSLinux,
			Arch:              domain.ArchAMD64,
			NativeRunnerReady: true,
		}))

		if err := <-result; !errors.Is(err, ErrAgentDisconnected) {
			t.Fatalf("session result = %v", err)
		}
		if snapshots.count() != 1 || !snapshots.last().NativeRunnerReady {
			t.Fatalf("committed snapshots = %#v", snapshots.snapshots)
		}
		if disconnects.count() != 1 {
			t.Fatalf("offline projections = %d, want 1", disconnects.count())
		}
		if broker.ConnectedCount() != 0 {
			t.Fatal("failed snapshot ACK retained a connected session")
		}
		if _, online := broker.Snapshot("node-1"); online {
			t.Fatal("failed snapshot ACK retained online readiness")
		}
	})

	t.Run("stalled snapshot ACK can be superseded without blocking the node", func(t *testing.T) {
		releaseACK := make(chan struct{})
		snapshots := &recordingSnapshotConsumer{
			entered: make(chan int, 8),
		}
		consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
		consumers.Snapshot = snapshots
		broker := NewAgentBroker(1, consumers)
		_, firstResult := startReadyBrokerSession(t, broker, "node-1")
		if call := <-snapshots.entered; call != 1 {
			t.Fatalf("initial snapshot call = %d, want 1", call)
		}

		second := newFakeAgentSession("node-1")
		secondResult := make(chan error, 1)
		go func() {
			secondResult <- broker.serveSession(context.Background(), second)
		}()
		second.send(brokerEnvelope(t, "stalled-ack-hello", transport.MessageHello, struct {
			NodeID string `json:"nodeId"`
		}{NodeID: "node-1"}))
		assertBrokerAck(t, second, "stalled-ack-hello")
		for {
			select {
			case <-second.writeEntered:
			default:
				goto secondWritesDrained
			}
		}
	secondWritesDrained:
		second.setWriteBlock(releaseACK)
		second.send(brokerEnvelope(t, "stalled-ack-snapshot", transport.MessageSnapshot, AgentSnapshot{
			NodeID: "node-1",
			OS:     domain.OSLinux,
			Arch:   domain.ArchAMD64,
		}))
		if call := <-snapshots.entered; call != 2 {
			t.Fatalf("stalled snapshot call = %d, want 2", call)
		}
		select {
		case <-second.writeEntered:
		case <-time.After(time.Second):
			t.Fatal("snapshot ACK did not reach the stalled write")
		}

		third, thirdResult := startReadyBrokerSession(t, broker, "node-1")
		if err := <-firstResult; !errors.Is(err, ErrAgentSessionReplaced) {
			t.Fatalf("first session result = %v", err)
		}
		if err := <-secondResult; !errors.Is(err, ErrAgentSessionReplaced) {
			t.Fatalf("stalled session result = %v", err)
		}
		if snapshots.count() != 3 {
			t.Fatalf("snapshot commits = %d, want 3", snapshots.count())
		}
		close(releaseACK)

		third.disconnect()
		if err := <-thirdResult; !errors.Is(err, ErrAgentDisconnected) {
			t.Fatalf("third session result = %v", err)
		}
	})
}

func TestAgentBrokerReplacementGenerationRejectsOldHeartbeatProjection(t *testing.T) {
	releaseReplacement := make(chan struct{})
	snapshots := &recordingSnapshotConsumer{
		entered:   make(chan int, 8),
		blockCall: 2,
		release:   releaseReplacement,
	}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Snapshot = snapshots
	consumers.Readiness = snapshots
	broker := NewAgentBroker(1, consumers)
	first, firstResult := startReadyBrokerSessionWithSnapshot(t, broker, AgentSnapshot{
		NodeID:            "node-1",
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		NativeRunnerReady: true,
	})
	if call := <-snapshots.entered; call != 1 {
		t.Fatalf("first snapshot call = %d", call)
	}

	second := newFakeAgentSession("node-1")
	secondResult := make(chan error, 1)
	go func() { secondResult <- broker.serveSession(context.Background(), second) }()
	second.send(brokerEnvelope(t, "replacement-heartbeat-hello", transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: "node-1"}))
	assertBrokerAck(t, second, "replacement-heartbeat-hello")
	second.send(brokerEnvelope(t, "replacement-heartbeat-snapshot", transport.MessageSnapshot, AgentSnapshot{
		NodeID:            "node-1",
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		NativeRunnerReady: true,
	}))
	if call := <-snapshots.entered; call != 2 {
		t.Fatalf("replacement snapshot call = %d", call)
	}

	first.send(brokerEnvelope(t, "old-heartbeat", transport.MessageHeartbeat, transport.AgentHeartbeat{
		NodeID:            "node-1",
		NativeRunnerReady: false,
	}))
	select {
	case call := <-snapshots.entered:
		t.Fatalf("old heartbeat crossed replacement lifecycle boundary as call %d", call)
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseReplacement)
	assertBrokerAck(t, second, "replacement-heartbeat-snapshot")
	if err := <-firstResult; !errors.Is(err, ErrAgentSessionReplaced) {
		t.Fatalf("first session result = %v", err)
	}
	eventuallyBroker(t, func() bool { return snapshots.count() == 2 })
	if !snapshots.last().NativeRunnerReady {
		t.Fatal("old heartbeat overwrote replacement readiness")
	}

	second.disconnect()
	if err := <-secondResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("second session result = %v", err)
	}
}

func TestAgentBrokerReplacementGenerationRejectsOldLeaseExpiryProjection(t *testing.T) {
	releaseReplacement := make(chan struct{})
	snapshots := &recordingSnapshotConsumer{
		entered:   make(chan int, 8),
		blockCall: 2,
		release:   releaseReplacement,
	}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Snapshot = snapshots
	consumers.Readiness = snapshots
	broker := NewAgentBroker(1, consumers)
	_, firstResult := startReadyBrokerSessionWithSnapshot(t, broker, AgentSnapshot{
		NodeID:            "node-1",
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		NativeRunnerReady: true,
	})
	if call := <-snapshots.entered; call != 1 {
		t.Fatalf("first snapshot call = %d", call)
	}
	broker.mu.RLock()
	firstActor := broker.sessions["node-1"]
	broker.mu.RUnlock()
	if firstActor == nil {
		t.Fatal("first actor was not active")
	}
	firstActor.stateMu.Lock()
	expiringGeneration := firstActor.readinessGeneration
	firstActor.stateMu.Unlock()

	second := newFakeAgentSession("node-1")
	secondResult := make(chan error, 1)
	go func() { secondResult <- broker.serveSession(context.Background(), second) }()
	second.send(brokerEnvelope(t, "replacement-expiry-hello", transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: "node-1"}))
	assertBrokerAck(t, second, "replacement-expiry-hello")
	second.send(brokerEnvelope(t, "replacement-expiry-snapshot", transport.MessageSnapshot, AgentSnapshot{
		NodeID:            "node-1",
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		NativeRunnerReady: true,
	}))
	if call := <-snapshots.entered; call != 2 {
		t.Fatalf("replacement snapshot call = %d", call)
	}

	expired := make(chan struct{})
	go func() {
		firstActor.expireReadiness(expiringGeneration)
		close(expired)
	}()
	select {
	case call := <-snapshots.entered:
		t.Fatalf("old lease expiry crossed replacement lifecycle boundary as call %d", call)
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseReplacement)
	assertBrokerAck(t, second, "replacement-expiry-snapshot")
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("old lease expiry did not leave lifecycle boundary")
	}
	if err := <-firstResult; !errors.Is(err, ErrAgentSessionReplaced) {
		t.Fatalf("first session result = %v", err)
	}
	eventuallyBroker(t, func() bool { return snapshots.count() == 2 })
	if !snapshots.last().NativeRunnerReady {
		t.Fatal("old lease expiry overwrote replacement readiness")
	}

	second.disconnect()
	if err := <-secondResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("second session result = %v", err)
	}
}

func TestAgentBrokerRejectsHelloThatDoesNotMatchCredential(t *testing.T) {
	broker := NewAgentBroker(1, acceptingAgentConsumers(newRecordingUpdateConsumer()))
	session := newFakeAgentSession("credential-node")
	result := make(chan error, 1)
	go func() { result <- broker.serveSession(context.Background(), session) }()
	session.send(brokerEnvelope(t, "hello-mismatch", transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: "payload-node"}))
	if err := <-result; !errors.Is(err, ErrAgentProtocol) {
		t.Fatalf("session result = %v", err)
	}
	if broker.ConnectedCount() != 0 {
		t.Fatalf("mismatched identity became active: %d", broker.ConnectedCount())
	}
}

func TestAgentSessionReadErrorClassificationPreservesSecurityBoundary(t *testing.T) {
	t.Parallel()

	credential := enroll.Credential{
		NodeID: "00112233445566778899aabbccddeeff",
	}
	typed := transport.AgentProtocolRejection(
		credential,
		transport.ErrProtocolVersion,
	)
	if got := classifyAgentSessionError(typed); got != typed {
		t.Fatalf("typed rejection changed identity: got %v, want %v", got, typed)
	}
	protocol := classifyAgentSessionError(transport.ErrProtocolVersion)
	if !errors.Is(protocol, ErrAgentProtocol) ||
		!errors.Is(protocol, transport.ErrProtocolVersion) {
		t.Fatalf("protocol classification = %v", protocol)
	}
	if got := classifyAgentSessionError(errors.New("connection reset")); !errors.Is(
		got,
		ErrAgentDisconnected,
	) {
		t.Fatalf("I/O classification = %v, want disconnected", got)
	}
}

func TestAgentBrokerDoesNotAcknowledgeUpdateWithoutConsumer(t *testing.T) {
	broker := NewAgentBroker(1, acceptingAgentConsumers(nil))
	session, serveResult := startReadyBrokerSession(t, broker, "node-1")
	metadata := brokerCancelMetadata("command-no-consumer", "execution-1")
	if _, err := broker.SendCancel(context.Background(), "node-1", metadata); !errors.Is(err, ErrExecutionUpdateConsumerRequired) {
		t.Fatalf("send without consumer = %v", err)
	}
	session.send(brokerEnvelope(t, "unsolicited-update", transport.MessageExecutionUpdate, transport.ExecutionUpdate{
		NodeID:      "node-1",
		CommandID:   metadata.CommandID,
		ExecutionID: metadata.ExecutionID,
		State:       domain.ExecutionRunning,
	}))
	if err := <-serveResult; !errors.Is(err, ErrAgentProtocol) {
		t.Fatalf("unsolicited update result = %v", err)
	}
	select {
	case envelope := <-session.writes:
		t.Fatalf("unowned update was acknowledged: %#v", envelope)
	case <-time.After(20 * time.Millisecond):
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
