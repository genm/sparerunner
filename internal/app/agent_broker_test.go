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

func (consumer *recordingSnapshotConsumer) last() AgentSnapshot {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if len(consumer.snapshots) == 0 {
		return AgentSnapshot{}
	}
	return cloneAgentSnapshot(consumer.snapshots[len(consumer.snapshots)-1])
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
	session.disconnect()
	_ = <-serveResult
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

func TestAgentBrokerReadinessLeaseExpiresWithoutDroppingOnlineSnapshot(t *testing.T) {
	snapshots := &recordingSnapshotConsumer{}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.Snapshot = snapshots
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

func TestAgentBrokerReplacesOnlyFullyReadySameNodeSession(t *testing.T) {
	broker := NewAgentBroker(1, acceptingAgentConsumers(newRecordingUpdateConsumer()))
	first, firstResult := startReadyBrokerSession(t, broker, "node-1")
	commandResult := make(chan error, 1)
	go func() {
		_, err := broker.SendCancel(context.Background(), "node-1", brokerCancelMetadata("command-replaced", "execution-1"))
		commandResult <- err
	}()
	_ = receiveBrokerWrite(t, first)

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
	assertBrokerAck(t, second, "second-snapshot")
	if err := <-commandResult; !errors.Is(err, ErrAgentSessionReplaced) {
		t.Fatalf("pending command result = %v", err)
	}
	if err := <-firstResult; !errors.Is(err, ErrAgentSessionReplaced) {
		t.Fatalf("first session result = %v", err)
	}
	if snapshot, ok := broker.Snapshot("node-1"); !ok || snapshot.Arch != "amd64" {
		t.Fatalf("active snapshot = %#v, %t", snapshot, ok)
	}

	partial.disconnect()
	_ = <-partialResult
	second.disconnect()
	_ = <-secondResult
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
