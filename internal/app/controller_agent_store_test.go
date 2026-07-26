package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

type recordingControllerAgentStore struct {
	commands  []store.IssuedAgentCommand
	snapshots []store.NodeAgentSnapshot
	updates   []store.AgentExecutionUpdate
	err       error
}

func (recording *recordingControllerAgentStore) CommitAgentCommand(_ context.Context, command store.IssuedAgentCommand) (bool, error) {
	recording.commands = append(recording.commands, command)
	return false, recording.err
}

func (recording *recordingControllerAgentStore) RecordAgentSnapshot(_ context.Context, snapshot store.NodeAgentSnapshot) error {
	recording.snapshots = append(recording.snapshots, snapshot)
	return recording.err
}

func (recording *recordingControllerAgentStore) RecordAgentExecutionUpdate(_ context.Context, update store.AgentExecutionUpdate) (bool, error) {
	recording.updates = append(recording.updates, update)
	return false, recording.err
}

func TestStoreBackedAgentConsumersMapOnlyNonSecretDurableFields(t *testing.T) {
	recording := &recordingControllerAgentStore{}
	consumers := newStoreBackedAgentConsumers(recording)
	var commandDigest [32]byte
	commandDigest[0], commandDigest[31] = 0x01, 0xfe
	command := AgentCommandRecord{
		NodeID: "node-agent",
		Kind:   transport.MessageStart,
		Metadata: transport.CommandMetadata{
			CommandID:       "command-agent",
			ControllerEpoch: 7,
			ExecutionID:     "execution-agent",
			ExpectedState:   domain.ExecutionPreparing,
		},
		PayloadDigest: commandDigest,
	}
	if err := consumers.Commands.HandleAgentCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	wantCommand := store.IssuedAgentCommand{
		NodeID: "node-agent",
		Type:   domain.CommandStart,
		Command: domain.Command{
			ID:              "command-agent",
			ControllerEpoch: 7,
			ExecutionID:     "execution-agent",
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   "01" + strings.Repeat("00", 30) + "fe",
		},
	}
	if !reflect.DeepEqual(recording.commands, []store.IssuedAgentCommand{wantCommand}) {
		t.Fatalf("mapped command = %+v, want %+v", recording.commands, wantCommand)
	}

	snapshot := AgentSnapshot{
		NodeID:             "node-agent",
		OS:                 "linux",
		Arch:               "arm64",
		NativeRunnerReady:  true,
		MaxControllerEpoch: 7,
		Commands:           []domain.Command{wantCommand.Command},
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        "execution-agent",
			State:              domain.ExecutionRunning,
			ObservedAtUnixNano: 10,
		}},
		CleanupTombstones: []transport.AgentCleanupTombstone{{
			ExecutionID:        "execution-old",
			FailureCode:        domain.CleanupProcessResidue,
			RecordedAtUnixNano: 9,
		}},
	}
	if err := consumers.Snapshot.HandleAgentSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if len(recording.snapshots) != 1 {
		t.Fatalf("snapshot calls = %d, want 1", len(recording.snapshots))
	}
	gotSnapshot := recording.snapshots[0]
	if gotSnapshot.NodeID != "node-agent" || gotSnapshot.OS != domain.OSLinux ||
		gotSnapshot.Architecture != domain.ArchARM64 ||
		!gotSnapshot.NativeRunnerReady ||
		!reflect.DeepEqual(gotSnapshot.Journal.Commands, snapshot.Commands) ||
		len(gotSnapshot.Journal.Observations) != 1 ||
		len(gotSnapshot.Journal.CleanupTombstones) != 1 {
		t.Fatalf("mapped snapshot = %+v", gotSnapshot)
	}
	// The adapter owns a copy rather than retaining broker slice storage.
	snapshot.Commands[0].PayloadDigest = strings.Repeat("f", 64)
	if gotSnapshot.Journal.Commands[0].PayloadDigest == snapshot.Commands[0].PayloadDigest {
		t.Fatal("snapshot command slice aliases the broker payload")
	}

	var updateDigest [32]byte
	updateDigest[0] = 0xab
	update := AgentExecutionUpdateRecord{
		MessageID: "update-agent",
		Update: transport.ExecutionUpdate{
			NodeID:      "node-agent",
			CommandID:   "command-agent",
			ExecutionID: "execution-agent",
			State:       domain.ExecutionRunning,
			Replayed:    true,
		},
		PayloadDigest: updateDigest,
	}
	if err := consumers.ExecutionUpdates.HandleExecutionUpdate(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if len(recording.updates) != 1 || recording.updates[0].PayloadDigest != "ab"+strings.Repeat("00", 31) ||
		recording.updates[0].MessageID != update.MessageID || recording.updates[0].ErrorCode != domain.ExecutionErrorNone {
		t.Fatalf("mapped update = %+v", recording.updates)
	}
}

func TestStoreBackedAgentConsumersFailClosedOnUnsupportedKindAndStoreFailure(t *testing.T) {
	recording := &recordingControllerAgentStore{}
	consumers := newStoreBackedAgentConsumers(recording)
	if err := consumers.Commands.HandleAgentCommand(context.Background(), AgentCommandRecord{
		NodeID: "node-agent",
		Kind:   transport.MessageHeartbeat,
	}); err == nil {
		t.Fatal("unsupported message kind reached durable command storage")
	}
	if len(recording.commands) != 0 {
		t.Fatal("unsupported message kind mutated durable storage")
	}

	sentinel := errors.New("durable store unavailable")
	recording.err = sentinel
	if err := consumers.ExecutionUpdates.HandleExecutionUpdate(context.Background(), AgentExecutionUpdateRecord{
		MessageID: "update-agent",
		Update: transport.ExecutionUpdate{
			NodeID:      "node-agent",
			CommandID:   "command-agent",
			ExecutionID: "execution-agent",
			State:       domain.ExecutionRunning,
		},
	}); !errors.Is(err, sentinel) {
		t.Fatalf("store failure = %v, want sentinel", err)
	}

	nilConsumers := newStoreBackedAgentConsumers(nil)
	if err := nilConsumers.Snapshot.HandleAgentSnapshot(context.Background(), AgentSnapshot{}); !errors.Is(err, ErrAgentSnapshotConsumerRequired) {
		t.Fatalf("nil store snapshot = %v", err)
	}
}
