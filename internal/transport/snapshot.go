package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/genm/tewake/internal/domain"
)

type AgentExecutionObservation struct {
	ExecutionID        domain.ExecutionID    `json:"executionId"`
	State              domain.ExecutionState `json:"state"`
	ObservedAtUnixNano int64                 `json:"observedAtUnixNano"`
}

type AgentCleanupTombstone struct {
	ExecutionID        domain.ExecutionID        `json:"executionId"`
	FailureCode        domain.CleanupFailureCode `json:"failureCode"`
	RecordedAtUnixNano int64                     `json:"recordedAtUnixNano"`
}

// AgentSnapshot is the non-secret reconciliation evidence sent at session
// activation. Command entries contain only the authenticated payload digest;
// JIT bodies, paths, process output, and private material have no field here.
type AgentSnapshot struct {
	NodeID             domain.NodeID               `json:"nodeId"`
	OS                 domain.OperatingSystem      `json:"os"`
	Arch               domain.Architecture         `json:"arch"`
	NativeRunnerReady  bool                        `json:"nativeRunnerReady"`
	MaxControllerEpoch domain.ControllerEpoch      `json:"maxControllerEpoch"`
	Commands           []domain.Command            `json:"commands"`
	Observations       []AgentExecutionObservation `json:"observations"`
	CleanupTombstones  []AgentCleanupTombstone     `json:"cleanupTombstones"`
}

func (snapshot AgentSnapshot) Validate() error {
	if snapshot.NodeID == "" || snapshot.OS.Validate("agent_snapshot.os") != nil ||
		snapshot.Arch.Validate("agent_snapshot.architecture") != nil {
		return ErrInvalidCommand
	}
	commandIDs := make(map[domain.CommandID]struct{}, len(snapshot.Commands))
	for _, command := range snapshot.Commands {
		if command.Validate() != nil || command.ControllerEpoch > snapshot.MaxControllerEpoch {
			return ErrInvalidCommand
		}
		if _, duplicate := commandIDs[command.ID]; duplicate {
			return ErrInvalidCommand
		}
		commandIDs[command.ID] = struct{}{}
	}
	observationIDs := make(map[domain.ExecutionID]struct{}, len(snapshot.Observations))
	for _, observation := range snapshot.Observations {
		if observation.ExecutionID == "" || observation.ObservedAtUnixNano <= 0 ||
			observation.State.Validate("agent_snapshot.observation.state") != nil {
			return ErrInvalidCommand
		}
		if _, duplicate := observationIDs[observation.ExecutionID]; duplicate {
			return ErrInvalidCommand
		}
		observationIDs[observation.ExecutionID] = struct{}{}
	}
	tombstoneIDs := make(map[domain.ExecutionID]struct{}, len(snapshot.CleanupTombstones))
	for _, tombstone := range snapshot.CleanupTombstones {
		if tombstone.ExecutionID == "" || tombstone.RecordedAtUnixNano <= 0 ||
			tombstone.FailureCode.Validate("agent_snapshot.cleanup_tombstone.failure_code") != nil {
			return ErrInvalidCommand
		}
		if _, duplicate := tombstoneIDs[tombstone.ExecutionID]; duplicate {
			return ErrInvalidCommand
		}
		tombstoneIDs[tombstone.ExecutionID] = struct{}{}
	}
	return nil
}

func EncodeAgentSnapshot(snapshot AgentSnapshot) (json.RawMessage, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(snapshot)
	if err != nil || int64(len(payload)) > MaxEnvelopeBytes {
		return nil, ErrInvalidCommand
	}
	return payload, nil
}

func DecodeAgentSnapshot(payload []byte) (AgentSnapshot, error) {
	if len(payload) == 0 || int64(len(payload)) > MaxEnvelopeBytes || rejectDuplicateJSONKeys(payload) != nil {
		return AgentSnapshot{}, ErrInvalidCommand
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var wire struct {
		NodeID             domain.NodeID               `json:"nodeId"`
		OS                 domain.OperatingSystem      `json:"os"`
		Arch               domain.Architecture         `json:"arch"`
		NativeRunnerReady  *bool                       `json:"nativeRunnerReady"`
		MaxControllerEpoch domain.ControllerEpoch      `json:"maxControllerEpoch"`
		Commands           []domain.Command            `json:"commands"`
		Observations       []AgentExecutionObservation `json:"observations"`
		CleanupTombstones  []AgentCleanupTombstone     `json:"cleanupTombstones"`
	}
	if err := decoder.Decode(&wire); err != nil || wire.NativeRunnerReady == nil {
		return AgentSnapshot{}, ErrInvalidCommand
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return AgentSnapshot{}, ErrInvalidCommand
	}
	snapshot := AgentSnapshot{
		NodeID:             wire.NodeID,
		OS:                 wire.OS,
		Arch:               wire.Arch,
		NativeRunnerReady:  *wire.NativeRunnerReady,
		MaxControllerEpoch: wire.MaxControllerEpoch,
		Commands:           wire.Commands,
		Observations:       wire.Observations,
		CleanupTombstones:  wire.CleanupTombstones,
	}
	if err := snapshot.Validate(); err != nil {
		return AgentSnapshot{}, err
	}
	return snapshot, nil
}
