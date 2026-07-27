package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/genm/tewake/internal/domain"
)

// AgentHeartbeat is the complete, non-secret readiness observation refreshed
// during an authenticated Agent session.
type AgentHeartbeat struct {
	NodeID            domain.NodeID `json:"nodeId"`
	NativeRunnerReady bool          `json:"nativeRunnerReady"`
}

func (heartbeat AgentHeartbeat) Validate() error {
	if heartbeat.NodeID == "" {
		return ErrInvalidCommand
	}
	return nil
}

func EncodeAgentHeartbeat(heartbeat AgentHeartbeat) (json.RawMessage, error) {
	if heartbeat.Validate() != nil {
		return nil, ErrInvalidCommand
	}
	payload, err := json.Marshal(heartbeat)
	if err != nil || int64(len(payload)) > MaxEnvelopeBytes {
		return nil, ErrInvalidCommand
	}
	return payload, nil
}

func DecodeAgentHeartbeat(payload []byte) (AgentHeartbeat, error) {
	if len(payload) == 0 || int64(len(payload)) > MaxEnvelopeBytes ||
		rejectDuplicateJSONKeys(payload) != nil {
		return AgentHeartbeat{}, ErrInvalidCommand
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var wire struct {
		NodeID            domain.NodeID `json:"nodeId"`
		NativeRunnerReady *bool         `json:"nativeRunnerReady"`
	}
	if err := decoder.Decode(&wire); err != nil {
		return AgentHeartbeat{}, ErrInvalidCommand
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return AgentHeartbeat{}, ErrInvalidCommand
	}
	if wire.NativeRunnerReady == nil {
		return AgentHeartbeat{}, ErrInvalidCommand
	}
	heartbeat := AgentHeartbeat{
		NodeID:            wire.NodeID,
		NativeRunnerReady: *wire.NativeRunnerReady,
	}
	if err := heartbeat.Validate(); err != nil {
		return AgentHeartbeat{}, err
	}
	return heartbeat, nil
}
