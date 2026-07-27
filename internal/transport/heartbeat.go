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
	// AvailabilityIntent is the node owner's local decision. It is reported for
	// display and audit; capacity itself is withheld by NativeRunnerReady, so a
	// Controller that ignores this field can never over-admit because of it.
	AvailabilityIntent domain.AvailabilityIntent `json:"availabilityIntent,omitempty"`
	// ExcludedTargets mirrors the AgentSnapshot field at heartbeat cadence.
	// Absent means "no change reported"; an Agent that never populates it
	// (this PR) always omits the field.
	ExcludedTargets []domain.TargetID `json:"excludedTargets,omitempty"`
}

func (heartbeat AgentHeartbeat) Validate() error {
	if heartbeat.NodeID == "" {
		return ErrInvalidCommand
	}
	// An absent intent is "unspecified", which an Agent without the local
	// control surface reports. It is display provenance only: capacity is
	// withheld through NativeRunnerReady, so an unspecified value can never
	// admit a node whose owner stopped it.
	if heartbeat.AvailabilityIntent != "" &&
		heartbeat.AvailabilityIntent.Validate("agent_heartbeat.availability_intent") != nil {
		return ErrInvalidCommand
	}
	if heartbeat.ExcludedTargets != nil {
		if err := ValidateExcludedTargets(heartbeat.ExcludedTargets); err != nil {
			return ErrInvalidCommand
		}
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
		NodeID             domain.NodeID              `json:"nodeId"`
		NativeRunnerReady  *bool                      `json:"nativeRunnerReady"`
		AvailabilityIntent *domain.AvailabilityIntent `json:"availabilityIntent"`
		ExcludedTargets    *[]domain.TargetID         `json:"excludedTargets"`
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
	if wire.AvailabilityIntent != nil {
		heartbeat.AvailabilityIntent = *wire.AvailabilityIntent
	}
	if wire.ExcludedTargets != nil {
		heartbeat.ExcludedTargets = append([]domain.TargetID{}, *wire.ExcludedTargets...)
	}
	if err := heartbeat.Validate(); err != nil {
		return AgentHeartbeat{}, err
	}
	return heartbeat, nil
}
