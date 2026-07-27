// Package nodectl is the node-local control contract between the privileged
// Agent service and the unprivileged desktop surfaces (tray, launcher, CLI).
//
// It is deliberately an allowlist of two operations over a same-host endpoint:
// read non-secret node status, and set the node-local availability intent. No
// JIT material, token, certificate, join code, or process output has a field
// here, so a compromised desktop session cannot escalate through the Agent
// beyond withholding this computer's own capacity.
package nodectl

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/genm/tewake/internal/domain"
)

// ProtocolVersion is exact-matched. A mismatch is an explicit error before 1.0
// rather than a compatibility shim, so a stale desktop client never renders a
// state it does not understand.
const ProtocolVersion = 1

// MaxMessageBytes bounds one request or response frame. The contract carries a
// bounded status document, so anything larger is malformed input.
const MaxMessageBytes = 1 << 20

type Operation string

const (
	OperationStatus Operation = "status"
	OperationPause  Operation = "pause"
	OperationResume Operation = "resume"
)

func (operation Operation) Validate() error {
	switch operation {
	case OperationStatus, OperationPause, OperationResume:
		return nil
	default:
		return ErrUnsupportedOperation
	}
}

// Source names the requesting desktop surface for audit and display. It is
// self-reported provenance, never an authorization claim: authorization is the
// kernel-verified peer identity.
type Source string

const (
	SourceCLI     Source = "cli"
	SourceTray    Source = "tray"
	SourceRaycast Source = "raycast"
)

func (source Source) Validate() error {
	switch source {
	case SourceCLI, SourceTray, SourceRaycast:
		return nil
	default:
		return ErrInvalidRequest
	}
}

type Request struct {
	ProtocolVersion int       `json:"protocolVersion"`
	Operation       Operation `json:"operation"`
	Source          Source    `json:"source"`
}

func (request Request) Validate() error {
	if request.ProtocolVersion != ProtocolVersion {
		return ErrProtocolMismatch
	}
	if err := request.Operation.Validate(); err != nil {
		return err
	}
	return request.Source.Validate()
}

// RunningExecution is the non-secret identity of work this computer is doing.
// It carries no workspace path, log, or credential.
type RunningExecution struct {
	ExecutionID domain.ExecutionID    `json:"executionId"`
	State       domain.ExecutionState `json:"state"`
}

// Status is the complete document every desktop surface renders. Every field is
// observation, so a client that cannot reach the Agent has nothing to display
// and must show the unknown state instead of inventing one.
type Status struct {
	ProtocolVersion int                       `json:"protocolVersion"`
	NodeID          domain.NodeID             `json:"nodeId"`
	Intent          domain.AvailabilityIntent `json:"intent"`
	// IntentExplicit distinguishes an owner decision from the untouched default.
	IntentExplicit          bool   `json:"intentExplicit"`
	IntentChangedAtUnixNano int64  `json:"intentChangedAtUnixNano"`
	IntentChangedBy         string `json:"intentChangedBy"`
	// ControllerConnected is this Agent's own observation of its session. It is
	// not a claim about controller-side administrative state.
	ControllerConnected bool `json:"controllerConnected"`
	// PendingResume is true when the owner accepts jobs but the controller has
	// not confirmed it. Resuming adds capacity, so it stays ineffective until
	// then and must never render as accepting.
	PendingResume      bool               `json:"pendingResume"`
	NativeRunnerReady  bool               `json:"nativeRunnerReady"`
	RunningExecutions  []RunningExecution `json:"runningExecutions"`
	ObservedAtUnixNano int64              `json:"observedAtUnixNano"`
	AgentVersion       string             `json:"agentVersion"`
}

// EffectiveAccepting reports whether this node currently offers capacity as far
// as the Agent can observe. It is the conjunction the desktop surfaces render.
func (status Status) EffectiveAccepting() bool {
	return status.Intent.Accepts() && status.ControllerConnected && !status.PendingResume
}

type Response struct {
	ProtocolVersion int     `json:"protocolVersion"`
	OK              bool    `json:"ok"`
	Status          *Status `json:"status,omitempty"`
	ErrorClass      string  `json:"errorClass,omitempty"`
	Message         string  `json:"message,omitempty"`
}

// Machine-readable error classes. Desktop clients branch on these rather than
// on message text.
const (
	ErrorClassProtocolMismatch     = "protocol_mismatch"
	ErrorClassInvalidRequest       = "invalid_request"
	ErrorClassUnsupportedOperation = "unsupported_operation"
	ErrorClassUnauthorizedPeer     = "unauthorized_peer"
	ErrorClassEndpointUnavailable  = "endpoint_unavailable"
	ErrorClassEndpointUnsupported  = "endpoint_unsupported"
	ErrorClassAgentDegraded        = "agent_degraded"
)

var (
	ErrProtocolMismatch     = errors.New("node control protocol version mismatch")
	ErrInvalidRequest       = errors.New("node control request is invalid")
	ErrUnsupportedOperation = errors.New("node control operation is unsupported")
	ErrUnauthorizedPeer     = errors.New("node control peer is not an authorized local owner")
	ErrEndpointUnavailable  = errors.New("node control endpoint is unavailable")
	ErrEndpointUnsupported  = errors.New("node control endpoint is unsupported on this platform")
	ErrAgentDegraded        = errors.New("node control agent state is degraded")
)

// Error carries the machine-readable class across the process boundary so a
// launcher can distinguish "install the CLI" from "the agent refused you".
type Error struct {
	Class   string
	Message string
}

func (e *Error) Error() string { return e.Class + ": " + e.Message }

func errorClassFor(err error) string {
	switch {
	case errors.Is(err, ErrProtocolMismatch):
		return ErrorClassProtocolMismatch
	case errors.Is(err, ErrUnsupportedOperation):
		return ErrorClassUnsupportedOperation
	case errors.Is(err, ErrUnauthorizedPeer):
		return ErrorClassUnauthorizedPeer
	case errors.Is(err, ErrEndpointUnsupported):
		return ErrorClassEndpointUnsupported
	case errors.Is(err, ErrEndpointUnavailable):
		return ErrorClassEndpointUnavailable
	case errors.Is(err, ErrInvalidRequest):
		return ErrorClassInvalidRequest
	default:
		return ErrorClassAgentDegraded
	}
}

func decodeStrict(payload []byte, target any) error {
	if len(payload) == 0 || len(payload) > MaxMessageBytes {
		return ErrInvalidRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalidRequest
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return ErrInvalidRequest
	}
	return nil
}
