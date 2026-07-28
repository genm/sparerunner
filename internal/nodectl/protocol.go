// Package nodectl is the node-local control contract between the privileged
// Agent service and the unprivileged desktop surfaces (tray, launcher, CLI).
//
// It is deliberately a small allowlist over a same-host endpoint: read
// non-secret node status, set the node-local availability intent, and edit the
// node owner's per-Target exclusion set. No JIT material, token, certificate,
// join code, or process output has a field here, so a compromised desktop
// session cannot escalate through the Agent beyond withholding this computer's
// own capacity — in whole, or for individual GitHub Targets.
package nodectl

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/genm/sparerunner/internal/domain"
)

// ProtocolVersion is exact-matched. A mismatch is an explicit error before 1.0
// rather than a compatibility shim, so a stale desktop client never renders a
// state it does not understand. Version 2 added the per-Target exclusion
// operations, the per-Target view on Status, and target attribution on running
// executions.
const ProtocolVersion = 2

// MaxMessageBytes bounds one request or response frame. The contract carries a
// bounded status document, so anything larger is malformed input.
const MaxMessageBytes = 1 << 20

type Operation string

const (
	OperationStatus Operation = "status"
	OperationPause  Operation = "pause"
	OperationResume Operation = "resume"
	// OperationTargets reads the same status document as OperationStatus. It
	// exists as its own verb so a desktop surface can express "show me the
	// per-Target view" without implying it may mutate anything.
	OperationTargets Operation = "targets"
	OperationExclude Operation = "exclude"
	OperationInclude Operation = "include"
)

func (operation Operation) Validate() error {
	switch operation {
	case OperationStatus, OperationPause, OperationResume,
		OperationTargets, OperationExclude, OperationInclude:
		return nil
	default:
		return ErrUnsupportedOperation
	}
}

// carriesTargetID reports whether an operation takes a target identifier. It is
// an exact partition rather than a permissive check: an operation that does not
// take one must not be sent with one, so a client cannot smuggle a field the
// server would silently ignore.
func (operation Operation) carriesTargetID() bool {
	return operation == OperationExclude || operation == OperationInclude
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
	// TargetID names the GitHub Target for exclude and include, and must be
	// absent for every other operation. Its shape is validated here so garbage
	// from a desktop client can never reach SQLite or the controller wire; its
	// existence deliberately is not, because excluding a Target this node has
	// never heard of is a safe no-op rather than an error.
	TargetID domain.TargetID `json:"targetId,omitempty"`
}

func (request Request) Validate() error {
	if request.ProtocolVersion != ProtocolVersion {
		return ErrProtocolMismatch
	}
	if err := request.Operation.Validate(); err != nil {
		return err
	}
	if err := request.Source.Validate(); err != nil {
		return err
	}
	if !request.Operation.carriesTargetID() {
		if request.TargetID != "" {
			return ErrInvalidRequest
		}
		return nil
	}
	if request.TargetID.ValidateShape("request.target_id") != nil {
		return ErrInvalidRequest
	}
	return nil
}

// RunningExecution is the non-secret identity of work this computer is doing.
// It carries no workspace path, log, or credential.
type RunningExecution struct {
	ExecutionID domain.ExecutionID    `json:"executionId"`
	State       domain.ExecutionState `json:"state"`
	// TargetID, Scope, and ScopeKind name the org/repo this job belongs to, so a
	// desktop surface can say what the computer is working on. They are omitted
	// for an execution admitted before target attribution existed.
	TargetID  domain.TargetID        `json:"targetId,omitempty"`
	Scope     string                 `json:"scope,omitempty"`
	ScopeKind domain.TargetScopeKind `json:"scopeKind,omitempty"`
}

// EligibleTarget mirrors transport.EligibleTarget's shape for the node-local
// display contract. It is redeclared here rather than importing the
// transport package, so a compromised desktop session's dependency surface
// never reaches the wire/session-actor code that speaks to the Controller.
type EligibleTarget struct {
	TargetID     domain.TargetID        `json:"targetId"`
	ScopeKind    domain.TargetScopeKind `json:"scopeKind"`
	Scope        string                 `json:"scope"`
	ScaleSetName string                 `json:"scaleSetName"`
	// Excluded is the controller's adopted view, echoed back on the heartbeat
	// acknowledgement. LocallyExcluded is this computer's own durable decision.
	Excluded        bool `json:"excluded"`
	LocallyExcluded bool `json:"locallyExcluded"`
	// Pending is the honest disagreement between the two. Excluding is
	// subtractive and locally effective at once, so it renders as excluded while
	// still pending; including is additive and ineffective until the controller
	// releases it, so it must never render as served.
	Pending bool `json:"pending"`
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
	PendingResume     bool `json:"pendingResume"`
	NativeRunnerReady bool `json:"nativeRunnerReady"`
	// SharedRunnerIdentity is true when this node's native runner executes jobs
	// under the Agent's own uid instead of a dedicated per-runner uid. The
	// property that is dropped is uid isolation: a job can reach whatever the
	// Agent's own user can reach, including the Agent's state directory and any
	// other job running on this computer. It is always reported so an operator
	// can never mistake this mode for the isolated one, and it is observation
	// only — it never adds or withholds capacity.
	SharedRunnerIdentity bool               `json:"sharedRunnerIdentity"`
	RunningExecutions    []RunningExecution `json:"runningExecutions"`
	// EligibleTargets is the last-known list of configured GitHub Targets whose
	// Runner Profile matches this node's platform, as refreshed by the
	// controller's heartbeat acknowledgement. It is omitted until the first
	// heartbeat round trip completes, and it is display data only: it never
	// implies a free slot exists right now.
	//
	// It is a pointer so a confirmed-empty list (a successful heartbeat that
	// found zero eligible Targets) stays distinct from never-reported: a plain
	// slice with `omitempty` would encode both as an absent field, because
	// encoding/json's omitempty checks length rather than nilness for slices. A
	// pointer's omitempty checks only the pointer itself.
	EligibleTargets *[]EligibleTarget `json:"eligibleTargets,omitempty"`
	// UnknownExclusions are locally excluded Targets absent from the last
	// eligible list. Excluding a Target this node has never been told about —
	// while offline, or before the first heartbeat round trip — is deliberately
	// legal, so these render as not-currently-eligible rather than as an error.
	UnknownExclusions  []domain.TargetID `json:"unknownExclusions,omitempty"`
	ObservedAtUnixNano int64             `json:"observedAtUnixNano"`
	AgentVersion       string            `json:"agentVersion"`
}

// EffectiveAccepting reports whether this node currently offers capacity as far
// as the Agent can observe. It is the conjunction the desktop surfaces render.
func (status Status) EffectiveAccepting() bool {
	return status.Intent.Accepts() && status.ControllerConnected && !status.PendingResume
}

// Targets safely dereferences EligibleTargets. A caller that only needs to
// range or measure the list, rather than distinguish never-reported from
// confirmed-empty, uses this instead of a nil-pointer check at every call
// site.
func (status Status) Targets() []EligibleTarget {
	if status.EligibleTargets == nil {
		return nil
	}
	return *status.EligibleTargets
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
