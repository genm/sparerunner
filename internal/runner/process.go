package runner

import (
	"context"
	"log/slog"
)

type Process struct {
	PID         int
	Containment ContainmentRef
}

type StartRequest struct {
	Executable  string
	Directory   string
	Arguments   []string // non-secret flags only
	Containment ContainmentRef
	jit         jitArgument
}

func (StartRequest) String() string           { return "runner.start-request[redacted]" }
func (request StartRequest) GoString() string { return request.String() }
func (request StartRequest) LogValue() slog.Value {
	return slog.StringValue(request.String())
}
func (StartRequest) MarshalJSON() ([]byte, error) {
	return []byte(`{"runtimeMaterial":"redacted"}`), nil
}

// jitArgument is one-way runtime material. It intentionally cannot be formatted
// or serialized by callers that observe StartRequest.
type jitArgument struct{ value string }

func (j jitArgument) String() string   { return "runner.jit(redacted)" }
func (j jitArgument) GoString() string { return j.String() }
func (j jitArgument) LogValue() slog.Value {
	return slog.StringValue(j.String())
}
func (jitArgument) MarshalJSON() ([]byte, error) {
	return []byte(`"redacted"`), nil
}

// Supervisor owns the whole descendant tree, not merely the listener PID.
// Windows deliberately reports no strong capability until twk-009 supplies the
// Job Object adapter; callers must fail closed instead of claiming cleanup.
type Supervisor interface {
	StrongDescendantOwnership() bool
	// PrepareContainment creates or resolves the durable ownership boundary
	// before any JIT-bearing process can start. A crash in StateStarting can
	// therefore reconcile the boundary without trusting a PID.
	PrepareContainment(context.Context, string) (ContainmentRef, error)
	Start(context.Context, StartRequest) (Process, error)
	Stop(context.Context, Process) error
	Alive(Process) (bool, error)
}

func NewSupervisor() Supervisor { return newPlatformSupervisor() }
