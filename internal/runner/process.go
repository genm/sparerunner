package runner

import "context"

type Process struct {
	PID         int
	Containment ContainmentRef
}

type StartRequest struct {
	Executable string
	Directory  string
	Arguments  []string // non-secret flags only
	jit        jitArgument
}

// jitArgument is one-way runtime material. It intentionally cannot be formatted
// or serialized by callers that observe StartRequest.
type jitArgument struct{ value string }

func (j jitArgument) String() string   { return "runner.jit(redacted)" }
func (j jitArgument) GoString() string { return j.String() }

// Supervisor owns the whole descendant tree, not merely the listener PID.
// Windows deliberately reports no strong capability until twk-009 supplies the
// Job Object adapter; callers must fail closed instead of claiming cleanup.
type Supervisor interface {
	StrongDescendantOwnership() bool
	Start(context.Context, StartRequest) (Process, error)
	Stop(context.Context, Process) error
	Alive(Process) (bool, error)
}

func NewSupervisor() Supervisor { return newPlatformSupervisor() }
