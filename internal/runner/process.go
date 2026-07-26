package runner

import (
	"context"
	"log/slog"
	"sync"
)

type Process struct {
	PID         int
	Containment ContainmentRef
}

type StartRequest struct {
	Executable   string
	Directory    string
	Arguments    []string // non-secret flags only
	WorkspaceRef WorkspaceRef
	Containment  ContainmentRef
	jit          jitArgument
	verify       func(context.Context) error
}

func (StartRequest) String() string           { return "runner.start-request[redacted]" }
func (request StartRequest) GoString() string { return request.String() }
func (request StartRequest) LogValue() slog.Value {
	return slog.StringValue(request.String())
}
func (StartRequest) MarshalJSON() ([]byte, error) {
	return []byte(`{"runtimeMaterial":"redacted"}`), nil
}

// VerifyWorkspaceAtExec performs the platform-owned identity observation at the
// last possible point before exec. A strong Supervisor must call it while
// holding the same fence that linearizes Start with Stop.
func (request StartRequest) VerifyWorkspaceAtExec(ctx context.Context) error {
	if request.verify == nil {
		return ErrWorkspaceChanged
	}
	if err := request.verify(ctx); err != nil {
		return ErrWorkspaceChanged
	}
	return nil
}

// DeliverJIT synchronously exposes the JIT configuration to the platform start
// transaction exactly once. The request deliberately offers no raw accessor.
// Consumption is committed before the callback, so an error can never cause the
// same one-job credential to be delivered again.
func (request StartRequest) DeliverJIT(deliver func(string) error) error {
	if request.jit.lease == nil || deliver == nil {
		return ErrInvalidRequest
	}
	request.jit.lease.mu.Lock()
	if request.jit.lease.consumed {
		request.jit.lease.mu.Unlock()
		return ErrInvalidRequest
	}
	request.jit.lease.consumed = true
	value := request.jit.lease.value
	request.jit.lease.value = ""
	request.jit.lease.mu.Unlock()
	if err := deliver(value); err != nil {
		return ErrStartFailed
	}
	return nil
}

// jitArgument is one-way runtime material. It intentionally cannot be formatted
// or serialized by callers that observe StartRequest.
type jitArgument struct{ lease *jitLease }

type jitLease struct {
	mu       sync.Mutex
	consumed bool
	value    string
}

func newJITArgument(value string) jitArgument {
	return jitArgument{lease: &jitLease{value: value}}
}

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
	// WorkspaceBackend names the versioned WorkspaceRef encoding that Start can
	// verify at its exec boundary.
	WorkspaceBackend() string
	// PrepareContainment derives or resolves the durable ownership boundary before
	// any JIT-bearing process can start. It must be deterministic, idempotent, and
	// free of non-idempotent resources for an ExecutionID so a journal CAS loser
	// cannot own a second boundary. A crash in StateStarting can therefore
	// reconcile the boundary without trusting a PID. It returns an empty
	// FenceToken; the core adds the unique token before the Starting CAS.
	PrepareContainment(context.Context, string) (ContainmentRef, error)
	// Start must validate the containment FenceToken, call
	// StartRequest.VerifyWorkspaceAtExec while holding that fence, and consume
	// StartRequest.DeliverJIT exactly once in the same platform-owned transaction
	// immediately before exec. It returns ErrWorkspaceChanged or ErrStartFenced
	// without starting a process.
	Start(context.Context, StartRequest) (Process, error)
	// Stop is idempotent and linearizes with Start for one FenceToken. After Stop
	// returns nil, an in-flight or future Start carrying that token can never
	// create a process and must return ErrStartFenced.
	Stop(context.Context, Process) error
	Alive(Process) (bool, error)
}

func NewSupervisor() Supervisor { return newPlatformSupervisor() }
