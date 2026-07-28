package runner

import (
	"context"
	"log/slog"
	"os"
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
	lease := request.jit.lease
	if request.verify == nil || lease == nil {
		return ErrWorkspaceChanged
	}
	lease.mu.Lock()
	if lease.revoked || lease.verificationAttempted || lease.consumed {
		if !lease.revoked {
			lease.contractFailed = true
			lease.consumed = true
			lease.value = ""
		}
		lease.mu.Unlock()
		return ErrWorkspaceChanged
	}
	lease.verificationAttempted = true
	lease.mu.Unlock()

	verifyErr := request.verify(ctx)
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if verifyErr != nil || lease.revoked || lease.consumed || lease.contractFailed {
		if !lease.revoked {
			lease.contractFailed = true
			lease.consumed = true
			lease.value = ""
		}
		return ErrWorkspaceChanged
	}
	lease.workspaceVerified = true
	return nil
}

// DeliverJIT synchronously exposes the JIT configuration to the platform start
// transaction exactly once. The request deliberately offers no raw accessor.
// Consumption is committed before the callback, so an error can never cause the
// same one-job credential to be delivered again.
func (request StartRequest) DeliverJIT(deliver func(string) error) error {
	lease := request.jit.lease
	if lease == nil || deliver == nil {
		return ErrInvalidRequest
	}
	lease.mu.Lock()
	if lease.revoked {
		lease.mu.Unlock()
		return ErrInvalidRequest
	}
	if lease.consumed {
		lease.contractFailed = true
		lease.mu.Unlock()
		return ErrInvalidRequest
	}
	if !lease.workspaceVerified || lease.contractFailed {
		lease.contractFailed = true
		lease.consumed = true
		lease.value = ""
		lease.mu.Unlock()
		return ErrWorkspaceChanged
	}
	lease.consumed = true
	lease.deliveryInProgress = true
	value := lease.value
	lease.value = ""
	lease.mu.Unlock()

	deliverErr := deliver(value)
	lease.mu.Lock()
	defer lease.mu.Unlock()
	lease.deliveryInProgress = false
	if deliverErr != nil || lease.revoked || lease.contractFailed {
		lease.contractFailed = true
		return ErrStartFailed
	}
	lease.deliverySucceeded = true
	return nil
}

// finishStart atomically expires every retained StartRequest copy and proves
// that the platform completed the required verify-then-deliver sequence before
// returning success.
func (request StartRequest) finishStart() bool {
	lease := request.jit.lease
	if lease == nil {
		return false
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	satisfied := lease.workspaceVerified &&
		lease.deliverySucceeded &&
		!lease.deliveryInProgress &&
		!lease.contractFailed &&
		!lease.revoked
	lease.revoked = true
	lease.value = ""
	return satisfied
}

// jitArgument is one-way runtime material. It intentionally cannot be formatted
// or serialized by callers that observe StartRequest.
type jitArgument struct{ lease *jitLease }

type jitLease struct {
	mu                    sync.Mutex
	value                 string
	verificationAttempted bool
	workspaceVerified     bool
	consumed              bool
	deliveryInProgress    bool
	deliverySucceeded     bool
	contractFailed        bool
	revoked               bool
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
// Windows deliberately reports no strong capability until spr-009 supplies the
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

// CompletionWaiter is the optional platform contract used by a long-lived Agent
// to observe that the complete descendant boundary became empty. A returned nil
// is only an observation: Manager.Wait deliberately leaves the durable record in
// Running until the caller invokes Destroy and cleanup is verified.
//
// Implementations must honor ctx without inventing a job-duration timeout. A
// bare listener PID wait is insufficient because workflow descendants may outlive
// that process.
type CompletionWaiter interface {
	Wait(context.Context, Process) error
}

// CleanupFinalizer is the optional platform transaction that joins verified
// process absence, workspace removal, and durable fence finalization. It is
// used only for records with a versioned fenced containment.
type CleanupFinalizer interface {
	FinalizeCleanup(context.Context, Process, *os.Root, string, WorkspaceRef) error
	// GarbageCollectCleanup removes only the finalized tombstone left across the
	// Released journal commit. Failure is safe residue and must never recreate an
	// active authority.
	GarbageCollectCleanup(context.Context, Process) error
}

func NewSupervisor() Supervisor { return newPlatformSupervisor() }
