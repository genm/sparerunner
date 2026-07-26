// Package linux provides the Linux runner ownership boundary.  It deliberately
// keeps cgroup and workspace operations behind Runtime and Workspace seams so the
// lifecycle contract can be exercised on non-Linux development hosts.
package linux

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/genm/tewake/internal/runner"
)

const (
	// WorkspaceBackend is the versioned stat identity encoding shared by the
	// Cleaner and Supervisor.  It is intentionally independent of cgroup naming.
	WorkspaceBackend    = "linux-statx-v1"
	containmentBackend  = "linux-cgroup-v2-v1"
	maxJITMaterialBytes = 256 << 10

	// DefaultAmbiguousCleanupTimeout is shorter than the packaged Supervisor's
	// TimeoutStopSec. A lost local session therefore gets a bounded opportunity
	// to revoke and empty its cgroup before systemd owns final process teardown.
	DefaultAmbiguousCleanupTimeout = 20 * time.Second
	defaultObservationTimeout      = 5 * time.Second
)

// RunnerIdentity is the Unix credential bound to a dedicated runner slot.
// Sharing an identity across concurrently running slots is not a containment
// boundary and is rejected by the production application wiring.
type RunnerIdentity struct {
	UID int
	GID int
}

// IdentityResolver maps the durable containment observation to one slot's
// credential. It may reject a reference that is not assigned to this adapter.
// The core currently has no SlotKey in StartRequest; callers therefore must use
// one adapter/manager per slot until that public seam carries a SlotKey.
type IdentityResolver interface {
	ResolveRunnerIdentity(runner.ContainmentRef) (RunnerIdentity, error)
}

// StaticIdentity is the safe twk-007 default for slot 0. It is deliberately a
// single identity, not a claim that maxRunners greater than one is isolated.
type StaticIdentity RunnerIdentity

func (identity StaticIdentity) ResolveRunnerIdentity(runner.ContainmentRef) (RunnerIdentity, error) {
	result := RunnerIdentity(identity)
	if result.UID < 0 || result.GID < 0 {
		return RunnerIdentity{}, runner.ErrStrongOwnershipUnavailable
	}
	return result, nil
}

// Config contains only non-secret ownership configuration. JIT material never
// enters Config or logs.
type Config struct {
	Identity                IdentityResolver
	AmbiguousCleanupTimeout time.Duration
}

// Cgroup is the resolved v2 ownership boundary.  Scope is backend-private and
// contains no controller or JIT material.
type Cgroup struct {
	Scope        string
	HostEpoch    string
	InvocationID string
}

// LaunchSpec is the non-secret portion of a runner invocation.  The one-job JIT
// is supplied only through the anonymous reader passed to Runtime.Launch.
type LaunchSpec struct {
	Executable   string
	Directory    string
	Arguments    []string
	UID          int
	GID          int
	WorkspaceRef runner.WorkspaceRef
	Containment  runner.ContainmentRef
	// DirectoryHandle and ExecutableHandle are opened and verified by the root
	// helper. FileRuntime must launch through these descriptors rather than
	// resolving the Agent-writable executions pathname again.
	DirectoryHandle  *os.File
	ExecutableHandle *os.File
}

// Fence holds exclusive ownership of one containment/fence pair.  LockFence
// must not return until it can make Start and Stop linearizable across processes.
type Fence interface {
	Revoked() (bool, error)
	Launched() (bool, error)
	MarkLaunched(context.Context) error
	Revoke(context.Context) error
	Close() error
}

// Runtime is the Linux-specific control-plane seam.  Production implementations
// use a cgroup-v2 directory, durable fence records, cgroup.kill, and a launcher
// that receives JIT only from an anonymous pipe.  Tests inject an in-memory host.
type Runtime interface {
	EnsureCgroup(context.Context, string) (Cgroup, error)
	LockFence(context.Context, runner.ContainmentRef) (Fence, error)
	Launch(context.Context, LaunchSpec, io.Reader) (int, error)
	KillAndWait(context.Context, runner.ContainmentRef) error
	// WaitEmpty observes natural runner completion without imposing a job
	// timeout. Its lifetime is owned by the Agent process context.
	WaitEmpty(context.Context, runner.ContainmentRef) error
	Alive(context.Context, runner.ContainmentRef, int) (bool, error)
}

// SlotAdmission is required by the privileged helper to enforce the twk-007
// single-slot account boundary across executions and helper restarts.
type SlotAdmission interface {
	SlotBusy(context.Context, runner.ContainmentRef) (bool, error)
}

// RuntimeAdmission is the fixed, side-effect-free readiness boundary for the
// delegated cgroup and durable fence roots. The root Helper invokes it on every
// Agent heartbeat rather than relying on checks made only at Supervisor startup.
type RuntimeAdmission interface {
	ValidateAdmission(context.Context) error
}

type RuntimeCleanupFinalizer interface {
	FinalizeCleanup(
		context.Context,
		runner.ContainmentRef,
		*os.Root,
		string,
		runner.WorkspaceRef,
	) error
	GarbageCollectFence(context.Context, runner.ContainmentRef) error
}

type RuntimeShutdown interface {
	Shutdown(context.Context) error
}

// Workspace owns identity and deletion checks beneath the core-provided root
// handle.  Prepare must apply the dedicated runner identity before returning the
// ref; Observe must only observe and never repair the path.
type Workspace interface {
	ValidateRuntimeRoot(context.Context, string) error
	Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error)
	Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error)
	Remove(context.Context, *os.Root, string) error
	AgentIdentity() RunnerIdentity
	RunnerIdentity() RunnerIdentity
}

type WorkspaceSlotAdmission interface {
	SlotBusy(context.Context, *os.Root, string) (bool, error)
}

type WorkspaceAbsence interface {
	Absent(context.Context, *os.Root, string) (bool, error)
}

// Adapter implements runner.Supervisor and runner.Cleaner.
type Adapter struct {
	config    Config
	runtime   Runtime
	workspace Workspace
	cleanup   time.Duration

	// processMu keeps no lifecycle authority.  It only serializes Alive calls on
	// hosts whose process observation backend is not internally concurrent.
	processMu sync.Mutex
}

// New validates the adapter's static capability boundary.  A missing runtime or
// workspace is not allowed to degrade to PID-based ownership.
func New(config Config, runtime Runtime, workspace Workspace) (*Adapter, error) {
	if runtime == nil || workspace == nil || config.Identity == nil ||
		config.AmbiguousCleanupTimeout < 0 {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if config.AmbiguousCleanupTimeout == 0 {
		config.AmbiguousCleanupTimeout = DefaultAmbiguousCleanupTimeout
	}
	agentIdentity := workspace.AgentIdentity()
	runnerIdentity := workspace.RunnerIdentity()
	if agentIdentity.UID <= 0 || agentIdentity.GID <= 0 ||
		runnerIdentity.UID <= 0 || runnerIdentity.GID <= 0 ||
		agentIdentity.UID == runnerIdentity.UID ||
		agentIdentity.GID == runnerIdentity.GID {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return &Adapter{
		config: config, runtime: runtime, workspace: workspace,
		cleanup: config.AmbiguousCleanupTimeout,
	}, nil
}

func (*Adapter) StrongDescendantOwnership() bool { return true }
func (*Adapter) StrongWorkspaceOwnership() bool  { return true }
func (*Adapter) WorkspaceBackend() string        { return WorkspaceBackend }

func (adapter *Adapter) ValidateRuntimeRoot(ctx context.Context, root string) error {
	if err := ctx.Err(); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if err := adapter.workspace.ValidateRuntimeRoot(ctx, root); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (adapter *Adapter) PrepareWorkspace(ctx context.Context, root *os.Root, name string) (runner.WorkspaceRef, error) {
	if err := ctx.Err(); err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	ref, err := adapter.workspace.Prepare(ctx, root, name)
	if err != nil || ref.Backend != WorkspaceBackend || ref.OwnerID == "" {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return ref, nil
}

func (adapter *Adapter) WorkspaceRef(ctx context.Context, root *os.Root, name string) (runner.WorkspaceRef, error) {
	if err := ctx.Err(); err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	ref, err := adapter.workspace.Observe(ctx, root, name)
	if err != nil || ref.Backend != WorkspaceBackend || ref.OwnerID == "" {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return ref, nil
}

func (adapter *Adapter) RemoveAndVerify(ctx context.Context, root *os.Root, name string) error {
	if err := ctx.Err(); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := adapter.workspace.Remove(ctx, root, name); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (adapter *Adapter) PrepareContainment(ctx context.Context, executionID string) (runner.ContainmentRef, error) {
	if err := ctx.Err(); err != nil || executionID == "" {
		return runner.ContainmentRef{}, runner.ErrStrongOwnershipUnavailable
	}
	owner := containmentOwner(executionID)
	cgroup, err := adapter.runtime.EnsureCgroup(ctx, owner)
	if err != nil || cgroup.Scope != path.Join("tewake", owner) ||
		cgroup.HostEpoch == "" || cgroup.InvocationID != "" {
		return runner.ContainmentRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return runner.ContainmentRef{
		Backend:      containmentBackend,
		OwnerID:      owner,
		Scope:        cgroup.Scope,
		HostEpoch:    cgroup.HostEpoch,
		InvocationID: cgroup.InvocationID,
	}, nil
}

func (adapter *Adapter) Start(ctx context.Context, request runner.StartRequest) (runner.Process, error) {
	if err := ctx.Err(); err != nil {
		return runner.Process{Containment: request.Containment}, runner.ErrStartFenced
	}
	if !adapter.validContainment(request.Containment) || request.WorkspaceRef.Backend != WorkspaceBackend || request.WorkspaceRef.OwnerID == "" {
		return runner.Process{Containment: request.Containment}, runner.ErrStrongOwnershipUnavailable
	}
	identity, err := adapter.config.Identity.ResolveRunnerIdentity(request.Containment)
	if err != nil || identity.UID < 0 || identity.GID < 0 || identity != adapter.workspace.RunnerIdentity() {
		return runner.Process{Containment: request.Containment}, runner.ErrStrongOwnershipUnavailable
	}
	fence, err := adapter.runtime.LockFence(ctx, request.Containment)
	if err != nil {
		return runner.Process{Containment: request.Containment}, runner.ErrStartFenced
	}
	fenceClosed := false
	defer func() {
		if !fenceClosed {
			_ = fence.Close()
		}
	}()
	revoked, err := fence.Revoked()
	if err != nil || revoked {
		return runner.Process{Containment: request.Containment}, runner.ErrStartFenced
	}
	if err := request.VerifyWorkspaceAtExec(ctx); err != nil {
		return runner.Process{Containment: request.Containment}, runner.ErrWorkspaceChanged
	}

	pid := 0
	launchAttempted := false
	err = request.DeliverJIT(func(jit string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		launchAttempted = true
		startedPID, launchErr := adapter.launchWithPipe(ctx, request, jit, identity)
		if launchErr != nil {
			return launchErr
		}
		pid = startedPID
		return nil
	})
	if err != nil {
		if launchAttempted {
			// The pipe may have reached a launcher before a context or transport
			// failure.  Kill the exclusive cgroup before returning so the core's
			// later idempotent Stop is never the first containment action.
			// Once material may have reached exec, caller cancellation cannot
			// release capacity. The platform must prove the cgroup empty; service
			// shutdown remains the outer systemd ownership boundary.
			cleanupCtx, cancelCleanup := context.WithTimeout(
				context.WithoutCancel(ctx),
				adapter.cleanup,
			)
			defer cancelCleanup()
			_ = fence.Revoke(cleanupCtx)
			_ = adapter.runtime.KillAndWait(cleanupCtx, request.Containment)
		}
		return runner.Process{Containment: request.Containment}, err
	}
	if pid <= 0 {
		return runner.Process{Containment: request.Containment}, runner.ErrStartFailed
	}
	if err := fence.MarkLaunched(ctx); err != nil {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), adapter.cleanup)
		defer cancelCleanup()
		_ = fence.Revoke(cleanupCtx)
		_ = adapter.runtime.KillAndWait(cleanupCtx, request.Containment)
		return runner.Process{PID: pid, Containment: request.Containment}, runner.ErrStartFailed
	}
	if err := fence.Close(); err != nil {
		fenceClosed = true
		return runner.Process{PID: pid, Containment: request.Containment}, runner.ErrStartFailed
	}
	fenceClosed = true
	return runner.Process{PID: pid, Containment: request.Containment}, nil
}

func (adapter *Adapter) Stop(ctx context.Context, process runner.Process) error {
	if err := ctx.Err(); err != nil {
		return runner.ErrCleanupFailed
	}
	if !adapter.validContainment(process.Containment) {
		return runner.ErrCleanupFailed
	}
	fence, err := adapter.runtime.LockFence(ctx, process.Containment)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	fenceClosed := false
	defer func() {
		if !fenceClosed {
			_ = fence.Close()
		}
	}()
	if err := fence.Revoke(ctx); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := adapter.runtime.KillAndWait(ctx, process.Containment); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := fence.Close(); err != nil {
		fenceClosed = true
		return runner.ErrCleanupFailed
	}
	fenceClosed = true
	return nil
}

func (adapter *Adapter) FinalizeCleanup(
	ctx context.Context,
	process runner.Process,
	root *os.Root,
	name string,
	expected runner.WorkspaceRef,
) error {
	finalizer, ok := adapter.runtime.(RuntimeCleanupFinalizer)
	if !ok || ctx.Err() != nil || !adapter.validContainment(process.Containment) ||
		root == nil || !validAdapterWorkspaceName(name) ||
		process.Containment.OwnerID != "tewake-"+name ||
		expected.Backend != WorkspaceBackend || expected.OwnerID == "" {
		return runner.ErrCleanupFailed
	}
	if err := finalizer.FinalizeCleanup(ctx, process.Containment, root, name, expected); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (adapter *Adapter) GarbageCollectCleanup(ctx context.Context, process runner.Process) error {
	finalizer, ok := adapter.runtime.(RuntimeCleanupFinalizer)
	if !ok || ctx.Err() != nil || !adapter.validContainment(process.Containment) {
		return runner.ErrCleanupFailed
	}
	if err := finalizer.GarbageCollectFence(ctx, process.Containment); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (adapter *Adapter) Alive(process runner.Process) (bool, error) {
	if !adapter.validContainment(process.Containment) || process.PID <= 0 {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	adapter.processMu.Lock()
	defer adapter.processMu.Unlock()
	timeout := min(adapter.cleanup, defaultObservationTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	alive, err := adapter.runtime.Alive(ctx, process.Containment, process.PID)
	if err != nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	return alive, nil
}

// Wait blocks until the complete cgroup descendant tree is empty.  It is an
// optional Linux completion seam: the Agent calls Manager.Destroy only after
// this returns, so an Agent/controller transport disconnect does not stop the
// already-running trusted job.
func (adapter *Adapter) Wait(ctx context.Context, process runner.Process) error {
	if err := ctx.Err(); err != nil || !adapter.validContainment(process.Containment) || process.PID <= 0 {
		return runner.ErrStrongOwnershipUnavailable
	}
	if err := adapter.runtime.WaitEmpty(ctx, process.Containment); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

// launchWithPipe leaves no adapter launcher goroutine behind. Runtime.Launch is
// required to honor ctx; the only helper is the pipe writer, which is always
// joined before this function returns.
func (adapter *Adapter) launchWithPipe(ctx context.Context, request runner.StartRequest, jit string, identity RunnerIdentity) (int, error) {
	reader, writer := io.Pipe()
	written := make(chan error, 1)
	go func() {
		_, writeErr := io.WriteString(writer, jit)
		written <- writer.CloseWithError(writeErr)
	}()
	pid, launchErr := adapter.runtime.Launch(ctx, LaunchSpec{
		Executable:   request.Executable,
		Directory:    request.Directory,
		Arguments:    append([]string(nil), request.Arguments...),
		UID:          identity.UID,
		GID:          identity.GID,
		WorkspaceRef: request.WorkspaceRef,
		Containment:  request.Containment,
	}, reader)
	if launchErr != nil {
		_ = reader.CloseWithError(launchErr)
		_ = writer.CloseWithError(launchErr)
		<-written
		return 0, launchErr
	}
	select {
	case writeErr := <-written:
		if writeErr != nil {
			_ = reader.CloseWithError(writeErr)
			return 0, writeErr
		}
	case <-ctx.Done():
		_ = reader.CloseWithError(ctx.Err())
		_ = writer.CloseWithError(ctx.Err())
		<-written
		return 0, ctx.Err()
	}
	if pid <= 0 {
		return 0, errors.New("linux runner launcher returned no pid")
	}
	return pid, nil
}

func (adapter *Adapter) validContainment(ref runner.ContainmentRef) bool {
	return ref.Backend == containmentBackend &&
		ref.OwnerID != "" &&
		ref.Scope == path.Join("tewake", ref.OwnerID) &&
		ref.HostEpoch != "" &&
		ref.InvocationID == "" &&
		canonicalToken(ref.FenceToken)
}

func containmentOwner(executionID string) string {
	sum := sha256.Sum256([]byte(executionID))
	return "tewake-" + hex.EncodeToString(sum[:])
}

func canonicalToken(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func validAdapterWorkspaceName(name string) bool {
	if len(name) != 64 || path.Base(name) != name {
		return false
	}
	for _, character := range name {
		if !(character >= '0' && character <= '9') &&
			!(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

var _ runner.Supervisor = (*Adapter)(nil)
var _ runner.Cleaner = (*Adapter)(nil)
var _ runner.CompletionWaiter = (*Adapter)(nil)
var _ runner.CleanupFinalizer = (*Adapter)(nil)
