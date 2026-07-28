package linux

import (
	"context"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

// SharedWorkspaceBackend is the versioned stat identity encoding used only by
// the shared-identity mode. It is deliberately different from WorkspaceBackend
// so a workspace prepared under one mode can never be observed, verified, or
// released under the other: the two encodings record different owning
// credentials, and the core refuses a ref whose backend does not match the
// adapter that produced it.
const SharedWorkspaceBackend = "linux-shared-identity-statx-v1"

// RootlessAdapter is the opt-in Linux Supervisor for owners who cannot install
// a root Supervisor service. It is a sibling of Adapter, never a fallback for
// it: the production wiring selects it only from an explicit
// --allow-shared-runner-identity flag.
//
// It drops exactly one property of the privileged adapter: the job no longer
// runs under a dedicated non-login uid, so it executes with the Agent's own
// Unix credential and can read and write everything that user can, including
// the Agent's state directory. Native mode is already documented as being for
// TRUSTED private workflows and is not a sandbox; this mode is strictly weaker
// than the privileged one and is reported as node state so it can never be
// mistaken for it.
//
// Everything else is preserved exactly: descendant ownership through a
// per-execution cgroup-v2 child and cgroup.kill, the durable start fence that
// linearizes Start with Stop, workspace identity verified at the exec
// boundary, one-shot JIT delivered exactly once immediately before exec,
// verified cleanup, and quarantine when cleanup cannot be proven.
type RootlessAdapter struct {
	runtime   Runtime
	workspace Workspace
	identity  RunnerIdentity
	cleanup   time.Duration

	// processMu keeps no lifecycle authority. It only serializes Alive calls on
	// hosts whose process observation backend is not internally concurrent.
	processMu sync.Mutex
}

// NewRootless validates the shared-identity capability boundary. It refuses a
// runtime that cannot finalize cleanup, an identity that is not exactly one
// credential shared by the Agent and the job, and any privileged claim: this
// constructor must never be reachable from the root Supervisor wiring.
func NewRootless(nativeRuntime Runtime, workspace Workspace, ambiguousCleanupTimeout time.Duration) (*RootlessAdapter, error) {
	if nativeRuntime == nil || workspace == nil || ambiguousCleanupTimeout < 0 {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if _, ok := nativeRuntime.(RuntimeCleanupFinalizer); !ok {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if ambiguousCleanupTimeout == 0 {
		ambiguousCleanupTimeout = DefaultAmbiguousCleanupTimeout
	}
	agent := workspace.AgentIdentity()
	job := workspace.RunnerIdentity()
	// The whole point of this mode is that the two are the same credential. A
	// difference means the caller wired the privileged workspace by mistake, and
	// this adapter would then make ownership claims it cannot enforce.
	if agent != job || agent.UID <= 0 || agent.GID <= 0 {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return &RootlessAdapter{
		runtime:   nativeRuntime,
		workspace: workspace,
		identity:  agent,
		cleanup:   ambiguousCleanupTimeout,
	}, nil
}

// StrongDescendantOwnership is true only because cgroup.kill owns the whole
// descendant tree. NewRootlessRuntime proves the delegated cgroup subtree and
// cgroup.kill at construction, so this is never an optimistic claim.
func (*RootlessAdapter) StrongDescendantOwnership() bool { return true }

// StrongWorkspaceOwnership is true because the workspace identity is a durable
// stat observation the same credential can verify and remove. It is not a claim
// that the job is isolated from the Agent.
func (*RootlessAdapter) StrongWorkspaceOwnership() bool { return true }

func (*RootlessAdapter) WorkspaceBackend() string { return SharedWorkspaceBackend }

func (adapter *RootlessAdapter) ValidateRuntimeRoot(ctx context.Context, root string) error {
	if err := ctx.Err(); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if err := adapter.workspace.ValidateRuntimeRoot(ctx, root); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (adapter *RootlessAdapter) PrepareWorkspace(ctx context.Context, root *os.Root, name string) (runner.WorkspaceRef, error) {
	if err := ctx.Err(); err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	ref, err := adapter.workspace.Prepare(ctx, root, name)
	if err != nil || ref.Backend != SharedWorkspaceBackend || ref.OwnerID == "" {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return ref, nil
}

func (adapter *RootlessAdapter) WorkspaceRef(ctx context.Context, root *os.Root, name string) (runner.WorkspaceRef, error) {
	if err := ctx.Err(); err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	ref, err := adapter.workspace.Observe(ctx, root, name)
	if err != nil || ref.Backend != SharedWorkspaceBackend || ref.OwnerID == "" {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return ref, nil
}

func (adapter *RootlessAdapter) RemoveAndVerify(ctx context.Context, root *os.Root, name string) error {
	if err := ctx.Err(); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := adapter.workspace.Remove(ctx, root, name); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

// PrepareContainment is deterministic and idempotent for one ExecutionID: the
// owner is a pure hash of it and EnsureCgroup only creates the leaf. A journal
// CAS loser therefore cannot own a second boundary.
func (adapter *RootlessAdapter) PrepareContainment(ctx context.Context, executionID string) (runner.ContainmentRef, error) {
	if err := ctx.Err(); err != nil || executionID == "" {
		return runner.ContainmentRef{}, runner.ErrStrongOwnershipUnavailable
	}
	owner := containmentOwner(executionID)
	cgroup, err := adapter.runtime.EnsureCgroup(ctx, owner)
	if err != nil || cgroup.Scope != path.Join("sparerunner", owner) ||
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

// Start mirrors the privileged adapter's ordering exactly: lock the durable
// fence, refuse a revoked token, verify the workspace identity while still
// holding that fence, then consume the one-shot JIT exactly once inside the
// same platform transaction, immediately before exec.
func (adapter *RootlessAdapter) Start(ctx context.Context, request runner.StartRequest) (runner.Process, error) {
	if err := ctx.Err(); err != nil {
		return runner.Process{Containment: request.Containment}, runner.ErrStartFenced
	}
	if !adapter.validContainment(request.Containment) ||
		request.WorkspaceRef.Backend != SharedWorkspaceBackend || request.WorkspaceRef.OwnerID == "" {
		return runner.Process{Containment: request.Containment}, runner.ErrStrongOwnershipUnavailable
	}
	if adapter.identity != adapter.workspace.RunnerIdentity() ||
		adapter.identity != adapter.workspace.AgentIdentity() {
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
		startedPID, launchErr := adapter.launchWithPipe(ctx, request, jit)
		if launchErr != nil {
			return launchErr
		}
		pid = startedPID
		return nil
	})
	if err != nil {
		if launchAttempted {
			// Material may already have reached exec, so caller cancellation cannot
			// release capacity. Prove the cgroup empty here rather than leaving the
			// core's later idempotent Stop as the first containment action.
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

// Stop is idempotent and linearizes with Start on the same durable fence token.
// KillAndWait returns only after cgroup.kill has emptied the whole descendant
// tree, so a setsid grandchild cannot survive it.
func (adapter *RootlessAdapter) Stop(ctx context.Context, process runner.Process) error {
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

func (adapter *RootlessAdapter) FinalizeCleanup(
	ctx context.Context,
	process runner.Process,
	root *os.Root,
	name string,
	expected runner.WorkspaceRef,
) error {
	finalizer, ok := adapter.runtime.(RuntimeCleanupFinalizer)
	if !ok || ctx.Err() != nil || !adapter.validContainment(process.Containment) ||
		root == nil || !validAdapterWorkspaceName(name) ||
		process.Containment.OwnerID != "sparerunner-"+name ||
		expected.Backend != SharedWorkspaceBackend || expected.OwnerID == "" {
		return runner.ErrCleanupFailed
	}
	if err := finalizer.FinalizeCleanup(ctx, process.Containment, root, name, expected); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (adapter *RootlessAdapter) GarbageCollectCleanup(ctx context.Context, process runner.Process) error {
	finalizer, ok := adapter.runtime.(RuntimeCleanupFinalizer)
	if !ok || ctx.Err() != nil || !adapter.validContainment(process.Containment) {
		return runner.ErrCleanupFailed
	}
	if err := finalizer.GarbageCollectFence(ctx, process.Containment); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (adapter *RootlessAdapter) Alive(process runner.Process) (bool, error) {
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

// Wait blocks until the complete cgroup descendant tree is empty, so an
// Agent/controller transport disconnect does not stop an already-running
// trusted job.
func (adapter *RootlessAdapter) Wait(ctx context.Context, process runner.Process) error {
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
func (adapter *RootlessAdapter) launchWithPipe(ctx context.Context, request runner.StartRequest, jit string) (int, error) {
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
		UID:          adapter.identity.UID,
		GID:          adapter.identity.GID,
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
		return 0, runner.ErrStartFailed
	}
	return pid, nil
}

func (adapter *RootlessAdapter) validContainment(ref runner.ContainmentRef) bool {
	return ref.Backend == containmentBackend &&
		ref.OwnerID != "" &&
		ref.Scope == path.Join("sparerunner", ref.OwnerID) &&
		ref.HostEpoch != "" &&
		ref.InvocationID == "" &&
		canonicalToken(ref.FenceToken)
}

var _ runner.Supervisor = (*RootlessAdapter)(nil)
var _ runner.Cleaner = (*RootlessAdapter)(nil)
var _ runner.CompletionWaiter = (*RootlessAdapter)(nil)
var _ runner.CleanupFinalizer = (*RootlessAdapter)(nil)
