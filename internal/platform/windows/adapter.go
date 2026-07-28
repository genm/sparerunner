// Package windows owns the Windows native-runner boundary. It keeps Job Object,
// service-token, and filesystem authority behind narrow interfaces so lifecycle
// ordering can be race-tested on every development platform.
package windows

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

const (
	WorkspaceBackend   = "windows-file-id-v1"
	containmentBackend = "windows-job-object-v1"

	defaultCleanupTimeout     = 20 * time.Second
	defaultObservationTimeout = 5 * time.Second
)

// Fence is a durable execution-specific start/stop serialization point. Its
// revoked state survives service recovery; a later Start can never revive it.
type Fence interface {
	Revoked() (bool, error)
	MarkLaunched(context.Context) error
	Revoke(context.Context) error
	Close() error
}

// LaunchSpec contains only the fixed, non-secret runner invocation. JIT
// material is supplied separately through an anonymous reader.
type LaunchSpec struct {
	Executable   string
	Directory    string
	Arguments    []string
	WorkspaceRef runner.WorkspaceRef
	Containment  runner.ContainmentRef
}

// Runtime is implemented by the Windows Service/Job Object owner.
type Runtime interface {
	ValidateAdmission(context.Context) error
	HostEpoch() string
	EnsureJob(context.Context, runner.ContainmentRef) error
	LockFence(context.Context, runner.ContainmentRef) (Fence, error)
	Launch(context.Context, LaunchSpec, io.Reader) (int, error)
	TerminateAndWait(context.Context, runner.ContainmentRef) error
	WaitEmpty(context.Context, runner.ContainmentRef) error
	Alive(context.Context, runner.ContainmentRef, int) (bool, error)
	FinalizeCleanup(
		context.Context,
		runner.ContainmentRef,
		*os.Root,
		string,
		runner.WorkspaceRef,
		Workspace,
	) error
	GarbageCollectFence(context.Context, runner.ContainmentRef) error
}

// Workspace binds the disposable directory to its Windows file identity and
// dedicated runner-service access boundary.
type Workspace interface {
	ValidateRuntimeRoot(context.Context, string) error
	Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error)
	Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error)
	Remove(context.Context, *os.Root, string) error
}

// Adapter implements both runner.Supervisor and runner.Cleaner.
type Adapter struct {
	runtime   Runtime
	workspace Workspace
	cleanup   time.Duration
	processMu sync.Mutex
}

func New(runtime Runtime, workspace Workspace) (*Adapter, error) {
	if runtime == nil || workspace == nil || !canonicalToken(runtime.HostEpoch()) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return &Adapter{
		runtime: runtime, workspace: workspace, cleanup: defaultCleanupTimeout,
	}, nil
}

func (*Adapter) StrongDescendantOwnership() bool { return true }
func (*Adapter) StrongWorkspaceOwnership() bool  { return true }
func (*Adapter) WorkspaceBackend() string        { return WorkspaceBackend }

func (adapter *Adapter) ValidateRuntimeRoot(ctx context.Context, root string) error {
	if ctx == nil || ctx.Err() != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	// Readiness is live: losing the runner-identity service must immediately
	// withdraw capacity even when the Agent's controller session remains online.
	if err := adapter.runtime.ValidateAdmission(ctx); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if err := adapter.workspace.ValidateRuntimeRoot(ctx, root); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (adapter *Adapter) PrepareWorkspace(
	ctx context.Context,
	root *os.Root,
	name string,
) (runner.WorkspaceRef, error) {
	ref, err := adapter.workspace.Prepare(ctx, root, name)
	if err != nil || ref.Backend != WorkspaceBackend || ref.OwnerID == "" {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return ref, nil
}

func (adapter *Adapter) WorkspaceRef(
	ctx context.Context,
	root *os.Root,
	name string,
) (runner.WorkspaceRef, error) {
	ref, err := adapter.workspace.Observe(ctx, root, name)
	if err != nil || ref.Backend != WorkspaceBackend || ref.OwnerID == "" {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return ref, nil
}

func (adapter *Adapter) RemoveAndVerify(ctx context.Context, root *os.Root, name string) error {
	if ctx == nil || ctx.Err() != nil {
		return runner.ErrCleanupFailed
	}
	if err := adapter.workspace.Remove(ctx, root, name); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (adapter *Adapter) PrepareContainment(
	ctx context.Context,
	executionID string,
) (runner.ContainmentRef, error) {
	if ctx == nil || ctx.Err() != nil || executionID == "" {
		return runner.ContainmentRef{}, runner.ErrStrongOwnershipUnavailable
	}
	owner := containmentOwner(executionID)
	epoch := adapter.runtime.HostEpoch()
	ref := runner.ContainmentRef{
		Backend:   containmentBackend,
		OwnerID:   owner,
		Scope:     jobObjectName(epoch, owner),
		HostEpoch: epoch,
	}
	if err := adapter.runtime.EnsureJob(ctx, ref); err != nil {
		return runner.ContainmentRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return ref, nil
}

func (adapter *Adapter) Start(
	ctx context.Context,
	request runner.StartRequest,
) (runner.Process, error) {
	process := runner.Process{Containment: request.Containment}
	if ctx == nil || ctx.Err() != nil {
		return process, runner.ErrStartFenced
	}
	if !adapter.validContainment(request.Containment) ||
		request.WorkspaceRef.Backend != WorkspaceBackend ||
		request.WorkspaceRef.OwnerID == "" {
		return process, runner.ErrStrongOwnershipUnavailable
	}
	fence, err := adapter.runtime.LockFence(ctx, request.Containment)
	if err != nil {
		return process, runner.ErrStartFenced
	}
	closed := false
	defer func() {
		if !closed {
			_ = fence.Close()
		}
	}()
	revoked, err := fence.Revoked()
	if err != nil || revoked {
		return process, runner.ErrStartFenced
	}
	if err := request.VerifyWorkspaceAtExec(ctx); err != nil {
		return process, runner.ErrWorkspaceChanged
	}

	launchAttempted := false
	err = request.DeliverJIT(func(jit string) error {
		launchAttempted = true
		pid, launchErr := adapter.launchWithPipe(ctx, request, jit)
		process.PID = pid
		return launchErr
	})
	if err != nil {
		if launchAttempted {
			cleanupCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				adapter.cleanup,
			)
			defer cancel()
			_ = fence.Revoke(cleanupCtx)
			_ = adapter.runtime.TerminateAndWait(cleanupCtx, request.Containment)
		}
		return process, err
	}
	if process.PID <= 0 {
		return process, runner.ErrStartFailed
	}
	if err := fence.MarkLaunched(ctx); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adapter.cleanup)
		defer cancel()
		_ = fence.Revoke(cleanupCtx)
		_ = adapter.runtime.TerminateAndWait(cleanupCtx, request.Containment)
		return process, runner.ErrStartFailed
	}
	if err := fence.Close(); err != nil {
		closed = true
		return process, runner.ErrStartFailed
	}
	closed = true
	return process, nil
}

func (adapter *Adapter) Stop(ctx context.Context, process runner.Process) error {
	if ctx == nil || ctx.Err() != nil || !adapter.validContainment(process.Containment) {
		return runner.ErrCleanupFailed
	}
	fence, err := adapter.runtime.LockFence(ctx, process.Containment)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	closed := false
	defer func() {
		if !closed {
			_ = fence.Close()
		}
	}()
	if err := fence.Revoke(ctx); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := adapter.runtime.TerminateAndWait(ctx, process.Containment); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := fence.Close(); err != nil {
		closed = true
		return runner.ErrCleanupFailed
	}
	closed = true
	return nil
}

func (adapter *Adapter) Alive(process runner.Process) (bool, error) {
	if process.PID <= 0 || !adapter.validContainment(process.Containment) {
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

func (adapter *Adapter) Wait(ctx context.Context, process runner.Process) error {
	if ctx == nil || ctx.Err() != nil || process.PID <= 0 ||
		!adapter.validContainment(process.Containment) {
		return runner.ErrStrongOwnershipUnavailable
	}
	if err := adapter.runtime.WaitEmpty(ctx, process.Containment); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (adapter *Adapter) FinalizeCleanup(
	ctx context.Context,
	process runner.Process,
	root *os.Root,
	name string,
	expected runner.WorkspaceRef,
) error {
	if ctx == nil || ctx.Err() != nil || root == nil ||
		!adapter.validContainment(process.Containment) ||
		!validWorkspaceName(name) ||
		process.Containment.OwnerID != "tewake-"+name ||
		expected.Backend != WorkspaceBackend ||
		expected.OwnerID == "" {
		return runner.ErrCleanupFailed
	}
	if err := adapter.runtime.FinalizeCleanup(
		ctx,
		process.Containment,
		root,
		name,
		expected,
		adapter.workspace,
	); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (adapter *Adapter) GarbageCollectCleanup(
	ctx context.Context,
	process runner.Process,
) error {
	if ctx == nil || ctx.Err() != nil || !adapter.validContainment(process.Containment) {
		return runner.ErrCleanupFailed
	}
	if err := adapter.runtime.GarbageCollectFence(ctx, process.Containment); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (adapter *Adapter) launchWithPipe(
	ctx context.Context,
	request runner.StartRequest,
	jit string,
) (int, error) {
	reader, writer := io.Pipe()
	written := make(chan error, 1)
	go func() {
		_, err := io.WriteString(writer, jit)
		written <- writer.CloseWithError(err)
	}()
	pid, launchErr := adapter.runtime.Launch(ctx, LaunchSpec{
		Executable:   request.Executable,
		Directory:    request.Directory,
		Arguments:    append([]string(nil), request.Arguments...),
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
	case err := <-written:
		if err != nil {
			_ = reader.CloseWithError(err)
			return 0, err
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

func (adapter *Adapter) validContainment(ref runner.ContainmentRef) bool {
	return ref.Backend == containmentBackend &&
		ref.OwnerID != "" &&
		ref.Scope == jobObjectName(ref.HostEpoch, ref.OwnerID) &&
		canonicalToken(ref.HostEpoch) &&
		ref.InvocationID == "" &&
		canonicalToken(ref.FenceToken)
}

func containmentOwner(executionID string) string {
	sum := sha256.Sum256([]byte(executionID))
	return "tewake-" + hex.EncodeToString(sum[:])
}

func jobObjectName(epoch, owner string) string {
	return `Local\Tewake-` + epoch + "-" + owner
}

func canonicalToken(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') &&
			!(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func validWorkspaceName(name string) bool {
	if len(name) != 64 || filepath.Base(name) != name ||
		strings.ContainsAny(name, `/\`) {
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

var (
	_ runner.Supervisor       = (*Adapter)(nil)
	_ runner.Cleaner          = (*Adapter)(nil)
	_ runner.CompletionWaiter = (*Adapter)(nil)
	_ runner.CleanupFinalizer = (*Adapter)(nil)
)
