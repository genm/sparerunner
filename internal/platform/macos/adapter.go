// Package macos implements the macOS native-runner ownership boundary.
//
// A process group alone is not strong descendant ownership because a child may
// create another session. The production runtime therefore combines a fresh
// process group with one dedicated, non-login UID per runner slot and refuses
// admission while any process already exists under that UID.
package macos

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
	WorkspaceBackend   = "darwin-stat-v1"
	containmentBackend = "darwin-pgrp-uid-v1"

	defaultObservationTimeout = 5 * time.Second
)

type RunnerIdentity struct {
	UID int
	GID int
}

type IdentityResolver interface {
	ResolveRunnerIdentity(runner.ContainmentRef) (RunnerIdentity, error)
}

type StaticIdentity RunnerIdentity

func (identity StaticIdentity) ResolveRunnerIdentity(runner.ContainmentRef) (RunnerIdentity, error) {
	result := RunnerIdentity(identity)
	if result.UID <= 0 || result.GID <= 0 {
		return RunnerIdentity{}, runner.ErrStrongOwnershipUnavailable
	}
	return result, nil
}

type Config struct {
	Identity IdentityResolver
}

type ProcessGroup struct {
	Scope        string
	HostEpoch    string
	InvocationID string
}

type LaunchSpec struct {
	Executable   string
	Directory    string
	Arguments    []string
	Identity     RunnerIdentity
	WorkspaceRef runner.WorkspaceRef
	Containment  runner.ContainmentRef
}

type Fence interface {
	Revoked() (bool, error)
	Launched() (bool, error)
	// MarkLaunched is called while the child is blocked before receiving JIT.
	// A durable launched observation therefore always precedes runner exec.
	MarkLaunched(context.Context, int) error
	Revoke(context.Context) error
	Close() error
}

type Runtime interface {
	EnsureProcessGroup(context.Context, string) (ProcessGroup, error)
	LockFence(context.Context, runner.ContainmentRef) (Fence, error)
	Launch(
		context.Context,
		LaunchSpec,
		io.Reader,
		func(context.Context, int) error,
	) (int, error)
	KillAndWait(context.Context, runner.ContainmentRef) error
	WaitEmpty(context.Context, runner.ContainmentRef) error
	Alive(context.Context, runner.ContainmentRef, int) (bool, error)
	FinalizeFence(context.Context, runner.ContainmentRef) error
	GarbageCollectFence(context.Context, runner.ContainmentRef) error
}

type RuntimeAdmission interface {
	ValidateAdmission(context.Context) error
}

type Workspace interface {
	ValidateRuntimeRoot(context.Context, string) error
	Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error)
	Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error)
	Remove(context.Context, *os.Root, string) error
	Absent(context.Context, *os.Root, string) (bool, error)
	AgentIdentity() RunnerIdentity
	RunnerIdentity() RunnerIdentity
}

type Adapter struct {
	config    Config
	runtime   Runtime
	workspace Workspace

	processMu sync.Mutex
}

func New(config Config, nativeRuntime Runtime, workspace Workspace) (*Adapter, error) {
	if config.Identity == nil || nativeRuntime == nil || workspace == nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	agentIdentity := workspace.AgentIdentity()
	runnerIdentity := workspace.RunnerIdentity()
	if agentIdentity.UID < 0 || agentIdentity.GID < 0 ||
		runnerIdentity.UID <= 0 || runnerIdentity.GID <= 0 ||
		agentIdentity.UID == runnerIdentity.UID {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return &Adapter{
		config: config, runtime: nativeRuntime, workspace: workspace,
	}, nil
}

func (*Adapter) StrongDescendantOwnership() bool { return true }
func (*Adapter) StrongWorkspaceOwnership() bool  { return true }
func (*Adapter) WorkspaceBackend() string        { return WorkspaceBackend }

func (adapter *Adapter) ValidateRuntimeRoot(ctx context.Context, root string) error {
	if ctx == nil || ctx.Err() != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if admission, ok := adapter.runtime.(RuntimeAdmission); ok {
		if err := admission.ValidateAdmission(ctx); err != nil {
			return runner.ErrStrongOwnershipUnavailable
		}
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
	if ctx == nil || ctx.Err() != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
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
	if ctx == nil || ctx.Err() != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	ref, err := adapter.workspace.Observe(ctx, root, name)
	if err != nil || ref.Backend != WorkspaceBackend || ref.OwnerID == "" {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return ref, nil
}

func (adapter *Adapter) RemoveAndVerify(
	ctx context.Context,
	root *os.Root,
	name string,
) error {
	if ctx == nil || ctx.Err() != nil {
		return runner.ErrCleanupFailed
	}
	if err := adapter.workspace.Remove(ctx, root, name); err != nil {
		return runner.ErrCleanupFailed
	}
	absent, err := adapter.workspace.Absent(ctx, root, name)
	if err != nil || !absent {
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
	group, err := adapter.runtime.EnsureProcessGroup(ctx, owner)
	if err != nil || group.Scope != path.Join("tewake", owner) ||
		group.HostEpoch == "" || group.InvocationID != "" {
		return runner.ContainmentRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return runner.ContainmentRef{
		Backend:      containmentBackend,
		OwnerID:      owner,
		Scope:        group.Scope,
		HostEpoch:    group.HostEpoch,
		InvocationID: group.InvocationID,
	}, nil
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
	identity, err := adapter.config.Identity.ResolveRunnerIdentity(request.Containment)
	if err != nil || identity != adapter.workspace.RunnerIdentity() {
		return process, runner.ErrStrongOwnershipUnavailable
	}
	fence, err := adapter.runtime.LockFence(ctx, request.Containment)
	if err != nil {
		return process, runner.ErrStartFenced
	}
	defer fence.Close()
	revoked, revokedErr := fence.Revoked()
	launched, launchedErr := fence.Launched()
	if revokedErr != nil || launchedErr != nil || revoked || launched {
		return process, runner.ErrStartFenced
	}
	if err := request.VerifyWorkspaceAtExec(ctx); err != nil {
		return process, runner.ErrWorkspaceChanged
	}

	pid := 0
	launchAttempted := false
	err = request.DeliverJIT(func(jit string) error {
		launchAttempted = true
		reader, writer := io.Pipe()
		written := make(chan error, 1)
		go func() {
			_, writeErr := io.WriteString(writer, jit)
			written <- writer.CloseWithError(writeErr)
		}()
		startedPID, launchErr := adapter.runtime.Launch(
			ctx,
			LaunchSpec{
				Executable:   request.Executable,
				Directory:    request.Directory,
				Arguments:    append([]string(nil), request.Arguments...),
				Identity:     identity,
				WorkspaceRef: request.WorkspaceRef,
				Containment:  request.Containment,
			},
			reader,
			fence.MarkLaunched,
		)
		if launchErr != nil {
			_ = reader.CloseWithError(launchErr)
			_ = writer.CloseWithError(launchErr)
			<-written
			return launchErr
		}
		writeErr := <-written
		_ = reader.Close()
		if writeErr != nil || startedPID <= 0 {
			return runner.ErrStartFailed
		}
		pid = startedPID
		return nil
	})
	if err != nil {
		if launchAttempted {
			cleanupCtx := context.WithoutCancel(ctx)
			if stopErr := adapter.stop(cleanupCtx, request.Containment); stopErr != nil {
				return process, runner.ErrCleanupFailed
			}
		}
		if errors.Is(err, runner.ErrWorkspaceChanged) {
			return process, runner.ErrWorkspaceChanged
		}
		if errors.Is(err, runner.ErrStartFenced) {
			return process, runner.ErrStartFenced
		}
		return process, runner.ErrStartFailed
	}
	if pid <= 0 {
		return process, runner.ErrStartFailed
	}
	process.PID = pid
	return process, nil
}

func (adapter *Adapter) Stop(ctx context.Context, process runner.Process) error {
	if ctx == nil || ctx.Err() != nil || !adapter.validContainment(process.Containment) {
		return runner.ErrCleanupFailed
	}
	return adapter.stop(ctx, process.Containment)
}

func (adapter *Adapter) stop(ctx context.Context, containment runner.ContainmentRef) error {
	fence, err := adapter.runtime.LockFence(ctx, containment)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	if err := fence.Revoke(ctx); err != nil {
		_ = fence.Close()
		return runner.ErrCleanupFailed
	}
	if err := adapter.runtime.KillAndWait(ctx, containment); err != nil {
		_ = fence.Close()
		return runner.ErrCleanupFailed
	}
	if err := fence.Close(); err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), defaultObservationTimeout)
	defer cancel()
	alive, err := adapter.runtime.Alive(ctx, process.Containment, process.PID)
	if err != nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	return alive, nil
}

func (adapter *Adapter) Wait(ctx context.Context, process runner.Process) error {
	if ctx == nil || ctx.Err() != nil ||
		!adapter.validContainment(process.Containment) || process.PID <= 0 {
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
		process.Containment.OwnerID != "tewake-"+name ||
		expected.Backend != WorkspaceBackend || expected.OwnerID == "" {
		return runner.ErrCleanupFailed
	}
	observed, err := adapter.workspace.Observe(ctx, root, name)
	if err != nil || observed != expected {
		return runner.ErrCleanupFailed
	}
	if err := adapter.workspace.Remove(ctx, root, name); err != nil {
		return runner.ErrCleanupFailed
	}
	absent, err := adapter.workspace.Absent(ctx, root, name)
	if err != nil || !absent {
		return runner.ErrCleanupFailed
	}
	if err := adapter.runtime.FinalizeFence(ctx, process.Containment); err != nil {
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

func (adapter *Adapter) validContainment(ref runner.ContainmentRef) bool {
	return ref.Backend == containmentBackend &&
		ref.OwnerID != "" &&
		ref.Scope == path.Join("tewake", ref.OwnerID) &&
		ref.HostEpoch != "" &&
		ref.InvocationID == "" &&
		canonicalFenceToken(ref.FenceToken)
}

func containmentOwner(executionID string) string {
	sum := sha256.Sum256([]byte(executionID))
	return "tewake-" + hex.EncodeToString(sum[:])
}

func canonicalFenceToken(value string) bool {
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

var (
	_ runner.Supervisor       = (*Adapter)(nil)
	_ runner.Cleaner          = (*Adapter)(nil)
	_ runner.CompletionWaiter = (*Adapter)(nil)
	_ runner.CleanupFinalizer = (*Adapter)(nil)
)
