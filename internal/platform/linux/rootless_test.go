package linux

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/genm/sparerunner/internal/runner"
)

// sharedTestWorkspace is the shape this mode requires: one credential used by
// both the Agent and the job, encoded under SharedWorkspaceBackend.
type sharedTestWorkspace struct {
	identity RunnerIdentity
	backend  string
}

func newSharedTestWorkspace() sharedTestWorkspace {
	return sharedTestWorkspace{
		identity: RunnerIdentity{UID: 1001, GID: 1001},
		backend:  SharedWorkspaceBackend,
	}
}

func (sharedTestWorkspace) ValidateRuntimeRoot(context.Context, string) error { return nil }
func (workspace sharedTestWorkspace) Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{Backend: workspace.backend, OwnerID: "shared-workspace-1"}, nil
}
func (workspace sharedTestWorkspace) Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{Backend: workspace.backend, OwnerID: "shared-workspace-1"}, nil
}
func (sharedTestWorkspace) Remove(context.Context, *os.Root, string) error { return nil }
func (workspace sharedTestWorkspace) AgentIdentity() RunnerIdentity {
	return workspace.identity
}
func (workspace sharedTestWorkspace) RunnerIdentity() RunnerIdentity {
	return workspace.identity
}

func newRootlessAdapter(t *testing.T, nativeRuntime Runtime, workspace Workspace) *RootlessAdapter {
	t.Helper()
	adapter, err := NewRootless(nativeRuntime, workspace, 0)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func newRootlessManager(t *testing.T, adapter *RootlessAdapter) *runner.Manager {
	t.Helper()
	source := t.TempDir()
	if err := os.WriteFile(source+"/run.sh", []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	manager, err := runner.NewManager(runner.Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{source: source},
		Journal:     runner.NewMemoryJournal(),
		Supervisor:  adapter,
		Cleaner:     adapter,
	})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

// The privileged adapter's whole claim rests on the Agent and the job being
// different accounts. If a caller hands this adapter that workspace, it is a
// wiring mistake, and this adapter must not make ownership claims it cannot
// enforce.
func TestNewRootlessRefusesDistinctAgentAndRunnerIdentities(t *testing.T) {
	if _, err := NewRootless(newTestRuntime(), testWorkspace{}, 0); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("NewRootless error = %v", err)
	}
}

func TestNewRootlessRefusesRootIdentity(t *testing.T) {
	workspace := newSharedTestWorkspace()
	workspace.identity = RunnerIdentity{UID: 0, GID: 0}
	if _, err := NewRootless(newTestRuntime(), workspace, 0); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("NewRootless error = %v", err)
	}
}

func TestNewRootlessRefusesRuntimeWithoutCleanupFinalizer(t *testing.T) {
	runtimeWithoutFinalizer := struct{ Runtime }{Runtime: newTestRuntime()}
	if _, err := NewRootless(runtimeWithoutFinalizer, newSharedTestWorkspace(), 0); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("NewRootless error = %v", err)
	}
}

// The two modes must be unable to observe each other's workspaces even inside
// one process, because their refs record different owning credentials.
func TestRootlessWorkspaceBackendIsDistinctFromPrivileged(t *testing.T) {
	if SharedWorkspaceBackend == WorkspaceBackend {
		t.Fatal("shared-identity mode must not reuse the privileged workspace backend")
	}
	adapter := newRootlessAdapter(t, newTestRuntime(), newSharedTestWorkspace())
	if adapter.WorkspaceBackend() != SharedWorkspaceBackend {
		t.Fatalf("WorkspaceBackend = %q", adapter.WorkspaceBackend())
	}
	privileged := newAdapter(t, newTestRuntime())
	if privileged.WorkspaceBackend() == adapter.WorkspaceBackend() {
		t.Fatal("the two adapters must not share a workspace backend")
	}
}

func TestRootlessAdapterRefusesPrivilegedWorkspaceRef(t *testing.T) {
	workspace := newSharedTestWorkspace()
	workspace.backend = WorkspaceBackend
	adapter := newRootlessAdapter(t, newTestRuntime(), workspace)
	if _, err := adapter.PrepareWorkspace(context.Background(), nil, "name"); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("PrepareWorkspace error = %v", err)
	}
	if _, err := adapter.WorkspaceRef(context.Background(), nil, "name"); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("WorkspaceRef error = %v", err)
	}
}

func TestRootlessAdapterPipesJITOnlyAfterVerifiedStart(t *testing.T) {
	nativeRuntime := newTestRuntime()
	adapter := newRootlessAdapter(t, nativeRuntime, newSharedTestWorkspace())
	manager := newRootlessManager(t, adapter)
	request := runner.Preparation{ExecutionID: "rootless-pipe-start", Package: currentPackage(t)}
	state, err := manager.EnsureRunning(context.Background(), runner.Start{
		Preparation: request,
		JIT:         testJIT{value: "shared.jit.example.test"},
	})
	if err != nil || !state.Running {
		t.Fatalf("EnsureRunning = %#v, %v", state, err)
	}
	nativeRuntime.mu.Lock()
	defer nativeRuntime.mu.Unlock()
	if nativeRuntime.launches != 1 || nativeRuntime.jit != "shared.jit.example.test" {
		t.Fatalf("launches=%d jit=%q", nativeRuntime.launches, nativeRuntime.jit)
	}
}

// A revoked fence must stop the start before any one-job credential is
// delivered. This is identical to the privileged contract and is the reason the
// fence is taken before VerifyWorkspaceAtExec.
func TestRootlessAdapterFencedStartDoesNotReceiveJIT(t *testing.T) {
	nativeRuntime := newTestRuntime()
	nativeRuntime.revokeNew = true
	adapter := newRootlessAdapter(t, nativeRuntime, newSharedTestWorkspace())
	manager := newRootlessManager(t, adapter)
	request := runner.Preparation{ExecutionID: "rootless-fenced-start", Package: currentPackage(t)}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	_, err := manager.EnsureRunning(context.Background(), runner.Start{
		Preparation: request,
		JIT:         testJIT{value: "must-not-deliver.example.test"},
	})
	if !errors.Is(err, runner.ErrReconciliationRequired) {
		t.Fatalf("EnsureRunning error = %v", err)
	}
	nativeRuntime.mu.Lock()
	defer nativeRuntime.mu.Unlock()
	if nativeRuntime.launches != 0 || nativeRuntime.jit != "" {
		t.Fatalf("launches=%d jit=%q", nativeRuntime.launches, nativeRuntime.jit)
	}
}

// changedWorkspace observes a different identity than it prepared, which is
// exactly what a swapped workspace directory looks like at the exec boundary.
type changedWorkspace struct {
	sharedTestWorkspace
	observations int
}

func (workspace *changedWorkspace) Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	workspace.observations++
	if workspace.observations <= 1 {
		return runner.WorkspaceRef{Backend: SharedWorkspaceBackend, OwnerID: "shared-workspace-1"}, nil
	}
	return runner.WorkspaceRef{Backend: SharedWorkspaceBackend, OwnerID: "shared-workspace-2"}, nil
}

func TestRootlessAdapterRefusesChangedWorkspaceWithoutDeliveringJIT(t *testing.T) {
	nativeRuntime := newTestRuntime()
	workspace := &changedWorkspace{sharedTestWorkspace: newSharedTestWorkspace()}
	adapter := newRootlessAdapter(t, nativeRuntime, workspace)
	manager := newRootlessManager(t, adapter)
	request := runner.Preparation{ExecutionID: "rootless-changed-workspace", Package: currentPackage(t)}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.EnsureRunning(context.Background(), runner.Start{
		Preparation: request,
		JIT:         testJIT{value: "must-not-deliver.example.test"},
	}); err == nil {
		t.Fatal("EnsureRunning must refuse a changed workspace")
	}
	nativeRuntime.mu.Lock()
	defer nativeRuntime.mu.Unlock()
	if nativeRuntime.launches != 0 || nativeRuntime.jit != "" {
		t.Fatalf("launches=%d jit=%q", nativeRuntime.launches, nativeRuntime.jit)
	}
}

// testRuntime.KillAndWait fails if any fence is still unrevoked, so a passing
// Stop proves the durable revoke happens before cgroup.kill.
func TestRootlessAdapterRevokesFenceBeforeCgroupKill(t *testing.T) {
	nativeRuntime := newTestRuntime()
	adapter := newRootlessAdapter(t, nativeRuntime, newSharedTestWorkspace())
	manager := newRootlessManager(t, adapter)
	request := runner.Preparation{ExecutionID: "rootless-stop-order", Package: currentPackage(t)}
	if _, err := manager.EnsureRunning(context.Background(), runner.Start{
		Preparation: request,
		JIT:         testJIT{value: "shared.jit.example.test"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Destroy(context.Background(), request.ExecutionID); err != nil {
		t.Fatalf("Destroy error = %v", err)
	}
	nativeRuntime.mu.Lock()
	defer nativeRuntime.mu.Unlock()
	if nativeRuntime.kills == 0 {
		t.Fatal("Destroy must empty the containment")
	}
}

func TestRootlessAdapterRejectsUnfencedContainment(t *testing.T) {
	adapter := newRootlessAdapter(t, newTestRuntime(), newSharedTestWorkspace())
	unfenced := runner.ContainmentRef{
		Backend:   containmentBackend,
		OwnerID:   "sparerunner-x",
		Scope:     "sparerunner/sparerunner-x",
		HostEpoch: "boot-test",
	}
	if _, err := adapter.Start(context.Background(), runner.StartRequest{Containment: unfenced}); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("Start error = %v", err)
	}
	if err := adapter.Stop(context.Background(), runner.Process{PID: 1, Containment: unfenced}); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("Stop error = %v", err)
	}
}
