package macos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/genm/tewake/internal/runner"
)

type testWorkspace struct {
	removeErr error
	removed   bool
}

func (*testWorkspace) ValidateRuntimeRoot(context.Context, string) error { return nil }
func (*testWorkspace) Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-1"}, nil
}
func (*testWorkspace) Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-1"}, nil
}
func (workspace *testWorkspace) Remove(context.Context, *os.Root, string) error {
	if workspace.removeErr != nil {
		return workspace.removeErr
	}
	workspace.removed = true
	return nil
}
func (*testWorkspace) Absent(context.Context, *os.Root, string) (bool, error) {
	return true, nil
}
func (*testWorkspace) AgentIdentity() RunnerIdentity {
	return RunnerIdentity{UID: 2001, GID: 2001}
}
func (*testWorkspace) RunnerIdentity() RunnerIdentity {
	return RunnerIdentity{UID: 1001, GID: 1001}
}

type testFence struct {
	mu       sync.Mutex
	revoked  bool
	launched bool
}

func (fence *testFence) Revoked() (bool, error) {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	return fence.revoked, nil
}
func (fence *testFence) Launched() (bool, error) {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	return fence.launched, nil
}
func (fence *testFence) MarkLaunched(context.Context, int) error {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	if fence.revoked || fence.launched {
		return runner.ErrStartFenced
	}
	fence.launched = true
	return nil
}
func (fence *testFence) Revoke(context.Context) error {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	fence.revoked = true
	return nil
}
func (*testFence) Close() error { return nil }

type testRuntime struct {
	mu        sync.Mutex
	fences    map[string]*testFence
	launches  int
	kills     int
	jit       string
	revokeNew bool
	alive     bool
	waitErr   error
}

func newTestRuntime() *testRuntime {
	return &testRuntime{fences: make(map[string]*testFence), alive: true}
}

func (*testRuntime) EnsureProcessGroup(_ context.Context, owner string) (ProcessGroup, error) {
	return ProcessGroup{Scope: "tewake/" + owner, HostEpoch: "boot-test"}, nil
}
func (runtime *testRuntime) LockFence(_ context.Context, containment runner.ContainmentRef) (Fence, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	key := containment.OwnerID + ":" + containment.FenceToken
	if runtime.fences[key] == nil {
		runtime.fences[key] = &testFence{revoked: runtime.revokeNew}
	}
	return runtime.fences[key], nil
}
func (runtime *testRuntime) Launch(
	ctx context.Context,
	_ LaunchSpec,
	material io.Reader,
	admit func(context.Context, int) error,
) (int, error) {
	runtime.mu.Lock()
	runtime.launches++
	pid := 1000 + runtime.launches
	runtime.mu.Unlock()
	if err := admit(ctx, pid); err != nil {
		return 0, err
	}
	value, err := io.ReadAll(material)
	if err != nil {
		return 0, err
	}
	runtime.mu.Lock()
	runtime.jit = string(value)
	runtime.mu.Unlock()
	return pid, nil
}
func (runtime *testRuntime) KillAndWait(context.Context, runner.ContainmentRef) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	for _, fence := range runtime.fences {
		fence.mu.Lock()
		revoked := fence.revoked
		fence.mu.Unlock()
		if !revoked {
			return errors.New("kill preceded durable fence revocation")
		}
	}
	runtime.kills++
	runtime.alive = false
	return nil
}
func (runtime *testRuntime) WaitEmpty(context.Context, runner.ContainmentRef) error {
	return runtime.waitErr
}
func (runtime *testRuntime) Alive(context.Context, runner.ContainmentRef, int) (bool, error) {
	return runtime.alive, nil
}
func (*testRuntime) FinalizeFence(context.Context, runner.ContainmentRef) error {
	return nil
}
func (*testRuntime) GarbageCollectFence(context.Context, runner.ContainmentRef) error {
	return nil
}

type testPreparedPackage struct{ source string }

func (prepared testPreparedPackage) Materialize(destination *os.Root) error {
	data, err := os.ReadFile(filepath.Join(prepared.source, "run.sh"))
	if err != nil {
		return err
	}
	return destination.WriteFile("run.sh", data, 0o700)
}
func (testPreparedPackage) Close() error { return nil }

type testCache struct{ source string }

func (cache testCache) Ensure(context.Context, runner.Package) (runner.PreparedPackage, error) {
	return testPreparedPackage{source: cache.source}, nil
}

type testJIT struct{ value string }

func (jit testJIT) Digest() string {
	sum := sha256.Sum256([]byte(jit.value))
	return hex.EncodeToString(sum[:])
}
func (jit testJIT) Deliver(deliver func(string) error) error { return deliver(jit.value) }

func newAdapter(t *testing.T, runtime Runtime, workspace Workspace) *Adapter {
	t.Helper()
	adapter, err := New(Config{
		Identity: StaticIdentity{UID: 1001, GID: 1001},
	}, runtime, workspace)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func newManager(t *testing.T, adapter *Adapter) *runner.Manager {
	return newManagerWithJournal(t, adapter, runner.NewMemoryJournal())
}

func newManagerWithJournal(
	t *testing.T,
	adapter *Adapter,
	journal runner.Journal,
) *runner.Manager {
	t.Helper()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "run.sh"), []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	manager, err := runner.NewManager(runner.Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{source: source},
		Journal:     journal,
		Supervisor:  adapter,
		Cleaner:     adapter,
	})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func TestAgentRestartAdoptsRunningSlotWithoutSecondListener(t *testing.T) {
	nativeRuntime := newTestRuntime()
	workspace := &testWorkspace{}
	adapter := newAdapter(t, nativeRuntime, workspace)
	journal := runner.NewMemoryJournal()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "run.sh"), []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	newRuntimeManager := func() *runner.Manager {
		manager, err := runner.NewManager(runner.Options{
			RuntimeRoot: runtimeRoot,
			Cache:       testCache{source: source},
			Journal:     journal,
			Supervisor:  adapter,
			Cleaner:     adapter,
		})
		if err != nil {
			t.Fatal(err)
		}
		return manager
	}
	first := newRuntimeManager()
	preparation := runner.Preparation{
		ExecutionID: "macos-restart-adoption",
		Package:     currentPackage(t),
	}
	if _, err := first.EnsureRunning(context.Background(), runner.Start{
		Preparation: preparation,
		JIT:         testJIT{value: "restart-adoption.example.test"},
	}); err != nil {
		t.Fatal(err)
	}
	restarted := newRuntimeManager()
	state, err := restarted.Recover(context.Background(), preparation.ExecutionID)
	if err != nil || !state.Running {
		t.Fatalf("Recover=%#v err=%v", state, err)
	}
	nativeRuntime.mu.Lock()
	defer nativeRuntime.mu.Unlock()
	if nativeRuntime.launches != 1 {
		t.Fatalf("runner listeners=%d", nativeRuntime.launches)
	}
}

func currentPackage(t *testing.T) runner.Package {
	t.Helper()
	pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

func TestAdapterRunsOneJITThenRevokesBeforeProcessCleanup(t *testing.T) {
	nativeRuntime := newTestRuntime()
	workspace := &testWorkspace{}
	adapter := newAdapter(t, nativeRuntime, workspace)
	manager := newManager(t, adapter)
	preparation := runner.Preparation{
		ExecutionID: "macos-one-job",
		Package:     currentPackage(t),
	}
	state, err := manager.EnsureRunning(context.Background(), runner.Start{
		Preparation: preparation,
		JIT:         testJIT{value: "jit-macos.example.test"},
	})
	if err != nil || !state.Running {
		t.Fatalf("EnsureRunning=%#v err=%v", state, err)
	}
	state, err = manager.Destroy(context.Background(), preparation.ExecutionID)
	if err != nil || state.State != runner.StateReleased || !workspace.removed {
		t.Fatalf("Destroy=%#v removed=%v err=%v", state, workspace.removed, err)
	}
	nativeRuntime.mu.Lock()
	defer nativeRuntime.mu.Unlock()
	if nativeRuntime.launches != 1 || nativeRuntime.kills != 1 ||
		nativeRuntime.jit != "jit-macos.example.test" {
		t.Fatalf(
			"launches=%d kills=%d jit=%q",
			nativeRuntime.launches,
			nativeRuntime.kills,
			nativeRuntime.jit,
		)
	}
}

func TestAdapterFencedReplayNeverDeliversJIT(t *testing.T) {
	nativeRuntime := newTestRuntime()
	nativeRuntime.revokeNew = true
	adapter := newAdapter(t, nativeRuntime, &testWorkspace{})
	manager := newManager(t, adapter)
	preparation := runner.Preparation{
		ExecutionID: "macos-fenced-replay",
		Package:     currentPackage(t),
	}
	if _, err := manager.EnsurePrepared(context.Background(), preparation); err != nil {
		t.Fatal(err)
	}
	state, err := manager.EnsureRunning(context.Background(), runner.Start{
		Preparation: preparation,
		JIT:         testJIT{value: "must-not-deliver.example.test"},
	})
	if !errors.Is(err, runner.ErrReconciliationRequired) || state.Running {
		t.Fatalf("EnsureRunning=%#v err=%v", state, err)
	}
	nativeRuntime.mu.Lock()
	defer nativeRuntime.mu.Unlock()
	if nativeRuntime.launches != 0 || nativeRuntime.jit != "" {
		t.Fatalf("launches=%d jit=%q", nativeRuntime.launches, nativeRuntime.jit)
	}
}

func TestCleanupFailureQuarantinesInsteadOfReleasingSlot(t *testing.T) {
	nativeRuntime := newTestRuntime()
	workspace := &testWorkspace{removeErr: errors.New("disk cleanup failed")}
	adapter := newAdapter(t, nativeRuntime, workspace)
	manager := newManager(t, adapter)
	preparation := runner.Preparation{
		ExecutionID: "macos-cleanup-failure",
		Package:     currentPackage(t),
	}
	if _, err := manager.EnsureRunning(context.Background(), runner.Start{
		Preparation: preparation,
		JIT:         testJIT{value: "cleanup-failure.example.test"},
	}); err != nil {
		t.Fatal(err)
	}
	state, err := manager.Destroy(context.Background(), preparation.ExecutionID)
	if !errors.Is(err, runner.ErrQuarantined) || !state.Quarantined ||
		state.State != runner.StateCleanupFailed {
		t.Fatalf("Destroy=%#v err=%v", state, err)
	}
}

func TestAdapterRejectsSharedAgentAndRunnerIdentity(t *testing.T) {
	workspace := &testWorkspaceWithIdentity{
		agent:  RunnerIdentity{UID: 1001, GID: 1001},
		runner: RunnerIdentity{UID: 1001, GID: 1001},
	}
	_, err := New(
		Config{Identity: StaticIdentity{UID: 1001, GID: 1001}},
		newTestRuntime(),
		workspace,
	)
	if !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("New error=%v", err)
	}
}

type testWorkspaceWithIdentity struct {
	testWorkspace
	agent  RunnerIdentity
	runner RunnerIdentity
}

func (workspace *testWorkspaceWithIdentity) AgentIdentity() RunnerIdentity {
	return workspace.agent
}
func (workspace *testWorkspaceWithIdentity) RunnerIdentity() RunnerIdentity {
	return workspace.runner
}
