package linux

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
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

type testWorkspace struct{}

func (testWorkspace) ValidateRuntimeRoot(context.Context, string) error { return nil }
func (testWorkspace) Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-1"}, nil
}
func (testWorkspace) Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-1"}, nil
}
func (testWorkspace) Remove(context.Context, *os.Root, string) error { return nil }
func (testWorkspace) AgentIdentity() RunnerIdentity {
	return RunnerIdentity{UID: 2001, GID: 2001}
}
func (testWorkspace) RunnerIdentity() RunnerIdentity {
	return RunnerIdentity{UID: 1001, GID: 1001}
}

type testFence struct {
	mu       sync.Mutex
	revoked  bool
	launched bool
	closeErr bool
}

func (fence *testFence) Revoked() (bool, error) {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	return fence.revoked, nil
}
func (fence *testFence) Revoke(context.Context) error {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	fence.revoked = true
	return nil
}
func (fence *testFence) Launched() (bool, error) {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	return fence.launched, nil
}
func (fence *testFence) MarkLaunched(context.Context) error {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	if fence.revoked || fence.launched {
		return runner.ErrCleanupFailed
	}
	fence.launched = true
	return nil
}
func (fence *testFence) Close() error {
	if fence.closeErr {
		return errors.New("fence close failed")
	}
	return nil
}

type testRuntime struct {
	mu        sync.Mutex
	fences    map[string]*testFence
	launches  int
	kills     int
	jit       string
	revokeNew bool
	closeErr  bool
}

func newTestRuntime() *testRuntime { return &testRuntime{fences: make(map[string]*testFence)} }

func (*testRuntime) EnsureCgroup(_ context.Context, owner string) (Cgroup, error) {
	return Cgroup{Scope: "sparerunner/" + owner, HostEpoch: "boot-test"}, nil
}
func (runtime *testRuntime) LockFence(_ context.Context, containment runner.ContainmentRef) (Fence, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	key := containment.OwnerID + ":" + containment.FenceToken
	if runtime.fences[key] == nil {
		runtime.fences[key] = &testFence{revoked: runtime.revokeNew, closeErr: runtime.closeErr}
	}
	return runtime.fences[key], nil
}
func (runtime *testRuntime) Launch(_ context.Context, _ LaunchSpec, material io.Reader) (int, error) {
	value, err := io.ReadAll(material)
	if err != nil {
		return 0, err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.launches++
	runtime.jit = string(value)
	return 1000 + runtime.launches, nil
}
func (runtime *testRuntime) KillAndWait(context.Context, runner.ContainmentRef) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	for _, fence := range runtime.fences {
		fence.mu.Lock()
		revoked := fence.revoked
		fence.mu.Unlock()
		if !revoked {
			return errors.New("cgroup kill preceded durable fence revocation")
		}
	}
	runtime.kills++
	return nil
}
func (*testRuntime) WaitEmpty(context.Context, runner.ContainmentRef) error {
	return nil
}
func (*testRuntime) Alive(context.Context, runner.ContainmentRef, int) (bool, error) {
	return true, nil
}
func (*testRuntime) FinalizeCleanup(
	ctx context.Context,
	_ runner.ContainmentRef,
	root *os.Root,
	name string,
	_ runner.WorkspaceRef,
) error {
	if err := ctx.Err(); err != nil || root == nil {
		return runner.ErrCleanupFailed
	}
	if err := root.RemoveAll(name); err != nil {
		return runner.ErrCleanupFailed
	}
	if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		return runner.ErrCleanupFailed
	}
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

func newManager(t *testing.T, adapter *Adapter) *runner.Manager {
	t.Helper()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "run.sh"), []byte("#!/bin/sh\n"), 0o700); err != nil {
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

func newAdapter(t *testing.T, runtime Runtime) *Adapter {
	t.Helper()
	adapter, err := New(Config{
		Identity: StaticIdentity{UID: 1001, GID: 1001},
	}, runtime, testWorkspace{})
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func TestAdapterRejectsRootRunnerIdentityAtAdmission(t *testing.T) {
	workspace := testWorkspaceWithIdentity{identity: RunnerIdentity{UID: 0, GID: 0}}
	if _, err := New(Config{Identity: StaticIdentity{UID: 0, GID: 0}}, newTestRuntime(), workspace); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("New error=%v", err)
	}
}

func TestAdapterRejectsRuntimeWithoutCleanupFinalizer(t *testing.T) {
	runtimeWithoutFinalizer := struct{ Runtime }{Runtime: newTestRuntime()}
	if _, err := New(
		Config{Identity: StaticIdentity{UID: 1001, GID: 1001}},
		runtimeWithoutFinalizer,
		testWorkspace{},
	); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("New error=%v", err)
	}
}

type testWorkspaceWithIdentity struct {
	testWorkspace
	identity RunnerIdentity
}

func (workspace testWorkspaceWithIdentity) RunnerIdentity() RunnerIdentity {
	return workspace.identity
}

func currentPackage(t *testing.T) runner.Package {
	t.Helper()
	pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

func TestAdapterPipesJITOnlyAfterVerifiedStart(t *testing.T) {
	runtime := newTestRuntime()
	adapter := newAdapter(t, runtime)
	manager := newManager(t, adapter)
	request := runner.Preparation{ExecutionID: "linux-pipe-start", Package: currentPackage(t)}
	state, err := manager.EnsureRunning(context.Background(), runner.Start{Preparation: request, JIT: testJIT{value: "jit.example.test"}})
	if err != nil || !state.Running {
		t.Fatalf("EnsureRunning = %#v, %v", state, err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.launches != 1 || runtime.jit != "jit.example.test" {
		t.Fatalf("launches=%d jit=%q", runtime.launches, runtime.jit)
	}
}

func TestAdapterFencedStartDoesNotReceiveJIT(t *testing.T) {
	runtime := newTestRuntime()
	runtime.revokeNew = true
	adapter := newAdapter(t, runtime)
	manager := newManager(t, adapter)
	request := runner.Preparation{ExecutionID: "linux-fenced-start", Package: currentPackage(t)}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	_, err := manager.EnsureRunning(context.Background(), runner.Start{Preparation: request, JIT: testJIT{value: "must-not-deliver.example.test"}})
	if !errors.Is(err, runner.ErrReconciliationRequired) {
		t.Fatalf("EnsureRunning error = %v", err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.launches != 0 || runtime.jit != "" {
		t.Fatalf("launches=%d jit=%q", runtime.launches, runtime.jit)
	}
}

func TestAdapterRejectsIdentityOutsideWorkspaceSlot(t *testing.T) {
	runtime := newTestRuntime()
	adapter, err := New(Config{
		Identity: StaticIdentity{UID: 1002, GID: 1002},
	}, runtime, testWorkspace{})
	if err != nil {
		t.Fatal(err)
	}
	manager := newManager(t, adapter)
	request := runner.Preparation{ExecutionID: "linux-slot-identity", Package: currentPackage(t)}
	_, err = manager.EnsureRunning(context.Background(), runner.Start{Preparation: request, JIT: testJIT{value: "must-not-deliver.example.test"}})
	if !errors.Is(err, runner.ErrStartFailed) {
		t.Fatalf("EnsureRunning error = %v", err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.launches != 0 || runtime.jit != "" {
		t.Fatalf("launches=%d jit=%q", runtime.launches, runtime.jit)
	}
}

func TestAdapterRevokesFenceBeforeCgroupKill(t *testing.T) {
	runtime := newTestRuntime()
	adapter := newAdapter(t, runtime)
	manager := newManager(t, adapter)
	request := runner.Preparation{ExecutionID: "linux-stop-order", Package: currentPackage(t)}
	if _, err := manager.EnsureRunning(context.Background(), runner.Start{Preparation: request, JIT: testJIT{value: "jit.example.test"}}); err != nil {
		t.Fatal(err)
	}
	state, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || state.State != runner.StateReleased {
		t.Fatalf("Destroy = %#v, %v", state, err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.kills != 1 {
		t.Fatalf("cgroup kills = %d", runtime.kills)
	}
}

func TestAdapterNeverClaimsRunningWhenFenceSessionCloseFails(t *testing.T) {
	runtime := newTestRuntime()
	runtime.closeErr = true
	adapter := newAdapter(t, runtime)
	manager := newManager(t, adapter)
	request := runner.Preparation{ExecutionID: "linux-close-failure", Package: currentPackage(t)}
	state, err := manager.EnsureRunning(context.Background(), runner.Start{
		Preparation: request,
		JIT:         testJIT{value: "jit.example.test"},
	})
	if !errors.Is(err, runner.ErrQuarantined) || state.Running || !state.Quarantined {
		t.Fatalf("EnsureRunning=%#v err=%v", state, err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.kills == 0 {
		t.Fatal("fence close failure did not trigger containment cleanup")
	}
}

type blockedRuntime struct{ *testRuntime }

func (runtime *blockedRuntime) Launch(ctx context.Context, _ LaunchSpec, _ io.Reader) (int, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

type blockedLaunchAndCleanupRuntime struct {
	*testRuntime
	cleanupCanceled chan struct{}
	cleanupOnce     sync.Once
	launchEntered   chan struct{}
	launchOnce      sync.Once
}

func (runtime *blockedLaunchAndCleanupRuntime) Launch(ctx context.Context, _ LaunchSpec, _ io.Reader) (int, error) {
	runtime.launchOnce.Do(func() { close(runtime.launchEntered) })
	<-ctx.Done()
	return 0, ctx.Err()
}

func (runtime *blockedLaunchAndCleanupRuntime) KillAndWait(ctx context.Context, _ runner.ContainmentRef) error {
	<-ctx.Done()
	runtime.cleanupOnce.Do(func() { close(runtime.cleanupCanceled) })
	return ctx.Err()
}

func TestAdapterCancelsBlockedAnonymousPipeLaunch(t *testing.T) {
	runtime := &blockedRuntime{testRuntime: newTestRuntime()}
	adapter := newAdapter(t, runtime)
	manager := newManager(t, adapter)
	request := runner.Preparation{ExecutionID: "linux-cancelled-pipe", Package: currentPackage(t)}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := manager.EnsureRunning(ctx, runner.Start{Preparation: request, JIT: testJIT{value: "jit.example.test"}})
	// The core receives the already-cancelled context for its recovery Stop and
	// therefore quarantines rather than claiming a durable rollback it cannot
	// journal. Adapter.Start still killed its cgroup before returning.
	if !errors.Is(err, runner.ErrQuarantined) {
		t.Fatalf("EnsureRunning error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cancelled launch took %s", elapsed)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	// Start performs the first containment kill before returning; the core then
	// issues its idempotent Stop. At least one verified kill is required here.
	if runtime.launches != 0 || runtime.kills < 1 {
		t.Fatalf("launches=%d kills=%d", runtime.launches, runtime.kills)
	}
}

func TestAdapterBoundsAmbiguousStartCleanupAndLeavesFenceRevoked(t *testing.T) {
	nativeRuntime := &blockedLaunchAndCleanupRuntime{
		testRuntime:     newTestRuntime(),
		cleanupCanceled: make(chan struct{}),
		launchEntered:   make(chan struct{}),
	}
	adapter, err := New(Config{
		Identity:                StaticIdentity{UID: 1001, GID: 1001},
		AmbiguousCleanupTimeout: 20 * time.Millisecond,
	}, nativeRuntime, testWorkspace{})
	if err != nil {
		t.Fatal(err)
	}
	manager := newManager(t, adapter)
	request := runner.Preparation{ExecutionID: "linux-bounded-cleanup", Package: currentPackage(t)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-nativeRuntime.launchEntered:
			cancel()
		case <-ctx.Done():
		}
	}()
	started := time.Now()
	state, err := manager.EnsureRunning(ctx, runner.Start{
		Preparation: request,
		JIT:         testJIT{value: "bounded-cleanup.example.test"},
	})
	if !errors.Is(err, runner.ErrQuarantined) || !state.Quarantined {
		t.Fatalf("EnsureRunning=%#v err=%v", state, err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("bounded cleanup took %s", elapsed)
	}
	select {
	case <-nativeRuntime.cleanupCanceled:
	default:
		t.Fatal("ambiguous cleanup did not reach its deadline")
	}
	nativeRuntime.mu.Lock()
	defer nativeRuntime.mu.Unlock()
	for _, fence := range nativeRuntime.fences {
		fence.mu.Lock()
		revoked := fence.revoked
		fence.mu.Unlock()
		if !revoked {
			t.Fatal("timed out cleanup released an unrevoked start fence")
		}
	}
}

func TestAdapterWaitObservesWholeContainmentAndHonorsCallerContext(t *testing.T) {
	runtime := &waitingRuntime{testRuntime: newTestRuntime(), entered: make(chan struct{})}
	adapter := newAdapter(t, runtime)
	containment, err := adapter.PrepareContainment(context.Background(), "linux-wait")
	if err != nil {
		t.Fatal(err)
	}
	containment.FenceToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- adapter.Wait(ctx, runner.Process{PID: 1234, Containment: containment})
	}()
	<-runtime.entered
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
			t.Fatalf("Wait error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait ignored caller cancellation")
	}
}

type waitingRuntime struct {
	*testRuntime
	entered chan struct{}
	once    sync.Once
}

func (runtime *waitingRuntime) WaitEmpty(ctx context.Context, _ runner.ContainmentRef) error {
	runtime.once.Do(func() { close(runtime.entered) })
	<-ctx.Done()
	return ctx.Err()
}

type stalledObservationRuntime struct {
	*testRuntime
	entered chan struct{}
	once    sync.Once
}

func (runtime *stalledObservationRuntime) Alive(
	ctx context.Context,
	_ runner.ContainmentRef,
	_ int,
) (bool, error) {
	runtime.once.Do(func() { close(runtime.entered) })
	<-ctx.Done()
	return false, ctx.Err()
}

func TestAdapterAliveBoundsStalledHelperObservation(t *testing.T) {
	runtime := &stalledObservationRuntime{testRuntime: newTestRuntime(), entered: make(chan struct{})}
	adapter := newAdapter(t, runtime)
	adapter.cleanup = 20 * time.Millisecond
	containment, err := adapter.PrepareContainment(context.Background(), "linux-stalled-alive")
	if err != nil {
		t.Fatal(err)
	}
	containment.FenceToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	started := time.Now()
	alive, err := adapter.Alive(runner.Process{PID: 1234, Containment: containment})
	if alive || !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("Alive=%v err=%v", alive, err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("stalled Alive exceeded its bound: %s", elapsed)
	}
	select {
	case <-runtime.entered:
	default:
		t.Fatal("runtime observation was not attempted")
	}
}
