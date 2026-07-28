package windows

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

	"github.com/genm/sparerunner/internal/runner"
)

type testWorkspace struct {
	removeErr error
	ref       runner.WorkspaceRef
}

func (workspace *testWorkspace) ValidateRuntimeRoot(context.Context, string) error { return nil }
func (workspace *testWorkspace) Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return workspace.ref, nil
}
func (workspace *testWorkspace) Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return workspace.ref, nil
}
func (workspace *testWorkspace) Remove(_ context.Context, root *os.Root, name string) error {
	if workspace.removeErr != nil {
		return workspace.removeErr
	}
	if err := root.RemoveAll(name); err != nil {
		return err
	}
	if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		return runner.ErrCleanupFailed
	}
	return nil
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
func (fence *testFence) Revoke(context.Context) error {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	fence.revoked = true
	return nil
}
func (fence *testFence) MarkLaunched(context.Context) error {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	if fence.revoked || fence.launched {
		return runner.ErrStartFenced
	}
	fence.launched = true
	return nil
}
func (fence *testFence) Close() error { return nil }

type testRuntime struct {
	mu       sync.Mutex
	fences   map[string]*testFence
	jit      string
	launches int
	kills    int
}

func newTestRuntime() *testRuntime {
	return &testRuntime{fences: make(map[string]*testFence)}
}

func (*testRuntime) ValidateAdmission(context.Context) error { return nil }
func (*testRuntime) HostEpoch() string                       { return "0123456789abcdef0123456789abcdef" }
func (*testRuntime) EnsureJob(context.Context, runner.ContainmentRef) error {
	return nil
}
func (runtime *testRuntime) LockFence(_ context.Context, ref runner.ContainmentRef) (Fence, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	key := ref.OwnerID + ":" + ref.FenceToken
	if runtime.fences[key] == nil {
		runtime.fences[key] = &testFence{}
	}
	return runtime.fences[key], nil
}
func (runtime *testRuntime) Launch(_ context.Context, _ LaunchSpec, material io.Reader) (int, error) {
	jit, err := io.ReadAll(material)
	if err != nil {
		return 0, err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.launches++
	runtime.jit = string(jit)
	return 4000 + runtime.launches, nil
}
func (runtime *testRuntime) TerminateAndWait(context.Context, runner.ContainmentRef) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	for _, fence := range runtime.fences {
		fence.mu.Lock()
		revoked := fence.revoked
		fence.mu.Unlock()
		if !revoked {
			return errors.New("job termination preceded fence revocation")
		}
	}
	runtime.kills++
	return nil
}
func (*testRuntime) WaitEmpty(context.Context, runner.ContainmentRef) error { return nil }
func (*testRuntime) Alive(context.Context, runner.ContainmentRef, int) (bool, error) {
	return true, nil
}
func (runtime *testRuntime) FinalizeCleanup(
	ctx context.Context,
	_ runner.ContainmentRef,
	root *os.Root,
	name string,
	expected runner.WorkspaceRef,
	workspace Workspace,
) error {
	if observed, err := workspace.Observe(ctx, root, name); err != nil || observed != expected {
		return runner.ErrCleanupFailed
	}
	return workspace.Remove(ctx, root, name)
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

func newManager(t *testing.T, runtime Runtime, workspace *testWorkspace) *runner.Manager {
	t.Helper()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "run.sh"), []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	adapter, err := New(runtime, workspace)
	if err != nil {
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

func currentPackage(t *testing.T) runner.Package {
	t.Helper()
	pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

func TestAdapterRunsOneJobAndRemovesItsWorkspace(t *testing.T) {
	runtime := newTestRuntime()
	workspace := &testWorkspace{
		ref: runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "volume-file-service-runner"},
	}
	manager := newManager(t, runtime, workspace)
	request := runner.Preparation{ExecutionID: "execution-windows", Package: currentPackage(t)}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.EnsureRunning(context.Background(), runner.Start{
		Preparation: request,
		JIT:         testJIT{value: "windows-jit-canary.example.test"},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != runner.StateReleased || snapshot.Quarantined {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.launches != 1 || runtime.kills != 1 ||
		runtime.jit != "windows-jit-canary.example.test" {
		t.Fatalf("launches=%d kills=%d jit=%q", runtime.launches, runtime.kills, runtime.jit)
	}
}

func TestAdapterLockedWorkspaceQuarantinesInsteadOfReleasing(t *testing.T) {
	runtime := newTestRuntime()
	workspace := &testWorkspace{
		ref:       runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "locked-file-identity"},
		removeErr: errors.New("sharing violation"),
	}
	manager := newManager(t, runtime, workspace)
	request := runner.Preparation{ExecutionID: "execution-locked", Package: currentPackage(t)}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	snapshot, err := manager.Destroy(context.Background(), request.ExecutionID)
	if !errors.Is(err, runner.ErrQuarantined) {
		t.Fatalf("Destroy error = %v", err)
	}
	if snapshot.State != runner.StateCleanupFailed || !snapshot.Quarantined {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestAdapterFailsClosedWhenRuntimeAdmissionIsUnavailable(t *testing.T) {
	runtime := newTestRuntime()
	unavailable := runtimeWithAdmissionError{testRuntime: runtime}
	workspace := &testWorkspace{
		ref: runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace"},
	}
	adapter, err := New(unavailable, workspace)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runner.NewManager(runner.Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{},
		Journal:     runner.NewMemoryJournal(),
		Supervisor:  adapter,
		Cleaner:     adapter,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Ready(context.Background()); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("Ready error = %v", err)
	}
}

type runtimeWithAdmissionError struct {
	*testRuntime
}

func (runtimeWithAdmissionError) ValidateAdmission(context.Context) error {
	return errors.New("injected runtime admission failure")
}
