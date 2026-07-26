//go:build darwin || linux

package runner_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/genm/tewake/internal/runner"
)

type fakeCache struct{ content string }

func (f fakeCache) Ensure(context.Context, runner.Package) (runner.ArchiveRef, error) {
	return runner.ArchiveRef{Directory: f.content, Archive: "test-tree"}, nil
}

type fakeJIT struct{ value string }

func (j fakeJIT) Digest() string {
	sum := sha256.Sum256([]byte(j.value))
	return hex.EncodeToString(sum[:])
}
func (j fakeJIT) Deliver(deliver func(string) error) error { return deliver(j.value) }

type failingCleaner struct{}

func (failingCleaner) StrongWorkspaceOwnership() bool { return true }
func (failingCleaner) WorkspaceBackend() string       { return "test-v1" }
func (failingCleaner) ValidateRuntimeRoot(context.Context, string) error {
	return nil
}
func (failingCleaner) PrepareWorkspace(_ context.Context, _ *os.Root, name string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{Backend: "test-v1", OwnerID: "test:" + name}, nil
}
func (failingCleaner) WorkspaceRef(_ context.Context, _ *os.Root, name string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{Backend: "test-v1", OwnerID: "test:" + name}, nil
}

func (failingCleaner) RemoveAndVerify(context.Context, *os.Root, string) error {
	return errors.New("permission denied: secret path")
}

type strongTestCleaner struct{}

func (strongTestCleaner) StrongWorkspaceOwnership() bool { return true }
func (strongTestCleaner) WorkspaceBackend() string       { return "test-v1" }
func (strongTestCleaner) ValidateRuntimeRoot(_ context.Context, root string) error {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}
func (strongTestCleaner) PrepareWorkspace(_ context.Context, _ *os.Root, name string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{Backend: "test-v1", OwnerID: "test:" + name}, nil
}
func (strongTestCleaner) WorkspaceRef(_ context.Context, _ *os.Root, name string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{Backend: "test-v1", OwnerID: "test:" + name}, nil
}
func (strongTestCleaner) RemoveAndVerify(_ context.Context, root *os.Root, name string) error {
	if err := root.RemoveAll(name); err != nil {
		return err
	}
	if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		return runner.ErrCleanupFailed
	}
	return nil
}

type packageBoundarySupervisor struct {
	mu         sync.Mutex
	fenced     map[string]bool
	running    map[string]bool
	starts     int
	stops      int
	deliveries int
	jitDigest  string
}

func newPackageBoundarySupervisor() *packageBoundarySupervisor {
	return &packageBoundarySupervisor{
		fenced:  make(map[string]bool),
		running: make(map[string]bool),
	}
}

func (*packageBoundarySupervisor) StrongDescendantOwnership() bool { return true }
func (*packageBoundarySupervisor) WorkspaceBackend() string        { return "test-v1" }
func (*packageBoundarySupervisor) PrepareContainment(_ context.Context, executionID string) (runner.ContainmentRef, error) {
	return runner.ContainmentRef{Backend: "external-test", OwnerID: "owner-" + executionID}, nil
}
func (supervisor *packageBoundarySupervisor) Start(ctx context.Context, request runner.StartRequest) (runner.Process, error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	token := request.Containment.FenceToken
	if supervisor.fenced[token] {
		return runner.Process{Containment: request.Containment}, runner.ErrStartFenced
	}
	if err := request.VerifyWorkspaceAtExec(ctx); err != nil {
		return runner.Process{Containment: request.Containment}, err
	}
	if err := request.DeliverJIT(func(value string) error {
		sum := sha256.Sum256([]byte(value))
		supervisor.jitDigest = hex.EncodeToString(sum[:])
		supervisor.deliveries++
		return nil
	}); err != nil {
		return runner.Process{Containment: request.Containment}, err
	}
	supervisor.starts++
	supervisor.running[token] = true
	return runner.Process{PID: supervisor.starts, Containment: request.Containment}, nil
}
func (supervisor *packageBoundarySupervisor) Stop(_ context.Context, process runner.Process) error {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	token := process.Containment.FenceToken
	supervisor.stops++
	supervisor.fenced[token] = true
	delete(supervisor.running, token)
	return nil
}
func (supervisor *packageBoundarySupervisor) Alive(process runner.Process) (bool, error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.running[process.Containment.FenceToken], nil
}

func TestExternalPlatformSupervisorConsumesJITOnceWithoutRawAccessor(t *testing.T) {
	content := fakeRunner(t)
	runtimeRoot := t.TempDir()
	journal := runner.NewMemoryJournal()
	supervisor := newPackageBoundarySupervisor()
	manager, err := runner.NewManager(runner.Options{
		RuntimeRoot: runtimeRoot,
		Cache:       fakeCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongTestCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := runner.Preparation{ExecutionID: "external-platform-jit", Package: currentPackage(t)}
	const jitCanary = "external-platform-jit.example.test"
	running, err := manager.EnsureRunning(context.Background(), runner.Start{
		Preparation: request,
		JIT:         fakeJIT{jitCanary},
	})
	if err != nil || !running.Running {
		t.Fatalf("EnsureRunning = %#v, %v", running, err)
	}
	expected := fakeJIT{jitCanary}.Digest()
	if supervisor.starts != 1 || supervisor.deliveries != 1 || supervisor.jitDigest != expected {
		t.Fatalf(
			"starts=%d deliveries=%d digest=%q",
			supervisor.starts,
			supervisor.deliveries,
			supervisor.jitDigest,
		)
	}
	released, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || released.State != runner.StateReleased || supervisor.stops != 1 {
		t.Fatalf("Destroy = %#v, %v stops=%d", released, err, supervisor.stops)
	}
}

func TestLifecycleIsIdempotentAndKillsDescendants(t *testing.T) {
	if !runner.NewSupervisor().StrongDescendantOwnership() {
		t.Skip("platform containment adapter is a later gate")
	}
	content := fakeRunner(t)
	journal := runner.NewMemoryJournal()
	manager, runtimeRoot := newManager(t, content, journal, nil)
	request := runner.Preparation{ExecutionID: "execution-one", Package: currentPackage(t), DisableUpdate: true}
	prepared, err := manager.EnsurePrepared(context.Background(), request)
	if err != nil || !prepared.Prepared {
		t.Fatalf("EnsurePrepared = %#v, %v", prepared, err)
	}
	if replay, err := manager.EnsurePrepared(context.Background(), request); err != nil || replay != prepared {
		t.Fatalf("prepared replay = %#v, %v", replay, err)
	}
	const jitCanary = "jit-canary-must-not-persist"
	started, err := manager.EnsureRunning(context.Background(), runner.Start{Preparation: request, JIT: fakeJIT{jitCanary}})
	if err != nil || !started.Running {
		t.Fatalf("EnsureRunning = %#v, %v", started, err)
	}
	if _, err := manager.EnsureRunning(context.Background(), runner.Start{Preparation: request, JIT: fakeJIT{jitCanary}}); err != nil {
		t.Fatalf("running replay: %v", err)
	}
	record, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found {
		t.Fatalf("journal record = %#v, %v, found=%v", record, err, found)
	}
	if strings.Contains(fmt.Sprintf("%#v", record), jitCanary) {
		t.Fatal("journal retained opaque JIT material")
	}
	executionRoot := filepath.Join(runtimeRoot, "executions", record.RootName)
	starts := eventuallyReadFile(t, filepath.Join(executionRoot, "starts"))
	if string(starts) != "start\n" {
		t.Fatalf("runner start count = %q", starts)
	}
	childData := eventuallyReadFile(t, filepath.Join(executionRoot, "child.pid"))
	childPID, err := strconv.Atoi(string(childData))
	if err != nil {
		t.Fatal(err)
	}
	if destroyed, err := manager.Destroy(context.Background(), request.ExecutionID); err != nil || destroyed.State != runner.StateReleased {
		t.Fatalf("Destroy = %#v, %v", destroyed, err)
	}
	eventuallyGone(t, childPID)
	if _, err := os.Stat(executionRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("execution root still exists: %v", err)
	}
	if replay, err := manager.Destroy(context.Background(), request.ExecutionID); err != nil || replay.State != runner.StateReleased {
		t.Fatalf("destroy replay = %#v, %v", replay, err)
	}
}

func TestChangedExecutionSpecAndCrashStartingFailClosed(t *testing.T) {
	if !runner.NewSupervisor().StrongDescendantOwnership() {
		t.Skip("platform containment adapter is a later gate")
	}
	content := fakeRunner(t)
	journal := runner.NewMemoryJournal()
	manager, _ := newManager(t, content, journal, nil)
	request := runner.Preparation{ExecutionID: "execution-replay", Package: currentPackage(t)}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	changed := request
	changed.DisableUpdate = true
	if _, err := manager.EnsurePrepared(context.Background(), changed); !errors.Is(err, runner.ErrExecutionConflict) {
		t.Fatalf("changed spec error = %v", err)
	}
	// A journal recovered in starting state cannot safely infer whether a listener
	// was spawned before the crash, so reopening refuses a potentially duplicate run.
	record, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found {
		t.Fatalf("prepared record = %#v, found=%v, err=%v", record, found, err)
	}
	record.State = runner.StateStarting
	record.RootName = strings.Repeat("a", 64)
	record.JITDigest = fakeJIT{"hang"}.Digest()
	record.WorkspaceRef = runner.WorkspaceRef{}
	record.Containment = runner.ContainmentRef{}
	if _, swapped, err := journal.CompareAndSwap(context.Background(), record.ExecutionID, record.Revision, strings.Repeat("b", 32), record.Record); err != nil || !swapped {
		t.Fatalf("persist invalid crash record: swapped=%v err=%v", swapped, err)
	}
	reopened, _ := newManager(t, content, journal, nil)
	if _, err := reopened.EnsureRunning(context.Background(), runner.Start{Preparation: request, JIT: fakeJIT{"hang"}}); !errors.Is(err, runner.ErrReconciliationRequired) {
		t.Fatalf("reopened start error = %v", err)
	}
}

func TestCleanupFailureWritesTombstoneAndQuarantines(t *testing.T) {
	content := fakeRunner(t)
	journal := runner.NewMemoryJournal()
	manager, _ := newManager(t, content, journal, failingCleaner{})
	request := runner.Preparation{ExecutionID: "execution-cleanup", Package: currentPackage(t)}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	state, err := manager.Destroy(context.Background(), request.ExecutionID)
	if !errors.Is(err, runner.ErrQuarantined) || !state.Quarantined {
		t.Fatalf("Destroy = %#v, %v", state, err)
	}
	record, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found || !record.Tombstone || record.State != runner.StateCleanupFailed {
		t.Fatalf("tombstone = %#v, %v, found=%v", record, err, found)
	}
}

func TestPreparedCleanupRemovesRunnerCredentialAndWorkspaceMaterial(t *testing.T) {
	content := fakeRunner(t)
	journal := runner.NewMemoryJournal()
	manager, runtimeRoot := newManager(t, content, journal, strongTestCleaner{})
	request := runner.Preparation{ExecutionID: "execution-sensitive-files", Package: currentPackage(t)}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	record, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found {
		t.Fatalf("prepared record = %#v, found=%v, err=%v", record, found, err)
	}
	executionRoot := filepath.Join(runtimeRoot, "executions", record.RootName)
	for name, value := range map[string]string{
		".runner":                "runner-registration-canary",
		".credentials":           "credential-canary",
		".credentials_rsaparams": "rsa-canary",
		"_work/job/secret":       "workspace-canary",
	} {
		path := filepath.Join(executionRoot, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	released, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || released.State != runner.StateReleased {
		t.Fatalf("Destroy = %#v, %v", released, err)
	}
	if _, err := os.Lstat(executionRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("execution root remained after cleanup: %v", err)
	}
}

func TestThirtyTwoDistinctPreparationsRemainUnique(t *testing.T) {
	content := fakeRunner(t)
	manager, runtimeRoot := newManager(t, content, runner.NewMemoryJournal(), nil)
	pkg := currentPackage(t)
	const count = 32
	errs := make(chan error, count)
	var group sync.WaitGroup
	for index := range count {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := manager.EnsurePrepared(context.Background(), runner.Preparation{ExecutionID: fmt.Sprintf("concurrent-%d", index), Package: pkg})
			errs <- err
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(runtimeRoot, "executions"))
	if err != nil || len(entries) != count {
		t.Fatalf("execution roots=%d err=%v", len(entries), err)
	}
}

func BenchmarkPreparedCacheHit(b *testing.B) {
	content := b.TempDir()
	writeFakeRunner(b, content)
	packageValue, _ := runner.OfficialPackage(runner.CurrentPlatform())
	for b.Loop() {
		journal := runner.NewMemoryJournal()
		manager, err := runner.NewManager(runner.Options{RuntimeRoot: b.TempDir(), Cache: fakeCache{content}, Journal: journal, Cleaner: strongTestCleaner{}})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := manager.EnsurePrepared(context.Background(), runner.Preparation{ExecutionID: fmt.Sprintf("bench-%d", b.N), Package: packageValue}); err != nil {
			b.Fatal(err)
		}
	}
}

func newManager(t *testing.T, content string, journal runner.Journal, cleaner runner.Cleaner) (*runner.Manager, string) {
	t.Helper()
	if cleaner == nil {
		cleaner = strongTestCleaner{}
	}
	runtimeRoot := t.TempDir()
	manager, err := runner.NewManager(runner.Options{RuntimeRoot: runtimeRoot, Cache: fakeCache{content}, Journal: journal, Cleaner: cleaner})
	if err != nil {
		t.Fatal(err)
	}
	return manager, runtimeRoot
}

func currentPackage(t *testing.T) runner.Package {
	t.Helper()
	pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil {
		t.Skipf("unsupported integration platform: %v", err)
	}
	return pkg
}

func fakeRunner(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	writeFakeRunner(t, directory)
	return directory
}

func writeFakeRunner(t testing.TB, directory string) {
	t.Helper()
	script := "#!/bin/sh\nprintf 'start\\n' >> starts\nmkdir -p _work\nprintf x > .runner\nprintf x > .credentials\nprintf x > .credentials_rsaparams\nprintf x > _work/secret\ncase \"$2\" in fail) exit 1 ;; success) exit 0 ;; esac\n(while :; do sleep 1; done) &\nchild=$!\nprintf '%s' \"$child\" > child.pid\nwhile :; do sleep 1; done\n"
	if err := os.WriteFile(filepath.Join(directory, "run.sh"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
}

func eventuallyGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("descendant process %d survived cleanup", pid)
}

func eventuallyReadFile(t *testing.T, filename string) []byte {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(filename)
		if err == nil {
			return data
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("runner did not create %s", filename)
	return nil
}
