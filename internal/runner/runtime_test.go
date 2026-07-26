package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type testCache struct{ root string }

func (c testCache) Ensure(context.Context, Package) (ArchiveRef, error) {
	return ArchiveRef{Directory: c.root, Archive: "test-tree"}, nil
}

type testSupervisor struct {
	starts, stops int
	stopErr       error
	prepared      ContainmentRef
	startRequest  StartRequest
	observed      []Process
	stopped       []Process
}
type strongCleaner struct{ rootCleaner }

func (strongCleaner) StrongWorkspaceOwnership() bool { return true }
func (strongCleaner) ValidateRuntimeRoot(_ context.Context, root string) error {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrStrongOwnershipUnavailable
	}
	return nil
}
func (strongCleaner) WorkspaceRef(_ context.Context, _ *os.Root, name string) (string, error) {
	return "test:" + name, nil
}

func (*testSupervisor) StrongDescendantOwnership() bool { return true }
func (s *testSupervisor) PrepareContainment(_ context.Context, executionID string) (ContainmentRef, error) {
	s.prepared = ContainmentRef{Backend: "test", OwnerID: "unit-" + executionID}
	return s.prepared, nil
}
func (s *testSupervisor) Start(_ context.Context, request StartRequest) (Process, error) {
	s.starts++
	s.startRequest = request
	return Process{PID: s.starts, Containment: s.prepared}, nil
}
func (s *testSupervisor) Stop(_ context.Context, process Process) error {
	s.stops++
	s.stopped = append(s.stopped, process)
	return s.stopErr
}
func (s *testSupervisor) Alive(process Process) (bool, error) {
	s.observed = append(s.observed, process)
	return true, nil
}

type callbackJIT struct {
	calls int
	after error
	value string
}

func (j *callbackJIT) Digest() string {
	sum := sha256.Sum256([]byte(j.value))
	return hex.EncodeToString(sum[:])
}
func (j *callbackJIT) Deliver(deliver func(string) error) error {
	for range j.calls {
		if err := deliver(j.value); err != nil {
			return err
		}
	}
	return j.after
}

func TestJITCallbackReplayStopsSingleProcess(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := &testSupervisor{}
	journal := NewMemoryJournal()
	manager, err := NewManager(Options{RuntimeRoot: t.TempDir(), Cache: testCache{content}, Journal: journal, Supervisor: supervisor, Cleaner: strongCleaner{}})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "callback-replay", Package: pkg}
	_, err = manager.EnsureRunning(context.Background(), Start{Preparation: request, JIT: &callbackJIT{calls: 2, value: "canary"}})
	if !errors.Is(err, ErrInvalidRequest) || supervisor.starts != 1 || supervisor.stops != 1 {
		t.Fatalf("result=%v starts=%d stops=%d", err, supervisor.starts, supervisor.stops)
	}
	record, _, _ := journal.Load(context.Background(), request.ExecutionID)
	if record.State != StatePrepared || record.JITDigest != "" {
		t.Fatalf("record = %#v", record)
	}
}

func TestJITDeliveryFailureWithStopFailureQuarantines(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := &testSupervisor{stopErr: ErrCleanupFailed}
	journal := NewMemoryJournal()
	manager, err := NewManager(Options{RuntimeRoot: t.TempDir(), Cache: testCache{content}, Journal: journal, Supervisor: supervisor, Cleaner: strongCleaner{}})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "stop-failure", Package: pkg}
	_, err = manager.EnsureRunning(context.Background(), Start{Preparation: request, JIT: &callbackJIT{calls: 1, after: errors.New("delivery failed"), value: "canary"}})
	if !errors.Is(err, ErrQuarantined) {
		t.Fatalf("EnsureRunning error = %v", err)
	}
	record, _, _ := journal.Load(context.Background(), request.ExecutionID)
	if record.State != StateCleanupFailed || !record.Tombstone {
		t.Fatalf("record = %#v", record)
	}
}

func TestPreparedExecutionCanBeDestroyedIdempotently(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{content},
		Journal:     NewMemoryJournal(),
		Supervisor:  &testSupervisor{},
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "cancel-before-start", Package: pkg}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	first, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || first.State != StateReleased {
		t.Fatalf("first Destroy = %#v, %v", first, err)
	}
	second, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || second != first {
		t.Fatalf("replayed Destroy = %#v, %v", second, err)
	}
}

func TestLifecycleRetainsContainmentAcrossStartReplayInspectAndDestroy(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := &testSupervisor{}
	journal := NewMemoryJournal()
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	preparation := Preparation{ExecutionID: "containment-round-trip", Package: pkg}
	start := Start{Preparation: preparation, JIT: &callbackJIT{calls: 1, value: "jit-value"}}
	running, err := manager.EnsureRunning(context.Background(), start)
	if err != nil || !running.Running {
		t.Fatalf("EnsureRunning = %#v, %v", running, err)
	}
	expected := ContainmentRef{Backend: "test", OwnerID: "unit-" + preparation.ExecutionID}
	if supervisor.startRequest.Containment != expected {
		t.Fatalf("start containment = %#v", supervisor.startRequest.Containment)
	}
	if _, err := manager.EnsureRunning(context.Background(), start); err != nil {
		t.Fatalf("running replay: %v", err)
	}
	if _, err := manager.Inspect(context.Background(), preparation.ExecutionID); err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if len(supervisor.observed) != 2 {
		t.Fatalf("Alive calls = %#v", supervisor.observed)
	}
	for _, process := range supervisor.observed {
		if process.Containment != expected {
			t.Fatalf("observed containment = %#v", process.Containment)
		}
	}
	if _, err := manager.Destroy(context.Background(), preparation.ExecutionID); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if len(supervisor.stopped) != 1 || supervisor.stopped[0].Containment != expected {
		t.Fatalf("stopped processes = %#v", supervisor.stopped)
	}
	record, found, err := journal.Load(context.Background(), preparation.ExecutionID)
	if err != nil || !found || record.Containment != expected || record.State != StateReleased {
		t.Fatalf("released record = %#v, found=%v, err=%v", record, found, err)
	}
}

func TestStartingCrashUsesDurableContainmentForCleanupWithoutRestart(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := NewMemoryJournal()
	supervisor := &testSupervisor{}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "crash-after-containment", Package: pkg}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	record, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found {
		t.Fatalf("prepared record = %#v, found=%v, err=%v", record, found, err)
	}
	record.State = StateStarting
	record.JITDigest = (&callbackJIT{value: "jit-value"}).Digest()
	record.Containment = ContainmentRef{Backend: "test", OwnerID: "durable-owner"}
	if err := journal.Save(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	}); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("starting replay error = %v", err)
	}
	released, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || released.State != StateReleased {
		t.Fatalf("Destroy starting record = %#v, %v", released, err)
	}
	if len(supervisor.stopped) != 1 || supervisor.stopped[0].PID != 0 || supervisor.stopped[0].Containment != record.Containment {
		t.Fatalf("starting cleanup process = %#v", supervisor.stopped)
	}
}

type countingCache struct{ calls int }

func (cache *countingCache) Ensure(context.Context, Package) (ArchiveRef, error) {
	cache.calls++
	return ArchiveRef{}, errors.New("unexpected cache call")
}

func TestPreparationFailsBeforeSideEffectsWithoutStrongWorkspaceOwnership(t *testing.T) {
	cache := &countingCache{}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       cache,
		Journal:     NewMemoryJournal(),
		Supervisor:  &testSupervisor{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	_, err = manager.EnsurePrepared(context.Background(), Preparation{ExecutionID: "weak-workspace", Package: pkg})
	if !errors.Is(err, ErrStrongOwnershipUnavailable) {
		t.Fatalf("EnsurePrepared error = %v", err)
	}
	if cache.calls != 0 {
		t.Fatalf("weak preparation reached cache %d times", cache.calls)
	}
}

type failOnSaveJournal struct {
	inner  *MemoryJournal
	saves  int
	failAt int
}

func (journal *failOnSaveJournal) Load(ctx context.Context, executionID string) (Record, bool, error) {
	return journal.inner.Load(ctx, executionID)
}
func (journal *failOnSaveJournal) Save(ctx context.Context, record Record) error {
	journal.saves++
	if journal.saves == journal.failAt {
		return errors.New("injected journal failure")
	}
	return journal.inner.Save(ctx, record)
}

func TestRunningCommitFailureStopsProcessAndLeavesStartingRecordReconcileable(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := &failOnSaveJournal{inner: NewMemoryJournal(), failAt: 3}
	supervisor := &testSupervisor{}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "running-commit-failure", Package: pkg}
	_, err = manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	})
	if !errors.Is(err, ErrJournal) || supervisor.starts != 1 || supervisor.stops != 1 {
		t.Fatalf("EnsureRunning error=%v starts=%d stops=%d", err, supervisor.starts, supervisor.stops)
	}
	record, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found || record.State != StateStarting || !validContainment(record.Containment) {
		t.Fatalf("durable crash record = %#v, found=%v, err=%v", record, found, err)
	}
	journal.failAt = 0
	released, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || released.State != StateReleased || supervisor.stops != 2 {
		t.Fatalf("reconciled Destroy = %#v, err=%v, stops=%d", released, err, supervisor.stops)
	}
}
