package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type testCache struct{ root string }

func (c testCache) Ensure(context.Context, Package) (ArchiveRef, error) {
	return ArchiveRef{Directory: c.root, Archive: "test-tree"}, nil
}

type testSupervisor struct {
	starts, stops, prepareCalls int
	startErr                    error
	stopErr                     error
	prepared                    ContainmentRef
	startRequest                StartRequest
	observed                    []Process
	stopped                     []Process
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
func (strongCleaner) PrepareWorkspace(_ context.Context, _ *os.Root, name string) (string, error) {
	return "test:" + name, nil
}
func (strongCleaner) WorkspaceRef(_ context.Context, _ *os.Root, name string) (string, error) {
	return "test:" + name, nil
}

func (*testSupervisor) StrongDescendantOwnership() bool { return true }
func (s *testSupervisor) PrepareContainment(_ context.Context, executionID string) (ContainmentRef, error) {
	s.prepareCalls++
	s.prepared = ContainmentRef{Backend: "test", OwnerID: "unit-" + executionID}
	return s.prepared, nil
}
func (s *testSupervisor) Start(_ context.Context, request StartRequest) (Process, error) {
	s.starts++
	s.startRequest = request
	if s.startErr != nil {
		return Process{Containment: s.prepared}, s.startErr
	}
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
	calls      int
	deliveries int
	after      error
	value      string
}

func (j *callbackJIT) Digest() string {
	sum := sha256.Sum256([]byte(j.value))
	return hex.EncodeToString(sum[:])
}
func (j *callbackJIT) Deliver(deliver func(string) error) error {
	j.deliveries++
	for range j.calls {
		if err := deliver(j.value); err != nil {
			return err
		}
	}
	return j.after
}

type sequencedWorkspaceCleaner struct {
	strongCleaner
	prepareRef   string
	observed     []string
	observations int
}

func (cleaner *sequencedWorkspaceCleaner) PrepareWorkspace(context.Context, *os.Root, string) (string, error) {
	return cleaner.prepareRef, nil
}

func (cleaner *sequencedWorkspaceCleaner) WorkspaceRef(context.Context, *os.Root, string) (string, error) {
	index := cleaner.observations
	cleaner.observations++
	if index >= len(cleaner.observed) {
		index = len(cleaner.observed) - 1
	}
	return cleaner.observed[index], nil
}

type controlledCleaner struct {
	strongCleaner
	removeErr error
	removes   int
}

type barrierSupervisor struct {
	mu           sync.Mutex
	gate         chan struct{}
	prepareCalls int
	starts       int
	stops        int
}

func newBarrierSupervisor() *barrierSupervisor {
	return &barrierSupervisor{gate: make(chan struct{})}
}

func (*barrierSupervisor) StrongDescendantOwnership() bool { return true }

func (supervisor *barrierSupervisor) PrepareContainment(_ context.Context, executionID string) (ContainmentRef, error) {
	supervisor.mu.Lock()
	supervisor.prepareCalls++
	if supervisor.prepareCalls == 2 {
		close(supervisor.gate)
	}
	supervisor.mu.Unlock()
	<-supervisor.gate
	return ContainmentRef{Backend: "test", OwnerID: "unit-" + executionID}, nil
}

func (supervisor *barrierSupervisor) Start(_ context.Context, request StartRequest) (Process, error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.starts++
	return Process{PID: supervisor.starts, Containment: request.Containment}, nil
}

func (supervisor *barrierSupervisor) Stop(context.Context, Process) error {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.stops++
	return nil
}

func (*barrierSupervisor) Alive(Process) (bool, error) { return true, nil }

func (supervisor *barrierSupervisor) counts() (int, int, int) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.prepareCalls, supervisor.starts, supervisor.stops
}

func (cleaner *controlledCleaner) RemoveAndVerify(ctx context.Context, root *os.Root, name string) error {
	cleaner.removes++
	if cleaner.removeErr != nil {
		return cleaner.removeErr
	}
	return cleaner.strongCleaner.RemoveAndVerify(ctx, root, name)
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

func TestWorkspaceReplacementBetweenPreparationReplayAndStartFailsBeforeJIT(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	const originalRef = "test:original-workspace"
	cleaner := &sequencedWorkspaceCleaner{
		prepareRef: originalRef,
		observed:   []string{originalRef, "test:replacement-workspace"},
	}
	supervisor := &testSupervisor{}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{content},
		Journal:     NewMemoryJournal(),
		Supervisor:  supervisor,
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "workspace-replaced-before-start", Package: pkg}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	jit := &callbackJIT{calls: 1, value: "jit-value"}
	state, err := manager.EnsureRunning(context.Background(), Start{Preparation: request, JIT: jit})
	if !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("EnsureRunning = %#v, %v", state, err)
	}
	if cleaner.observations != 2 || supervisor.prepared != (ContainmentRef{}) || supervisor.starts != 0 || jit.deliveries != 0 {
		t.Fatalf(
			"observations=%d containment=%#v starts=%d jit deliveries=%d",
			cleaner.observations,
			supervisor.prepared,
			supervisor.starts,
			jit.deliveries,
		)
	}
}

func TestWorkspaceReplacementAfterStartClaimQuarantinesBeforeSupervisorStart(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	const originalRef = "test:original-workspace"
	cleaner := &sequencedWorkspaceCleaner{
		prepareRef: originalRef,
		observed:   []string{originalRef, originalRef, "test:replacement-workspace"},
	}
	supervisor := &testSupervisor{}
	journal := NewMemoryJournal()
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "workspace-replaced-after-claim", Package: pkg}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	jit := &callbackJIT{calls: 1, value: "jit-value"}
	state, err := manager.EnsureRunning(context.Background(), Start{Preparation: request, JIT: jit})
	if !errors.Is(err, ErrQuarantined) || !state.Quarantined {
		t.Fatalf("EnsureRunning = %#v, %v", state, err)
	}
	if cleaner.observations != 3 || supervisor.prepareCalls != 1 || supervisor.starts != 0 || supervisor.stops != 1 || jit.deliveries != 1 {
		t.Fatalf(
			"observations=%d prepares=%d starts=%d stops=%d jit deliveries=%d",
			cleaner.observations,
			supervisor.prepareCalls,
			supervisor.starts,
			supervisor.stops,
			jit.deliveries,
		)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateCleanupFailed || !record.Tombstone {
		t.Fatalf("quarantine record = %#v, found=%v, err=%v", record, found, loadErr)
	}
}

func TestSupervisorWorkspaceIdentityFailureQuarantines(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := &testSupervisor{startErr: ErrWorkspaceChanged}
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
	request := Preparation{ExecutionID: "supervisor-workspace-mismatch", Package: pkg}
	state, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	})
	if !errors.Is(err, ErrQuarantined) || !state.Quarantined {
		t.Fatalf("EnsureRunning = %#v, %v", state, err)
	}
	if supervisor.starts != 1 || supervisor.stops != 1 || supervisor.startRequest.WorkspaceRef == "" {
		t.Fatalf("starts=%d stops=%d request=%#v", supervisor.starts, supervisor.stops, supervisor.startRequest)
	}
}

func TestTwoManagersUseJournalCASToStartExactlyOneRunner(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	journal := NewMemoryJournal()
	initial, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  &testSupervisor{},
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "two-manager-start-claim", Package: pkg}
	if _, err := initial.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	supervisor := newBarrierSupervisor()
	managerA, err := NewManager(Options{RuntimeRoot: runtimeRoot, Cache: testCache{content}, Journal: journal, Supervisor: supervisor, Cleaner: strongCleaner{}})
	if err != nil {
		t.Fatal(err)
	}
	managerB, err := NewManager(Options{RuntimeRoot: runtimeRoot, Cache: testCache{content}, Journal: journal, Supervisor: supervisor, Cleaner: strongCleaner{}})
	if err != nil {
		t.Fatal(err)
	}
	jitA := &callbackJIT{calls: 1, value: "same-jit"}
	jitB := &callbackJIT{calls: 1, value: "same-jit"}
	results := make(chan error, 2)
	var group sync.WaitGroup
	for _, attempt := range []struct {
		manager *Manager
		jit     *callbackJIT
	}{{managerA, jitA}, {managerB, jitB}} {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := attempt.manager.EnsureRunning(context.Background(), Start{Preparation: request, JIT: attempt.jit})
			results <- err
		}()
	}
	group.Wait()
	close(results)
	var succeeded, reconciled int
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrReconciliationRequired):
			reconciled++
		default:
			t.Fatalf("unexpected EnsureRunning error = %v", err)
		}
	}
	prepares, starts, stops := supervisor.counts()
	if succeeded != 1 || reconciled != 1 || prepares != 2 || starts != 1 || stops != 0 || jitA.deliveries+jitB.deliveries != 1 {
		t.Fatalf(
			"succeeded=%d reconciled=%d prepares=%d starts=%d stops=%d deliveries=%d",
			succeeded,
			reconciled,
			prepares,
			starts,
			stops,
			jitA.deliveries+jitB.deliveries,
		)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateRunning || record.Revision != 4 {
		t.Fatalf("final record = %#v, found=%v, err=%v", record, found, loadErr)
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
	record, swapped, err := journal.CompareAndSwap(context.Background(), record.ExecutionID, record.Revision, record.Record)
	if err != nil || !swapped {
		t.Fatalf("persist starting record = %#v, swapped=%v, err=%v", record, swapped, err)
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

type brokenArchiveCache struct{}

func (brokenArchiveCache) Ensure(context.Context, Package) (ArchiveRef, error) {
	return ArchiveRef{Archive: "archive"}, nil
}

func TestPreparationFailureRemovesRootAndCommitsTerminalFailure(t *testing.T) {
	journal := NewMemoryJournal()
	runtimeRoot := t.TempDir()
	manager, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       brokenArchiveCache{},
		Journal:     journal,
		Supervisor:  &testSupervisor{},
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "package-materialization-failure", Package: pkg}
	state, err := manager.EnsurePrepared(context.Background(), request)
	if !errors.Is(err, ErrPackageIntegrity) || state.State != StateFailed {
		t.Fatalf("EnsurePrepared = %#v, %v", state, err)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateFailed || record.Revision != 2 {
		t.Fatalf("failed record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	root := filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))
	if _, statErr := os.Lstat(root); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed preparation root remained: %v", statErr)
	}
	if _, err := manager.EnsurePrepared(context.Background(), request); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("failed preparation replay error = %v", err)
	}
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

func TestPreparingCrashRecordBlocksReplayAndQuarantinesWithoutWorkspaceIdentity(t *testing.T) {
	cache := &countingCache{}
	journal := NewMemoryJournal()
	runtimeRoot := t.TempDir()
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "crash-during-preparation", Package: pkg}
	record := Record{
		ExecutionID: request.ExecutionID,
		SpecDigest:  preparationDigest(request),
		State:       StatePreparing,
		RootName:    executionRootName(request.ExecutionID),
	}
	if _, created, err := journal.Create(context.Background(), record); err != nil || !created {
		t.Fatalf("Create preparing record: created=%v err=%v", created, err)
	}
	manager, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       cache,
		Journal:     journal,
		Supervisor:  &testSupervisor{},
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.EnsurePrepared(context.Background(), request); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("EnsurePrepared error = %v", err)
	}
	if cache.calls != 0 {
		t.Fatalf("preparing replay reached cache %d times", cache.calls)
	}
	state, err := manager.Destroy(context.Background(), request.ExecutionID)
	if !errors.Is(err, ErrQuarantined) || !state.Quarantined {
		t.Fatalf("Destroy = %#v, %v", state, err)
	}
	durable, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || durable.State != StateCleanupFailed || !durable.Tombstone {
		t.Fatalf("durable quarantine = %#v, found=%v, err=%v", durable, found, loadErr)
	}
}

type failOnCommitJournal struct {
	inner   *MemoryJournal
	commits int
	failAt  int
}

func (journal *failOnCommitJournal) Load(ctx context.Context, executionID string) (VersionedRecord, bool, error) {
	return journal.inner.Load(ctx, executionID)
}
func (journal *failOnCommitJournal) Create(ctx context.Context, record Record) (VersionedRecord, bool, error) {
	journal.commits++
	if journal.commits == journal.failAt {
		return VersionedRecord{}, false, errors.New("injected journal failure")
	}
	return journal.inner.Create(ctx, record)
}
func (journal *failOnCommitJournal) CompareAndSwap(ctx context.Context, executionID string, expectedRevision uint64, record Record) (VersionedRecord, bool, error) {
	journal.commits++
	if journal.commits == journal.failAt {
		return VersionedRecord{}, false, errors.New("injected journal failure")
	}
	return journal.inner.CompareAndSwap(ctx, executionID, expectedRevision, record)
}

func TestRunningCommitFailureStopsProcessAndLeavesStartingRecordReconcileable(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := &failOnCommitJournal{inner: NewMemoryJournal(), failAt: 4}
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

func TestRunningCommitAndContainmentStopFailurePersistQuarantine(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := &failOnCommitJournal{inner: NewMemoryJournal(), failAt: 4}
	supervisor := &testSupervisor{stopErr: ErrCleanupFailed}
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
	request := Preparation{ExecutionID: "running-commit-and-stop-failure", Package: pkg}
	state, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	})
	if !errors.Is(err, ErrQuarantined) || !state.Quarantined || supervisor.starts != 1 || supervisor.stops != 1 {
		t.Fatalf("EnsureRunning=%#v err=%v starts=%d stops=%d", state, err, supervisor.starts, supervisor.stops)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateCleanupFailed || !record.Tombstone || record.Revision != 4 {
		t.Fatalf("quarantine record = %#v, found=%v, err=%v", record, found, loadErr)
	}
}

func TestDestroyPersistsCleaningBeforeDestructiveSideEffects(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := &failOnCommitJournal{inner: NewMemoryJournal(), failAt: 3}
	cleaner := &controlledCleaner{}
	supervisor := &testSupervisor{}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "cleaning-intent-save-failure", Package: pkg}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Destroy(context.Background(), request.ExecutionID); !errors.Is(err, ErrJournal) {
		t.Fatalf("Destroy error = %v", err)
	}
	record, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found || record.State != StatePrepared {
		t.Fatalf("durable record = %#v, found=%v, err=%v", record, found, err)
	}
	if cleaner.removes != 0 || supervisor.stops != 0 {
		t.Fatalf("remove calls=%d stop calls=%d", cleaner.removes, supervisor.stops)
	}
}

func TestFailedQuarantineWriteLeavesDurableCleaningAndBlocksAdmission(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := &failOnCommitJournal{inner: NewMemoryJournal(), failAt: 4}
	cleaner := &controlledCleaner{removeErr: errors.New("injected cleanup failure")}
	supervisor := &testSupervisor{}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "quarantine-save-failure", Package: pkg}
	if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Destroy(context.Background(), request.ExecutionID); !errors.Is(err, ErrJournal) {
		t.Fatalf("Destroy error = %v", err)
	}
	record, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found || record.State != StateCleaning || record.Tombstone {
		t.Fatalf("durable cleanup intent = %#v, found=%v, err=%v", record, found, err)
	}
	if _, err := manager.EnsurePrepared(context.Background(), request); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("EnsurePrepared after failed quarantine = %v", err)
	}
	jit := &callbackJIT{calls: 1, value: "jit-value"}
	if _, err := manager.EnsureRunning(context.Background(), Start{Preparation: request, JIT: jit}); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("EnsureRunning after failed quarantine = %v", err)
	}
	if supervisor.prepared != (ContainmentRef{}) || supervisor.starts != 0 || jit.deliveries != 0 {
		t.Fatalf("containment=%#v starts=%d jit deliveries=%d", supervisor.prepared, supervisor.starts, jit.deliveries)
	}
	journal.failAt = 0
	cleaner.removeErr = nil
	released, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || released.State != StateReleased {
		t.Fatalf("retry Destroy = %#v, %v", released, err)
	}
}
