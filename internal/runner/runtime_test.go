package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type testCache struct{ root string }

type testPreparedPackage struct{ root string }

func (prepared testPreparedPackage) Materialize(destination *os.Root) error {
	return copyTree(prepared.root, destination)
}
func (testPreparedPackage) Close() error { return nil }

func (c testCache) Ensure(context.Context, Package) (PreparedPackage, error) {
	return testPreparedPackage{root: c.root}, nil
}

type testSupervisor struct {
	starts, stops, prepareCalls int
	waits                       int
	startErr                    error
	stopErr                     error
	waitErr                     error
	waitGate                    chan struct{}
	waitEntered                 chan struct{}
	waitOnce                    sync.Once
	materializeOnStart          bool
	deliverTwice                bool
	skipJITDelivery             bool
	workspaceBackend            string
	jitDeliveries               int
	prepared                    ContainmentRef
	startRequest                StartRequest
	observed                    []Process
	stopped                     []Process
	waited                      []Process
}

type cleanupFinalizingSupervisor struct {
	*testSupervisor
	finalizeErr   error
	garbageErr    error
	garbageBlock  bool
	finalized     bool
	finalizeCalls int
	garbageCalls  int
}

func (supervisor *cleanupFinalizingSupervisor) FinalizeCleanup(
	ctx context.Context,
	_ Process,
	root *os.Root,
	name string,
	_ WorkspaceRef,
) error {
	supervisor.finalizeCalls++
	if err := ctx.Err(); err != nil {
		return err
	}
	if supervisor.finalizeErr != nil {
		return supervisor.finalizeErr
	}
	if supervisor.finalized {
		if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			return ErrCleanupFailed
		}
		return nil
	}
	if _, err := root.Lstat(name); err != nil {
		return ErrCleanupFailed
	}
	if err := root.RemoveAll(name); err != nil {
		return ErrCleanupFailed
	}
	if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		return ErrCleanupFailed
	}
	supervisor.finalized = true
	return nil
}

func (supervisor *cleanupFinalizingSupervisor) GarbageCollectCleanup(ctx context.Context, _ Process) error {
	supervisor.garbageCalls++
	if supervisor.garbageBlock {
		<-ctx.Done()
		return ctx.Err()
	}
	if supervisor.garbageErr == nil {
		supervisor.finalized = false
	}
	return supervisor.garbageErr
}

type failReleasedJournal struct {
	inner        *MemoryJournal
	failReleased bool
}

func (journal *failReleasedJournal) Load(ctx context.Context, executionID string) (VersionedRecord, bool, error) {
	return journal.inner.Load(ctx, executionID)
}
func (journal *failReleasedJournal) Create(ctx context.Context, mutationToken string, record Record) (VersionedRecord, bool, error) {
	return journal.inner.Create(ctx, mutationToken, record)
}
func (journal *failReleasedJournal) CompareAndSwap(
	ctx context.Context,
	executionID string,
	expectedRevision uint64,
	mutationToken string,
	record Record,
) (VersionedRecord, bool, error) {
	if journal.failReleased && record.State == StateReleased {
		current, _, _ := journal.inner.Load(ctx, executionID)
		return current, false, ErrJournal
	}
	return journal.inner.CompareAndSwap(ctx, executionID, expectedRevision, mutationToken, record)
}

type strongCleaner struct{ rootCleaner }

func (strongCleaner) StrongWorkspaceOwnership() bool { return true }
func (strongCleaner) WorkspaceBackend() string       { return "test-v1" }
func (strongCleaner) ValidateRuntimeRoot(_ context.Context, root string) error {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrStrongOwnershipUnavailable
	}
	return nil
}
func (strongCleaner) PrepareWorkspace(_ context.Context, _ *os.Root, name string) (WorkspaceRef, error) {
	return WorkspaceRef{Backend: "test-v1", OwnerID: "test:" + name}, nil
}
func (strongCleaner) WorkspaceRef(_ context.Context, _ *os.Root, name string) (WorkspaceRef, error) {
	return WorkspaceRef{Backend: "test-v1", OwnerID: "test:" + name}, nil
}

type readinessCleaner struct {
	strongCleaner
	err   error
	calls int
}

func (cleaner *readinessCleaner) ValidateRuntimeRoot(ctx context.Context, _ string) error {
	cleaner.calls++
	if err := ctx.Err(); err != nil {
		return err
	}
	return cleaner.err
}

func (*testSupervisor) StrongDescendantOwnership() bool { return true }
func (s *testSupervisor) WorkspaceBackend() string {
	if s.workspaceBackend != "" {
		return s.workspaceBackend
	}
	return "test-v1"
}
func (s *testSupervisor) PrepareContainment(_ context.Context, executionID string) (ContainmentRef, error) {
	s.prepareCalls++
	s.prepared = ContainmentRef{Backend: "test", OwnerID: "unit-" + executionID}
	return s.prepared, nil
}
func (s *testSupervisor) Start(ctx context.Context, request StartRequest) (Process, error) {
	s.startRequest = request
	if err := request.VerifyWorkspaceAtExec(ctx); err != nil {
		return Process{Containment: request.Containment}, err
	}
	if s.skipJITDelivery {
		s.starts++
		return Process{PID: s.starts, Containment: request.Containment}, nil
	}
	if err := request.DeliverJIT(func(value string) error {
		if value == "" {
			return errors.New("empty test JIT")
		}
		s.jitDeliveries++
		if s.materializeOnStart {
			if err := os.MkdirAll(filepath.Join(request.Directory, "_work"), 0o700); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(request.Directory, ".credentials"), []byte("credential-canary"), 0o600); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return Process{Containment: request.Containment}, err
	}
	if s.deliverTwice {
		if err := request.DeliverJIT(func(string) error { return nil }); err != nil {
			return Process{Containment: request.Containment}, err
		}
	}
	s.starts++
	if s.startErr != nil {
		return Process{Containment: request.Containment}, s.startErr
	}
	return Process{PID: s.starts, Containment: request.Containment}, nil
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
func (s *testSupervisor) Wait(ctx context.Context, process Process) error {
	s.waits++
	s.waited = append(s.waited, process)
	if s.waitEntered != nil {
		s.waitOnce.Do(func() { close(s.waitEntered) })
	}
	if s.waitGate != nil {
		select {
		case <-s.waitGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.waitErr
}

func TestManagerReadyRevalidatesPlatformOwnershipAndRuntimeRoot(t *testing.T) {
	cleaner := &readinessCleaner{}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{root: t.TempDir()},
		Journal:     NewMemoryJournal(),
		Supervisor:  &testSupervisor{},
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Ready(context.Background()); err != nil || cleaner.calls != 1 {
		t.Fatalf("Ready error=%v calls=%d", err, cleaner.calls)
	}
	cleaner.err = errors.New("helper unavailable")
	if err := manager.Ready(context.Background()); !errors.Is(err, ErrStrongOwnershipUnavailable) || cleaner.calls != 2 {
		t.Fatalf("failed Ready error=%v calls=%d", err, cleaner.calls)
	}
}

func prepareRunningForFinalization(
	t *testing.T,
	executionID string,
	journal Journal,
	supervisor Supervisor,
) (*Manager, Preparation, string) {
	t.Helper()
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	manager, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{root: content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := OfficialPackage(CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	request := Preparation{ExecutionID: executionID, Package: pkg}
	if _, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "finalize-jit"},
	}); err != nil {
		t.Fatal(err)
	}
	return manager, request, runtimeRoot
}

func TestDestroyCommitsReleasedOnlyAfterPlatformFinalization(t *testing.T) {
	journal := NewMemoryJournal()
	supervisor := &cleanupFinalizingSupervisor{testSupervisor: &testSupervisor{}}
	manager, request, runtimeRoot := prepareRunningForFinalization(
		t, "platform-finalize-success", journal, supervisor,
	)
	released, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || released.State != StateReleased {
		t.Fatalf("Destroy=%#v err=%v", released, err)
	}
	if supervisor.finalizeCalls != 1 || supervisor.garbageCalls != 1 {
		t.Fatalf("finalize=%d garbage=%d", supervisor.finalizeCalls, supervisor.garbageCalls)
	}
	if _, err := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released workspace remains: %v", err)
	}
}

func TestFinalizeFailureQuarantinesWithoutReleasingOrErasingWorkspace(t *testing.T) {
	journal := NewMemoryJournal()
	supervisor := &cleanupFinalizingSupervisor{
		testSupervisor: &testSupervisor{},
		finalizeErr:    errors.New("injected finalize failure"),
	}
	manager, request, runtimeRoot := prepareRunningForFinalization(
		t, "platform-finalize-failure", journal, supervisor,
	)
	state, err := manager.Destroy(context.Background(), request.ExecutionID)
	if !errors.Is(err, ErrQuarantined) || !state.Quarantined {
		t.Fatalf("Destroy=%#v err=%v", state, err)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateCleanupFailed {
		t.Fatalf("record=%#v found=%v err=%v", record, found, loadErr)
	}
	if _, err := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); err != nil {
		t.Fatalf("failed finalize erased workspace: %v", err)
	}
	if supervisor.garbageCalls != 0 {
		t.Fatal("failed finalize reached finalized-fence GC")
	}
}

func TestRestartRecoversCleaningAfterFinalizeBeforeReleasedCommit(t *testing.T) {
	journal := &failReleasedJournal{inner: NewMemoryJournal(), failReleased: true}
	supervisor := &cleanupFinalizingSupervisor{testSupervisor: &testSupervisor{}}
	manager, request, runtimeRoot := prepareRunningForFinalization(
		t, "restart-after-platform-finalize", journal, supervisor,
	)
	if state, err := manager.Destroy(context.Background(), request.ExecutionID); err == nil || state.State == StateReleased {
		t.Fatalf("first Destroy=%#v err=%v", state, err)
	}
	record, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found || record.State != StateCleaning || !supervisor.finalized {
		t.Fatalf("crash boundary record=%#v found=%v err=%v finalized=%v", record, found, err, supervisor.finalized)
	}
	if _, err := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized workspace remains: %v", err)
	}
	journal.failReleased = false
	restarted, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{root: t.TempDir()},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	released, err := restarted.Recover(context.Background(), request.ExecutionID)
	if err != nil || released.State != StateReleased {
		t.Fatalf("Recover=%#v err=%v", released, err)
	}
	if supervisor.finalizeCalls < 2 || supervisor.garbageCalls != 1 {
		t.Fatalf("finalize=%d garbage=%d", supervisor.finalizeCalls, supervisor.garbageCalls)
	}
}

func TestReleasedGarbageCollectionIsBoundedAndRetriedWithoutChangingTerminalState(t *testing.T) {
	journal := NewMemoryJournal()
	supervisor := &cleanupFinalizingSupervisor{
		testSupervisor: &testSupervisor{},
		garbageBlock:   true,
	}
	manager, request, _ := prepareRunningForFinalization(
		t,
		"bounded-finalized-garbage-collection",
		journal,
		supervisor,
	)
	started := time.Now()
	released, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || released.State != StateReleased {
		t.Fatalf("Destroy=%#v err=%v", released, err)
	}
	if elapsed := time.Since(started); elapsed > 2*cleanupHousekeepingTimeout {
		t.Fatalf("garbage collection blocked Destroy for %s", elapsed)
	}
	if !supervisor.finalized || supervisor.garbageCalls != 1 {
		t.Fatalf(
			"finalized=%v garbageCalls=%d",
			supervisor.finalized,
			supervisor.garbageCalls,
		)
	}

	supervisor.garbageBlock = false
	restarted, err := NewManager(Options{
		RuntimeRoot: manager.runtimeRoot,
		Cache:       manager.cache,
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     manager.cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := restarted.Recover(context.Background(), request.ExecutionID)
	if err != nil || recovered.State != StateReleased {
		t.Fatalf("Recover=%#v err=%v", recovered, err)
	}
	if supervisor.finalized || supervisor.garbageCalls != 2 {
		t.Fatalf(
			"finalized=%v garbageCalls=%d",
			supervisor.finalized,
			supervisor.garbageCalls,
		)
	}
}

func TestManagerReadyRejectsBackendMismatchBeforeRootProbe(t *testing.T) {
	cleaner := &readinessCleaner{}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{root: t.TempDir()},
		Journal:     NewMemoryJournal(),
		Supervisor:  &testSupervisor{workspaceBackend: "other-v1"},
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Ready(context.Background()); !errors.Is(err, ErrStrongOwnershipUnavailable) || cleaner.calls != 0 {
		t.Fatalf("Ready error=%v calls=%d", err, cleaner.calls)
	}
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

type concurrentCallbackJIT struct{ value string }

func (jit *concurrentCallbackJIT) Digest() string {
	sum := sha256.Sum256([]byte(jit.value))
	return hex.EncodeToString(sum[:])
}

func (jit *concurrentCallbackJIT) Deliver(deliver func(string) error) error {
	results := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			results <- deliver(jit.value)
		}()
	}
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			return err
		}
	}
	return nil
}

type sequencedWorkspaceCleaner struct {
	strongCleaner
	prepareRef   WorkspaceRef
	observed     []WorkspaceRef
	observations int
}

func (cleaner *sequencedWorkspaceCleaner) PrepareWorkspace(context.Context, *os.Root, string) (WorkspaceRef, error) {
	return cleaner.prepareRef, nil
}

func (cleaner *sequencedWorkspaceCleaner) WorkspaceRef(context.Context, *os.Root, string) (WorkspaceRef, error) {
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

type blockingCleaner struct {
	strongCleaner
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	removes int
}

func newBlockingCleaner() *blockingCleaner {
	return &blockingCleaner{entered: make(chan struct{}), release: make(chan struct{})}
}

func (cleaner *blockingCleaner) RemoveAndVerify(ctx context.Context, root *os.Root, name string) error {
	cleaner.mu.Lock()
	cleaner.removes++
	call := cleaner.removes
	cleaner.mu.Unlock()
	if call != 1 {
		return errors.New("concurrent cleanup owner")
	}
	cleaner.once.Do(func() { close(cleaner.entered) })
	<-cleaner.release
	return cleaner.strongCleaner.RemoveAndVerify(ctx, root, name)
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
func (*barrierSupervisor) WorkspaceBackend() string        { return "test-v1" }

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

func (supervisor *barrierSupervisor) Start(ctx context.Context, request StartRequest) (Process, error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if err := request.VerifyWorkspaceAtExec(ctx); err != nil {
		return Process{Containment: request.Containment}, err
	}
	if err := request.DeliverJIT(requireTestJIT); err != nil {
		return Process{Containment: request.Containment}, err
	}
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

type fencedSupervisor struct {
	mu            sync.Mutex
	startEntered  chan struct{}
	releaseStart  chan struct{}
	enteredOnce   sync.Once
	fenced        map[string]bool
	startAttempts int
	starts        int
	stops         int
}

func newFencedSupervisor() *fencedSupervisor {
	return &fencedSupervisor{
		startEntered: make(chan struct{}),
		releaseStart: make(chan struct{}),
		fenced:       make(map[string]bool),
	}
}

func (*fencedSupervisor) StrongDescendantOwnership() bool { return true }
func (*fencedSupervisor) WorkspaceBackend() string        { return "test-v1" }

func (*fencedSupervisor) PrepareContainment(_ context.Context, executionID string) (ContainmentRef, error) {
	return ContainmentRef{Backend: "test", OwnerID: "unit-" + executionID}, nil
}

func (supervisor *fencedSupervisor) Start(ctx context.Context, request StartRequest) (Process, error) {
	supervisor.mu.Lock()
	supervisor.startAttempts++
	supervisor.mu.Unlock()
	supervisor.enteredOnce.Do(func() { close(supervisor.startEntered) })
	<-supervisor.releaseStart

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if supervisor.fenced[request.Containment.FenceToken] {
		return Process{Containment: request.Containment}, ErrStartFenced
	}
	if err := request.VerifyWorkspaceAtExec(ctx); err != nil {
		return Process{Containment: request.Containment}, err
	}
	if err := request.DeliverJIT(requireTestJIT); err != nil {
		return Process{Containment: request.Containment}, err
	}
	supervisor.starts++
	return Process{PID: supervisor.starts, Containment: request.Containment}, nil
}

func (supervisor *fencedSupervisor) Stop(_ context.Context, process Process) error {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.stops++
	supervisor.fenced[process.Containment.FenceToken] = true
	return nil
}

func (*fencedSupervisor) Alive(Process) (bool, error) { return true, nil }

func (supervisor *fencedSupervisor) counts() (int, int, int) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.startAttempts, supervisor.starts, supervisor.stops
}

type startFirstSupervisor struct {
	mu            sync.Mutex
	linearized    chan struct{}
	releaseStart  chan struct{}
	once          sync.Once
	fenced        map[string]bool
	running       map[string]bool
	request       StartRequest
	startAttempts int
	starts        int
	stops         int
}

func newStartFirstSupervisor() *startFirstSupervisor {
	return &startFirstSupervisor{
		linearized:   make(chan struct{}),
		releaseStart: make(chan struct{}),
		fenced:       make(map[string]bool),
		running:      make(map[string]bool),
	}
}

func (*startFirstSupervisor) StrongDescendantOwnership() bool { return true }
func (*startFirstSupervisor) WorkspaceBackend() string        { return "test-v1" }

func (*startFirstSupervisor) PrepareContainment(_ context.Context, executionID string) (ContainmentRef, error) {
	return ContainmentRef{Backend: "test", OwnerID: "unit-" + executionID}, nil
}

func (supervisor *startFirstSupervisor) Start(ctx context.Context, request StartRequest) (Process, error) {
	supervisor.mu.Lock()
	supervisor.startAttempts++
	if supervisor.fenced[request.Containment.FenceToken] {
		supervisor.mu.Unlock()
		return Process{Containment: request.Containment}, ErrStartFenced
	}
	if err := request.VerifyWorkspaceAtExec(ctx); err != nil {
		supervisor.mu.Unlock()
		return Process{Containment: request.Containment}, err
	}
	if err := request.DeliverJIT(requireTestJIT); err != nil {
		supervisor.mu.Unlock()
		return Process{Containment: request.Containment}, err
	}
	supervisor.starts++
	pid := supervisor.starts
	supervisor.running[request.Containment.FenceToken] = true
	supervisor.request = request
	supervisor.once.Do(func() { close(supervisor.linearized) })
	supervisor.mu.Unlock()

	<-supervisor.releaseStart
	return Process{PID: pid, Containment: request.Containment}, nil
}

func (supervisor *startFirstSupervisor) Stop(_ context.Context, process Process) error {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.stops++
	supervisor.fenced[process.Containment.FenceToken] = true
	delete(supervisor.running, process.Containment.FenceToken)
	return nil
}

func (supervisor *startFirstSupervisor) Alive(process Process) (bool, error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.running[process.Containment.FenceToken], nil
}

func (supervisor *startFirstSupervisor) counts() (int, int, int, int, StartRequest) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.startAttempts, supervisor.starts, supervisor.stops, len(supervisor.running), supervisor.request
}

func (cleaner *controlledCleaner) RemoveAndVerify(ctx context.Context, root *os.Root, name string) error {
	cleaner.removes++
	if cleaner.removeErr != nil {
		return cleaner.removeErr
	}
	return cleaner.strongCleaner.RemoveAndVerify(ctx, root, name)
}

func requireTestJIT(value string) error {
	if value == "" {
		return errors.New("empty test JIT")
	}
	return nil
}

func TestJITCallbackReplayStopsSingleProcess(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := &testSupervisor{materializeOnStart: true}
	journal := NewMemoryJournal()
	runtimeRoot := t.TempDir()
	manager, err := NewManager(Options{RuntimeRoot: runtimeRoot, Cache: testCache{content}, Journal: journal, Supervisor: supervisor, Cleaner: strongCleaner{}})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "callback-replay", Package: pkg}
	_, err = manager.EnsureRunning(context.Background(), Start{Preparation: request, JIT: &callbackJIT{calls: 2, value: "canary"}})
	if !errors.Is(err, ErrStartFailed) || supervisor.starts != 1 || supervisor.stops != 1 {
		t.Fatalf("result=%v starts=%d stops=%d", err, supervisor.starts, supervisor.stops)
	}
	record, _, _ := journal.Load(context.Background(), request.ExecutionID)
	if record.State != StateFailed || record.JITDigest != "" {
		t.Fatalf("record = %#v", record)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed start root remained: %v", statErr)
	}
}

func TestConcurrentJITCallbacksStillStartAtMostOneProcess(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := &testSupervisor{materializeOnStart: true}
	journal := NewMemoryJournal()
	runtimeRoot := t.TempDir()
	manager, err := NewManager(Options{RuntimeRoot: runtimeRoot, Cache: testCache{content}, Journal: journal, Supervisor: supervisor, Cleaner: strongCleaner{}})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "concurrent-jit-callback", Package: pkg}
	_, err = manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &concurrentCallbackJIT{value: "canary"},
	})
	if !errors.Is(err, ErrStartFailed) || supervisor.starts != 1 || supervisor.stops != 1 {
		t.Fatalf("result=%v starts=%d stops=%d", err, supervisor.starts, supervisor.stops)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateFailed || record.JITDigest != "" {
		t.Fatalf("record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed concurrent callback root remained: %v", statErr)
	}
}

func TestSupervisorCannotConsumeOneJobJITTwice(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := &testSupervisor{materializeOnStart: true, deliverTwice: true}
	journal := NewMemoryJournal()
	runtimeRoot := t.TempDir()
	manager, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "jit-consumed-twice", Package: pkg}
	state, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	})
	if !errors.Is(err, ErrStartFailed) || state.State != StateFailed {
		t.Fatalf("EnsureRunning = %#v, %v", state, err)
	}
	if supervisor.jitDeliveries != 1 || supervisor.starts != 0 || supervisor.stops != 1 {
		t.Fatalf(
			"deliveries=%d starts=%d stops=%d",
			supervisor.jitDeliveries,
			supervisor.starts,
			supervisor.stops,
		)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed JIT root remained: %v", statErr)
	}
}

func TestSupervisorSuccessWithoutJITDeliveryStopsAndCleansRuntime(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := &testSupervisor{skipJITDelivery: true}
	journal := NewMemoryJournal()
	runtimeRoot := t.TempDir()
	manager, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "jit-not-delivered", Package: pkg}
	state, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	})
	if !errors.Is(err, ErrStartFailed) || state.State != StateFailed {
		t.Fatalf("EnsureRunning = %#v, %v", state, err)
	}
	if supervisor.jitDeliveries != 0 || supervisor.starts != 1 || supervisor.stops != 1 {
		t.Fatalf(
			"deliveries=%d starts=%d stops=%d",
			supervisor.jitDeliveries,
			supervisor.starts,
			supervisor.stops,
		)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateFailed || record.JITDigest != "" {
		t.Fatalf("record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed JIT root remained: %v", statErr)
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
	originalRef := WorkspaceRef{Backend: "test-v1", OwnerID: "test:original-workspace"}
	cleaner := &sequencedWorkspaceCleaner{
		prepareRef: originalRef,
		observed: []WorkspaceRef{
			originalRef,
			{Backend: "test-v1", OwnerID: "test:replacement-workspace"},
		},
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
	originalRef := WorkspaceRef{Backend: "test-v1", OwnerID: "test:original-workspace"}
	cleaner := &sequencedWorkspaceCleaner{
		prepareRef: originalRef,
		observed: []WorkspaceRef{
			originalRef,
			originalRef,
			{Backend: "test-v1", OwnerID: "test:replacement-workspace"},
		},
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
	if supervisor.starts != 1 || supervisor.stops != 1 || !validWorkspaceRef(supervisor.startRequest.WorkspaceRef) {
		t.Fatalf("starts=%d stops=%d request=%#v", supervisor.starts, supervisor.stops, supervisor.startRequest)
	}
}

func TestSupervisorRechecksTypedWorkspaceIdentityAtExecBoundary(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	originalRef := WorkspaceRef{Backend: "test-v1", OwnerID: "test:original-workspace"}
	cleaner := &sequencedWorkspaceCleaner{
		prepareRef: originalRef,
		observed: []WorkspaceRef{
			originalRef,
			originalRef,
			{Backend: "test-v1", OwnerID: "test:replacement-workspace"},
		},
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
	request := Preparation{ExecutionID: "workspace-replaced-at-exec", Package: pkg}
	state, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	})
	if !errors.Is(err, ErrQuarantined) || !state.Quarantined {
		t.Fatalf("EnsureRunning = %#v, %v", state, err)
	}
	if cleaner.observations != 3 || supervisor.starts != 0 || supervisor.stops != 1 {
		t.Fatalf("observations=%d starts=%d stops=%d", cleaner.observations, supervisor.starts, supervisor.stops)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateCleanupFailed || !record.Tombstone {
		t.Fatalf("quarantine record = %#v, found=%v, err=%v", record, found, loadErr)
	}
}

func TestDestroyStopsRunningContainmentBeforeWorkspaceMismatchQuarantine(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	originalRef := WorkspaceRef{Backend: "test-v1", OwnerID: "test:original-workspace"}
	cleaner := &sequencedWorkspaceCleaner{
		prepareRef: originalRef,
		observed: []WorkspaceRef{
			originalRef,
			originalRef,
			originalRef,
			{Backend: "test-v1", OwnerID: "test:replacement-workspace"},
		},
	}
	supervisor := &testSupervisor{}
	journal := NewMemoryJournal()
	runtimeRoot := t.TempDir()
	manager, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "destroy-running-workspace-mismatch", Package: pkg}
	if running, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	}); err != nil || !running.Running {
		t.Fatalf("EnsureRunning = %#v, %v", running, err)
	}
	state, err := manager.Destroy(context.Background(), request.ExecutionID)
	if !errors.Is(err, ErrQuarantined) || !state.Quarantined {
		t.Fatalf("Destroy = %#v, %v", state, err)
	}
	if supervisor.stops != 1 || len(supervisor.stopped) != 1 || supervisor.stopped[0].Containment.FenceToken == "" {
		t.Fatalf("stopped = %#v", supervisor.stopped)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateCleanupFailed || !record.Tombstone {
		t.Fatalf("quarantine record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); statErr != nil {
		t.Fatalf("mismatched workspace was removed: %v", statErr)
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

func TestRestartedManagerRecoversRunningContainmentAndCleansIt(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	journal := NewMemoryJournal()
	supervisor := &testSupervisor{}
	first, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "restart-running-adoption", Package: pkg}
	if _, err := first.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "restart-jit"},
	}); err != nil {
		t.Fatal(err)
	}
	runningRecord, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found || runningRecord.State != StateRunning {
		t.Fatalf("running record=%#v found=%v err=%v", runningRecord, found, err)
	}

	restarted, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := restarted.Recover(context.Background(), request.ExecutionID)
	if err != nil || recovered.State != StateRunning {
		t.Fatalf("Recover = %#v, %v", recovered, err)
	}
	expectedProcess := Process{PID: runningRecord.PID, Containment: runningRecord.Containment}
	if len(supervisor.observed) != 1 || supervisor.observed[0] != expectedProcess {
		t.Fatalf("adopted process=%#v want=%#v", supervisor.observed, expectedProcess)
	}
	if _, err := restarted.Wait(context.Background(), request.ExecutionID); err != nil {
		t.Fatalf("Wait after recovery: %v", err)
	}
	released, err := restarted.Destroy(context.Background(), request.ExecutionID)
	if err != nil || released.State != StateReleased {
		t.Fatalf("Destroy after recovery = %#v, %v", released, err)
	}
	if supervisor.starts != 1 || supervisor.stops != 1 || supervisor.waits != 1 {
		t.Fatalf("starts=%d stops=%d waits=%d", supervisor.starts, supervisor.stops, supervisor.waits)
	}
}

func TestLaunchAckLossStartingJournalDestroysWithoutReplayingJIT(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	journal := NewMemoryJournal()
	supervisor := &testSupervisor{}
	first, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "restart-starting-cleanup", Package: pkg}
	if _, err := first.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	record, found, err := journal.Load(context.Background(), request.ExecutionID)
	if err != nil || !found {
		t.Fatalf("Load prepared = %#v, %v", record, err)
	}
	jitDigest := sha256.Sum256([]byte("lost-one-shot-jit"))
	record.State = StateStarting
	record.JITDigest = hex.EncodeToString(jitDigest[:])
	record.Containment = ContainmentRef{
		Backend:    "test",
		OwnerID:    "unit-" + request.ExecutionID,
		FenceToken: strings.Repeat("a", 32),
	}
	// Starting is the journal boundary when the root Helper may have launched and
	// durably marked the listener but the Agent lost its PID ACK. Recovery must
	// destroy that exact containment instead of requesting another one-shot JIT.
	mutation, err := newOpaqueToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, swapped, err := journal.CompareAndSwap(
		context.Background(), request.ExecutionID, record.Revision, mutation, record.Record,
	); err != nil || !swapped {
		t.Fatalf("seed Starting swapped=%v err=%v", swapped, err)
	}

	restarted, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	released, err := restarted.Recover(context.Background(), request.ExecutionID)
	if err != nil || released.State != StateReleased {
		t.Fatalf("Recover Starting = %#v, %v", released, err)
	}
	if supervisor.starts != 0 || supervisor.jitDeliveries != 0 || supervisor.stops != 1 {
		t.Fatalf("starts=%d deliveries=%d stops=%d", supervisor.starts, supervisor.jitDeliveries, supervisor.stops)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("recovered Starting workspace remained: %v", statErr)
	}
}

func TestDestroyFencesInFlightStartBeforeReleasingWorkspace(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	journal := NewMemoryJournal()
	supervisor := newFencedSupervisor()
	managerA, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	managerB, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "destroy-fences-start", Package: pkg}
	if _, err := managerA.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	startResult := make(chan error, 1)
	go func() {
		_, startErr := managerA.EnsureRunning(context.Background(), Start{
			Preparation: request,
			JIT:         &callbackJIT{calls: 1, value: "jit-value"},
		})
		startResult <- startErr
	}()
	<-supervisor.startEntered

	released, err := managerB.Destroy(context.Background(), request.ExecutionID)
	if err != nil || released.State != StateReleased {
		t.Fatalf("Destroy = %#v, %v", released, err)
	}
	close(supervisor.releaseStart)
	if startErr := <-startResult; !errors.Is(startErr, ErrReconciliationRequired) {
		t.Fatalf("in-flight EnsureRunning error = %v", startErr)
	}

	attempts, starts, stops := supervisor.counts()
	if attempts != 1 || starts != 0 || stops != 2 {
		t.Fatalf("start attempts=%d starts=%d stops=%d", attempts, starts, stops)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateReleased || record.Revision != 5 {
		t.Fatalf("released record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("released workspace remained: %v", statErr)
	}
}

func TestDestroyFencesInFlightStartBeforeWorkspaceMismatchQuarantine(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	originalRef := WorkspaceRef{Backend: "test-v1", OwnerID: "test:original-workspace"}
	cleaner := &sequencedWorkspaceCleaner{
		prepareRef: originalRef,
		observed: []WorkspaceRef{
			originalRef,
			originalRef,
			originalRef,
			{Backend: "test-v1", OwnerID: "test:replacement-workspace"},
		},
	}
	runtimeRoot := t.TempDir()
	journal := NewMemoryJournal()
	supervisor := newFencedSupervisor()
	managerA, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	managerB, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "destroy-fences-mismatched-start", Package: pkg}
	if _, err := managerA.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	startResult := make(chan error, 1)
	go func() {
		_, startErr := managerA.EnsureRunning(context.Background(), Start{
			Preparation: request,
			JIT:         &callbackJIT{calls: 1, value: "jit-value"},
		})
		startResult <- startErr
	}()
	<-supervisor.startEntered

	state, err := managerB.Destroy(context.Background(), request.ExecutionID)
	if !errors.Is(err, ErrQuarantined) || !state.Quarantined {
		t.Fatalf("Destroy = %#v, %v", state, err)
	}
	close(supervisor.releaseStart)
	if startErr := <-startResult; !errors.Is(startErr, ErrReconciliationRequired) {
		t.Fatalf("in-flight EnsureRunning error = %v", startErr)
	}
	attempts, starts, stops := supervisor.counts()
	if attempts != 1 || starts != 0 || stops != 2 {
		t.Fatalf("start attempts=%d starts=%d stops=%d", attempts, starts, stops)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateCleanupFailed || !record.Tombstone {
		t.Fatalf("quarantine record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); statErr != nil {
		t.Fatalf("mismatched workspace was removed: %v", statErr)
	}
}

func TestDestroyStopsStartThatLinearizedBeforeRunningCommit(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	journal := NewMemoryJournal()
	supervisor := newStartFirstSupervisor()
	managerA, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	managerB, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "destroy-stops-linearized-start", Package: pkg}
	if _, err := managerA.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	startResult := make(chan error, 1)
	go func() {
		_, startErr := managerA.EnsureRunning(context.Background(), Start{
			Preparation: request,
			JIT:         &callbackJIT{calls: 1, value: "jit-value"},
		})
		startResult <- startErr
	}()
	<-supervisor.linearized

	released, err := managerB.Destroy(context.Background(), request.ExecutionID)
	if err != nil || released.State != StateReleased {
		t.Fatalf("Destroy = %#v, %v", released, err)
	}
	close(supervisor.releaseStart)
	if startErr := <-startResult; !errors.Is(startErr, ErrReconciliationRequired) {
		t.Fatalf("in-flight EnsureRunning error = %v", startErr)
	}

	attempts, starts, stops, running, staleRequest := supervisor.counts()
	if attempts != 1 || starts != 1 || stops != 2 || running != 0 {
		t.Fatalf("attempts=%d starts=%d stops=%d running=%d", attempts, starts, stops, running)
	}
	if _, err := supervisor.Start(context.Background(), staleRequest); !errors.Is(err, ErrStartFenced) {
		t.Fatalf("stale Start error = %v", err)
	}
	attempts, starts, stops, running, _ = supervisor.counts()
	if attempts != 2 || starts != 1 || stops != 2 || running != 0 {
		t.Fatalf("after replay attempts=%d starts=%d stops=%d running=%d", attempts, starts, stops, running)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateReleased || record.Revision != 5 {
		t.Fatalf("released record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("released workspace remained: %v", statErr)
	}
}

func TestOnlyCleaningClaimWinnerPerformsDestructiveTeardown(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	journal := NewMemoryJournal()
	cleaner := newBlockingCleaner()
	managerA, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  &testSupervisor{},
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	managerB, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  &testSupervisor{},
		Cleaner:     cleaner,
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "single-cleanup-owner", Package: pkg}
	if _, err := managerA.EnsurePrepared(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	firstResult := make(chan struct {
		state Snapshot
		err   error
	}, 1)
	go func() {
		state, destroyErr := managerA.Destroy(context.Background(), request.ExecutionID)
		firstResult <- struct {
			state Snapshot
			err   error
		}{state: state, err: destroyErr}
	}()
	<-cleaner.entered

	second, err := managerB.Destroy(context.Background(), request.ExecutionID)
	if !errors.Is(err, ErrReconciliationRequired) || second.State != StateCleaning {
		t.Fatalf("second Destroy = %#v, %v", second, err)
	}
	close(cleaner.release)
	first := <-firstResult
	if first.err != nil || first.state.State != StateReleased {
		t.Fatalf("first Destroy = %#v, %v", first.state, first.err)
	}
	cleaner.mu.Lock()
	removes := cleaner.removes
	cleaner.mu.Unlock()
	if removes != 1 {
		t.Fatalf("remove calls = %d", removes)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateReleased {
		t.Fatalf("released record = %#v, found=%v, err=%v", record, found, loadErr)
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
	expected := supervisor.startRequest.Containment
	if expected.Backend != "test" || expected.OwnerID != "unit-"+preparation.ExecutionID || !canonicalFenceToken(expected.FenceToken) {
		t.Fatalf("start containment = %#v", expected)
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

func TestWaitObservesExactRunningContainmentWithoutReleasingWorkspace(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := &testSupervisor{waitGate: make(chan struct{}), waitEntered: make(chan struct{})}
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
	start := Start{
		Preparation: Preparation{ExecutionID: "wait-running-containment", Package: pkg},
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	}
	running, err := manager.EnsureRunning(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}

	waited := make(chan struct {
		snapshot Snapshot
		err      error
	}, 1)
	go func() {
		snapshot, waitErr := manager.Wait(context.Background(), start.ExecutionID)
		waited <- struct {
			snapshot Snapshot
			err      error
		}{snapshot: snapshot, err: waitErr}
	}()
	<-supervisor.waitEntered
	if supervisor.waits != 1 || len(supervisor.waited) != 1 {
		t.Fatalf("completion waits = %d %#v", supervisor.waits, supervisor.waited)
	}
	select {
	case result := <-waited:
		t.Fatalf("Wait returned before containment emptied: %#v, %v", result.snapshot, result.err)
	default:
	}
	close(supervisor.waitGate)
	result := <-waited
	if result.err != nil || result.snapshot != running {
		t.Fatalf("Wait = %#v, %v; want running snapshot %#v", result.snapshot, result.err, running)
	}
	record, found, err := journal.Load(context.Background(), start.ExecutionID)
	if err != nil || !found || record.State != StateRunning {
		t.Fatalf("Wait changed durable lifecycle: %#v, found=%v, err=%v", record, found, err)
	}
	if supervisor.waited[0].Containment != supervisor.startRequest.Containment {
		t.Fatalf("waited containment = %#v, start = %#v", supervisor.waited[0].Containment, supervisor.startRequest.Containment)
	}
}

func TestWaitCancellationLeavesRunningExecutionForReconciliation(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := &testSupervisor{waitGate: make(chan struct{}), waitEntered: make(chan struct{})}
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
	start := Start{
		Preparation: Preparation{ExecutionID: "wait-context-cancel", Package: pkg},
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	}
	if _, err := manager.EnsureRunning(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan struct {
		snapshot Snapshot
		err      error
	}, 1)
	go func() {
		snapshot, waitErr := manager.Wait(ctx, start.ExecutionID)
		result <- struct {
			snapshot Snapshot
			err      error
		}{snapshot: snapshot, err: waitErr}
	}()
	<-supervisor.waitEntered
	cancel()
	waitResult := <-result
	snapshot, err := waitResult.snapshot, waitResult.err
	if !errors.Is(err, context.Canceled) || snapshot.State != StateRunning {
		t.Fatalf("Wait = %#v, %v", snapshot, err)
	}
	record, found, loadErr := journal.Load(context.Background(), start.ExecutionID)
	if loadErr != nil || !found || record.State != StateRunning || record.Tombstone {
		t.Fatalf("cancelled wait mutated journal: %#v, found=%v, err=%v", record, found, loadErr)
	}
}

type supervisorWithoutCompletionWait struct {
	Supervisor
}

func TestWaitFailsClosedWhenPlatformCannotObserveDescendantCompletion(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	platform := &testSupervisor{}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       testCache{content},
		Journal:     NewMemoryJournal(),
		Supervisor:  supervisorWithoutCompletionWait{Supervisor: platform},
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	start := Start{
		Preparation: Preparation{ExecutionID: "wait-unsupported-platform", Package: pkg},
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	}
	if _, err := manager.EnsureRunning(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	snapshot, err := manager.Wait(context.Background(), start.ExecutionID)
	if !errors.Is(err, ErrStrongOwnershipUnavailable) || snapshot.State != StateRunning {
		t.Fatalf("Wait = %#v, %v", snapshot, err)
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
	record.Containment = ContainmentRef{Backend: "test", OwnerID: "durable-owner", FenceToken: strings.Repeat("a", 32)}
	record, swapped, err := journal.CompareAndSwap(context.Background(), record.ExecutionID, record.Revision, strings.Repeat("b", 32), record.Record)
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

func (cache *countingCache) Ensure(context.Context, Package) (PreparedPackage, error) {
	cache.calls++
	return nil, errors.New("unexpected cache call")
}

func TestWorkspaceBackendMismatchFailsBeforePreparationOrJIT(t *testing.T) {
	cache := &countingCache{}
	journal := NewMemoryJournal()
	supervisor := &testSupervisor{workspaceBackend: "test-v2"}
	manager, err := NewManager(Options{
		RuntimeRoot: t.TempDir(),
		Cache:       cache,
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "workspace-backend-mismatch", Package: pkg}
	jit := &callbackJIT{calls: 1, value: "jit-value"}
	if _, err := manager.EnsureRunning(context.Background(), Start{Preparation: request, JIT: jit}); !errors.Is(err, ErrStrongOwnershipUnavailable) {
		t.Fatalf("EnsureRunning error = %v", err)
	}
	if cache.calls != 0 || supervisor.prepareCalls != 0 || supervisor.starts != 0 || jit.deliveries != 0 {
		t.Fatalf(
			"cache=%d prepares=%d starts=%d deliveries=%d",
			cache.calls,
			supervisor.prepareCalls,
			supervisor.starts,
			jit.deliveries,
		)
	}
	if _, found, loadErr := journal.Load(context.Background(), request.ExecutionID); loadErr != nil || found {
		t.Fatalf("journal found=%v err=%v", found, loadErr)
	}
}

type brokenArchiveCache struct{}

type brokenPreparedPackage struct{}

func (brokenPreparedPackage) Materialize(*os.Root) error { return ErrPackageIntegrity }
func (brokenPreparedPackage) Close() error               { return nil }

func (brokenArchiveCache) Ensure(context.Context, Package) (PreparedPackage, error) {
	return brokenPreparedPackage{}, nil
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
	if _, created, err := journal.Create(context.Background(), strings.Repeat("a", 32), record); err != nil || !created {
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
func (journal *failOnCommitJournal) Create(ctx context.Context, mutationToken string, record Record) (VersionedRecord, bool, error) {
	journal.commits++
	if journal.commits == journal.failAt {
		return VersionedRecord{}, false, errors.New("injected journal failure")
	}
	return journal.inner.Create(ctx, mutationToken, record)
}
func (journal *failOnCommitJournal) CompareAndSwap(ctx context.Context, executionID string, expectedRevision uint64, mutationToken string, record Record) (VersionedRecord, bool, error) {
	journal.commits++
	if journal.commits == journal.failAt {
		return VersionedRecord{}, false, errors.New("injected journal failure")
	}
	return journal.inner.CompareAndSwap(ctx, executionID, expectedRevision, mutationToken, record)
}

type writeThenErrorJournal struct {
	inner                *MemoryJournal
	commits              int
	failAt               int
	loadFailuresAfterCAS int
	pendingLoadFailures  int
}

func (journal *writeThenErrorJournal) Load(ctx context.Context, executionID string) (VersionedRecord, bool, error) {
	if journal.pendingLoadFailures > 0 {
		journal.pendingLoadFailures--
		return VersionedRecord{}, false, errors.New("injected post-commit read failure")
	}
	return journal.inner.Load(ctx, executionID)
}

func (journal *writeThenErrorJournal) Create(ctx context.Context, mutationToken string, record Record) (VersionedRecord, bool, error) {
	journal.commits++
	return journal.inner.Create(ctx, mutationToken, record)
}

func (journal *writeThenErrorJournal) CompareAndSwap(ctx context.Context, executionID string, expectedRevision uint64, mutationToken string, record Record) (VersionedRecord, bool, error) {
	journal.commits++
	updated, swapped, err := journal.inner.CompareAndSwap(ctx, executionID, expectedRevision, mutationToken, record)
	if err == nil && swapped && journal.commits == journal.failAt {
		journal.pendingLoadFailures = journal.loadFailuresAfterCAS
		return updated, true, errors.New("injected write-then-error")
	}
	return updated, swapped, err
}

type foreignMutationJournal struct {
	inner    *MemoryJournal
	commits  int
	failAt   int
	conflict bool
}

func (journal *foreignMutationJournal) Load(ctx context.Context, executionID string) (VersionedRecord, bool, error) {
	return journal.inner.Load(ctx, executionID)
}

func (journal *foreignMutationJournal) Create(ctx context.Context, mutationToken string, record Record) (VersionedRecord, bool, error) {
	journal.commits++
	return journal.inner.Create(ctx, mutationToken, record)
}

func (journal *foreignMutationJournal) CompareAndSwap(ctx context.Context, executionID string, expectedRevision uint64, mutationToken string, record Record) (VersionedRecord, bool, error) {
	journal.commits++
	if journal.commits == journal.failAt {
		if journal.conflict {
			record.PID = 999
			record.Containment.FenceToken = strings.Repeat("e", 32)
		}
		updated, swapped, err := journal.inner.CompareAndSwap(
			ctx,
			executionID,
			expectedRevision,
			strings.Repeat("f", 32),
			record,
		)
		if err != nil || !swapped {
			return updated, swapped, err
		}
		return updated, true, errors.New("injected foreign mutation")
	}
	return journal.inner.CompareAndSwap(ctx, executionID, expectedRevision, mutationToken, record)
}

func TestIdenticalForeignMutationIsNeverClaimedAsOwnCommit(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := &foreignMutationJournal{inner: NewMemoryJournal(), failAt: 4}
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
	request := Preparation{ExecutionID: "foreign-mutation", Package: pkg}
	jit := &callbackJIT{calls: 1, value: "jit-value"}
	if _, err := manager.EnsureRunning(context.Background(), Start{Preparation: request, JIT: jit}); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("EnsureRunning error = %v", err)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateFailed || record.Revision != 6 || record.MutationToken == strings.Repeat("f", 32) {
		t.Fatalf("foreign record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	replayJIT := &callbackJIT{calls: 1, value: "jit-value"}
	if _, err := manager.EnsureRunning(context.Background(), Start{Preparation: request, JIT: replayJIT}); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("replay error = %v", err)
	}
	if supervisor.starts != 1 || supervisor.stops != 1 || jit.deliveries != 1 || replayJIT.deliveries != 0 {
		t.Fatalf(
			"starts=%d stops=%d first deliveries=%d replay deliveries=%d",
			supervisor.starts,
			supervisor.stops,
			jit.deliveries,
			replayJIT.deliveries,
		)
	}
}

func TestConflictingForeignRuntimeIsNotCleanedAsOwnProcess(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := &foreignMutationJournal{inner: NewMemoryJournal(), failAt: 4, conflict: true}
	supervisor := &testSupervisor{materializeOnStart: true}
	runtimeRoot := t.TempDir()
	manager, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "conflicting-foreign-runtime", Package: pkg}
	if _, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	}); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("EnsureRunning error = %v", err)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateRunning || record.PID != 999 || record.Containment.FenceToken != strings.Repeat("e", 32) {
		t.Fatalf("foreign record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	if supervisor.starts != 1 || supervisor.stops != 1 {
		t.Fatalf("starts=%d stops=%d", supervisor.starts, supervisor.stops)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); statErr != nil {
		t.Fatalf("foreign runtime root was removed: %v", statErr)
	}
}

func TestWriteThenErrorCASIsResolvedByMutationToken(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := &writeThenErrorJournal{inner: NewMemoryJournal(), failAt: 4}
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
	request := Preparation{ExecutionID: "write-then-error-resolved", Package: pkg}
	running, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	})
	if err != nil || !running.Running || supervisor.starts != 1 || supervisor.stops != 0 {
		t.Fatalf("EnsureRunning=%#v err=%v starts=%d stops=%d", running, err, supervisor.starts, supervisor.stops)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateRunning || record.Revision != 4 {
		t.Fatalf("running record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	if replay, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	}); err != nil || !replay.Running {
		t.Fatalf("owned replay = %#v, %v", replay, err)
	}
}

func TestAmbiguousRunningCommitAndStopFailureReloadLatestBeforeQuarantine(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := &writeThenErrorJournal{
		inner:                NewMemoryJournal(),
		failAt:               4,
		loadFailuresAfterCAS: 1,
	}
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
	request := Preparation{ExecutionID: "ambiguous-running-stop-failure", Package: pkg}
	state, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	})
	if !errors.Is(err, ErrQuarantined) || !state.Quarantined || supervisor.starts != 1 || supervisor.stops != 1 {
		t.Fatalf("EnsureRunning=%#v err=%v starts=%d stops=%d", state, err, supervisor.starts, supervisor.stops)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateCleanupFailed || !record.Tombstone || record.Revision != 5 {
		t.Fatalf("quarantine record = %#v, found=%v, err=%v", record, found, loadErr)
	}
}

func TestAmbiguousRunningCommitWithSuccessfulStopCleansDurableRuntime(t *testing.T) {
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := &writeThenErrorJournal{
		inner:                NewMemoryJournal(),
		failAt:               4,
		loadFailuresAfterCAS: 1,
	}
	supervisor := &testSupervisor{materializeOnStart: true}
	runtimeRoot := t.TempDir()
	manager, err := NewManager(Options{
		RuntimeRoot: runtimeRoot,
		Cache:       testCache{content},
		Journal:     journal,
		Supervisor:  supervisor,
		Cleaner:     strongCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pkg, _ := OfficialPackage(CurrentPlatform())
	request := Preparation{ExecutionID: "ambiguous-running-stop-success", Package: pkg}
	state, err := manager.EnsureRunning(context.Background(), Start{
		Preparation: request,
		JIT:         &callbackJIT{calls: 1, value: "jit-value"},
	})
	if !errors.Is(err, ErrJournal) || state.State != StateFailed || supervisor.starts != 1 || supervisor.stops != 1 {
		t.Fatalf("EnsureRunning=%#v err=%v starts=%d stops=%d", state, err, supervisor.starts, supervisor.stops)
	}
	record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
	if loadErr != nil || !found || record.State != StateFailed || record.Revision != 6 {
		t.Fatalf("failed record = %#v, found=%v, err=%v", record, found, loadErr)
	}
	if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed runtime remained: %v", statErr)
	}
}

func TestRunningCommitFailureStopsProcessAndCleansRuntime(t *testing.T) {
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
	if err != nil || !found || record.State != StateFailed || record.JITDigest != "" || record.Containment != (ContainmentRef{}) {
		t.Fatalf("durable failed record = %#v, found=%v, err=%v", record, found, err)
	}
	journal.failAt = 0
	failed, err := manager.Destroy(context.Background(), request.ExecutionID)
	if err != nil || failed.State != StateFailed || supervisor.stops != 1 {
		t.Fatalf("idempotent Destroy = %#v, err=%v, stops=%d", failed, err, supervisor.stops)
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

func TestDestroyResolvesTerminalJournalFailureAfterVerifiedCleanup(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		journal func() Journal
	}{
		{
			name: "write rejected before commit",
			journal: func() Journal {
				return &failOnCommitJournal{inner: NewMemoryJournal(), failAt: 4}
			},
		},
		{
			name: "write committed before response and first reload failed",
			journal: func() Journal {
				return &writeThenErrorJournal{
					inner:                NewMemoryJournal(),
					failAt:               4,
					loadFailuresAfterCAS: 1,
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			content := t.TempDir()
			if err := os.WriteFile(filepath.Join(content, "run.sh"), []byte("x"), 0o700); err != nil {
				t.Fatal(err)
			}
			journal := testCase.journal()
			runtimeRoot := t.TempDir()
			manager, err := NewManager(Options{
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
			request := Preparation{ExecutionID: "terminal-cleanup-journal-failure", Package: pkg}
			if _, err := manager.EnsurePrepared(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			released, err := manager.Destroy(context.Background(), request.ExecutionID)
			if err != nil || released.State != StateReleased {
				t.Fatalf("Destroy = %#v, %v", released, err)
			}
			record, found, loadErr := journal.Load(context.Background(), request.ExecutionID)
			if loadErr != nil || !found || record.State != StateReleased {
				t.Fatalf("released record = %#v, found=%v, err=%v", record, found, loadErr)
			}
			if _, statErr := os.Lstat(filepath.Join(runtimeRoot, "executions", executionRootName(request.ExecutionID))); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("released workspace remained: %v", statErr)
			}
		})
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
