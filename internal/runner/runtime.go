package runner

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

const cleanupHousekeepingTimeout = time.Second

// JITConfig is the narrow secret handoff used by the runtime. github.JITConfig
// satisfies it, while this package never gains an accessor, formatter, logger,
// or journal field for the encoded value.
type JITConfig interface {
	Digest() string
	Deliver(func(string) error) error
}

type Preparation struct {
	ExecutionID   string
	Package       Package
	DisableUpdate bool
}

type Start struct {
	Preparation
	JIT JITConfig
}

type Snapshot struct {
	ExecutionID string
	State       State
	Prepared    bool
	Running     bool
	Quarantined bool
}

// Cleaner allows a platform adapter to implement deletion and absence checks.
// It receives a directory handle plus a hashed descendant name, never a raw
// execution ID or secret-bearing absolute path.
type Cleaner interface {
	RemoveAndVerify(context.Context, *os.Root, string) error
	StrongWorkspaceOwnership() bool
	// WorkspaceBackend identifies the versioned identity encoding and verifier,
	// not the process containment backend.
	WorkspaceBackend() string
	ValidateRuntimeRoot(context.Context, string) error
	// PrepareWorkspace may apply the platform runner identity and returns the
	// durable identity observation that later cleanup must match.
	PrepareWorkspace(context.Context, *os.Root, string) (WorkspaceRef, error)
	// WorkspaceRef observes only; it must never repair ownership during cleanup.
	WorkspaceRef(context.Context, *os.Root, string) (WorkspaceRef, error)
}

type PackageCache interface {
	Ensure(context.Context, Package) (PreparedPackage, error)
}

// PreparedPackage is a capability for one verified package object. Production
// Cache implementations keep the verified archive file descriptor open so
// materialization never resolves a path after validation.
type PreparedPackage interface {
	Materialize(*os.Root) error
	Close() error
}

type rootCleaner struct{}

func (rootCleaner) StrongWorkspaceOwnership() bool { return false }
func (rootCleaner) WorkspaceBackend() string       { return "" }
func (rootCleaner) ValidateRuntimeRoot(context.Context, string) error {
	return ErrStrongOwnershipUnavailable
}
func (rootCleaner) PrepareWorkspace(context.Context, *os.Root, string) (WorkspaceRef, error) {
	return WorkspaceRef{}, ErrStrongOwnershipUnavailable
}
func (rootCleaner) WorkspaceRef(context.Context, *os.Root, string) (WorkspaceRef, error) {
	return WorkspaceRef{}, ErrStrongOwnershipUnavailable
}

func (rootCleaner) RemoveAndVerify(_ context.Context, root *os.Root, name string) error {
	// Absence before cleanup is not success: a renamed root can still contain
	// credentials. Platform adapters add durable file identity in task-007/008.
	if _, err := root.Lstat(name); err != nil {
		return ErrCleanupFailed
	}
	if err := root.RemoveAll(name); err != nil {
		return ErrCleanupFailed
	}
	if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		return ErrCleanupFailed
	}
	return nil
}

type Options struct {
	RuntimeRoot string
	Cache       PackageCache
	Journal     Journal
	Supervisor  Supervisor
	Cleaner     Cleaner
}

// Manager is the execution-ID keyed lifecycle API. It serializes local calls;
// journal records make replays safe across manager recreation. A crash in the
// start-before-PID-persist window is deliberately reconciliation-required so it
// cannot create a second runner merely to regain liveness.
type Manager struct {
	runtimeRoot  string
	cache        PackageCache
	journal      Journal
	supervisor   Supervisor
	cleaner      Cleaner
	owned        map[string]runtimeOwnership
	cleanupOwned map[string]uint64
	mu           sync.Mutex
}

// Ready revalidates the complete platform admission boundary without creating
// runtime state. Platform implementations may use this call for an actual
// helper/socket round trip; a cached startup result is never sufficient.
func (m *Manager) Ready(ctx context.Context) error {
	if m == nil || ctx == nil || m.cleaner == nil || m.supervisor == nil {
		return ErrStrongOwnershipUnavailable
	}
	if err := ctx.Err(); err != nil {
		return ErrStrongOwnershipUnavailable
	}
	if _, ok := m.supervisor.(CompletionWaiter); !ok {
		return ErrStrongOwnershipUnavailable
	}
	if _, ok := m.supervisor.(CleanupFinalizer); !ok {
		return ErrStrongOwnershipUnavailable
	}
	cleanerBackend := m.cleaner.WorkspaceBackend()
	if !m.cleaner.StrongWorkspaceOwnership() ||
		!m.supervisor.StrongDescendantOwnership() ||
		cleanerBackend == "" ||
		m.supervisor.WorkspaceBackend() != cleanerBackend {
		return ErrStrongOwnershipUnavailable
	}
	if err := m.cleaner.ValidateRuntimeRoot(ctx, m.runtimeRoot); err != nil {
		return ErrStrongOwnershipUnavailable
	}
	return nil
}

type runtimeOwnership struct {
	Revision    uint64
	Containment ContainmentRef
}

// Recover re-establishes one durable runtime's in-process lifecycle owner after
// an Agent restart. It never starts a process or consumes new JIT material.
//
// A persisted Running record is adopted only after the platform proves that the
// exact workspace and containment still exist. Starting and Cleaning records
// are destructive recovery intents: the old one-shot JIT cannot be replayed, so
// recovery fences the containment and converges through verified cleanup.
func (m *Manager) Recover(ctx context.Context, executionID string) (Snapshot, error) {
	if executionID == "" {
		return Snapshot{}, ErrInvalidRequest
	}
	m.mu.Lock()
	record, found, err := m.load(ctx, executionID)
	if err != nil {
		m.mu.Unlock()
		return Snapshot{}, err
	}
	if !found {
		m.mu.Unlock()
		return Snapshot{}, ErrExecutionNotFound
	}
	if !validVersionedRecord(record) {
		m.mu.Unlock()
		return Snapshot{}, ErrReconciliationRequired
	}
	switch record.State {
	case StateReleased:
		m.forget(record.ExecutionID)
		m.forgetCleanup(record.ExecutionID)
		m.mu.Unlock()
		if finalizer, ok := m.supervisor.(CleanupFinalizer); ok &&
			validFencedContainment(record.Containment) {
			// Released is the journal authority that makes tombstone collection
			// safe. Retrying here avoids platform startup guessing whether a
			// finalized record preceded or followed the Released commit.
			m.garbageCollectCleanup(finalizer, Process{Containment: record.Containment})
		}
		return snapshot(record.Record), nil
	case StateFailed:
		m.forget(record.ExecutionID)
		m.forgetCleanup(record.ExecutionID)
		m.mu.Unlock()
		return snapshot(record.Record), nil
	case StateCleanupFailed:
		m.forget(record.ExecutionID)
		m.forgetCleanup(record.ExecutionID)
		m.mu.Unlock()
		return snapshot(record.Record), ErrQuarantined
	case StatePrepared:
		// Prepared contains no JIT material or process. The Agent command journal
		// decides whether it is a completed Prepare or an accepted Start whose
		// secret was lost before delivery.
		m.mu.Unlock()
		return snapshot(record.Record), nil
	case StateRunning:
		if !m.supervisor.StrongDescendantOwnership() ||
			!m.workspaceMatches(ctx, record.Record) ||
			!validFencedContainment(record.Containment) {
			record, err = m.claimRecoveryCleanup(ctx, record)
			if err != nil {
				m.mu.Unlock()
				return snapshot(record.Record), err
			}
			break
		}
		// Alive also validates that the exact durable containment can be opened.
		// A false result is a naturally completed cgroup and remains adoptable:
		// CompletionWaiter will return immediately and the Agent will clean it.
		if _, aliveErr := m.supervisor.Alive(Process{PID: record.PID, Containment: record.Containment}); aliveErr == nil {
			m.remember(record)
			m.mu.Unlock()
			return snapshot(record.Record), nil
		}
		record, err = m.claimRecoveryCleanup(ctx, record)
		if err != nil {
			m.mu.Unlock()
			return snapshot(record.Record), err
		}
	case StatePreparing, StateStarting:
		record, err = m.claimRecoveryCleanup(ctx, record)
		if err != nil {
			m.mu.Unlock()
			return snapshot(record.Record), err
		}
	case StateCleaning:
		// Cleaning is a durable stop-before-delete intent. A restarted Agent may
		// safely take over that idempotent work after revalidating the record.
		m.rememberCleanup(record)
	default:
		m.mu.Unlock()
		return snapshot(record.Record), ErrReconciliationRequired
	}
	m.mu.Unlock()
	return m.Destroy(ctx, executionID)
}

func (m *Manager) claimRecoveryCleanup(ctx context.Context, record VersionedRecord) (VersionedRecord, error) {
	if record.State != StateCleaning {
		record.State = StateCleaning
		updated, err := m.compareAndSwap(ctx, record)
		if err != nil {
			return updated, err
		}
		record = updated
	}
	m.forget(record.ExecutionID)
	m.rememberCleanup(record)
	return record, nil
}

func NewManager(options Options) (*Manager, error) {
	if options.RuntimeRoot == "" || options.Cache == nil || options.Journal == nil {
		return nil, ErrInvalidRequest
	}
	if options.Supervisor == nil {
		options.Supervisor = NewSupervisor()
	}
	if options.Cleaner == nil {
		options.Cleaner = rootCleaner{}
	}
	if err := os.MkdirAll(options.RuntimeRoot, 0o700); err != nil {
		return nil, ErrInvalidRequest
	}
	return &Manager{
		runtimeRoot:  options.RuntimeRoot,
		cache:        options.Cache,
		journal:      options.Journal,
		supervisor:   options.Supervisor,
		cleaner:      options.Cleaner,
		owned:        make(map[string]runtimeOwnership),
		cleanupOwned: make(map[string]uint64),
	}, nil
}

func (m *Manager) EnsurePrepared(ctx context.Context, request Preparation) (Snapshot, error) {
	if err := validatePreparation(request); err != nil {
		return Snapshot{}, err
	}
	if !m.cleaner.StrongWorkspaceOwnership() {
		// Creating a root that cannot later be identity-checked would turn a
		// cancelled preparation into an unrecoverable credential workspace.
		return Snapshot{}, ErrStrongOwnershipUnavailable
	}
	if m.cleaner.WorkspaceBackend() == "" {
		return Snapshot{}, ErrStrongOwnershipUnavailable
	}
	if err := m.cleaner.ValidateRuntimeRoot(ctx, m.runtimeRoot); err != nil {
		return Snapshot{}, ErrStrongOwnershipUnavailable
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	record, found, err := m.load(ctx, request.ExecutionID)
	if err != nil {
		return Snapshot{}, err
	}
	digest := preparationDigest(request)
	if found {
		if !validVersionedRecord(record) {
			return Snapshot{}, ErrReconciliationRequired
		}
		if record.SpecDigest != digest {
			return Snapshot{}, ErrExecutionConflict
		}
		if activeState(record.State) && !m.workspaceMatches(ctx, record.Record) {
			return snapshot(record.Record), ErrReconciliationRequired
		}
		return snapshot(record.Record), stateError(record.Record)
	}
	preparedPackage, err := m.cache.Ensure(ctx, request.Package)
	if err != nil {
		return Snapshot{}, err
	}
	if preparedPackage == nil {
		return Snapshot{}, ErrPackageIntegrity
	}
	defer preparedPackage.Close()
	rootName := executionRootName(request.ExecutionID)
	record, created, err := m.create(ctx, Record{
		ExecutionID: request.ExecutionID,
		SpecDigest:  digest,
		State:       StatePreparing,
		RootName:    rootName,
	})
	if err != nil {
		return Snapshot{}, err
	}
	if !created {
		return Snapshot{}, ErrReconciliationRequired
	}
	root, rootName, err := m.makeExecutionRoot(request.ExecutionID)
	if err != nil {
		// A conflicting or inaccessible deterministic root may contain material
		// from a crashed owner. Without a captured identity, absence is unproven.
		return m.quarantine(ctx, record)
	}
	if err := preparedPackage.Materialize(root); err != nil {
		root.Close()
		if removeErr := m.removeRoot(ctx, rootName); removeErr != nil {
			return m.quarantine(ctx, record)
		}
		return m.failPreparation(ctx, record, err)
	}
	if err := root.Close(); err != nil {
		if removeErr := m.removeRoot(ctx, rootName); removeErr != nil {
			return m.quarantine(ctx, record)
		}
		return m.failPreparation(ctx, record, ErrCleanupFailed)
	}
	workspaceRef, err := m.prepareWorkspace(ctx, rootName)
	if err != nil || !validWorkspaceRef(workspaceRef) || workspaceRef.Backend != m.cleaner.WorkspaceBackend() {
		if removeErr := m.removeRoot(ctx, rootName); removeErr != nil {
			return m.quarantine(ctx, record)
		}
		return m.failPreparation(ctx, record, ErrStrongOwnershipUnavailable)
	}
	record.State = StatePrepared
	record.WorkspaceRef = workspaceRef
	record, err = m.compareAndSwap(ctx, record)
	if err != nil {
		// The durable claim may have moved to Cleaning in another manager. Do
		// not race that owner by deleting its workspace from this stale path.
		return Snapshot{}, err
	}
	return snapshot(record.Record), nil
}

func (m *Manager) EnsureRunning(ctx context.Context, request Start) (Snapshot, error) {
	if err := validatePreparation(request.Preparation); err != nil || request.JIT == nil || !canonicalDigest(request.JIT.Digest()) {
		return Snapshot{}, ErrInvalidRequest
	}
	if !m.supervisor.StrongDescendantOwnership() {
		return Snapshot{}, ErrStrongOwnershipUnavailable
	}
	if !m.cleaner.StrongWorkspaceOwnership() {
		return Snapshot{}, ErrStrongOwnershipUnavailable
	}
	if m.cleaner.WorkspaceBackend() == "" || m.supervisor.WorkspaceBackend() != m.cleaner.WorkspaceBackend() {
		// The process boundary must understand exactly the same durable identity
		// encoding before preparation or JIT delivery can have side effects.
		return Snapshot{}, ErrStrongOwnershipUnavailable
	}
	prepared, err := m.EnsurePrepared(ctx, request.Preparation)
	if err != nil {
		return prepared, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	record, _, err := m.load(ctx, request.ExecutionID)
	if err != nil {
		return Snapshot{}, err
	}
	if !validVersionedRecord(record) {
		return Snapshot{}, ErrReconciliationRequired
	}
	// EnsurePrepared releases the manager lock before this point. Re-observe the
	// workspace identity while holding it again so a replaced directory never
	// reaches containment creation or the one-shot JIT handoff.
	if activeState(record.State) && !m.workspaceMatches(ctx, record.Record) {
		return snapshot(record.Record), ErrReconciliationRequired
	}
	jitDigest := request.JIT.Digest()
	switch record.State {
	case StateRunning:
		if record.JITDigest != jitDigest {
			return Snapshot{}, ErrExecutionConflict
		}
		if !m.owns(record) {
			// A recreated or overlapping Manager must reconcile OS state before
			// adopting a persisted Running observation. This also fences an
			// ambiguous commit whose caller never received success.
			return snapshot(record.Record), ErrReconciliationRequired
		}
		alive, err := m.supervisor.Alive(Process{PID: record.PID, Containment: record.Containment})
		if err != nil || !alive {
			return snapshot(record.Record), ErrReconciliationRequired
		}
		return snapshot(record.Record), nil
	case StateStarting, StateCleaning:
		return Snapshot{}, ErrReconciliationRequired
	case StatePrepared:
		// continue below
	default:
		return snapshot(record.Record), ErrExecutionConflict
	}
	containment, err := m.supervisor.PrepareContainment(ctx, record.ExecutionID)
	if err != nil || !validContainment(containment) || containment.FenceToken != "" {
		return Snapshot{}, ErrStrongOwnershipUnavailable
	}
	fenceToken, err := newOpaqueToken()
	if err != nil {
		return Snapshot{}, ErrStrongOwnershipUnavailable
	}
	containment.FenceToken = fenceToken
	record.State = StateStarting
	record.JITDigest = jitDigest
	record.Containment = containment
	record, err = m.compareAndSwap(ctx, record)
	if err != nil {
		// PrepareContainment is required to be deterministic and idempotent for
		// an ExecutionID. A CAS loser must not stop the winner's shared boundary.
		return Snapshot{}, err
	}
	var process Process
	started := false
	workspaceChanged := false
	var supervisorErr error
	var deliveryMu sync.Mutex
	if err := request.JIT.Deliver(func(value string) error {
		// Deliver is an internal one-shot contract, but serialize callbacks here
		// so a buggy concurrent implementation still cannot start two listeners.
		deliveryMu.Lock()
		defer deliveryMu.Unlock()
		if value == "" {
			return ErrInvalidRequest
		}
		actual := sha256.Sum256([]byte(value))
		if hex.EncodeToString(actual[:]) != jitDigest {
			return ErrInvalidRequest
		}
		if started {
			return ErrReconciliationRequired
		}
		// This is the last core-owned observation before the platform start
		// transaction. Supervisor.Start receives the same opaque reference and
		// must verify it again at the exec boundary.
		if !m.workspaceMatches(ctx, record.Record) {
			workspaceChanged = true
			return ErrWorkspaceChanged
		}
		started = true
		startRequest := StartRequest{
			Executable:   runnerExecutable(m.runtimeRoot, record.RootName),
			Directory:    executionPath(m.runtimeRoot, record.RootName),
			Arguments:    runnerArguments(request.DisableUpdate),
			WorkspaceRef: record.WorkspaceRef,
			Containment:  containment,
			jit:          newJITArgument(value),
			verify: func(verifyCtx context.Context) error {
				if !m.workspaceMatches(verifyCtx, record.Record) {
					return ErrWorkspaceChanged
				}
				return nil
			},
		}
		process, supervisorErr = m.supervisor.Start(ctx, startRequest)
		if contractSatisfied := startRequest.finishStart(); supervisorErr == nil && !contractSatisfied {
			// A platform adapter cannot defer or retain the one-job secret past
			// Start. Revoke all request copies before cleanup and fail closed.
			supervisorErr = ErrStartFailed
		}
		return supervisorErr
	}); err != nil {
		if stopErr := m.supervisor.Stop(ctx, Process{PID: process.PID, Containment: containment}); stopErr != nil {
			return m.quarantine(ctx, record)
		}
		if process.Containment != (ContainmentRef{}) && process.Containment != containment {
			return m.quarantine(ctx, record)
		}
		if workspaceChanged || errors.Is(supervisorErr, ErrWorkspaceChanged) {
			return m.quarantine(ctx, record)
		}
		if errors.Is(supervisorErr, ErrStartFenced) {
			return Snapshot{}, ErrReconciliationRequired
		}
		if started {
			return m.failStartedExecution(ctx, record, record.Record, ErrStartFailed)
		}
		record.State = StatePrepared
		record.JITDigest = ""
		record.PID = 0
		record.Containment = ContainmentRef{}
		var saveErr error
		record, saveErr = m.compareAndSwap(ctx, record)
		if saveErr != nil {
			return Snapshot{}, saveErr
		}
		return Snapshot{}, ErrInvalidRequest
	}
	if !started {
		if stopErr := m.supervisor.Stop(ctx, Process{PID: process.PID, Containment: containment}); stopErr != nil {
			return m.quarantine(ctx, record)
		}
		record.State = StatePrepared
		record.JITDigest = ""
		record.PID = 0
		record.Containment = ContainmentRef{}
		var saveErr error
		record, saveErr = m.compareAndSwap(ctx, record)
		if saveErr != nil {
			return m.quarantine(ctx, record)
		}
		return Snapshot{}, ErrInvalidRequest
	}
	if process.PID <= 0 || process.Containment != containment {
		if stopErr := m.supervisor.Stop(ctx, Process{PID: process.PID, Containment: containment}); stopErr != nil {
			return m.quarantine(ctx, record)
		}
		if process.Containment != containment {
			return m.quarantine(ctx, record)
		}
		return m.failStartedExecution(ctx, record, record.Record, ErrStartFailed)
	}
	record.State = StateRunning
	record.PID = process.PID
	record.Containment = process.Containment
	expectedRuntime := record.Record
	record, err = m.compareAndSwap(ctx, record)
	if err != nil {
		// A started process without a durable PID must not be replayed. Stop it
		// now; if that cannot be proven, the caller remains fail-closed.
		if stopErr := m.supervisor.Stop(ctx, process); stopErr != nil {
			return m.quarantine(ctx, record)
		}
		if sameStartedRuntime(record.Record, expectedRuntime) {
			return m.failStartedExecution(ctx, record, expectedRuntime, err)
		}
		return Snapshot{}, err
	}
	m.remember(record)
	return snapshot(record.Record), nil
}

func (m *Manager) Inspect(ctx context.Context, executionID string) (Snapshot, error) {
	if executionID == "" {
		return Snapshot{}, ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	record, found, err := m.load(ctx, executionID)
	if err != nil {
		return Snapshot{}, err
	}
	if !found {
		return Snapshot{}, ErrExecutionNotFound
	}
	if !validVersionedRecord(record) {
		return Snapshot{}, ErrReconciliationRequired
	}
	if activeState(record.State) && !m.workspaceMatches(ctx, record.Record) {
		return snapshot(record.Record), ErrReconciliationRequired
	}
	if record.State == StateRunning {
		if !m.owns(record) {
			return snapshot(record.Record), ErrReconciliationRequired
		}
		alive, observeErr := m.supervisor.Alive(Process{PID: record.PID, Containment: record.Containment})
		if observeErr != nil || !alive {
			return snapshot(record.Record), ErrReconciliationRequired
		}
	}
	return snapshot(record.Record), stateError(record.Record)
}

// Wait observes completion of the exact descendant boundary owned by this
// Manager. It does not mutate the runner journal or release the workspace:
// callers must follow a successful observation with Destroy so cleanup remains
// a separately durable, fail-closed transition.
//
// Agent shutdown cancels the caller context and leaves Running intact for later
// reconciliation. Controller or transport disconnects must not be used as this
// context because an already-running job continues locally.
func (m *Manager) Wait(ctx context.Context, executionID string) (Snapshot, error) {
	if executionID == "" {
		return Snapshot{}, ErrInvalidRequest
	}
	m.mu.Lock()
	record, found, err := m.load(ctx, executionID)
	if err != nil {
		m.mu.Unlock()
		return Snapshot{}, err
	}
	if !found {
		m.mu.Unlock()
		return Snapshot{}, ErrExecutionNotFound
	}
	if !validVersionedRecord(record) {
		m.mu.Unlock()
		return Snapshot{}, ErrReconciliationRequired
	}
	current := snapshot(record.Record)
	if record.State != StateRunning {
		m.mu.Unlock()
		if stateErr := stateError(record.Record); stateErr != nil {
			return current, stateErr
		}
		return current, ErrExecutionConflict
	}
	if !m.owns(record) {
		m.mu.Unlock()
		return current, ErrReconciliationRequired
	}
	waiter, supported := m.supervisor.(CompletionWaiter)
	if !supported || !m.supervisor.StrongDescendantOwnership() {
		m.mu.Unlock()
		return current, ErrStrongOwnershipUnavailable
	}
	process := Process{PID: record.PID, Containment: record.Containment}
	m.mu.Unlock()

	if err := waiter.Wait(ctx, process); err != nil {
		if ctx.Err() != nil {
			return current, ctx.Err()
		}
		// Raw platform errors may carry path or process detail. Preserve only the
		// reconciliation classification at this lifecycle boundary.
		return current, ErrReconciliationRequired
	}
	return current, nil
}

func (m *Manager) Destroy(ctx context.Context, executionID string) (Snapshot, error) {
	if executionID == "" {
		return Snapshot{}, ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	record, found, err := m.load(ctx, executionID)
	if err != nil {
		return Snapshot{}, err
	}
	if !found {
		return Snapshot{}, ErrExecutionNotFound
	}
	if !validVersionedRecord(record) {
		return Snapshot{}, ErrReconciliationRequired
	}
	if record.State == StateReleased || record.State == StateFailed {
		m.forget(record.ExecutionID)
		m.forgetCleanup(record.ExecutionID)
		return snapshot(record.Record), nil
	}
	if record.State == StateCleanupFailed {
		m.forget(record.ExecutionID)
		m.forgetCleanup(record.ExecutionID)
		return snapshot(record.Record), ErrQuarantined
	}
	if record.State != StateCleaning {
		// Persist teardown intent before the first destructive side effect. If a
		// later quarantine write fails, replay still sees Cleaning and can never
		// admit this workspace as Prepared or Running.
		record.State = StateCleaning
		record, err = m.compareAndSwap(ctx, record)
		if err != nil {
			return Snapshot{}, err
		}
		m.rememberCleanup(record)
	} else if !m.ownsCleanup(record) {
		// Only the Manager that won the Cleaning CAS may perform teardown. A
		// recreated Manager must reconcile the platform journal before adopting
		// destructive ownership.
		return snapshot(record.Record), ErrReconciliationRequired
	}
	m.forget(record.ExecutionID)
	// Fence and stop the process boundary before consulting a workspace that may
	// have been replaced or made unreadable. Quarantine blocks future admission,
	// but it is not a substitute for stopping an existing or in-flight runner.
	if record.PID > 0 || validContainment(record.Containment) {
		if !m.supervisor.StrongDescendantOwnership() || !validFencedContainment(record.Containment) {
			return m.quarantine(ctx, record)
		}
		if err := m.supervisor.Stop(ctx, Process{PID: record.PID, Containment: record.Containment}); err != nil {
			return m.quarantine(ctx, record)
		}
	}
	if !m.cleaner.StrongWorkspaceOwnership() {
		return m.quarantine(ctx, record)
	}
	if err := m.cleaner.ValidateRuntimeRoot(ctx, m.runtimeRoot); err != nil {
		return m.quarantine(ctx, record)
	}
	if !validWorkspaceRef(record.WorkspaceRef) {
		return m.quarantine(ctx, record)
	}
	process := Process{PID: record.PID, Containment: record.Containment}
	finalizer, supportsFinalization := m.supervisor.(CleanupFinalizer)
	if supportsFinalization && validFencedContainment(record.Containment) {
		if err := m.finalizeCleanup(ctx, finalizer, process, record.RootName, record.WorkspaceRef); err != nil {
			return m.quarantine(ctx, record)
		}
	} else {
		if ref, refErr := m.workspaceRef(ctx, record.RootName); refErr != nil || ref != record.WorkspaceRef {
			return m.quarantine(ctx, record)
		}
		if err := m.removeRoot(ctx, record.RootName); err != nil {
			return m.quarantine(ctx, record)
		}
	}
	terminal := record.Record
	terminal.State = StateReleased
	terminal.PID = 0
	record, err = m.commitCleanupTerminal(ctx, record, terminal)
	if err != nil {
		return Snapshot{}, err
	}
	m.forgetCleanup(record.ExecutionID)
	if supportsFinalization && validFencedContainment(record.Containment) {
		// The finalized tombstone bridges the filesystem/journal commit. Once
		// Released is durable it is safe, bounded residue; try to collect it now,
		// while Released recovery retries stale residue after an Agent crash.
		m.garbageCollectCleanup(finalizer, process)
	}
	return snapshot(record.Record), nil
}

func (m *Manager) garbageCollectCleanup(finalizer CleanupFinalizer, process Process) {
	housekeeping, cancel := context.WithTimeout(
		context.Background(),
		cleanupHousekeepingTimeout,
	)
	defer cancel()
	// A failure leaves an inert finalized tombstone as observable residue. The
	// Released recovery path retries it, but housekeeping never changes or
	// blocks the already committed terminal state indefinitely.
	_ = finalizer.GarbageCollectCleanup(housekeeping, process)
}

func (m *Manager) finalizeCleanup(
	ctx context.Context,
	finalizer CleanupFinalizer,
	process Process,
	name string,
	expected WorkspaceRef,
) error {
	parent, err := os.OpenRoot(m.runtimeRoot)
	if err != nil {
		return ErrCleanupFailed
	}
	defer parent.Close()
	executions, err := parent.OpenRoot("executions")
	if err != nil {
		return ErrCleanupFailed
	}
	defer executions.Close()
	if err := finalizer.FinalizeCleanup(ctx, process, executions, name, expected); err != nil {
		return ErrCleanupFailed
	}
	return nil
}

func (m *Manager) quarantine(ctx context.Context, record VersionedRecord) (Snapshot, error) {
	m.forget(record.ExecutionID)
	latest, found, err := m.load(ctx, record.ExecutionID)
	if err != nil {
		return Snapshot{}, err
	}
	if !found || !validVersionedRecord(latest) {
		return Snapshot{}, ErrReconciliationRequired
	}
	if latest.State == StateCleanupFailed {
		m.forgetCleanup(latest.ExecutionID)
		return snapshot(latest.Record), ErrQuarantined
	}
	record = latest
	record.State = StateCleanupFailed
	record.Tombstone = true
	record, err = m.compareAndSwap(ctx, record)
	if err != nil {
		return Snapshot{}, err
	}
	m.forgetCleanup(record.ExecutionID)
	return snapshot(record.Record), ErrQuarantined
}

func (m *Manager) failPreparation(ctx context.Context, record VersionedRecord, cause error) (Snapshot, error) {
	record.State = StateFailed
	record, err := m.compareAndSwap(ctx, record)
	if err != nil {
		return Snapshot{}, err
	}
	return snapshot(record.Record), cause
}

func (m *Manager) failStartedExecution(ctx context.Context, record VersionedRecord, expected Record, cause error) (Snapshot, error) {
	for {
		record.State = StateCleaning
		updated, err := m.compareAndSwap(ctx, record)
		if err == nil {
			record = updated
			m.rememberCleanup(record)
			break
		}
		if !errors.Is(err, ErrReconciliationRequired) || !sameStartedRuntime(updated.Record, expected) {
			return Snapshot{}, err
		}
		record = updated
		select {
		case <-ctx.Done():
			return Snapshot{}, ErrReconciliationRequired
		default:
		}
	}
	m.forget(record.ExecutionID)
	if !m.cleaner.StrongWorkspaceOwnership() || !m.workspaceMatches(ctx, record.Record) {
		return m.quarantine(ctx, record)
	}
	if err := m.removeRoot(ctx, record.RootName); err != nil {
		return m.quarantine(ctx, record)
	}
	terminal := record.Record
	terminal.State = StateFailed
	terminal.PID = 0
	terminal.JITDigest = ""
	terminal.WorkspaceRef = WorkspaceRef{}
	terminal.Containment = ContainmentRef{}
	record, err := m.commitCleanupTerminal(ctx, record, terminal)
	if err != nil {
		return Snapshot{}, err
	}
	m.forgetCleanup(record.ExecutionID)
	return snapshot(record.Record), cause
}

func (m *Manager) load(ctx context.Context, id string) (VersionedRecord, bool, error) {
	record, found, err := m.journal.Load(ctx, id)
	if err != nil {
		return VersionedRecord{}, false, ErrJournal
	}
	return record, found, nil
}

func (m *Manager) create(ctx context.Context, record Record) (VersionedRecord, bool, error) {
	mutationToken, err := newOpaqueToken()
	if err != nil {
		return VersionedRecord{}, false, ErrJournal
	}
	created, ok, err := m.journal.Create(ctx, mutationToken, record)
	if err != nil {
		return VersionedRecord{}, false, ErrJournal
	}
	if ok && (created.Revision != 1 || created.MutationToken != mutationToken || created.Record != record) {
		return VersionedRecord{}, false, ErrJournal
	}
	return created, ok, nil
}

func (m *Manager) compareAndSwap(ctx context.Context, record VersionedRecord) (VersionedRecord, error) {
	mutationToken, tokenErr := newOpaqueToken()
	if tokenErr != nil {
		return record, ErrJournal
	}
	updated, swapped, err := m.journal.CompareAndSwap(ctx, record.ExecutionID, record.Revision, mutationToken, record.Record)
	if err != nil {
		current, found, loadErr := m.journal.Load(ctx, record.ExecutionID)
		if loadErr != nil {
			return record, ErrJournal
		}
		if !found || !validVersionedRecord(current) {
			return record, ErrReconciliationRequired
		}
		if record.Revision != ^uint64(0) && current.Revision == record.Revision+1 && current.MutationToken == mutationToken && current.Record == record.Record {
			return current, nil
		}
		if current.Revision == record.Revision {
			return current, ErrJournal
		}
		return current, ErrReconciliationRequired
	}
	if !swapped {
		if !validVersionedRecord(updated) {
			return record, ErrReconciliationRequired
		}
		return updated, ErrReconciliationRequired
	}
	if record.Revision == ^uint64(0) || updated.Revision != record.Revision+1 || updated.MutationToken != mutationToken || updated.Record != record.Record {
		return record, ErrJournal
	}
	return updated, nil
}

func (m *Manager) owns(record VersionedRecord) bool {
	ownership, found := m.owned[record.ExecutionID]
	return found && ownership.Revision == record.Revision && ownership.Containment == record.Containment
}

func (m *Manager) remember(record VersionedRecord) {
	m.owned[record.ExecutionID] = runtimeOwnership{Revision: record.Revision, Containment: record.Containment}
}

func (m *Manager) forget(executionID string) {
	delete(m.owned, executionID)
}

func (m *Manager) ownsCleanup(record VersionedRecord) bool {
	revision, found := m.cleanupOwned[record.ExecutionID]
	return found && revision == record.Revision && record.State == StateCleaning
}

func (m *Manager) rememberCleanup(record VersionedRecord) {
	m.cleanupOwned[record.ExecutionID] = record.Revision
}

func (m *Manager) forgetCleanup(executionID string) {
	delete(m.cleanupOwned, executionID)
}

func (m *Manager) commitCleanupTerminal(ctx context.Context, cleaning VersionedRecord, terminal Record) (VersionedRecord, error) {
	candidate := cleaning
	candidate.Record = terminal
	updated, err := m.compareAndSwap(ctx, candidate)
	if err == nil {
		return updated, nil
	}
	if validVersionedRecord(updated) && updated.Revision > cleaning.Revision && updated.Record == terminal {
		return updated, nil
	}
	if errors.Is(err, ErrJournal) {
		current, found, loadErr := m.load(ctx, cleaning.ExecutionID)
		if loadErr != nil {
			return cleaning, loadErr
		}
		if !found || !validVersionedRecord(current) {
			return cleaning, ErrReconciliationRequired
		}
		if current.Record == terminal {
			return current, nil
		}
		updated = current
	}
	if !m.ownsCleanup(updated) || updated.Record != cleaning.Record {
		return updated, err
	}
	candidate = updated
	candidate.Record = terminal
	updated, retryErr := m.compareAndSwap(ctx, candidate)
	if retryErr == nil {
		return updated, nil
	}
	if validVersionedRecord(updated) && updated.Revision > cleaning.Revision && updated.Record == terminal {
		return updated, nil
	}
	return updated, retryErr
}

func (m *Manager) makeExecutionRoot(executionID string) (*os.Root, string, error) {
	parent, err := os.OpenRoot(m.runtimeRoot)
	if err != nil {
		return nil, "", ErrCleanupFailed
	}
	defer parent.Close()
	if err := parent.MkdirAll("executions", 0o700); err != nil {
		return nil, "", ErrCleanupFailed
	}
	executions, err := parent.OpenRoot("executions")
	if err != nil {
		return nil, "", ErrCleanupFailed
	}
	defer executions.Close()
	name := executionRootName(executionID)
	if err := executions.Mkdir(name, 0o700); err != nil {
		return nil, "", ErrExecutionConflict
	}
	root, err := executions.OpenRoot(name)
	if err != nil {
		return nil, "", ErrCleanupFailed
	}
	return root, name, nil
}

func (m *Manager) removeRoot(ctx context.Context, name string) error {
	parent, err := os.OpenRoot(m.runtimeRoot)
	if err != nil {
		return ErrCleanupFailed
	}
	defer parent.Close()
	executions, err := parent.OpenRoot("executions")
	if err != nil {
		return ErrCleanupFailed
	}
	defer executions.Close()
	if err := m.cleaner.RemoveAndVerify(ctx, executions, name); err != nil {
		return ErrCleanupFailed
	}
	return nil
}

func (m *Manager) workspaceRef(ctx context.Context, name string) (WorkspaceRef, error) {
	parent, err := os.OpenRoot(m.runtimeRoot)
	if err != nil {
		return WorkspaceRef{}, ErrCleanupFailed
	}
	defer parent.Close()
	executions, err := parent.OpenRoot("executions")
	if err != nil {
		return WorkspaceRef{}, ErrCleanupFailed
	}
	defer executions.Close()
	return m.cleaner.WorkspaceRef(ctx, executions, name)
}

func (m *Manager) prepareWorkspace(ctx context.Context, name string) (WorkspaceRef, error) {
	parent, err := os.OpenRoot(m.runtimeRoot)
	if err != nil {
		return WorkspaceRef{}, ErrCleanupFailed
	}
	defer parent.Close()
	executions, err := parent.OpenRoot("executions")
	if err != nil {
		return WorkspaceRef{}, ErrCleanupFailed
	}
	defer executions.Close()
	return m.cleaner.PrepareWorkspace(ctx, executions, name)
}

func (m *Manager) workspaceMatches(ctx context.Context, record Record) bool {
	if !validWorkspaceRef(record.WorkspaceRef) {
		return false
	}
	if err := m.cleaner.ValidateRuntimeRoot(ctx, m.runtimeRoot); err != nil {
		return false
	}
	ref, err := m.workspaceRef(ctx, record.RootName)
	return err == nil && ref == record.WorkspaceRef
}

func validatePreparation(request Preparation) error {
	if request.ExecutionID == "" || path.Base(request.ExecutionID) != request.ExecutionID || !request.Package.valid() {
		return ErrInvalidRequest
	}
	return nil
}

func preparationDigest(request Preparation) string {
	value := request.ExecutionID + "\x00" + request.Package.key() + "\x00"
	if request.DisableUpdate {
		value += "disable-update"
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func executionRootName(executionID string) string {
	sum := sha256.Sum256([]byte(executionID))
	return hex.EncodeToString(sum[:])
}

func runnerArguments(disableUpdate bool) []string {
	args := []string{"--ephemeral"}
	if disableUpdate {
		args = append(args, "--disableupdate")
	}
	return args
}

func runnerExecutable(runtimeRoot, name string) string {
	if runtime.GOOS == "windows" {
		return filepath.Join(executionPath(runtimeRoot, name), "run.cmd")
	}
	return filepath.Join(executionPath(runtimeRoot, name), "run.sh")
}

func executionPath(runtimeRoot, name string) string {
	return filepath.Join(runtimeRoot, "executions", name)
}

func snapshot(record Record) Snapshot {
	return Snapshot{ExecutionID: record.ExecutionID, State: record.State, Prepared: record.State == StatePrepared || record.State == StateStarting || record.State == StateRunning, Running: record.State == StateRunning, Quarantined: record.State == StateCleanupFailed || record.Tombstone}
}

func activeState(state State) bool {
	return state == StatePrepared || state == StateStarting || state == StateRunning
}

func stateError(record Record) error {
	switch record.State {
	case StateCleanupFailed:
		return ErrQuarantined
	case StatePreparing, StateStarting, StateCleaning:
		return ErrReconciliationRequired
	case StateReleased, StateFailed:
		return ErrExecutionConflict
	default:
		return nil
	}
}

func canonicalDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func newOpaqueToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func canonicalOpaqueToken(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func canonicalFenceToken(value string) bool { return canonicalOpaqueToken(value) }

func validRecord(record Record) bool {
	if record.ExecutionID == "" || record.RootName != executionRootName(record.ExecutionID) || !canonicalDigest(record.SpecDigest) {
		return false
	}
	if record.JITDigest != "" && !canonicalDigest(record.JITDigest) {
		return false
	}
	switch record.State {
	case StatePreparing:
		return record.PID == 0 && record.JITDigest == "" && record.WorkspaceRef == (WorkspaceRef{}) && record.Containment == (ContainmentRef{}) && !record.Tombstone
	case StatePrepared:
		return record.PID == 0 && record.JITDigest == "" && validWorkspaceRef(record.WorkspaceRef) && record.Containment == (ContainmentRef{}) && !record.Tombstone
	case StateStarting:
		return record.PID == 0 && record.JITDigest != "" && validWorkspaceRef(record.WorkspaceRef) && validFencedContainment(record.Containment) && !record.Tombstone
	case StateRunning:
		return record.PID > 0 && record.JITDigest != "" && validWorkspaceRef(record.WorkspaceRef) && validFencedContainment(record.Containment) && !record.Tombstone
	case StateCleaning:
		preparing := record.PID == 0 && record.JITDigest == "" && record.WorkspaceRef == (WorkspaceRef{}) && record.Containment == (ContainmentRef{})
		prepared := record.PID == 0 && record.JITDigest == "" && record.Containment == (ContainmentRef{})
		starting := record.PID == 0 && record.JITDigest != "" && validFencedContainment(record.Containment)
		running := record.PID > 0 && record.JITDigest != "" && validFencedContainment(record.Containment)
		active := validWorkspaceRef(record.WorkspaceRef) && (prepared || starting || running)
		return !record.Tombstone && (preparing || active)
	case StateReleased:
		prepared := record.JITDigest == "" && record.Containment == (ContainmentRef{})
		started := record.JITDigest != "" && validFencedContainment(record.Containment)
		return record.PID == 0 && validWorkspaceRef(record.WorkspaceRef) && !record.Tombstone && (prepared || started)
	case StateFailed:
		return record.PID == 0 && record.JITDigest == "" && record.WorkspaceRef == (WorkspaceRef{}) && record.Containment == (ContainmentRef{}) && !record.Tombstone
	case StateCleanupFailed:
		return record.Tombstone
	default:
		return false
	}
}

func validVersionedRecord(record VersionedRecord) bool {
	return record.Revision > 0 && canonicalOpaqueToken(record.MutationToken) && validRecord(record.Record)
}

func validContainment(ref ContainmentRef) bool {
	return ref.Backend != "" && ref.OwnerID != ""
}

func validWorkspaceRef(ref WorkspaceRef) bool {
	return ref.Backend != "" && ref.OwnerID != ""
}

func validFencedContainment(ref ContainmentRef) bool {
	return validContainment(ref) && canonicalFenceToken(ref.FenceToken)
}

func sameStartedRuntime(current, expected Record) bool {
	if current.ExecutionID != expected.ExecutionID ||
		current.SpecDigest != expected.SpecDigest ||
		current.JITDigest != expected.JITDigest ||
		current.RootName != expected.RootName ||
		current.WorkspaceRef != expected.WorkspaceRef ||
		current.Containment != expected.Containment {
		return false
	}
	switch current.State {
	case StateStarting:
		return current.PID == 0
	case StateRunning:
		return current.PID > 0 && (expected.PID == 0 || current.PID == expected.PID)
	default:
		return false
	}
}
