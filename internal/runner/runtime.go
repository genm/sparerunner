package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
)

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
	ValidateRuntimeRoot(context.Context, string) error
	// PrepareWorkspace may apply the platform runner identity and returns the
	// durable identity observation that later cleanup must match.
	PrepareWorkspace(context.Context, *os.Root, string) (string, error)
	// WorkspaceRef observes only; it must never repair ownership during cleanup.
	WorkspaceRef(context.Context, *os.Root, string) (string, error)
}

type PackageCache interface {
	Ensure(context.Context, Package) (ArchiveRef, error)
}

// ArchiveRef points only at a verified immutable release artifact. A shared
// extracted tree is never executable input; each execution extracts this archive
// beneath its own private root.
type ArchiveRef struct{ Directory, Archive string }

type rootCleaner struct{}

func (rootCleaner) StrongWorkspaceOwnership() bool { return false }
func (rootCleaner) ValidateRuntimeRoot(context.Context, string) error {
	return ErrStrongOwnershipUnavailable
}
func (rootCleaner) PrepareWorkspace(context.Context, *os.Root, string) (string, error) {
	return "", ErrStrongOwnershipUnavailable
}
func (rootCleaner) WorkspaceRef(context.Context, *os.Root, string) (string, error) {
	return "", ErrStrongOwnershipUnavailable
}

func (rootCleaner) RemoveAndVerify(_ context.Context, root *os.Root, name string) error {
	// Absence before cleanup is not success: a renamed root can still contain
	// credentials. Platform adapters add durable file identity in twk-007/008.
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
	runtimeRoot string
	cache       PackageCache
	journal     Journal
	supervisor  Supervisor
	cleaner     Cleaner
	mu          sync.Mutex
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
	return &Manager{options.RuntimeRoot, options.Cache, options.Journal, options.Supervisor, options.Cleaner, sync.Mutex{}}, nil
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
	archive, err := m.cache.Ensure(ctx, request.Package)
	if err != nil {
		return Snapshot{}, err
	}
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
	if err := materializePackage(archive, request.Package, root); err != nil {
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
	if err != nil || workspaceRef == "" {
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

func materializePackage(source ArchiveRef, pkg Package, destination *os.Root) error {
	if source.Archive == "test-tree" {
		return copyTree(source.Directory, destination)
	}
	if source.Directory == "" || source.Archive != "archive" {
		return ErrPackageIntegrity
	}
	archive, err := os.Open(filepath.Join(source.Directory, source.Archive))
	if err != nil {
		return ErrPackageIntegrity
	}
	defer archive.Close()
	return extractArchive(destination, archive, pkg.Format)
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
	if err != nil || !validContainment(containment) {
		return Snapshot{}, ErrStrongOwnershipUnavailable
	}
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
	if err := request.JIT.Deliver(func(value string) error {
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
		process, supervisorErr = m.supervisor.Start(ctx, StartRequest{
			Executable:   runnerExecutable(m.runtimeRoot, record.RootName),
			Directory:    executionPath(m.runtimeRoot, record.RootName),
			Arguments:    runnerArguments(request.DisableUpdate),
			WorkspaceRef: record.WorkspaceRef,
			Containment:  containment,
			jit:          jitArgument{value: value},
		})
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
	if !started || process.PID <= 0 || process.Containment != containment {
		if stopErr := m.supervisor.Stop(ctx, Process{PID: process.PID, Containment: containment}); stopErr != nil {
			return m.quarantine(ctx, record)
		}
		if process.Containment != containment {
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
	record.State = StateRunning
	record.PID = process.PID
	record.Containment = process.Containment
	record, err = m.compareAndSwap(ctx, record)
	if err != nil {
		// A started process without a durable PID must not be replayed. Stop it
		// now; if that cannot be proven, the caller remains fail-closed.
		if stopErr := m.supervisor.Stop(ctx, process); stopErr != nil {
			return m.quarantine(ctx, record)
		}
		return Snapshot{}, err
	}
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
		alive, observeErr := m.supervisor.Alive(Process{PID: record.PID, Containment: record.Containment})
		if observeErr != nil || !alive {
			return snapshot(record.Record), ErrReconciliationRequired
		}
	}
	return snapshot(record.Record), stateError(record.Record)
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
		return snapshot(record.Record), nil
	}
	if record.State == StateCleanupFailed {
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
	}
	if !m.cleaner.StrongWorkspaceOwnership() {
		return m.quarantine(ctx, record)
	}
	if err := m.cleaner.ValidateRuntimeRoot(ctx, m.runtimeRoot); err != nil {
		return m.quarantine(ctx, record)
	}
	if record.WorkspaceRef == "" {
		return m.quarantine(ctx, record)
	}
	if ref, refErr := m.workspaceRef(ctx, record.RootName); refErr != nil || ref != record.WorkspaceRef {
		return m.quarantine(ctx, record)
	}
	if record.PID > 0 || validContainment(record.Containment) {
		if !m.supervisor.StrongDescendantOwnership() || !validContainment(record.Containment) {
			return m.quarantine(ctx, record)
		}
		if err := m.supervisor.Stop(ctx, Process{PID: record.PID, Containment: record.Containment}); err != nil {
			return m.quarantine(ctx, record)
		}
	}
	if err := m.removeRoot(ctx, record.RootName); err != nil {
		return m.quarantine(ctx, record)
	}
	record.State = StateReleased
	record.PID = 0
	record, err = m.compareAndSwap(ctx, record)
	if err != nil {
		return Snapshot{}, err
	}
	return snapshot(record.Record), nil
}

func (m *Manager) quarantine(ctx context.Context, record VersionedRecord) (Snapshot, error) {
	record.State = StateCleanupFailed
	record.Tombstone = true
	record, err := m.compareAndSwap(ctx, record)
	if err != nil {
		return Snapshot{}, err
	}
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

func (m *Manager) load(ctx context.Context, id string) (VersionedRecord, bool, error) {
	record, found, err := m.journal.Load(ctx, id)
	if err != nil {
		return VersionedRecord{}, false, ErrJournal
	}
	return record, found, nil
}

func (m *Manager) create(ctx context.Context, record Record) (VersionedRecord, bool, error) {
	created, ok, err := m.journal.Create(ctx, record)
	if err != nil {
		return VersionedRecord{}, false, ErrJournal
	}
	if ok && (created.Revision != 1 || created.Record != record) {
		return VersionedRecord{}, false, ErrJournal
	}
	return created, ok, nil
}

func (m *Manager) compareAndSwap(ctx context.Context, record VersionedRecord) (VersionedRecord, error) {
	updated, swapped, err := m.journal.CompareAndSwap(ctx, record.ExecutionID, record.Revision, record.Record)
	if err != nil {
		return record, ErrJournal
	}
	if !swapped {
		return record, ErrReconciliationRequired
	}
	if record.Revision == ^uint64(0) || updated.Revision != record.Revision+1 || updated.Record != record.Record {
		return record, ErrJournal
	}
	return updated, nil
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

func (m *Manager) workspaceRef(ctx context.Context, name string) (string, error) {
	parent, err := os.OpenRoot(m.runtimeRoot)
	if err != nil {
		return "", ErrCleanupFailed
	}
	defer parent.Close()
	executions, err := parent.OpenRoot("executions")
	if err != nil {
		return "", ErrCleanupFailed
	}
	defer executions.Close()
	return m.cleaner.WorkspaceRef(ctx, executions, name)
}

func (m *Manager) prepareWorkspace(ctx context.Context, name string) (string, error) {
	parent, err := os.OpenRoot(m.runtimeRoot)
	if err != nil {
		return "", ErrCleanupFailed
	}
	defer parent.Close()
	executions, err := parent.OpenRoot("executions")
	if err != nil {
		return "", ErrCleanupFailed
	}
	defer executions.Close()
	return m.cleaner.PrepareWorkspace(ctx, executions, name)
}

func (m *Manager) workspaceMatches(ctx context.Context, record Record) bool {
	if record.WorkspaceRef == "" {
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

func validRecord(record Record) bool {
	if record.ExecutionID == "" || record.RootName != executionRootName(record.ExecutionID) || !canonicalDigest(record.SpecDigest) {
		return false
	}
	if record.JITDigest != "" && !canonicalDigest(record.JITDigest) {
		return false
	}
	switch record.State {
	case StatePreparing:
		return record.PID == 0 && record.JITDigest == "" && record.WorkspaceRef == "" && record.Containment == (ContainmentRef{}) && !record.Tombstone
	case StatePrepared:
		return record.PID == 0 && record.JITDigest == "" && record.WorkspaceRef != "" && record.Containment == (ContainmentRef{}) && !record.Tombstone
	case StateStarting:
		return record.PID == 0 && record.JITDigest != "" && record.WorkspaceRef != "" && validContainment(record.Containment) && !record.Tombstone
	case StateRunning:
		return record.PID > 0 && record.JITDigest != "" && record.WorkspaceRef != "" && validContainment(record.Containment) && !record.Tombstone
	case StateCleaning:
		preparing := record.PID == 0 && record.JITDigest == "" && record.WorkspaceRef == "" && record.Containment == (ContainmentRef{})
		prepared := record.PID == 0 && record.JITDigest == "" && record.Containment == (ContainmentRef{})
		starting := record.PID == 0 && record.JITDigest != "" && validContainment(record.Containment)
		running := record.PID > 0 && record.JITDigest != "" && validContainment(record.Containment)
		active := record.WorkspaceRef != "" && (prepared || starting || running)
		return !record.Tombstone && (preparing || active)
	case StateReleased:
		return record.PID == 0 && record.WorkspaceRef != "" && !record.Tombstone
	case StateFailed:
		return record.PID == 0 && record.JITDigest == "" && record.WorkspaceRef == "" && record.Containment == (ContainmentRef{}) && !record.Tombstone
	case StateCleanupFailed:
		return record.Tombstone
	default:
		return false
	}
}

func validVersionedRecord(record VersionedRecord) bool {
	return record.Revision > 0 && validRecord(record.Record)
}

func validContainment(ref ContainmentRef) bool {
	return ref.Backend != "" && ref.OwnerID != ""
}
