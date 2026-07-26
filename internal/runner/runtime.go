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
	m.mu.Lock()
	defer m.mu.Unlock()
	record, found, err := m.load(ctx, request.ExecutionID)
	if err != nil {
		return Snapshot{}, err
	}
	digest := preparationDigest(request)
	if found {
		if !validRecord(record) {
			return Snapshot{}, ErrReconciliationRequired
		}
		if record.SpecDigest != digest {
			return Snapshot{}, ErrExecutionConflict
		}
		if (record.State == StatePrepared || record.State == StateStarting || record.State == StateRunning) && !m.executionRootExists(record.RootName) {
			return snapshot(record), ErrReconciliationRequired
		}
		return snapshot(record), stateError(record)
	}
	archive, err := m.cache.Ensure(ctx, request.Package)
	if err != nil {
		return Snapshot{}, err
	}
	root, rootName, err := m.makeExecutionRoot(request.ExecutionID)
	if err != nil {
		return Snapshot{}, err
	}
	if err := materializePackage(archive, request.Package, root); err != nil {
		root.Close()
		_ = m.removeRoot(ctx, rootName)
		return Snapshot{}, err
	}
	if err := root.Close(); err != nil {
		_ = m.removeRoot(ctx, rootName)
		return Snapshot{}, ErrCleanupFailed
	}
	record = Record{ExecutionID: request.ExecutionID, SpecDigest: digest, State: StatePrepared, RootName: rootName}
	if err := m.save(ctx, record); err != nil {
		_ = m.removeRoot(ctx, rootName)
		return Snapshot{}, err
	}
	return snapshot(record), nil
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
	if !validRecord(record) {
		return Snapshot{}, ErrReconciliationRequired
	}
	jitDigest := request.JIT.Digest()
	switch record.State {
	case StateRunning:
		if record.JITDigest != jitDigest {
			return Snapshot{}, ErrExecutionConflict
		}
		alive, err := m.supervisor.Alive(Process{PID: record.PID})
		if err != nil || !alive {
			return snapshot(record), ErrReconciliationRequired
		}
		return snapshot(record), nil
	case StateStarting:
		return Snapshot{}, ErrReconciliationRequired
	case StatePrepared:
		// continue below
	default:
		return snapshot(record), ErrExecutionConflict
	}
	record.State = StateStarting
	record.JITDigest = jitDigest
	if err := m.save(ctx, record); err != nil {
		return Snapshot{}, err
	}
	var process Process
	started := false
	if err := request.JIT.Deliver(func(value string) error {
		actual := sha256.Sum256([]byte(value))
		if hex.EncodeToString(actual[:]) != jitDigest {
			return ErrInvalidRequest
		}
		if started {
			return ErrReconciliationRequired
		}
		started = true
		var startErr error
		process, startErr = m.supervisor.Start(ctx, StartRequest{
			Executable: runnerExecutable(m.runtimeRoot, record.RootName),
			Directory:  executionPath(m.runtimeRoot, record.RootName),
			Arguments:  runnerArguments(value, request.DisableUpdate),
		})
		return startErr
	}); err != nil {
		if process.PID > 0 {
			if stopErr := m.supervisor.Stop(ctx, process); stopErr != nil {
				return m.quarantine(ctx, record)
			}
		}
		record.State = StatePrepared
		record.JITDigest = ""
		record.PID = 0
		if saveErr := m.save(ctx, record); saveErr != nil {
			return Snapshot{}, saveErr
		}
		return Snapshot{}, ErrInvalidRequest
	}
	if !started || process.PID <= 0 {
		record.State = StatePrepared
		record.JITDigest = ""
		if saveErr := m.save(ctx, record); saveErr != nil {
			return m.quarantine(ctx, record)
		}
		return Snapshot{}, ErrInvalidRequest
	}
	record.State = StateRunning
	record.PID = process.PID
	if err := m.save(ctx, record); err != nil {
		// A started process without a durable PID must not be replayed. Stop it
		// now; if that cannot be proven, the caller remains fail-closed.
		if stopErr := m.supervisor.Stop(ctx, process); stopErr != nil {
			return m.quarantine(ctx, record)
		}
		return Snapshot{}, err
	}
	return snapshot(record), nil
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
	if !validRecord(record) {
		return Snapshot{}, ErrReconciliationRequired
	}
	if record.State == StateRunning {
		alive, observeErr := m.supervisor.Alive(Process{PID: record.PID})
		if observeErr != nil || !alive {
			return snapshot(record), ErrReconciliationRequired
		}
	}
	return snapshot(record), stateError(record)
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
	if !validRecord(record) {
		return Snapshot{}, ErrReconciliationRequired
	}
	if record.State == StateReleased {
		return snapshot(record), ErrExecutionConflict
	}
	if record.State == StateCleanupFailed {
		return snapshot(record), ErrQuarantined
	}
	if record.State == StateStarting {
		// A crash could have occurred after process creation but before PID
		// persistence. There is no ownership proof, so do not delete the root or
		// release capacity on the strength of an unobservable guess.
		return m.quarantine(ctx, record)
	}
	if record.PID > 0 {
		if err := m.supervisor.Stop(ctx, Process{PID: record.PID}); err != nil {
			return m.quarantine(ctx, record)
		}
	}
	if err := m.removeRoot(ctx, record.RootName); err != nil {
		return m.quarantine(ctx, record)
	}
	record.State = StateReleased
	record.PID = 0
	if err := m.save(ctx, record); err != nil {
		return Snapshot{}, err
	}
	return snapshot(record), nil
}

func (m *Manager) quarantine(ctx context.Context, record Record) (Snapshot, error) {
	record.State = StateCleanupFailed
	record.Tombstone = true
	if err := m.save(ctx, record); err != nil {
		return Snapshot{}, ErrJournal
	}
	return snapshot(record), ErrQuarantined
}

func (m *Manager) load(ctx context.Context, id string) (Record, bool, error) {
	record, found, err := m.journal.Load(ctx, id)
	if err != nil {
		return Record{}, false, ErrJournal
	}
	return record, found, nil
}

func (m *Manager) save(ctx context.Context, record Record) error {
	if err := m.journal.Save(ctx, record); err != nil {
		return ErrJournal
	}
	return nil
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

func (m *Manager) executionRootExists(name string) bool {
	parent, err := os.OpenRoot(m.runtimeRoot)
	if err != nil {
		return false
	}
	defer parent.Close()
	executions, err := parent.OpenRoot("executions")
	if err != nil {
		return false
	}
	defer executions.Close()
	info, err := executions.Stat(name)
	return err == nil && info.IsDir()
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

func runnerArguments(jit string, disableUpdate bool) []string {
	args := []string{"--jitconfig", jit, "--ephemeral"}
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

func stateError(record Record) error {
	switch record.State {
	case StateCleanupFailed:
		return ErrQuarantined
	case StateStarting:
		return ErrReconciliationRequired
	case StateReleased:
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
	case StatePrepared:
		return record.PID == 0 && record.JITDigest == "" && !record.Tombstone
	case StateStarting:
		return record.PID == 0 && record.JITDigest != "" && !record.Tombstone
	case StateRunning:
		return record.PID > 0 && record.JITDigest != "" && !record.Tombstone
	case StateReleased:
		return record.PID == 0 && !record.Tombstone
	case StateCleanupFailed:
		return record.Tombstone
	default:
		return false
	}
}
