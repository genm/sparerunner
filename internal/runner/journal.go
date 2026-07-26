package runner

import (
	"context"
	"sync"
)

type State string

const (
	StatePreparing     State = "preparing"
	StatePrepared      State = "prepared"
	StateStarting      State = "starting"
	StateRunning       State = "running"
	StateCleaning      State = "cleaning"
	StateReleased      State = "released"
	StateFailed        State = "failed"
	StateCleanupFailed State = "cleanup_failed"
)

// Record contains only replay-safe metadata. In particular it contains neither
// JIT configuration nor process arguments, because those are secret-bearing.
type Record struct {
	ExecutionID  string
	SpecDigest   string
	JITDigest    string
	State        State
	RootName     string
	PID          int
	Tombstone    bool
	Containment  ContainmentRef
	WorkspaceRef WorkspaceRef
}

// VersionedRecord separates the storage concurrency token from lifecycle data.
// Revision is strictly positive for persisted records and is never supplied by
// a controller command or derived from process state. MutationToken identifies
// the writer of the latest revision so a write-then-error can be resolved without
// mistaking another Manager's identical transition for success.
type VersionedRecord struct {
	Record
	Revision      uint64
	MutationToken string
}

// WorkspaceRef is a durable, comparable observation produced by the platform
// workspace authority. Backend identifies the authority and OwnerID is its
// canonical opaque identity. Neither field is a path or a credential.
type WorkspaceRef struct {
	Backend string
	OwnerID string
}

// ContainmentRef is durable platform ownership metadata. PID is observation
// only. OwnerID is the platform authority (systemd unit, exclusive macOS slot
// UID, or Windows Job Object name); Scope and epoch fields carry backend-specific
// reconciliation observations without making the core lifecycle Linux-shaped.
type ContainmentRef struct {
	Backend      string
	OwnerID      string
	Scope        string
	HostEpoch    string
	InvocationID string
	FenceToken   string
}

// Journal is the durable local observation boundary. Create and CompareAndSwap
// are the only mutation paths: unconditional last-writer-wins updates would allow
// two agent instances to start the same execution. A future SQLite journal can
// implement these with INSERT and revision-guarded UPDATE without receiving JIT
// material.
type Journal interface {
	Load(context.Context, string) (VersionedRecord, bool, error)
	Create(context.Context, string, Record) (VersionedRecord, bool, error)
	CompareAndSwap(context.Context, string, uint64, string, Record) (VersionedRecord, bool, error)
}

type MemoryJournal struct {
	mu      sync.Mutex
	records map[string]VersionedRecord
}

func NewMemoryJournal() *MemoryJournal {
	return &MemoryJournal{records: make(map[string]VersionedRecord)}
}

func (j *MemoryJournal) Load(_ context.Context, executionID string) (VersionedRecord, bool, error) {
	if j == nil {
		return VersionedRecord{}, false, ErrJournal
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	record, ok := j.records[executionID]
	return record, ok, nil
}

func (j *MemoryJournal) Create(_ context.Context, mutationToken string, record Record) (VersionedRecord, bool, error) {
	if j == nil || record.ExecutionID == "" || !canonicalOpaqueToken(mutationToken) {
		return VersionedRecord{}, false, ErrJournal
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if existing, found := j.records[record.ExecutionID]; found {
		return existing, false, nil
	}
	created := VersionedRecord{Record: record, Revision: 1, MutationToken: mutationToken}
	j.records[record.ExecutionID] = created
	return created, true, nil
}

func (j *MemoryJournal) CompareAndSwap(_ context.Context, executionID string, expectedRevision uint64, mutationToken string, next Record) (VersionedRecord, bool, error) {
	if j == nil || executionID == "" || next.ExecutionID != executionID || expectedRevision == 0 || !canonicalOpaqueToken(mutationToken) {
		return VersionedRecord{}, false, ErrJournal
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	current, found := j.records[executionID]
	if !found || current.Revision != expectedRevision {
		return current, false, nil
	}
	if expectedRevision == ^uint64(0) {
		return VersionedRecord{}, false, ErrJournal
	}
	updated := VersionedRecord{Record: next, Revision: expectedRevision + 1, MutationToken: mutationToken}
	j.records[executionID] = updated
	return updated, true, nil
}
