package runner

import (
	"context"
	"sync"
)

type State string

const (
	StatePrepared      State = "prepared"
	StateStarting      State = "starting"
	StateRunning       State = "running"
	StateReleased      State = "released"
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
	WorkspaceRef string
}

// ContainmentRef is durable platform ownership metadata. PID is observation
// only; twk-007 fills this with a systemd unit/cgroup/boot/invocation identity.
type ContainmentRef struct{ Backend, Unit, ControlGroup, BootID, InvocationID string }

// Journal is the durable local observation boundary. A future SQLite journal may
// implement it without changing lifecycle behavior or receiving JIT material.
type Journal interface {
	Load(context.Context, string) (Record, bool, error)
	Save(context.Context, Record) error
}

type MemoryJournal struct {
	mu      sync.Mutex
	records map[string]Record
}

func NewMemoryJournal() *MemoryJournal { return &MemoryJournal{records: make(map[string]Record)} }

func (j *MemoryJournal) Load(_ context.Context, executionID string) (Record, bool, error) {
	if j == nil {
		return Record{}, false, ErrJournal
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	record, ok := j.records[executionID]
	return record, ok, nil
}

func (j *MemoryJournal) Save(_ context.Context, record Record) error {
	if j == nil || record.ExecutionID == "" {
		return ErrJournal
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.records[record.ExecutionID] = record
	return nil
}
