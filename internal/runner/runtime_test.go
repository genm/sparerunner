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

func (c testCache) Ensure(context.Context, Package) (string, error) { return c.root, nil }

type testSupervisor struct {
	starts, stops int
	stopErr       error
}
type strongCleaner struct{ rootCleaner }

func (strongCleaner) StrongWorkspaceOwnership() bool { return true }

func (*testSupervisor) StrongDescendantOwnership() bool { return true }
func (s *testSupervisor) Start(context.Context, StartRequest) (Process, error) {
	s.starts++
	return Process{PID: s.starts}, nil
}
func (s *testSupervisor) Stop(context.Context, Process) error { s.stops++; return s.stopErr }
func (*testSupervisor) Alive(Process) (bool, error)           { return true, nil }

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
