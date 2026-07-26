//go:build darwin || linux

package runner

import (
	"context"
	"errors"
	"testing"
)

func TestRecreatedSupervisorRejectsUnownedPID(t *testing.T) {
	owner := NewSupervisor()
	process, err := owner.Start(context.Background(), StartRequest{Executable: "/bin/sh", Directory: t.TempDir(), Arguments: []string{"-c", "while :; do sleep 1; done"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Stop(context.Background(), process) })
	recreated := NewSupervisor()
	if err := recreated.Stop(context.Background(), process); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("recreated Stop error = %v", err)
	}
	if _, err := recreated.Alive(process); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("recreated Alive error = %v", err)
	}
	if alive, err := owner.Alive(process); err != nil || !alive {
		t.Fatalf("owner process unexpectedly signalled: alive=%v err=%v", alive, err)
	}
}
