package domain_test

import (
	"errors"
	"testing"

	"github.com/genm/tewake/internal/domain"
)

func TestExecutionSnapshotCannotBypassStateMachine(t *testing.T) {
	execution, err := domain.NewExecution("execution-1", "target-1", domain.SlotKey{NodeID: "node-1", Index: 0})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := execution.Snapshot()
	snapshot.ID = "other-execution"
	snapshot.TargetID = "other-target"
	snapshot.Slot = domain.SlotKey{NodeID: "other-node", Index: 9}
	snapshot.State = domain.ExecutionRunning

	if got := execution.Snapshot(); got != (domain.ExecutionSnapshot{
		ID:       "execution-1",
		TargetID: "target-1",
		Slot:     domain.SlotKey{NodeID: "node-1", Index: 0},
		State:    domain.ExecutionPending,
	}) {
		t.Fatalf("live execution changed through snapshot: %+v", got)
	}
	assertValidationCode(t, execution.Transition(domain.ExecutionRunning), "invalid_execution_transition")
	if got := execution.CurrentState(); got != domain.ExecutionPending {
		t.Fatalf("state after rejected bypass = %q, want %q", got, domain.ExecutionPending)
	}
}

func TestRestoreExecutionRejectsInvalidSnapshots(t *testing.T) {
	tests := []struct {
		name     string
		snapshot domain.ExecutionSnapshot
		code     string
	}{
		{
			name:     "empty ID",
			snapshot: domain.ExecutionSnapshot{TargetID: "target-1", Slot: domain.SlotKey{NodeID: "node-1", Index: 0}, State: domain.ExecutionPending},
			code:     "required",
		},
		{
			name:     "negative slot index",
			snapshot: domain.ExecutionSnapshot{ID: "execution-1", TargetID: "target-1", Slot: domain.SlotKey{NodeID: "node-1", Index: -1}, State: domain.ExecutionPending},
			code:     "invalid_slot_index",
		},
		{
			name:     "unknown state",
			snapshot: domain.ExecutionSnapshot{ID: "execution-1", TargetID: "target-1", Slot: domain.SlotKey{NodeID: "node-1", Index: 0}, State: "unknown"},
			code:     "invalid_execution_state",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := domain.RestoreExecution(test.snapshot)
			assertValidationCode(t, err, test.code)
		})
	}
}

func assertValidationCode(t *testing.T, err error, want string) {
	t.Helper()
	var validationErr *domain.ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("error = %v, want ValidationError code %q", err, want)
	}
	if validationErr.Code != want {
		t.Fatalf("error code = %q, want %q", validationErr.Code, want)
	}
}
