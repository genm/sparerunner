package domain

import "sync"

type ExecutionState string

const (
	ExecutionPending       ExecutionState = "pending"
	ExecutionReserved      ExecutionState = "reserved"
	ExecutionPreparing     ExecutionState = "preparing"
	ExecutionRunning       ExecutionState = "running"
	ExecutionCleaning      ExecutionState = "cleaning"
	ExecutionReleased      ExecutionState = "released"
	ExecutionFailed        ExecutionState = "failed"
	ExecutionCleanupFailed ExecutionState = "cleanup_failed"
	ExecutionQuarantined   ExecutionState = "quarantined"
)

func (s ExecutionState) Validate(field string) error {
	if isKnownExecutionState(s) {
		return nil
	}
	return invalid("invalid_execution_state", field, "is not a known execution state")
}

// Execution holds desired lifecycle state; agents keep OS observations separately.
type Execution struct {
	mu       sync.RWMutex
	id       ExecutionID
	targetID TargetID
	slot     SlotKey
	state    ExecutionState
}

// ExecutionSnapshot is the durable, copy-safe representation of an execution.
// Mutating a snapshot cannot alter a live Execution; RestoreExecution is the only
// rehydration boundary and validates every field before reconstructing one.
type ExecutionSnapshot struct {
	ID       ExecutionID
	TargetID TargetID
	Slot     SlotKey
	State    ExecutionState
}

func NewExecution(id ExecutionID, targetID TargetID, slot SlotKey) (*Execution, error) {
	return RestoreExecution(ExecutionSnapshot{
		ID:       id,
		TargetID: targetID,
		Slot:     slot,
		State:    ExecutionPending,
	})
}

// RestoreExecution rehydrates a durable desired state without allowing stores or
// schedulers to mutate a live execution directly.
func RestoreExecution(snapshot ExecutionSnapshot) (*Execution, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	return &Execution{
		id:       snapshot.ID,
		targetID: snapshot.TargetID,
		slot:     snapshot.Slot,
		state:    snapshot.State,
	}, nil
}

func (s ExecutionSnapshot) Validate() error {
	if err := required(string(s.ID), "execution.id"); err != nil {
		return err
	}
	if err := required(string(s.TargetID), "execution.target_id"); err != nil {
		return err
	}
	if err := required(string(s.Slot.NodeID), "execution.slot.node_id"); err != nil {
		return err
	}
	if s.Slot.Index < 0 {
		return invalid("invalid_slot_index", "execution.slot.index", "must not be negative")
	}
	return s.State.Validate("execution.state")
}

func (e *Execution) Snapshot() ExecutionSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return ExecutionSnapshot{
		ID:       e.id,
		TargetID: e.targetID,
		Slot:     e.slot,
		State:    e.state,
	}
}

func (e *Execution) CurrentState() ExecutionState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state
}

func (e *Execution) Transition(next ExecutionState) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !validExecutionTransition(e.state, next) {
		return invalid("invalid_execution_transition", "execution.state", "transition is not allowed")
	}
	e.state = next
	return nil
}

func (e *Execution) IsTerminal() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return isTerminalExecutionState(e.state)
}

func validExecutionTransition(current, next ExecutionState) bool {
	switch current {
	case ExecutionPending:
		return next == ExecutionReserved
	case ExecutionReserved:
		return next == ExecutionPreparing || next == ExecutionFailed
	case ExecutionPreparing:
		return next == ExecutionRunning || next == ExecutionFailed
	case ExecutionRunning:
		return next == ExecutionCleaning || next == ExecutionFailed
	case ExecutionCleaning:
		return next == ExecutionReleased || next == ExecutionCleanupFailed
	case ExecutionCleanupFailed:
		return next == ExecutionQuarantined
	default:
		return false
	}
}

func isTerminalExecutionState(state ExecutionState) bool {
	switch state {
	case ExecutionReleased, ExecutionFailed, ExecutionQuarantined:
		return true
	default:
		return false
	}
}

func isKnownExecutionState(state ExecutionState) bool {
	switch state {
	case ExecutionPending, ExecutionReserved, ExecutionPreparing, ExecutionRunning,
		ExecutionCleaning, ExecutionReleased, ExecutionFailed, ExecutionCleanupFailed,
		ExecutionQuarantined:
		return true
	default:
		return false
	}
}
