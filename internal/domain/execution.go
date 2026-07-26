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
	ID       ExecutionID
	TargetID TargetID
	Slot     SlotKey
	State    ExecutionState
}

func NewExecution(id ExecutionID, targetID TargetID, slot SlotKey) (*Execution, error) {
	if err := required(string(id), "execution.id"); err != nil {
		return nil, err
	}
	if err := required(string(targetID), "execution.target_id"); err != nil {
		return nil, err
	}
	if err := required(string(slot.NodeID), "execution.slot.node_id"); err != nil {
		return nil, err
	}
	if slot.Index < 0 {
		return nil, invalid("invalid_slot_index", "execution.slot.index", "must not be negative")
	}
	return &Execution{ID: id, TargetID: targetID, Slot: slot, State: ExecutionPending}, nil
}

func (e *Execution) CurrentState() ExecutionState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.State
}

func (e *Execution) Transition(next ExecutionState) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !validExecutionTransition(e.State, next) {
		return invalid("invalid_execution_transition", "execution.state", "transition is not allowed")
	}
	e.State = next
	return nil
}

func (e *Execution) IsTerminal() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return isTerminalExecutionState(e.State)
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
