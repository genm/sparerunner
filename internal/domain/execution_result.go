package domain

// ExecutionErrorCode is the persistence-safe failure classification shared by
// Agent journals, Controller observations, and the transport adapter. Raw
// runner, workspace, process, and JIT errors never cross this boundary.
type ExecutionErrorCode string

const (
	ExecutionErrorNone            ExecutionErrorCode = ""
	ExecutionErrorConflict        ExecutionErrorCode = "execution_conflict"
	ExecutionErrorReconciliation  ExecutionErrorCode = "reconciliation_required"
	ExecutionErrorQuarantined     ExecutionErrorCode = "quarantined"
	ExecutionErrorCleanup         ExecutionErrorCode = "cleanup_failed"
	ExecutionErrorStart           ExecutionErrorCode = "start_failed"
	ExecutionErrorPlatform        ExecutionErrorCode = "platform_unavailable"
	ExecutionErrorJournal         ExecutionErrorCode = "journal_failed"
	ExecutionErrorCommandRejected ExecutionErrorCode = "command_rejected"
	// ExecutionErrorTargetExcluded is the node owner's own refusal. The agent
	// re-reads its durable per-Target exclusion set at the exec boundary, so a
	// stale controller dispatch is refused as a classified execution failure
	// rather than as a transport rejection that would redeliver forever.
	ExecutionErrorTargetExcluded ExecutionErrorCode = "target_excluded"
)

func (code ExecutionErrorCode) Validate(field string) error {
	switch code {
	case ExecutionErrorNone, ExecutionErrorConflict, ExecutionErrorReconciliation,
		ExecutionErrorQuarantined, ExecutionErrorCleanup, ExecutionErrorStart,
		ExecutionErrorPlatform, ExecutionErrorJournal, ExecutionErrorCommandRejected,
		ExecutionErrorTargetExcluded:
		return nil
	default:
		return invalid("invalid_execution_error_code", field, "is not a known execution error code")
	}
}

// ValidateExecutionResult keeps state/error combinations consistent across the
// Agent outbox, transport, and Controller persistence boundaries.
func ValidateExecutionResult(state ExecutionState, code ExecutionErrorCode, field string) error {
	if err := state.Validate(field + ".state"); err != nil {
		return err
	}
	if err := code.Validate(field + ".error_code"); err != nil {
		return err
	}
	switch state {
	case ExecutionFailed, ExecutionCleanupFailed, ExecutionQuarantined:
		if code == ExecutionErrorNone {
			return invalid("missing_execution_error_code", field+".error_code", "is required for a failed execution state")
		}
	default:
		if code != ExecutionErrorNone {
			return invalid("unexpected_execution_error_code", field+".error_code", "is not allowed for a non-failed execution state")
		}
	}
	return nil
}

// CleanupFailureCode is the persistence-safe cleanup tombstone classification.
// Raw filesystem, process, workspace, and runner errors must never be stored or
// transported.
type CleanupFailureCode string

const (
	CleanupVerificationFailed CleanupFailureCode = "cleanup_verification_failed"
	CleanupProcessResidue     CleanupFailureCode = "process_residue"
	CleanupWorkspaceRemoval   CleanupFailureCode = "workspace_removal_failed"
)

func (code CleanupFailureCode) Validate(field string) error {
	switch code {
	case CleanupVerificationFailed, CleanupProcessResidue, CleanupWorkspaceRemoval:
		return nil
	default:
		return invalid("invalid_cleanup_failure_code", field, "is not a known cleanup failure code")
	}
}
