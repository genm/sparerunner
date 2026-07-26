package domain

import "testing"

func TestExecutionErrorCodeIsClosed(t *testing.T) {
	valid := []ExecutionErrorCode{
		ExecutionErrorNone,
		ExecutionErrorConflict,
		ExecutionErrorReconciliation,
		ExecutionErrorQuarantined,
		ExecutionErrorCleanup,
		ExecutionErrorStart,
		ExecutionErrorPlatform,
		ExecutionErrorJournal,
		ExecutionErrorCommandRejected,
	}
	for _, code := range valid {
		if err := code.Validate("execution.error_code"); err != nil {
			t.Fatalf("Validate(%q): %v", code, err)
		}
	}
	if err := ExecutionErrorCode("runner output canary").Validate("execution.error_code"); err == nil {
		t.Fatal("unknown execution error code was accepted")
	}
}

func TestExecutionResultRejectsInconsistentStateAndError(t *testing.T) {
	if err := ValidateExecutionResult(ExecutionRunning, ExecutionErrorNone, "execution"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateExecutionResult(ExecutionFailed, ExecutionErrorStart, "execution"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateExecutionResult(ExecutionCleaning, ExecutionErrorReconciliation, "execution"); err == nil {
		t.Fatal("non-failed state accepted an error code")
	}
	if err := ValidateExecutionResult(ExecutionCleanupFailed, ExecutionErrorNone, "execution"); err == nil {
		t.Fatal("failed state accepted an empty error code")
	}
}
