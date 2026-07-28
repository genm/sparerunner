package app

import (
	"context"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/store"
)

// TestControllerRunnerEmptyPollReconcilesAssignedDemand proves the coordinator
// re-evaluates desired runner count when a long poll returns no message. The
// pinned scale-set listener does exactly this on a nil message; without it a job
// assigned while the Controller was busy waits for the next unrelated message
// before anything is created.
func TestControllerRunnerEmptyPollReconcilesAssignedDemand(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	execution := domain.ExecutionSnapshot{
		ID:       "twk-exec-assigned-demand",
		TargetID: "target-1",
		Slot:     domain.SlotKey{NodeID: domain.NodeID("00000000000000000000000000000001"), Index: 0},
		State:    domain.ExecutionReserved,
	}
	created := store.GitHubJobClaim{
		ScaleSetID: 7,
		ClaimKey:   -1,
		Origin:     store.GitHubClaimFromAssignedDemand,
		Execution:  execution,
		State:      store.GitHubClaimAcquired,
	}
	stateStore.assignedDemand = store.GitHubAssignedDemandResult{
		Observed: true,
		Desired:  1,
		Active:   0,
		Created:  &created,
	}
	session := newRunnerCoordinatorFakeSession(nil)
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	agent.setNativeRunnerReady(true)
	reconciler := &recordingGitHubClaimReconciler{}
	coordinator := newControllerRunnerForEpochWithReconciler(
		t,
		stateStore,
		session,
		agent,
		newRunnerCoordinatorFakeLifecycle(),
		3,
		reconciler,
	)

	message, err := coordinator.PollOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if message != nil {
		t.Fatalf("empty poll returned message %#v", message)
	}
	bindings := stateStore.assignedDemandBindings()
	if len(bindings) != 1 {
		t.Fatalf("assigned demand reconciliations = %d, want 1", len(bindings))
	}
	// The demand reconciliation must be bound to the same node and authority the
	// poll advertised capacity for, and must carry the claim gate rather than
	// bypassing it.
	if bindings[0].NodeID != domain.NodeID("00000000000000000000000000000001") ||
		bindings[0].TargetID != "target-1" ||
		bindings[0].Slot != 0 ||
		!bindings[0].ClaimEnabled ||
		bindings[0].PollAuthority.AdvertisedCapacity != 1 {
		t.Fatalf("assigned demand binding = %#v", bindings[0])
	}
	if applied := reconciler.appliedClaims(); len(applied) != 1 ||
		applied[0].ClaimKey != created.ClaimKey ||
		applied[0].Origin != store.GitHubClaimFromAssignedDemand {
		t.Fatalf("projected claims = %#v", reconciler.appliedClaims())
	}
}

// TestControllerRunnerEmptyPollWithoutCapacityStillReportsDemand proves an
// unservable node reconciles with the gate closed, so nothing is created and the
// shortfall is not silently dropped.
func TestControllerRunnerEmptyPollWithoutCapacityStillReportsDemand(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	stateStore.capacity = 0
	stateStore.assignedDemand = store.GitHubAssignedDemandResult{
		Observed: true,
		Desired:  1,
		Active:   0,
		Unserved: 1,
	}
	session := newRunnerCoordinatorFakeSession(nil)
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	agent.setNativeRunnerReady(true)
	reconciler := &recordingGitHubClaimReconciler{}
	coordinator := newControllerRunnerForEpochWithReconciler(
		t,
		stateStore,
		session,
		agent,
		newRunnerCoordinatorFakeLifecycle(),
		3,
		reconciler,
	)

	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	bindings := stateStore.assignedDemandBindings()
	if len(bindings) != 1 {
		t.Fatalf("assigned demand reconciliations = %d, want 1", len(bindings))
	}
	if bindings[0].ClaimEnabled {
		t.Fatalf("zero-capacity poll enabled the claim gate: %#v", bindings[0])
	}
	if applied := reconciler.appliedClaims(); len(applied) != 0 {
		t.Fatalf("zero-capacity poll projected claims = %#v", applied)
	}
}

// recordingGitHubClaimReconciler accepts everything the accepting reconciler
// does and additionally records the claims projected into it.
type recordingGitHubClaimReconciler struct {
	acceptingControllerRunnerReconciler
	claims []store.GitHubJobClaim
}

func (reconciler *recordingGitHubClaimReconciler) ApplyGitHubClaim(
	claim store.GitHubJobClaim,
) error {
	reconciler.claims = append(reconciler.claims, claim)
	return nil
}

func (reconciler *recordingGitHubClaimReconciler) appliedClaims() []store.GitHubJobClaim {
	return append([]store.GitHubJobClaim(nil), reconciler.claims...)
}
