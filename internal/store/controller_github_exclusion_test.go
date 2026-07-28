package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/genm/tewake/internal/domain"
)

// TestNodeTargetExclusionWithholdsCapacityClaimAndRecovery exercises all three
// consumers of the single slot-availability predicate. Enforcement lives in one
// place precisely so advertised capacity, the claim boundary, and fresh-recovery
// admission cannot disagree; this proves each refuses an excluded Target and
// admits the same binding once the owner includes it again.
func TestNodeTargetExclusionWithholdsCapacityClaimAndRecovery(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-exclusion.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 6)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	enableGitHubClaimForTest(t, controller, &binding, 7, domain.ArchAMD64)

	// Baseline: every path admits the binding before any exclusion is adopted.
	assertSlotAdmission(t, controller, binding, true)

	digest := currentSnapshotDigest(t, controller, nodeID)
	exclusions := []domain.TargetID{binding.TargetID}
	if err := controller.RecordNodeOwnerState(
		ctx, binding.NodeID, digest, "", &exclusions, nil); err != nil {
		t.Fatal(err)
	}

	assertSlotAdmission(t, controller, binding, false)
	commit, err := controller.CommitGitHubQueueMessage(
		ctx, githubQueueMessageForTest(7, 301, 701), binding)
	if err != nil {
		t.Fatal(err)
	}
	if commit.Claim != nil {
		t.Fatalf("excluded target claimed execution: %#v", commit.Claim)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)

	// A different Target on the same node is untouched: exclusion is per-Target,
	// not a node-wide stop.
	otherBinding := binding
	otherBinding.TargetID = "target-github-other"
	if capacity, err := controller.GitHubSingleSlotCapacity(
		ctx, otherBinding); err != nil || capacity != 1 {
		t.Fatalf("unexcluded sibling target capacity = (%d, %v), want 1", capacity, err)
	}

	// Including the Target again restores every path.
	empty := []domain.TargetID{}
	if err := controller.RecordNodeOwnerState(
		ctx, binding.NodeID, digest, "", &empty, nil); err != nil {
		t.Fatal(err)
	}
	assertSlotAdmission(t, controller, binding, true)
	included, err := controller.CommitGitHubQueueMessage(
		ctx, githubQueueMessageForTest(7, 302, 702), binding)
	if err != nil {
		t.Fatal(err)
	}
	if included.Claim == nil {
		t.Fatal("included target did not claim available work")
	}
}

// assertSlotAdmission checks the advertised-capacity read and the fresh-recovery
// admission guard, the two paths that can be observed without also committing a
// claim.
func assertSlotAdmission(
	t *testing.T,
	controller *ControllerStore,
	binding SingleSlotBinding,
	want bool,
) {
	t.Helper()
	ctx := context.Background()
	capacity, err := controller.GitHubSingleSlotCapacity(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	wantCapacity := 0
	if want {
		wantCapacity = 1
	}
	if capacity != wantCapacity {
		t.Fatalf("advertised capacity = %d, want %d", capacity, wantCapacity)
	}

	tx, err := controller.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	admissionErr := requireGitHubFreshRecoveryAdmission(ctx, tx, binding, 1000)
	if want && admissionErr != nil {
		t.Fatalf("recovery admission = %v, want admitted", admissionErr)
	}
	if !want && !errors.Is(admissionErr, ErrGitHubRecoveryAvailabilityPending) {
		t.Fatalf("recovery admission = %v, want pending", admissionErr)
	}
}
