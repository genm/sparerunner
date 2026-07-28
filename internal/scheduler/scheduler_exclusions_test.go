package scheduler

import (
	"testing"

	"github.com/genm/sparerunner/internal/domain"
)

// TestExcludedTargetRemovesOnlyThatTargetsCandidates proves the node-selection
// filter is subtractive and per-Target: the owner's exclusion removes one node
// from one Target's candidate set without touching that node's service of other
// Targets or other nodes' service of the excluded one.
func TestExcludedTargetRemovesOnlyThatTargetsCandidates(t *testing.T) {
	excluded := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1)
	excluded.ExcludedTargets = []domain.TargetID{"alpha"}
	open := testNodeSnapshot("node-b", domain.OSLinux, domain.ArchAMD64, 1)
	targets := []TargetSpec{
		testTargetSpec("alpha", nil, nil),
		testTargetSpec("beta", nil, nil),
	}
	instance := newTestScheduler(t, []NodeSnapshot{excluded, open}, targets, nil)

	// alpha may only land on node-b; the single grant proves node-a was skipped
	// rather than merely deprioritized.
	grant, ok, err := instance.GrantNext([]domain.TargetID{"alpha"})
	if err != nil || !ok {
		t.Fatalf("alpha grant = (%#v, %t, %v)", grant, ok, err)
	}
	if grant.Slot.NodeID != "node-b" {
		t.Fatalf("alpha placed on %q, want node-b", grant.Slot.NodeID)
	}
	if _, ok, err := instance.GrantNext([]domain.TargetID{"alpha"}); err != nil || ok {
		t.Fatalf("alpha found capacity on the excluding node: ok = %t, err = %v", ok, err)
	}

	// beta is untouched by the exclusion and still places on node-a.
	betaGrant, ok, err := instance.GrantNext([]domain.TargetID{"beta"})
	if err != nil || !ok {
		t.Fatalf("beta grant = (%#v, %t, %v)", betaGrant, ok, err)
	}
	if betaGrant.Slot.NodeID != "node-a" {
		t.Fatalf("beta placed on %q, want node-a", betaGrant.Slot.NodeID)
	}
}

// TestUpdateNodeAppliesAndClearsExclusions covers the mid-session path: an
// adopted exclusion takes effect on the next placement, and including the
// Target again restores the node as a candidate.
func TestUpdateNodeAppliesAndClearsExclusions(t *testing.T) {
	node := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1)
	targets := []TargetSpec{testTargetSpec("alpha", nil, nil)}
	instance := newTestScheduler(t, []NodeSnapshot{node}, targets, nil)

	excluded := node
	excluded.ExcludedTargets = []domain.TargetID{"alpha"}
	if err := instance.UpdateNode(excluded); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := instance.GrantNext([]domain.TargetID{"alpha"}); err != nil || ok {
		t.Fatalf("excluded node still granted: ok = %t, err = %v", ok, err)
	}

	included := node
	included.ExcludedTargets = []domain.TargetID{}
	if err := instance.UpdateNode(included); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := instance.GrantNext([]domain.TargetID{"alpha"}); err != nil || !ok {
		t.Fatalf("included node did not grant: ok = %t, err = %v", ok, err)
	}
}

// TestNodeSnapshotRejectsEmptyExcludedTarget keeps an unusable identity out of
// the placement filter rather than silently ignoring it.
func TestNodeSnapshotRejectsEmptyExcludedTarget(t *testing.T) {
	node := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1)
	node.ExcludedTargets = []domain.TargetID{" "}
	if err := node.Validate(); err == nil {
		t.Fatal("empty excluded target identity was accepted")
	}
}

// TestExcludedTargetsAreClonedIntoNodeState guards against the scheduler
// aliasing caller storage: a caller mutating its slice after the update must
// not silently change placement.
func TestExcludedTargetsAreClonedIntoNodeState(t *testing.T) {
	node := testNodeSnapshot("node-a", domain.OSLinux, domain.ArchAMD64, 1)
	node.ExcludedTargets = []domain.TargetID{"alpha"}
	targets := []TargetSpec{testTargetSpec("alpha", nil, nil)}
	instance := newTestScheduler(t, []NodeSnapshot{node}, targets, nil)

	node.ExcludedTargets[0] = "beta"
	if _, ok, err := instance.GrantNext([]domain.TargetID{"alpha"}); err != nil || ok {
		t.Fatalf("caller mutation changed placement: ok = %t, err = %v", ok, err)
	}
}
