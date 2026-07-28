package store

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/runner"
)

func nodeOwnerSnapshot(
	nodeID domain.NodeID,
	epoch domain.ControllerEpoch,
	intent domain.AvailabilityIntent,
	exclusions []domain.TargetID,
) NodeAgentSnapshot {
	return NodeAgentSnapshot{
		NodeID:             nodeID,
		OS:                 domain.OSLinux,
		Architecture:       domain.ArchAMD64,
		RunnerVersion:      runner.OfficialRunnerVersion,
		NativeRunnerReady:  true,
		AvailabilityIntent: intent,
		ExcludedTargets:    exclusions,
		Journal:            AgentSnapshot{MaxControllerEpoch: epoch},
	}
}

func adoptedIntent(t *testing.T, controller *ControllerStore, nodeID string) (string, bool) {
	t.Helper()
	var intent sql.NullString
	if err := controller.db.QueryRow(
		`SELECT availability_intent FROM agent_session_snapshots WHERE node_id = ?`,
		nodeID,
	).Scan(&intent); err != nil {
		t.Fatal(err)
	}
	return intent.String, intent.Valid
}

// TestSharedRunnerIdentityAdoptionPreservesLastKnownWhenOmitted pins the
// fail-safe direction of the nil semantics: a snapshot or heartbeat that does
// not report the isolation mode keeps whatever was adopted. Silently resetting
// to false would downgrade a reported weakness into an unearned claim of uid
// isolation, which is the one wrong answer an operator cannot detect.
func TestSharedRunnerIdentityAdoptionPreservesLastKnownWhenOmitted(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-shared-identity.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 3)
	node := domain.NodeID(nodeID)

	// Never reported stays NULL: absent must not render as the isolated mode.
	if err := controller.RecordAgentSnapshot(
		ctx, nodeOwnerSnapshot(node, epoch, "", nil)); err != nil {
		t.Fatal(err)
	}
	if states, err := controller.ReadNodeOwnerStates(ctx); err != nil ||
		states[node].SharedRunnerIdentity != nil {
		t.Fatalf(
			"unreported isolation mode = %#v, err = %v, want nil",
			states[node].SharedRunnerIdentity, err,
		)
	}

	// A snapshot that reports the weaker mode adopts it.
	shared := true
	snapshot := nodeOwnerSnapshot(node, epoch, "", nil)
	snapshot.SharedRunnerIdentity = &shared
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	assertAdoptedSharedRunnerIdentity(t, controller, node, true)

	// A later snapshot that omits it keeps the adopted true.
	if err := controller.RecordAgentSnapshot(
		ctx, nodeOwnerSnapshot(node, epoch, "", nil)); err != nil {
		t.Fatal(err)
	}
	assertAdoptedSharedRunnerIdentity(t, controller, node, true)

	// A heartbeat that omits it keeps it too, even while adopting other state.
	digest := currentSnapshotDigest(t, controller, nodeID)
	if err := controller.RecordNodeOwnerState(
		ctx, node, digest, domain.AvailabilityStopped, nil, nil); err != nil {
		t.Fatal(err)
	}
	assertAdoptedSharedRunnerIdentity(t, controller, node, true)

	// Only an explicit report moves it back to the isolated mode.
	isolated := false
	if err := controller.RecordNodeOwnerState(
		ctx, node, digest, "", nil, &isolated); err != nil {
		t.Fatal(err)
	}
	assertAdoptedSharedRunnerIdentity(t, controller, node, false)

	// Adoption writes no audit event: it records no actor's decision.
	if count := auditActionCount(t, controller, AuditActionNodeAvailabilityChanged); count != 1 {
		t.Fatalf("availability audit events = %d, want only the intent change", count)
	}
}

func assertAdoptedSharedRunnerIdentity(
	t *testing.T,
	controller *ControllerStore,
	node domain.NodeID,
	want bool,
) {
	t.Helper()
	states, err := controller.ReadNodeOwnerStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	adopted := states[node].SharedRunnerIdentity
	if adopted == nil || *adopted != want {
		t.Fatalf("adopted shared runner identity = %#v, want %t", adopted, want)
	}
}

func auditActionCount(t *testing.T, controller *ControllerStore, action AuditAction) int {
	t.Helper()
	var count int
	if err := controller.db.QueryRow(
		`SELECT count(*) FROM management_audit_events WHERE action = ?`,
		action,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// TestRecordAgentSnapshotAdoptsOwnerStateInSnapshotTransaction proves the three
// distinct exclusion semantics the wire carries: absent keeps what was adopted,
// an explicit empty set clears it, and a populated set replaces it wholesale.
// Adoption is committed by the snapshot transaction itself, so it precedes any
// capacity advertisement after a reconnect.
func TestRecordAgentSnapshotAdoptsOwnerStateInSnapshotTransaction(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-owner-snapshot.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 3)
	node := domain.NodeID(nodeID)

	// A first snapshot without owner state adopts nothing and never reports an
	// intent the owner did not set.
	if err := controller.RecordAgentSnapshot(
		ctx, nodeOwnerSnapshot(node, epoch, "", nil)); err != nil {
		t.Fatal(err)
	}
	if _, reported := adoptedIntent(t, controller, nodeID); reported {
		t.Fatal("controller invented an availability intent")
	}
	if excluded, err := controller.ReadNodeTargetExclusions(ctx, node); err != nil ||
		len(excluded) != 0 {
		t.Fatalf("exclusions = (%#v, %v), want empty", excluded, err)
	}

	if err := controller.RecordAgentSnapshot(ctx, nodeOwnerSnapshot(
		node, epoch, domain.AvailabilityStopped,
		[]domain.TargetID{"target-b", "target-a"},
	)); err != nil {
		t.Fatal(err)
	}
	if intent, reported := adoptedIntent(t, controller, nodeID); !reported ||
		intent != string(domain.AvailabilityStopped) {
		t.Fatalf("adopted intent = (%q, %t), want stopped", intent, reported)
	}
	excluded, err := controller.ReadNodeTargetExclusions(ctx, node)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(excluded, []domain.TargetID{"target-a", "target-b"}) {
		t.Fatalf("adopted exclusions = %#v, want sorted [target-a target-b]", excluded)
	}

	// Absent owner state on a later snapshot is "no change reported".
	if err := controller.RecordAgentSnapshot(
		ctx, nodeOwnerSnapshot(node, epoch, "", nil)); err != nil {
		t.Fatal(err)
	}
	if intent, _ := adoptedIntent(t, controller, nodeID); intent != string(domain.AvailabilityStopped) {
		t.Fatalf("absent intent replaced the adopted one: %q", intent)
	}
	if excluded, err := controller.ReadNodeTargetExclusions(ctx, node); err != nil ||
		len(excluded) != 2 {
		t.Fatalf("absent exclusion set replaced the adopted one: %#v, %v", excluded, err)
	}

	// An explicit empty set is a distinct authoritative "no exclusions".
	if err := controller.RecordAgentSnapshot(ctx, nodeOwnerSnapshot(
		node, epoch, domain.AvailabilityAccepting, []domain.TargetID{},
	)); err != nil {
		t.Fatal(err)
	}
	if excluded, err := controller.ReadNodeTargetExclusions(ctx, node); err != nil ||
		len(excluded) != 0 {
		t.Fatalf("explicit empty set did not clear adoption: %#v, %v", excluded, err)
	}
	if intent, _ := adoptedIntent(t, controller, nodeID); intent != string(domain.AvailabilityAccepting) {
		t.Fatalf("adopted intent = %q, want accepting", intent)
	}
}

// TestNodeOwnerAdoptionAuditsExactlyOnChange keeps the audit trail meaningful:
// a reconnect that re-reports the same owner state must not append a redundant
// event, while every real change is recorded with the node as actor.
func TestNodeOwnerAdoptionAuditsExactlyOnChange(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-owner-audit.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 3)
	node := domain.NodeID(nodeID)

	snapshot := nodeOwnerSnapshot(
		node, epoch, domain.AvailabilityStopped, []domain.TargetID{"target-a"})
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if got := auditActionCount(t, controller, AuditActionNodeAvailabilityChanged); got != 1 {
		t.Fatalf("availability audit rows = %d, want 1", got)
	}
	if got := auditActionCount(t, controller, AuditActionNodeTargetExclusionChanged); got != 1 {
		t.Fatalf("exclusion audit rows = %d, want 1", got)
	}
	var actor AuditActor
	var resourceID string
	if err := controller.db.QueryRow(
		`SELECT actor, resource_id FROM management_audit_events WHERE action = ?`,
		AuditActionNodeTargetExclusionChanged,
	).Scan(&actor, &resourceID); err != nil {
		t.Fatal(err)
	}
	if actor != AuditActorNode || resourceID != nodeID {
		t.Fatalf("audit actor/resource = (%s, %s), want (node, %s)", actor, resourceID, nodeID)
	}

	// Re-adopting the identical state is a no-op for the audit log.
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if got := auditActionCount(t, controller, AuditActionNodeAvailabilityChanged); got != 1 {
		t.Fatalf("no-op re-adoption appended availability audit: %d rows", got)
	}
	if got := auditActionCount(t, controller, AuditActionNodeTargetExclusionChanged); got != 1 {
		t.Fatalf("no-op re-adoption appended exclusion audit: %d rows", got)
	}
}

// TestNodeOwnerAdoptionFailsClosedWhenAuditDegraded proves the adoption is
// bound to its audit evidence: with audit authority degraded the owner change
// is refused outright rather than adopted silently.
func TestNodeOwnerAdoptionFailsClosedWhenAuditDegraded(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-owner-audit-degraded.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 3)
	node := domain.NodeID(nodeID)
	if err := controller.RecordAgentSnapshot(
		ctx, nodeOwnerSnapshot(node, epoch, "", nil)); err != nil {
		t.Fatal(err)
	}
	digest := currentSnapshotDigest(t, controller, nodeID)
	controller.degradeManagementAudit()

	exclusions := []domain.TargetID{"target-a"}
	if err := controller.RecordNodeOwnerState(
		ctx, node, digest, domain.AvailabilityStopped, &exclusions, nil,
	); !errors.Is(err, ErrManagementAuditPersistence) {
		t.Fatalf("heartbeat adoption error = %v, want audit persistence", err)
	}
	if excluded, err := controller.ReadNodeTargetExclusions(ctx, node); err != nil ||
		len(excluded) != 0 {
		t.Fatalf("degraded audit still adopted exclusions: %#v, %v", excluded, err)
	}
	if err := controller.RecordAgentSnapshot(ctx, nodeOwnerSnapshot(
		node, epoch, domain.AvailabilityStopped, exclusions,
	)); !errors.Is(err, ErrManagementAuditPersistence) {
		t.Fatalf("snapshot adoption error = %v, want audit persistence", err)
	}
}

// TestRecordNodeOwnerStateRequiresCurrentSnapshotAuthority mirrors the
// readiness lease guard: a heartbeat may only adopt owner state for the exact
// full journal that activated the session it arrived on.
func TestRecordNodeOwnerStateRequiresCurrentSnapshotAuthority(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-owner-digest.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 3)
	node := domain.NodeID(nodeID)
	if err := controller.RecordAgentSnapshot(
		ctx, nodeOwnerSnapshot(node, epoch, "", nil)); err != nil {
		t.Fatal(err)
	}
	exclusions := []domain.TargetID{"target-a"}

	stale := "0000000000000000000000000000000000000000000000000000000000000000"
	if err := controller.RecordNodeOwnerState(
		ctx, node, stale, "", &exclusions, nil); err == nil {
		t.Fatal("stale snapshot digest adopted owner state")
	}
	if excluded, err := controller.ReadNodeTargetExclusions(ctx, node); err != nil ||
		len(excluded) != 0 {
		t.Fatalf("stale adoption wrote rows: %#v, %v", excluded, err)
	}

	digest := currentSnapshotDigest(t, controller, nodeID)
	if err := controller.RecordNodeOwnerState(
		ctx, node, digest, domain.AvailabilityAccepting, &exclusions, nil); err != nil {
		t.Fatal(err)
	}
	excluded, err := controller.ReadNodeTargetExclusions(ctx, node)
	if err != nil || !reflect.DeepEqual(excluded, exclusions) {
		t.Fatalf("adopted exclusions = (%#v, %v), want %#v", excluded, err, exclusions)
	}

	// Nothing reported is an explicit no-op rather than a wipe.
	if err := controller.RecordNodeOwnerState(ctx, node, digest, "", nil, nil); err != nil {
		t.Fatal(err)
	}
	if excluded, err := controller.ReadNodeTargetExclusions(ctx, node); err != nil ||
		len(excluded) != 1 {
		t.Fatalf("no-change adoption wiped exclusions: %#v, %v", excluded, err)
	}
}

// TestReadNodeOwnerStatesBulkReadMatchesPerNodeReads proves the bulk reader
// used by the management API listing agrees with the per-node primitives:
// a node that never reported is absent, a node that only excluded targets has
// a nil intent, and a node that reported both surfaces both.
func TestReadNodeOwnerStatesBulkReadMatchesPerNodeReads(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-owner-bulk.db")
	defer controller.Close()

	silentID, silentEpoch := enrollControllerAgentNode(t, controller, 3)
	silent := domain.NodeID(silentID)
	if err := controller.RecordAgentSnapshot(
		ctx, nodeOwnerSnapshot(silent, silentEpoch, "", nil)); err != nil {
		t.Fatal(err)
	}

	exclusionOnlyID, exclusionOnlyEpoch := enrollControllerAgentNode(t, controller, 4)
	exclusionOnly := domain.NodeID(exclusionOnlyID)
	if err := controller.RecordAgentSnapshot(ctx, nodeOwnerSnapshot(
		exclusionOnly, exclusionOnlyEpoch, "", []domain.TargetID{"target-a"},
	)); err != nil {
		t.Fatal(err)
	}

	fullID, fullEpoch := enrollControllerAgentNode(t, controller, 5)
	full := domain.NodeID(fullID)
	if err := controller.RecordAgentSnapshot(ctx, nodeOwnerSnapshot(
		full, fullEpoch, domain.AvailabilityStopped,
		[]domain.TargetID{"target-b", "target-a"},
	)); err != nil {
		t.Fatal(err)
	}

	states, err := controller.ReadNodeOwnerStates(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, reported := states[silent]; reported {
		t.Fatalf("silent node reported owner state: %#v", states[silent])
	}

	exclusionOnlyState, reported := states[exclusionOnly]
	if !reported {
		t.Fatal("exclusion-only node missing from bulk read")
	}
	if exclusionOnlyState.Intent != nil {
		t.Fatalf("exclusion-only node intent = %v, want nil", *exclusionOnlyState.Intent)
	}
	if !reflect.DeepEqual(exclusionOnlyState.Exclusions, []domain.TargetID{"target-a"}) {
		t.Fatalf("exclusion-only exclusions = %#v, want [target-a]", exclusionOnlyState.Exclusions)
	}

	fullState, reported := states[full]
	if !reported {
		t.Fatal("full node missing from bulk read")
	}
	if fullState.Intent == nil || *fullState.Intent != domain.AvailabilityStopped {
		t.Fatalf("full node intent = %v, want stopped", fullState.Intent)
	}
	if !reflect.DeepEqual(fullState.Exclusions, []domain.TargetID{"target-a", "target-b"}) {
		t.Fatalf("full node exclusions = %#v, want sorted [target-a target-b]", fullState.Exclusions)
	}
}

func currentSnapshotDigest(t *testing.T, controller *ControllerStore, nodeID string) string {
	t.Helper()
	var digest string
	if err := controller.db.QueryRow(
		`SELECT snapshot_digest FROM agent_snapshot_authority WHERE node_id = ?`,
		nodeID,
	).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	return digest
}
