package reconcile

import (
	"testing"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

func exclusionTestController(t *testing.T) *Controller {
	t.Helper()
	return restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)}})
}

func exclusionSnapshot(excluded *[]domain.TargetID) transport.AgentSnapshot {
	return transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		ExcludedTargets:    excluded,
	}
}

// TestSnapshotExclusionsProjectAndHonorAbsence proves the projection follows the
// wire semantics exactly: a present set replaces, an absent one is "no change
// reported" and keeps what was last adopted.
func TestSnapshotExclusionsProjectAndHonorAbsence(t *testing.T) {
	controller := exclusionTestController(t)
	result, err := controller.ReconcileAgentSnapshot(
		exclusionSnapshot(transport.TargetIDSet("target-a")))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Scheduler.ExcludedTargets) != 1 ||
		result.Scheduler.ExcludedTargets[0] != "target-a" {
		t.Fatalf("projected exclusions = %#v, want [target-a]", result.Scheduler.ExcludedTargets)
	}

	absent, err := controller.ReconcileAgentSnapshot(exclusionSnapshot(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(absent.Scheduler.ExcludedTargets) != 1 {
		t.Fatalf("absent set discarded the adopted exclusions: %#v", absent.Scheduler.ExcludedTargets)
	}

	cleared, err := controller.ReconcileAgentSnapshot(
		exclusionSnapshot(transport.TargetIDSet()))
	if err != nil {
		t.Fatal(err)
	}
	if len(cleared.Scheduler.ExcludedTargets) != 0 {
		t.Fatalf("explicit empty set did not clear: %#v", cleared.Scheduler.ExcludedTargets)
	}
}

// TestDegradationKeepsExclusions is the fail-closed half: every path that zeroes
// NativeReady must leave the owner's exclusions intact. Losing them during
// degradation would let a Target the owner withdrew become placeable again the
// moment the node recovers.
func TestDegradationKeepsExclusions(t *testing.T) {
	for _, test := range []struct {
		name    string
		degrade func(t *testing.T, controller *Controller) NodeResult
	}{
		{
			name: "disconnect",
			degrade: func(t *testing.T, controller *Controller) NodeResult {
				t.Helper()
				result, err := controller.Disconnect("node-a")
				if err != nil {
					t.Fatal(err)
				}
				return result
			},
		},
		{
			name: "administrative drain",
			degrade: func(t *testing.T, controller *Controller) NodeResult {
				t.Helper()
				if err := controller.SetAdministrativeState(
					"node-a", domain.NodeDraining, false); err != nil {
					t.Fatal(err)
				}
				fleet := controller.FleetSnapshot()
				return NodeResult{Scheduler: fleet.Nodes[0]}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller := exclusionTestController(t)
			if _, err := controller.ReconcileAgentSnapshot(
				exclusionSnapshot(transport.TargetIDSet("target-a"))); err != nil {
				t.Fatal(err)
			}
			result := test.degrade(t, controller)
			if result.Scheduler.NativeReady {
				t.Fatalf("degradation left the node ready: %#v", result.Scheduler)
			}
			if len(result.Scheduler.ExcludedTargets) != 1 ||
				result.Scheduler.ExcludedTargets[0] != "target-a" {
				t.Fatalf("degradation dropped exclusions: %#v", result.Scheduler.ExcludedTargets)
			}
		})
	}
}

// TestApplyNodeOwnerStateUpdatesProjectionMidSession covers the heartbeat path:
// adoption reaches the placement projection without waiting for a reconnect.
func TestApplyNodeOwnerStateUpdatesProjectionMidSession(t *testing.T) {
	controller := exclusionTestController(t)
	if _, err := controller.ReconcileAgentSnapshot(exclusionSnapshot(nil)); err != nil {
		t.Fatal(err)
	}
	if err := controller.ApplyNodeOwnerState(
		"node-a", domain.AvailabilityStopped, []domain.TargetID{"target-a"},
	); err != nil {
		t.Fatal(err)
	}
	fleet := controller.FleetSnapshot()
	if len(fleet.Nodes) != 1 ||
		len(fleet.Nodes[0].ExcludedTargets) != 1 ||
		fleet.Nodes[0].ExcludedTargets[0] != "target-a" {
		t.Fatalf("mid-session adoption did not reach the projection: %#v", fleet.Nodes)
	}

	// Reporting nothing is an explicit no-op, not a wipe.
	if err := controller.ApplyNodeOwnerState("node-a", "", nil); err != nil {
		t.Fatal(err)
	}
	if fleet := controller.FleetSnapshot(); len(fleet.Nodes[0].ExcludedTargets) != 1 {
		t.Fatalf("no-change adoption wiped exclusions: %#v", fleet.Nodes[0].ExcludedTargets)
	}

	// An unknown node cannot invent projection state.
	if err := controller.ApplyNodeOwnerState(
		"node-missing", "", []domain.TargetID{"target-a"}); err == nil {
		t.Fatal("unknown node adopted owner state")
	}
}
