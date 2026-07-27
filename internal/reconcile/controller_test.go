package reconcile

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

func TestStartAdvancesEpochAndStartsEveryNodeOfflineWithZeroCapacity(t *testing.T) {
	authority := &memoryAuthority{
		nextEpoch: 4,
		snapshot: store.ControllerSnapshot{
			Nodes: []store.NodeAdministration{
				{NodeID: "node-b", State: domain.NodeActive},
				{NodeID: "node-a", State: domain.NodeActive},
			},
		},
	}
	controller, err := Start(context.Background(), authority, Config{
		Nodes: []NodeDefinition{
			testNodeDefinition("node-b", 1),
			testNodeDefinition("node-a", 1),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if controller.Epoch() != 4 || authority.advanceCalls != 1 || authority.snapshotCalls != 1 {
		t.Fatalf("startup authority = epoch %d, advances %d, snapshots %d",
			controller.Epoch(), authority.advanceCalls, authority.snapshotCalls)
	}

	fleet := controller.FleetSnapshot()
	if len(fleet.Nodes) != 2 {
		t.Fatalf("nodes = %#v", fleet.Nodes)
	}
	for _, node := range fleet.Nodes {
		if node.Node.ObservedState != domain.NodeOffline || node.Reconciled || node.NativeReady {
			t.Fatalf("startup node advertised capacity: %#v", node)
		}
	}
	if got := []domain.NodeID{fleet.Nodes[0].Node.ID, fleet.Nodes[1].Node.ID}; got[0] != "node-a" || got[1] != "node-b" {
		t.Fatalf("node order = %v", got)
	}
}

func TestRestoreRestartKeepsUnobservedNodeAtZeroUntilFreshSnapshotDefinesPlatform(t *testing.T) {
	restarted, err := RestoreRestart(store.ControllerRestartSnapshot{
		Controller: store.ControllerSnapshot{
			ControllerEpoch: 4,
			Nodes: []store.NodeAdministration{{
				NodeID: "node-new",
				State:  domain.NodeActive,
			}},
		},
		NodeTopology: []store.RestartNodeTopology{{
			NodeID:              "node-new",
			CertificateSerial:   "certificate-new",
			CredentialEpoch:     1,
			AdministrativeState: domain.NodeActive,
			MaxRunners:          1,
		}},
	}, func() time.Time { return time.Unix(500, 0) })
	if err != nil {
		t.Fatal(err)
	}
	before := restarted.FleetSnapshot()
	if len(before.Nodes) != 0 ||
		len(before.Statuses) != 1 ||
		before.Statuses[0].Phase != NodeOffline {
		t.Fatalf("unobserved restart fleet = %#v", before)
	}

	result, err := restarted.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-new",
		OS:                 domain.OSMacOS,
		Arch:               domain.ArchARM64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status.Phase != NodeReady ||
		!result.Scheduler.Reconciled ||
		!result.Scheduler.NativeReady ||
		result.Scheduler.Node.OS != domain.OSMacOS ||
		result.Scheduler.Node.Architecture != domain.ArchARM64 {
		t.Fatalf("first authenticated platform result = %#v", result)
	}
	after := restarted.FleetSnapshot()
	if len(after.Nodes) != 1 || after.Nodes[0].Node.OS != domain.OSMacOS {
		t.Fatalf("observed restart fleet = %#v", after)
	}

	if _, err := restarted.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-new",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchARM64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 4,
	}); !hasCode(err, "node_platform_mismatch") {
		t.Fatalf("changed platform error = %v", err)
	}
}

func TestStartFailsClosedWhenEpochSnapshotDoesNotMatchAdvance(t *testing.T) {
	authority := &memoryAuthority{
		nextEpoch: 8,
		snapshot: store.ControllerSnapshot{
			ControllerEpoch: 7,
			Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
		},
		preserveSnapshotEpoch: true,
	}
	if _, err := Start(context.Background(), authority, Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
	}); !hasCode(err, "controller_epoch_mismatch") {
		t.Fatalf("epoch mismatch = %v", err)
	}
}

func TestReconcileNodesIndependentlyAndPreserveRunningJobOnDisconnect(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-a",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionRunning,
	}
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes: []store.NodeAdministration{
			{NodeID: "node-a", State: domain.NodeActive},
			{NodeID: "node-b", State: domain.NodeActive},
		},
		Reservations: []store.SlotReservation{{
			Slot:  execution.Slot,
			Owner: domain.SlotOwner{TargetID: execution.TargetID, ExecutionID: execution.ID},
		}},
		Executions: []domain.ExecutionSnapshot{execution},
	}, Config{
		Nodes: []NodeDefinition{
			testNodeDefinition("node-a", 1),
			testNodeDefinition("node-b", 1),
		},
	})

	result, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-b",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Scheduler.Reconciled || !result.Scheduler.NativeReady ||
		result.Status.Phase != NodeReady {
		t.Fatalf("independent node result = %#v", result)
	}
	fleet := controller.FleetSnapshot()
	if fleet.Nodes[0].Node.ID != "node-a" || fleet.Nodes[0].Reconciled {
		t.Fatalf("offline node unexpectedly reconciled: %#v", fleet.Nodes[0])
	}
	if fleet.Nodes[1].Node.ID != "node-b" || !fleet.Nodes[1].Reconciled {
		t.Fatalf("online node did not restore independently: %#v", fleet.Nodes[1])
	}

	running, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        execution.ID,
			State:              domain.ExecutionRunning,
			ObservedAtUnixNano: 10,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !running.Scheduler.Reconciled ||
		len(running.Scheduler.ActiveExecutions) != 1 ||
		running.Scheduler.ActiveExecutions[0] != execution.ID {
		t.Fatalf("running reconciliation = %#v", running)
	}
	disconnected, err := controller.Disconnect("node-a")
	if err != nil {
		t.Fatal(err)
	}
	if disconnected.Status.Phase != NodeOffline ||
		disconnected.Scheduler.Reconciled ||
		disconnected.Scheduler.NativeReady ||
		len(disconnected.Scheduler.ActiveExecutions) != 1 ||
		len(disconnected.SuppressedReservations) != 1 {
		t.Fatalf("disconnect discarded local job authority: %#v", disconnected)
	}
	if len(disconnected.Actions) != 0 {
		t.Fatalf("disconnect emitted job-killing actions: %#v", disconnected.Actions)
	}
}

func TestPriorEpochPrepareReplayIsExactAndDoesNotIssueDuplicateCommand(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-a",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionReserved,
	}
	command := domain.Command{
		ID:              "prepare-execution-a",
		ControllerEpoch: 1,
		ExecutionID:     execution.ID,
		ExpectedState:   domain.ExecutionReserved,
		PayloadDigest:   domain.PayloadDigest([]byte("prepare")),
	}
	controller := restoreForTest(t, authorityForExecution(2, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
		Commands: []IssuedCommand{{
			NodeID:  "node-a",
			Type:    domain.CommandPrepare,
			Command: command,
		}},
	})

	result, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{command},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Actions) != 1 {
		t.Fatalf("actions = %#v", result.Actions)
	}
	action := result.Actions[0]
	if action.Kind != ActionReplayCommand || action.CommandID != command.ID ||
		action.ControllerEpoch != command.ControllerEpoch {
		t.Fatalf("prepare recovery action = %#v", action)
	}
	if action.Kind == ActionIssuePrepare {
		t.Fatal("prior command was replaced by a duplicate prepare")
	}
	if len(result.SuppressedReservations) != 1 {
		t.Fatalf("unresolved prior-epoch reservation was advertised: %#v", result)
	}
}

func TestPersistedStartIdentityIsNeverReplayedWithoutOneShotJITMaterial(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-a",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionPreparing,
	}
	start := domain.Command{
		ID:              "start-execution-a",
		ControllerEpoch: 1,
		ExecutionID:     execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start")),
	}
	controller := restoreForTest(t, authorityForExecution(2, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
		Commands: []IssuedCommand{{
			NodeID:  "node-a",
			Type:    domain.CommandStart,
			Command: start,
		}},
	})

	result, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if containsAction(result.Actions, ActionReplayCommand) ||
		containsAction(result.Actions, ActionIssuePrepare) {
		t.Fatalf("secret-bearing start was replayed or replaced: %#v", result.Actions)
	}
	if len(result.Actions) != 1 ||
		result.Actions[0].Kind != ActionInspectAndDestroy ||
		len(result.SuppressedReservations) != 1 ||
		result.Scheduler.Reconciled ||
		result.Scheduler.NativeReady {
		t.Fatalf("start without durable JIT was admitted: %#v", result)
	}
}

func TestReadinessChangePreservesExecutionUpdateFoldedAfterFullSnapshot(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-readiness",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionPreparing,
	}
	start := domain.Command{
		ID:              "start-execution-readiness",
		ControllerEpoch: 1,
		ExecutionID:     execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start-readiness")),
	}
	controller := restoreForTest(t, authorityForExecution(2, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
		Commands: []IssuedCommand{{
			NodeID:  "node-a",
			Type:    domain.CommandStart,
			Command: start,
		}},
	})
	if _, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{start},
	}); err != nil {
		t.Fatal(err)
	}
	if err := controller.ApplyExecutionUpdate(transport.ExecutionUpdate{
		NodeID:      "node-a",
		CommandID:   start.ID,
		ExecutionID: execution.ID,
		State:       domain.ExecutionRunning,
	}); err != nil {
		t.Fatal(err)
	}
	if err := controller.ApplyAgentReadiness("node-a", false); err != nil {
		t.Fatal(err)
	}
	if err := controller.ApplyAgentReadiness("node-a", true); err != nil {
		t.Fatal(err)
	}
	fleet := controller.FleetSnapshot()
	if len(fleet.Nodes) != 1 ||
		!fleet.Nodes[0].Reconciled ||
		!fleet.Nodes[0].NativeReady ||
		len(fleet.Nodes[0].ActiveExecutions) != 1 ||
		fleet.Nodes[0].ActiveExecutions[0] != execution.ID {
		t.Fatalf("readiness replay forgot running observation: %#v", fleet)
	}
	if len(fleet.Reservations) != 1 ||
		fleet.Reservations[0].ExecutionID != execution.ID {
		t.Fatalf("readiness replay released running reservation: %#v", fleet.Reservations)
	}
}

func TestApplyExecutionUpdateCommandConflictLeavesProjectionUnchanged(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-command-conflict",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionCleaning,
	}
	cancel := domain.Command{
		ID:              "cancel-command-conflict",
		ControllerEpoch: 1,
		ExecutionID:     execution.ID,
		ExpectedState:   domain.ExecutionRunning,
		PayloadDigest:   domain.PayloadDigest([]byte("cancel-command-conflict")),
	}
	controller := restoreForTest(t, authorityForExecution(2, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
		Commands: []IssuedCommand{{
			NodeID:  "node-a",
			Type:    domain.CommandCancel,
			Command: cancel,
		}},
		Now: func() time.Time { return time.Unix(100, 0) },
	})
	if _, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        execution.ID,
			State:              domain.ExecutionCleaning,
			ObservedAtUnixNano: 10,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	controller.mu.Lock()
	node := cloneNodeRuntime(controller.nodes["node-a"])
	conflicting := cancel
	conflicting.PayloadDigest = domain.PayloadDigest([]byte("different-command-authority"))
	node.snapshot.Commands = []domain.Command{conflicting}
	controller.nodes["node-a"] = node
	controller.mu.Unlock()
	beforeFleet, beforeAdmission, beforeNode := captureProjectionForFailure(t, controller, "node-a")

	err := controller.ApplyExecutionUpdate(transport.ExecutionUpdate{
		NodeID:      "node-a",
		CommandID:   cancel.ID,
		ExecutionID: execution.ID,
		State:       domain.ExecutionReleased,
	})
	if !hasCode(err, "agent_command_authority_mismatch") {
		t.Fatalf("command conflict = %v", err)
	}
	assertProjectionUnchanged(t, controller, "node-a", beforeFleet, beforeAdmission, beforeNode)
}

func TestApplyExecutionUpdateTimestampExhaustionLeavesProjectionUnchanged(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-timestamp-exhaustion",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionRunning,
	}
	cancel := domain.Command{
		ID:              "cancel-timestamp-exhaustion",
		ControllerEpoch: 1,
		ExecutionID:     execution.ID,
		ExpectedState:   domain.ExecutionRunning,
		PayloadDigest:   domain.PayloadDigest([]byte("cancel-timestamp-exhaustion")),
	}
	controller := restoreForTest(t, authorityForExecution(2, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
		Commands: []IssuedCommand{{
			NodeID:  "node-a",
			Type:    domain.CommandCancel,
			Command: cancel,
		}},
		Now: func() time.Time { return time.Unix(0, 1) },
	})
	if _, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{cancel},
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        execution.ID,
			State:              domain.ExecutionRunning,
			ObservedAtUnixNano: math.MaxInt64,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	beforeFleet, beforeAdmission, beforeNode := captureProjectionForFailure(t, controller, "node-a")

	err := controller.ApplyExecutionUpdate(transport.ExecutionUpdate{
		NodeID:      "node-a",
		CommandID:   cancel.ID,
		ExecutionID: execution.ID,
		State:       domain.ExecutionReleased,
	})
	if !hasCode(err, "agent_observation_timestamp_exhausted") {
		t.Fatalf("timestamp exhaustion = %v", err)
	}
	assertProjectionUnchanged(t, controller, "node-a", beforeFleet, beforeAdmission, beforeNode)
}

func TestApplyReconciliationExecutionUpdateTimestampExhaustionLeavesProjectionUnchanged(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-reconciliation-timestamp-exhaustion",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionFailed,
	}
	cancel := domain.Command{
		ID:              "cancel-reconciliation-timestamp-exhaustion",
		ControllerEpoch: 1,
		ExecutionID:     execution.ID,
		ExpectedState:   domain.ExecutionRunning,
		PayloadDigest:   domain.PayloadDigest([]byte("cancel-reconciliation-timestamp-exhaustion")),
	}
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes: []store.NodeAdministration{{
			NodeID: execution.Slot.NodeID,
			State:  domain.NodeActive,
		}},
		Executions: []domain.ExecutionSnapshot{execution},
	}, Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
		Commands: []IssuedCommand{{
			NodeID:  "node-a",
			Type:    domain.CommandCancel,
			Command: cancel,
		}},
		Now: func() time.Time { return time.Unix(0, 1) },
	})
	if _, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{cancel},
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        execution.ID,
			State:              domain.ExecutionCleaning,
			ObservedAtUnixNano: math.MaxInt64,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	beforeFleet, beforeAdmission, beforeNode := captureProjectionForFailure(t, controller, "node-a")

	err := controller.ApplyReconciliationExecutionUpdate(transport.ExecutionUpdate{
		NodeID:      "node-a",
		CommandID:   cancel.ID,
		ExecutionID: execution.ID,
		State:       domain.ExecutionQuarantined,
		ErrorCode:   transport.ExecutionErrorQuarantined,
	})
	if !hasCode(err, "agent_observation_timestamp_exhausted") {
		t.Fatalf("timestamp exhaustion = %v", err)
	}
	assertProjectionUnchanged(t, controller, "node-a", beforeFleet, beforeAdmission, beforeNode)
}

func TestUnknownLocalRuntimeBlocksNodeAndRequestsDestroy(t *testing.T) {
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 3,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{Nodes: []NodeDefinition{testNodeDefinition("node-a", 2)}})

	result, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 2,
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        "orphan-runtime",
			State:              domain.ExecutionRunning,
			ObservedAtUnixNano: 11,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Scheduler.Reconciled || result.Status.Phase != NodeReconciling {
		t.Fatalf("orphan runtime admitted node: %#v", result)
	}
	if len(result.Actions) != 1 || result.Actions[0].Kind != ActionInspectAndDestroy {
		t.Fatalf("orphan runtime actions = %#v", result.Actions)
	}
}

func TestCleanupTombstoneQuarantinesAndSuppressesEveryNodeReservation(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-a",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionCleaning,
	}
	controller := restoreForTest(t, authorityForExecution(2, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 2)},
	})

	result, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		CleanupTombstones: []transport.AgentCleanupTombstone{{
			ExecutionID:        execution.ID,
			FailureCode:        domain.CleanupProcessResidue,
			RecordedAtUnixNano: 20,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status.Phase != NodeQuarantined ||
		result.Scheduler.Node.AdministrativeState != domain.NodeQuarantined ||
		result.Scheduler.Reconciled || result.Scheduler.NativeReady {
		t.Fatalf("tombstoned node = %#v", result)
	}
	if len(result.Actions) != 1 || result.Actions[0].Kind != ActionPersistQuarantine {
		t.Fatalf("tombstone actions = %#v", result.Actions)
	}
	if len(result.SuppressedReservations) != 1 {
		t.Fatalf("quarantined reservation was not suppressed: %#v", result)
	}
	if err := controller.SetAdministrativeState("node-a", domain.NodeActive, false); !hasCode(err, "cleanup_remediation_required") {
		t.Fatalf("quarantine cleared without remediation = %v", err)
	}
	if err := controller.SetAdministrativeState("node-a", domain.NodeActive, true); err != nil {
		t.Fatalf("explicit remediation = %v", err)
	}
}

func TestDrainPreservesRunningObservationButAdvertisesNoReservation(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-a",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionRunning,
	}
	controller := restoreForTest(t, authorityForExecution(2, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 2)},
	})
	snapshot := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        execution.ID,
			State:              domain.ExecutionRunning,
			ObservedAtUnixNano: 10,
		}},
	}
	if _, err := controller.ReconcileAgentSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := controller.SetAdministrativeState("node-a", domain.NodeDraining, false); err != nil {
		t.Fatal(err)
	}
	result, err := controller.ReconcileAgentSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status.Phase != NodeDraining ||
		result.Scheduler.Node.AdministrativeState != domain.NodeDraining ||
		len(result.Scheduler.ActiveExecutions) != 1 ||
		len(result.SuppressedReservations) != 1 ||
		len(result.Actions) != 0 {
		t.Fatalf("draining node = %#v", result)
	}
}

func TestRestoreRejectsMissingAndTerminalReservationAuthority(t *testing.T) {
	node := store.NodeAdministration{NodeID: "node-a", State: domain.NodeActive}
	active := domain.ExecutionSnapshot{
		ID:       "execution-a",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionRunning,
	}
	if _, err := Restore(1, store.ControllerSnapshot{
		ControllerEpoch: 1,
		Nodes:           []store.NodeAdministration{node},
		Executions:      []domain.ExecutionSnapshot{active},
	}, Config{Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)}}); !hasCode(err, "execution_reservation_mismatch") {
		t.Fatalf("missing active reservation = %v", err)
	}
	terminal := active
	terminal.State = domain.ExecutionReleased
	if _, err := Restore(1, authorityForExecution(1, terminal), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
	}); !hasCode(err, "terminal_execution_reserved") {
		t.Fatalf("terminal reservation = %v", err)
	}
}

func TestRestoreAcceptsQuarantinedExecutionWithoutReservation(t *testing.T) {
	quarantined := domain.ExecutionSnapshot{
		ID:       "execution-quarantined",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionQuarantined,
	}
	authority := store.ControllerSnapshot{
		ControllerEpoch: 1,
		Nodes: []store.NodeAdministration{{
			NodeID: "node-a",
			State:  domain.NodeQuarantined,
		}},
		Executions: []domain.ExecutionSnapshot{quarantined},
	}
	controller, err := Restore(1, authority, Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	fleet := controller.FleetSnapshot()
	if len(fleet.Nodes) != 1 ||
		len(fleet.Nodes[0].ActiveExecutions) != 0 ||
		fleet.Nodes[0].Node.AdministrativeState != domain.NodeQuarantined ||
		len(fleet.Statuses) != 1 ||
		fleet.Statuses[0].Phase != NodeOffline ||
		len(fleet.Reservations) != 0 {
		t.Fatalf("quarantined restart projection = %#v", fleet)
	}
}

func TestGitHubAmbiguityAlwaysSuppressesItsReservation(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-a",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionPreparing,
	}
	start := domain.Command{
		ID:              "start-a",
		ControllerEpoch: 1,
		ExecutionID:     execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start")),
	}
	controller := restoreForTest(t, authorityForExecution(2, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 2)},
		Commands: []IssuedCommand{{
			NodeID:  "node-a",
			Type:    domain.CommandStart,
			Command: start,
		}},
		GitHubFences: []GitHubFence{{
			ExecutionID:     execution.ID,
			ScaleSetID:      7,
			RunnerRequestID: 8,
			ClaimState:      store.GitHubClaimStartAmbiguous,
			Attempt: &store.GitHubJITAttempt{
				ScaleSetID:      7,
				RunnerRequestID: 8,
				Attempt:         1,
				ControllerEpoch: 1,
				RunnerName:      "tewake-a",
				State:           store.GitHubJITStartAmbiguous,
				RunnerID:        9,
				JITDigest:       domain.PayloadDigest([]byte("jit")),
				StartCommandID:  start.ID,
			},
		}},
	})

	result, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{start},
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        execution.ID,
			State:              domain.ExecutionRunning,
			ObservedAtUnixNano: 12,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SuppressedReservations) != 1 {
		t.Fatalf("ambiguous start advertised capacity: %#v", result)
	}
	if !containsAction(result.Actions, ActionConfirmAgentStartAccepted) {
		t.Fatalf("accepted start was not reconciled: %#v", result.Actions)
	}
}

func TestTerminalExecutionRetainsGitHubReconciliationIdentityWithoutHoldingSlot(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-terminal",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionReleased,
	}
	start := domain.Command{
		ID:              "start-terminal",
		ControllerEpoch: 1,
		ExecutionID:     execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start-terminal")),
	}
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes: []store.NodeAdministration{{
			NodeID: "node-a",
			State:  domain.NodeActive,
		}},
		Executions: []domain.ExecutionSnapshot{execution},
	}, Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
		Commands: []IssuedCommand{{
			NodeID:  "node-a",
			Type:    domain.CommandStart,
			Command: start,
		}},
		GitHubFences: []GitHubFence{{
			ExecutionID:     execution.ID,
			ScaleSetID:      7,
			RunnerRequestID: 8,
			ClaimState:      store.GitHubClaimReconciliationRequired,
			Attempt: &store.GitHubJITAttempt{
				ScaleSetID:      7,
				RunnerRequestID: 8,
				Attempt:         1,
				ControllerEpoch: 1,
				RunnerName:      "tewake-terminal",
				State:           store.GitHubJITStarted,
				RunnerID:        9,
				JITDigest:       domain.PayloadDigest([]byte("jit-terminal")),
				StartCommandID:  start.ID,
			},
		}},
	})
	result, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{start},
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        execution.ID,
			State:              domain.ExecutionReleased,
			ObservedAtUnixNano: 10,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !containsAction(result.Actions, ActionAwaitAgentObservation) ||
		len(result.SuppressedReservations) != 0 ||
		!result.Scheduler.Reconciled ||
		!result.Scheduler.NativeReady {
		t.Fatalf("terminal reconciliation result = %#v", result)
	}
}

func TestAcceptedAmbiguousStartIsNeverReplayedWithoutJITReconciliation(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-a",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionPreparing,
	}
	start := domain.Command{
		ID:              "start-a",
		ControllerEpoch: 1,
		ExecutionID:     execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start")),
	}
	controller := restoreForTest(t, authorityForExecution(2, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
		Commands: []IssuedCommand{{
			NodeID:  "node-a",
			Type:    domain.CommandStart,
			Command: start,
		}},
		GitHubFences: []GitHubFence{{
			ExecutionID:     execution.ID,
			ScaleSetID:      7,
			RunnerRequestID: 8,
			ClaimState:      store.GitHubClaimStartAmbiguous,
			Attempt: &store.GitHubJITAttempt{
				ScaleSetID:      7,
				RunnerRequestID: 8,
				Attempt:         1,
				ControllerEpoch: 1,
				RunnerName:      "tewake-a",
				State:           store.GitHubJITStartAmbiguous,
				RunnerID:        9,
				JITDigest:       domain.PayloadDigest([]byte("jit")),
				StartCommandID:  start.ID,
			},
		}},
	})
	result, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{start},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !containsAction(result.Actions, ActionConfirmAgentStartAccepted) ||
		containsAction(result.Actions, ActionReplayCommand) ||
		containsAction(result.Actions, ActionIssuePrepare) {
		t.Fatalf("secret-bearing start replayed after acceptance: %#v", result.Actions)
	}
}

func TestGitHubFenceProjectionAcceptsForwardStateAndRejectsDelayedApplyAndClear(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-fence-cas",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionPreparing,
	}
	controller := restoreForTest(t, authorityForExecution(4, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
	})
	intent := GitHubFence{
		ExecutionID:     execution.ID,
		ScaleSetID:      7,
		RunnerRequestID: 8,
		ClaimState:      store.GitHubClaimJITIntent,
		Attempt: &store.GitHubJITAttempt{
			ScaleSetID:      7,
			RunnerRequestID: 8,
			Attempt:         1,
			ControllerEpoch: 2,
			RunnerName:      "tewake-fence-cas",
			State:           store.GitHubJITIntent,
		},
	}
	generated := cloneGitHubFence(intent)
	generated.ClaimState = store.GitHubClaimJITGenerated
	generated.Attempt.State = store.GitHubJITGenerated
	generated.Attempt.RunnerID = 9
	generated.Attempt.JITDigest = domain.PayloadDigest([]byte("jit-fence-cas"))
	generated.Attempt.StartCommandID = "start-fence-cas"
	dispatching := cloneGitHubFence(generated)
	dispatching.ClaimState = store.GitHubClaimStartDispatching
	dispatching.Attempt.State = store.GitHubJITStartDispatching

	beforeFleet, beforeAdmission, beforeNode := captureProjectionForFailure(t, controller, "node-a")
	if err := controller.ClearGitHubFence(intent); !hasCode(err, "github_fence_clear_mismatch") {
		t.Fatalf("unknown Clear = %v", err)
	}
	assertProjectionUnchanged(
		t,
		controller,
		"node-a",
		beforeFleet,
		beforeAdmission,
		beforeNode,
	)

	if err := controller.ApplyGitHubFence(intent); err != nil {
		t.Fatal(err)
	}
	if err := controller.ApplyGitHubFence(dispatching); err != nil {
		t.Fatalf("legitimate reachable dispatching advance = %v", err)
	}
	if err := controller.ApplyGitHubFence(dispatching); err != nil {
		t.Fatalf("exact active replay = %v", err)
	}

	if err := controller.ApplyGitHubFence(generated); !hasCode(err, "github_fence_update_regressed") {
		t.Fatalf("delayed Apply = %v", err)
	}
	if current := controller.fences[execution.ID]; !githubFencesEqual(current, dispatching) {
		t.Fatalf("delayed Apply replaced newer fence: %#v", current)
	}
	if err := controller.ClearGitHubFence(generated); !hasCode(err, "github_fence_clear_mismatch") {
		t.Fatalf("delayed Clear = %v", err)
	}
	wrongGeneration := cloneGitHubFence(dispatching)
	wrongGeneration.Attempt.Attempt = 2
	wrongGeneration.Attempt.ControllerEpoch = 3
	if err := controller.ClearGitHubFence(wrongGeneration); !hasCode(err, "github_fence_clear_mismatch") {
		t.Fatalf("different attempt generation Clear = %v", err)
	}
	if current := controller.fences[execution.ID]; !githubFencesEqual(current, dispatching) {
		t.Fatalf("stale Clear mutated newer fence: %#v", current)
	}

	if err := controller.ClearGitHubFence(dispatching); err != nil {
		t.Fatalf("exact Clear = %v", err)
	}
	if _, found := controller.fences[execution.ID]; found {
		t.Fatalf("exact Clear retained active fence: %#v", controller.fences[execution.ID])
	}
	if err := controller.ClearGitHubFence(dispatching); err != nil {
		t.Fatalf("exact Clear replay = %v", err)
	}
	if err := controller.ApplyGitHubFence(dispatching); !hasCode(err, "github_fence_update_regressed") {
		t.Fatalf("delayed Apply after exact Clear = %v", err)
	}
	if _, found := controller.fences[execution.ID]; found {
		t.Fatalf("delayed Apply resurrected cleared fence: %#v", controller.fences[execution.ID])
	}
}

func TestTerminalGitHubFenceBlocksNodeAdmissionUntilExactRemovalFenceClears(
	t *testing.T,
) {
	const nodeID domain.NodeID = "node-terminal-github-fence"
	execution := domain.ExecutionSnapshot{
		ID:       "execution-terminal-github-fence",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: nodeID, Index: 0},
		State:    domain.ExecutionReleased,
	}
	started := GitHubFence{
		ExecutionID:     execution.ID,
		ScaleSetID:      7,
		RunnerRequestID: 8,
		ClaimState:      store.GitHubClaimReconciliationRequired,
		Attempt: &store.GitHubJITAttempt{
			ScaleSetID:      7,
			RunnerRequestID: 8,
			Attempt:         1,
			ControllerEpoch: 3,
			RunnerName:      "tewake-terminal-github-fence",
			State:           store.GitHubJITStarted,
			RunnerID:        9,
			JITDigest:       "jit-terminal-github-fence",
			StartCommandID:  "start-terminal-github-fence",
		},
	}
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 4,
		Nodes: []store.NodeAdministration{{
			NodeID: nodeID,
			State:  domain.NodeActive,
		}},
		Executions: []domain.ExecutionSnapshot{execution},
	}, Config{
		Nodes:        []NodeDefinition{testNodeDefinition(nodeID, 2)},
		GitHubFences: []GitHubFence{started},
	})
	if _, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             nodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 4,
	}); err != nil {
		t.Fatal(err)
	}
	admission, err := controller.Admission(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if admission.AllowsNewCapacity || !admission.AllowsRecovery {
		t.Fatalf("terminal active fence admission = %#v", admission)
	}

	removalPending := cloneGitHubFence(started)
	removalPending.Attempt.State = store.GitHubJITRemovalPending
	if err := controller.ApplyGitHubFence(removalPending); err != nil {
		t.Fatalf("Started -> RemovalPending fence = %v", err)
	}
	if err := controller.ClearGitHubFence(started); !hasCode(
		err,
		"github_fence_clear_mismatch",
	) {
		t.Fatalf("stale Started clear = %v", err)
	}
	if err := controller.ClearGitHubFence(removalPending); err != nil {
		t.Fatalf("exact RemovalPending clear = %v", err)
	}
	admission, err = controller.Admission(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if !admission.AllowsNewCapacity || !admission.AllowsRecovery {
		t.Fatalf("cleared terminal fence admission = %#v", admission)
	}
}

func TestGitHubFenceProjectionRequiresMonotonicAttemptGeneration(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-fence-generation",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionPreparing,
	}
	controller := restoreForTest(t, authorityForExecution(4, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
	})
	old := GitHubFence{
		ExecutionID:     execution.ID,
		ScaleSetID:      7,
		RunnerRequestID: 8,
		ClaimState:      store.GitHubClaimReconciliationRequired,
		Attempt: &store.GitHubJITAttempt{
			ScaleSetID:      7,
			RunnerRequestID: 8,
			Attempt:         1,
			ControllerEpoch: 1,
			RunnerName:      "tewake-fence-generation",
			State:           store.GitHubJITRemovalPending,
			RunnerID:        9,
		},
	}
	next := cloneGitHubFence(old)
	next.ClaimState = store.GitHubClaimJITIntent
	next.Attempt.Attempt = 2
	next.Attempt.ControllerEpoch = 3
	next.Attempt.State = store.GitHubJITIntent
	next.Attempt.RunnerID = 0

	if err := controller.ApplyGitHubFence(old); err != nil {
		t.Fatal(err)
	}
	if err := controller.ApplyGitHubFence(next); err != nil {
		t.Fatalf("later attempt generation = %v", err)
	}
	if err := controller.ApplyGitHubFence(old); !hasCode(err, "github_fence_update_regressed") {
		t.Fatalf("older attempt overwrote later generation: %v", err)
	}
	if err := controller.ClearGitHubFence(old); !hasCode(err, "github_fence_clear_mismatch") {
		t.Fatalf("older attempt cleared later generation: %v", err)
	}

	higherAttemptOlderEpoch := cloneGitHubFence(next)
	higherAttemptOlderEpoch.Attempt.Attempt = 3
	higherAttemptOlderEpoch.Attempt.ControllerEpoch = 2
	if err := controller.ApplyGitHubFence(higherAttemptOlderEpoch); !hasCode(err, "github_fence_update_regressed") {
		t.Fatalf("inconsistent later attempt generation = %v", err)
	}
	sameAttemptRewrittenEpoch := cloneGitHubFence(next)
	sameAttemptRewrittenEpoch.Attempt.ControllerEpoch = 4
	if err := controller.ApplyGitHubFence(sameAttemptRewrittenEpoch); !hasCode(err, "github_fence_update_regressed") {
		t.Fatalf("rewritten attempt epoch = %v", err)
	}
	if current := controller.fences[execution.ID]; !githubFencesEqual(current, next) {
		t.Fatalf("generation conflicts mutated current fence: %#v", current)
	}
}

func TestGitHubFenceProjectionAllowsLostJITRemovalAfterAgentAccepted(t *testing.T) {
	execution := domain.ExecutionSnapshot{
		ID:       "execution-fence-lost-jit",
		TargetID: "target-a",
		Slot:     domain.SlotKey{NodeID: "node-a", Index: 0},
		State:    domain.ExecutionPreparing,
	}
	controller := restoreForTest(t, authorityForExecution(4, execution), Config{
		Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)},
	})
	accepted := GitHubFence{
		ExecutionID:     execution.ID,
		ScaleSetID:      7,
		RunnerRequestID: 8,
		ClaimState:      store.GitHubClaimReconciliationRequired,
		Attempt: &store.GitHubJITAttempt{
			ScaleSetID:      7,
			RunnerRequestID: 8,
			Attempt:         1,
			ControllerEpoch: 2,
			RunnerName:      "tewake-fence-lost-jit",
			State:           store.GitHubJITAgentAccepted,
			RunnerID:        9,
			JITDigest:       domain.PayloadDigest([]byte("jit-fence-lost-jit")),
			StartCommandID:  "start-fence-lost-jit",
		},
	}
	removal := cloneGitHubFence(accepted)
	removal.Attempt.State = store.GitHubJITRemovalPending

	if err := controller.ApplyGitHubFence(accepted); err != nil {
		t.Fatal(err)
	}
	if err := controller.ApplyGitHubFence(removal); err != nil {
		t.Fatalf("AgentAccepted to RemovalPending advance = %v", err)
	}
	if current := controller.fences[execution.ID]; !githubFencesEqual(current, removal) {
		t.Fatalf("lost-JIT removal did not replace accepted fence: %#v", current)
	}
	if err := controller.ApplyGitHubFence(accepted); !hasCode(err, "github_fence_update_regressed") {
		t.Fatalf("delayed AgentAccepted resurrected after removal = %v", err)
	}
	if current := controller.fences[execution.ID]; !githubFencesEqual(current, removal) {
		t.Fatalf("delayed accepted fence mutated removal authority: %#v", current)
	}
}

func TestSnapshotRejectionLeavesPreviousOnlineAuthorityUntouched(t *testing.T) {
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)}})
	if _, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
	}); err != nil {
		t.Fatal(err)
	}
	before := controller.FleetSnapshot()
	_, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 3,
	})
	if !hasCode(err, "future_agent_epoch") {
		t.Fatalf("future epoch = %v", err)
	}
	after := controller.FleetSnapshot()
	if !after.Nodes[0].Reconciled || before.Nodes[0].Node.ObservedState != after.Nodes[0].Node.ObservedState {
		t.Fatalf("invalid snapshot replaced last-known state: before=%#v after=%#v", before, after)
	}
}

type memoryAuthority struct {
	nextEpoch             domain.ControllerEpoch
	snapshot              store.ControllerSnapshot
	preserveSnapshotEpoch bool
	advanceErr            error
	snapshotErr           error
	advanceCalls          int
	snapshotCalls         int
}

func (authority *memoryAuthority) AdvanceEpoch(context.Context) (domain.ControllerEpoch, error) {
	authority.advanceCalls++
	if authority.advanceErr != nil {
		return 0, authority.advanceErr
	}
	if !authority.preserveSnapshotEpoch {
		authority.snapshot.ControllerEpoch = authority.nextEpoch
	}
	return authority.nextEpoch, nil
}

func (authority *memoryAuthority) Snapshot(context.Context) (store.ControllerSnapshot, error) {
	authority.snapshotCalls++
	if authority.snapshotErr != nil {
		return store.ControllerSnapshot{}, authority.snapshotErr
	}
	return authority.snapshot, nil
}

func testNodeDefinition(nodeID domain.NodeID, maxRunners int) NodeDefinition {
	return NodeDefinition{
		Node: domain.Node{
			ID:                  nodeID,
			DisplayName:         string(nodeID),
			OS:                  domain.OSLinux,
			Architecture:        domain.ArchAMD64,
			MaxRunners:          maxRunners,
			AdministrativeState: domain.NodeActive,
			ObservedState:       domain.NodeOffline,
		},
		RunnerVersionPolicy: domain.RunnerVersionAutoUpdate,
		RunnerUpdate:        ManagedRunnerUpdate(),
	}
}

func authorityForExecution(epoch domain.ControllerEpoch, execution domain.ExecutionSnapshot) store.ControllerSnapshot {
	return store.ControllerSnapshot{
		ControllerEpoch: epoch,
		Nodes:           []store.NodeAdministration{{NodeID: execution.Slot.NodeID, State: domain.NodeActive}},
		Reservations: []store.SlotReservation{{
			Slot:  execution.Slot,
			Owner: domain.SlotOwner{TargetID: execution.TargetID, ExecutionID: execution.ID},
		}},
		Executions: []domain.ExecutionSnapshot{execution},
	}
}

func restoreForTest(t *testing.T, snapshot store.ControllerSnapshot, config Config) *Controller {
	t.Helper()
	controller, err := Restore(snapshot.ControllerEpoch, snapshot, config)
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func captureProjectionForFailure(
	t *testing.T,
	controller *Controller,
	nodeID domain.NodeID,
) (FleetSnapshot, NodeAdmission, nodeRuntime) {
	t.Helper()
	fleet := controller.FleetSnapshot()
	admission, err := controller.Admission(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	controller.mu.RLock()
	node := cloneNodeRuntime(controller.nodes[nodeID])
	controller.mu.RUnlock()
	return fleet, admission, node
}

func assertProjectionUnchanged(
	t *testing.T,
	controller *Controller,
	nodeID domain.NodeID,
	beforeFleet FleetSnapshot,
	beforeAdmission NodeAdmission,
	beforeNode nodeRuntime,
) {
	t.Helper()
	afterFleet := controller.FleetSnapshot()
	afterAdmission, err := controller.Admission(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	controller.mu.RLock()
	afterNode := cloneNodeRuntime(controller.nodes[nodeID])
	controller.mu.RUnlock()
	if !reflect.DeepEqual(afterFleet, beforeFleet) {
		t.Fatalf("failed apply mutated FleetSnapshot:\nbefore=%#v\nafter=%#v", beforeFleet, afterFleet)
	}
	if !reflect.DeepEqual(afterAdmission, beforeAdmission) {
		t.Fatalf("failed apply mutated Admission:\nbefore=%#v\nafter=%#v", beforeAdmission, afterAdmission)
	}
	if !reflect.DeepEqual(afterNode, beforeNode) {
		t.Fatalf("failed apply mutated retained node:\nbefore=%#v\nafter=%#v", beforeNode, afterNode)
	}
}

func containsAction(actions []Action, kind ActionKind) bool {
	for _, action := range actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}

func hasCode(err error, code string) bool {
	var reconcileError *Error
	return errors.As(err, &reconcileError) && reconcileError.Code == code
}

func TestControllerConcurrentSnapshotAndDisconnectKeepsInvariants(t *testing.T) {
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)}})
	snapshot := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
	}
	const workers = 32
	errorsFound := make(chan error, workers*2)
	for index := 0; index < workers; index++ {
		go func() {
			_, err := controller.ReconcileAgentSnapshot(snapshot)
			errorsFound <- err
		}()
		go func() {
			_, err := controller.Disconnect("node-a")
			errorsFound <- err
		}()
	}
	for index := 0; index < workers*2; index++ {
		if err := <-errorsFound; err != nil {
			t.Fatal(err)
		}
	}
	fleet := controller.FleetSnapshot()
	if len(fleet.Nodes) != 1 {
		t.Fatalf("fleet = %#v", fleet)
	}
}

func TestStartPropagatesAuthorityFailuresWithoutConstructingController(t *testing.T) {
	want := errors.New("disk unavailable")
	authority := &memoryAuthority{advanceErr: want}
	if controller, err := Start(context.Background(), authority, Config{}); controller != nil || !errors.Is(err, want) {
		t.Fatalf("advance failure = (%#v, %v)", controller, err)
	}

	authority = &memoryAuthority{nextEpoch: 1, snapshotErr: want}
	if controller, err := Start(context.Background(), authority, Config{}); controller != nil || !errors.Is(err, want) {
		t.Fatalf("snapshot failure = (%#v, %v)", controller, err)
	}
}

func TestRunnerFreshnessFailureKeepsConnectedNodeDegraded(t *testing.T) {
	definition := testNodeDefinition("node-a", 1)
	definition.RunnerVersionPolicy = domain.RunnerVersionPinned
	definition.RunnerUpdate = RunnerUpdateStatus{
		State:            RunnerUpdateExpired,
		PinnedVersion:    "2.335.0",
		LatestVersion:    "2.336.0",
		LatestReleasedAt: time.Unix(100, 0).Add(-GitHubRunnerUpdateWindow),
		Deadline:         time.Unix(100, 0),
		ObservedAt:       time.Unix(90, 0),
		FreshUntil:       time.Unix(90, 0).Add(GitHubRunnerUpdateWindow),
	}
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{Nodes: []NodeDefinition{definition}})
	result, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status.Phase != NodeDegraded || result.Scheduler.NativeReady || result.Scheduler.Reconciled {
		t.Fatalf("expired runner admitted node: %#v", result)
	}
}

func TestPinnedRunnerBecomesDegradedAtDeadlineWithoutAnotherAgentSnapshot(t *testing.T) {
	released := time.Unix(100, 0).UTC()
	now := released.Add(time.Hour)
	update, err := EvaluateRunnerUpdate(now, domain.RunnerVersionPinned, RunnerReleaseObservation{
		HasValue:         true,
		PinnedVersion:    "2.335.0",
		LatestVersion:    "2.336.0",
		LatestReleasedAt: released,
		ObservedAt:       released.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	definition := testNodeDefinition("node-a", 1)
	definition.RunnerVersionPolicy = domain.RunnerVersionPinned
	definition.RunnerUpdate = update
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{
		Nodes: []NodeDefinition{definition},
		Now:   func() time.Time { return now },
	})
	if _, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if !controller.FleetSnapshot().Nodes[0].Reconciled {
		t.Fatal("supported pinned runner did not admit")
	}
	now = released.Add(GitHubRunnerUpdateWindow)
	fleet := controller.FleetSnapshot()
	if fleet.Nodes[0].Reconciled || fleet.Nodes[0].NativeReady ||
		fleet.Statuses[0].Phase != NodeDegraded {
		t.Fatalf("deadline did not degrade live node: %#v", fleet)
	}
}

func TestRunnerReleaseRefreshDegradesImmediatelyAndRecoversAfterAgentSnapshot(t *testing.T) {
	released := time.Unix(100, 0).UTC()
	now := released.Add(time.Hour)
	current, err := EvaluateRunnerUpdate(now, domain.RunnerVersionPinned, RunnerReleaseObservation{
		HasValue:         true,
		PinnedVersion:    "2.336.0",
		LatestVersion:    "2.336.0",
		LatestReleasedAt: released,
		ObservedAt:       released.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	definition := testNodeDefinition("node-a", 1)
	definition.RunnerVersionPolicy = domain.RunnerVersionPinned
	definition.RunnerUpdate = current
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{
		Nodes: []NodeDefinition{definition},
		Now:   func() time.Time { return now },
	})
	snapshot := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
	}
	if _, err := controller.ReconcileAgentSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	stale, err := EvaluateRunnerUpdate(now, domain.RunnerVersionPinned, RunnerReleaseObservation{
		HasValue:         true,
		PinnedVersion:    "2.336.0",
		LatestVersion:    "2.336.0",
		LatestReleasedAt: released,
		ObservedAt:       released.Add(time.Minute),
		Stale:            true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.SetRunnerUpdateStatus("node-a", stale); err != nil {
		t.Fatal(err)
	}
	if fleet := controller.FleetSnapshot(); fleet.Nodes[0].Reconciled ||
		fleet.Statuses[0].Phase != NodeDegraded {
		t.Fatalf("stale release observation did not degrade node: %#v", fleet)
	}
	if err := controller.SetRunnerUpdateStatus("node-a", current); err != nil {
		t.Fatal(err)
	}
	if controller.FleetSnapshot().Nodes[0].Reconciled {
		t.Fatal("release refresh bypassed fresh Agent package observation")
	}
	if result, err := controller.ReconcileAgentSnapshot(snapshot); err != nil ||
		!result.Scheduler.Reconciled {
		t.Fatalf("fresh package reconciliation = (%#v, %v)", result, err)
	}
}
