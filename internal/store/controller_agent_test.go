package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/runner"
)

func TestControllerAgentCommandAndUpdateLifecycleIsDurableAndReplaySafe(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-agent.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 1)
	assignment := testAssignment(901, "execution-agent-1", nodeID, 0)
	if _, replayed, err := controller.Assign(ctx, assignment); err != nil || replayed {
		t.Fatalf("assign = (%t, %v)", replayed, err)
	}

	prepare := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              "prepare-agent-1",
			ControllerEpoch: epoch,
			ExecutionID:     assignment.Execution.ID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   digestForTest("prepare-payload"),
		},
	}
	if replayed, err := controller.CommitAgentCommand(ctx, prepare); err != nil || replayed {
		t.Fatalf("commit prepare = (%t, %v)", replayed, err)
	}
	if replayed, err := controller.CommitAgentCommand(ctx, prepare); err != nil || !replayed {
		t.Fatalf("replay prepare = (%t, %v)", replayed, err)
	}
	mismatched := prepare
	mismatched.Command.PayloadDigest = digestForTest("different-prepare-payload")
	if _, err := controller.CommitAgentCommand(ctx, mismatched); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("mismatched prepare replay = %v", err)
	}

	// Start cannot be admitted until the Agent's durable Preparing update has
	// advanced desired state. A failed precondition must leave no replay row.
	start := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandStart,
		Command: domain.Command{
			ID:              "start-agent-1",
			ControllerEpoch: epoch,
			ExecutionID:     assignment.Execution.ID,
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   digestForTest("start-payload-with-secret-redacted"),
		},
	}
	if _, err := controller.CommitAgentCommand(ctx, start); err == nil {
		t.Fatal("start command bypassed the preparing precondition")
	}
	assertCount(t, controller.db, "SELECT count(*) FROM agent_commands WHERE command_id='start-agent-1'", 0)

	preparing := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-preparing",
		CommandID:     prepare.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionPreparing,
		PayloadDigest: digestForTest("update-preparing-wire"),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(ctx, preparing); err != nil || replayed {
		t.Fatalf("record preparing = (%t, %v)", replayed, err)
	}
	if replayed, err := controller.CommitAgentCommand(ctx, start); err != nil || replayed {
		t.Fatalf("commit start = (%t, %v)", replayed, err)
	}

	for index, state := range []domain.ExecutionState{
		domain.ExecutionRunning,
		domain.ExecutionReleased,
	} {
		update := AgentExecutionUpdate{
			NodeID:        domain.NodeID(nodeID),
			MessageID:     "update-" + string(state),
			CommandID:     start.Command.ID,
			ExecutionID:   assignment.Execution.ID,
			State:         state,
			PayloadDigest: digestForTest("update-wire-" + string(state)),
		}
		replayed, err := controller.RecordAgentExecutionUpdate(ctx, update)
		if err != nil || replayed {
			t.Fatalf("record update %d %q = (%t, %v)", index, state, replayed, err)
		}
		if state == domain.ExecutionRunning {
			if replayed, err := controller.RecordAgentExecutionUpdate(ctx, update); err != nil || !replayed {
				t.Fatalf("exact update replay = (%t, %v)", replayed, err)
			}
			update.PayloadDigest = digestForTest("changed-running-wire")
			if _, err := controller.RecordAgentExecutionUpdate(ctx, update); !errors.Is(err, ErrReplayMismatch) {
				t.Fatalf("mismatched update replay = %v", err)
			}
			// An ACK-lost Prepare can be replayed after the execution has
			// advanced. Its current Running observation is valid evidence, not
			// an attempt to regress the desired state.
			prepareReplay := AgentExecutionUpdate{
				NodeID:        domain.NodeID(nodeID),
				MessageID:     "update-prepare-replayed-running",
				CommandID:     prepare.Command.ID,
				ExecutionID:   assignment.Execution.ID,
				State:         domain.ExecutionRunning,
				Replayed:      true,
				PayloadDigest: digestForTest("update-prepare-replayed-running-wire"),
			}
			if replayed, err := controller.RecordAgentExecutionUpdate(ctx, prepareReplay); err != nil || replayed {
				t.Fatalf("advanced Prepare replay = (%t, %v)", replayed, err)
			}
		}
	}

	// GitHub may redeliver the assignment after desired state has advanced.
	// Replay returns the current state without creating another slot or runner.
	replayedExecution, replayed, err := controller.Assign(ctx, assignment)
	if err != nil || !replayed || replayedExecution.State != domain.ExecutionReleased {
		t.Fatalf("advanced assignment replay = (%+v, %t, %v)", replayedExecution, replayed, err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
	reused := testAssignment(904, "execution-agent-reused", nodeID, 0)
	if _, replayed, err := controller.Assign(ctx, reused); err != nil || replayed {
		t.Fatalf("reuse released slot = (%t, %v)", replayed, err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 2)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations WHERE execution_id='execution-agent-reused'", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_commands", 2)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_execution_updates", 4)
	assertDatabaseExcludesText(t, controller, "start-payload-with-secret-redacted")
}

func TestControllerReplayAgentCommandRequiresExactCurrentSnapshotAuthority(
	t *testing.T,
) {
	ctx := context.Background()
	controller := openController(t, "controller-agent-replay-snapshot-cas.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 7)
	_, prepare := assignAndIssuePrepare(
		t,
		controller,
		nodeID,
		epoch,
		920,
		"execution-agent-replay-snapshot-cas",
		0,
	)
	first := NodeAgentSnapshot{
		NodeID:            domain.NodeID(nodeID),
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		RunnerVersion:     runner.OfficialRunnerVersion,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
			Commands:           []domain.Command{prepare.Command},
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, first); err != nil {
		t.Fatal(err)
	}
	firstDigest, err := nodeAgentSnapshotDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	if replayed, err := controller.ReplayAgentCommand(
		ctx,
		prepare,
		firstDigest,
	); err != nil || !replayed {
		t.Fatalf("current snapshot replay = (%t, %v)", replayed, err)
	}

	second := first
	second.RunnerVersion = "2.337.0"
	if err := controller.RecordAgentSnapshot(ctx, second); err != nil {
		t.Fatal(err)
	}
	secondDigest, err := nodeAgentSnapshotDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if secondDigest == firstDigest {
		t.Fatal("superseding snapshot retained the prior authority digest")
	}
	if _, err := controller.ReplayAgentCommand(
		ctx,
		prepare,
		firstDigest,
	); err == nil {
		t.Fatal("superseded snapshot authorized Prepare replay")
	}
	if replayed, err := controller.ReplayAgentCommand(
		ctx,
		prepare,
		secondDigest,
	); err != nil || !replayed {
		t.Fatalf("replacement snapshot replay = (%t, %v)", replayed, err)
	}

	if _, err := controller.AdvanceEpoch(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ReplayAgentCommand(
		ctx,
		prepare,
		secondDigest,
	); err == nil {
		t.Fatal("prior-epoch snapshot authorized replay after Controller restart")
	}
}

func TestControllerTerminalUpdateLeaseMutationIsAtomic(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-agent-atomic.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 3)
	assignment, prepare := assignAndIssuePrepare(t, controller, nodeID, epoch, 905, "execution-agent-atomic", 0)

	if _, err := controller.db.Exec(`CREATE TRIGGER reject_slot_release
		BEFORE DELETE ON slot_reservations
		BEGIN SELECT RAISE(ABORT, 'injected slot release failure'); END`); err != nil {
		t.Fatal(err)
	}
	failed := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-failed-atomic",
		CommandID:     prepare.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionFailed,
		ErrorCode:     domain.ExecutionErrorStart,
		PayloadDigest: digestForTest("update-failed-atomic-wire"),
	}
	if _, err := controller.RecordAgentExecutionUpdate(ctx, failed); err == nil {
		t.Fatal("injected slot release failure was hidden")
	}
	assertExecutionState(t, controller, assignment.Execution.ID, domain.ExecutionReserved)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations WHERE execution_id='execution-agent-atomic'", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_execution_updates WHERE message_id='update-failed-atomic'", 0)

	if _, err := controller.db.Exec(`DROP TRIGGER reject_slot_release`); err != nil {
		t.Fatal(err)
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(ctx, failed); err != nil || replayed {
		t.Fatalf("retry failed update = (%t, %v)", replayed, err)
	}
	assertExecutionState(t, controller, assignment.Execution.ID, domain.ExecutionFailed)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations WHERE execution_id='execution-agent-atomic'", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM executions WHERE id='execution-agent-atomic'", 1)

	reused := testAssignment(906, "execution-agent-after-failed", nodeID, 0)
	if _, replayed, err := controller.Assign(ctx, reused); err != nil || replayed {
		t.Fatalf("reuse cleanup-complete failed slot = (%t, %v)", replayed, err)
	}
}

func TestControllerTerminalUpdateRejectsMissingLease(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-agent-missing-lease.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 6)
	assignment, prepare := assignAndIssuePrepare(
		t,
		controller,
		nodeID,
		epoch,
		911,
		"execution-agent-missing-lease",
		0,
	)

	// Simulate out-of-band corruption after the store has opened. The terminal
	// update must report the missing lease instead of claiming a successful
	// capacity release.
	if _, err := controller.db.Exec(`
		UPDATE executions SET state = 'released' WHERE id = 'execution-agent-missing-lease';
		DELETE FROM slot_reservations WHERE execution_id = 'execution-agent-missing-lease';
		UPDATE executions SET state = 'reserved' WHERE id = 'execution-agent-missing-lease';`); err != nil {
		t.Fatal(err)
	}
	failed := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-failed-missing-lease",
		CommandID:     prepare.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionFailed,
		ErrorCode:     domain.ExecutionErrorStart,
		PayloadDigest: digestForTest("update-failed-missing-lease-wire"),
	}
	if _, err := controller.RecordAgentExecutionUpdate(ctx, failed); err == nil {
		t.Fatal("terminal update hid its missing slot reservation")
	}
	assertExecutionState(t, controller, assignment.Execution.ID, domain.ExecutionReserved)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations WHERE execution_id='execution-agent-missing-lease'", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_execution_updates WHERE message_id='update-failed-missing-lease'", 0)
}

func TestControllerCleanupFailureRetainsLeaseAndQuarantinesNode(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller-agent-quarantine.db")
	controller := openControllerPath(t, path)
	nodeID, epoch := enrollControllerAgentNode(t, controller, 4)
	assignment, prepare := assignAndIssuePrepare(t, controller, nodeID, epoch, 907, "execution-agent-quarantine", 0)
	preparing := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-quarantine-preparing",
		CommandID:     prepare.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionPreparing,
		PayloadDigest: digestForTest("update-quarantine-preparing-wire"),
	}
	if _, err := controller.RecordAgentExecutionUpdate(ctx, preparing); err != nil {
		t.Fatal(err)
	}
	start := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandStart,
		Command: domain.Command{
			ID:              "start-agent-quarantine",
			ControllerEpoch: epoch,
			ExecutionID:     assignment.Execution.ID,
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   digestForTest("start-agent-quarantine"),
		},
	}
	if _, err := controller.CommitAgentCommand(ctx, start); err != nil {
		t.Fatal(err)
	}
	cleanupFailed := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-cleanup-failed",
		CommandID:     start.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionCleanupFailed,
		ErrorCode:     domain.ExecutionErrorCleanup,
		PayloadDigest: digestForTest("update-cleanup-failed-wire"),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(ctx, cleanupFailed); err != nil || replayed {
		t.Fatalf("cleanup failure update = (%t, %v)", replayed, err)
	}
	assertNodeAdministrativeState(t, controller, nodeID, domain.NodeQuarantined)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations WHERE execution_id='execution-agent-quarantine'", 1)

	quarantined := cleanupFailed
	quarantined.MessageID = "update-quarantined"
	quarantined.State = domain.ExecutionQuarantined
	quarantined.ErrorCode = domain.ExecutionErrorQuarantined
	quarantined.PayloadDigest = digestForTest("update-quarantined-wire")
	if replayed, err := controller.RecordAgentExecutionUpdate(ctx, quarantined); err != nil || replayed {
		t.Fatalf("quarantined update = (%t, %v)", replayed, err)
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(ctx, quarantined); err != nil || !replayed {
		t.Fatalf("exact quarantined replay = (%t, %v)", replayed, err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations WHERE execution_id='execution-agent-quarantine'", 0)
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	controller = openControllerPath(t, path)
	defer controller.Close()
	restart, err := controller.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(restart.Controller.Executions) != 1 ||
		restart.Controller.Executions[0].State != domain.ExecutionQuarantined ||
		len(restart.Controller.Reservations) != 0 {
		t.Fatalf("quarantined restart authority = %#v", restart.Controller)
	}

	other := testAssignment(908, "execution-agent-quarantine-other", nodeID, 1)
	if _, _, err := controller.Assign(ctx, other); err != nil {
		t.Fatal(err)
	}
	rejected := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              "prepare-after-quarantine",
			ControllerEpoch: epoch,
			ExecutionID:     other.Execution.ID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   digestForTest("prepare-after-quarantine"),
		},
	}
	if _, err := controller.CommitAgentCommand(ctx, rejected); err == nil {
		t.Fatal("quarantined node accepted a new command")
	}
	assertCount(t, controller.db, "SELECT count(*) FROM agent_commands WHERE command_id='prepare-after-quarantine'", 0)
	if _, err := controller.RevokeNode(ctx, nodeID); err != nil {
		t.Fatal(err)
	}
	assertNodeAdministrativeState(t, controller, nodeID, domain.NodeRevoked)
	snapshot, err := controller.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Nodes) != 1 ||
		snapshot.Nodes[0] != (NodeAdministration{NodeID: domain.NodeID(nodeID), State: domain.NodeRevoked}) {
		t.Fatalf("revoked node authority snapshot = %+v", snapshot.Nodes)
	}
}

func TestControllerSnapshotAuthorityRejectsUnknownEvidenceAtomically(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-snapshot-authority.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 5)
	assignment, prepare := assignAndIssuePrepare(t, controller, nodeID, epoch, 909, "execution-snapshot-authority", 0)

	unknownCommand := prepare.Command
	unknownCommand.ID = "unknown-controller-command"
	unknownCommand.PayloadDigest = digestForTest("unknown-controller-command")
	if err := controller.RecordAgentSnapshot(ctx, NodeAgentSnapshot{
		NodeID:       domain.NodeID(nodeID),
		OS:           domain.OSLinux,
		Architecture: domain.ArchAMD64,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
			Commands:           []domain.Command{unknownCommand},
		},
	}); err == nil {
		t.Fatal("snapshot accumulated a command unknown to the Controller")
	}
	assertCount(t, controller.db, "SELECT count(*) FROM agent_session_snapshots", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_snapshot_commands", 0)

	snapshot := NodeAgentSnapshot{
		NodeID:       domain.NodeID(nodeID),
		OS:           domain.OSLinux,
		Architecture: domain.ArchAMD64,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
			Commands:           []domain.Command{prepare.Command},
			Observations: []ObservationSnapshot{{
				ExecutionID:        assignment.Execution.ID,
				State:              domain.ExecutionPreparing,
				ObservedAtUnixNano: 100,
			}},
			CleanupTombstones: []CleanupTombstoneSnapshot{
				{
					ExecutionID:        assignment.Execution.ID,
					FailureCode:        CleanupWorkspaceRemoval,
					RecordedAtUnixNano: 101,
				},
				{
					ExecutionID:        "unknown-execution",
					FailureCode:        CleanupProcessResidue,
					RecordedAtUnixNano: 102,
				},
			},
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err == nil {
		t.Fatal("snapshot accumulated evidence for an unknown execution")
	}
	assertCount(t, controller.db, "SELECT count(*) FROM agent_session_snapshots", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_snapshot_commands", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_snapshot_observations", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_snapshot_cleanup_tombstones", 0)
	assertNodeAdministrativeState(t, controller, nodeID, domain.NodeActive)

	foreign := testAssignment(910, "execution-owned-by-another-node", "different-node", 0)
	if _, _, err := controller.Assign(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	foreignObservation := snapshot
	foreignObservation.Journal.CleanupTombstones = nil
	foreignObservation.Journal.Observations[0].ExecutionID = foreign.Execution.ID
	if err := controller.RecordAgentSnapshot(ctx, foreignObservation); err == nil {
		t.Fatal("snapshot accumulated an observation owned by another node")
	}
	assertCount(t, controller.db, "SELECT count(*) FROM agent_session_snapshots", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_snapshot_observations", 0)

	snapshot.Journal.Observations[0].ExecutionID = assignment.Execution.ID
	snapshot.Journal.CleanupTombstones = snapshot.Journal.CleanupTombstones[:1]
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatalf("known cleanup tombstone snapshot = %v", err)
	}
	assertNodeAdministrativeState(t, controller, nodeID, domain.NodeQuarantined)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations WHERE execution_id='execution-snapshot-authority'", 1)
}

func TestControllerMissingNodeAuthorityFailsClosed(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-missing-node-authority.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 6)
	assignment := testAssignment(911, "execution-missing-node-authority", nodeID, 0)
	if _, _, err := controller.Assign(ctx, assignment); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.db.Exec(`DELETE FROM node_administrative_states WHERE node_id = ?`, nodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Snapshot(ctx); err == nil {
		t.Fatal("snapshot hid an enrolled node whose administrative authority was missing")
	}
	prepare := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              "prepare-missing-node-authority",
			ControllerEpoch: epoch,
			ExecutionID:     assignment.Execution.ID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   digestForTest("prepare-missing-node-authority"),
		},
	}
	if _, err := controller.CommitAgentCommand(ctx, prepare); err == nil {
		t.Fatal("command issuance ignored missing node administrative authority")
	}
	assertCount(t, controller.db, "SELECT count(*) FROM agent_commands WHERE command_id='prepare-missing-node-authority'", 0)
}

func TestControllerAgentSnapshotRetainsLastKnownEvidenceAndRejectsRegression(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-snapshot.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 2)
	assignment := testAssignment(902, "execution-agent-2", nodeID, 0)
	if _, _, err := controller.Assign(ctx, assignment); err != nil {
		t.Fatal(err)
	}
	cleanupAssignment := testAssignment(903, "cleanup-agent-2", nodeID, 1)
	if _, _, err := controller.Assign(ctx, cleanupAssignment); err != nil {
		t.Fatal(err)
	}
	command := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              "prepare-agent-2",
			ControllerEpoch: epoch,
			ExecutionID:     assignment.Execution.ID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   digestForTest("prepare-agent-2"),
		},
	}
	if _, err := controller.CommitAgentCommand(ctx, command); err != nil {
		t.Fatal(err)
	}
	first := NodeAgentSnapshot{
		NodeID:            domain.NodeID(nodeID),
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
			Commands:           []domain.Command{command.Command},
			Observations: []ObservationSnapshot{{
				ExecutionID:        assignment.Execution.ID,
				State:              domain.ExecutionPreparing,
				ObservedAtUnixNano: 100,
			}},
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, first); err != nil {
		t.Fatal(err)
	}
	var nativeRunnerReady int
	if err := controller.db.QueryRow(`SELECT native_runner_ready
		FROM agent_session_snapshots WHERE node_id = ?`, nodeID).Scan(&nativeRunnerReady); err != nil ||
		nativeRunnerReady != 1 {
		t.Fatalf("stored native runner readiness = %d, %v", nativeRunnerReady, err)
	}
	advanced := first
	advanced.NativeRunnerReady = false
	advanced.Journal.Observations = []ObservationSnapshot{{
		ExecutionID:        assignment.Execution.ID,
		State:              domain.ExecutionReleased,
		ObservedAtUnixNano: 200,
	}}
	advanced.Journal.CleanupTombstones = []CleanupTombstoneSnapshot{{
		ExecutionID:        cleanupAssignment.Execution.ID,
		FailureCode:        CleanupWorkspaceRemoval,
		RecordedAtUnixNano: 190,
	}}
	if err := controller.RecordAgentSnapshot(ctx, advanced); err != nil {
		t.Fatalf("offline forward snapshot = %v", err)
	}
	if err := controller.db.QueryRow(`SELECT native_runner_ready
		FROM agent_session_snapshots WHERE node_id = ?`, nodeID).Scan(&nativeRunnerReady); err != nil ||
		nativeRunnerReady != 0 {
		t.Fatalf("degraded native runner readiness = %d, %v", nativeRunnerReady, err)
	}

	regressed := advanced
	regressed.Journal.Observations = []ObservationSnapshot{{
		ExecutionID:        assignment.Execution.ID,
		State:              domain.ExecutionRunning,
		ObservedAtUnixNano: 300,
	}}
	if err := controller.RecordAgentSnapshot(ctx, regressed); err == nil {
		t.Fatal("terminal snapshot state regressed to running")
	}
	future := advanced
	future.Journal.MaxControllerEpoch = epoch + 1
	if err := controller.RecordAgentSnapshot(ctx, future); err == nil {
		t.Fatal("future controller epoch snapshot was accepted")
	}
	mismatchedCommand := advanced
	mismatchedCommand.Journal.Commands = append([]domain.Command(nil), command.Command)
	mismatchedCommand.Journal.Commands[0].PayloadDigest = digestForTest("forged-command")
	if err := controller.RecordAgentSnapshot(ctx, mismatchedCommand); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("forged accepted command = %v", err)
	}
	duplicate := advanced
	duplicate.Journal.Commands = []domain.Command{command.Command, command.Command}
	if err := controller.RecordAgentSnapshot(ctx, duplicate); err == nil {
		t.Fatal("duplicate command snapshot was accepted")
	}

	// Failed updates never replace last-known evidence with an empty healthy
	// view because snapshot rows are updated individually, not delete-and-fill.
	assertCount(t, controller.db, "SELECT count(*) FROM agent_snapshot_commands", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_snapshot_observations WHERE state='released'", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_snapshot_cleanup_tombstones", 1)
}

func TestControllerAgentSnapshotKeepsFirstPlatformImmutableAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller-platform-authority.db")
	controller := openControllerPath(t, path)
	nodeID, epoch := enrollControllerAgentNode(t, controller, 7)
	first := NodeAgentSnapshot{
		NodeID:            domain.NodeID(nodeID),
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, first); err != nil {
		t.Fatal(err)
	}
	mismatched := first
	mismatched.OS = domain.OSMacOS
	mismatched.Architecture = domain.ArchARM64
	mismatched.NativeRunnerReady = false
	if err := controller.RecordAgentSnapshot(ctx, mismatched); err == nil {
		t.Fatal("changed Agent platform replaced immutable topology")
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openControllerPath(t, path)
	defer reopened.Close()
	restart, err := reopened.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(restart.NodeTopology) != 1 {
		t.Fatalf("restart topology = %#v", restart.NodeTopology)
	}
	topology := restart.NodeTopology[0]
	if topology.NodeID != domain.NodeID(nodeID) ||
		topology.OS != domain.OSLinux ||
		topology.Architecture != domain.ArchAMD64 ||
		!topology.LastNativeRunnerReady {
		t.Fatalf("rejected platform changed restart authority: %#v", topology)
	}
}

func TestControllerCurrentAgentSnapshotReplacesMembershipWithoutDeletingHistory(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-current-snapshot-membership.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 8)
	assignment, prepare := assignAndIssuePrepare(
		t,
		controller,
		nodeID,
		epoch,
		912,
		"execution-current-snapshot-membership",
		0,
	)
	observation := ObservationSnapshot{
		ExecutionID:        assignment.Execution.ID,
		State:              domain.ExecutionPreparing,
		ObservedAtUnixNano: 100,
	}
	first := NodeAgentSnapshot{
		NodeID:            domain.NodeID(nodeID),
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
			Commands:           []domain.Command{prepare.Command},
			Observations:       []ObservationSnapshot{observation},
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, first); err != nil {
		t.Fatal(err)
	}
	firstDigest, err := nodeAgentSnapshotDigest(first)
	if err != nil {
		t.Fatal(err)
	}

	// A later full snapshot omits the command and observation. Historical rows
	// remain for audit, but neither row may authorize a new Controller mutation.
	second := first
	second.NativeRunnerReady = false
	second.Journal.Commands = nil
	second.Journal.Observations = nil
	if err := controller.RecordAgentSnapshot(ctx, second); err != nil {
		t.Fatal(err)
	}
	secondDigest, err := nodeAgentSnapshotDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if secondDigest == firstDigest {
		t.Fatal("different full snapshots produced the same authority digest")
	}
	assertCount(t, controller.db, "SELECT count(*) FROM agent_snapshot_commands", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_snapshot_observations", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_current_snapshot_commands", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_current_snapshot_observations", 0)

	if _, _, err := controller.AdoptAgentSnapshotObservation(
		ctx,
		domain.NodeID(nodeID),
		domain.ExecutionReserved,
		observation,
		firstDigest,
		epoch,
	); err == nil {
		t.Fatal("superseded Agent snapshot digest authorized desired-state adoption")
	}
	if _, _, err := controller.AdoptAgentSnapshotObservation(
		ctx,
		domain.NodeID(nodeID),
		domain.ExecutionReserved,
		observation,
		secondDigest,
		epoch,
	); err == nil {
		t.Fatal("historical observation absent from the current snapshot authorized adoption")
	}
	assertExecutionState(t, controller, assignment.Execution.ID, domain.ExecutionReserved)
	assertCount(
		t,
		controller.db,
		"SELECT count(*) FROM slot_reservations WHERE execution_id='execution-current-snapshot-membership'",
		1,
	)
}

func TestControllerTerminalSnapshotWaitsForExactOutboxUpdate(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-terminal-snapshot-outbox-order.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 8)
	assignment, prepare := assignAndIssuePrepare(
		t,
		controller,
		nodeID,
		epoch,
		913,
		"execution-terminal-snapshot-outbox-order",
		0,
	)
	observation := ObservationSnapshot{
		ExecutionID:        assignment.Execution.ID,
		State:              domain.ExecutionReleased,
		ObservedAtUnixNano: 100,
	}
	snapshot := NodeAgentSnapshot{
		NodeID:            domain.NodeID(nodeID),
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
			Commands:           []domain.Command{prepare.Command},
			Observations:       []ObservationSnapshot{observation},
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	snapshotDigest, err := nodeAgentSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := controller.AdoptAgentSnapshotObservation(
		ctx,
		domain.NodeID(nodeID),
		domain.ExecutionReserved,
		observation,
		snapshotDigest,
		epoch,
	); !errors.Is(err, ErrAgentTerminalUpdatePending) {
		t.Fatalf("terminal snapshot adoption = %v, want pending outbox", err)
	}
	assertExecutionState(
		t,
		controller,
		assignment.Execution.ID,
		domain.ExecutionReserved,
	)
	assertCount(
		t,
		controller.db,
		`SELECT count(*) FROM slot_reservations WHERE execution_id =
			'execution-terminal-snapshot-outbox-order'`,
		1,
	)

	update := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-terminal-snapshot-outbox-order",
		CommandID:     prepare.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionReleased,
		PayloadDigest: digestForTest("update-terminal-snapshot-outbox-order"),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(
		ctx,
		update,
	); err != nil || replayed {
		t.Fatalf("terminal outbox update = (%t, %v)", replayed, err)
	}
	assertExecutionState(
		t,
		controller,
		assignment.Execution.ID,
		domain.ExecutionReleased,
	)
	assertCount(
		t,
		controller.db,
		`SELECT count(*) FROM slot_reservations WHERE execution_id =
			'execution-terminal-snapshot-outbox-order'`,
		0,
	)

	adopted, changed, err := controller.AdoptAgentSnapshotObservation(
		ctx,
		domain.NodeID(nodeID),
		domain.ExecutionReleased,
		observation,
		snapshotDigest,
		epoch,
	)
	if err != nil || changed || adopted.State != domain.ExecutionReleased {
		t.Fatalf(
			"terminal snapshot replay after outbox = (%#v, %t, %v)",
			adopted,
			changed,
			err,
		)
	}
}

func TestControllerReconciliationCancelRequiresTerminalDesiredAndExactCurrentSnapshot(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-reconciliation-cancel.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 9)
	assignment, prepare := assignAndIssuePrepare(
		t,
		controller,
		nodeID,
		epoch,
		913,
		"execution-reconciliation-cancel",
		0,
	)
	preparing := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-reconciliation-preparing",
		CommandID:     prepare.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionPreparing,
		PayloadDigest: digestForTest("update-reconciliation-preparing"),
	}
	if _, err := controller.RecordAgentExecutionUpdate(ctx, preparing); err != nil {
		t.Fatal(err)
	}
	start := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandStart,
		Command: domain.Command{
			ID:              "start-reconciliation-cancel",
			ControllerEpoch: epoch,
			ExecutionID:     assignment.Execution.ID,
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   digestForTest("start-reconciliation-cancel"),
		},
	}
	if _, err := controller.CommitAgentCommand(ctx, start); err != nil {
		t.Fatal(err)
	}
	released := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-reconciliation-released",
		CommandID:     start.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionReleased,
		PayloadDigest: digestForTest("update-reconciliation-released"),
	}
	if _, err := controller.RecordAgentExecutionUpdate(ctx, released); err != nil {
		t.Fatal(err)
	}
	assertExecutionState(t, controller, assignment.Execution.ID, domain.ExecutionReleased)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)

	// Desired state is terminal, while the Agent's current authenticated journal
	// still proves a local runtime. This contradiction is the only boundary in
	// which a recovery-only Cancel may bypass the ordinary desired-state CAS.
	observation := ObservationSnapshot{
		ExecutionID:        assignment.Execution.ID,
		State:              domain.ExecutionRunning,
		ObservedAtUnixNano: 200,
	}
	snapshot := NodeAgentSnapshot{
		NodeID:            domain.NodeID(nodeID),
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
			Commands:           []domain.Command{prepare.Command, start.Command},
			Observations:       []ObservationSnapshot{observation},
		},
	}
	reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	snapshotDigest, err := nodeAgentSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	cancel := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandCancel,
		Command: domain.Command{
			ID:              "cancel-reconciliation-runtime",
			ControllerEpoch: reconciliationEpoch,
			ExecutionID:     assignment.Execution.ID,
			ExpectedState:   domain.ExecutionRunning,
			PayloadDigest:   digestForTest("cancel-reconciliation-runtime"),
		},
	}
	if replayed, err := controller.CommitAgentReconciliationCommand(
		ctx,
		cancel,
		snapshotDigest,
	); err != nil || replayed {
		t.Fatalf("commit reconciliation Cancel = (%t, %v)", replayed, err)
	}
	if replayed, err := controller.CommitAgentReconciliationCommand(
		ctx,
		cancel,
		snapshotDigest,
	); err != nil || !replayed {
		t.Fatalf("replay reconciliation Cancel = (%t, %v)", replayed, err)
	}

	ordinary := cancel
	ordinary.Command.ID = "ordinary-cancel-terminal-desired"
	ordinary.Command.PayloadDigest = digestForTest("ordinary-cancel-terminal-desired")
	if _, err := controller.CommitAgentCommand(ctx, ordinary); err == nil {
		t.Fatal("ordinary Cancel bypassed terminal desired-state authority")
	}
	assertCount(
		t,
		controller.db,
		"SELECT count(*) FROM reconciliation_agent_commands WHERE command_id='cancel-reconciliation-runtime'",
		1,
	)
	assertCount(
		t,
		controller.db,
		"SELECT count(*) FROM agent_commands WHERE command_id='ordinary-cancel-terminal-desired'",
		0,
	)

	terminalUpdate := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-reconciliation-runtime-released",
		CommandID:     cancel.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionReleased,
		PayloadDigest: digestForTest("update-reconciliation-runtime-released"),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(
		ctx,
		terminalUpdate,
	); err != nil || replayed {
		t.Fatalf("record reconciliation terminal update = (%t, %v)", replayed, err)
	}
	assertExecutionState(t, controller, assignment.Execution.ID, domain.ExecutionReleased)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
	assertCount(
		t,
		controller.db,
		"SELECT count(*) FROM agent_execution_updates WHERE message_id='update-reconciliation-runtime-released'",
		1,
	)
}

func TestControllerReconciliationCleanupFailurePreservesTerminalDesiredAndQuarantines(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-reconciliation-cleanup-failed.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 1)
	assignment, prepare := assignAndIssuePrepare(
		t,
		controller,
		nodeID,
		epoch,
		914,
		"execution-reconciliation-cleanup-failed",
		0,
	)
	failed := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-terminal-before-reconciliation",
		CommandID:     prepare.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionFailed,
		ErrorCode:     domain.ExecutionErrorStart,
		PayloadDigest: digestForTest("update-terminal-before-reconciliation"),
	}
	if _, err := controller.RecordAgentExecutionUpdate(ctx, failed); err != nil {
		t.Fatal(err)
	}
	observation := ObservationSnapshot{
		ExecutionID:        assignment.Execution.ID,
		State:              domain.ExecutionPreparing,
		ObservedAtUnixNano: 300,
	}
	snapshot := NodeAgentSnapshot{
		NodeID:            domain.NodeID(nodeID),
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
			Commands:           []domain.Command{prepare.Command},
			Observations:       []ObservationSnapshot{observation},
		},
	}
	reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	snapshotDigest, err := nodeAgentSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	cancel := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandCancel,
		Command: domain.Command{
			ID:              "cancel-reconciliation-cleanup-failed",
			ControllerEpoch: reconciliationEpoch,
			ExecutionID:     assignment.Execution.ID,
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   digestForTest("cancel-reconciliation-cleanup-failed"),
		},
	}
	if _, err := controller.CommitAgentReconciliationCommand(
		ctx,
		cancel,
		snapshotDigest,
	); err != nil {
		t.Fatal(err)
	}
	cleanupFailed := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "update-reconciliation-cleanup-failed",
		CommandID:     cancel.Command.ID,
		ExecutionID:   assignment.Execution.ID,
		State:         domain.ExecutionCleanupFailed,
		ErrorCode:     domain.ExecutionErrorCleanup,
		PayloadDigest: digestForTest("update-reconciliation-cleanup-failed"),
	}
	if _, err := controller.RecordAgentExecutionUpdate(ctx, cleanupFailed); err != nil {
		t.Fatal(err)
	}
	assertExecutionState(t, controller, assignment.Execution.ID, domain.ExecutionFailed)
	assertNodeAdministrativeState(t, controller, nodeID, domain.NodeQuarantined)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
}

func TestControllerFailsDesiredExecutionOnlyFromExactCurrentSnapshotAbsence(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-snapshot-absence.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 2)
	assignment, prepare := assignAndIssuePrepare(
		t,
		controller,
		nodeID,
		epoch,
		915,
		"execution-snapshot-absence",
		0,
	)
	first := NodeAgentSnapshot{
		NodeID:       domain.NodeID(nodeID),
		OS:           domain.OSLinux,
		Architecture: domain.ArchAMD64,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
			Commands:           []domain.Command{prepare.Command},
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, first); err != nil {
		t.Fatal(err)
	}
	firstDigest, err := nodeAgentSnapshotDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.RunnerVersion = "2.0.0"
	if err := controller.RecordAgentSnapshot(ctx, second); err != nil {
		t.Fatal(err)
	}
	secondDigest, err := nodeAgentSnapshotDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.FailDesiredExecutionFromSnapshot(
		ctx,
		domain.NodeID(nodeID),
		assignment.Execution.ID,
		domain.ExecutionReserved,
		firstDigest,
		epoch,
	); err == nil {
		t.Fatal("superseded Agent snapshot absence released desired capacity")
	}
	assertExecutionState(t, controller, assignment.Execution.ID, domain.ExecutionReserved)
	assertCount(
		t,
		controller.db,
		"SELECT count(*) FROM slot_reservations WHERE execution_id='execution-snapshot-absence'",
		1,
	)

	failedExecution, err := controller.FailDesiredExecutionFromSnapshot(
		ctx,
		domain.NodeID(nodeID),
		assignment.Execution.ID,
		domain.ExecutionReserved,
		secondDigest,
		epoch,
	)
	if err != nil {
		t.Fatal(err)
	}
	if failedExecution.State != domain.ExecutionFailed {
		t.Fatalf("failed execution = %#v", failedExecution)
	}
	assertExecutionState(t, controller, assignment.Execution.ID, domain.ExecutionFailed)
	assertCount(
		t,
		controller.db,
		"SELECT count(*) FROM slot_reservations WHERE execution_id='execution-snapshot-absence'",
		0,
	)
}

func TestControllerAgentReadinessCASPreservesFullSnapshotMembership(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-readiness-cas.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 1)
	assignment, prepare := assignAndIssuePrepare(
		t, controller, nodeID, epoch, 916, "execution-readiness-cas", 0)
	snapshot := NodeAgentSnapshot{
		NodeID:            domain.NodeID(nodeID),
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		RunnerVersion:     "2.336.0",
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
			Commands:           []domain.Command{prepare.Command},
			Observations: []ObservationSnapshot{{
				ExecutionID:        assignment.Execution.ID,
				State:              domain.ExecutionPreparing,
				ObservedAtUnixNano: 1,
			}},
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	digest, err := nodeAgentSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.RecordAgentReadiness(
		ctx, snapshot.NodeID, digest, false,
	); err != nil {
		t.Fatal(err)
	}
	var commandCount, observationCount int
	if err := controller.db.QueryRowContext(ctx, `SELECT count(*)
		FROM agent_current_snapshot_commands
		WHERE node_id=? AND command_id=? AND snapshot_digest=?`,
		snapshot.NodeID, prepare.Command.ID, digest).Scan(&commandCount); err != nil {
		t.Fatal(err)
	}
	if err := controller.db.QueryRowContext(ctx, `SELECT count(*)
		FROM agent_current_snapshot_observations
		WHERE node_id=? AND execution_id=? AND state='preparing'
			AND snapshot_digest=?`,
		snapshot.NodeID, assignment.Execution.ID, digest).Scan(
		&observationCount); err != nil {
		t.Fatal(err)
	}
	if commandCount != 1 || observationCount != 1 {
		t.Fatalf("readiness rewrote full snapshot membership: commands=%d observations=%d",
			commandCount, observationCount)
	}
	var ready bool
	var revision uint64
	var retainedDigest string
	if err := controller.db.QueryRowContext(ctx, `SELECT s.native_runner_ready,
		a.revision, a.snapshot_digest
		FROM agent_session_snapshots s
		JOIN agent_snapshot_authority a ON a.node_id=s.node_id
		WHERE s.node_id=?`, snapshot.NodeID).Scan(
		&ready, &revision, &retainedDigest); err != nil {
		t.Fatal(err)
	}
	if ready || revision != 2 || retainedDigest != digest {
		t.Fatalf("readiness authority = ready:%t revision:%d digest:%q",
			ready, revision, retainedDigest)
	}
	if err := controller.RecordAgentDisconnect(
		ctx, snapshot.NodeID, digest,
	); err != nil {
		t.Fatal(err)
	}
	if err := controller.db.QueryRowContext(ctx, `SELECT s.native_runner_ready,
		a.revision, a.snapshot_digest
		FROM agent_session_snapshots s
		JOIN agent_snapshot_authority a ON a.node_id=s.node_id
		WHERE s.node_id=?`, snapshot.NodeID).Scan(
		&ready, &revision, &retainedDigest); err != nil {
		t.Fatal(err)
	}
	if ready || revision != 3 || retainedDigest != digest {
		t.Fatalf("disconnect authority = ready:%t revision:%d digest:%q",
			ready, revision, retainedDigest)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM agent_current_snapshot_commands", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM agent_current_snapshot_observations", 1)

	newer := snapshot
	newer.RunnerVersion = "2.337.0"
	if err := controller.RecordAgentSnapshot(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := controller.RecordAgentReadiness(
		ctx, snapshot.NodeID, digest, true,
	); err == nil {
		t.Fatal("stale readiness digest advanced a newer full snapshot")
	}
	if err := controller.RecordAgentDisconnect(
		ctx, snapshot.NodeID, digest,
	); err == nil {
		t.Fatal("stale disconnect digest revoked a newer full snapshot")
	}
}

func enrollControllerAgentNode(t *testing.T, controller *ControllerStore, seed int) (string, domain.ControllerEpoch) {
	t.Helper()
	ctx := context.Background()
	epoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	token := enrollmentToken(byte(seed), uint64(epoch))
	if err := controller.CreateToken(ctx, token); err != nil {
		t.Fatal(err)
	}
	nodeID := enrollmentNodeID(seed)
	if err := controller.ConsumeEnrollment(ctx, token, enrollmentNodeRecord(nodeID, "a"+string(rune('0'+seed)), time.Unix(100, 0))); err != nil {
		t.Fatal(err)
	}
	return nodeID, epoch
}

func assignAndIssuePrepare(
	t *testing.T,
	controller *ControllerStore,
	nodeID string,
	epoch domain.ControllerEpoch,
	messageID MessageID,
	executionID string,
	slot int,
) (Assignment, IssuedAgentCommand) {
	t.Helper()
	ctx := context.Background()
	assignment := testAssignment(messageID, executionID, nodeID, slot)
	if _, replayed, err := controller.Assign(ctx, assignment); err != nil || replayed {
		t.Fatalf("assign %s = (%t, %v)", executionID, replayed, err)
	}
	prepare := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              domain.CommandID("prepare-" + executionID),
			ControllerEpoch: epoch,
			ExecutionID:     assignment.Execution.ID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   digestForTest("prepare-" + executionID),
		},
	}
	if replayed, err := controller.CommitAgentCommand(ctx, prepare); err != nil || replayed {
		t.Fatalf("commit prepare %s = (%t, %v)", executionID, replayed, err)
	}
	return assignment, prepare
}

func assertExecutionState(t *testing.T, controller *ControllerStore, executionID domain.ExecutionID, want domain.ExecutionState) {
	t.Helper()
	var got domain.ExecutionState
	if err := controller.db.QueryRow(`SELECT state FROM executions WHERE id = ?`, executionID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("execution %s state = %q, want %q", executionID, got, want)
	}
}

func assertNodeAdministrativeState(
	t *testing.T,
	controller *ControllerStore,
	nodeID string,
	want domain.NodeAdministrativeState,
) {
	t.Helper()
	var got domain.NodeAdministrativeState
	if err := controller.db.QueryRow(`SELECT administrative_state
		FROM node_administrative_states WHERE node_id = ?`, nodeID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("node %s administrative state = %q, want %q", nodeID, got, want)
	}
}

func digestForTest(value string) string {
	return domain.PayloadDigest([]byte(value))
}

func assertDatabaseExcludesText(t *testing.T, controller *ControllerStore, value string) {
	t.Helper()
	if value == "" {
		t.Fatal("secret canary must not be empty")
	}
	for _, table := range []string{
		"agent_commands",
		"agent_session_snapshots",
		"agent_snapshot_commands",
		"agent_snapshot_observations",
		"agent_snapshot_cleanup_tombstones",
		"agent_execution_updates",
	} {
		rows, err := controller.db.Query("SELECT * FROM " + table)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for rows.Next() {
			for index := range values {
				pointers[index] = &values[index]
			}
			if err := rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			for _, field := range values {
				if text, ok := field.(string); ok && strings.Contains(text, value) {
					rows.Close()
					t.Fatalf("secret canary persisted in %s", table)
				}
			}
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
