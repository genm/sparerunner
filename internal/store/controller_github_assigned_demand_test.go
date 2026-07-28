package store

import (
	"context"
	"testing"

	"github.com/genm/tewake/internal/domain"
)

// recordAssignedDemandForTest publishes the one statistic the scale-set
// protocol actually scales on.
func recordAssignedDemandForTest(
	t *testing.T,
	controller *ControllerStore,
	scaleSetID ScaleSetID,
	assigned int,
) {
	t.Helper()
	if err := controller.RecordGitHubSessionDemand(
		context.Background(),
		GitHubSessionDemand{
			ScaleSetID:        scaleSetID,
			SessionID:         "session-assigned-demand",
			TotalAssignedJobs: assigned,
		},
	); err != nil {
		t.Fatal(err)
	}
}

// TestGitHubAssignedDemandRaisesExecutionsToAssignedCount is the regression for
// the live failure: GitHub assigned the queued job directly and never offered
// it, so TotalAssignedJobs was 1, TotalAvailableJobs was 0, no JobAvailable ever
// arrived, and the workflow sat queued forever while Tewake acknowledged every
// message correctly.
func TestGitHubAssignedDemandRaisesExecutionsToAssignedCount(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-assigned-demand.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 6)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	const scaleSetID ScaleSetID = 7
	enableGitHubClaimForTest(t, controller, &binding, scaleSetID, domain.ArchAMD64)
	recordAssignedDemandForTest(t, controller, scaleSetID, 1)

	demand, err := controller.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil {
		t.Fatal(err)
	}
	if !demand.Observed || demand.Desired != 1 || demand.Active != 0 ||
		demand.Created == nil || demand.Unserved != 0 {
		t.Fatalf("assigned demand reconciliation = %#v", demand)
	}
	claim := *demand.Created
	if claim.Origin != GitHubClaimFromAssignedDemand {
		t.Fatalf("claim origin = %q", claim.Origin)
	}
	// The claim key must never impersonate a provider request ID.
	if claim.ClaimKey >= 0 || claim.RunnerRequestID != 0 ||
		claim.SourceMessageID != 0 {
		t.Fatalf("assigned-demand claim identity = %#v", claim)
	}
	// Nothing to acquire: GitHub already assigned the job and matches it to
	// whichever ephemeral runner registers.
	if claim.State != GitHubClaimAcquired {
		t.Fatalf("assigned-demand claim state = %q, want acquired", claim.State)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 0)

	// Reaching the same statistic again converges to the same count.
	repeat, err := controller.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil {
		t.Fatal(err)
	}
	if repeat.Created != nil || repeat.Active != 1 || repeat.Unserved != 0 {
		t.Fatalf("repeated assigned demand = %#v", repeat)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)

	// The claim is actionable through the shared lifecycle, entering it at the
	// state the acquire handshake would have left an offered job in.
	actionable, found, err := controller.NextActionableGitHubClaim(ctx, scaleSetID)
	if err != nil || !found {
		t.Fatalf("next actionable claim = (%#v, %t, %v)", actionable, found, err)
	}
	if actionable.ClaimKey != claim.ClaimKey ||
		actionable.State != GitHubClaimAcquired {
		t.Fatalf("actionable assigned-demand claim = %#v", actionable)
	}
}

// TestGitHubAssignedDemandNeverInventsUnobservedStatistics proves the empty-poll
// path cannot conjure a runner before GitHub has ever reported a statistic.
func TestGitHubAssignedDemandNeverInventsUnobservedStatistics(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-demand-unobserved.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 6)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	const scaleSetID ScaleSetID = 7
	enableGitHubClaimForTest(t, controller, &binding, scaleSetID, domain.ArchAMD64)

	demand, err := controller.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil {
		t.Fatal(err)
	}
	if demand.Observed || demand.Created != nil || demand.Unserved != 0 {
		t.Fatalf("unobserved demand = %#v", demand)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 0)

	// Zero assigned jobs is an observation, not demand.
	recordAssignedDemandForTest(t, controller, scaleSetID, 0)
	idle, err := controller.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil {
		t.Fatal(err)
	}
	if !idle.Observed || idle.Created != nil || idle.Unserved != 0 {
		t.Fatalf("zero assigned demand = %#v", idle)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 0)
}

// TestGitHubAssignedDemandSurvivesMessageRedeliveryAndRestartWithoutDuplicating
// covers the two ways the same demand is reached twice: commit-before-ack
// redelivery of the message that carried it, and a Controller restart.
func TestGitHubAssignedDemandSurvivesMessageRedeliveryAndRestartWithoutDuplicating(
	t *testing.T,
) {
	ctx := context.Background()
	path := privateTestDir(t) + "/controller-github-demand-redelivery.db"
	controller := openControllerPath(t, path)
	nodeID, _ := enrollControllerAgentNode(t, controller, 6)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	const scaleSetID ScaleSetID = 7
	enableGitHubClaimForTest(t, controller, &binding, scaleSetID, domain.ArchAMD64)
	recordAssignedDemandForTest(t, controller, scaleSetID, 1)

	// A JobAssigned message carries no runner request ID at all, which is
	// exactly the shape live GitHub delivers.
	message := GitHubQueueMessage{
		ScaleSetID: scaleSetID,
		MessageID:  4001,
		Digest:     digestForTest("assigned-demand-message"),
		Jobs: []GitHubJobEvent{{
			Type:           GitHubJobAssigned,
			RepositoryName: "repo",
			OwnerName:      "arieal",
			JobID:          "9a1e0f5c-0000-4000-8000-000000000001",
			WorkflowRunID:  55,
		}},
	}
	commit, err := controller.CommitGitHubQueueMessage(ctx, message, binding)
	if err != nil {
		t.Fatal(err)
	}
	if commit.Claim != nil || commit.AssignedDemand.Created == nil {
		t.Fatalf("assigned message commit = %#v", commit)
	}
	created := *commit.AssignedDemand.Created
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)

	// Redelivery: the ACK never reached GitHub, so the identical message is
	// delivered again. Reconciling to a count, rather than creating one per
	// message seen, is what keeps this from starting a second runner.
	replay, err := controller.CommitGitHubQueueMessage(ctx, message, binding)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed || replay.AssignedDemand.Created != nil ||
		replay.AssignedDemand.Active != 1 {
		t.Fatalf("assigned message redelivery = %#v", replay)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: durable state, not process memory, is the authority.
	restarted := openControllerPath(t, path)
	defer restarted.Close()
	after, err := restarted.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil {
		t.Fatal(err)
	}
	if after.Created != nil || after.Active != 1 || after.Desired != 1 {
		t.Fatalf("assigned demand after restart = %#v", after)
	}
	assertCount(t, restarted.db, "SELECT count(*) FROM github_job_claims", 1)
	assertCount(t, restarted.db, "SELECT count(*) FROM executions", 1)

	surviving, found, err := restarted.GitHubClaim(
		ctx, scaleSetID, created.ClaimKey)
	if err != nil || !found {
		t.Fatalf("claim after restart = (%#v, %t, %v)", surviving, found, err)
	}
	if surviving.Origin != GitHubClaimFromAssignedDemand ||
		surviving.RunnerRequestID != 0 {
		t.Fatalf("restarted claim identity = %#v", surviving)
	}
}

// TestGitHubAssignedDemandRefusedByEveryCapacityGateCreatesNothing proves
// assigned demand raises the desired count without bypassing a single gate, and
// that the refusal is reported rather than dropped.
func TestGitHubAssignedDemandRefusedByEveryCapacityGateCreatesNothing(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-demand-refused.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 6)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	const scaleSetID ScaleSetID = 7
	enableGitHubClaimForTest(t, controller, &binding, scaleSetID, domain.ArchAMD64)
	recordAssignedDemandForTest(t, controller, scaleSetID, 2)

	// The node owner excluded this Target.
	digest := currentSnapshotDigest(t, controller, nodeID)
	exclusions := []domain.TargetID{binding.TargetID}
	if err := controller.RecordNodeOwnerState(
		ctx, binding.NodeID, digest, "", &exclusions, nil); err != nil {
		t.Fatal(err)
	}
	excluded, err := controller.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil {
		t.Fatal(err)
	}
	if excluded.Created != nil || excluded.Unserved != 2 {
		t.Fatalf("excluded target demand = %#v", excluded)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 0)

	empty := []domain.TargetID{}
	if err := controller.RecordNodeOwnerState(
		ctx, binding.NodeID, digest, "", &empty, nil); err != nil {
		t.Fatal(err)
	}

	// The poll itself found no claimable node.
	disabled := binding
	disabled.ClaimEnabled = false
	stopped, err := controller.ReconcileGitHubAssignedDemand(ctx, scaleSetID, disabled)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.Created != nil || stopped.Unserved != 2 {
		t.Fatalf("unclaimable poll demand = %#v", stopped)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 0)

	// The single slot is usable exactly once, so demand of two yields one
	// execution and one honestly reported shortfall.
	first, err := controller.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil {
		t.Fatal(err)
	}
	if first.Created == nil || first.Unserved != 1 {
		t.Fatalf("first served demand = %#v", first)
	}
	occupied, err := controller.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil {
		t.Fatal(err)
	}
	if occupied.Created != nil || occupied.Active != 1 || occupied.Unserved != 1 {
		t.Fatalf("demand against occupied slot = %#v", occupied)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
}

// TestGitHubAssignedDemandFenceCrashLeavesReconcilableNonRunnableState proves an
// assigned-demand execution is fenced before the first provider side effect
// exactly like an offered one, so a crash between deciding and calling GitHub
// cannot silently double-start.
func TestGitHubAssignedDemandFenceCrashLeavesReconcilableNonRunnableState(
	t *testing.T,
) {
	ctx := context.Background()
	path := privateTestDir(t) + "/controller-github-demand-fence-crash.db"
	controller := openControllerPath(t, path)
	nodeID, epoch := enrollControllerAgentNode(t, controller, 6)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	const scaleSetID ScaleSetID = 7
	enableGitHubClaimForTest(t, controller, &binding, scaleSetID, domain.ArchAMD64)
	recordAssignedDemandForTest(t, controller, scaleSetID, 1)

	demand, err := controller.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil || demand.Created == nil {
		t.Fatalf("assigned demand = (%#v, %v)", demand, err)
	}
	claim := *demand.Created
	executionID := claim.Execution.ID

	prepare := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              domain.CommandID("demand-prepare-" + executionID),
			ControllerEpoch: epoch,
			ExecutionID:     executionID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   digestForTest("demand-prepare-" + string(executionID)),
		},
	}
	if replayed, err := controller.CommitAgentCommand(ctx, prepare); err != nil || replayed {
		t.Fatalf("commit prepare = (%t, %v)", replayed, err)
	}
	preparing := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "demand-preparing-" + string(executionID),
		CommandID:     prepare.Command.ID,
		ExecutionID:   executionID,
		State:         domain.ExecutionPreparing,
		PayloadDigest: digestForTest("demand-preparing-" + string(executionID)),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(ctx, preparing); err != nil || replayed {
		t.Fatalf("record preparing = (%t, %v)", replayed, err)
	}
	if err := controller.MarkGitHubPreparing(ctx, scaleSetID, claim.ClaimKey); err != nil {
		t.Fatal(err)
	}
	// The fence is written before GenerateJITConfig is ever called. Crash here.
	attempt, replayedAttempt, err := controller.BeginGitHubJITAttempt(
		ctx, scaleSetID, claim.ClaimKey, epoch, "tewake-demand-runner")
	if err != nil || replayedAttempt {
		t.Fatalf("begin JIT attempt = (%#v, %t, %v)", attempt, replayedAttempt, err)
	}
	if attempt.ClaimKey != claim.ClaimKey || attempt.State != GitHubJITIntent {
		t.Fatalf("assigned-demand JIT attempt = %#v", attempt)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := openControllerPath(t, path)
	defer restarted.Close()
	// Non-runnable: the surviving intent means a JIT body may have crossed
	// GitHub, so the claim is not offered as ordinary actionable work.
	if actionable, found, err := restarted.NextActionableGitHubClaim(
		ctx, scaleSetID); err != nil || found {
		t.Fatalf("crashed intent stayed actionable = (%#v, %t, %v)",
			actionable, found, err)
	}
	// Reconcilable: it is surfaced as a fence naming the exact attempt.
	fence, found, err := restarted.NextGitHubReconciliationFence(ctx, scaleSetID)
	if err != nil || !found {
		t.Fatalf("reconciliation fence = (%#v, %t, %v)", fence, found, err)
	}
	if fence.Claim.ClaimKey != claim.ClaimKey ||
		fence.Claim.Origin != GitHubClaimFromAssignedDemand ||
		fence.Attempt == nil || fence.Attempt.Attempt != attempt.Attempt {
		t.Fatalf("assigned-demand fence = %#v", fence)
	}
	// A second reconciliation of the same demand must not add a runner while
	// the crashed one is still unresolved.
	after, err := restarted.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil {
		t.Fatal(err)
	}
	if after.Created != nil || after.Active != 1 {
		t.Fatalf("demand during pending reconciliation = %#v", after)
	}
}

// TestGitHubAssignedDemandCorrelatesLifecycleByRunnerIdentity proves the
// zero-request JobStarted and JobCompleted events GitHub sends for an assigned
// job resolve to the demand-created attempt through provider runner identity,
// which is the only correlation such a claim can have.
func TestGitHubAssignedDemandCorrelatesLifecycleByRunnerIdentity(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-demand-correlation.db")
	defer controller.Close()
	nodeID, epoch := enrollControllerAgentNode(t, controller, 6)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	const scaleSetID ScaleSetID = 7
	const runnerName = "tewake-demand-correlated"
	const runnerID = 4242
	enableGitHubClaimForTest(t, controller, &binding, scaleSetID, domain.ArchAMD64)
	recordAssignedDemandForTest(t, controller, scaleSetID, 1)

	demand, err := controller.ReconcileGitHubAssignedDemand(ctx, scaleSetID, binding)
	if err != nil || demand.Created == nil {
		t.Fatalf("assigned demand = (%#v, %v)", demand, err)
	}
	claim := *demand.Created
	executionID := claim.Execution.ID

	prepare := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              domain.CommandID("demand-prepare-" + executionID),
			ControllerEpoch: epoch,
			ExecutionID:     executionID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   digestForTest("demand-prepare-" + string(executionID)),
		},
	}
	if replayed, err := controller.CommitAgentCommand(ctx, prepare); err != nil || replayed {
		t.Fatalf("commit prepare = (%t, %v)", replayed, err)
	}
	preparing := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "demand-preparing-" + string(executionID),
		CommandID:     prepare.Command.ID,
		ExecutionID:   executionID,
		State:         domain.ExecutionPreparing,
		PayloadDigest: digestForTest("demand-preparing-" + string(executionID)),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(ctx, preparing); err != nil || replayed {
		t.Fatalf("record preparing = (%t, %v)", replayed, err)
	}
	if err := controller.MarkGitHubPreparing(ctx, scaleSetID, claim.ClaimKey); err != nil {
		t.Fatal(err)
	}
	attempt, _, err := controller.BeginGitHubJITAttempt(
		ctx, scaleSetID, claim.ClaimKey, epoch, runnerName)
	if err != nil {
		t.Fatal(err)
	}
	startCommandID := domain.CommandID("demand-start-" + executionID)
	if err := controller.MarkGitHubJITGenerated(
		ctx, attempt, runnerID, digestForTest("demand-jit"), startCommandID,
	); err != nil {
		t.Fatal(err)
	}
	attempt.RunnerID = runnerID
	attempt.JITDigest = digestForTest("demand-jit")
	attempt.StartCommandID = startCommandID
	attempt.State = GitHubJITGenerated

	// Live GitHub reports pickup for an assigned job with no runner request ID,
	// naming only the provider runner it matched.
	lifecycle := GitHubQueueMessage{
		ScaleSetID: scaleSetID,
		MessageID:  4101,
		Digest:     digestForTest("demand-lifecycle-message"),
		Jobs: []GitHubJobEvent{{
			Type:       GitHubJobStarted,
			RunnerID:   runnerID,
			RunnerName: runnerName,
		}},
	}
	if _, err := controller.CommitGitHubQueueMessage(ctx, lifecycle, binding); err != nil {
		t.Fatal(err)
	}
	proven, err := githubJITAttemptPickupProven(ctx, controller.db, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if !proven {
		t.Fatal("zero-request JobStarted did not correlate to the assigned-demand attempt")
	}

	// A completion for a different provider runner must not correlate.
	other := attempt
	other.RunnerID = runnerID + 1
	other.RunnerName = runnerName + "-other"
	if _, err := githubJITAttemptPickupProven(ctx, controller.db, other); err == nil {
		t.Fatal("unknown provider runner identity was accepted as pickup proof")
	}
}
