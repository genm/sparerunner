package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/runner"
)

func TestControllerRestartSnapshotIsCoherentSecretFreeAndSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller-restart-read-model.db")
	controller := openControllerPath(t, path)

	attempt, start := prepareGitHubStartDispatchForArchTest(
		t,
		controller,
		3,
		91,
		901,
		9001,
		domain.ArchARM64,
	)
	const secretCanary = "jit-config-secret-canary.example.test"
	jitDigest := digestForTest(secretCanary)
	if _, err := controller.db.ExecContext(ctx, `UPDATE github_jit_attempts
		SET jit_digest = ?
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		jitDigest,
		attempt.ScaleSetID,
		attempt.ClaimKey,
		attempt.Attempt,
	); err != nil {
		t.Fatal(err)
	}
	attempt.JITDigest = jitDigest
	if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.State = GitHubJITStartAmbiguous
	claim, found, err := controller.GitHubClaim(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found {
		t.Fatalf("GitHub claim = (%#v, %t, %v)", claim, found, err)
	}
	snapshot := NodeAgentSnapshot{
		NodeID:            start.NodeID,
		OS:                domain.OSLinux,
		Architecture:      domain.ArchARM64,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
			Commands:           []domain.Command{start.Command},
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}

	before, err := controller.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.Controller.ControllerEpoch != start.Command.ControllerEpoch ||
		len(before.Controller.Reservations) != 1 ||
		len(before.Controller.Executions) != 1 ||
		len(before.NodeTopology) != 1 ||
		len(before.IssuedCommands) != 2 ||
		len(before.GitHubFences) != 1 {
		t.Fatalf("restart read model = %#v", before)
	}
	node := before.NodeTopology[0]
	if node.NodeID != start.NodeID ||
		!node.PlatformObserved ||
		node.OS != domain.OSLinux ||
		node.Architecture != domain.ArchARM64 ||
		node.MaxRunners != domain.DefaultMaxRunners ||
		!node.LastNativeRunnerReady ||
		node.AdministrativeState != domain.NodeActive {
		t.Fatalf("restart node topology = %#v", node)
	}
	fence := before.GitHubFences[0]
	if fence.Claim.Execution.ID != start.Command.ExecutionID ||
		fence.Claim.State != GitHubClaimStartAmbiguous ||
		fence.Attempt == nil ||
		*fence.Attempt != attempt {
		t.Fatalf("restart GitHub fence = %#v", fence)
	}
	if before.IssuedCommands[1] != start {
		t.Fatalf("restart issued start = %#v, want %#v", before.IssuedCommands[1], start)
	}

	encoded, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), secretCanary) {
		t.Fatal("restart read model exposed JIT material")
	}

	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openControllerPath(t, path)
	defer reopened.Close()
	after, err := reopened.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("restart read model changed across reopen\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestControllerRestartSnapshotRejectsInconsistentGitHubFence(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-restart-invalid-fence.db")
	defer controller.Close()
	attempt, _ := prepareGitHubStartDispatchForTest(
		t,
		controller,
		4,
		92,
		902,
		9002,
	)
	if _, err := controller.db.ExecContext(ctx, `UPDATE github_job_claims
		SET state = ?
		WHERE scale_set_id = ? AND claim_key = ?`,
		GitHubClaimStartAmbiguous,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.RestartSnapshot(ctx); err == nil {
		t.Fatal("inconsistent claim/JIT fence was accepted as restart authority")
	}
}

func TestGitHubQueueMessageKeepsMixedEventsSeparateFromSingleSlotClaim(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-mixed.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 3)
	binding := SingleSlotBinding{TargetID: "target-github", NodeID: domain.NodeID(nodeID), Slot: 0, ClaimEnabled: true}
	enableGitHubClaimForTest(t, controller, &binding, 7, domain.ArchAMD64)
	message := githubQueueMessageForTest(7, 101, 501)
	message.Jobs = append(message.Jobs,
		GitHubJobEvent{Type: GitHubJobAssigned, RunnerRequestID: 502},
		// Live GitHub omits ClaimKey on JobAssigned. Rejecting that shape
		// made the store refuse every real message, so the queue redelivered a
		// genuinely runnable job forever instead of starting it.
		GitHubJobEvent{Type: GitHubJobAssigned, RunnerRequestID: 0},
		// A job canceled before any runner picked it up arrives with no runner
		// identity either. Rejecting it wedged the queue the same way.
		GitHubJobEvent{Type: GitHubJobCompleted, RunnerRequestID: 0, Result: "canceled"},
		GitHubJobEvent{Type: GitHubJobStarted, RunnerRequestID: 503, RunnerID: 33, RunnerName: "sparerunner-existing"},
		GitHubJobEvent{Type: GitHubJobCompleted, RunnerRequestID: 504, RunnerID: 34, RunnerName: "sparerunner-complete", Result: "succeeded"},
	)

	committed, err := controller.CommitGitHubQueueMessage(ctx, message, binding)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Replayed || committed.Claim == nil ||
		committed.Claim.ClaimKey != 501 ||
		committed.Claim.Execution.State != domain.ExecutionReserved {
		t.Fatalf("commit = %#v", committed)
	}
	dispatchReady, err := controller.GitHubPendingClaimDispatchReady(
		ctx,
		*committed.Claim,
	)
	if err != nil || dispatchReady {
		t.Fatalf(
			"unacknowledged pending dispatch authority = (%t, %v), want false",
			dispatchReady,
			err,
		)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_queue_messages", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_message_jobs", 6)
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)

	replayed, err := controller.CommitGitHubQueueMessage(ctx, message, binding)
	if err != nil || !replayed.Replayed || replayed.Claim == nil {
		t.Fatalf("replay = (%#v, %v)", replayed, err)
	}
	message.Digest = digestForTest("changed-message")
	if _, err := controller.CommitGitHubQueueMessage(ctx, message, binding); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed message replay error = %v, want ErrReplayMismatch", err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_message_jobs", 6)
}

func TestGitHubQueueCommitFailsClosedAfterManagementAuditDegrades(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-audit-degraded.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 3)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	enableGitHubClaimForTest(t, controller, &binding, 7, domain.ArchAMD64)
	auditChanged := controller.ManagementAuditChange()
	controller.degradeManagementAudit()
	select {
	case <-auditChanged:
	default:
		t.Fatal("management audit change was not signaled")
	}

	if _, err := controller.CommitGitHubQueueMessage(
		ctx,
		githubQueueMessageForTest(7, 102, 505),
		binding,
	); !errors.Is(err, ErrManagementAuditPersistence) {
		t.Fatalf("degraded audit queue commit error = %v", err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_queue_messages", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
}

func TestGitHubQueueCommitLinearizesWithManagementAuditDegradation(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-audit-linearized.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 3)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	enableGitHubClaimForTest(t, controller, &binding, 7, domain.ArchAMD64)

	commitEntered := make(chan struct{})
	releaseCommit := make(chan struct{})
	controller.beforeGitHubQueueCommit = func() {
		close(commitEntered)
		<-releaseCommit
	}
	commitResult := make(chan error, 1)
	go func() {
		_, err := controller.CommitGitHubQueueMessage(
			ctx,
			githubQueueMessageForTest(7, 103, 506),
			binding,
		)
		commitResult <- err
	}()
	select {
	case <-commitEntered:
	case <-time.After(time.Second):
		t.Fatal("queue commit did not reach the audit-gated commit point")
	}

	degradeStarted := make(chan struct{})
	degradeDone := make(chan struct{})
	go func() {
		close(degradeStarted)
		controller.degradeManagementAudit()
		close(degradeDone)
	}()
	<-degradeStarted
	select {
	case <-degradeDone:
		t.Fatal("audit degradation crossed an in-progress gated commit")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseCommit)
	if err := <-commitResult; err != nil {
		t.Fatalf("commit linearized before audit degradation = %v", err)
	}
	select {
	case <-degradeDone:
	case <-time.After(time.Second):
		t.Fatal("audit degradation did not complete after prior commit")
	}
	controller.beforeGitHubQueueCommit = nil
	if controller.ManagementAuditHealthy() {
		t.Fatal("audit authority remained healthy after degradation")
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_queue_messages", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)

	if _, err := controller.CommitGitHubQueueMessage(
		ctx,
		githubQueueMessageForTest(7, 104, 507),
		binding,
	); !errors.Is(err, ErrManagementAuditPersistence) {
		t.Fatalf("post-degradation queue commit error = %v", err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_queue_messages", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)
}

func TestGitHubSingleSlotRejectsInactiveNodeAndNeverDoubleClaims(t *testing.T) {
	ctx := context.Background()
	for _, state := range []domain.NodeAdministrativeState{
		domain.NodeDraining,
		domain.NodeQuarantined,
	} {
		t.Run(string(state), func(t *testing.T) {
			controller := openController(t, "controller-github-"+string(state)+".db")
			defer controller.Close()
			nodeID, _ := enrollControllerAgentNode(t, controller, 4)
			if _, err := controller.db.Exec(`UPDATE node_administrative_states
				SET administrative_state = ? WHERE node_id = ?`, state, nodeID); err != nil {
				t.Fatal(err)
			}
			binding := SingleSlotBinding{TargetID: "target-github", NodeID: domain.NodeID(nodeID), Slot: 0, ClaimEnabled: true}
			capacity, err := controller.GitHubSingleSlotCapacity(ctx, binding)
			if err != nil || capacity != 0 {
				t.Fatalf("capacity = (%d, %v), want 0", capacity, err)
			}
			commit, err := controller.CommitGitHubQueueMessage(
				ctx, githubQueueMessageForTest(7, 201, 601), binding)
			if err != nil {
				t.Fatal(err)
			}
			if commit.Claim != nil {
				t.Fatalf("inactive node claimed execution: %#v", commit.Claim)
			}
			assertCount(t, controller.db, "SELECT count(*) FROM github_queue_messages", 1)
			assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 0)
			assertCount(t, controller.db, "SELECT count(*) FROM executions", 0)
		})
	}

	controller := openController(t, "controller-github-capacity.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 5)
	binding := SingleSlotBinding{TargetID: "target-github", NodeID: domain.NodeID(nodeID), Slot: 0, ClaimEnabled: true}
	enableGitHubClaimForTest(t, controller, &binding, 7, domain.ArchAMD64)
	if _, err := controller.CommitGitHubQueueMessage(
		ctx, githubQueueMessageForTest(7, 202, 602), binding); err != nil {
		t.Fatal(err)
	}
	second, err := controller.CommitGitHubQueueMessage(
		ctx, githubQueueMessageForTest(7, 203, 603), binding)
	if err != nil {
		t.Fatal(err)
	}
	if second.Claim != nil {
		t.Fatalf("second claim = %#v, want nil", second.Claim)
	}
	capacity, err := controller.GitHubSingleSlotCapacity(ctx, binding)
	if err != nil || capacity != 0 {
		t.Fatalf("reserved capacity = (%d, %v), want 0", capacity, err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)

	duplicateAvailability := githubQueueMessageForTest(7, 204, 602)
	duplicate, err := controller.CommitGitHubQueueMessage(ctx, duplicateAvailability, binding)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Claim == nil || duplicate.Claim.ClaimKey != 602 ||
		duplicate.UnclaimedAvailable {
		t.Fatalf("duplicate availability = %#v", duplicate)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)
}

func TestGitHubWorkAdmissionRechecksActiveNodeAtEachExternalIntentBoundary(t *testing.T) {
	ctx := context.Background()

	t.Run("acquire", func(t *testing.T) {
		controller := openController(t, "controller-github-acquire-admission.db")
		defer controller.Close()
		nodeID, _ := enrollControllerAgentNode(t, controller, 1)
		binding := SingleSlotBinding{
			TargetID:     "target-github",
			NodeID:       domain.NodeID(nodeID),
			Slot:         0,
			ClaimEnabled: true,
		}
		enableGitHubClaimForTest(t, controller, &binding, 31, domain.ArchAMD64)
		if _, err := controller.CommitGitHubQueueMessage(
			ctx, githubQueueMessageForTest(31, 701, 1701), binding,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := controller.db.Exec(`UPDATE node_administrative_states
			SET administrative_state = ? WHERE node_id = ?`,
			domain.NodeQuarantined, nodeID); err != nil {
			t.Fatal(err)
		}
		if _, err := controller.BeginGitHubAcquire(ctx, 31, 1701); !errors.Is(err, ErrGitHubClaimState) {
			t.Fatalf("quarantined acquire admission error = %v", err)
		}
		claim, found, err := controller.GitHubClaim(ctx, 31, 1701)
		if err != nil || !found || claim.State != GitHubClaimPending {
			t.Fatalf("claim after rejected acquire = (%#v, %t, %v)", claim, found, err)
		}
	})

	t.Run("jit generation", func(t *testing.T) {
		controller := openController(t, "controller-github-jit-admission.db")
		defer controller.Close()
		nodeID, epoch := prepareGitHubClaimForJITTest(t, controller, 2, 32, 702, 1702)
		if _, err := controller.db.Exec(`UPDATE node_administrative_states
			SET administrative_state = ? WHERE node_id = ?`,
			domain.NodeDraining, nodeID); err != nil {
			t.Fatal(err)
		}
		if _, _, err := controller.BeginGitHubJITAttempt(
			ctx, 32, 1702, epoch, "sparerunner-jit-admission",
		); !errors.Is(err, ErrGitHubClaimState) {
			t.Fatalf("draining JIT admission error = %v", err)
		}
		assertCount(t, controller.db, "SELECT count(*) FROM github_jit_attempts WHERE scale_set_id=32", 0)
	})

	t.Run("start dispatch", func(t *testing.T) {
		controller := openController(t, "controller-github-start-admission.db")
		defer controller.Close()
		nodeID, epoch := prepareGitHubClaimForJITTest(t, controller, 3, 33, 703, 1703)
		attempt, replayed, err := controller.BeginGitHubJITAttempt(
			ctx, 33, 1703, epoch, "sparerunner-start-admission")
		if err != nil || replayed {
			t.Fatalf("begin JIT = (%#v, %t, %v)", attempt, replayed, err)
		}
		attempt.RunnerID = 81
		attempt.JITDigest = digestForTest("start-admission")
		attempt.StartCommandID = "start-admission"
		if err := controller.MarkGitHubJITGenerated(
			ctx, attempt, attempt.RunnerID, attempt.JITDigest, attempt.StartCommandID,
		); err != nil {
			t.Fatal(err)
		}
		attempt.State = GitHubJITGenerated
		if _, err := controller.db.Exec(`UPDATE node_administrative_states
			SET administrative_state = ? WHERE node_id = ?`,
			domain.NodeQuarantined, nodeID); err != nil {
			t.Fatal(err)
		}
		if err := controller.BeginGitHubStartDispatch(ctx, attempt); !errors.Is(err, ErrGitHubClaimState) {
			t.Fatalf("quarantined start admission error = %v", err)
		}
		claim, found, err := controller.GitHubClaim(ctx, attempt.ScaleSetID, attempt.ClaimKey)
		if err != nil || !found || claim.State != GitHubClaimJITGenerated {
			t.Fatalf("claim after rejected start = (%#v, %t, %v)", claim, found, err)
		}
		current, found, err := controller.CurrentGitHubJITAttempt(
			ctx, attempt.ScaleSetID, attempt.ClaimKey)
		if err != nil || !found || current.State != GitHubJITGenerated {
			t.Fatalf("attempt after rejected start = (%#v, %t, %v)", current, found, err)
		}
	})
}

func TestGitHubExactMessageReplayCreatesLateClaimWhenNodeBecomesEligible(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-late-claim.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 9)
	binding := SingleSlotBinding{
		TargetID: "target-github", NodeID: domain.NodeID(nodeID),
		Slot: 0, ClaimEnabled: false,
	}
	enableGitHubClaimForTest(t, controller, &binding, 8, domain.ArchAMD64)
	message := githubQueueMessageForTest(8, 205, 604)
	first, err := controller.CommitGitHubQueueMessage(ctx, message, binding)
	if err != nil {
		t.Fatal(err)
	}
	if first.Claim != nil || !first.UnclaimedAvailable || first.Replayed {
		t.Fatalf("ineligible first commit = %#v", first)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_queue_messages", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 0)

	binding.ClaimEnabled = true
	replayed, err := controller.CommitGitHubQueueMessage(ctx, message, binding)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.UnclaimedAvailable ||
		replayed.Claim == nil || replayed.Claim.ClaimKey != 604 {
		t.Fatalf("eligible exact replay = %#v", replayed)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_queue_messages", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)
}

func TestGitHubMessageWithMoreAvailabilityThanCapacityRemainsUnacknowledgeable(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-multi-available.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 2)
	binding := SingleSlotBinding{
		TargetID: "target-github", NodeID: domain.NodeID(nodeID),
		Slot: 0, ClaimEnabled: true,
	}
	message := githubQueueMessageForTest(8, 206, 605)
	message.Jobs = append(message.Jobs, GitHubJobEvent{
		Type: GitHubJobAvailable, RunnerRequestID: 606,
		ExecutionID: "github-execution-606",
	})
	commit, err := controller.CommitGitHubQueueMessage(ctx, message, binding)
	if err != nil {
		t.Fatal(err)
	}
	if commit.Claim != nil || !commit.UnclaimedAvailable {
		t.Fatalf("multi-available commit = %#v", commit)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM github_message_jobs", 2)
}

func TestGitHubClaimAndJITAttemptSurviveRestartWithoutSecretBody(t *testing.T) {
	ctx := context.Background()
	path := privateTestDir(t) + "/controller-github-restart.db"
	controller := openControllerPath(t, path)
	nodeID, epoch := enrollControllerAgentNode(t, controller, 6)
	binding := SingleSlotBinding{TargetID: "target-github", NodeID: domain.NodeID(nodeID), Slot: 0, ClaimEnabled: true}
	enableGitHubClaimForTest(t, controller, &binding, 9, domain.ArchAMD64)
	message := githubQueueMessageForTest(9, 301, 701)
	if _, err := controller.CommitGitHubQueueMessage(ctx, message, binding); err != nil {
		t.Fatal(err)
	}
	acquire, err := controller.BeginGitHubAcquire(ctx, 9, 701)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubAcquired(ctx, acquire); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.db.Exec(`UPDATE executions SET state = 'preparing'
		WHERE id = ?`, message.Jobs[0].ExecutionID); err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubPreparing(ctx, 9, 701); err != nil {
		t.Fatal(err)
	}
	attempt, replayed, err := controller.BeginGitHubJITAttempt(
		ctx, 9, 701, epoch, "sparerunner-deterministic")
	if err != nil || replayed {
		t.Fatalf("begin JIT = (%#v, %t, %v)", attempt, replayed, err)
	}
	const jitCanary = "jit-body-canary-must-not-enter-sqlite"
	jitDigest := digestForTest(jitCanary)
	if err := controller.MarkGitHubJITGenerated(
		ctx, attempt, 77, jitDigest, "start-command-701"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openControllerPath(t, path)
	defer reopened.Close()
	claim, found, err := reopened.GitHubClaim(ctx, 9, 701)
	if err != nil || !found || claim.State != GitHubClaimJITGenerated {
		t.Fatalf("reopened claim = (%#v, %t, %v)", claim, found, err)
	}
	current, found, err := reopened.CurrentGitHubJITAttempt(ctx, 9, 701)
	if err != nil || !found || current.RunnerID != 77 ||
		current.JITDigest != jitDigest || current.StartCommandID != "start-command-701" {
		t.Fatalf("reopened JIT attempt = (%#v, %t, %v)", current, found, err)
	}
	var leaked int
	if err := reopened.db.QueryRow(`SELECT count(*) FROM github_jit_attempts
		WHERE runner_name LIKE '%' || ? || '%'
			OR coalesce(jit_digest, '') LIKE '%' || ? || '%'
			OR start_command_id LIKE '%' || ? || '%'`,
		jitCanary, jitCanary, jitCanary).Scan(&leaked); err != nil {
		t.Fatal(err)
	}
	if leaked != 0 {
		t.Fatal("JIT body canary entered durable storage")
	}
}

func TestGitHubAcquireAndGenerationAmbiguityRemainNonActionable(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-ambiguous.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 7)
	binding := SingleSlotBinding{TargetID: "target-github", NodeID: domain.NodeID(nodeID), Slot: 0, ClaimEnabled: true}
	enableGitHubClaimForTest(t, controller, &binding, 11, domain.ArchAMD64)
	message := githubQueueMessageForTest(11, 401, 801)
	if _, err := controller.CommitGitHubQueueMessage(ctx, message, binding); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.BeginGitHubAcquire(ctx, 11, 801); err != nil {
		t.Fatal(err)
	}
	if _, found, err := controller.NextActionableGitHubClaim(ctx, 11); err != nil || found {
		t.Fatalf("ambiguous acquire actionable = (%t, %v)", found, err)
	}
	claim, found, err := controller.GitHubClaim(ctx, 11, 801)
	if err != nil || !found || claim.State != GitHubClaimAcquireAmbiguous {
		t.Fatalf("ambiguous claim = (%#v, %t, %v)", claim, found, err)
	}

	// A separate claim demonstrates that a generation intent is persisted before
	// the API call and never becomes actionable after an uncertain result.
	controller2 := openController(t, "controller-github-jit-ambiguous.db")
	defer controller2.Close()
	nodeID2, epoch2 := enrollControllerAgentNode(t, controller2, 8)
	binding2 := SingleSlotBinding{TargetID: "target-github", NodeID: domain.NodeID(nodeID2), Slot: 0, ClaimEnabled: true}
	enableGitHubClaimForTest(t, controller2, &binding2, 12, domain.ArchAMD64)
	message2 := githubQueueMessageForTest(12, 402, 802)
	if _, err := controller2.CommitGitHubQueueMessage(ctx, message2, binding2); err != nil {
		t.Fatal(err)
	}
	acquire, err := controller2.BeginGitHubAcquire(ctx, 12, 802)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller2.MarkGitHubAcquired(ctx, acquire); err != nil {
		t.Fatal(err)
	}
	if _, err := controller2.db.Exec(`UPDATE executions SET state = 'preparing'
		WHERE id = ?`, message2.Jobs[0].ExecutionID); err != nil {
		t.Fatal(err)
	}
	if err := controller2.MarkGitHubPreparing(ctx, 12, 802); err != nil {
		t.Fatal(err)
	}
	attempt, _, err := controller2.BeginGitHubJITAttempt(
		ctx, 12, 802, epoch2, "sparerunner-ambiguous")
	if err != nil {
		t.Fatal(err)
	}
	if err := controller2.MarkGitHubJITGenerationAmbiguous(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	if _, found, err := controller2.NextActionableGitHubClaim(ctx, 12); err != nil || found {
		t.Fatalf("ambiguous JIT actionable = (%t, %v)", found, err)
	}
	replayed, replay, err := controller2.BeginGitHubJITAttempt(
		ctx, 12, 802, epoch2, "sparerunner-ambiguous")
	if err != nil || !replay || replayed.State != GitHubJITGenerationAmbiguous {
		t.Fatalf("ambiguous JIT replay = (%#v, %t, %v)", replayed, replay, err)
	}
	if _, err := controller2.MarkGitHubJITReconciledAbsent(
		ctx, attempt, epoch2, "", 1); !errors.Is(err, ErrStaleControllerEpoch) {
		t.Fatalf("same-epoch JIT reconciliation = %v", err)
	}
	reconciliationEpoch, err := controller2.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	absenceSnapshot := NodeAgentSnapshot{
		NodeID:       domain.NodeID(nodeID2),
		OS:           domain.OSLinux,
		Architecture: domain.ArchAMD64,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch2,
		},
	}
	if err := controller2.RecordAgentSnapshot(ctx, absenceSnapshot); err != nil {
		t.Fatal(err)
	}
	absenceDigest, err := nodeAgentSnapshotDigest(absenceSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller2.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		absenceDigest,
		1,
	); !errors.Is(err, ErrGitHubJITAbsencePending) {
		t.Fatalf("first generation absence = %v", err)
	}
	current, found, err := controller2.CurrentGitHubJITAttempt(
		ctx,
		12,
		802,
	)
	if err != nil || !found || current.State != GitHubJITGenerationAmbiguous {
		t.Fatalf("single generation absence changed attempt = (%#v, %t, %v)", current, found, err)
	}
	controller2.now = func() time.Time {
		return time.Unix(100, 0).Add(GitHubRunnerAbsenceConfirmationDelay)
	}
	if _, err := controller2.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		absenceDigest,
		1,
	); err != nil {
		t.Fatal(err)
	}
	current, found, err = controller2.CurrentGitHubJITAttempt(ctx, 12, 802)
	if err != nil || !found || current.State != GitHubJITReconciledAbsent {
		t.Fatalf("second generation absence = (%#v, %t, %v)", current, found, err)
	}
}

func TestGitHubAcquireRearmRequiresFreshCommittedAvailabilityAndExactAttempt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "controller-github-acquire-attempt.db")
	controller := openControllerPath(t, path)
	nodeID, _ := enrollControllerAgentNode(t, controller, 1)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	enableGitHubClaimForTest(t, controller, &binding, 41, domain.ArchAMD64)
	const (
		scaleSetID    ScaleSetID = 41
		runnerRequest            = int64(1801)
	)
	original := githubQueueMessageForTest(scaleSetID, 801, runnerRequest)
	if _, err := controller.CommitGitHubQueueMessage(ctx, original, binding); err != nil {
		t.Fatal(err)
	}
	first, err := controller.BeginGitHubAcquire(ctx, scaleSetID, runnerRequest)
	if err != nil {
		t.Fatal(err)
	}
	if first.Attempt != 1 ||
		first.EvidenceMessage != original.MessageID ||
		first.ControllerEpoch == 0 {
		t.Fatalf("first acquire token = %#v", first)
	}

	// Neither an unrelated event nor exact replay of the original availability
	// can re-arm an ambiguous external write.
	assigned := GitHubQueueMessage{
		ScaleSetID: scaleSetID,
		MessageID:  802,
		Digest:     digestForTest("assigned-evidence-does-not-rearm"),
		Jobs: []GitHubJobEvent{{
			Type:            GitHubJobAssigned,
			RunnerRequestID: runnerRequest,
		}},
	}
	if _, err := controller.CommitGitHubQueueMessage(ctx, assigned, binding); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.CommitGitHubQueueMessage(ctx, original, binding); err != nil {
		t.Fatal(err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 1)
	claim, found, err := controller.GitHubClaim(ctx, scaleSetID, runnerRequest)
	if err != nil || !found || claim.State != GitHubClaimAcquireAmbiguous {
		t.Fatalf("claim after non-authoritative messages = (%#v, %t, %v)", claim, found, err)
	}

	fresh := githubQueueMessageForTest(scaleSetID, 803, runnerRequest)
	commit, err := controller.CommitGitHubQueueMessage(ctx, fresh, binding)
	if err != nil {
		t.Fatal(err)
	}
	if commit.Replayed || commit.Claim == nil ||
		commit.Claim.State != GitHubClaimPending {
		t.Fatalf("fresh rearm commit = %#v", commit)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 2)

	// A second fresh message observed while the durable retry is already pending
	// does not create another authority cycle.
	extra := githubQueueMessageForTest(scaleSetID, 804, runnerRequest)
	if _, err := controller.CommitGitHubQueueMessage(ctx, extra, binding); err != nil {
		t.Fatal(err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 2)
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openControllerPath(t, path)
	defer reopened.Close()
	restartEpoch, err := reopened.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type beginResult struct {
		attempt GitHubAcquireAttempt
		err     error
	}
	results := make(chan beginResult, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			attempt, err := reopened.BeginGitHubAcquire(
				ctx,
				scaleSetID,
				runnerRequest,
			)
			results <- beginResult{attempt: attempt, err: err}
		}()
	}
	wait.Wait()
	close(results)
	var retry GitHubAcquireAttempt
	successes := 0
	conflicts := 0
	for result := range results {
		switch {
		case result.err == nil:
			retry = result.attempt
			successes++
		case errors.Is(result.err, ErrGitHubClaimState):
			conflicts++
		default:
			t.Fatalf("parallel BeginGitHubAcquire = %v", result.err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("parallel Begin results = successes %d, conflicts %d", successes, conflicts)
	}
	if retry.Attempt != 2 ||
		retry.EvidenceMessage != fresh.MessageID ||
		retry.ControllerEpoch != restartEpoch {
		t.Fatalf("durable restart acquire token = %#v", retry)
	}

	if err := reopened.MarkGitHubAcquired(ctx, first); err == nil {
		t.Fatal("late prior-epoch acquire result changed the current attempt")
	}
	changed := retry
	changed.EvidenceMessage = extra.MessageID
	if err := reopened.MarkGitHubAcquired(ctx, changed); err == nil {
		t.Fatal("changed acquire evidence token was accepted")
	}
	claim, found, err = reopened.GitHubClaim(ctx, scaleSetID, runnerRequest)
	if err != nil || !found || claim.State != GitHubClaimAcquireAmbiguous {
		t.Fatalf("claim changed after rejected token = (%#v, %t, %v)", claim, found, err)
	}
	if err := reopened.MarkGitHubAcquired(ctx, retry); err != nil {
		t.Fatal(err)
	}
	if err := reopened.MarkGitHubAcquired(ctx, retry); err != nil {
		t.Fatalf("exact acquire completion replay = %v", err)
	}
	claim, found, err = reopened.GitHubClaim(ctx, scaleSetID, runnerRequest)
	if err != nil || !found || claim.State != GitHubClaimAcquired {
		t.Fatalf("acquired claim = (%#v, %t, %v)", claim, found, err)
	}
	assertCount(
		t,
		reopened.db,
		"SELECT count(*) FROM github_acquire_attempts WHERE state='acquired'",
		1,
	)
}

func TestGitHubStartedTransitionMarksRunningAtomically(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-started-running.db")
	defer controller.Close()

	attempt, start := prepareGitHubStartDispatchForTest(t, controller, 1, 21, 501, 901)
	running := AgentExecutionUpdate{
		NodeID:        start.NodeID,
		MessageID:     "github-start-running-901",
		CommandID:     start.Command.ID,
		ExecutionID:   start.Command.ExecutionID,
		State:         domain.ExecutionRunning,
		PayloadDigest: digestForTest("github-start-running-901"),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(ctx, running); err != nil || replayed {
		t.Fatalf("record running = (%t, %v)", replayed, err)
	}
	if err := controller.MarkGitHubRunning(ctx, attempt); err != nil {
		t.Fatal(err)
	}

	claim, found, err := controller.GitHubClaim(
		ctx, attempt.ScaleSetID, attempt.ClaimKey)
	if err != nil || !found || claim.State != GitHubClaimRunning ||
		claim.Execution.State != domain.ExecutionRunning {
		t.Fatalf("running claim = (%#v, %t, %v)", claim, found, err)
	}
	current, found, err := controller.CurrentGitHubJITAttempt(
		ctx, attempt.ScaleSetID, attempt.ClaimKey)
	if err != nil || !found || current.State != GitHubJITStarted {
		t.Fatalf("started attempt = (%#v, %t, %v)", current, found, err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)
}

func TestGitHubStartedTransitionRejectsMissingRunningObservationWithoutPartialWrite(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-started-precondition.db")
	defer controller.Close()

	attempt, _ := prepareGitHubStartDispatchForTest(t, controller, 4, 23, 504, 904)
	if err := controller.MarkGitHubRunning(ctx, attempt); !errors.Is(err, ErrGitHubJITStartNotProven) {
		t.Fatalf(
			"missing Running observation error = %v, want ErrGitHubJITStartNotProven",
			err,
		)
	}

	claim, found, err := controller.GitHubClaim(
		ctx, attempt.ScaleSetID, attempt.ClaimKey)
	if err != nil || !found ||
		claim.State != GitHubClaimStartDispatching ||
		claim.Execution.State != domain.ExecutionPreparing {
		t.Fatalf("unchanged claim = (%#v, %t, %v)", claim, found, err)
	}
	current, found, err := controller.CurrentGitHubJITAttempt(
		ctx, attempt.ScaleSetID, attempt.ClaimKey)
	if err != nil || !found || current.State != GitHubJITStartDispatching {
		t.Fatalf("unchanged attempt = (%#v, %t, %v)", current, found, err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)
}

func TestGitHubAmbiguousStartConvergesOnlyFromExactDurableAgentAuthority(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-reconcile-accepted.db")
	defer controller.Close()

	attempt, start := prepareGitHubStartDispatchForTest(
		t, controller, 5, 24, 505, 905)
	if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	observation := ObservationSnapshot{
		ExecutionID:        start.Command.ExecutionID,
		State:              domain.ExecutionRunning,
		ObservedAtUnixNano: 10,
	}
	snapshot := NodeAgentSnapshot{
		NodeID:            start.NodeID,
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
			Commands:           []domain.Command{start.Command},
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
	if err := controller.MarkGitHubJITObservedStarted(
		ctx, attempt, reconciliationEpoch, observation,
		snapshotDigest); err != nil {
		t.Fatal(err)
	}
	claim, found, err := controller.GitHubClaim(
		ctx, attempt.ScaleSetID, attempt.ClaimKey)
	if err != nil || !found ||
		claim.State != GitHubClaimRunning ||
		claim.Execution.State != domain.ExecutionRunning {
		t.Fatalf("reconciled claim = (%#v, %t, %v)", claim, found, err)
	}
	current, found, err := controller.CurrentGitHubJITAttempt(
		ctx, attempt.ScaleSetID, attempt.ClaimKey)
	if err != nil || !found || current.State != GitHubJITStarted {
		t.Fatalf("reconciled attempt = (%#v, %t, %v)", current, found, err)
	}
}

func TestGitHubAmbiguousStartRejectsSupersededSnapshotWithoutPartialWrite(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-reconcile-unadopted.db")
	defer controller.Close()

	attempt, start := prepareGitHubStartDispatchForTest(
		t, controller, 6, 25, 506, 906)
	if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	observation := ObservationSnapshot{
		ExecutionID:        start.Command.ExecutionID,
		State:              domain.ExecutionRunning,
		ObservedAtUnixNano: 11,
	}
	snapshot := NodeAgentSnapshot{
		NodeID:            start.NodeID,
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
			Commands:           []domain.Command{start.Command},
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
	superseding := snapshot
	superseding.NativeRunnerReady = false
	superseding.Journal.Commands = nil
	superseding.Journal.Observations = nil
	if err := controller.RecordAgentSnapshot(ctx, superseding); err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubJITObservedStarted(
		ctx, attempt, reconciliationEpoch, observation,
		snapshotDigest); err == nil {
		t.Fatal("superseded Agent snapshot authorized JIT start")
	}
	claim, found, err := controller.GitHubClaim(
		ctx, attempt.ScaleSetID, attempt.ClaimKey)
	if err != nil || !found ||
		claim.State != GitHubClaimStartAmbiguous ||
		claim.Execution.State != domain.ExecutionPreparing {
		t.Fatalf("claim changed after rejection = (%#v, %t, %v)", claim, found, err)
	}
	current, found, err := controller.CurrentGitHubJITAttempt(
		ctx, attempt.ScaleSetID, attempt.ClaimKey)
	if err != nil || !found || current.State != GitHubJITStartAmbiguous {
		t.Fatalf("attempt changed after rejection = (%#v, %t, %v)", current, found, err)
	}
}

func TestGitHubAgentAcceptedRequiresCurrentSnapshotAndValidDispatchOrder(t *testing.T) {
	ctx := context.Background()

	t.Run("exact current Start acceptance", func(t *testing.T) {
		controller := openController(t, "controller-github-agent-accepted.db")
		defer controller.Close()
		attempt, start := prepareGitHubStartDispatchForTest(
			t,
			controller,
			1,
			26,
			507,
			907,
		)
		if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
			t.Fatal(err)
		}
		attempt.State = GitHubJITStartAmbiguous
		reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		snapshot := NodeAgentSnapshot{
			NodeID:            start.NodeID,
			OS:                domain.OSLinux,
			Architecture:      domain.ArchAMD64,
			NativeRunnerReady: true,
			Journal: AgentSnapshot{
				MaxControllerEpoch: start.Command.ControllerEpoch,
				Commands:           []domain.Command{start.Command},
			},
		}
		if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
		digest, err := nodeAgentSnapshotDigest(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := controller.MarkGitHubJITAgentAccepted(
			ctx,
			attempt,
			reconciliationEpoch,
			digest,
		); err != nil {
			t.Fatal(err)
		}
		current, found, err := controller.CurrentGitHubJITAttempt(
			ctx,
			attempt.ScaleSetID,
			attempt.ClaimKey,
		)
		if err != nil || !found || current.State != GitHubJITAgentAccepted {
			t.Fatalf("Agent-accepted attempt = (%#v, %t, %v)", current, found, err)
		}
		assertCount(
			t,
			controller.db,
			"SELECT count(*) FROM github_jit_snapshot_authority WHERE decision='agent_accepted'",
			1,
		)
	})

	t.Run("superseded command membership", func(t *testing.T) {
		controller := openController(t, "controller-github-agent-accepted-superseded.db")
		defer controller.Close()
		attempt, start := prepareGitHubStartDispatchForTest(
			t,
			controller,
			2,
			27,
			508,
			908,
		)
		if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
			t.Fatal(err)
		}
		attempt.State = GitHubJITStartAmbiguous
		reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		accepted := NodeAgentSnapshot{
			NodeID:       start.NodeID,
			OS:           domain.OSLinux,
			Architecture: domain.ArchAMD64,
			Journal: AgentSnapshot{
				MaxControllerEpoch: start.Command.ControllerEpoch,
				Commands:           []domain.Command{start.Command},
			},
		}
		if err := controller.RecordAgentSnapshot(ctx, accepted); err != nil {
			t.Fatal(err)
		}
		acceptedDigest, err := nodeAgentSnapshotDigest(accepted)
		if err != nil {
			t.Fatal(err)
		}
		current := accepted
		current.NativeRunnerReady = true
		current.Journal.Commands = nil
		if err := controller.RecordAgentSnapshot(ctx, current); err != nil {
			t.Fatal(err)
		}
		currentDigest, err := nodeAgentSnapshotDigest(current)
		if err != nil {
			t.Fatal(err)
		}
		for _, digest := range []string{acceptedDigest, currentDigest} {
			if err := controller.MarkGitHubJITAgentAccepted(
				ctx,
				attempt,
				reconciliationEpoch,
				digest,
			); err == nil {
				t.Fatalf("non-current Start acceptance authorized with digest %s", digest)
			}
		}
		assertCount(t, controller.db, "SELECT count(*) FROM github_jit_snapshot_authority", 0)
	})

	t.Run("generated state cannot report accepted Start", func(t *testing.T) {
		controller := openController(t, "controller-github-agent-accepted-generated.db")
		defer controller.Close()
		attempt, start := prepareGitHubStartDispatchForTest(
			t,
			controller,
			3,
			28,
			509,
			909,
		)
		if _, err := controller.db.ExecContext(ctx, `UPDATE github_jit_attempts
			SET state = ? WHERE scale_set_id = ? AND claim_key = ?
				AND attempt = ?`,
			GitHubJITGenerated,
			attempt.ScaleSetID,
			attempt.ClaimKey,
			attempt.Attempt,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := controller.db.ExecContext(ctx, `UPDATE github_job_claims
			SET state = ? WHERE scale_set_id = ? AND claim_key = ?`,
			GitHubClaimJITGenerated,
			attempt.ScaleSetID,
			attempt.ClaimKey,
		); err != nil {
			t.Fatal(err)
		}
		attempt.State = GitHubJITGenerated
		reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		snapshot := NodeAgentSnapshot{
			NodeID:       start.NodeID,
			OS:           domain.OSLinux,
			Architecture: domain.ArchAMD64,
			Journal: AgentSnapshot{
				MaxControllerEpoch: start.Command.ControllerEpoch,
				Commands:           []domain.Command{start.Command},
			},
		}
		if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
		digest, err := nodeAgentSnapshotDigest(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := controller.MarkGitHubJITAgentAccepted(
			ctx,
			attempt,
			reconciliationEpoch,
			digest,
		); !errors.Is(err, ErrGitHubJITState) {
			t.Fatalf("generated state accepted Start = %v", err)
		}
	})
}

func TestGitHubRunnerRemovalAndAbsenceUseExactIdentityAndCurrentAgentAuthority(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-removal-authority.db")
	defer controller.Close()
	attempt, start := prepareGitHubStartDispatchForTest(
		t,
		controller,
		4,
		29,
		510,
		910,
	)
	if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.State = GitHubJITStartAmbiguous
	reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	absent := NodeAgentSnapshot{
		NodeID:       start.NodeID,
		OS:           domain.OSLinux,
		Architecture: domain.ArchAMD64,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, absent); err != nil {
		t.Fatal(err)
	}
	absenceDigest, err := nodeAgentSnapshotDigest(absent)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubJITRemovalPending(
		ctx,
		attempt,
		reconciliationEpoch,
		absenceDigest,
		1,
		false,
	); err != nil {
		t.Fatal(err)
	}
	attempt.State = GitHubJITRemovalPending
	if _, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		absenceDigest,
		1,
	); !errors.Is(err, ErrGitHubJITAbsencePending) {
		t.Fatalf("single runner absence = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*GitHubJITAttempt)
	}{
		{
			name: "runner ID",
			mutate: func(value *GitHubJITAttempt) {
				value.RunnerID++
			},
		},
		{
			name: "JIT digest",
			mutate: func(value *GitHubJITAttempt) {
				value.JITDigest = digestForTest("different-removal-jit")
			},
		},
		{
			name: "Start command",
			mutate: func(value *GitHubJITAttempt) {
				value.StartCommandID = "different-removal-start"
			},
		},
	}
	for _, testCase := range tests {
		changed := attempt
		testCase.mutate(&changed)
		if _, err := controller.MarkGitHubJITReconciledAbsent(
			ctx,
			changed,
			reconciliationEpoch,
			absenceDigest,
			1,
		); !errors.Is(err, ErrGitHubJITState) {
			t.Fatalf("changed %s absence = %v", testCase.name, err)
		}
	}

	accepted := absent
	accepted.NativeRunnerReady = true
	accepted.Journal.Commands = []domain.Command{start.Command}
	if err := controller.RecordAgentSnapshot(ctx, accepted); err != nil {
		t.Fatal(err)
	}
	acceptedDigest, err := nodeAgentSnapshotDigest(accepted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		acceptedDigest,
		1,
	); err == nil {
		t.Fatal("accepted Start appearing after removal authorized absence")
	}
	current, found, err := controller.CurrentGitHubJITAttempt(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found || current.State != GitHubJITRemovalPending {
		t.Fatalf("removal fence changed after contradiction = (%#v, %t, %v)", current, found, err)
	}

	// A newer full snapshot may prove the command absent again. Historical
	// accepted-command rows remain audit evidence but cannot block or authorize
	// the decision by themselves.
	if err := controller.RecordAgentSnapshot(ctx, absent); err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time {
		return time.Unix(100, 0).Add(GitHubRunnerAbsenceConfirmationDelay)
	}
	if _, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		absenceDigest,
		1,
	); err != nil {
		t.Fatal(err)
	}
	current, found, err = controller.CurrentGitHubJITAttempt(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found ||
		current.State != GitHubJITReconciledAbsent ||
		current.RunnerID != 0 ||
		current.JITDigest != "" ||
		current.StartCommandID != "" {
		t.Fatalf("reconciled absent attempt = (%#v, %t, %v)", current, found, err)
	}
	claim, found, err := controller.GitHubClaim(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found || claim.State != GitHubClaimPreparing {
		t.Fatalf("claim after exact absence = (%#v, %t, %v)", claim, found, err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_jit_snapshot_authority", 0)
}

func TestGitHubRunnerAbsenceRestartsAfterSessionAuthorityTransition(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-removal-session-authority.db")
	defer controller.Close()
	attempt, start := prepareGitHubStartDispatchForTest(
		t,
		controller,
		4,
		39,
		610,
		1010,
	)
	if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.State = GitHubJITStartAmbiguous
	reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	absent := NodeAgentSnapshot{
		NodeID:       start.NodeID,
		OS:           domain.OSLinux,
		Architecture: domain.ArchAMD64,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, absent); err != nil {
		t.Fatal(err)
	}
	snapshotDigest, err := nodeAgentSnapshotDigest(absent)
	if err != nil {
		t.Fatal(err)
	}
	session, err := controller.ReadGitHubScaleSetSessionHealth(
		ctx,
		attempt.ScaleSetID,
	)
	if err != nil || session.TransitionGeneration != 1 {
		t.Fatalf("initial session authority = (%#v, %v)", session, err)
	}
	if err := controller.MarkGitHubJITRemovalPending(
		ctx,
		attempt,
		reconciliationEpoch,
		snapshotDigest,
		session.TransitionGeneration,
		false,
	); err != nil {
		t.Fatal(err)
	}
	attempt.State = GitHubJITRemovalPending
	if _, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		snapshotDigest,
		session.TransitionGeneration,
	); !errors.Is(err, ErrGitHubJITAbsencePending) {
		t.Fatalf("first provider absence = %v", err)
	}

	transitionAt := time.Unix(100, 0).Add(
		GitHubRunnerAbsenceConfirmationDelay,
	)
	controller.now = func() time.Time { return transitionAt }
	session, err = controller.RecordGitHubScaleSetSessionFailure(
		ctx,
		attempt.ScaleSetID,
		GitHubObservationProvider5xx,
	)
	if err != nil || session.TransitionGeneration != 2 {
		t.Fatalf("transitioned session authority = (%#v, %v)", session, err)
	}
	if _, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		snapshotDigest,
		1,
	); !errors.Is(err, ErrGitHubJITAbsencePending) {
		t.Fatalf("stale pre-query session authority = %v", err)
	}
	var retainedGeneration uint64
	if err := controller.db.QueryRow(`SELECT github_session_generation
		FROM github_jit_snapshot_authority
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		attempt.ScaleSetID,
		attempt.ClaimKey,
		attempt.Attempt,
	).Scan(&retainedGeneration); err != nil {
		t.Fatal(err)
	}
	if retainedGeneration != 1 {
		t.Fatalf("stale authority changed first absence generation to %d", retainedGeneration)
	}
	if _, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		snapshotDigest,
		session.TransitionGeneration,
	); !errors.Is(err, ErrGitHubJITAbsencePending) {
		t.Fatalf("first absence after session transition = %v", err)
	}
	var resetGeneration uint64
	var resetAt int64
	if err := controller.db.QueryRow(`SELECT github_session_generation,
		updated_at_unix_nano
		FROM github_jit_snapshot_authority
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		attempt.ScaleSetID,
		attempt.ClaimKey,
		attempt.Attempt,
	).Scan(&resetGeneration, &resetAt); err != nil {
		t.Fatal(err)
	}
	if resetGeneration != session.TransitionGeneration ||
		resetAt != transitionAt.UnixNano() {
		t.Fatalf("reset absence authority = (generation=%d, at=%d)",
			resetGeneration, resetAt)
	}
	controller.now = func() time.Time {
		return transitionAt.Add(GitHubRunnerAbsenceConfirmationDelay)
	}
	if _, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		snapshotDigest,
		session.TransitionGeneration,
	); err != nil {
		t.Fatal(err)
	}
}

func TestGitHubJITTransitionRejectsChangedDurableIdentityWithoutPartialWrite(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*GitHubJITAttempt)
	}{
		{
			name: "runner ID",
			mutate: func(attempt *GitHubJITAttempt) {
				attempt.RunnerID++
			},
		},
		{
			name: "JIT digest",
			mutate: func(attempt *GitHubJITAttempt) {
				attempt.JITDigest = digestForTest("different-jit")
			},
		},
		{
			name: "start command ID",
			mutate: func(attempt *GitHubJITAttempt) {
				attempt.StartCommandID = "different-start-command"
			},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			controller := openController(t, "controller-github-jit-identity.db")
			defer controller.Close()

			attempt, _ := prepareGitHubStartDispatchForTest(
				t, controller, index+1, 41, MessageID(801+index), int64(1201+index))
			original := attempt
			test.mutate(&attempt)

			if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); !errors.Is(err, ErrGitHubJITState) {
				t.Fatalf("changed identity transition error = %v, want ErrGitHubJITState", err)
			}
			current, found, err := controller.CurrentGitHubJITAttempt(
				ctx, original.ScaleSetID, original.ClaimKey)
			if err != nil || !found || current != original {
				t.Fatalf("attempt changed after rejected transition = (%#v, %t, %v), want %#v",
					current, found, err, original)
			}
			claim, found, err := controller.GitHubClaim(
				ctx, original.ScaleSetID, original.ClaimKey)
			if err != nil || !found || claim.State != GitHubClaimStartDispatching {
				t.Fatalf("claim changed after rejected transition = (%#v, %t, %v)", claim, found, err)
			}
		})
	}
}

func TestGitHubJITTransitionRejectsEpochAndClaimStateSubstitution(t *testing.T) {
	t.Run("later epoch cannot adopt an older attempt", func(t *testing.T) {
		ctx := context.Background()
		controller := openController(t, "controller-github-jit-epoch-substitution.db")
		defer controller.Close()
		attempt, _ := prepareGitHubStartDispatchForTest(t, controller, 1, 42, 811, 1211)
		laterEpoch, err := controller.AdvanceEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		attempt.ControllerEpoch = laterEpoch
		if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); !errors.Is(err, ErrGitHubJITState) {
			t.Fatalf("later epoch substitution error = %v, want ErrGitHubJITState", err)
		}
		current, found, err := controller.CurrentGitHubJITAttempt(
			ctx, attempt.ScaleSetID, attempt.ClaimKey)
		if err != nil || !found || current.State != GitHubJITStartDispatching {
			t.Fatalf("attempt after epoch substitution = (%#v, %t, %v)", current, found, err)
		}
	})

	t.Run("claim state cannot be overwritten by an attempt transition", func(t *testing.T) {
		ctx := context.Background()
		controller := openController(t, "controller-github-jit-claim-substitution.db")
		defer controller.Close()
		attempt, _ := prepareGitHubStartDispatchForTest(t, controller, 2, 43, 812, 1212)
		if _, err := controller.db.Exec(`UPDATE github_job_claims SET state = ?
			WHERE scale_set_id = ? AND claim_key = ?`,
			GitHubClaimReconciliationRequired, attempt.ScaleSetID, attempt.ClaimKey); err != nil {
			t.Fatal(err)
		}
		if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); !errors.Is(err, ErrGitHubClaimState) {
			t.Fatalf("claim state substitution error = %v, want ErrGitHubClaimState", err)
		}
		current, found, err := controller.CurrentGitHubJITAttempt(
			ctx, attempt.ScaleSetID, attempt.ClaimKey)
		if err != nil || !found || current.State != GitHubJITStartDispatching {
			t.Fatalf("attempt after claim substitution = (%#v, %t, %v)", current, found, err)
		}
	})
}

func TestGitHubLostJITReleasedWaitsForExactAbsenceAndFreshAvailability(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-lost-jit-rearm.db")
	defer controller.Close()

	const (
		scaleSetID      = ScaleSetID(44)
		sourceMessageID = MessageID(814)
		freshMessageID  = MessageID(815)
		claimKey        = int64(1214)
	)
	attempt, start := prepareGitHubStartDispatchForTest(
		t,
		controller,
		4,
		scaleSetID,
		sourceMessageID,
		claimKey,
	)
	if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.State = GitHubJITStartAmbiguous
	reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	acceptedSnapshot := NodeAgentSnapshot{
		NodeID:            start.NodeID,
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		RunnerVersion:     runner.OfficialRunnerVersion,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
			Commands:           []domain.Command{start.Command},
			Observations: []ObservationSnapshot{{
				ExecutionID:        start.Command.ExecutionID,
				State:              domain.ExecutionPreparing,
				ObservedAtUnixNano: 20,
			}},
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, acceptedSnapshot); err != nil {
		t.Fatal(err)
	}
	acceptedDigest, err := nodeAgentSnapshotDigest(acceptedSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubJITAgentAccepted(
		ctx,
		attempt,
		reconciliationEpoch,
		acceptedDigest,
	); err != nil {
		t.Fatal(err)
	}
	attempt.State = GitHubJITAgentAccepted
	releasedObservation := ObservationSnapshot{
		ExecutionID:        start.Command.ExecutionID,
		State:              domain.ExecutionReleased,
		ObservedAtUnixNano: 21,
	}
	releasedSnapshot := NodeAgentSnapshot{
		NodeID:            start.NodeID,
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		RunnerVersion:     runner.OfficialRunnerVersion,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
			Commands:           []domain.Command{start.Command},
			Observations:       []ObservationSnapshot{releasedObservation},
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, releasedSnapshot); err != nil {
		t.Fatal(err)
	}
	snapshotDigest, err := nodeAgentSnapshotDigest(releasedSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubJITObservedStarted(
		ctx,
		attempt,
		reconciliationEpoch,
		releasedObservation,
		snapshotDigest,
	); !errors.Is(err, ErrGitHubJITTerminalPending) {
		t.Fatalf("snapshot-before-outbox Released proof = %v", err)
	}
	assertCount(
		t,
		controller.db,
		`SELECT count(*) FROM agent_execution_updates
			WHERE state IN ('running', 'cleaning')`,
		0,
	)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)
	if replayed, err := controller.RecordAgentExecutionUpdate(
		ctx,
		AgentExecutionUpdate{
			NodeID:        start.NodeID,
			MessageID:     "github-lost-jit-released",
			CommandID:     start.Command.ID,
			ExecutionID:   start.Command.ExecutionID,
			State:         domain.ExecutionReleased,
			PayloadDigest: digestForTest("github-lost-jit-released"),
		},
	); err != nil || replayed {
		t.Fatalf("durable terminal Start update = (%t, %v)", replayed, err)
	}
	if err := controller.MarkGitHubJITObservedStarted(
		ctx,
		attempt,
		reconciliationEpoch,
		releasedObservation,
		snapshotDigest,
	); !errors.Is(err, ErrGitHubJITStartNotProven) {
		t.Fatalf("terminal-only Released start proof = %v", err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)

	prunedSnapshot := NodeAgentSnapshot{
		NodeID:            start.NodeID,
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		RunnerVersion:     runner.OfficialRunnerVersion,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, prunedSnapshot); err != nil {
		t.Fatal(err)
	}
	prunedDigest, err := nodeAgentSnapshotDigest(prunedSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	prunedHistory, err := controller.ReconcileGitHubJITPrunedHistory(
		ctx,
		attempt,
		reconciliationEpoch,
		prunedDigest,
	)
	if err != nil ||
		prunedHistory.Started ||
		prunedHistory.LostTerminal != domain.ExecutionReleased {
		t.Fatalf("pruned terminal-only Start history = (%#v, %v)",
			prunedHistory, err)
	}
	if err := controller.MarkGitHubJITRemovalPending(
		ctx,
		attempt,
		reconciliationEpoch,
		prunedDigest,
		1,
		false,
	); err != nil {
		t.Fatal(err)
	}
	attempt.State = GitHubJITRemovalPending
	if _, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		prunedDigest,
		1,
	); !errors.Is(err, ErrGitHubJITAbsencePending) {
		t.Fatalf("first exact absence = %v", err)
	}
	rotatedPrunedSnapshot := prunedSnapshot
	rotatedPrunedSnapshot.Journal.MaxControllerEpoch = reconciliationEpoch
	if err := controller.RecordAgentSnapshot(
		ctx,
		rotatedPrunedSnapshot,
	); err != nil {
		t.Fatal(err)
	}
	rotatedPrunedDigest, err := nodeAgentSnapshotDigest(rotatedPrunedSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		rotatedPrunedDigest,
		1,
	); !errors.Is(err, ErrGitHubJITAbsencePending) {
		t.Fatalf("rotated snapshot first absence = %v", err)
	}
	controller.now = func() time.Time {
		return time.Unix(100, 0).Add(GitHubRunnerAbsenceConfirmationDelay)
	}
	absence, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		attempt,
		reconciliationEpoch,
		rotatedPrunedDigest,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !absence.AwaitingAvailability ||
		absence.TerminalExecution == nil ||
		absence.Claim.State != GitHubClaimReconciliationRequired ||
		absence.Claim.Execution.State != domain.ExecutionReleased ||
		*absence.TerminalExecution != absence.Claim.Execution {
		t.Fatalf("lost-JIT dormant result = %#v", absence)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
	if _, found, err := controller.NextGitHubReconciliationFence(
		ctx,
		scaleSetID,
	); err != nil || found {
		t.Fatalf("dormant lost-JIT remained a restart fence = (%t, %v)", found, err)
	}
	if _, found, err := controller.NextActionableGitHubClaim(
		ctx,
		scaleSetID,
	); err != nil || found {
		t.Fatalf("provider absence rearmed without availability = (%t, %v)", found, err)
	}

	binding := SingleSlotBinding{
		TargetID:     absence.Claim.Execution.TargetID,
		NodeID:       absence.Claim.Execution.Slot.NodeID,
		Slot:         absence.Claim.Execution.Slot.Index,
		ClaimEnabled: true,
	}
	state, err := controller.ReadGitHubPollState(
		ctx,
		GitHubTargetRuntimeBinding{
			TargetID:   binding.TargetID,
			ScaleSetID: scaleSetID,
			ProfileID: domain.RunnerProfileID(
				fmt.Sprintf("profile-%d", scaleSetID),
			),
		},
		binding.NodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	state.ClaimAuthority.AdvertisedCapacity = 1
	binding.PollAuthority = state.ClaimAuthority

	source := githubQueueMessageForTest(
		scaleSetID,
		sourceMessageID,
		claimKey,
	)
	replay, err := controller.CommitGitHubQueueMessage(ctx, source, binding)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed ||
		replay.Claim == nil ||
		replay.Claim.Execution.ID != start.Command.ExecutionID ||
		replay.Claim.State != GitHubClaimReconciliationRequired {
		t.Fatalf("old message replay rearmed lost JIT = %#v", replay)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)

	freshA := githubQueueMessageForTest(
		scaleSetID,
		freshMessageID,
		claimKey,
	)
	freshA.Jobs[0].ExecutionID = "github-execution-lost-jit-retry-a"
	freshB := githubQueueMessageForTest(
		scaleSetID,
		freshMessageID+1,
		claimKey,
	)
	freshB.Jobs[0].ExecutionID = "github-execution-lost-jit-retry-b"
	blockedBinding := binding
	blockedBinding.ClaimEnabled = false
	if _, err := controller.CommitGitHubQueueMessage(
		ctx,
		freshA,
		blockedBinding,
	); !errors.Is(err, ErrGitHubRecoveryAvailabilityPending) {
		t.Fatalf(
			"lost-JIT availability without admission = %v, want pending",
			err,
		)
	}
	assertCount(
		t,
		controller.db,
		`SELECT count(*) FROM github_queue_messages
			WHERE scale_set_id = 44 AND message_id = 815`,
		0,
	)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)

	freshMessages := []GitHubQueueMessage{freshA, freshB}
	results := make([]GitHubMessageCommit, len(freshMessages))
	errs := make([]error, len(freshMessages))
	startConcurrent := make(chan struct{})
	var commits sync.WaitGroup
	for index := range freshMessages {
		commits.Add(1)
		go func(index int) {
			defer commits.Done()
			<-startConcurrent
			results[index], errs[index] = controller.CommitGitHubQueueMessage(
				ctx,
				freshMessages[index],
				binding,
			)
		}(index)
	}
	close(startConcurrent)
	commits.Wait()
	for index := range results {
		if errs[index] != nil ||
			results[index].Replayed ||
			results[index].UnclaimedAvailable ||
			results[index].Claim == nil ||
			results[index].Claim.State != GitHubClaimPending ||
			results[index].Claim.Execution.State != domain.ExecutionReserved ||
			results[index].Claim.CurrentAttempt != attempt.Attempt {
			t.Fatalf("concurrent fresh availability %d = (%#v, %v)",
				index, results[index], errs[index])
		}
	}
	rearmedClaim, found, err := controller.GitHubClaim(
		ctx,
		scaleSetID,
		claimKey,
	)
	if err != nil || !found {
		t.Fatalf("concurrent rearmed claim = (%#v, %t, %v)",
			rearmedClaim, found, err)
	}
	var selectedFresh GitHubQueueMessage
	switch rearmedClaim.SourceMessageID {
	case freshA.MessageID:
		selectedFresh = freshA
	case freshB.MessageID:
		selectedFresh = freshB
	default:
		t.Fatalf("concurrent rearm used unknown evidence = %#v", rearmedClaim)
	}
	if rearmedClaim.Execution.ID != selectedFresh.Jobs[0].ExecutionID {
		t.Fatalf("concurrent rearm split message/execution authority = %#v",
			rearmedClaim)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 2)

	freshReplay, err := controller.CommitGitHubQueueMessage(
		ctx,
		selectedFresh,
		binding,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !freshReplay.Replayed ||
		freshReplay.Claim == nil ||
		freshReplay.Claim.Execution.ID != selectedFresh.Jobs[0].ExecutionID {
		t.Fatalf("fresh replay duplicated rearm = %#v", freshReplay)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 2)

	acquire, err := controller.BeginGitHubAcquire(
		ctx,
		scaleSetID,
		claimKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	if acquire.Attempt != 2 ||
		acquire.EvidenceMessage != selectedFresh.MessageID ||
		acquire.ControllerEpoch != reconciliationEpoch {
		t.Fatalf("rearmed acquire authority = %#v", acquire)
	}
	if err := controller.MarkGitHubAcquired(ctx, acquire); err != nil {
		t.Fatal(err)
	}
	retryPrepare := IssuedAgentCommand{
		NodeID: start.NodeID,
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              "github-lost-jit-retry-prepare",
			ControllerEpoch: reconciliationEpoch,
			ExecutionID:     selectedFresh.Jobs[0].ExecutionID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   digestForTest("github-lost-jit-retry-prepare"),
		},
	}
	if replayed, err := controller.CommitAgentCommand(
		ctx,
		retryPrepare,
	); err != nil || replayed {
		t.Fatalf("retry Prepare authority = (%t, %v)", replayed, err)
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(
		ctx,
		AgentExecutionUpdate{
			NodeID:        start.NodeID,
			MessageID:     "github-lost-jit-retry-preparing",
			CommandID:     retryPrepare.Command.ID,
			ExecutionID:   retryPrepare.Command.ExecutionID,
			State:         domain.ExecutionPreparing,
			PayloadDigest: digestForTest("github-lost-jit-retry-preparing"),
		},
	); err != nil || replayed {
		t.Fatalf("retry Preparing = (%t, %v)", replayed, err)
	}
	if err := controller.MarkGitHubPreparing(
		ctx,
		scaleSetID,
		claimKey,
	); err != nil {
		t.Fatal(err)
	}
	nextAttempt, replayed, err := controller.BeginGitHubJITAttempt(
		ctx,
		scaleSetID,
		claimKey,
		reconciliationEpoch,
		attempt.RunnerName,
	)
	if err != nil || replayed || nextAttempt.Attempt != attempt.Attempt+1 {
		t.Fatalf("post-availability next JIT = (%#v, %t, %v)", nextAttempt, replayed, err)
	}
}

func TestGitHubLostJITTerminalFailuresCleanProviderWithoutReusingUnsafeSlot(t *testing.T) {
	tests := []struct {
		name                 string
		state                domain.ExecutionState
		errorCode            domain.ExecutionErrorCode
		seed                 int
		messageID            MessageID
		requestID            int64
		wantAwaiting         bool
		wantCleanupBlocked   bool
		wantReservationCount int
	}{
		{
			name:                 "failed",
			state:                domain.ExecutionFailed,
			errorCode:            domain.ExecutionErrorStart,
			seed:                 1,
			messageID:            831,
			requestID:            1231,
			wantAwaiting:         true,
			wantReservationCount: 0,
		},
		{
			name:                 "cleanup failed",
			state:                domain.ExecutionCleanupFailed,
			errorCode:            domain.ExecutionErrorCleanup,
			seed:                 2,
			messageID:            832,
			requestID:            1232,
			wantCleanupBlocked:   true,
			wantReservationCount: 1,
		},
		{
			name:                 "quarantined",
			state:                domain.ExecutionQuarantined,
			errorCode:            domain.ExecutionErrorQuarantined,
			seed:                 3,
			messageID:            833,
			requestID:            1233,
			wantCleanupBlocked:   true,
			wantReservationCount: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			controller := openController(
				t,
				"controller-github-lost-jit-"+strings.ReplaceAll(test.name, " ", "-")+".db",
			)
			defer controller.Close()
			attempt, start := prepareGitHubStartDispatchForTest(
				t,
				controller,
				test.seed,
				45,
				test.messageID,
				test.requestID,
			)
			if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
				t.Fatal(err)
			}
			attempt.State = GitHubJITStartAmbiguous
			reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			accepted := NodeAgentSnapshot{
				NodeID:            start.NodeID,
				OS:                domain.OSLinux,
				Architecture:      domain.ArchAMD64,
				RunnerVersion:     runner.OfficialRunnerVersion,
				NativeRunnerReady: true,
				Journal: AgentSnapshot{
					MaxControllerEpoch: start.Command.ControllerEpoch,
					Commands:           []domain.Command{start.Command},
					Observations: []ObservationSnapshot{{
						ExecutionID:        start.Command.ExecutionID,
						State:              domain.ExecutionPreparing,
						ObservedAtUnixNano: 40,
					}},
				},
			}
			if err := controller.RecordAgentSnapshot(ctx, accepted); err != nil {
				t.Fatal(err)
			}
			acceptedDigest, err := nodeAgentSnapshotDigest(accepted)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.MarkGitHubJITAgentAccepted(
				ctx,
				attempt,
				reconciliationEpoch,
				acceptedDigest,
			); err != nil {
				t.Fatal(err)
			}
			attempt.State = GitHubJITAgentAccepted

			terminalObservation := ObservationSnapshot{
				ExecutionID:        start.Command.ExecutionID,
				State:              test.state,
				ObservedAtUnixNano: 41,
			}
			terminalSnapshot := accepted
			terminalSnapshot.Journal.Observations =
				[]ObservationSnapshot{terminalObservation}
			if err := controller.RecordAgentSnapshot(
				ctx,
				terminalSnapshot,
			); err != nil {
				t.Fatal(err)
			}
			terminalDigest, err := nodeAgentSnapshotDigest(terminalSnapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.MarkGitHubJITObservedStarted(
				ctx,
				attempt,
				reconciliationEpoch,
				terminalObservation,
				terminalDigest,
			); !errors.Is(err, ErrGitHubJITTerminalPending) {
				t.Fatalf("terminal snapshot before exact update = %v", err)
			}
			assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)
			if test.wantReservationCount == 1 {
				assertNodeAdministrativeState(
					t,
					controller,
					string(start.NodeID),
					domain.NodeQuarantined,
				)
			}

			if replayed, err := controller.RecordAgentExecutionUpdate(
				ctx,
				AgentExecutionUpdate{
					NodeID:        start.NodeID,
					MessageID:     "github-lost-jit-terminal-" + test.name,
					CommandID:     start.Command.ID,
					ExecutionID:   start.Command.ExecutionID,
					State:         test.state,
					ErrorCode:     test.errorCode,
					PayloadDigest: digestForTest("github-lost-jit-terminal-" + test.name),
				},
			); err != nil || replayed {
				t.Fatalf("durable terminal update = (%t, %v)", replayed, err)
			}
			if err := controller.MarkGitHubJITObservedStarted(
				ctx,
				attempt,
				reconciliationEpoch,
				terminalObservation,
				terminalDigest,
			); !errors.Is(err, ErrGitHubJITStartNotProven) {
				t.Fatalf("terminal-only Start proof = %v", err)
			}
			assertCount(
				t,
				controller.db,
				"SELECT count(*) FROM slot_reservations",
				test.wantReservationCount,
			)
			if err := controller.MarkGitHubJITRemovalPending(
				ctx,
				attempt,
				reconciliationEpoch,
				terminalDigest,
				1,
				false,
			); err != nil {
				t.Fatal(err)
			}
			attempt.State = GitHubJITRemovalPending
			if _, err := controller.MarkGitHubJITReconciledAbsent(
				ctx,
				attempt,
				reconciliationEpoch,
				terminalDigest,
				1,
			); !errors.Is(err, ErrGitHubJITAbsencePending) {
				t.Fatalf("first provider absence = %v", err)
			}
			controller.now = func() time.Time {
				return time.Unix(100, 0).Add(GitHubRunnerAbsenceConfirmationDelay)
			}
			absence, err := controller.MarkGitHubJITReconciledAbsent(
				ctx,
				attempt,
				reconciliationEpoch,
				terminalDigest,
				1,
			)
			if err != nil {
				t.Fatal(err)
			}
			if absence.TerminalExecution == nil ||
				absence.Claim.Execution.State != test.state ||
				absence.AwaitingAvailability != test.wantAwaiting ||
				absence.CleanupBlocked != test.wantCleanupBlocked {
				t.Fatalf("terminal absence result = %#v", absence)
			}
			assertCount(
				t,
				controller.db,
				"SELECT count(*) FROM slot_reservations",
				test.wantReservationCount,
			)
			if _, found, err := controller.NextGitHubReconciliationFence(
				ctx,
				attempt.ScaleSetID,
			); err != nil || found {
				t.Fatalf("terminal absence retained restart fence = (%t, %v)",
					found, err)
			}
			if !test.wantCleanupBlocked {
				return
			}
			assertNodeAdministrativeState(
				t,
				controller,
				string(start.NodeID),
				domain.NodeQuarantined,
			)
			binding := SingleSlotBinding{
				TargetID:     absence.Claim.Execution.TargetID,
				NodeID:       absence.Claim.Execution.Slot.NodeID,
				Slot:         absence.Claim.Execution.Slot.Index,
				ClaimEnabled: true,
			}
			capacity, err := controller.GitHubSingleSlotCapacity(ctx, binding)
			if err != nil || capacity != 0 {
				t.Fatalf("cleanup-blocked capacity = (%d, %v)", capacity, err)
			}
			fresh := githubQueueMessageForTest(
				attempt.ScaleSetID,
				test.messageID+100,
				test.requestID,
			)
			fresh.Jobs[0].ExecutionID =
				domain.ExecutionID("github-cleanup-blocked-fresh-" + test.name)
			commit, err := controller.CommitGitHubQueueMessage(
				ctx,
				fresh,
				binding,
			)
			if err != nil ||
				commit.Claim == nil ||
				commit.Claim.Execution.ID != start.Command.ExecutionID ||
				commit.Claim.State != GitHubClaimReconciliationRequired {
				t.Fatalf("cleanup-blocked fresh availability = (%#v, %v)",
					commit, err)
			}
			assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
			assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 1)
			assertCount(
				t,
				controller.db,
				"SELECT count(*) FROM slot_reservations",
				test.wantReservationCount,
			)
		})
	}
}

func TestGitHubStartedTransitionConvergesFastTerminalExecutionAtomically(t *testing.T) {
	tests := []struct {
		name      string
		state     domain.ExecutionState
		errorCode domain.ExecutionErrorCode
		seed      int
		messageID MessageID
		requestID int64
		wantSlot  int
	}{
		{
			name:      "released",
			state:     domain.ExecutionReleased,
			errorCode: domain.ExecutionErrorNone,
			seed:      2,
			messageID: 502,
			requestID: 902,
			wantSlot:  0,
		},
		{
			name:      "failed",
			state:     domain.ExecutionFailed,
			errorCode: domain.ExecutionErrorStart,
			seed:      3,
			messageID: 503,
			requestID: 903,
			wantSlot:  0,
		},
		{
			name:      "cleanup failed",
			state:     domain.ExecutionCleanupFailed,
			errorCode: domain.ExecutionErrorCleanup,
			seed:      4,
			messageID: 504,
			requestID: 904,
			wantSlot:  1,
		},
		{
			name:      "quarantined",
			state:     domain.ExecutionQuarantined,
			errorCode: domain.ExecutionErrorQuarantined,
			seed:      5,
			messageID: 505,
			requestID: 905,
			wantSlot:  0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			controller := openController(t, "controller-github-fast-terminal-"+test.name+".db")
			defer controller.Close()

			attempt, start := prepareGitHubStartDispatchForTest(
				t, controller, test.seed, 22, test.messageID, test.requestID)
			reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			observation := ObservationSnapshot{
				ExecutionID:        start.Command.ExecutionID,
				State:              test.state,
				ObservedAtUnixNano: 31,
			}
			snapshot := NodeAgentSnapshot{
				NodeID:            start.NodeID,
				OS:                domain.OSLinux,
				Architecture:      domain.ArchAMD64,
				RunnerVersion:     runner.OfficialRunnerVersion,
				NativeRunnerReady: true,
				Journal: AgentSnapshot{
					MaxControllerEpoch: start.Command.ControllerEpoch,
					Commands:           []domain.Command{start.Command},
					Observations:       []ObservationSnapshot{observation},
				},
			}
			if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			digest, err := nodeAgentSnapshotDigest(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.MarkGitHubJITObservedStarted(
				ctx,
				attempt,
				reconciliationEpoch,
				observation,
				digest,
			); !errors.Is(err, ErrGitHubJITTerminalPending) {
				t.Fatalf("terminal snapshot before outbox = %v", err)
			}
			claim, found, err := controller.GitHubClaim(
				ctx,
				attempt.ScaleSetID,
				attempt.ClaimKey,
			)
			if err != nil || !found ||
				claim.Execution.State != domain.ExecutionPreparing {
				t.Fatalf("snapshot-only terminal changed desired state = (%#v, %t, %v)",
					claim, found, err)
			}
			assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)
			if test.wantSlot == 1 {
				assertNodeAdministrativeState(
					t,
					controller,
					string(start.NodeID),
					domain.NodeQuarantined,
				)
			}

			running := AgentExecutionUpdate{
				NodeID:        start.NodeID,
				MessageID:     "github-start-running-" + test.name,
				CommandID:     start.Command.ID,
				ExecutionID:   start.Command.ExecutionID,
				State:         domain.ExecutionRunning,
				PayloadDigest: digestForTest("github-start-running-" + test.name),
			}
			if replayed, err := controller.RecordAgentExecutionUpdate(ctx, running); err != nil || replayed {
				t.Fatalf("record running = (%t, %v)", replayed, err)
			}
			if err := controller.MarkGitHubJITObservedStarted(
				ctx,
				attempt,
				reconciliationEpoch,
				observation,
				digest,
			); err != nil {
				t.Fatalf("durable Running did not close JIT fence = %v", err)
			}
			claim, found, err = controller.GitHubClaim(
				ctx, attempt.ScaleSetID, attempt.ClaimKey)
			if err != nil || !found ||
				claim.State != GitHubClaimRunning ||
				claim.Execution.State != domain.ExecutionRunning {
				t.Fatalf("Running-only proof projected terminal snapshot = (%#v, %t, %v)",
					claim, found, err)
			}
			current, found, err := controller.CurrentGitHubJITAttempt(
				ctx, attempt.ScaleSetID, attempt.ClaimKey)
			if err != nil || !found || current.State != GitHubJITStarted {
				t.Fatalf("started attempt = (%#v, %t, %v)", current, found, err)
			}
			assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)

			// Terminal lifecycle delivery is the sole owner of terminal desired
			// state and lease mutation. Snapshot cleanup evidence may quarantine
			// earlier, and the outbox update must still remain acceptable after
			// the JIT fence was closed from Running history.
			terminal := AgentExecutionUpdate{
				NodeID:        start.NodeID,
				MessageID:     "github-start-terminal-" + test.name,
				CommandID:     start.Command.ID,
				ExecutionID:   start.Command.ExecutionID,
				State:         test.state,
				ErrorCode:     test.errorCode,
				PayloadDigest: digestForTest("github-start-terminal-" + test.name),
			}
			if replayed, err := controller.RecordAgentExecutionUpdate(
				ctx,
				terminal,
			); err != nil || replayed {
				t.Fatalf("record terminal after JIT convergence = (%t, %v)",
					replayed, err)
			}
			finalClaim, found, err := controller.GitHubClaim(
				ctx,
				attempt.ScaleSetID,
				attempt.ClaimKey,
			)
			if err != nil || !found ||
				finalClaim.State != GitHubClaimRunning ||
				finalClaim.Execution.State != test.state {
				t.Fatalf("terminal outbox claim = (%#v, %t, %v)",
					finalClaim, found, err)
			}
			assertCount(
				t,
				controller.db,
				"SELECT count(*) FROM slot_reservations",
				test.wantSlot,
			)
			if test.wantSlot == 1 {
				assertNodeAdministrativeState(
					t,
					controller,
					string(start.NodeID),
					domain.NodeQuarantined,
				)
			}
			if err := controller.MarkGitHubJITObservedStarted(
				ctx,
				current,
				reconciliationEpoch,
				observation,
				digest,
			); err != nil {
				t.Fatalf("terminal replay convergence = %v", err)
			}
			if _, found, err := controller.NextGitHubReconciliationFence(
				ctx,
				attempt.ScaleSetID,
			); err != nil || found {
				t.Fatalf("fast terminal starved restart poll = (%t, %v)", found, err)
			}
		})
	}
}

func TestGitHubPrunedStartedTerminalHistoryCreatesProviderCleanupIntent(t *testing.T) {
	tests := []struct {
		name      string
		state     domain.ExecutionState
		errorCode domain.ExecutionErrorCode
		seed      int
		messageID MessageID
		requestID int64
	}{
		{
			name:      "released",
			state:     domain.ExecutionReleased,
			errorCode: domain.ExecutionErrorNone,
			seed:      1,
			messageID: 521,
			requestID: 921,
		},
		{
			name:      "failed",
			state:     domain.ExecutionFailed,
			errorCode: domain.ExecutionErrorStart,
			seed:      2,
			messageID: 522,
			requestID: 922,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			controller := openController(
				t,
				"controller-github-pruned-started-"+test.name+".db",
			)
			defer controller.Close()

			attempt, start := prepareGitHubStartDispatchForTest(
				t,
				controller,
				test.seed,
				24,
				test.messageID,
				test.requestID,
			)
			if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
				t.Fatal(err)
			}
			attempt.State = GitHubJITStartAmbiguous
			for _, update := range []AgentExecutionUpdate{
				{
					NodeID:        start.NodeID,
					MessageID:     "github-pruned-running-" + test.name,
					CommandID:     start.Command.ID,
					ExecutionID:   start.Command.ExecutionID,
					State:         domain.ExecutionRunning,
					PayloadDigest: digestForTest("github-pruned-running-" + test.name),
				},
				{
					NodeID:        start.NodeID,
					MessageID:     "github-pruned-terminal-" + test.name,
					CommandID:     start.Command.ID,
					ExecutionID:   start.Command.ExecutionID,
					State:         test.state,
					ErrorCode:     test.errorCode,
					PayloadDigest: digestForTest("github-pruned-terminal-" + test.name),
				},
			} {
				if replayed, err := controller.RecordAgentExecutionUpdate(
					ctx,
					update,
				); err != nil || replayed {
					t.Fatalf("record %s = (%t, %v)", update.State, replayed, err)
				}
			}
			assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)

			reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			pruned := NodeAgentSnapshot{
				NodeID:            start.NodeID,
				OS:                domain.OSLinux,
				Architecture:      domain.ArchAMD64,
				RunnerVersion:     runner.OfficialRunnerVersion,
				NativeRunnerReady: true,
				Journal: AgentSnapshot{
					MaxControllerEpoch: start.Command.ControllerEpoch,
				},
			}
			if err := controller.RecordAgentSnapshot(ctx, pruned); err != nil {
				t.Fatal(err)
			}
			digest, err := nodeAgentSnapshotDigest(pruned)
			if err != nil {
				t.Fatal(err)
			}
			result, err := controller.ReconcileGitHubJITPrunedHistory(
				ctx,
				attempt,
				reconciliationEpoch,
				digest,
			)
			if err != nil || !result.Started || result.LostTerminal != "" {
				t.Fatalf("pruned started history = (%#v, %v)", result, err)
			}
			current, found, err := controller.CurrentGitHubJITAttempt(
				ctx,
				attempt.ScaleSetID,
				attempt.ClaimKey,
			)
			if err != nil || !found || current.State != GitHubJITStarted {
				t.Fatalf("pruned current attempt = (%#v, %t, %v)",
					current, found, err)
			}
			claim, found, err := controller.GitHubClaim(
				ctx,
				attempt.ScaleSetID,
				attempt.ClaimKey,
			)
			if err != nil || !found ||
				claim.State != GitHubClaimRunning ||
				claim.Execution.State != test.state {
				t.Fatalf("pruned terminal claim = (%#v, %t, %v)",
					claim, found, err)
			}
			if _, found, err := controller.NextGitHubReconciliationFence(
				ctx,
				attempt.ScaleSetID,
			); err != nil || found {
				t.Fatalf("pruned terminal retained fence = (%t, %v)", found, err)
			}

			binding := SingleSlotBinding{
				TargetID:     claim.Execution.TargetID,
				NodeID:       claim.Execution.Slot.NodeID,
				Slot:         claim.Execution.Slot.Index,
				ClaimEnabled: true,
			}
			pollState, err := controller.ReadGitHubPollState(
				ctx,
				GitHubTargetRuntimeBinding{
					TargetID:   binding.TargetID,
					ScaleSetID: attempt.ScaleSetID,
					ProfileID: domain.RunnerProfileID(
						fmt.Sprintf("profile-%d", attempt.ScaleSetID),
					),
				},
				binding.NodeID,
			)
			if err != nil {
				t.Fatal(err)
			}
			pollState.ClaimAuthority.AdvertisedCapacity = 1
			binding.PollAuthority = pollState.ClaimAuthority

			// Running proves only that the official process started. Without an
			// exact GitHub JobStarted/JobCompleted event, fresh availability
			// records only replacement intent and fences the slot until
			// provider cleanup proves the old runner absent.
			fresh := githubQueueMessageForTest(
				attempt.ScaleSetID,
				test.messageID+100,
				test.requestID,
			)
			fresh.Jobs[0].ExecutionID =
				domain.ExecutionID("github-pruned-fresh-" + test.name)
			commit, err := controller.CommitGitHubQueueMessage(
				ctx,
				fresh,
				binding,
			)
			if err != nil ||
				commit.Claim == nil ||
				commit.Claim.Execution.ID != claim.Execution.ID ||
				commit.Claim.State != GitHubClaimReconciliationRequired ||
				commit.RequeueIntent == nil ||
				commit.RequeueIntent.Replacement.ID != fresh.Jobs[0].ExecutionID {
				t.Fatalf("started terminal fresh availability = (%#v, %v)",
					commit, err)
			}
			assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
			assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 1)
			assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
			if capacity, err := controller.GitHubSingleSlotCapacity(
				ctx,
				binding,
			); err != nil || capacity != 0 {
				t.Fatalf("cleanup intent capacity = (%d, %v), want zero",
					capacity, err)
			}
		})
	}
}

func TestGitHubFreshRequeueCreatesDurableCleanupIntentWithoutPickupProof(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-unpicked-requeue-intent.db")
	defer controller.Close()

	attempt, start := prepareGitHubStartDispatchForTest(
		t,
		controller,
		3,
		64,
		901,
		1901,
	)
	if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	for _, update := range []AgentExecutionUpdate{
		{
			NodeID:        start.NodeID,
			MessageID:     "github-unpicked-running",
			CommandID:     start.Command.ID,
			ExecutionID:   start.Command.ExecutionID,
			State:         domain.ExecutionRunning,
			PayloadDigest: digestForTest("github-unpicked-running"),
		},
		{
			NodeID:        start.NodeID,
			MessageID:     "github-unpicked-released",
			CommandID:     start.Command.ID,
			ExecutionID:   start.Command.ExecutionID,
			State:         domain.ExecutionReleased,
			PayloadDigest: digestForTest("github-unpicked-released"),
		},
	} {
		if replayed, err := controller.RecordAgentExecutionUpdate(
			ctx,
			update,
		); err != nil || replayed {
			t.Fatalf("record %s = (%t, %v)", update.State, replayed, err)
		}
	}

	reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pruned := NodeAgentSnapshot{
		NodeID:            start.NodeID,
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		RunnerVersion:     runner.OfficialRunnerVersion,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, pruned); err != nil {
		t.Fatal(err)
	}
	snapshotDigest, err := nodeAgentSnapshotDigest(pruned)
	if err != nil {
		t.Fatal(err)
	}
	result, err := controller.ReconcileGitHubJITPrunedHistory(
		ctx,
		attempt,
		reconciliationEpoch,
		snapshotDigest,
	)
	if err != nil || !result.Started {
		t.Fatalf("pruned start = (%#v, %v)", result, err)
	}
	current, found, err := controller.CurrentGitHubJITAttempt(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found || current.State != GitHubJITStarted {
		t.Fatalf("current attempt = (%#v, %t, %v)", current, found, err)
	}
	claim, found, err := controller.GitHubClaim(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found || claim.Execution.State != domain.ExecutionReleased {
		t.Fatalf("terminal claim = (%#v, %t, %v)", claim, found, err)
	}

	binding := SingleSlotBinding{
		TargetID:     claim.Execution.TargetID,
		NodeID:       claim.Execution.Slot.NodeID,
		Slot:         claim.Execution.Slot.Index,
		ClaimEnabled: true,
	}
	pollState, err := controller.ReadGitHubPollState(
		ctx,
		GitHubTargetRuntimeBinding{
			TargetID:   binding.TargetID,
			ScaleSetID: attempt.ScaleSetID,
			ProfileID: domain.RunnerProfileID(
				fmt.Sprintf("profile-%d", attempt.ScaleSetID),
			),
		},
		binding.NodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	pollState.ClaimAuthority.AdvertisedCapacity = 1
	binding.PollAuthority = pollState.ClaimAuthority

	fresh := githubQueueMessageForTest(
		attempt.ScaleSetID,
		902,
		attempt.ClaimKey,
	)
	fresh.Jobs[0].ExecutionID = "github-unpicked-replacement"
	// GitHub may repeat the same availability within one batch. The first
	// event owns the immutable replacement identity; restart dispatch
	// authority requires at least one matching source event, not exactly one.
	fresh.Jobs = append(fresh.Jobs, fresh.Jobs[0])
	commit, err := controller.CommitGitHubQueueMessage(ctx, fresh, binding)
	if err != nil {
		t.Fatal(err)
	}
	if commit.Replayed || commit.Claim == nil ||
		commit.Claim.State != GitHubClaimReconciliationRequired ||
		commit.RequeueIntent == nil ||
		commit.RequeueIntent.Claim != *commit.Claim ||
		commit.RequeueIntent.Attempt != current ||
		commit.RequeueIntent.Replacement.ID != fresh.Jobs[0].ExecutionID ||
		commit.RequeueIntent.Replacement.State != domain.ExecutionReserved {
		t.Fatalf("fresh unpicked requeue commit = %#v", commit)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_unpicked_requeue_intents", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
	if capacity, err := controller.GitHubSingleSlotCapacity(
		ctx,
		binding,
	); err != nil || capacity != 0 {
		t.Fatalf("cleanup intent capacity = (%d, %v), want zero", capacity, err)
	}

	replayed, err := controller.CommitGitHubQueueMessage(ctx, fresh, binding)
	if err != nil || !replayed.Replayed || replayed.RequeueIntent == nil {
		t.Fatalf("replayed intent = (%#v, %v)", replayed, err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_unpicked_requeue_intents", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)

	sessionHealth, err := controller.ReadGitHubScaleSetSessionHealth(
		ctx,
		attempt.ScaleSetID,
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(time.Hour)
	controller.now = func() time.Time { return now }
	if err := controller.MarkGitHubJITRemovalPending(
		ctx,
		current,
		reconciliationEpoch,
		snapshotDigest,
		sessionHealth.TransitionGeneration,
		false,
	); err != nil {
		t.Fatalf("mark unpicked runner removal pending = %v", err)
	}
	current.State = GitHubJITRemovalPending
	if _, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		current,
		reconciliationEpoch,
		snapshotDigest,
		sessionHealth.TransitionGeneration,
	); !errors.Is(err, ErrGitHubJITAbsencePending) {
		t.Fatalf("first unpicked absence = %v, want pending", err)
	}
	now = now.Add(GitHubRunnerAbsenceConfirmationDelay)
	absence, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		current,
		reconciliationEpoch,
		snapshotDigest,
		sessionHealth.TransitionGeneration,
	)
	if err != nil {
		t.Fatal(err)
	}
	if absence.ReplacementExecution == nil ||
		!absence.ReplacementClaimed ||
		*absence.ReplacementExecution != commit.RequeueIntent.Replacement ||
		absence.Claim.Execution != commit.RequeueIntent.Replacement ||
		absence.Claim.State != GitHubClaimPending {
		t.Fatalf("confirmed unpicked replacement = %#v", absence)
	}
	dispatchReady, err := controller.GitHubPendingClaimDispatchReady(
		ctx,
		absence.Claim,
	)
	if err != nil || !dispatchReady {
		t.Fatalf(
			"confirmed replacement dispatch authority = (%t, %v), want ready",
			dispatchReady,
			err,
		)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_unpicked_requeue_intents", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 2)
	assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 2)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)

	// Startup must not trust the internal drive-before-poll marker without its
	// exact source-message lineage. Keep the foreign key valid so this exercises
	// the semantic invariant rather than SQLite's structural checks.
	path := controller.path
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	execRaw(t, path, `UPDATE github_job_claims
		SET source_message_id = 901
		WHERE state = 'pending'`)
	recovery, err := OpenController(ctx, path, Options{})
	if recovery == nil ||
		!errors.Is(err, ErrRecoveryMode) ||
		!errors.Is(err, ErrCorruptBackup) {
		t.Fatalf("corrupt replacement lineage open = (%v, %v)", recovery, err)
	}
	if readyErr := recovery.Ready(); !errors.Is(readyErr, ErrRecoveryMode) {
		t.Fatalf("corrupt replacement lineage readiness = %v", readyErr)
	}
	if err := recovery.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGitHubFreshRequeueAcceptsFastTerminalMarkRunningRace(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-fast-terminal-requeue.db")
	defer controller.Close()

	attempt, start := prepareGitHubStartDispatchForTest(
		t,
		controller,
		8,
		91,
		1201,
		2301,
	)
	for _, update := range []AgentExecutionUpdate{
		{
			NodeID:        start.NodeID,
			MessageID:     "github-fast-terminal-running",
			CommandID:     start.Command.ID,
			ExecutionID:   start.Command.ExecutionID,
			State:         domain.ExecutionRunning,
			PayloadDigest: digestForTest("github-fast-terminal-running"),
		},
		{
			NodeID:        start.NodeID,
			MessageID:     "github-fast-terminal-released",
			CommandID:     start.Command.ID,
			ExecutionID:   start.Command.ExecutionID,
			State:         domain.ExecutionReleased,
			PayloadDigest: digestForTest("github-fast-terminal-released"),
		},
	} {
		if replayed, err := controller.RecordAgentExecutionUpdate(
			ctx,
			update,
		); err != nil || replayed {
			t.Fatalf("record %s = (%t, %v)", update.State, replayed, err)
		}
	}
	if err := controller.MarkGitHubRunning(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	current, found, err := controller.CurrentGitHubJITAttempt(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found || current.State != GitHubJITStarted {
		t.Fatalf("fast terminal attempt = (%#v, %t, %v)", current, found, err)
	}
	claim, found, err := controller.GitHubClaim(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found ||
		claim.State != GitHubClaimReconciliationRequired ||
		claim.Execution.State != domain.ExecutionReleased {
		t.Fatalf("fast terminal claim = (%#v, %t, %v)", claim, found, err)
	}
	binding := refreshGitHubRequeueBindingForTest(
		t,
		controller,
		current,
		claim,
		NodeAgentSnapshot{
			NodeID:            start.NodeID,
			OS:                domain.OSLinux,
			Architecture:      domain.ArchAMD64,
			RunnerVersion:     runner.OfficialRunnerVersion,
			NativeRunnerReady: true,
			Journal: AgentSnapshot{
				MaxControllerEpoch: start.Command.ControllerEpoch,
				Observations: []ObservationSnapshot{{
					ExecutionID:        start.Command.ExecutionID,
					State:              domain.ExecutionReleased,
					ObservedAtUnixNano: 1201,
				}},
			},
		},
	)
	fresh := githubQueueMessageForTest(
		attempt.ScaleSetID,
		1202,
		attempt.ClaimKey,
	)
	fresh.Jobs[0].ExecutionID = "github-fast-terminal-replacement"
	commit, err := controller.CommitGitHubQueueMessage(
		ctx,
		fresh,
		binding,
	)
	if err != nil || commit.RequeueIntent == nil ||
		commit.RequeueIntent.Claim.State != GitHubClaimReconciliationRequired ||
		commit.RequeueIntent.Replacement.ID != fresh.Jobs[0].ExecutionID {
		t.Fatalf("fast terminal requeue = (%#v, %v)", commit, err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
}

func TestGitHubFreshRequeueRollsBackUntilTerminalOutboxCatchesSnapshot(
	t *testing.T,
) {
	ctx := context.Background()
	controller := openController(t, "controller-github-terminal-outbox-pending.db")
	defer controller.Close()

	attempt, start := prepareGitHubStartDispatchForTest(
		t,
		controller,
		9,
		92,
		1211,
		2311,
	)
	running := AgentExecutionUpdate{
		NodeID:        start.NodeID,
		MessageID:     "github-outbox-pending-running",
		CommandID:     start.Command.ID,
		ExecutionID:   start.Command.ExecutionID,
		State:         domain.ExecutionRunning,
		PayloadDigest: digestForTest("github-outbox-pending-running"),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(
		ctx,
		running,
	); err != nil || replayed {
		t.Fatalf("record Running = (%t, %v)", replayed, err)
	}
	if err := controller.MarkGitHubRunning(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	claim, found, err := controller.GitHubClaim(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found ||
		claim.State != GitHubClaimRunning ||
		claim.Execution.State != domain.ExecutionRunning {
		t.Fatalf("running claim = (%#v, %t, %v)", claim, found, err)
	}
	terminalSnapshot := NodeAgentSnapshot{
		NodeID:            start.NodeID,
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		RunnerVersion:     runner.OfficialRunnerVersion,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
			Observations: []ObservationSnapshot{{
				ExecutionID:        start.Command.ExecutionID,
				State:              domain.ExecutionReleased,
				ObservedAtUnixNano: 1212,
			}},
		},
	}
	binding := refreshGitHubRequeueBindingForTest(
		t,
		controller,
		attempt,
		claim,
		terminalSnapshot,
	)
	binding.ClaimEnabled = false
	binding.PollAuthority.AdvertisedCapacity = 0
	fresh := githubQueueMessageForTest(
		attempt.ScaleSetID,
		1212,
		attempt.ClaimKey,
	)
	fresh.Jobs[0].ExecutionID = "github-outbox-pending-replacement"
	if _, err := controller.CommitGitHubQueueMessage(
		ctx,
		fresh,
		binding,
	); !errors.Is(err, ErrGitHubRequeueTerminalPending) {
		t.Fatalf("snapshot-before-outbox availability = %v", err)
	}
	assertCount(
		t,
		controller.db,
		`SELECT count(*) FROM github_queue_messages
			WHERE scale_set_id = 92 AND message_id = 1212`,
		0,
	)
	assertCount(t, controller.db, "SELECT count(*) FROM github_unpicked_requeue_intents", 0)

	terminal := AgentExecutionUpdate{
		NodeID:        start.NodeID,
		MessageID:     "github-outbox-pending-released",
		CommandID:     start.Command.ID,
		ExecutionID:   start.Command.ExecutionID,
		State:         domain.ExecutionReleased,
		PayloadDigest: digestForTest("github-outbox-pending-released"),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(
		ctx,
		terminal,
	); err != nil || replayed {
		t.Fatalf("record terminal outbox = (%t, %v)", replayed, err)
	}
	claim, found, err = controller.GitHubClaim(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found ||
		claim.Execution.State != domain.ExecutionReleased {
		t.Fatalf("terminal outbox claim = (%#v, %t, %v)", claim, found, err)
	}
	binding = refreshGitHubRequeueBindingForTest(
		t,
		controller,
		attempt,
		claim,
		terminalSnapshot,
	)
	commit, err := controller.CommitGitHubQueueMessage(
		ctx,
		fresh,
		binding,
	)
	if err != nil || commit.Replayed || commit.RequeueIntent == nil ||
		commit.RequeueIntent.Replacement.ID != fresh.Jobs[0].ExecutionID {
		t.Fatalf("redelivered availability = (%#v, %v)", commit, err)
	}
	assertCount(
		t,
		controller.db,
		`SELECT count(*) FROM github_queue_messages
			WHERE scale_set_id = 92 AND message_id = 1212`,
		1,
	)
	assertCount(t, controller.db, "SELECT count(*) FROM github_unpicked_requeue_intents", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
}

func TestGitHubFreshTerminalRequeueRollsBackUntilRecoveryAdmission(t *testing.T) {
	tests := []string{
		"claim disabled",
		"poll authority stale",
		"slot occupied",
	}
	for index, name := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			controller := openController(
				t,
				fmt.Sprintf("controller-github-requeue-admission-%d.db", index),
			)
			defer controller.Close()

			fixture := prepareGitHubStartedTerminalRequeueFixture(
				t,
				controller,
				1+index,
				ScaleSetID(120+index),
				MessageID(2200+index),
				int64(3200+index),
				domain.ExecutionReleased,
				"",
			)
			fresh := githubQueueMessageForTest(
				fixture.current.ScaleSetID,
				MessageID(4200+index),
				fixture.current.ClaimKey,
			)
			fresh.Jobs[0].ExecutionID = domain.ExecutionID(
				fmt.Sprintf("github-requeue-admission-%d", index),
			)
			blocked := fixture.binding
			eligible := fixture.binding

			var releaseOccupant func()
			switch name {
			case "claim disabled":
				blocked.ClaimEnabled = false
			case "poll authority stale":
				if err := controller.RecordAgentReadiness(
					ctx,
					fixture.start.NodeID,
					fixture.snapshotDigest,
					false,
				); err != nil {
					t.Fatal(err)
				}
			case "slot occupied":
				assignment, prepare := assignAndIssuePrepare(
					t,
					controller,
					string(fixture.start.NodeID),
					fixture.reconciliationEpoch,
					MessageID(5200+index),
					fmt.Sprintf("github-requeue-occupant-%d", index),
					0,
				)
				releaseOccupant = func() {
					t.Helper()
					failed := AgentExecutionUpdate{
						NodeID:        fixture.start.NodeID,
						MessageID:     fmt.Sprintf("github-requeue-occupant-failed-%d", index),
						CommandID:     prepare.Command.ID,
						ExecutionID:   assignment.Execution.ID,
						State:         domain.ExecutionFailed,
						ErrorCode:     domain.ExecutionErrorStart,
						PayloadDigest: digestForTest(fmt.Sprintf("github-requeue-occupant-failed-%d", index)),
					}
					if replayed, err := controller.RecordAgentExecutionUpdate(
						ctx,
						failed,
					); err != nil || replayed {
						t.Fatalf("release occupied slot = (%t, %v)", replayed, err)
					}
				}
			default:
				t.Fatalf("unknown admission case %q", name)
			}

			if _, err := controller.CommitGitHubQueueMessage(
				ctx,
				fresh,
				blocked,
			); !errors.Is(err, ErrGitHubRecoveryAvailabilityPending) {
				t.Fatalf("blocked fresh availability = %v, want pending", err)
			}
			assertCount(
				t,
				controller.db,
				fmt.Sprintf(
					`SELECT count(*) FROM github_queue_messages
						WHERE scale_set_id = %d AND message_id = %d`,
					fixture.current.ScaleSetID,
					fresh.MessageID,
				),
				0,
			)
			assertCount(
				t,
				controller.db,
				"SELECT count(*) FROM github_unpicked_requeue_intents",
				0,
			)

			switch name {
			case "poll authority stale":
				if err := controller.RecordAgentReadiness(
					ctx,
					fixture.start.NodeID,
					fixture.snapshotDigest,
					true,
				); err != nil {
					t.Fatal(err)
				}
				state, err := controller.ReadGitHubPollState(
					ctx,
					eligible.PollAuthority.Binding,
					eligible.NodeID,
				)
				if err != nil {
					t.Fatal(err)
				}
				state.ClaimAuthority.AdvertisedCapacity = 1
				eligible.PollAuthority = state.ClaimAuthority
			case "slot occupied":
				releaseOccupant()
			}

			commit, err := controller.CommitGitHubQueueMessage(
				ctx,
				fresh,
				eligible,
			)
			if err != nil || commit.Replayed || commit.RequeueIntent == nil ||
				commit.RequeueIntent.Replacement.ID != fresh.Jobs[0].ExecutionID {
				t.Fatalf("eligible redelivery = (%#v, %v)", commit, err)
			}
			assertCount(
				t,
				controller.db,
				fmt.Sprintf(
					`SELECT count(*) FROM github_queue_messages
						WHERE scale_set_id = %d AND message_id = %d`,
					fixture.current.ScaleSetID,
					fresh.MessageID,
				),
				1,
			)
			assertCount(
				t,
				controller.db,
				"SELECT count(*) FROM github_unpicked_requeue_intents",
				1,
			)
		})
	}
}

func refreshGitHubRequeueBindingForTest(
	t *testing.T,
	controller *ControllerStore,
	attempt GitHubJITAttempt,
	claim GitHubJobClaim,
	snapshot NodeAgentSnapshot,
) SingleSlotBinding {
	t.Helper()
	ctx := context.Background()
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	binding := SingleSlotBinding{
		TargetID:     claim.Execution.TargetID,
		NodeID:       claim.Execution.Slot.NodeID,
		Slot:         claim.Execution.Slot.Index,
		ClaimEnabled: true,
	}
	pollState, err := controller.ReadGitHubPollState(
		ctx,
		GitHubTargetRuntimeBinding{
			TargetID:   binding.TargetID,
			ScaleSetID: attempt.ScaleSetID,
			ProfileID: domain.RunnerProfileID(
				fmt.Sprintf("profile-%d", attempt.ScaleSetID),
			),
		},
		binding.NodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	pollState.ClaimAuthority.AdvertisedCapacity = 1
	binding.PollAuthority = pollState.ClaimAuthority
	return binding
}

type githubStartedTerminalRequeueFixture struct {
	attempt             GitHubJITAttempt
	current             GitHubJITAttempt
	start               IssuedAgentCommand
	reconciliationEpoch domain.ControllerEpoch
	snapshotDigest      string
	claim               GitHubJobClaim
	binding             SingleSlotBinding
}

func prepareGitHubStartedTerminalRequeueFixture(
	t *testing.T,
	controller *ControllerStore,
	seed int,
	scaleSetID ScaleSetID,
	messageID MessageID,
	claimKey int64,
	terminalState domain.ExecutionState,
	errorCode domain.ExecutionErrorCode,
) githubStartedTerminalRequeueFixture {
	t.Helper()
	ctx := context.Background()
	attempt, start := prepareGitHubStartDispatchForTest(
		t,
		controller,
		seed,
		scaleSetID,
		messageID,
		claimKey,
	)
	if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	for _, update := range []AgentExecutionUpdate{
		{
			NodeID:        start.NodeID,
			MessageID:     fmt.Sprintf("github-requeue-running-%d", claimKey),
			CommandID:     start.Command.ID,
			ExecutionID:   start.Command.ExecutionID,
			State:         domain.ExecutionRunning,
			PayloadDigest: digestForTest(fmt.Sprintf("github-requeue-running-%d", claimKey)),
		},
		{
			NodeID:        start.NodeID,
			MessageID:     fmt.Sprintf("github-requeue-terminal-%d", claimKey),
			CommandID:     start.Command.ID,
			ExecutionID:   start.Command.ExecutionID,
			State:         terminalState,
			ErrorCode:     errorCode,
			PayloadDigest: digestForTest(fmt.Sprintf("github-requeue-terminal-%d", claimKey)),
		},
	} {
		if replayed, err := controller.RecordAgentExecutionUpdate(
			ctx,
			update,
		); err != nil || replayed {
			t.Fatalf("record %s = (%t, %v)", update.State, replayed, err)
		}
	}
	reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pruned := NodeAgentSnapshot{
		NodeID:            start.NodeID,
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		RunnerVersion:     runner.OfficialRunnerVersion,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: start.Command.ControllerEpoch,
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, pruned); err != nil {
		t.Fatal(err)
	}
	snapshotDigest, err := nodeAgentSnapshotDigest(pruned)
	if err != nil {
		t.Fatal(err)
	}
	history, err := controller.ReconcileGitHubJITPrunedHistory(
		ctx,
		attempt,
		reconciliationEpoch,
		snapshotDigest,
	)
	if err != nil || !history.Started || history.LostTerminal != "" {
		t.Fatalf("reconcile started terminal history = (%#v, %v)", history, err)
	}
	current, found, err := controller.CurrentGitHubJITAttempt(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found || current.State != GitHubJITStarted {
		t.Fatalf("current started attempt = (%#v, %t, %v)", current, found, err)
	}
	claim, found, err := controller.GitHubClaim(
		ctx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found ||
		claim.State != GitHubClaimRunning ||
		claim.Execution.State != terminalState {
		t.Fatalf("current terminal claim = (%#v, %t, %v)", claim, found, err)
	}
	binding := SingleSlotBinding{
		TargetID:     claim.Execution.TargetID,
		NodeID:       claim.Execution.Slot.NodeID,
		Slot:         claim.Execution.Slot.Index,
		ClaimEnabled: true,
	}
	pollState, err := controller.ReadGitHubPollState(
		ctx,
		GitHubTargetRuntimeBinding{
			TargetID:   binding.TargetID,
			ScaleSetID: attempt.ScaleSetID,
			ProfileID: domain.RunnerProfileID(
				fmt.Sprintf("profile-%d", attempt.ScaleSetID),
			),
		},
		binding.NodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	pollState.ClaimAuthority.AdvertisedCapacity = 1
	binding.PollAuthority = pollState.ClaimAuthority
	return githubStartedTerminalRequeueFixture{
		attempt:             attempt,
		current:             current,
		start:               start,
		reconciliationEpoch: reconciliationEpoch,
		snapshotDigest:      snapshotDigest,
		claim:               claim,
		binding:             binding,
	}
}

func TestGitHubExactPickupAuthorityBlocksOnlyMatchingTerminalRequeue(t *testing.T) {
	tests := []struct {
		name          string
		eventType     GitHubJobEventType
		result        string
		runnerIDDelta int
		nameSuffix    string
		zeroRequest   bool
		wantIntent    bool
	}{
		{
			name:       "matching JobStarted",
			eventType:  GitHubJobStarted,
			wantIntent: false,
		},
		{
			name:       "matching JobCompleted",
			eventType:  GitHubJobCompleted,
			result:     GitHubJobResultSucceeded,
			wantIntent: false,
		},
		{
			name:        "matching zero-request JobStarted",
			eventType:   GitHubJobStarted,
			zeroRequest: true,
			wantIntent:  false,
		},
		{
			name:        "matching zero-request failed JobCompleted",
			eventType:   GitHubJobCompleted,
			result:      GitHubJobResultFailed,
			zeroRequest: true,
			wantIntent:  false,
		},
		{
			name:       "matching canceled JobCompleted is not pickup",
			eventType:  GitHubJobCompleted,
			result:     GitHubJobResultCanceled,
			wantIntent: true,
		},
		{
			name:        "matching zero-request canceled JobCompleted is not pickup",
			eventType:   GitHubJobCompleted,
			result:      GitHubJobResultCanceled,
			zeroRequest: true,
			wantIntent:  true,
		},
		{
			name:          "different runner",
			eventType:     GitHubJobStarted,
			runnerIDDelta: 1,
			nameSuffix:    "-different",
			wantIntent:    true,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			controller := openController(
				t,
				fmt.Sprintf("controller-github-pickup-authority-%d.db", index),
			)
			defer controller.Close()
			fixture := prepareGitHubStartedTerminalRequeueFixture(
				t,
				controller,
				index+1,
				ScaleSetID(71+index),
				MessageID(1001+index*10),
				int64(2101+index),
				domain.ExecutionReleased,
				domain.ExecutionErrorNone,
			)
			pickup := GitHubQueueMessage{
				ScaleSetID: fixture.current.ScaleSetID,
				MessageID:  MessageID(1002 + index*10),
				Digest:     digestForTest("github-pickup-authority-" + test.name),
				Jobs: []GitHubJobEvent{{
					Type:            test.eventType,
					RunnerRequestID: fixture.current.ClaimKey,
					RunnerID:        fixture.current.RunnerID + test.runnerIDDelta,
					RunnerName:      fixture.current.RunnerName + test.nameSuffix,
					Result:          test.result,
				}},
			}
			if test.zeroRequest {
				pickup.Jobs[0].RunnerRequestID = 0
			}
			if _, err := controller.CommitGitHubQueueMessage(
				ctx,
				pickup,
				fixture.binding,
			); err != nil {
				t.Fatal(err)
			}
			fresh := githubQueueMessageForTest(
				fixture.current.ScaleSetID,
				MessageID(1003+index*10),
				fixture.current.ClaimKey,
			)
			fresh.Jobs[0].ExecutionID = domain.ExecutionID(
				fmt.Sprintf("github-pickup-replacement-%d", index),
			)
			commit, err := controller.CommitGitHubQueueMessage(
				ctx,
				fresh,
				fixture.binding,
			)
			if err != nil {
				t.Fatal(err)
			}
			if (commit.RequeueIntent != nil) != test.wantIntent {
				t.Fatalf("requeue intent = %#v, want present %t",
					commit.RequeueIntent, test.wantIntent)
			}
			if test.wantIntent {
				if commit.Claim == nil ||
					commit.Claim.State != GitHubClaimReconciliationRequired {
					t.Fatalf("mismatched pickup claim = %#v", commit.Claim)
				}
				assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
				assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
				if capacity, err := controller.GitHubSingleSlotCapacity(
					ctx,
					fixture.binding,
				); err != nil || capacity != 0 {
					t.Fatalf("mismatched pickup capacity = (%d, %v), want zero",
						capacity, err)
				}
			} else {
				if commit.Claim == nil ||
					commit.Claim.State != GitHubClaimRunning ||
					commit.Claim.Execution.ID != fixture.claim.Execution.ID {
					t.Fatalf("exact pickup claim = %#v", commit.Claim)
				}
				assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
				assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
			}
		})
	}
}

func TestGitHubSameBatchZeroRequestPickupPreventsReplacementIntent(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-zero-pickup-same-batch.db")
	defer controller.Close()
	fixture := prepareGitHubStartedTerminalRequeueFixture(
		t,
		controller,
		6,
		131,
		3101,
		4101,
		domain.ExecutionReleased,
		domain.ExecutionErrorNone,
	)
	fresh := githubQueueMessageForTest(
		fixture.current.ScaleSetID,
		3102,
		fixture.current.ClaimKey,
	)
	fresh.Jobs[0].ExecutionID = "zero-pickup-same-batch-replacement"
	fresh.Jobs = append([]GitHubJobEvent{{
		Type:            GitHubJobStarted,
		RunnerRequestID: 0,
		RunnerID:        fixture.current.RunnerID,
		RunnerName:      fixture.current.RunnerName,
	}}, fresh.Jobs...)

	commit, err := controller.CommitGitHubQueueMessage(
		ctx,
		fresh,
		fixture.binding,
	)
	if err != nil {
		t.Fatal(err)
	}
	if commit.RequeueIntent != nil ||
		commit.Claim == nil ||
		commit.Claim.State != GitHubClaimRunning ||
		commit.Claim.Execution.ID != fixture.claim.Execution.ID {
		t.Fatalf("same-batch zero-request pickup commit = %#v", commit)
	}
	assertCount(
		t,
		controller.db,
		"SELECT count(*) FROM github_unpicked_requeue_intents",
		0,
	)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
}

func TestGitHubLatePickupDiscardsIntentAfterExactAbsence(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-late-pickup.db")
	defer controller.Close()
	fixture := prepareGitHubStartedTerminalRequeueFixture(
		t,
		controller,
		4,
		81,
		1101,
		2201,
		domain.ExecutionReleased,
		domain.ExecutionErrorNone,
	)
	fresh := githubQueueMessageForTest(
		fixture.current.ScaleSetID,
		1102,
		fixture.current.ClaimKey,
	)
	fresh.Jobs[0].ExecutionID = "github-late-pickup-replacement"
	commit, err := controller.CommitGitHubQueueMessage(
		ctx,
		fresh,
		fixture.binding,
	)
	if err != nil || commit.RequeueIntent == nil {
		t.Fatalf("create late-pickup intent = (%#v, %v)", commit, err)
	}
	health, err := controller.ReadGitHubScaleSetSessionHealth(
		ctx,
		fixture.current.ScaleSetID,
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(time.Hour)
	controller.now = func() time.Time { return now }
	if err := controller.MarkGitHubJITRemovalPending(
		ctx,
		fixture.current,
		fixture.reconciliationEpoch,
		fixture.snapshotDigest,
		health.TransitionGeneration,
		true,
	); err != nil {
		t.Fatal(err)
	}
	fixture.current.State = GitHubJITRemovalPending

	pickup := GitHubQueueMessage{
		ScaleSetID: fixture.current.ScaleSetID,
		MessageID:  1103,
		Digest:     digestForTest("github-late-exact-pickup"),
		Jobs: []GitHubJobEvent{{
			Type:            GitHubJobStarted,
			RunnerRequestID: 0,
			RunnerID:        fixture.current.RunnerID,
			RunnerName:      fixture.current.RunnerName,
		}},
	}
	if _, err := controller.CommitGitHubQueueMessage(
		ctx,
		pickup,
		fixture.binding,
	); err != nil {
		t.Fatal(err)
	}
	intent, found, err := controller.GitHubUnpickedRequeueIntent(
		ctx,
		fixture.current.ScaleSetID,
		fixture.current.ClaimKey,
	)
	if err != nil || !found || !intent.PickupProven {
		t.Fatalf("late pickup intent = (%#v, %t, %v)", intent, found, err)
	}
	if err := controller.MarkGitHubJITRemovalPending(
		ctx,
		fixture.current,
		fixture.reconciliationEpoch,
		fixture.snapshotDigest,
		health.TransitionGeneration,
		false,
	); err == nil {
		t.Fatal("late pickup authorized destructive provider removal")
	}

	now = now.Add(GitHubRunnerAbsenceConfirmationDelay)
	absence, err := controller.MarkGitHubJITReconciledAbsent(
		ctx,
		fixture.current,
		fixture.reconciliationEpoch,
		fixture.snapshotDigest,
		health.TransitionGeneration,
	)
	if err != nil {
		t.Fatal(err)
	}
	if absence.ReplacementExecution != nil ||
		absence.ReplacementClaimed ||
		absence.Claim.Execution.ID != fixture.claim.Execution.ID ||
		absence.Claim.State != GitHubClaimRunning {
		t.Fatalf("late pickup resolution = %#v", absence)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_unpicked_requeue_intents", 0)
	assertCount(t, controller.db, "SELECT count(*) FROM executions", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_acquire_attempts", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
}

func enableGitHubClaimForTest(
	t *testing.T,
	controller *ControllerStore,
	binding *SingleSlotBinding,
	scaleSetID ScaleSetID,
	architecture domain.Architecture,
) {
	t.Helper()
	ctx := context.Background()
	epoch, err := controller.EnrollmentEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := NodeAgentSnapshot{
		NodeID:            binding.NodeID,
		OS:                domain.OSLinux,
		Architecture:      architecture,
		RunnerVersion:     runner.OfficialRunnerVersion,
		NativeRunnerReady: true,
		Journal: AgentSnapshot{
			MaxControllerEpoch: epoch,
		},
	}
	if err := controller.RecordAgentSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	profileID := domain.RunnerProfileID(
		fmt.Sprintf("profile-%d", scaleSetID),
	)
	policy := RunnerProfileUpdatePolicy{
		ProfileID:     profileID,
		VersionPolicy: domain.RunnerVersionAutoUpdate,
		RunnerVersion: runner.OfficialRunnerVersion,
		Revision:      1,
	}
	if _, err := controller.ConfigureRunnerProfile(ctx, policy); err != nil {
		t.Fatal(err)
	}
	runtimeBinding := GitHubTargetRuntimeBinding{
		TargetID:   binding.TargetID,
		ScaleSetID: scaleSetID,
		ProfileID:  profileID,
	}
	if _, err := controller.ConfigureGitHubTargetRuntimeBinding(
		ctx,
		runtimeBinding,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.RecordGitHubScaleSetSessionSuccess(
		ctx,
		scaleSetID,
	); err != nil {
		t.Fatal(err)
	}
	state, err := controller.ReadGitHubPollState(
		ctx,
		runtimeBinding,
		binding.NodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	state.ClaimAuthority.AdvertisedCapacity = 1
	binding.PollAuthority = state.ClaimAuthority
}

func prepareGitHubStartDispatchForTest(
	t *testing.T,
	controller *ControllerStore,
	seed int,
	scaleSetID ScaleSetID,
	messageID MessageID,
	claimKey int64,
) (GitHubJITAttempt, IssuedAgentCommand) {
	t.Helper()
	return prepareGitHubStartDispatchForArchTest(
		t,
		controller,
		seed,
		scaleSetID,
		messageID,
		claimKey,
		domain.ArchAMD64,
	)
}

func prepareGitHubStartDispatchForArchTest(
	t *testing.T,
	controller *ControllerStore,
	seed int,
	scaleSetID ScaleSetID,
	messageID MessageID,
	claimKey int64,
	architecture domain.Architecture,
) (GitHubJITAttempt, IssuedAgentCommand) {
	t.Helper()
	ctx := context.Background()
	nodeID, epoch := enrollControllerAgentNode(t, controller, seed)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	enableGitHubClaimForTest(
		t,
		controller,
		&binding,
		scaleSetID,
		architecture,
	)
	message := githubQueueMessageForTest(scaleSetID, messageID, claimKey)
	if _, err := controller.CommitGitHubQueueMessage(ctx, message, binding); err != nil {
		t.Fatal(err)
	}
	acquire, err := controller.BeginGitHubAcquire(ctx, scaleSetID, claimKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubAcquired(ctx, acquire); err != nil {
		t.Fatal(err)
	}

	executionID := message.Jobs[0].ExecutionID
	prepare := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              domain.CommandID("github-prepare-" + executionID),
			ControllerEpoch: epoch,
			ExecutionID:     executionID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   digestForTest("github-prepare-" + string(executionID)),
		},
	}
	if replayed, err := controller.CommitAgentCommand(ctx, prepare); err != nil || replayed {
		t.Fatalf("commit prepare = (%t, %v)", replayed, err)
	}
	preparing := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "github-preparing-" + string(executionID),
		CommandID:     prepare.Command.ID,
		ExecutionID:   executionID,
		State:         domain.ExecutionPreparing,
		PayloadDigest: digestForTest("github-preparing-" + string(executionID)),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(ctx, preparing); err != nil || replayed {
		t.Fatalf("record preparing = (%t, %v)", replayed, err)
	}
	if err := controller.MarkGitHubPreparing(ctx, scaleSetID, claimKey); err != nil {
		t.Fatal(err)
	}

	attempt, replayed, err := controller.BeginGitHubJITAttempt(
		ctx, scaleSetID, claimKey, epoch, "sparerunner-fast-terminal")
	if err != nil || replayed {
		t.Fatalf("begin JIT = (%#v, %t, %v)", attempt, replayed, err)
	}
	const runnerID = 81
	jitDigest := digestForTest("github-jit-" + string(executionID))
	startCommandID := domain.CommandID("github-start-" + executionID)
	if err := controller.MarkGitHubJITGenerated(
		ctx, attempt, runnerID, jitDigest, startCommandID); err != nil {
		t.Fatal(err)
	}
	attempt.RunnerID = runnerID
	attempt.JITDigest = jitDigest
	attempt.StartCommandID = startCommandID
	attempt.State = GitHubJITGenerated
	if err := controller.BeginGitHubStartDispatch(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.State = GitHubJITStartDispatching

	start := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandStart,
		Command: domain.Command{
			ID:              startCommandID,
			ControllerEpoch: epoch,
			ExecutionID:     executionID,
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   digestForTest("github-start-" + string(executionID)),
		},
	}
	if replayed, err := controller.CommitAgentCommand(ctx, start); err != nil || replayed {
		t.Fatalf("commit start = (%t, %v)", replayed, err)
	}
	return attempt, start
}

func prepareGitHubClaimForJITTest(
	t *testing.T,
	controller *ControllerStore,
	seed int,
	scaleSetID ScaleSetID,
	messageID MessageID,
	claimKey int64,
) (string, domain.ControllerEpoch) {
	t.Helper()
	ctx := context.Background()
	nodeID, epoch := enrollControllerAgentNode(t, controller, seed)
	binding := SingleSlotBinding{
		TargetID:     "target-github",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	enableGitHubClaimForTest(
		t,
		controller,
		&binding,
		scaleSetID,
		domain.ArchAMD64,
	)
	message := githubQueueMessageForTest(scaleSetID, messageID, claimKey)
	if _, err := controller.CommitGitHubQueueMessage(ctx, message, binding); err != nil {
		t.Fatal(err)
	}
	acquire, err := controller.BeginGitHubAcquire(ctx, scaleSetID, claimKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubAcquired(ctx, acquire); err != nil {
		t.Fatal(err)
	}
	prepare := IssuedAgentCommand{
		NodeID: domain.NodeID(nodeID),
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              domain.CommandID("github-prepare-" + message.Jobs[0].ExecutionID),
			ControllerEpoch: epoch,
			ExecutionID:     message.Jobs[0].ExecutionID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   digestForTest("github-prepare-" + string(message.Jobs[0].ExecutionID)),
		},
	}
	if replayed, err := controller.CommitAgentCommand(ctx, prepare); err != nil || replayed {
		t.Fatalf("commit prepare = (%t, %v)", replayed, err)
	}
	preparing := AgentExecutionUpdate{
		NodeID:        domain.NodeID(nodeID),
		MessageID:     "github-preparing-" + string(message.Jobs[0].ExecutionID),
		CommandID:     prepare.Command.ID,
		ExecutionID:   message.Jobs[0].ExecutionID,
		State:         domain.ExecutionPreparing,
		PayloadDigest: digestForTest("github-preparing-" + string(message.Jobs[0].ExecutionID)),
	}
	if replayed, err := controller.RecordAgentExecutionUpdate(ctx, preparing); err != nil || replayed {
		t.Fatalf("record preparing = (%t, %v)", replayed, err)
	}
	if err := controller.MarkGitHubPreparing(ctx, scaleSetID, claimKey); err != nil {
		t.Fatal(err)
	}
	return nodeID, epoch
}

func githubQueueMessageForTest(
	scaleSetID ScaleSetID,
	messageID MessageID,
	claimKey int64,
) GitHubQueueMessage {
	return GitHubQueueMessage{
		ScaleSetID: scaleSetID,
		MessageID:  messageID,
		Digest:     digestForTest("github-message-" + string(rune(messageID))),
		Jobs: []GitHubJobEvent{{
			Type:            GitHubJobAvailable,
			RunnerRequestID: claimKey,
			ExecutionID:     domain.ExecutionID("github-execution-" + string(rune(claimKey))),
		}},
	}
}
