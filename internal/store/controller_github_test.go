package store

import (
	"context"
	"errors"
	"testing"

	"github.com/genm/tewake/internal/domain"
)

func TestGitHubQueueMessageKeepsMixedEventsSeparateFromSingleSlotClaim(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "controller-github-mixed.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 3)
	binding := SingleSlotBinding{TargetID: "target-github", NodeID: domain.NodeID(nodeID), Slot: 0, ClaimEnabled: true}
	message := githubQueueMessageForTest(7, 101, 501)
	message.Jobs = append(message.Jobs,
		GitHubJobEvent{Type: GitHubJobAssigned, RunnerRequestID: 502},
		GitHubJobEvent{Type: GitHubJobStarted, RunnerRequestID: 503, RunnerID: 33, RunnerName: "tewake-existing"},
		GitHubJobEvent{Type: GitHubJobCompleted, RunnerRequestID: 504, RunnerID: 34, RunnerName: "tewake-complete", Result: "succeeded"},
	)

	committed, err := controller.CommitGitHubQueueMessage(ctx, message, binding)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Replayed || committed.Claim == nil ||
		committed.Claim.RunnerRequestID != 501 ||
		committed.Claim.Execution.State != domain.ExecutionReserved {
		t.Fatalf("commit = %#v", committed)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_queue_messages", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM github_message_jobs", 4)
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
	assertCount(t, controller.db, "SELECT count(*) FROM github_message_jobs", 4)
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
	if duplicate.Claim == nil || duplicate.Claim.RunnerRequestID != 602 ||
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
		if err := controller.BeginGitHubAcquire(ctx, 31, 1701); !errors.Is(err, ErrGitHubClaimState) {
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
			ctx, 32, 1702, epoch, "tewake-jit-admission",
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
			ctx, 33, 1703, epoch, "tewake-start-admission")
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
		claim, found, err := controller.GitHubClaim(ctx, attempt.ScaleSetID, attempt.RunnerRequestID)
		if err != nil || !found || claim.State != GitHubClaimJITGenerated {
			t.Fatalf("claim after rejected start = (%#v, %t, %v)", claim, found, err)
		}
		current, found, err := controller.CurrentGitHubJITAttempt(
			ctx, attempt.ScaleSetID, attempt.RunnerRequestID)
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
		replayed.Claim == nil || replayed.Claim.RunnerRequestID != 604 {
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
	message := githubQueueMessageForTest(9, 301, 701)
	if _, err := controller.CommitGitHubQueueMessage(ctx, message, binding); err != nil {
		t.Fatal(err)
	}
	if err := controller.BeginGitHubAcquire(ctx, 9, 701); err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubAcquired(ctx, 9, 701); err != nil {
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
		ctx, 9, 701, epoch, "tewake-deterministic")
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
	message := githubQueueMessageForTest(11, 401, 801)
	if _, err := controller.CommitGitHubQueueMessage(ctx, message, binding); err != nil {
		t.Fatal(err)
	}
	if err := controller.BeginGitHubAcquire(ctx, 11, 801); err != nil {
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
	message2 := githubQueueMessageForTest(12, 402, 802)
	if _, err := controller2.CommitGitHubQueueMessage(ctx, message2, binding2); err != nil {
		t.Fatal(err)
	}
	if err := controller2.BeginGitHubAcquire(ctx, 12, 802); err != nil {
		t.Fatal(err)
	}
	if err := controller2.MarkGitHubAcquired(ctx, 12, 802); err != nil {
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
		ctx, 12, 802, epoch2, "tewake-ambiguous")
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
		ctx, 12, 802, epoch2, "tewake-ambiguous")
	if err != nil || !replay || replayed.State != GitHubJITGenerationAmbiguous {
		t.Fatalf("ambiguous JIT replay = (%#v, %t, %v)", replayed, replay, err)
	}
	if err := controller2.MarkGitHubJITReconciledAbsent(
		ctx, attempt, epoch2); !errors.Is(err, ErrStaleControllerEpoch) {
		t.Fatalf("same-epoch JIT reconciliation = %v", err)
	}
	reconciliationEpoch, err := controller2.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller2.MarkGitHubJITReconciledAbsent(
		ctx, attempt, reconciliationEpoch); err != nil {
		t.Fatal(err)
	}
	next, replay, err := controller2.BeginGitHubJITAttempt(
		ctx, 12, 802, reconciliationEpoch, "tewake-ambiguous")
	if err != nil || replay || next.Attempt != 2 ||
		next.ControllerEpoch != reconciliationEpoch {
		t.Fatalf("post-reconciliation JIT = (%#v, %t, %v)", next, replay, err)
	}
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
		ctx, attempt.ScaleSetID, attempt.RunnerRequestID)
	if err != nil || !found || claim.State != GitHubClaimRunning ||
		claim.Execution.State != domain.ExecutionRunning {
		t.Fatalf("running claim = (%#v, %t, %v)", claim, found, err)
	}
	current, found, err := controller.CurrentGitHubJITAttempt(
		ctx, attempt.ScaleSetID, attempt.RunnerRequestID)
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
	if err := controller.MarkGitHubRunning(ctx, attempt); !errors.Is(err, ErrGitHubClaimState) {
		t.Fatalf("missing Running observation error = %v, want ErrGitHubClaimState", err)
	}

	claim, found, err := controller.GitHubClaim(
		ctx, attempt.ScaleSetID, attempt.RunnerRequestID)
	if err != nil || !found ||
		claim.State != GitHubClaimStartDispatching ||
		claim.Execution.State != domain.ExecutionPreparing {
		t.Fatalf("unchanged claim = (%#v, %t, %v)", claim, found, err)
	}
	current, found, err := controller.CurrentGitHubJITAttempt(
		ctx, attempt.ScaleSetID, attempt.RunnerRequestID)
	if err != nil || !found || current.State != GitHubJITStartDispatching {
		t.Fatalf("unchanged attempt = (%#v, %t, %v)", current, found, err)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)
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
				ctx, original.ScaleSetID, original.RunnerRequestID)
			if err != nil || !found || current != original {
				t.Fatalf("attempt changed after rejected transition = (%#v, %t, %v), want %#v",
					current, found, err, original)
			}
			claim, found, err := controller.GitHubClaim(
				ctx, original.ScaleSetID, original.RunnerRequestID)
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
			ctx, attempt.ScaleSetID, attempt.RunnerRequestID)
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
			WHERE scale_set_id = ? AND runner_request_id = ?`,
			GitHubClaimReconciliationRequired, attempt.ScaleSetID, attempt.RunnerRequestID); err != nil {
			t.Fatal(err)
		}
		if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); !errors.Is(err, ErrGitHubClaimState) {
			t.Fatalf("claim state substitution error = %v, want ErrGitHubClaimState", err)
		}
		current, found, err := controller.CurrentGitHubJITAttempt(
			ctx, attempt.ScaleSetID, attempt.RunnerRequestID)
		if err != nil || !found || current.State != GitHubJITStartDispatching {
			t.Fatalf("attempt after claim substitution = (%#v, %t, %v)", current, found, err)
		}
	})
}

func TestGitHubStartedTransitionConvergesFastTerminalExecutionAtomically(t *testing.T) {
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
			seed:      2,
			messageID: 502,
			requestID: 902,
		},
		{
			name:      "failed",
			state:     domain.ExecutionFailed,
			errorCode: domain.ExecutionErrorStart,
			seed:      3,
			messageID: 503,
			requestID: 903,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			controller := openController(t, "controller-github-fast-terminal-"+test.name+".db")
			defer controller.Close()

			attempt, start := prepareGitHubStartDispatchForTest(
				t, controller, test.seed, 22, test.messageID, test.requestID)
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

			// The Agent persists and reports its terminal cleanup result before
			// the Controller can durably project the returned Running ACK.
			terminal := AgentExecutionUpdate{
				NodeID:        start.NodeID,
				MessageID:     "github-start-terminal-" + test.name,
				CommandID:     start.Command.ID,
				ExecutionID:   start.Command.ExecutionID,
				State:         test.state,
				ErrorCode:     test.errorCode,
				PayloadDigest: digestForTest("github-start-terminal-" + test.name),
			}
			if replayed, err := controller.RecordAgentExecutionUpdate(ctx, terminal); err != nil || replayed {
				t.Fatalf("record terminal = (%t, %v)", replayed, err)
			}
			assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)

			if err := controller.MarkGitHubRunning(ctx, attempt); err != nil {
				t.Fatal(err)
			}

			claim, found, err := controller.GitHubClaim(
				ctx, attempt.ScaleSetID, attempt.RunnerRequestID)
			if err != nil || !found ||
				claim.State != GitHubClaimReconciliationRequired ||
				claim.Execution.State != test.state {
				t.Fatalf("terminal claim = (%#v, %t, %v)", claim, found, err)
			}
			current, found, err := controller.CurrentGitHubJITAttempt(
				ctx, attempt.ScaleSetID, attempt.RunnerRequestID)
			if err != nil || !found || current.State != GitHubJITStarted {
				t.Fatalf("started terminal attempt = (%#v, %t, %v)", current, found, err)
			}
			assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
		})
	}
}

func prepareGitHubStartDispatchForTest(
	t *testing.T,
	controller *ControllerStore,
	seed int,
	scaleSetID ScaleSetID,
	messageID MessageID,
	runnerRequestID int64,
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
	message := githubQueueMessageForTest(scaleSetID, messageID, runnerRequestID)
	if _, err := controller.CommitGitHubQueueMessage(ctx, message, binding); err != nil {
		t.Fatal(err)
	}
	if err := controller.BeginGitHubAcquire(ctx, scaleSetID, runnerRequestID); err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubAcquired(ctx, scaleSetID, runnerRequestID); err != nil {
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
	if err := controller.MarkGitHubPreparing(ctx, scaleSetID, runnerRequestID); err != nil {
		t.Fatal(err)
	}

	attempt, replayed, err := controller.BeginGitHubJITAttempt(
		ctx, scaleSetID, runnerRequestID, epoch, "tewake-fast-terminal")
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
	runnerRequestID int64,
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
	message := githubQueueMessageForTest(scaleSetID, messageID, runnerRequestID)
	if _, err := controller.CommitGitHubQueueMessage(ctx, message, binding); err != nil {
		t.Fatal(err)
	}
	if err := controller.BeginGitHubAcquire(ctx, scaleSetID, runnerRequestID); err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubAcquired(ctx, scaleSetID, runnerRequestID); err != nil {
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
	if err := controller.MarkGitHubPreparing(ctx, scaleSetID, runnerRequestID); err != nil {
		t.Fatal(err)
	}
	return nodeID, epoch
}

func githubQueueMessageForTest(
	scaleSetID ScaleSetID,
	messageID MessageID,
	runnerRequestID int64,
) GitHubQueueMessage {
	return GitHubQueueMessage{
		ScaleSetID: scaleSetID,
		MessageID:  messageID,
		Digest:     digestForTest("github-message-" + string(rune(messageID))),
		Jobs: []GitHubJobEvent{{
			Type:            GitHubJobAvailable,
			RunnerRequestID: runnerRequestID,
			ExecutionID:     domain.ExecutionID("github-execution-" + string(rune(runnerRequestID))),
		}},
	}
}
