package app

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/reconcile"
	"github.com/genm/sparerunner/internal/runner"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

func TestControllerRunnerNormalVerticalCommitsMixedMessageBeforeAck(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	session.beforeDelete = func() {
		if stateStore.claim == nil || stateStore.claim.State != store.GitHubClaimPending {
			t.Fatalf("GitHub ack preceded durable claim: %#v", stateStore.claim)
		}
	}
	coordinator := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)

	message, err := coordinator.PollAndDriveOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if message == nil || message.ID != 41 {
		t.Fatalf("message = %#v", message)
	}
	if stateStore.commits != 1 || stateStore.events != 4 || session.deleteCalls != 1 {
		t.Fatalf("commits/events/delete = %d/%d/%d", stateStore.commits, stateStore.events, session.deleteCalls)
	}
	if session.acquireCalls != 1 || lifecycle.generateCalls != 1 ||
		agent.prepareCalls != 1 || agent.startCalls != 1 {
		t.Fatalf("acquire/generate/prepare/start = %d/%d/%d/%d",
			session.acquireCalls, lifecycle.generateCalls, agent.prepareCalls, agent.startCalls)
	}
	if stateStore.claim == nil || stateStore.claim.State != store.GitHubClaimRunning {
		t.Fatalf("claim = %#v, want running", stateStore.claim)
	}
	if agent.delivered != "opaque-jit-canary" {
		t.Fatalf("delivered JIT = %q, want opaque canary", agent.delivered)
	}
}

func TestControllerRunnerUsesExactFenceTokensAgainstRealProjection(t *testing.T) {
	const (
		nodeID domain.NodeID          = "00000000000000000000000000000001"
		epoch  domain.ControllerEpoch = 3
	)
	projection, err := reconcile.Restore(
		epoch,
		store.ControllerSnapshot{
			ControllerEpoch: epoch,
			Nodes: []store.NodeAdministration{{
				NodeID: nodeID,
				State:  domain.NodeActive,
			}},
		},
		reconcile.Config{
			Nodes: []reconcile.NodeDefinition{{
				Node: domain.Node{
					ID:                  nodeID,
					DisplayName:         "test-node",
					OS:                  domain.OSLinux,
					Architecture:        domain.ArchAMD64,
					MaxRunners:          1,
					AdministrativeState: domain.NodeActive,
					ObservedState:       domain.NodeOffline,
				},
				RunnerVersionPolicy: domain.RunnerVersionAutoUpdate,
				RunnerUpdate:        reconcile.ManagedRunnerUpdate(),
			}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	agentSnapshot := AgentSnapshot{
		NodeID:            nodeID,
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		RunnerVersion:     runner.OfficialRunnerVersion,
		NativeRunnerReady: true,
	}
	if _, err := projection.ReconcileAgentSnapshot(agentSnapshot); err != nil {
		t.Fatal(err)
	}
	reconciler := &recordingRealControllerRunnerReconciler{
		controller: projection,
	}
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState:   domain.ExecutionPreparing,
		startState:     domain.ExecutionRunning,
		snapshot:       agentSnapshot,
		snapshotOnline: true,
	}
	coordinator := newControllerRunnerForEpochWithReconciler(
		t,
		stateStore,
		session,
		agent,
		newRunnerCoordinatorFakeLifecycle(),
		epoch,
		reconciler,
	)
	// Committing the claim deliberately invalidates the admission snapshot
	// captured by the long poll. PollAndDriveOnce therefore leaves the durable
	// claim for the next drive instead of acting under the stale snapshot.
	if _, err := coordinator.PollAndDriveOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	drove, err := coordinator.DriveNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !drove {
		t.Fatal("durable claim was not driven after admission invalidation")
	}

	reconciler.mu.Lock()
	applied := append([]reconcile.GitHubFence(nil), reconciler.applied...)
	cleared := append([]reconcile.GitHubFence(nil), reconciler.cleared...)
	reconciler.mu.Unlock()
	if len(applied) != 4 || len(cleared) != 2 ||
		applied[0].ClaimState != store.GitHubClaimAcquireAmbiguous ||
		applied[3].ClaimState != store.GitHubClaimStartDispatching ||
		!reflect.DeepEqual(cleared[0], applied[0]) ||
		!reflect.DeepEqual(cleared[1], applied[3]) {
		t.Fatalf("applied/cleared fence tokens = %#v / %#v", applied, cleared)
	}
	if err := projection.ApplyGitHubFence(applied[2]); err == nil {
		t.Fatal("delayed generated Apply resurrected a cleared start fence")
	}
	if err := projection.ClearGitHubFence(applied[2]); err == nil {
		t.Fatal("delayed generated Clear matched a newer cleared fence")
	}
	admission, err := projection.Admission(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range admission.Actions {
		switch action.Kind {
		case reconcile.ActionObserveGitHubClaim,
			reconcile.ActionObserveGitHubRunner,
			reconcile.ActionConfirmAgentStartAccepted,
			reconcile.ActionAwaitAgentObservation:
			t.Fatalf("delayed token restored provider suppression: %#v", action)
		}
	}
}

func TestControllerRunnerMessageReplayAfterAckFailureDoesNotDuplicateClaim(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	session.deleteErrors = []error{errors.New("ack unavailable"), nil}
	coordinator := newControllerRunnerForTest(
		t, stateStore, session, &runnerCoordinatorFakeAgent{},
		newRunnerCoordinatorFakeLifecycle())

	if _, err := coordinator.PollOnce(context.Background()); err == nil {
		t.Fatal("ack failure was hidden")
	}
	if stateStore.commits != 1 || stateStore.claim == nil ||
		stateStore.claim.State != store.GitHubClaimPending {
		t.Fatalf("commit after failed ack = %d, claim %#v", stateStore.commits, stateStore.claim)
	}
	if stateStore.runtimeFreshness.Session.Freshness != store.RuntimeFreshnessStale ||
		len(stateStore.sessionFailures) != 1 {
		t.Fatalf(
			"session after failed ack = %#v, failures %v",
			stateStore.runtimeFreshness.Session,
			stateStore.sessionFailures,
		)
	}
	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stateStore.commits != 1 || stateStore.replays != 1 || session.deleteCalls != 2 ||
		len(session.pollCapacities) != 2 || session.pollCapacities[1] != 0 ||
		stateStore.runtimeFreshness.Session.Freshness != store.RuntimeFreshnessFresh {
		t.Fatalf(
			"commits/replays/deletes/capacities/session = %d/%d/%d/%v/%#v",
			stateStore.commits,
			stateStore.replays,
			session.deleteCalls,
			session.pollCapacities,
			stateStore.runtimeFreshness.Session,
		)
	}
	if stateStore.claim.ClaimKey != 7001 {
		t.Fatalf("replayed claim = %#v", stateStore.claim)
	}
}

func TestControllerRunnerStableReplayIdentityIgnoresVolatileStatisticsUnderRace(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		newRunnerCoordinatorFakeSession(nil),
		&runnerCoordinatorFakeAgent{},
		newRunnerCoordinatorFakeLifecycle(),
	)
	pollState, err := stateStore.ReadGitHubPollState(
		context.Background(),
		coordinator.runtimeBinding(),
		coordinator.config.NodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	pollState.ClaimAuthority.AdvertisedCapacity = 1
	coordinator.setPollScope(controllerRunnerPollScope{
		authority: pollState.ClaimAuthority,
		nodeID:    coordinator.config.NodeID,
		claimable: true,
	})
	defer coordinator.clearPollScope()
	first := *testControllerRunnerMessage()
	first.Jobs = append([]github.JobMessage(nil), first.Jobs...)
	second := first
	second.Jobs = append([]github.JobMessage(nil), first.Jobs...)
	second.Statistics = github.Statistics{
		TotalAvailableJobs:     91,
		TotalAcquiredJobs:      82,
		TotalAssignedJobs:      73,
		TotalRunningJobs:       64,
		TotalRegisteredRunners: 55,
		TotalBusyRunners:       46,
		TotalIdleRunners:       37,
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	for _, message := range []github.Message{first, second} {
		go func() {
			<-start
			results <- coordinator.CommitMessage(context.Background(), message)
		}()
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("same wire message replay error = %v", err)
		}
	}

	stateStore.mu.Lock()
	storedJobCount := -1
	if stateStore.message != nil {
		storedJobCount = len(stateStore.message.Jobs)
	}
	if stateStore.commits != 1 || stateStore.replays != 1 ||
		stateStore.events != len(first.Jobs) || stateStore.message == nil ||
		storedJobCount != len(first.Jobs) || stateStore.claim == nil {
		t.Fatalf("stable replay commits/replays/events/jobs/claim = %d/%d/%d/%d/%#v",
			stateStore.commits,
			stateStore.replays,
			stateStore.events,
			storedJobCount,
			stateStore.claim,
		)
	}
	executionID := stateStore.claim.Execution.ID
	claimKey := stateStore.claim.ClaimKey
	storedDigest := stateStore.message.Digest
	stateStore.mu.Unlock()
	if executionID != deterministicExecutionID(
		first.ScaleSetID,
		first.ID,
		first.Jobs[0].RunnerRequestID,
	) ||
		claimKey != first.Jobs[0].RunnerRequestID {
		t.Fatalf("stable replay execution/request = %s/%d", executionID, claimKey)
	}

	changed := first
	changed.Jobs = append([]github.JobMessage(nil), first.Jobs...)
	changed.Jobs[0].RepositoryName = "meaningfully-different-repository"
	if err := coordinator.CommitMessage(context.Background(), changed); !errors.Is(err, store.ErrReplayMismatch) {
		t.Fatalf("changed Jobs replay error = %v, want ErrReplayMismatch", err)
	}
	stateStore.mu.Lock()
	defer stateStore.mu.Unlock()
	if stateStore.commits != 1 || stateStore.replays != 1 ||
		stateStore.events != len(first.Jobs) || len(stateStore.message.Jobs) != len(first.Jobs) ||
		stateStore.message.Digest != storedDigest || stateStore.claim == nil ||
		stateStore.claim.Execution.ID != executionID ||
		stateStore.claim.ClaimKey != claimKey {
		t.Fatalf("changed Jobs mutated durable replay state: commits=%d replays=%d events=%d jobs=%d message=%#v claim=%#v",
			stateStore.commits,
			stateStore.replays,
			stateStore.events,
			len(stateStore.message.Jobs),
			stateStore.message,
			stateStore.claim,
		)
	}
}

func TestControllerRunnerRestartRecoversAckedPendingClaimBeforeAnotherPoll(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	firstSession := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	first := newControllerRunnerForTest(
		t, stateStore, firstSession, &runnerCoordinatorFakeAgent{},
		newRunnerCoordinatorFakeLifecycle())
	if _, err := first.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stateStore.claim == nil || stateStore.claim.State != store.GitHubClaimPending {
		t.Fatalf("acked pending claim = %#v", stateStore.claim)
	}

	restartedSession := newRunnerCoordinatorFakeSession(nil)
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	restarted := newControllerRunnerForTest(t, stateStore, restartedSession, agent, lifecycle)
	drove, err := restarted.DriveNext(context.Background())
	if err != nil || !drove {
		t.Fatalf("DriveNext after restart = (%t, %v)", drove, err)
	}
	if restartedSession.acquireCalls != 1 || stateStore.claim.State != store.GitHubClaimRunning {
		t.Fatalf("restart acquire/state = %d/%s", restartedSession.acquireCalls, stateStore.claim.State)
	}
}

func TestControllerRunnerRunOwnsPollFirstLoopUntilCancellation(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	coordinator := newControllerRunnerForTest(
		t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())
	ctx, cancel := context.WithCancel(context.Background())
	session.afterPoll = func() {
		session.afterPoll = nil
		cancel()
	}
	if err := coordinator.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if stateStore.commits != 1 || session.deleteCalls != 1 ||
		agent.startCalls != 1 {
		t.Fatalf("run commits/deletes/starts = %d/%d/%d",
			stateStore.commits, session.deleteCalls, agent.startCalls)
	}
}

func TestControllerRunnerAcquireAmbiguityNeverPrepares(t *testing.T) {
	tests := []struct {
		name     string
		acquired []int64
		err      error
	}{
		{name: "empty response", acquired: []int64{}},
		{name: "transport error", err: errors.New("transport unavailable")},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			stateStore := newRunnerCoordinatorFakeStore()
			session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
			session.acquired = testCase.acquired
			session.acquireErr = testCase.err
			agent := &runnerCoordinatorFakeAgent{prepareState: domain.ExecutionPreparing}
			lifecycle := newRunnerCoordinatorFakeLifecycle()
			coordinator := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)
			if _, err := coordinator.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubAcquireAmbiguous) {
				t.Fatalf("error = %v, want ErrGitHubAcquireAmbiguous", err)
			}
			if stateStore.claim.State != store.GitHubClaimAcquireAmbiguous {
				t.Fatalf("claim state = %q", stateStore.claim.State)
			}
			if agent.prepareCalls != 0 || lifecycle.generateCalls != 0 {
				t.Fatalf("prepare/generate after ambiguous acquire = %d/%d", agent.prepareCalls, lifecycle.generateCalls)
			}
			if drove, err := coordinator.DriveNext(context.Background()); err != nil || drove {
				t.Fatalf("ambiguous claim became actionable = (%t, %v)", drove, err)
			}
		})
	}
}

func TestControllerRunnerOfflineAgentAdvertisesAndClaimsNoCapacity(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		snapshot: AgentSnapshot{
			NodeID:            "00000000000000000000000000000001",
			OS:                domain.OSLinux,
			Arch:              domain.ArchAMD64,
			NativeRunnerReady: true,
		},
		snapshotOnline: false,
	}
	coordinator := newControllerRunnerForTest(
		t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())
	if _, err := coordinator.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubAvailableUnclaimed) {
		t.Fatalf("offline available error = %v", err)
	}
	if stateStore.message == nil || stateStore.claim != nil {
		t.Fatalf("offline commit/claim = (%#v, %#v)", stateStore.message, stateStore.claim)
	}
	if session.deleteCalls != 0 || session.acquireCalls != 0 || agent.prepareCalls != 0 {
		t.Fatalf("offline delete/acquire/prepare = %d/%d/%d", session.deleteCalls, session.acquireCalls, agent.prepareCalls)
	}
}

func TestControllerRunnerOnlineAgentWithoutNativeRuntimeAdvertisesNoCapacity(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		snapshot: AgentSnapshot{
			NodeID: "00000000000000000000000000000001",
			OS:     domain.OSLinux,
			Arch:   domain.ArchAMD64,
		},
		snapshotOnline: true,
	}
	coordinator := newControllerRunnerForTest(
		t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())
	if _, err := coordinator.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubAvailableUnclaimed) {
		t.Fatalf("runtime-unavailable error = %v", err)
	}
	if len(session.pollCapacities) != 1 || session.pollCapacities[0] != 0 {
		t.Fatalf("runtime-unavailable capacities = %v, want [0]", session.pollCapacities)
	}
	if stateStore.claim != nil || session.deleteCalls != 0 ||
		session.acquireCalls != 0 || agent.prepareCalls != 0 {
		t.Fatalf("runtime-unavailable claim/delete/acquire/prepare = %#v/%d/%d/%d",
			stateStore.claim, session.deleteCalls, session.acquireCalls, agent.prepareCalls)
	}
}

func TestControllerRunnerAuditFailureAdvertisesNoCapacityAndBlocksLateAcquire(t *testing.T) {
	t.Run("failed before poll", func(t *testing.T) {
		stateStore := newRunnerCoordinatorFakeStore()
		stateStore.auditHealthy = false
		session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
		agent := &runnerCoordinatorFakeAgent{
			prepareState: domain.ExecutionPreparing,
			startState:   domain.ExecutionRunning,
		}
		agent.setNativeRunnerReady(true)
		coordinator := newControllerRunnerForTest(
			t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())

		if _, err := coordinator.PollAndDriveOnce(context.Background()); !errors.Is(
			err,
			ErrControllerRunnerAdmission,
		) {
			t.Fatalf("audit-unavailable poll error = %v", err)
		}
		if len(session.pollCapacities) != 1 || session.pollCapacities[0] != 0 {
			t.Fatalf("audit-unavailable capacities = %v, want [0]", session.pollCapacities)
		}
		if stateStore.claim != nil || session.acquireCalls != 0 ||
			agent.prepareCalls != 0 || agent.startCalls != 0 {
			t.Fatalf(
				"audit-unavailable claim/acquire/prepare/start = %#v/%d/%d/%d",
				stateStore.claim,
				session.acquireCalls,
				agent.prepareCalls,
				agent.startCalls,
			)
		}
	})

	t.Run("failed during long poll", func(t *testing.T) {
		stateStore := newRunnerCoordinatorFakeStore()
		session := newRunnerCoordinatorFakeSession(nil)
		started := make(chan int, 1)
		session.poll = func(ctx context.Context, capacity int) (*github.Message, error) {
			started <- capacity
			<-ctx.Done()
			return nil, ctx.Err()
		}
		agent := &runnerCoordinatorFakeAgent{
			snapshot: AgentSnapshot{
				NodeID: "00000000000000000000000000000001",
				OS:     domain.OSLinux,
				Arch:   domain.ArchAMD64,
			},
			snapshotOnline: true,
		}
		agent.setNativeRunnerReady(true)
		coordinator := newControllerRunnerForTest(
			t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())

		result := make(chan error, 1)
		go func() {
			_, err := coordinator.PollAndDriveOnce(context.Background())
			result <- err
		}()
		select {
		case capacity := <-started:
			if capacity != 1 {
				t.Fatalf("initial audit-healthy capacity = %d, want 1", capacity)
			}
		case <-time.After(time.Second):
			t.Fatal("GitHub long poll did not start")
		}
		stateStore.degradeManagementAudit()
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("intentional audit cancellation = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("audit change did not interrupt GitHub long poll")
		}
		if stateStore.claim != nil || stateStore.commits != 0 ||
			session.deleteCalls != 0 || session.acquireCalls != 0 {
			t.Fatalf(
				"audit cancellation claim/commit/delete/acquire = %#v/%d/%d/%d",
				stateStore.claim,
				stateStore.commits,
				session.deleteCalls,
				session.acquireCalls,
			)
		}

		session.mu.Lock()
		session.poll = func(context.Context, int) (*github.Message, error) {
			return nil, nil
		}
		session.mu.Unlock()
		if _, err := coordinator.PollOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		session.mu.Lock()
		capacities := append([]int(nil), session.pollCapacities...)
		session.mu.Unlock()
		if len(capacities) != 2 || capacities[1] != 0 {
			t.Fatalf("audit-degraded capacities = %v, want [1 0]", capacities)
		}
	})

	t.Run("failed after poll before commit", func(t *testing.T) {
		stateStore := newRunnerCoordinatorFakeStore()
		session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
		agent := &runnerCoordinatorFakeAgent{
			prepareState: domain.ExecutionPreparing,
			startState:   domain.ExecutionRunning,
		}
		agent.setNativeRunnerReady(true)
		coordinator := newControllerRunnerForTest(
			t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())
		session.afterPoll = stateStore.degradeManagementAudit

		if _, err := coordinator.PollOnce(context.Background()); !errors.Is(
			err,
			ErrControllerRunnerAdmission,
		) {
			t.Fatalf("post-poll audit failure = %v", err)
		}
		if stateStore.claim != nil || stateStore.commits != 0 ||
			session.deleteCalls != 0 || session.acquireCalls != 0 {
			t.Fatalf(
				"post-poll audit failure claim/commit/delete/acquire = %#v/%d/%d/%d",
				stateStore.claim,
				stateStore.commits,
				session.deleteCalls,
				session.acquireCalls,
			)
		}
	})

	t.Run("failed after claim commit", func(t *testing.T) {
		stateStore := newRunnerCoordinatorFakeStore()
		session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
		agent := &runnerCoordinatorFakeAgent{
			prepareState: domain.ExecutionPreparing,
			startState:   domain.ExecutionRunning,
		}
		agent.setNativeRunnerReady(true)
		coordinator := newControllerRunnerForTest(
			t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())

		if _, err := coordinator.PollOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		stateStore.mu.Lock()
		stateStore.auditHealthy = false
		stateStore.mu.Unlock()
		drove, err := coordinator.DriveNext(context.Background())
		if !drove || !errors.Is(err, ErrControllerRunnerAdmission) {
			t.Fatalf("late audit failure drive = (%t, %v)", drove, err)
		}
		if session.acquireCalls != 0 || agent.prepareCalls != 0 || agent.startCalls != 0 {
			t.Fatalf(
				"late audit failure acquire/prepare/start = %d/%d/%d",
				session.acquireCalls,
				agent.prepareCalls,
				agent.startCalls,
			)
		}
	})
}

func TestControllerRunnerReadinessChangesInterruptLongPollAndRefreshCapacity(t *testing.T) {
	for _, testCase := range []struct {
		name            string
		initialReady    bool
		nextReady       bool
		initialCapacity int
		nextCapacity    int
	}{
		{
			name:         "runtime stops while capacity is advertised",
			initialReady: true, nextReady: false,
			initialCapacity: 1, nextCapacity: 0,
		},
		{
			name:         "runtime recovers while zero capacity is advertised",
			initialReady: false, nextReady: true,
			initialCapacity: 0, nextCapacity: 1,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stateStore := newRunnerCoordinatorFakeStore()
			session := newRunnerCoordinatorFakeSession(nil)
			started := make(chan int, 1)
			session.poll = func(ctx context.Context, capacity int) (*github.Message, error) {
				started <- capacity
				<-ctx.Done()
				return nil, ctx.Err()
			}
			agent := &runnerCoordinatorFakeAgent{
				snapshot: AgentSnapshot{
					NodeID: "00000000000000000000000000000001",
					OS:     domain.OSLinux,
					Arch:   domain.ArchAMD64,
				},
				snapshotOnline: true,
			}
			agent.setNativeRunnerReady(testCase.initialReady)
			coordinator := newControllerRunnerForTest(
				t,
				stateStore,
				session,
				agent,
				newRunnerCoordinatorFakeLifecycle(),
			)

			result := make(chan error, 1)
			go func() {
				_, err := coordinator.PollAndDriveOnce(context.Background())
				result <- err
			}()
			select {
			case capacity := <-started:
				if capacity != testCase.initialCapacity {
					t.Fatalf("initial long-poll capacity = %d, want %d",
						capacity, testCase.initialCapacity)
				}
			case <-time.After(time.Second):
				t.Fatal("GitHub long poll did not start")
			}

			agent.setNativeRunnerReady(testCase.nextReady)
			select {
			case err := <-result:
				if err != nil {
					t.Fatalf("intentional readiness cancellation = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("readiness change did not interrupt GitHub long poll")
			}
			if session.acquireCalls != 0 || stateStore.claim != nil {
				t.Fatalf("readiness refresh acquired or claimed work: acquire=%d claim=%#v",
					session.acquireCalls, stateStore.claim)
			}
			if len(stateStore.sessionFailures) != 0 {
				t.Fatalf(
					"intentional readiness cancellation marked provider stale: %v",
					stateStore.sessionFailures,
				)
			}

			session.mu.Lock()
			session.poll = func(context.Context, int) (*github.Message, error) {
				return nil, nil
			}
			session.mu.Unlock()
			if _, err := coordinator.PollOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			session.mu.Lock()
			capacities := append([]int(nil), session.pollCapacities...)
			session.mu.Unlock()
			if len(capacities) != 2 || capacities[1] != testCase.nextCapacity {
				t.Fatalf("refreshed capacities = %v, want second %d",
					capacities, testCase.nextCapacity)
			}
		})
	}
}

func TestControllerRunnerRunWakesCapacityZeroPendingClaimAndRetriesWithoutAnotherPoll(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	agent.setNativeRunnerReady(true)
	first := newControllerRunnerForTest(
		t,
		stateStore,
		newRunnerCoordinatorFakeSession(testControllerRunnerMessage()),
		agent,
		newRunnerCoordinatorFakeLifecycle(),
	)
	if _, err := first.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	agent.setNativeRunnerReady(false)

	pollStarted := make(chan int, 4)
	restartedSession := newRunnerCoordinatorFakeSession(nil)
	restartedSession.poll = func(ctx context.Context, capacity int) (*github.Message, error) {
		pollStarted <- capacity
		<-ctx.Done()
		return nil, ctx.Err()
	}
	restarted := newControllerRunnerForTest(
		t,
		stateStore,
		restartedSession,
		agent,
		newRunnerCoordinatorFakeLifecycle(),
	)
	runContext, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- restarted.Run(runContext) }()

	select {
	case capacity := <-pollStarted:
		if capacity != 0 {
			t.Fatalf("pending-claim poll capacity = %d, want 0", capacity)
		}
	case <-time.After(time.Second):
		t.Fatal("restart did not poll before driving the pending claim")
	}
	restartedSession.mu.Lock()
	acquireCalls := restartedSession.acquireCalls
	restartedSession.mu.Unlock()
	if acquireCalls != 0 {
		t.Fatalf("pending claim acquired before poll/readiness refresh: %d", acquireCalls)
	}

	agent.setNativeRunnerReady(true)
	waitControllerRunnerCondition(t, func() bool {
		stateStore.mu.Lock()
		defer stateStore.mu.Unlock()
		return stateStore.claim != nil && stateStore.claim.State == store.GitHubClaimRunning
	})
	restartedSession.mu.Lock()
	acquireCalls = restartedSession.acquireCalls
	restartedSession.mu.Unlock()
	if acquireCalls != 1 {
		t.Fatalf("recovered pending claim acquire calls = %d", acquireCalls)
	}
	cancelRun()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func TestControllerRunnerReadinessChangeIterationDoesNotDrivePendingClaim(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	agent.setNativeRunnerReady(true)
	first := newControllerRunnerForTest(
		t,
		stateStore,
		newRunnerCoordinatorFakeSession(testControllerRunnerMessage()),
		agent,
		newRunnerCoordinatorFakeLifecycle(),
	)
	if _, err := first.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	agent.setNativeRunnerReady(false)

	pollStarted := make(chan int, 1)
	session := newRunnerCoordinatorFakeSession(nil)
	session.poll = func(ctx context.Context, capacity int) (*github.Message, error) {
		pollStarted <- capacity
		<-ctx.Done()
		return nil, ctx.Err()
	}
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		agent,
		newRunnerCoordinatorFakeLifecycle(),
	)
	result := make(chan error, 1)
	go func() {
		_, err := coordinator.PollAndDriveOnce(context.Background())
		result <- err
	}()
	select {
	case capacity := <-pollStarted:
		if capacity != 0 {
			t.Fatalf("pending-claim capacity = %d, want 0", capacity)
		}
	case <-time.After(time.Second):
		t.Fatal("capacity-zero long poll did not start")
	}
	agent.setNativeRunnerReady(true)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("readiness-change poll result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("readiness change did not end PollAndDriveOnce")
	}
	session.mu.Lock()
	acquireCalls := session.acquireCalls
	session.mu.Unlock()
	stateStore.mu.Lock()
	claimState := stateStore.claim.State
	stateStore.mu.Unlock()
	if acquireCalls != 0 || claimState != store.GitHubClaimPending {
		t.Fatalf("readiness-change iteration acquire/state = %d/%s",
			acquireCalls, claimState)
	}
}

func TestControllerRunnerRunDoesNotAcquireWhenReadinessDropsAfterMessageAck(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	message := testControllerRunnerMessage()
	session := newRunnerCoordinatorFakeSession(nil)
	var pollCalls atomic.Int32
	session.poll = func(ctx context.Context, _ int) (*github.Message, error) {
		if pollCalls.Add(1) == 1 {
			copyMessage := *message
			copyMessage.Jobs = append([]github.JobMessage(nil), message.Jobs...)
			return &copyMessage, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	agent.setNativeRunnerReady(true)
	dropped := make(chan struct{})
	session.beforeDelete = func() {
		agent.setNativeRunnerReady(false)
		close(dropped)
	}
	acquiredAfterPolls := make(chan int32, 1)
	session.afterAcquire = func() {
		acquiredAfterPolls <- pollCalls.Load()
	}
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		agent,
		newRunnerCoordinatorFakeLifecycle(),
	)
	runContext, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- coordinator.Run(runContext) }()

	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("readiness did not drop at message acknowledgement")
	}
	time.Sleep(10 * time.Millisecond)
	session.mu.Lock()
	acquireCalls := session.acquireCalls
	session.mu.Unlock()
	if acquireCalls != 0 {
		t.Fatalf("job acquired while Agent was unready: %d", acquireCalls)
	}
	stateStore.mu.Lock()
	claimState := stateStore.claim.State
	stateStore.mu.Unlock()
	if claimState != store.GitHubClaimPending {
		t.Fatalf("claim state while unready = %s, want pending", claimState)
	}

	agent.setNativeRunnerReady(true)
	select {
	case calls := <-acquiredAfterPolls:
		if calls != 1 {
			t.Fatalf("claim recovery waited for another GitHub poll: poll calls=%d", calls)
		}
	case <-time.After(time.Second):
		t.Fatal("same durable claim was not acquired after readiness recovery")
	}
	waitControllerRunnerCondition(t, func() bool {
		stateStore.mu.Lock()
		defer stateStore.mu.Unlock()
		return stateStore.claim.State == store.GitHubClaimRunning
	})
	cancelRun()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func TestControllerRunnerRunStopsBeforePrepareWhenReadinessDropsDuringAcquire(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	message := testControllerRunnerMessage()
	session := newRunnerCoordinatorFakeSession(nil)
	var pollCalls atomic.Int32
	session.poll = func(ctx context.Context, _ int) (*github.Message, error) {
		if pollCalls.Add(1) == 1 {
			copyMessage := *message
			copyMessage.Jobs = append([]github.JobMessage(nil), message.Jobs...)
			return &copyMessage, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	agent.setNativeRunnerReady(true)
	acquireFinished := make(chan struct{})
	session.afterAcquire = func() {
		agent.setNativeRunnerReady(false)
		close(acquireFinished)
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	coordinator := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)
	runContext, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- coordinator.Run(runContext) }()

	select {
	case <-acquireFinished:
	case <-time.After(time.Second):
		t.Fatal("GitHub acquire did not complete")
	}
	waitControllerRunnerCondition(t, func() bool {
		stateStore.mu.Lock()
		defer stateStore.mu.Unlock()
		return stateStore.claim.State == store.GitHubClaimAcquired
	})
	agent.mu.Lock()
	prepareCalls := agent.prepareCalls
	agent.mu.Unlock()
	lifecycle.mu.Lock()
	generateCalls := lifecycle.generateCalls
	lifecycle.mu.Unlock()
	if prepareCalls != 0 || generateCalls != 0 {
		t.Fatalf("unready post-acquire prepare/generate calls = %d/%d",
			prepareCalls, generateCalls)
	}

	agent.setNativeRunnerReady(true)
	waitControllerRunnerCondition(t, func() bool {
		stateStore.mu.Lock()
		defer stateStore.mu.Unlock()
		return stateStore.claim.State == store.GitHubClaimRunning
	})
	cancelRun()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func TestControllerRunnerRunStopsBeforeJITWhenReadinessDropsAfterPrepare(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	message := testControllerRunnerMessage()
	session := newRunnerCoordinatorFakeSession(nil)
	var pollCalls atomic.Int32
	session.poll = func(ctx context.Context, _ int) (*github.Message, error) {
		if pollCalls.Add(1) == 1 {
			copyMessage := *message
			copyMessage.Jobs = append([]github.JobMessage(nil), message.Jobs...)
			return &copyMessage, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	agent.setNativeRunnerReady(true)
	prepared := make(chan struct{})
	agent.afterPrepare = func() {
		agent.setNativeRunnerReady(false)
		close(prepared)
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	coordinator := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)
	runContext, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- coordinator.Run(runContext) }()

	select {
	case <-prepared:
	case <-time.After(time.Second):
		t.Fatal("Agent prepare did not complete")
	}
	waitControllerRunnerCondition(t, func() bool {
		stateStore.mu.Lock()
		defer stateStore.mu.Unlock()
		return stateStore.claim.State == store.GitHubClaimPreparing
	})
	lifecycle.mu.Lock()
	generateCalls := lifecycle.generateCalls
	lifecycle.mu.Unlock()
	if generateCalls != 0 {
		t.Fatalf("JIT generated while Agent was unready: %d", generateCalls)
	}

	agent.mu.Lock()
	agent.afterPrepare = nil
	agent.mu.Unlock()
	agent.setNativeRunnerReady(true)
	waitControllerRunnerCondition(t, func() bool {
		stateStore.mu.Lock()
		defer stateStore.mu.Unlock()
		return stateStore.claim.State == store.GitHubClaimRunning
	})
	cancelRun()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func TestControllerRunnerFalseHeartbeatKeepsSessionOnlineButPreventsClaimAndAcquire(t *testing.T) {
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	broker := NewAgentBrokerWithOptions(1, consumers, AgentBrokerOptions{
		ReadinessLease: time.Second,
	})
	agentSession, serveResult := startReadyBrokerSessionWithSnapshot(t, broker, AgentSnapshot{
		NodeID:            "00000000000000000000000000000001",
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		NativeRunnerReady: true,
	})
	agentSession.send(brokerEnvelope(t, "runtime-stopped", transport.MessageHeartbeat, transport.AgentHeartbeat{
		NodeID:            "00000000000000000000000000000001",
		NativeRunnerReady: false,
	}))
	assertBrokerAck(t, agentSession, "runtime-stopped")

	stateStore := newRunnerCoordinatorFakeStore()
	githubSession := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		githubSession,
		broker,
		newRunnerCoordinatorFakeLifecycle(),
	)
	if _, err := coordinator.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubAvailableUnclaimed) {
		t.Fatalf("false-heartbeat available error = %v", err)
	}
	snapshot, online, _ := broker.Readiness("00000000000000000000000000000001")
	if !online || snapshot.NativeRunnerReady {
		t.Fatalf("broker readiness = online:%t ready:%t", online, snapshot.NativeRunnerReady)
	}
	if len(githubSession.pollCapacities) != 1 || githubSession.pollCapacities[0] != 0 ||
		stateStore.claim != nil || githubSession.acquireCalls != 0 {
		t.Fatalf("capacity/claim/acquire = %v/%#v/%d",
			githubSession.pollCapacities, stateStore.claim, githubSession.acquireCalls)
	}
	agentSession.disconnect()
	if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("serve result = %v", err)
	}
}

func TestControllerRunnerDisconnectBetweenPollAndCommitRetriesUntilLateClaim(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	coordinator := newControllerRunnerForTest(
		t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())
	session.afterPoll = func() {
		agent.setSnapshotOnline(false)
		session.afterPoll = nil
	}

	if _, err := coordinator.PollOnce(context.Background()); !errors.Is(err, ErrGitHubAvailableUnclaimed) {
		t.Fatalf("disconnect commit error = %v", err)
	}
	if len(session.pollCapacities) != 1 || session.pollCapacities[0] != 1 {
		t.Fatalf("advertised capacities = %v, want initial 1", session.pollCapacities)
	}
	if stateStore.message == nil || stateStore.claim != nil || session.deleteCalls != 0 {
		t.Fatalf("disconnect message/claim/delete = (%#v, %#v, %d)",
			stateStore.message, stateStore.claim, session.deleteCalls)
	}

	agent.setSnapshotOnline(true)
	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stateStore.replays != 1 || stateStore.claim == nil ||
		stateStore.claim.State != store.GitHubClaimPending || session.deleteCalls != 1 {
		t.Fatalf("late replay/claim/delete = %d/%#v/%d",
			stateStore.replays, stateStore.claim, session.deleteCalls)
	}
}

func TestControllerRunnerOfflineMixedNonAvailableMessageStillAcknowledges(t *testing.T) {
	message := testControllerRunnerMessage()
	message.Jobs = message.Jobs[1:]
	message.Statistics.TotalAvailableJobs = 0
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(message)
	agent := &runnerCoordinatorFakeAgent{
		snapshot: AgentSnapshot{
			NodeID:            "00000000000000000000000000000001",
			OS:                domain.OSLinux,
			Arch:              domain.ArchAMD64,
			NativeRunnerReady: true,
		},
	}
	coordinator := newControllerRunnerForTest(
		t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())
	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stateStore.claim != nil || session.deleteCalls != 1 {
		t.Fatalf("non-available claim/delete = (%#v, %d)", stateStore.claim, session.deleteCalls)
	}
}

func TestControllerRunnerDrivesClaimButDoesNotAckAdditionalUnclaimedAvailability(t *testing.T) {
	message := testControllerRunnerMessage()
	message.Jobs = append(message.Jobs, github.JobMessage{
		Type: github.MessageTypeJobAvailable, RunnerRequestID: 7005,
		RepositoryName: "sparerunner", OwnerName: "example-org",
		JobID: "job-5", WorkflowRunID: 55,
	})
	message.Statistics.TotalAvailableJobs = 2
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(message)
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	coordinator := newControllerRunnerForTest(
		t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())
	if _, err := coordinator.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubAvailableUnclaimed) {
		t.Fatalf("multi-available error = %v", err)
	}
	if session.deleteCalls != 0 || stateStore.claim != nil ||
		agent.prepareCalls != 0 {
		t.Fatalf("multi-available delete/claim = %d/%#v", session.deleteCalls, stateStore.claim)
	}
}

func TestControllerRunnerGenerationAmbiguityRequiresTwoAbsenceObservations(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
		snapshot: AgentSnapshot{
			NodeID:            "00000000000000000000000000000001",
			OS:                domain.OSLinux,
			Arch:              domain.ArchAMD64,
			NativeRunnerReady: true,
		},
		snapshotOnline: true,
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	lifecycle.generateErrors = []error{errors.New("JIT API unavailable"), nil}
	coordinator := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)

	if _, err := coordinator.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("generation error = %v", err)
	}
	if stateStore.claim.State != store.GitHubClaimJITGenerationAmbiguous ||
		lifecycle.generateCalls != 1 {
		t.Fatalf("ambiguous state/generations = %s/%d", stateStore.claim.State, lifecycle.generateCalls)
	}
	if drove, err := coordinator.DriveNext(context.Background()); err != nil || drove {
		t.Fatalf("unreconciled JIT became actionable = (%t, %v)", drove, err)
	}
	if lifecycle.generateCalls != 1 {
		t.Fatalf("JIT regenerated before reconciliation: %d", lifecycle.generateCalls)
	}

	lifecycle.observedRunner = nil
	restarted := newControllerRunnerForEpoch(
		t, stateStore, session, agent, lifecycle, 4)
	if err := restarted.ReconcileJITAttempt(
		context.Background(),
		7001,
	); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("first generation absence = %v", err)
	}
	if stateStore.claim.State != store.GitHubClaimJITGenerationAmbiguous ||
		stateStore.attempt.State != store.GitHubJITGenerationAmbiguous ||
		stateStore.generationAbsences != 1 {
		t.Fatalf("first absence did not retain durable pending fence: %#v / %#v",
			stateStore.claim, stateStore.attempt)
	}
	if lifecycle.generateCalls != 1 || agent.startCalls != 0 {
		t.Fatalf("single absence regenerated/started = %d/%d", lifecycle.generateCalls, agent.startCalls)
	}
	if err := restarted.ReconcileJITAttempt(
		context.Background(), 7001,
	); err != nil {
		t.Fatalf("second generation absence = %v", err)
	}
	if stateStore.claim.State != store.GitHubClaimPreparing ||
		stateStore.attempt.State != store.GitHubJITReconciledAbsent {
		t.Fatalf("second absence did not restore preparing state: %#v / %#v",
			stateStore.claim, stateStore.attempt)
	}
}

func TestControllerRunnerAcquireAmbiguityRetriesOnlyForFreshCommittedAvailability(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	session.acquireErr = errors.New("connection lost after acquire write")
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	coordinator := newControllerRunnerForTest(
		t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())

	if _, err := coordinator.PollAndDriveOnce(
		context.Background()); !errors.Is(err, ErrGitHubAcquireAmbiguous) {
		t.Fatalf("initial acquire error = %v", err)
	}
	if stateStore.claim == nil ||
		stateStore.claim.State != store.GitHubClaimAcquireAmbiguous ||
		session.acquireCalls != 1 {
		t.Fatalf("initial claim/acquire calls = %#v/%d",
			stateStore.claim, session.acquireCalls)
	}

	fresh := testControllerRunnerMessage()
	fresh.ID = 42
	session.mu.Lock()
	session.message = fresh
	session.acquireErr = nil
	session.mu.Unlock()
	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stateStore.claim.State != store.GitHubClaimPending ||
		session.acquireCalls != 1 {
		t.Fatalf("fresh evidence did not persist pending attempt first = %s/%d",
			stateStore.claim.State, session.acquireCalls)
	}
	if drove, err := coordinator.DriveNext(context.Background()); err != nil || !drove {
		t.Fatalf("drive durable reacquire attempt = (%t, %v)", drove, err)
	}
	if session.acquireCalls != 2 {
		t.Fatalf("durable reacquire calls = %d, want 2", session.acquireCalls)
	}

	// Re-delivery of the same committed message is replay evidence, not a new
	// observation, and therefore cannot authorize another Acquire call.
	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if session.acquireCalls != 2 {
		t.Fatalf("replayed availability retried Acquire: %d", session.acquireCalls)
	}
}

func TestControllerRunnerNilJITResultIsDurablyAmbiguousAndNeverDispatched(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	lifecycle.nilJIT = true
	coordinator := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)

	if _, err := coordinator.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("nil JIT result error = %v", err)
	}
	if stateStore.claim.State != store.GitHubClaimJITGenerationAmbiguous ||
		agent.startCalls != 0 {
		t.Fatalf("nil JIT state/start calls = %s/%d", stateStore.claim.State, agent.startCalls)
	}
}

func TestControllerRunnerPrepareFailureDoesNotGenerateJIT(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionFailed,
		prepareCode:  domain.ExecutionErrorPlatform,
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	coordinator := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)
	if _, err := coordinator.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubPrepareFailed) {
		t.Fatalf("prepare failure = %v", err)
	}
	if stateStore.claim.State != store.GitHubClaimPrepareFailed {
		t.Fatalf("claim state = %q", stateStore.claim.State)
	}
	if lifecycle.generateCalls != 0 || agent.startCalls != 0 {
		t.Fatalf("JIT/start after prepare failure = %d/%d", lifecycle.generateCalls, agent.startCalls)
	}
}

func TestControllerRunnerAmbiguousStartNeedsSessionWatermarkAndNeverDeletesByName(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startErr:     errors.New("connection lost after write"),
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	coordinator := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)
	if _, err := coordinator.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubStartAmbiguous) {
		t.Fatalf("start error = %v", err)
	}
	attempt := stateStore.attempt
	if attempt.State != store.GitHubJITStartAmbiguous || attempt.StartCommandID == "" {
		t.Fatalf("attempt = %#v", attempt)
	}
	restarted := newControllerRunnerForEpoch(
		t, stateStore, session, agent, lifecycle, 4)
	lifecycle.observedRunner = &github.RunnerReference{
		ID: 999, Name: attempt.RunnerName, ScaleSetID: 7,
	}
	if err := restarted.ReconcileJITAttempt(context.Background(), 7001); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("same-name different-ID reconciliation error = %v", err)
	}
	if lifecycle.removeCalls != 0 {
		t.Fatalf("same-name different-ID runner was removed: %d", lifecycle.removeCalls)
	}
	agent.snapshotOnline = true
	agent.snapshot = AgentSnapshot{
		NodeID:            "00000000000000000000000000000001",
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		NativeRunnerReady: true,
		Commands: []domain.Command{{
			ID:              attempt.StartCommandID,
			ControllerEpoch: 3,
			ExecutionID:     stateStore.claim.Execution.ID,
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   domain.PayloadDigest([]byte("accepted-start")),
		}},
	}
	if err := restarted.ReconcileJITAttempt(context.Background(), 7001); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("start ambiguity without session watermark = %v", err)
	}
	if lifecycle.removeCalls != 0 {
		t.Fatalf("accepted runner removal calls = %d", lifecycle.removeCalls)
	}
}

func TestControllerRunnerAmbiguousStartAdoptsExactAcceptedAgentRuntimeWithoutProviderDelete(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startErr:     errors.New("connection lost after Agent accepted Start"),
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	first := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)
	if _, err := first.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubStartAmbiguous) {
		t.Fatalf("start error = %v", err)
	}
	attempt := stateStore.attempt
	command := domain.Command{
		ID:              attempt.StartCommandID,
		ControllerEpoch: attempt.ControllerEpoch,
		ExecutionID:     stateStore.claim.Execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("exact accepted start")),
	}
	stateStore.issued[command.ID] = store.IssuedAgentCommand{
		NodeID:  stateStore.claim.Execution.Slot.NodeID,
		Type:    domain.CommandStart,
		Command: command,
	}
	agent.snapshotOnline = true
	agent.snapshot = AgentSnapshot{
		NodeID:             stateStore.claim.Execution.Slot.NodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: attempt.ControllerEpoch,
		Commands:           []domain.Command{command},
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        stateStore.claim.Execution.ID,
			State:              domain.ExecutionRunning,
			ObservedAtUnixNano: 1,
		}},
	}
	restarted := newControllerRunnerForEpoch(
		t, stateStore, session, agent, lifecycle, 4)
	if err := restarted.ReconcileJITAttempt(context.Background(), 7001); err != nil {
		t.Fatal(err)
	}
	if stateStore.attempt.State != store.GitHubJITStarted ||
		stateStore.claim.State != store.GitHubClaimRunning ||
		stateStore.claim.Execution.State != domain.ExecutionRunning {
		t.Fatalf("reconciled attempt/claim = %#v / %#v",
			stateStore.attempt, stateStore.claim)
	}
	if lifecycle.getCalls != 0 || lifecycle.removeCalls != 0 ||
		agent.startCalls != 1 {
		t.Fatalf("provider lookup/removal/start calls = %d/%d/%d",
			lifecycle.getCalls, lifecycle.removeCalls, agent.startCalls)
	}
}

func TestControllerRunnerAmbiguousStartRejectsAgentCommandPayloadSubstitution(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startErr:     errors.New("connection lost after write"),
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	first := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)
	if _, err := first.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubStartAmbiguous) {
		t.Fatalf("start error = %v", err)
	}
	attempt := stateStore.attempt
	issued := domain.Command{
		ID:              attempt.StartCommandID,
		ControllerEpoch: attempt.ControllerEpoch,
		ExecutionID:     stateStore.claim.Execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("Controller authority")),
	}
	stateStore.issued[issued.ID] = store.IssuedAgentCommand{
		NodeID:  stateStore.claim.Execution.Slot.NodeID,
		Type:    domain.CommandStart,
		Command: issued,
	}
	substituted := issued
	substituted.PayloadDigest = domain.PayloadDigest([]byte("substituted payload"))
	agent.snapshotOnline = true
	agent.snapshot = AgentSnapshot{
		NodeID:             stateStore.claim.Execution.Slot.NodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: attempt.ControllerEpoch,
		Commands:           []domain.Command{substituted},
	}
	restarted := newControllerRunnerForEpoch(
		t, stateStore, session, agent, lifecycle, 4)
	if err := restarted.ReconcileJITAttempt(
		context.Background(), 7001); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("substituted command reconciliation error = %v", err)
	}
	if lifecycle.getCalls != 0 || lifecycle.removeCalls != 0 ||
		stateStore.attempt.State != store.GitHubJITStartAmbiguous {
		t.Fatalf("lookup/removal/attempt = %d/%d/%#v",
			lifecycle.getCalls, lifecycle.removeCalls, stateStore.attempt)
	}
}

func TestControllerRunnerGeneratedReconciliationNeverDeletesDifferentRunnerID(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startErr:     errors.New("connection lost before write"),
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	first := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)
	if _, err := first.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubStartAmbiguous) {
		t.Fatalf("start error = %v", err)
	}

	// Model a crash after the generated identity was committed but before the
	// durable start-dispatch transition. This is the only generated state where
	// task-007 may remove an unaccepted registration.
	stateStore.mu.Lock()
	stateStore.attempt.State = store.GitHubJITGenerated
	stateStore.claim.State = store.GitHubClaimJITGenerated
	attempt := stateStore.attempt
	stateStore.mu.Unlock()

	agent.setSnapshotOnline(true)
	lifecycle.observedRunner = &github.RunnerReference{
		ID: 999, Name: attempt.RunnerName, ScaleSetID: 7,
	}
	restarted := newControllerRunnerForEpoch(
		t, stateStore, session, agent, lifecycle, 4)
	if err := restarted.ReconcileJITAttempt(context.Background(), 7001); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("different-ID reconciliation error = %v", err)
	}
	if lifecycle.getCalls != 1 || lifecycle.removeCalls != 0 {
		t.Fatalf("lookup/removal calls = %d/%d", lifecycle.getCalls, lifecycle.removeCalls)
	}
}

func TestControllerRunnerKnownRunnerAbsencePersistsRemovalPendingBeforeLaterRead(t *testing.T) {
	coordinator, stateStore, _, lifecycle :=
		newControllerRunnerGeneratedAttemptForReconciliation(t)

	if err := coordinator.ReconcileJITAttempt(context.Background(), 7001); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("first absence reconciliation error = %v", err)
	}
	if stateStore.attempt.State != store.GitHubJITRemovalPending ||
		stateStore.claim.State != store.GitHubClaimReconciliationRequired {
		t.Fatalf("first absence must persist pending removal: attempt=%#v claim=%#v",
			stateStore.attempt, stateStore.claim)
	}
	if lifecycle.getCalls != 1 || lifecycle.removeCalls != 0 {
		t.Fatalf("first absence lookup/removal calls = %d/%d", lifecycle.getCalls, lifecycle.removeCalls)
	}

	if err := coordinator.ReconcileJITAttempt(context.Background(), 7001); err != nil {
		t.Fatalf("later absence reconciliation error = %v", err)
	}
	if stateStore.attempt.State != store.GitHubJITReconciledAbsent ||
		stateStore.claim.State != store.GitHubClaimPreparing {
		t.Fatalf("later absence was not durably reconciled: attempt=%#v claim=%#v",
			stateStore.attempt, stateStore.claim)
	}
	if lifecycle.getCalls != 2 || lifecycle.removeCalls != 0 {
		t.Fatalf("later absence lookup/removal calls = %d/%d", lifecycle.getCalls, lifecycle.removeCalls)
	}
}

func TestControllerRunnerExactRunnerRemovalRequiresLaterAbsenceRead(t *testing.T) {
	coordinator, stateStore, _, lifecycle :=
		newControllerRunnerGeneratedAttemptForReconciliation(t)
	lifecycle.observedRunner = &github.RunnerReference{
		ID:         stateStore.attempt.RunnerID,
		Name:       stateStore.attempt.RunnerName,
		ScaleSetID: 7,
	}

	if err := coordinator.ReconcileJITAttempt(context.Background(), 7001); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("runner removal reconciliation error = %v", err)
	}
	if stateStore.attempt.State != store.GitHubJITRemovalPending ||
		stateStore.claim.State != store.GitHubClaimReconciliationRequired {
		t.Fatalf("DELETE must leave durable pending removal: attempt=%#v claim=%#v",
			stateStore.attempt, stateStore.claim)
	}
	if lifecycle.getCalls != 1 || lifecycle.removeCalls != 1 {
		t.Fatalf("DELETE lookup/removal calls = %d/%d", lifecycle.getCalls, lifecycle.removeCalls)
	}

	// The provider's later read is the only authority that can prove DELETE
	// took effect; the DELETE response itself never proves runner absence. Two
	// exact nil reads are required after the deletion fence.
	lifecycle.observedRunner = nil
	if err := coordinator.ReconcileJITAttempt(context.Background(), 7001); !errors.Is(
		err,
		ErrGitHubReconciliationRequired,
	) {
		t.Fatalf("first post-DELETE absence reconciliation error = %v", err)
	}
	if stateStore.attempt.State != store.GitHubJITRemovalPending ||
		stateStore.claim.State != store.GitHubClaimReconciliationRequired {
		t.Fatalf("first post-DELETE absence escaped its fence: attempt=%#v claim=%#v",
			stateStore.attempt, stateStore.claim)
	}
	if err := coordinator.ReconcileJITAttempt(context.Background(), 7001); err != nil {
		t.Fatalf("confirmed post-DELETE absence reconciliation error = %v", err)
	}
	if stateStore.attempt.State != store.GitHubJITReconciledAbsent ||
		stateStore.claim.State != store.GitHubClaimPreparing {
		t.Fatalf("post-DELETE absence was not durably reconciled: attempt=%#v claim=%#v",
			stateStore.attempt, stateStore.claim)
	}
	if lifecycle.getCalls != 3 || lifecycle.removeCalls != 1 {
		t.Fatalf("post-DELETE lookup/removal calls = %d/%d", lifecycle.getCalls, lifecycle.removeCalls)
	}
}

func TestControllerRunnerLostJITWaitsForDurableTerminalThenCleansExactProviderRunner(t *testing.T) {
	tests := []struct {
		name           string
		state          domain.ExecutionState
		wantCapacity   int
		cleanupBlocked bool
	}{
		{
			name:         "released",
			state:        domain.ExecutionReleased,
			wantCapacity: 1,
		},
		{
			name:           "cleanup failed",
			state:          domain.ExecutionCleanupFailed,
			wantCapacity:   0,
			cleanupBlocked: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stateStore := newRunnerCoordinatorFakeStore()
			session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
			agent := &runnerCoordinatorFakeAgent{
				prepareState: domain.ExecutionPreparing,
				startErr:     errors.New("connection lost after Agent accepted Start"),
			}
			lifecycle := newRunnerCoordinatorFakeLifecycle()
			first := newControllerRunnerForTest(
				t,
				stateStore,
				session,
				agent,
				lifecycle,
			)
			if _, err := first.PollAndDriveOnce(context.Background()); !errors.Is(
				err,
				ErrGitHubStartAmbiguous,
			) {
				t.Fatalf("setup start ambiguity = %v", err)
			}

			stateStore.mu.Lock()
			attempt := stateStore.attempt
			executionID := stateStore.claim.Execution.ID
			issuedCommand := domain.Command{
				ID:              attempt.StartCommandID,
				ControllerEpoch: attempt.ControllerEpoch,
				ExecutionID:     executionID,
				ExpectedState:   domain.ExecutionPreparing,
				PayloadDigest: domain.PayloadDigest(
					[]byte("lost-jit-accepted-start-" + test.name),
				),
			}
			stateStore.issued[attempt.StartCommandID] = store.IssuedAgentCommand{
				NodeID:  stateStore.claim.Execution.Slot.NodeID,
				Type:    domain.CommandStart,
				Command: issuedCommand,
			}
			stateStore.mu.Unlock()
			agent.mu.Lock()
			agent.snapshot.MaxControllerEpoch = attempt.ControllerEpoch
			agent.snapshot.Commands = []domain.Command{issuedCommand}
			agent.snapshot.Observations = []transport.AgentExecutionObservation{{
				ExecutionID:        executionID,
				State:              test.state,
				ObservedAtUnixNano: 100,
			}}
			agent.mu.Unlock()
			agent.setSnapshotOnline(true)
			restarted := newControllerRunnerForEpoch(
				t,
				stateStore,
				session,
				agent,
				lifecycle,
				4,
			)
			lifecycle.observedRunner = &github.RunnerReference{
				ID:         attempt.RunnerID,
				Name:       attempt.RunnerName,
				ScaleSetID: 7,
			}

			if err := restarted.ReconcileJITAttempt(
				context.Background(),
				attempt.ClaimKey,
			); !errors.Is(err, ErrGitHubReconciliationRequired) ||
				!errors.Is(err, store.ErrGitHubJITTerminalPending) {
				t.Fatalf("snapshot-before-outbox reconciliation = %v", err)
			}
			if lifecycle.getCalls != 0 || lifecycle.removeCalls != 0 {
				t.Fatalf("snapshot-only terminal reached provider: query/remove=%d/%d",
					lifecycle.getCalls, lifecycle.removeCalls)
			}
			stateStore.mu.Lock()
			if stateStore.attempt.State != store.GitHubJITStartAmbiguous ||
				stateStore.claim.Execution.State != domain.ExecutionPreparing {
				t.Fatalf("snapshot-only terminal changed authority: attempt=%#v claim=%#v",
					stateStore.attempt, stateStore.claim)
			}
			stateStore.durableTerminal = true
			stateStore.mu.Unlock()

			if err := restarted.ReconcileJITAttempt(
				context.Background(),
				attempt.ClaimKey,
			); !errors.Is(err, ErrGitHubReconciliationRequired) {
				t.Fatalf("exact provider deletion reconciliation = %v", err)
			}
			if lifecycle.getCalls != 1 || lifecycle.removeCalls != 1 {
				t.Fatalf("exact provider cleanup calls = %d/%d",
					lifecycle.getCalls, lifecycle.removeCalls)
			}
			lifecycle.observedRunner = nil
			if err := restarted.ReconcileJITAttempt(
				context.Background(),
				attempt.ClaimKey,
			); !errors.Is(err, ErrGitHubReconciliationRequired) ||
				!errors.Is(err, store.ErrGitHubJITAbsencePending) {
				t.Fatalf("first post-delete absence = %v", err)
			}
			if err := restarted.ReconcileJITAttempt(
				context.Background(),
				attempt.ClaimKey,
			); err != nil {
				t.Fatalf("confirmed post-delete absence = %v", err)
			}
			stateStore.mu.Lock()
			if stateStore.attempt.State != store.GitHubJITReconciledAbsent ||
				stateStore.claim.State != store.GitHubClaimReconciliationRequired ||
				stateStore.claim.Execution.State != test.state ||
				stateStore.capacity != test.wantCapacity {
				t.Fatalf("lost-JIT terminal result: attempt=%#v claim=%#v capacity=%d",
					stateStore.attempt, stateStore.claim, stateStore.capacity)
			}
			if test.cleanupBlocked && stateStore.capacity != 0 {
				t.Fatal("cleanup-blocked lost JIT returned capacity")
			}
			stateStore.mu.Unlock()
			if lifecycle.getCalls != 3 ||
				lifecycle.removeCalls != 1 ||
				lifecycle.generateCalls != 1 ||
				agent.startCalls != 1 {
				t.Fatalf("lost-JIT duplicated work: query/remove/generate/start=%d/%d/%d/%d",
					lifecycle.getCalls,
					lifecycle.removeCalls,
					lifecycle.generateCalls,
					agent.startCalls)
			}
		})
	}
}

func TestControllerRunnerPrunedDurableStartHistoryNeverQueriesOrRemovesProvider(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startErr:     errors.New("connection lost before Start result"),
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	first := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
	)
	if _, err := first.PollAndDriveOnce(context.Background()); !errors.Is(
		err,
		ErrGitHubStartAmbiguous,
	) {
		t.Fatalf("setup start ambiguity = %v", err)
	}
	stateStore.mu.Lock()
	attempt := stateStore.attempt
	stateStore.prunedStarted = true
	stateStore.claim.Execution.State = domain.ExecutionReleased
	stateStore.capacity = 1
	stateStore.mu.Unlock()
	agent.mu.Lock()
	agent.snapshot.Commands = nil
	agent.snapshot.Observations = nil
	agent.mu.Unlock()
	agent.setSnapshotOnline(true)
	restarted := newControllerRunnerForEpoch(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
		4,
	)
	if err := restarted.ReconcileJITAttempt(
		context.Background(),
		attempt.ClaimKey,
	); err != nil {
		t.Fatalf("pruned durable start reconciliation = %v", err)
	}
	stateStore.mu.Lock()
	defer stateStore.mu.Unlock()
	if stateStore.attempt.State != store.GitHubJITStarted ||
		stateStore.claim.State != store.GitHubClaimRunning ||
		stateStore.claim.Execution.State != domain.ExecutionReleased {
		t.Fatalf("pruned durable start result: attempt=%#v claim=%#v",
			stateStore.attempt, stateStore.claim)
	}
	if lifecycle.getCalls != 0 ||
		lifecycle.removeCalls != 0 ||
		lifecycle.generateCalls != 1 ||
		agent.startCalls != 1 {
		t.Fatalf("pruned started history touched provider/work: query/remove/generate/start=%d/%d/%d/%d",
			lifecycle.getCalls,
			lifecycle.removeCalls,
			lifecycle.generateCalls,
			agent.startCalls)
	}
}

func TestControllerRunnerRunPollsPickupBeforeFirstUnpickedRunnerDelete(
	t *testing.T,
) {
	stateStore, agent, lifecycle, attempt :=
		prepareControllerRunnerUnpickedRequeueForTest(t)
	session := newRunnerCoordinatorFakeSession(nil)
	var pollCalls int
	session.poll = func(
		ctx context.Context,
		_ int,
	) (*github.Message, error) {
		pollCalls++
		switch pollCalls {
		case 1:
			return testControllerRunnerRequeueAvailableMessage(61), nil
		case 2:
			return testControllerRunnerPickupMessage(
				62,
				attempt,
			), nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
	)
	coordinator.reconciliationRetry = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := coordinator.Run(ctx); err != nil {
		t.Fatalf("Run result = %v", err)
	}
	stateStore.mu.Lock()
	defer stateStore.mu.Unlock()
	if pollCalls < 2 ||
		lifecycle.removeCalls != 0 ||
		stateStore.replacementCreates != 0 ||
		stateStore.requeueIntent == nil ||
		!stateStore.requeueIntent.PickupProven ||
		stateStore.acquireAttempt.Attempt != 1 ||
		lifecycle.generateCalls != 1 ||
		agent.startCalls != 1 {
		t.Fatalf(
			"pickup-before-delete state: polls=%d commits=%d messages=%#v deletes=%d removes=%d replacements=%d intent=%#v acquire_attempt=%d generate/start=%d/%d",
			pollCalls,
			stateStore.commits,
			stateStore.messages,
			session.deleteCalls,
			lifecycle.removeCalls,
			stateStore.replacementCreates,
			stateStore.requeueIntent,
			stateStore.acquireAttempt.Attempt,
			lifecycle.generateCalls,
			agent.startCalls,
		)
	}
}

func TestControllerRunnerRunCommitsLatePickupBetweenDeleteAndAbsenceConfirmation(
	t *testing.T,
) {
	stateStore, agent, lifecycle, attempt :=
		prepareControllerRunnerUnpickedRequeueForTest(t)
	lifecycle.remove = func(
		context.Context,
		github.RunnerReference,
	) error {
		lifecycle.mu.Lock()
		lifecycle.observedRunner = nil
		lifecycle.mu.Unlock()
		return nil
	}
	session := newRunnerCoordinatorFakeSession(nil)
	ctx, cancel := context.WithCancel(context.Background())
	var pollCalls int
	session.poll = func(
		context.Context,
		int,
	) (*github.Message, error) {
		pollCalls++
		switch pollCalls {
		case 1:
			return testControllerRunnerRequeueAvailableMessage(71), nil
		case 2:
			// This zero-capacity observation separates intent creation from
			// the exact provider DELETE.
			return nil, nil
		case 3:
			// GitHub reports pickup after DELETE was issued but before the
			// two-read absence fence can create replacement authority.
			return testControllerRunnerPickupMessage(
				72,
				attempt,
			), nil
		case 4:
			return nil, nil
		default:
			cancel()
			return nil, nil
		}
	}
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
	)
	coordinator.reconciliationRetry = time.Millisecond
	result := make(chan error, 1)
	go func() {
		result <- coordinator.Run(ctx)
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run result = %v", err)
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("Run did not finish late-pickup reconciliation")
	}
	stateStore.mu.Lock()
	defer stateStore.mu.Unlock()
	if pollCalls < 5 ||
		lifecycle.removeCalls != 1 ||
		stateStore.replacementCreates != 0 ||
		stateStore.requeueIntent != nil ||
		stateStore.claim.State != store.GitHubClaimRunning ||
		stateStore.claim.Execution.ID == "" ||
		stateStore.acquireAttempt.Attempt != 1 ||
		lifecycle.generateCalls != 1 ||
		agent.startCalls != 1 {
		t.Fatalf(
			"late-pickup state: polls=%d commits=%d messages=%#v deletes=%d removes=%d replacements=%d intent=%#v claim=%#v acquire_attempt=%d generate/start=%d/%d",
			pollCalls,
			stateStore.commits,
			stateStore.messages,
			session.deleteCalls,
			lifecycle.removeCalls,
			stateStore.replacementCreates,
			stateStore.requeueIntent,
			stateStore.claim,
			stateStore.acquireAttempt.Attempt,
			lifecycle.generateCalls,
			agent.startCalls,
		)
	}
}

func TestControllerRunnerRunCreatesOneReplacementOnlyAfterConfirmedAbsence(
	t *testing.T,
) {
	stateStore, agent, lifecycle, _ :=
		prepareControllerRunnerUnpickedRequeueForTest(t)
	lifecycle.remove = func(
		context.Context,
		github.RunnerReference,
	) error {
		lifecycle.mu.Lock()
		lifecycle.observedRunner = nil
		lifecycle.mu.Unlock()
		return nil
	}
	session := newRunnerCoordinatorFakeSession(nil)
	ctx, cancel := context.WithCancel(context.Background())
	var pollCalls int
	session.poll = func(
		context.Context,
		int,
	) (*github.Message, error) {
		pollCalls++
		switch pollCalls {
		case 1:
			return testControllerRunnerRequeueAvailableMessage(81), nil
		case 2, 3, 4:
			return nil, nil
		default:
			cancel()
			return nil, context.Canceled
		}
	}
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
	)
	agent.mu.Lock()
	agent.afterStart = cancel
	agent.mu.Unlock()
	coordinator.reconciliationRetry = time.Millisecond
	if err := coordinator.Run(ctx); err != nil {
		t.Fatalf("Run result = %v", err)
	}
	stateStore.mu.Lock()
	defer stateStore.mu.Unlock()
	if pollCalls != 4 ||
		lifecycle.removeCalls != 1 ||
		stateStore.replacementCreates != 1 ||
		stateStore.requeueIntent != nil ||
		stateStore.claim.State != store.GitHubClaimRunning ||
		stateStore.claim.Execution.ID == "" ||
		stateStore.claim.Execution.State != domain.ExecutionRunning ||
		stateStore.acquireAttempt.Attempt != 2 ||
		session.acquireCalls != 1 ||
		lifecycle.generateCalls != 2 ||
		agent.prepareCalls != 2 ||
		agent.startCalls != 2 {
		t.Fatalf(
			"confirmed-absence state: polls=%d removes=%d replacements=%d intent=%#v claim=%#v acquire_attempt=%d replacement acquire=%d generate=%d prepare/start=%d/%d",
			pollCalls,
			lifecycle.removeCalls,
			stateStore.replacementCreates,
			stateStore.requeueIntent,
			stateStore.claim,
			stateStore.acquireAttempt.Attempt,
			session.acquireCalls,
			lifecycle.generateCalls,
			agent.prepareCalls,
			agent.startCalls,
		)
	}
}

func TestControllerRunnerCanceledCompletionStillReplacesUnpickedRunner(
	t *testing.T,
) {
	stateStore, agent, lifecycle, attempt :=
		prepareControllerRunnerUnpickedRequeueForTest(t)
	lifecycle.remove = func(
		context.Context,
		github.RunnerReference,
	) error {
		lifecycle.mu.Lock()
		lifecycle.observedRunner = nil
		lifecycle.mu.Unlock()
		return nil
	}
	session := newRunnerCoordinatorFakeSession(nil)
	ctx, cancel := context.WithCancel(context.Background())
	var pollCalls int
	session.poll = func(
		context.Context,
		int,
	) (*github.Message, error) {
		pollCalls++
		switch pollCalls {
		case 1:
			return testControllerRunnerCanceledRequeueMessage(
				86,
				attempt,
			), nil
		case 2, 3, 4:
			return nil, nil
		default:
			t.Fatal("replacement waited for another GitHub long poll")
			return nil, nil
		}
	}
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
	)
	agent.mu.Lock()
	agent.afterStart = cancel
	agent.mu.Unlock()
	coordinator.reconciliationRetry = time.Millisecond
	if err := coordinator.Run(ctx); err != nil {
		t.Fatalf("Run result = %v", err)
	}
	stateStore.mu.Lock()
	defer stateStore.mu.Unlock()
	if pollCalls != 4 ||
		lifecycle.removeCalls != 1 ||
		stateStore.replacementCreates != 1 ||
		stateStore.requeueIntent != nil ||
		stateStore.claim.State != store.GitHubClaimRunning ||
		stateStore.claim.Execution.State != domain.ExecutionRunning ||
		session.acquireCalls != 1 ||
		lifecycle.generateCalls != 2 ||
		agent.prepareCalls != 2 ||
		agent.startCalls != 2 {
		t.Fatalf(
			"canceled completion requeue: polls=%d removes=%d replacements=%d intent=%#v claim=%#v acquire=%d generate=%d prepare/start=%d/%d",
			pollCalls,
			lifecycle.removeCalls,
			stateStore.replacementCreates,
			stateStore.requeueIntent,
			stateStore.claim,
			session.acquireCalls,
			lifecycle.generateCalls,
			agent.prepareCalls,
			agent.startCalls,
		)
	}
}

func TestControllerRunnerRestartRestoresIntentFenceBeforeDrivingReplacement(
	t *testing.T,
) {
	stateStore, agent, lifecycle, _ :=
		prepareControllerRunnerUnpickedRequeueForTest(t)
	creator := newControllerRunnerForTest(
		t,
		stateStore,
		newRunnerCoordinatorFakeSession(nil),
		agent,
		lifecycle,
	)
	if err := creator.CommitMessage(
		context.Background(),
		*testControllerRunnerRequeueAvailableMessage(91),
	); err != nil {
		t.Fatal(err)
	}
	stateStore.mu.Lock()
	intent := *stateStore.requeueIntent
	stateStore.mu.Unlock()

	const restartEpoch domain.ControllerEpoch = 4
	agent.mu.Lock()
	agent.snapshot.MaxControllerEpoch = restartEpoch
	restartAgentSnapshot := cloneAgentSnapshot(agent.snapshot)
	agent.mu.Unlock()
	projection, err := reconcile.RestoreRestart(
		store.ControllerRestartSnapshot{
			Controller: store.ControllerSnapshot{
				ControllerEpoch: restartEpoch,
				Nodes: []store.NodeAdministration{{
					NodeID: intent.Claim.Execution.Slot.NodeID,
					State:  domain.NodeActive,
				}},
				Executions: []domain.ExecutionSnapshot{
					intent.Claim.Execution,
				},
			},
			NodeTopology: []store.RestartNodeTopology{{
				NodeID:              intent.Claim.Execution.Slot.NodeID,
				DisplayName:         string(intent.Claim.Execution.Slot.NodeID),
				CertificateSerial:   "restart-unpicked-certificate",
				CredentialEpoch:     1,
				AdministrativeState: domain.NodeActive,
				MaxRunners:          1,
				PlatformObserved:    true,
				OS:                  domain.OSLinux,
				Architecture:        domain.ArchAMD64,
			}},
			GitHubFences: []store.GitHubReconciliationFence{{
				Claim:   intent.Claim,
				Attempt: &intent.Attempt,
			}},
		},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projection.ReconcileAgentSnapshot(
		restartAgentSnapshot,
	); err != nil {
		t.Fatal(err)
	}
	admission, err := projection.Admission(
		intent.Claim.Execution.Slot.NodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if admission.AllowsNewCapacity || !admission.AllowsRecovery {
		t.Fatalf("restored intent admission = %#v", admission)
	}
	reconciler := &recordingRealControllerRunnerReconciler{
		controller: projection,
	}
	lifecycle.remove = func(
		context.Context,
		github.RunnerReference,
	) error {
		lifecycle.mu.Lock()
		lifecycle.observedRunner = nil
		lifecycle.mu.Unlock()
		return nil
	}
	session := newRunnerCoordinatorFakeSession(nil)
	ctx, cancel := context.WithCancel(context.Background())
	var pollCalls int
	session.poll = func(
		context.Context,
		int,
	) (*github.Message, error) {
		pollCalls++
		stateStore.mu.Lock()
		replacements := stateStore.replacementCreates
		acquireAttempt := stateStore.acquireAttempt.Attempt
		stateStore.mu.Unlock()
		switch pollCalls {
		case 1:
			if lifecycle.removeCalls != 0 ||
				replacements != 0 ||
				acquireAttempt != 1 {
				t.Fatalf(
					"restart drove before first zero-capacity poll: removes=%d replacements=%d acquire_attempt=%d",
					lifecycle.removeCalls,
					replacements,
					acquireAttempt,
				)
			}
		case 2, 3:
			if replacements != 0 || acquireAttempt != 1 {
				t.Fatalf(
					"restart created replacement before confirmed absence: poll=%d replacements=%d acquire_attempt=%d",
					pollCalls,
					replacements,
					acquireAttempt,
				)
			}
		default:
			if replacements != 1 || acquireAttempt != 2 {
				t.Fatalf(
					"restart final replacement authority: replacements=%d acquire_attempt=%d",
					replacements,
					acquireAttempt,
				)
			}
			cancel()
			return nil, context.Canceled
		}
		return nil, nil
	}
	restarted := newControllerRunnerForEpochWithReconciler(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
		restartEpoch,
		reconciler,
	)
	agent.mu.Lock()
	agent.afterStart = cancel
	agent.mu.Unlock()
	restarted.reconciliationRetry = time.Millisecond
	if err := restarted.Run(ctx); err != nil {
		t.Fatalf("restarted Run result = %v", err)
	}
	stateStore.mu.Lock()
	replacement := stateStore.claim.Execution
	replacementCreates := stateStore.replacementCreates
	stateStore.mu.Unlock()
	fleet := projection.FleetSnapshot()
	oldFenceClears := 0
	for _, cleared := range reconciler.cleared {
		if cleared.ExecutionID == intent.Claim.Execution.ID &&
			cleared.Attempt != nil &&
			cleared.Attempt.State == store.GitHubJITRemovalPending {
			oldFenceClears++
		}
	}
	if pollCalls != 3 ||
		replacementCreates != 1 ||
		session.acquireCalls != 1 ||
		lifecycle.generateCalls != 2 ||
		agent.prepareCalls != 2 ||
		agent.startCalls != 2 ||
		len(fleet.Reservations) != 1 ||
		fleet.Reservations[0].ExecutionID != replacement.ID ||
		oldFenceClears != 1 {
		t.Fatalf(
			"restart projection result: polls=%d replacement=%#v acquire=%d generate=%d prepare/start=%d/%d fleet=%#v cleared=%#v",
			pollCalls,
			replacement,
			session.acquireCalls,
			lifecycle.generateCalls,
			agent.prepareCalls,
			agent.startCalls,
			fleet,
			reconciler.cleared,
		)
	}
}

func prepareControllerRunnerUnpickedRequeueForTest(
	t *testing.T,
) (
	*runnerCoordinatorFakeStore,
	*runnerCoordinatorFakeAgent,
	*runnerCoordinatorFakeLifecycle,
	store.GitHubJITAttempt,
) {
	t.Helper()
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	first := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
	)
	if _, err := first.PollAndDriveOnce(context.Background()); err != nil {
		t.Fatalf("initial runner lifecycle = %v", err)
	}
	stateStore.mu.Lock()
	attempt := stateStore.attempt
	executionID := stateStore.claim.Execution.ID
	stateStore.claim.Execution.State = domain.ExecutionReleased
	stateStore.claim.State = store.GitHubClaimRunning
	stateStore.capacity = 1
	stateStore.mu.Unlock()
	agent.mu.Lock()
	agent.snapshot.Observations = []transport.AgentExecutionObservation{{
		ExecutionID:        executionID,
		State:              domain.ExecutionReleased,
		ObservedAtUnixNano: 100,
	}}
	agent.snapshot.Commands = nil
	agent.mu.Unlock()
	agent.setSnapshotOnline(true)
	lifecycle.observedRunner = &github.RunnerReference{
		ID:         attempt.RunnerID,
		Name:       attempt.RunnerName,
		ScaleSetID: github.ScaleSetID(attempt.ScaleSetID),
	}
	return stateStore, agent, lifecycle, attempt
}

func testControllerRunnerRequeueAvailableMessage(id int) *github.Message {
	return &github.Message{
		ScaleSetID: 7,
		ID:         id,
		Statistics: github.Statistics{TotalAvailableJobs: 1},
		Jobs: []github.JobMessage{{
			Type:            github.MessageTypeJobAvailable,
			RunnerRequestID: 7001,
			RepositoryName:  "sparerunner",
			OwnerName:       "example-org",
			JobID:           "job-1",
			WorkflowRunID:   51,
		}},
	}
}

func testControllerRunnerPickupMessage(
	id int,
	attempt store.GitHubJITAttempt,
) *github.Message {
	return &github.Message{
		ScaleSetID: 7,
		ID:         id,
		Statistics: github.Statistics{
			TotalAssignedJobs: 1,
			TotalRunningJobs:  1,
		},
		Jobs: []github.JobMessage{{
			Type:            github.MessageTypeJobStarted,
			RunnerRequestID: attempt.ClaimKey,
			RunnerID:        attempt.RunnerID,
			RunnerName:      attempt.RunnerName,
			RepositoryName:  "sparerunner",
			OwnerName:       "example-org",
			JobID:           "job-1",
			WorkflowRunID:   51,
		}},
	}
}

func testControllerRunnerCanceledRequeueMessage(
	id int,
	attempt store.GitHubJITAttempt,
) *github.Message {
	message := testControllerRunnerRequeueAvailableMessage(id)
	message.Jobs = append([]github.JobMessage{{
		Type:            github.MessageTypeJobCompleted,
		RunnerRequestID: 0,
		RunnerID:        attempt.RunnerID,
		RunnerName:      attempt.RunnerName,
		Result:          github.JobResultCanceled,
		RepositoryName:  "sparerunner",
		OwnerName:       "example-org",
		JobID:           "job-1",
		WorkflowRunID:   51,
	}}, message.Jobs...)
	return message
}

func TestControllerRunnerPrunedTerminalOnlyHistoryCleansExactProviderRunner(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startErr:     errors.New("connection lost before Start result"),
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	first := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
	)
	if _, err := first.PollAndDriveOnce(context.Background()); !errors.Is(
		err,
		ErrGitHubStartAmbiguous,
	) {
		t.Fatalf("setup start ambiguity = %v", err)
	}
	stateStore.mu.Lock()
	attempt := stateStore.attempt
	stateStore.prunedLostTerminal = domain.ExecutionFailed
	stateStore.claim.Execution.State = domain.ExecutionFailed
	stateStore.capacity = 1
	stateStore.mu.Unlock()
	agent.mu.Lock()
	agent.snapshot.Commands = nil
	agent.snapshot.Observations = nil
	agent.mu.Unlock()
	agent.setSnapshotOnline(true)
	restarted := newControllerRunnerForEpoch(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
		4,
	)
	lifecycle.observedRunner = &github.RunnerReference{
		ID:         attempt.RunnerID,
		Name:       attempt.RunnerName,
		ScaleSetID: 7,
	}
	if err := restarted.ReconcileJITAttempt(
		context.Background(),
		attempt.ClaimKey,
	); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("pruned terminal exact DELETE = %v", err)
	}
	if lifecycle.getCalls != 1 || lifecycle.removeCalls != 1 {
		t.Fatalf("pruned terminal provider cleanup calls = %d/%d",
			lifecycle.getCalls, lifecycle.removeCalls)
	}
	lifecycle.observedRunner = nil
	if err := restarted.ReconcileJITAttempt(
		context.Background(),
		attempt.ClaimKey,
	); !errors.Is(err, store.ErrGitHubJITAbsencePending) {
		t.Fatalf("pruned terminal first absence = %v", err)
	}
	if err := restarted.ReconcileJITAttempt(
		context.Background(),
		attempt.ClaimKey,
	); err != nil {
		t.Fatalf("pruned terminal confirmed absence = %v", err)
	}
	stateStore.mu.Lock()
	defer stateStore.mu.Unlock()
	if stateStore.attempt.State != store.GitHubJITReconciledAbsent ||
		stateStore.claim.State != store.GitHubClaimReconciliationRequired ||
		stateStore.claim.Execution.State != domain.ExecutionFailed ||
		stateStore.capacity != 1 {
		t.Fatalf("pruned terminal dormant result: attempt=%#v claim=%#v capacity=%d",
			stateStore.attempt, stateStore.claim, stateStore.capacity)
	}
	if lifecycle.getCalls != 3 ||
		lifecycle.removeCalls != 1 ||
		lifecycle.generateCalls != 1 ||
		agent.startCalls != 1 {
		t.Fatalf("pruned terminal duplicated work: query/remove/generate/start=%d/%d/%d/%d",
			lifecycle.getCalls,
			lifecycle.removeCalls,
			lifecycle.generateCalls,
			agent.startCalls)
	}
}

func TestControllerRunnerPersistedQuarantineStillAdvancesSameProviderFence(t *testing.T) {
	const (
		nodeID domain.NodeID          = "00000000000000000000000000000001"
		epoch  domain.ControllerEpoch = 4
	)
	execution := domain.ExecutionSnapshot{
		ID:       "github-cleanup-blocked-execution",
		TargetID: "target-1",
		Slot:     domain.SlotKey{NodeID: nodeID, Index: 0},
		State:    domain.ExecutionCleanupFailed,
	}
	start := domain.Command{
		ID:              "github-cleanup-blocked-start",
		ControllerEpoch: epoch - 1,
		ExecutionID:     execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("cleanup-blocked-start")),
	}
	attempt := store.GitHubJITAttempt{
		ScaleSetID:      7,
		ClaimKey:        7001,
		Attempt:         1,
		ControllerEpoch: epoch - 1,
		RunnerName:      deterministicRunnerName(7, 7001),
		State:           store.GitHubJITRemovalPending,
		RunnerID:        91,
		JITDigest:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		StartCommandID:  start.ID,
	}
	claim := store.GitHubJobClaim{
		ScaleSetID:      attempt.ScaleSetID,
		ClaimKey:        attempt.ClaimKey,
		Origin:          store.GitHubClaimFromJobAvailable,
		RunnerRequestID: attempt.ClaimKey,
		SourceMessageID: 41,
		Execution:       execution,
		State:           store.GitHubClaimReconciliationRequired,
		CurrentAttempt:  attempt.Attempt,
	}
	fence := reconcile.GitHubFence{
		ExecutionID: execution.ID,
		ScaleSetID:  attempt.ScaleSetID,
		ClaimKey:    attempt.ClaimKey,
		ClaimState:  claim.State,
		Attempt:     &attempt,
	}
	projection, err := reconcile.Restore(
		epoch,
		store.ControllerSnapshot{
			ControllerEpoch: epoch,
			Nodes: []store.NodeAdministration{{
				NodeID: nodeID,
				State:  domain.NodeActive,
			}},
			Reservations: []store.SlotReservation{{
				Slot: execution.Slot,
				Owner: domain.SlotOwner{
					TargetID:    execution.TargetID,
					ExecutionID: execution.ID,
				},
			}},
			Executions: []domain.ExecutionSnapshot{execution},
		},
		reconcile.Config{
			Nodes: []reconcile.NodeDefinition{{
				Node: domain.Node{
					ID:                  nodeID,
					DisplayName:         "cleanup-blocked-node",
					OS:                  domain.OSLinux,
					Architecture:        domain.ArchAMD64,
					MaxRunners:          1,
					AdministrativeState: domain.NodeActive,
					ObservedState:       domain.NodeOffline,
				},
				RunnerVersionPolicy: domain.RunnerVersionAutoUpdate,
				RunnerUpdate:        reconcile.ManagedRunnerUpdate(),
			}},
			Commands: []reconcile.IssuedCommand{{
				NodeID:  nodeID,
				Type:    domain.CommandStart,
				Command: start,
			}},
			GitHubFences: []reconcile.GitHubFence{fence},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	agentSnapshot := AgentSnapshot{
		NodeID:             nodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		RunnerVersion:      runner.OfficialRunnerVersion,
		NativeRunnerReady:  true,
		MaxControllerEpoch: epoch - 1,
		CleanupTombstones: []transport.AgentCleanupTombstone{{
			ExecutionID:        execution.ID,
			FailureCode:        domain.CleanupProcessResidue,
			RecordedAtUnixNano: 50,
		}},
	}
	result, err := projection.ReconcileAgentSnapshot(agentSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Actions) < 1 ||
		result.Actions[0].Kind != reconcile.ActionPersistQuarantine ||
		result.Scheduler.Node.AdministrativeState != domain.NodeQuarantined ||
		result.Scheduler.Reconciled ||
		len(result.SuppressedReservations) != 1 {
		t.Fatalf("cleanup-blocked projection = %#v", result)
	}
	reconciler := &recordingRealControllerRunnerReconciler{
		controller: projection,
	}
	stateStore := newRunnerCoordinatorFakeStore()
	stateStore.claim = &claim
	stateStore.attempt = attempt
	stateStore.capacity = 0
	stateStore.lostJITTerminal = domain.ExecutionCleanupFailed
	stateStore.issued[start.ID] = store.IssuedAgentCommand{
		NodeID:  nodeID,
		Type:    domain.CommandStart,
		Command: start,
	}
	agent := &runnerCoordinatorFakeAgent{
		snapshot:       agentSnapshot,
		snapshotOnline: true,
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	lifecycle.observedRunner = &github.RunnerReference{
		ID:         attempt.RunnerID,
		Name:       attempt.RunnerName,
		ScaleSetID: github.ScaleSetID(attempt.ScaleSetID),
	}
	coordinator := newControllerRunnerForEpochWithReconciler(
		t,
		stateStore,
		newRunnerCoordinatorFakeSession(nil),
		agent,
		lifecycle,
		epoch,
		reconciler,
	)

	drove, err := coordinator.DriveNext(context.Background())
	if !drove || !errors.Is(err, ErrGitHubReconciliationPending) {
		t.Fatalf("quarantined exact DELETE drive = (%t, %v)", drove, err)
	}
	if lifecycle.getCalls != 1 || lifecycle.removeCalls != 1 {
		t.Fatalf("quarantined provider cleanup calls = %d/%d",
			lifecycle.getCalls, lifecycle.removeCalls)
	}
	lifecycle.observedRunner = nil
	drove, err = coordinator.DriveNext(context.Background())
	if !drove || !errors.Is(err, ErrGitHubReconciliationPending) {
		t.Fatalf("quarantined first absence drive = (%t, %v)", drove, err)
	}
	drove, err = coordinator.DriveNext(context.Background())
	if !drove || err != nil {
		t.Fatalf("quarantined confirmed absence drive = (%t, %v)", drove, err)
	}
	if lifecycle.getCalls != 3 || lifecycle.removeCalls != 1 {
		t.Fatalf("quarantined absence calls = %d/%d",
			lifecycle.getCalls, lifecycle.removeCalls)
	}
	stateStore.mu.Lock()
	if stateStore.capacity != 0 ||
		stateStore.claim.Execution.State != domain.ExecutionCleanupFailed ||
		stateStore.attempt.State != store.GitHubJITReconciledAbsent {
		t.Fatalf("quarantine/provider result: capacity=%d claim=%#v attempt=%#v",
			stateStore.capacity, stateStore.claim, stateStore.attempt)
	}
	stateStore.mu.Unlock()
	admission, err := projection.Admission(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if admission.AllowsNewCapacity ||
		admission.Node.Node.AdministrativeState != domain.NodeQuarantined ||
		len(admission.Actions) != 1 ||
		admission.Actions[0].Kind != reconcile.ActionPersistQuarantine {
		t.Fatalf("provider cleanup cleared quarantine = %#v", admission)
	}
	fleet := projection.FleetSnapshot()
	if len(fleet.Reservations) != 1 ||
		fleet.Reservations[0].ExecutionID != execution.ID {
		t.Fatalf("provider cleanup released quarantined slot = %#v",
			fleet.Reservations)
	}
	drove, err = coordinator.DriveNext(context.Background())
	if drove || err != nil {
		t.Fatalf("quarantine without provider fence did not remain blocked = (%t, %v)",
			drove, err)
	}
	if lifecycle.getCalls != 3 || lifecycle.removeCalls != 1 {
		t.Fatalf("cleared fence repeated provider cleanup = %d/%d",
			lifecycle.getCalls, lifecycle.removeCalls)
	}
}

func TestControllerRunnerJITProviderFailuresRemainFencedAndObservable(t *testing.T) {
	tests := []struct {
		name        string
		stage       github.ProviderOperation
		configure   func(*runnerCoordinatorFakeStore, *runnerCoordinatorFakeLifecycle)
		timeout     bool
		wantClass   store.GitHubObservationFailureClass
		wantPending bool
		wantStale   bool
	}{
		{
			name:  "query 503",
			stage: github.ProviderQueryRunner,
			configure: func(
				_ *runnerCoordinatorFakeStore,
				lifecycle *runnerCoordinatorFakeLifecycle,
			) {
				lifecycle.getErr = &github.ProviderHTTPStatusError{
					StatusCode: 503,
					Err:        errors.New("provider unavailable"),
				}
			},
			wantClass: store.GitHubObservationProvider5xx,
			wantStale: true,
		},
		{
			name:  "remove network failure",
			stage: github.ProviderRemoveRunner,
			configure: func(
				state *runnerCoordinatorFakeStore,
				lifecycle *runnerCoordinatorFakeLifecycle,
			) {
				lifecycle.observedRunner = &github.RunnerReference{
					ID:         state.attempt.RunnerID,
					Name:       state.attempt.RunnerName,
					ScaleSetID: github.ScaleSetID(state.attempt.ScaleSetID),
				}
				lifecycle.removeErr = &net.OpError{
					Op:  "write",
					Net: "tcp",
					Err: errors.New("connection reset"),
				}
			},
			wantClass:   store.GitHubObservationNetwork,
			wantPending: true,
			wantStale:   true,
		},
		{
			name:  "query timeout",
			stage: github.ProviderQueryRunner,
			configure: func(
				_ *runnerCoordinatorFakeStore,
				lifecycle *runnerCoordinatorFakeLifecycle,
			) {
				lifecycle.query = func(
					ctx context.Context,
					_ github.RunnerQuery,
				) (*github.RunnerReference, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
			},
			timeout:   true,
			wantClass: store.GitHubObservationTimeout,
			wantStale: true,
		},
		{
			name:  "intentional cancellation is not stale",
			stage: github.ProviderQueryRunner,
			configure: func(
				_ *runnerCoordinatorFakeStore,
				lifecycle *runnerCoordinatorFakeLifecycle,
			) {
				lifecycle.getErr = context.Canceled
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			coordinator, stateStore, _, lifecycle :=
				newControllerRunnerGeneratedAttemptForReconciliation(t)
			testCase.configure(stateStore, lifecycle)
			if testCase.timeout {
				coordinator.finiteOperationContext = func(
					ctx context.Context,
				) (context.Context, context.CancelFunc) {
					return context.WithTimeout(ctx, 10*time.Millisecond)
				}
			}
			err := coordinator.ReconcileJITAttempt(
				context.Background(), 7001)
			if !errors.Is(err, ErrGitHubReconciliationRequired) {
				t.Fatalf("reconciliation error = %v", err)
			}
			var providerFailure *github.ProviderFailure
			if !errors.As(err, &providerFailure) ||
				providerFailure.Operation != testCase.stage {
				t.Fatalf("provider failure = %#v", providerFailure)
			}
			if testCase.wantStale {
				if len(stateStore.sessionFailures) != 1 ||
					stateStore.sessionFailures[0] != testCase.wantClass ||
					stateStore.runtimeFreshness.Session.Freshness != store.RuntimeFreshnessStale {
					t.Fatalf("provider failure not persisted: failures=%v session=%#v",
						stateStore.sessionFailures, stateStore.runtimeFreshness.Session)
				}
			} else if len(stateStore.sessionFailures) != 0 {
				t.Fatalf("intentional cancellation marked stale: %v",
					stateStore.sessionFailures)
			}
			if testCase.wantPending {
				if stateStore.attempt.State != store.GitHubJITRemovalPending ||
					lifecycle.removeCalls != 1 {
					t.Fatalf("failed removal lost fence: %#v calls=%d",
						stateStore.attempt, lifecycle.removeCalls)
				}
			} else if stateStore.attempt.State != store.GitHubJITGenerated {
				t.Fatalf("failed query changed fence: %#v", stateStore.attempt)
			}
		})
	}
}

func TestGitHubFailureClassificationUsesTerminalNetworkErrorOverPriorStatus(t *testing.T) {
	terminal := &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: errors.New("connection reset"),
	}
	failure := &github.ProviderFailure{
		Operation: github.ProviderQueryRunner,
		Err: &github.ProviderHTTPStatusError{
			StatusCode: 401,
			Err:        terminal,
		},
	}
	if got := ClassifyGitHubObservationFailure(failure); got != store.GitHubObservationNetwork {
		t.Fatalf("terminal network error classified as %q", got)
	}
	failure.Err = &github.ProviderHTTPStatusError{
		StatusCode: 503,
		Err:        context.DeadlineExceeded,
	}
	if got := ClassifyGitHubObservationFailure(failure); got != store.GitHubObservationTimeout {
		t.Fatalf("terminal deadline classified as %q", got)
	}
}

func TestControllerRunnerRunDrivesRecoveredWorkBeforeLongPoll(t *testing.T) {
	tests := []struct {
		name         string
		claimState   store.GitHubClaimState
		execution    domain.ExecutionState
		wantPrepares int
	}{
		{
			name:         "acquired",
			claimState:   store.GitHubClaimAcquired,
			execution:    domain.ExecutionReserved,
			wantPrepares: 1,
		},
		{
			name:       "preparing",
			claimState: store.GitHubClaimPreparing,
			execution:  domain.ExecutionPreparing,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			stateStore := newRunnerCoordinatorFakeStore()
			session := newRunnerCoordinatorFakeSession(
				testControllerRunnerMessage())
			agent := &runnerCoordinatorFakeAgent{
				prepareState: domain.ExecutionPreparing,
				startState:   domain.ExecutionRunning,
			}
			coordinator := newControllerRunnerForTest(
				t, stateStore, session, agent,
				newRunnerCoordinatorFakeLifecycle())
			if _, err := coordinator.PollOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			stateStore.mu.Lock()
			stateStore.claim.State = testCase.claimState
			stateStore.claim.Execution.State = testCase.execution
			stateStore.acquireDone = testCase.claimState != store.GitHubClaimPending
			stateStore.mu.Unlock()

			started := make(chan struct{})
			var startOnce sync.Once
			agent.afterStart = func() { startOnce.Do(func() { close(started) }) }
			pollEntered := make(chan bool, 1)
			session.mu.Lock()
			session.message = nil
			session.pollCapacities = nil
			session.poll = func(ctx context.Context, _ int) (*github.Message, error) {
				select {
				case <-started:
					pollEntered <- true
				default:
					pollEntered <- false
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			session.mu.Unlock()

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- coordinator.Run(ctx) }()
			select {
			case startWasFirst := <-pollEntered:
				if !startWasFirst {
					cancel()
					t.Fatal("long poll started before recovered runner")
				}
			case <-time.After(time.Second):
				cancel()
				t.Fatal("Run did not reach post-recovery poll")
			}
			cancel()
			if err := <-result; err != nil {
				t.Fatalf("Run result = %v", err)
			}
			if agent.prepareCalls != testCase.wantPrepares ||
				agent.startCalls != 1 ||
				stateStore.claim.State != store.GitHubClaimRunning {
				t.Fatalf("recovered claim = prepare:%d start:%d claim:%#v",
					agent.prepareCalls, agent.startCalls, stateStore.claim)
			}
		})
	}
}

func TestControllerRunnerRunKeepsUnacknowledgedPendingClaimPollFirst(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	coordinator := newControllerRunnerForTest(
		t, stateStore, session, agent, newRunnerCoordinatorFakeLifecycle())
	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	pollEntered := make(chan struct{}, 1)
	session.mu.Lock()
	session.message = nil
	session.pollCapacities = nil
	session.poll = func(ctx context.Context, _ int) (*github.Message, error) {
		pollEntered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	session.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- coordinator.Run(ctx) }()
	select {
	case <-pollEntered:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("pending recovery did not wait for provider redelivery")
	}
	if agent.prepareCalls != 0 || agent.startCalls != 0 ||
		session.acquireCalls != 0 {
		cancel()
		t.Fatalf("pending claim drove before redelivery: prepare=%d start=%d acquire=%d",
			agent.prepareCalls, agent.startCalls, session.acquireCalls)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run result = %v", err)
	}
}

func TestControllerRunnerRestartDispatchesReconciledReplacementBeforeLongPoll(
	t *testing.T,
) {
	stateStore := newRunnerCoordinatorFakeStore()
	execution := domain.ExecutionSnapshot{
		ID:       "reconciled-replacement-execution",
		TargetID: "target-1",
		Slot: domain.SlotKey{
			NodeID: "00000000000000000000000000000001",
			Index:  0,
		},
		State: domain.ExecutionReserved,
	}
	stateStore.claim = &store.GitHubJobClaim{
		ScaleSetID:      7,
		ClaimKey:        7001,
		Origin:          store.GitHubClaimFromJobAvailable,
		RunnerRequestID: 7001,
		SourceMessageID: 91,
		Execution:       execution,
		State:           store.GitHubClaimPending,
		CurrentAttempt:  1,
	}
	stateStore.acquireAttempt = store.GitHubAcquireAttempt{
		ScaleSetID:      7,
		ClaimKey:        7001,
		Attempt:         2,
		EvidenceMessage: 91,
		ControllerEpoch: 3,
	}
	stateStore.attempt = store.GitHubJITAttempt{
		ScaleSetID:      7,
		ClaimKey:        7001,
		Attempt:         1,
		ControllerEpoch: 3,
		RunnerName:      "sparerunner-reconciled-old",
		State:           store.GitHubJITReconciledAbsent,
	}
	stateStore.capacity = 0
	stateStore.reconciledPending = true

	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	started := make(chan struct{})
	var startOnce sync.Once
	agent.afterStart = func() { startOnce.Do(func() { close(started) }) }
	session := newRunnerCoordinatorFakeSession(nil)
	pollEntered := make(chan bool, 1)
	session.poll = func(ctx context.Context, _ int) (*github.Message, error) {
		select {
		case <-started:
			pollEntered <- true
		default:
			pollEntered <- false
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	coordinator := newControllerRunnerForEpoch(
		t,
		stateStore,
		session,
		agent,
		newRunnerCoordinatorFakeLifecycle(),
		4,
	)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- coordinator.Run(ctx) }()
	select {
	case startWasFirst := <-pollEntered:
		if !startWasFirst {
			cancel()
			t.Fatal("reconciled replacement entered long poll before Start")
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("reconciled replacement did not reach the post-Start poll")
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run result = %v", err)
	}
	if session.acquireCalls != 1 ||
		agent.prepareCalls != 1 ||
		agent.startCalls != 1 {
		t.Fatalf(
			"restart dispatch calls = acquire:%d prepare:%d start:%d, want 1/1/1",
			session.acquireCalls,
			agent.prepareCalls,
			agent.startCalls,
		)
	}
}

func TestControllerRunnerFiniteMutationDeadlinesPersistTimeoutAndKeepFence(t *testing.T) {
	tests := []struct {
		name      string
		operation github.ProviderOperation
		configure func(
			*runnerCoordinatorFakeStore,
			*runnerCoordinatorFakeSession,
			*runnerCoordinatorFakeLifecycle,
		)
		wantClaim store.GitHubClaimState
	}{
		{
			name:      "acquire",
			operation: github.ProviderAcquireJobs,
			configure: func(
				_ *runnerCoordinatorFakeStore,
				session *runnerCoordinatorFakeSession,
				_ *runnerCoordinatorFakeLifecycle,
			) {
				session.acquire = func(
					ctx context.Context,
					_ []int64,
				) ([]int64, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
			},
			wantClaim: store.GitHubClaimAcquireAmbiguous,
		},
		{
			name:      "generate JIT",
			operation: github.ProviderGenerateJIT,
			configure: func(
				state *runnerCoordinatorFakeStore,
				_ *runnerCoordinatorFakeSession,
				lifecycle *runnerCoordinatorFakeLifecycle,
			) {
				state.claim.State = store.GitHubClaimPreparing
				state.claim.Execution.State = domain.ExecutionPreparing
				lifecycle.generate = func(
					ctx context.Context,
					_ github.JITRequest,
				) (runner.JITConfig, github.RunnerReference, error) {
					<-ctx.Done()
					return nil, github.RunnerReference{}, ctx.Err()
				}
			},
			wantClaim: store.GitHubClaimJITGenerationAmbiguous,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			stateStore := newRunnerCoordinatorFakeStore()
			session := newRunnerCoordinatorFakeSession(
				testControllerRunnerMessage())
			lifecycle := newRunnerCoordinatorFakeLifecycle()
			coordinator := newControllerRunnerForTest(
				t, stateStore, session,
				&runnerCoordinatorFakeAgent{
					prepareState: domain.ExecutionPreparing,
					startState:   domain.ExecutionRunning,
				},
				lifecycle,
			)
			if _, err := coordinator.PollOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			testCase.configure(stateStore, session, lifecycle)
			coordinator.finiteOperationContext = func(
				ctx context.Context,
			) (context.Context, context.CancelFunc) {
				return context.WithTimeout(ctx, 10*time.Millisecond)
			}
			_, err := coordinator.DriveNext(context.Background())
			var providerFailure *github.ProviderFailure
			if !errors.As(err, &providerFailure) ||
				providerFailure.Operation != testCase.operation {
				t.Fatalf("deadline error = %v", err)
			}
			if len(stateStore.sessionFailures) != 1 ||
				stateStore.sessionFailures[0] != store.GitHubObservationTimeout ||
				stateStore.runtimeFreshness.Session.Freshness != store.RuntimeFreshnessStale ||
				stateStore.claim.State != testCase.wantClaim {
				t.Fatalf("deadline state = failures:%v freshness:%#v claim:%#v",
					stateStore.sessionFailures,
					stateStore.runtimeFreshness.Session,
					stateStore.claim)
			}
		})
	}
}

func TestControllerRunnerRunRetriesJITProviderFailureBeforePolling(t *testing.T) {
	coordinator, stateStore, _, lifecycle :=
		newControllerRunnerGeneratedAttemptForReconciliation(t)
	coordinator.reconciliationRetry = time.Millisecond
	var queryCalls atomic.Int32
	lifecycle.query = func(
		_ context.Context,
		_ github.RunnerQuery,
	) (*github.RunnerReference, error) {
		if queryCalls.Add(1) == 1 {
			return nil, &github.ProviderHTTPStatusError{
				StatusCode: 503,
				Err:        errors.New("provider unavailable"),
			}
		}
		return nil, nil
	}
	session := coordinator.session.(*runnerCoordinatorFakeSession)
	pollEntered := make(chan struct{}, 1)
	session.mu.Lock()
	session.message = nil
	session.poll = func(ctx context.Context, _ int) (*github.Message, error) {
		pollEntered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	session.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- coordinator.Run(ctx) }()
	select {
	case <-pollEntered:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("Run did not retry JIT provider failure")
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run result = %v", err)
	}
	if queryCalls.Load() < 2 ||
		len(stateStore.sessionFailures) != 1 ||
		stateStore.sessionFailures[0] != store.GitHubObservationProvider5xx ||
		stateStore.attempt.State != store.GitHubJITReconciledAbsent {
		t.Fatalf("retry state = queries:%d failures:%v attempt:%#v",
			queryCalls.Load(), stateStore.sessionFailures, stateStore.attempt)
	}
}

func TestControllerRunnerRunCompletesTwoReadAbsenceBeforeLongPoll(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startState:   domain.ExecutionRunning,
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	lifecycle.generateErrors = []error{
		errors.New("JIT API failed before a registration was created"),
	}
	first := newControllerRunnerForTest(
		t, stateStore, session, agent, lifecycle)
	if _, err := first.PollAndDriveOnce(
		context.Background(),
	); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("generation ambiguity setup = %v", err)
	}
	agent.setSnapshotOnline(true)
	restarted := newControllerRunnerForEpoch(
		t, stateStore, session, agent, lifecycle, 4)
	restarted.reconciliationRetry = time.Millisecond

	pollEntered := make(chan struct{}, 1)
	session.mu.Lock()
	session.message = nil
	session.poll = func(ctx context.Context, _ int) (*github.Message, error) {
		pollEntered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	session.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- restarted.Run(ctx) }()
	select {
	case <-pollEntered:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("Run did not reach poll after two-read reconciliation")
	}
	stateStore.mu.Lock()
	claim := cloneRunnerCoordinatorClaim(stateStore.claim)
	attempt := stateStore.attempt
	absenceReads := stateStore.generationAbsences
	stateStore.mu.Unlock()
	if claim == nil || claim.State != store.GitHubClaimPreparing ||
		attempt.State != store.GitHubJITReconciledAbsent ||
		absenceReads != 2 || lifecycle.getCalls != 2 ||
		lifecycle.generateCalls != 1 || agent.startCalls != 0 {
		cancel()
		t.Fatalf(
			"pre-poll reconciliation = claim:%#v attempt:%#v absences:%d queries:%d generations:%d starts:%d",
			claim,
			attempt,
			absenceReads,
			lifecycle.getCalls,
			lifecycle.generateCalls,
			agent.startCalls,
		)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run result = %v", err)
	}
}

func TestControllerRunnerAcceptedStartAfterRemovalPendingBlocksProviderAbsence(t *testing.T) {
	coordinator, stateStore, agent, lifecycle :=
		newControllerRunnerGeneratedAttemptForReconciliation(t)
	lifecycle.observedRunner = &github.RunnerReference{
		ID:         stateStore.attempt.RunnerID,
		Name:       stateStore.attempt.RunnerName,
		ScaleSetID: 7,
	}
	if err := coordinator.ReconcileJITAttempt(context.Background(), 7001); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("runner removal reconciliation error = %v", err)
	}
	if stateStore.attempt.State != store.GitHubJITRemovalPending || lifecycle.removeCalls != 1 {
		t.Fatalf("removal was not durably pending: attempt=%#v removals=%d",
			stateStore.attempt, lifecycle.removeCalls)
	}

	attempt := stateStore.attempt
	acceptedStart := domain.Command{
		ID:              attempt.StartCommandID,
		ControllerEpoch: attempt.ControllerEpoch,
		ExecutionID:     stateStore.claim.Execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("accepted-after-removal-pending")),
	}
	stateStore.issued[acceptedStart.ID] = store.IssuedAgentCommand{
		NodeID:  stateStore.claim.Execution.Slot.NodeID,
		Type:    domain.CommandStart,
		Command: acceptedStart,
	}
	agent.snapshot = AgentSnapshot{
		NodeID:             stateStore.claim.Execution.Slot.NodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: attempt.ControllerEpoch,
		Commands:           []domain.Command{acceptedStart},
	}
	// Model a provider read that would otherwise prove absence. Exact accepted
	// Start authority after the removal fence must block that provider path.
	lifecycle.observedRunner = nil
	if err := coordinator.ReconcileJITAttempt(context.Background(), 7001); !errors.Is(err, ErrGitHubReconciliationRequired) {
		t.Fatalf("accepted Start after removal pending error = %v", err)
	}
	if stateStore.attempt.State != store.GitHubJITRemovalPending ||
		stateStore.claim.State != store.GitHubClaimReconciliationRequired {
		t.Fatalf("accepted Start changed pending removal state: attempt=%#v claim=%#v",
			stateStore.attempt, stateStore.claim)
	}
	if lifecycle.getCalls != 1 || lifecycle.removeCalls != 1 {
		t.Fatalf("accepted Start reached provider reconciliation: lookups/removals=%d/%d",
			lifecycle.getCalls, lifecycle.removeCalls)
	}
}

func newControllerRunnerGeneratedAttemptForReconciliation(
	t *testing.T,
) (*ControllerRunnerCoordinator, *runnerCoordinatorFakeStore, *runnerCoordinatorFakeAgent, *runnerCoordinatorFakeLifecycle) {
	t.Helper()
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		prepareState: domain.ExecutionPreparing,
		startErr:     errors.New("connection lost before Start result"),
	}
	lifecycle := newRunnerCoordinatorFakeLifecycle()
	first := newControllerRunnerForTest(t, stateStore, session, agent, lifecycle)
	if _, err := first.PollAndDriveOnce(context.Background()); !errors.Is(err, ErrGitHubStartAmbiguous) {
		t.Fatalf("setup start ambiguity error = %v", err)
	}

	// A crash after JIT generation but before Start dispatch leaves a generated
	// runner identity that a later Controller epoch must reconcile.
	stateStore.attempt.State = store.GitHubJITGenerated
	stateStore.claim.State = store.GitHubClaimJITGenerated
	agent.setSnapshotOnline(true)
	return newControllerRunnerForEpoch(t, stateStore, session, agent, lifecycle, 4),
		stateStore, agent, lifecycle
}

func newControllerRunnerForTest(
	t *testing.T,
	stateStore controllerRunnerStore,
	session controllerRunnerMessageSession,
	agent controllerRunnerAgent,
	lifecycle ControllerRunnerLifecycle,
) *ControllerRunnerCoordinator {
	t.Helper()
	return newControllerRunnerForEpoch(t, stateStore, session, agent, lifecycle, 3)
}

func newControllerRunnerForEpoch(
	t *testing.T,
	stateStore controllerRunnerStore,
	session controllerRunnerMessageSession,
	agent controllerRunnerAgent,
	lifecycle ControllerRunnerLifecycle,
	controllerEpoch domain.ControllerEpoch,
) *ControllerRunnerCoordinator {
	return newControllerRunnerForEpochWithReconciler(
		t,
		stateStore,
		session,
		agent,
		lifecycle,
		controllerEpoch,
		acceptingControllerRunnerReconciler{},
	)
}

func newControllerRunnerForEpochWithReconciler(
	t *testing.T,
	stateStore controllerRunnerStore,
	session controllerRunnerMessageSession,
	agent controllerRunnerAgent,
	lifecycle ControllerRunnerLifecycle,
	controllerEpoch domain.ControllerEpoch,
	reconciler controllerRunnerReconciler,
) *ControllerRunnerCoordinator {
	t.Helper()
	if fakeStore, ok := stateStore.(*runnerCoordinatorFakeStore); ok {
		fakeStore.mu.Lock()
		fakeStore.controllerEpoch = controllerEpoch
		fakeStore.mu.Unlock()
	}
	if fakeAgent, ok := agent.(*runnerCoordinatorFakeAgent); ok &&
		fakeAgent.snapshot.NodeID == "" && !fakeAgent.snapshotOnline {
		fakeAgent.snapshot = AgentSnapshot{
			NodeID:            "00000000000000000000000000000001",
			OS:                domain.OSLinux,
			Arch:              domain.ArchAMD64,
			RunnerVersion:     runner.OfficialRunnerVersion,
			NativeRunnerReady: true,
		}
		fakeAgent.snapshotOnline = true
	}
	if fakeAgent, ok := agent.(*runnerCoordinatorFakeAgent); ok {
		if fakeAgent.snapshot.RunnerVersion == "" {
			fakeAgent.snapshot.RunnerVersion = runner.OfficialRunnerVersion
		}
		if fakeStore, ok := stateStore.(*runnerCoordinatorFakeStore); ok {
			fakeAgent.onSnapshotChange = func(snapshot AgentSnapshot) {
				fakeStore.setPollAgentSnapshotWithoutTest(
					snapshot,
					controllerEpoch,
				)
			}
		}
	}
	if fakeStore, ok := stateStore.(*runnerCoordinatorFakeStore); ok {
		snapshot, _, _ := agent.Readiness("00000000000000000000000000000001")
		if snapshot.NodeID != "" {
			fakeStore.setPollAgentSnapshot(t, snapshot, controllerEpoch)
		}
	}
	coordinator, err := NewControllerRunnerCoordinator(
		stateStore, session, agent, lifecycle,
		ControllerRunnerConfig{
			ScaleSetID:      7,
			TargetID:        "target-1",
			Scope:           "owner/repo",
			ScopeKind:       domain.TargetRepository,
			RunnerProfileID: "profile-1",
			VersionPolicy:   domain.RunnerVersionAutoUpdate,
			NodeID:          "00000000000000000000000000000001",
			ControllerEpoch: controllerEpoch,
			Reconciler:      reconciler,
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

type acceptingControllerRunnerReconciler struct{}

type recordingRealControllerRunnerReconciler struct {
	mu         sync.Mutex
	controller *reconcile.Controller
	applied    []reconcile.GitHubFence
	cleared    []reconcile.GitHubFence
}

func (reconciler *recordingRealControllerRunnerReconciler) Admission(
	nodeID domain.NodeID,
) (reconcile.NodeAdmission, error) {
	return reconciler.controller.Admission(nodeID)
}

func (reconciler *recordingRealControllerRunnerReconciler) ApplyGitHubClaim(
	claim store.GitHubJobClaim,
) error {
	return reconciler.controller.ApplyGitHubClaim(claim)
}

func (reconciler *recordingRealControllerRunnerReconciler) ApplyGitHubFence(
	fence reconcile.GitHubFence,
) error {
	if err := reconciler.controller.ApplyGitHubFence(fence); err != nil {
		return err
	}
	reconciler.mu.Lock()
	reconciler.applied = append(reconciler.applied, fence)
	reconciler.mu.Unlock()
	return nil
}

func (reconciler *recordingRealControllerRunnerReconciler) ClearGitHubFence(
	fence reconcile.GitHubFence,
) error {
	if err := reconciler.controller.ClearGitHubFence(fence); err != nil {
		return err
	}
	reconciler.mu.Lock()
	reconciler.cleared = append(reconciler.cleared, fence)
	reconciler.mu.Unlock()
	return nil
}

func (reconciler *recordingRealControllerRunnerReconciler) ApplyDesiredExecution(
	execution domain.ExecutionSnapshot,
) error {
	return reconciler.controller.ApplyDesiredExecution(execution)
}

func (acceptingControllerRunnerReconciler) Admission(
	domain.NodeID,
) (reconcile.NodeAdmission, error) {
	return reconcile.NodeAdmission{
		AllowsNewCapacity: true,
		AllowsRecovery:    true,
	}, nil
}

func (acceptingControllerRunnerReconciler) ApplyGitHubClaim(store.GitHubJobClaim) error {
	return nil
}

func (acceptingControllerRunnerReconciler) ApplyGitHubFence(reconcile.GitHubFence) error {
	return nil
}

func (acceptingControllerRunnerReconciler) ClearGitHubFence(
	reconcile.GitHubFence,
) error {
	return nil
}

func (acceptingControllerRunnerReconciler) ApplyDesiredExecution(
	domain.ExecutionSnapshot,
) error {
	return nil
}

func testControllerRunnerMessage() *github.Message {
	return &github.Message{
		ScaleSetID: 7,
		ID:         41,
		Statistics: github.Statistics{TotalAvailableJobs: 1},
		Jobs: []github.JobMessage{
			{Type: github.MessageTypeJobAvailable, RunnerRequestID: 7001, RepositoryName: "sparerunner", OwnerName: "example-org", JobID: "job-1", WorkflowRunID: 51},
			{Type: github.MessageTypeJobAssigned, RunnerRequestID: 7002, RepositoryName: "sparerunner", OwnerName: "example-org", JobID: "job-2", WorkflowRunID: 52},
			{Type: github.MessageTypeJobStarted, RunnerRequestID: 7003, RunnerID: 91, RunnerName: "existing-runner", RepositoryName: "sparerunner", OwnerName: "example-org", JobID: "job-3", WorkflowRunID: 53},
			{Type: github.MessageTypeJobCompleted, RunnerRequestID: 7004, RunnerID: 92, RunnerName: "complete-runner", Result: "succeeded", RepositoryName: "sparerunner", OwnerName: "example-org", JobID: "job-4", WorkflowRunID: 54},
		},
	}
}

type runnerCoordinatorFakeSession struct {
	mu             sync.Mutex
	message        *github.Message
	snapshot       github.SessionSnapshot
	deleteErrors   []error
	acquired       []int64
	acquireErr     error
	acquire        func(context.Context, []int64) ([]int64, error)
	beforeDelete   func()
	afterPoll      func()
	afterAcquire   func()
	poll           func(context.Context, int) (*github.Message, error)
	deleteCalls    int
	acquireCalls   int
	pollCapacities []int
}

func newRunnerCoordinatorFakeSession(message *github.Message) *runnerCoordinatorFakeSession {
	return &runnerCoordinatorFakeSession{
		message: message,
		snapshot: github.SessionSnapshot{
			ScaleSetID: 7,
			ID:         "session-1",
			Statistics: github.Statistics{},
		},
		acquired: []int64{7001},
	}
}

func (session *runnerCoordinatorFakeSession) Snapshot() (github.SessionSnapshot, error) {
	return session.snapshot, nil
}

func (session *runnerCoordinatorFakeSession) Poll(ctx context.Context, _ int, capacity int) (*github.Message, error) {
	session.mu.Lock()
	session.pollCapacities = append(session.pollCapacities, capacity)
	poll := session.poll
	if poll != nil {
		session.mu.Unlock()
		return poll(ctx, capacity)
	}
	if session.message == nil {
		session.mu.Unlock()
		return nil, nil
	}
	copyMessage := *session.message
	copyMessage.Jobs = append([]github.JobMessage(nil), session.message.Jobs...)
	if session.afterPoll != nil {
		session.afterPoll()
	}
	session.mu.Unlock()
	return &copyMessage, nil
}

func (session *runnerCoordinatorFakeSession) DeleteMessage(context.Context, int) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.deleteCalls++
	if session.beforeDelete != nil {
		session.beforeDelete()
	}
	if len(session.deleteErrors) == 0 {
		return nil
	}
	err := session.deleteErrors[0]
	session.deleteErrors = session.deleteErrors[1:]
	return err
}

func (session *runnerCoordinatorFakeSession) AcquireJobs(
	ctx context.Context,
	runnerRequestIDs []int64,
) ([]int64, error) {
	session.mu.Lock()
	session.acquireCalls++
	acquire := session.acquire
	acquired := append([]int64(nil), session.acquired...)
	acquireErr := session.acquireErr
	if session.afterAcquire != nil {
		session.afterAcquire()
	}
	session.mu.Unlock()
	if acquire != nil {
		return acquire(ctx, runnerRequestIDs)
	}
	return acquired, acquireErr
}

type runnerCoordinatorFakeStore struct {
	mu                 sync.Mutex
	nodes              []domain.NodeID
	capacity           int
	auditHealthy       bool
	auditChange        chan struct{}
	controllerEpoch    domain.ControllerEpoch
	runtimeFreshness   store.GitHubRuntimeFreshness
	agentAuthority     store.GitHubAgentPollAuthority
	sessionSuccesses   int
	sessionFailures    []store.GitHubObservationFailureClass
	demand             *store.GitHubSessionDemand
	message            *store.GitHubQueueMessage
	messages           map[store.MessageID]store.GitHubQueueMessage
	claim              *store.GitHubJobClaim
	acquireAttempt     store.GitHubAcquireAttempt
	acquireDispatching bool
	acquireDone        bool
	reconciledPending  bool
	attempt            store.GitHubJITAttempt
	generationAbsences int
	lostJITTerminal    domain.ExecutionState
	durableStartProof  bool
	durableTerminal    bool
	prunedStarted      bool
	prunedLostTerminal domain.ExecutionState
	requeueIntent      *store.GitHubUnpickedRequeueIntent
	replacementCreates int
	removalIssued      bool
	runnerAbsences     int
	issued             map[domain.CommandID]store.IssuedAgentCommand
	commits            int
	replays            int
	events             int
	assignedDemand     store.GitHubAssignedDemandResult
	demandBindings     []store.SingleSlotBinding
}

func newRunnerCoordinatorFakeStore() *runnerCoordinatorFakeStore {
	return &runnerCoordinatorFakeStore{
		capacity:     1,
		auditHealthy: true,
		auditChange:  make(chan struct{}),
		issued:       make(map[domain.CommandID]store.IssuedAgentCommand),
		messages:     make(map[store.MessageID]store.GitHubQueueMessage),
		runtimeFreshness: store.GitHubRuntimeFreshness{
			Binding: store.GitHubTargetRuntimeBinding{
				TargetID:   "target-1",
				ScaleSetID: 7,
				ProfileID:  "profile-1",
			},
			Profile: store.RunnerProfileUpdatePolicy{
				ProfileID:     "profile-1",
				VersionPolicy: domain.RunnerVersionAutoUpdate,
				RunnerVersion: runner.OfficialRunnerVersion,
				Revision:      1,
			},
			Release: store.GitHubRunnerReleaseState{
				Freshness: store.RuntimeFreshnessUnknown,
			},
			Session: store.GitHubScaleSetSessionHealth{
				ScaleSetID:            7,
				Freshness:             store.RuntimeFreshnessFresh,
				LastSuccessAtUnixNano: time.Unix(100, 0).UnixNano(),
				TransitionGeneration:  1,
			},
		},
	}
}

func (state *runnerCoordinatorFakeStore) ManagementAuditHealthy() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.auditHealthy
}

func (state *runnerCoordinatorFakeStore) ReadManagementConfiguration(
	context.Context,
) (store.ManagementConfiguration, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	configuration := store.ManagementConfiguration{}
	for _, nodeID := range state.nodes {
		configuration.Nodes = append(
			configuration.Nodes,
			store.ManagementNodeConfiguration{NodeID: nodeID, MaxRunners: 1},
		)
	}
	return configuration, nil
}

func (state *runnerCoordinatorFakeStore) ManagementAuditChange() <-chan struct{} {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.auditChange
}

func (state *runnerCoordinatorFakeStore) degradeManagementAudit() {
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.auditHealthy {
		return
	}
	state.auditHealthy = false
	close(state.auditChange)
}

func (state *runnerCoordinatorFakeStore) setPollAgentSnapshot(
	t *testing.T,
	snapshot AgentSnapshot,
	controllerEpoch domain.ControllerEpoch,
) {
	t.Helper()
	if err := state.setPollAgentSnapshotValue(snapshot, controllerEpoch); err != nil {
		t.Fatal(err)
	}
}

func (state *runnerCoordinatorFakeStore) setPollAgentSnapshotWithoutTest(
	snapshot AgentSnapshot,
	controllerEpoch domain.ControllerEpoch,
) {
	if err := state.setPollAgentSnapshotValue(snapshot, controllerEpoch); err != nil {
		panic(err)
	}
}

func (state *runnerCoordinatorFakeStore) setPollAgentSnapshotValue(
	snapshot AgentSnapshot,
	controllerEpoch domain.ControllerEpoch,
) error {
	digest, err := transport.AgentSnapshotDigest(snapshot)
	if err != nil {
		return err
	}
	state.mu.Lock()
	state.agentAuthority = store.GitHubAgentPollAuthority{
		NodeID:                    snapshot.NodeID,
		HasSnapshot:               true,
		Revision:                  state.agentAuthority.Revision + 1,
		SnapshotDigest:            digest,
		AcceptedByControllerEpoch: controllerEpoch,
		RunnerVersion:             snapshot.RunnerVersion,
		NativeRunnerReady:         snapshot.NativeRunnerReady,
	}
	state.mu.Unlock()
	return nil
}

func (state *runnerCoordinatorFakeStore) ReadGitHubPollState(
	_ context.Context,
	binding store.GitHubTargetRuntimeBinding,
	nodeID domain.NodeID,
) (store.GitHubPollState, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if binding != state.runtimeFreshness.Binding ||
		nodeID != state.agentAuthority.NodeID {
		return store.GitHubPollState{}, store.ErrRuntimeFreshnessBindingMismatch
	}
	authority := store.GitHubPollClaimAuthority{
		Binding:                     binding,
		ProfileRevision:             state.runtimeFreshness.Profile.Revision,
		VersionPolicy:               state.runtimeFreshness.Profile.VersionPolicy,
		RunnerVersion:               state.runtimeFreshness.Profile.RunnerVersion,
		SessionTransitionGeneration: state.runtimeFreshness.Session.TransitionGeneration,
		Agent:                       state.agentAuthority,
		ControllerEpoch:             state.controllerEpoch,
	}
	if authority.VersionPolicy == domain.RunnerVersionPinned {
		authority.ReleaseGeneration = state.runtimeFreshness.Release.Generation
	}
	return store.GitHubPollState{
		Runtime:        state.runtimeFreshness,
		ClaimAuthority: authority,
	}, nil
}

func (state *runnerCoordinatorFakeStore) RecordGitHubScaleSetSessionSuccess(
	_ context.Context,
	scaleSetID store.ScaleSetID,
) (store.GitHubScaleSetSessionHealth, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	health := state.runtimeFreshness.Session
	if health.ScaleSetID != scaleSetID {
		return store.GitHubScaleSetSessionHealth{}, store.ErrRuntimeFreshnessBindingMismatch
	}
	state.sessionSuccesses++
	if health.Freshness != store.RuntimeFreshnessFresh {
		health.TransitionGeneration++
	}
	if health.TransitionGeneration == 0 {
		health.TransitionGeneration = 1
	}
	health.Freshness = store.RuntimeFreshnessFresh
	health.LastSuccessAtUnixNano++
	if health.LastSuccessAtUnixNano <= 0 {
		health.LastSuccessAtUnixNano = time.Unix(100, 0).UnixNano()
	}
	health.FailureClass = ""
	health.FailureAtUnixNano = 0
	state.runtimeFreshness.Session = health
	return health, nil
}

func (state *runnerCoordinatorFakeStore) RecordGitHubScaleSetSessionFailure(
	_ context.Context,
	scaleSetID store.ScaleSetID,
	class store.GitHubObservationFailureClass,
) (store.GitHubScaleSetSessionHealth, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	health := state.runtimeFreshness.Session
	if health.ScaleSetID != scaleSetID {
		return store.GitHubScaleSetSessionHealth{}, store.ErrRuntimeFreshnessBindingMismatch
	}
	state.sessionFailures = append(state.sessionFailures, class)
	if health.Freshness == store.RuntimeFreshnessFresh {
		health.TransitionGeneration++
		health.Freshness = store.RuntimeFreshnessStale
	}
	if health.TransitionGeneration == 0 {
		health.TransitionGeneration = 1
	}
	health.FailureClass = class
	health.FailureAtUnixNano++
	if health.FailureAtUnixNano <= 0 {
		health.FailureAtUnixNano = time.Unix(100, 0).UnixNano()
	}
	state.runtimeFreshness.Session = health
	return health, nil
}

func (state *runnerCoordinatorFakeStore) ReadGitHubScaleSetSessionHealth(
	_ context.Context,
	scaleSetID store.ScaleSetID,
) (store.GitHubScaleSetSessionHealth, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	health := state.runtimeFreshness.Session
	if health.ScaleSetID != scaleSetID {
		return store.GitHubScaleSetSessionHealth{},
			store.ErrRuntimeFreshnessBindingMismatch
	}
	return health, nil
}

func (state *runnerCoordinatorFakeStore) RecordGitHubSessionDemand(_ context.Context, demand store.GitHubSessionDemand) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	copyDemand := demand
	state.demand = &copyDemand
	return nil
}

// ReconcileGitHubAssignedDemand records the binding each poll reconciles with
// and returns whatever the test staged. The default zero value reports no
// durable statistics, which keeps every pre-existing JobAvailable scenario
// unchanged.
func (state *runnerCoordinatorFakeStore) ReconcileGitHubAssignedDemand(
	_ context.Context,
	_ store.ScaleSetID,
	binding store.SingleSlotBinding,
) (store.GitHubAssignedDemandResult, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.demandBindings = append(state.demandBindings, binding)
	return state.assignedDemand, nil
}

func (state *runnerCoordinatorFakeStore) assignedDemandBindings() []store.SingleSlotBinding {
	state.mu.Lock()
	defer state.mu.Unlock()
	return append([]store.SingleSlotBinding(nil), state.demandBindings...)
}

func (state *runnerCoordinatorFakeStore) CommitGitHubQueueMessage(
	_ context.Context,
	message store.GitHubQueueMessage,
	binding store.SingleSlotBinding,
) (store.GitHubMessageCommit, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	replayed := false
	if existing, found := state.messages[message.MessageID]; found {
		if existing.ScaleSetID != message.ScaleSetID ||
			existing.Digest != message.Digest {
			return store.GitHubMessageCommit{}, store.ErrReplayMismatch
		}
		state.replays++
		replayed = true
	} else {
		copyMessage := message
		copyMessage.Jobs = append([]store.GitHubJobEvent(nil), message.Jobs...)
		state.message = &copyMessage
		state.messages[message.MessageID] = copyMessage
		state.commits++
		state.events = len(message.Jobs)
	}
	hasAvailable := false
	unclaimedAvailable := false
	var resolved *store.GitHubJobClaim
	var resolvedIntent *store.GitHubUnpickedRequeueIntent
	pickupProven := false
	for _, event := range message.Jobs {
		if runnerCoordinatorFakePickupProof(event) &&
			(event.RunnerRequestID == state.attempt.ClaimKey ||
				event.RunnerRequestID == 0) &&
			event.RunnerID == state.attempt.RunnerID &&
			event.RunnerName == state.attempt.RunnerName {
			pickupProven = true
		}
	}
	if state.requeueIntent != nil && pickupProven {
		state.requeueIntent.PickupProven = true
	}
	seenAvailable := make(map[int64]struct{})
	for _, event := range message.Jobs {
		if event.Type == store.GitHubJobAvailable {
			seenAvailable[event.RunnerRequestID] = struct{}{}
		}
	}
	if len(seenAvailable) > 1 && state.claim == nil {
		return store.GitHubMessageCommit{
			Replayed: replayed, UnclaimedAvailable: true,
		}, nil
	}
	clear(seenAvailable)
	for _, event := range message.Jobs {
		if event.Type != store.GitHubJobAvailable {
			continue
		}
		if _, duplicate := seenAvailable[event.RunnerRequestID]; duplicate {
			continue
		}
		seenAvailable[event.RunnerRequestID] = struct{}{}
		hasAvailable = true
		if state.claim != nil && state.claim.ClaimKey == event.RunnerRequestID {
			if state.requeueIntent != nil {
				resolved = state.claim
				intent := *state.requeueIntent
				resolvedIntent = &intent
				continue
			}
			if !replayed &&
				(state.claim.State == store.GitHubClaimRunning ||
					state.claim.State ==
						store.GitHubClaimReconciliationRequired) &&
				(state.claim.Execution.State == domain.ExecutionReleased ||
					state.claim.Execution.State == domain.ExecutionFailed) &&
				state.attempt.State == store.GitHubJITStarted {
				if pickupProven {
					resolved = state.claim
					continue
				}
				state.claim.State = store.GitHubClaimReconciliationRequired
				state.capacity = 0
				state.requeueIntent = &store.GitHubUnpickedRequeueIntent{
					Claim:   *state.claim,
					Attempt: state.attempt,
					Replacement: domain.ExecutionSnapshot{
						ID:       event.ExecutionID,
						TargetID: binding.TargetID,
						Slot: domain.SlotKey{
							NodeID: binding.NodeID,
							Index:  binding.Slot,
						},
						State: domain.ExecutionReserved,
					},
					SourceMessageID:  message.MessageID,
					SourceEventIndex: 0,
					ControllerEpoch:  state.controllerEpoch,
				}
				resolved = state.claim
				intent := *state.requeueIntent
				resolvedIntent = &intent
				continue
			}
			if !replayed &&
				state.claim.State == store.GitHubClaimReconciliationRequired &&
				(state.claim.Execution.State == domain.ExecutionReleased ||
					state.claim.Execution.State == domain.ExecutionFailed) &&
				state.attempt.State == store.GitHubJITReconciledAbsent {
				state.claim.SourceMessageID = message.MessageID
				state.claim.Execution = domain.ExecutionSnapshot{
					ID:       event.ExecutionID,
					TargetID: binding.TargetID,
					Slot: domain.SlotKey{
						NodeID: binding.NodeID,
						Index:  binding.Slot,
					},
					State: domain.ExecutionReserved,
				}
				state.claim.State = store.GitHubClaimPending
				state.capacity = 0
				state.acquireAttempt.Attempt++
				state.acquireAttempt.EvidenceMessage = message.MessageID
				state.acquireAttempt.ControllerEpoch = state.controllerEpoch
				state.acquireDispatching = false
				state.acquireDone = false
				state.reconciledPending = false
			}
			if !replayed &&
				state.claim.State == store.GitHubClaimAcquireAmbiguous &&
				state.acquireDispatching &&
				!state.acquireDone {
				state.acquireAttempt.Attempt++
				state.acquireAttempt.EvidenceMessage = message.MessageID
				state.acquireAttempt.ControllerEpoch = state.controllerEpoch
				state.acquireDispatching = false
				state.reconciledPending = false
				state.claim.State = store.GitHubClaimPending
			}
			if resolved == nil {
				resolved = state.claim
			}
			continue
		}
		if state.claim != nil || state.capacity == 0 || !binding.ClaimEnabled {
			unclaimedAvailable = true
			continue
		}
		state.claim = &store.GitHubJobClaim{
			ScaleSetID:      message.ScaleSetID,
			ClaimKey:        event.RunnerRequestID,
			Origin:          store.GitHubClaimFromJobAvailable,
			RunnerRequestID: event.RunnerRequestID,
			SourceMessageID: message.MessageID,
			Execution: domain.ExecutionSnapshot{
				ID:       event.ExecutionID,
				TargetID: binding.TargetID,
				Slot:     domain.SlotKey{NodeID: binding.NodeID, Index: binding.Slot},
				State:    domain.ExecutionReserved,
			},
			State: store.GitHubClaimPending,
		}
		state.capacity = 0
		state.acquireAttempt = store.GitHubAcquireAttempt{
			ScaleSetID:      message.ScaleSetID,
			ClaimKey:        event.RunnerRequestID,
			Attempt:         1,
			EvidenceMessage: message.MessageID,
			ControllerEpoch: state.controllerEpoch,
		}
		state.acquireDispatching = false
		state.acquireDone = false
		state.reconciledPending = false
		resolved = state.claim
	}
	return store.GitHubMessageCommit{
		Replayed:           replayed,
		Claim:              cloneRunnerCoordinatorClaim(resolved),
		RequeueIntent:      resolvedIntent,
		UnclaimedAvailable: hasAvailable && (resolved == nil || unclaimedAvailable),
	}, nil
}

func runnerCoordinatorFakePickupProof(event store.GitHubJobEvent) bool {
	switch event.Type {
	case store.GitHubJobStarted:
		return true
	case store.GitHubJobCompleted:
		return event.Result == store.GitHubJobResultSucceeded ||
			event.Result == store.GitHubJobResultFailed
	default:
		return false
	}
}

func (state *runnerCoordinatorFakeStore) GitHubSingleSlotCapacity(context.Context, store.SingleSlotBinding) (int, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.capacity, nil
}

func (state *runnerCoordinatorFakeStore) NextActionableGitHubClaim(context.Context, store.ScaleSetID) (store.GitHubJobClaim, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil {
		return store.GitHubJobClaim{}, false, nil
	}
	switch state.claim.State {
	case store.GitHubClaimPending, store.GitHubClaimAcquired, store.GitHubClaimPreparing:
		return *cloneRunnerCoordinatorClaim(state.claim), true, nil
	default:
		return store.GitHubJobClaim{}, false, nil
	}
}

func (state *runnerCoordinatorFakeStore) GitHubPendingClaimDispatchReady(
	_ context.Context,
	claim store.GitHubJobClaim,
) (bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil || *state.claim != claim ||
		claim.State != store.GitHubClaimPending {
		return false, store.ErrGitHubClaimState
	}
	return state.reconciledPending, nil
}

func (state *runnerCoordinatorFakeStore) GitHubClaim(context.Context, store.ScaleSetID, int64) (store.GitHubJobClaim, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil {
		return store.GitHubJobClaim{}, false, nil
	}
	return *cloneRunnerCoordinatorClaim(state.claim), true, nil
}

func (state *runnerCoordinatorFakeStore) GitHubUnpickedRequeueIntent(
	context.Context,
	store.ScaleSetID,
	int64,
) (store.GitHubUnpickedRequeueIntent, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.requeueIntent == nil {
		return store.GitHubUnpickedRequeueIntent{}, false, nil
	}
	return *state.requeueIntent, true, nil
}

func (state *runnerCoordinatorFakeStore) BeginGitHubAcquire(
	_ context.Context,
	scaleSetID store.ScaleSetID,
	claimKey int64,
) (store.GitHubAcquireAttempt, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil ||
		state.claim.ScaleSetID != scaleSetID ||
		state.claim.ClaimKey != claimKey ||
		state.claim.State != store.GitHubClaimPending ||
		state.acquireDispatching ||
		state.acquireDone {
		return store.GitHubAcquireAttempt{}, store.ErrGitHubClaimState
	}
	state.acquireAttempt.ControllerEpoch = state.controllerEpoch
	state.acquireDispatching = true
	state.reconciledPending = false
	state.claim.State = store.GitHubClaimAcquireAmbiguous
	return state.acquireAttempt, nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubAcquired(
	_ context.Context,
	attempt store.GitHubAcquireAttempt,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil ||
		state.claim.State != store.GitHubClaimAcquireAmbiguous ||
		!state.acquireDispatching ||
		state.acquireDone ||
		state.acquireAttempt != attempt ||
		attempt.ControllerEpoch != state.controllerEpoch {
		return store.ErrGitHubClaimState
	}
	state.acquireDispatching = false
	state.acquireDone = true
	state.claim.State = store.GitHubClaimAcquired
	return nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubPreparing(context.Context, store.ScaleSetID, int64) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil || state.claim.State != store.GitHubClaimAcquired {
		return store.ErrGitHubClaimState
	}
	state.claim.State = store.GitHubClaimPreparing
	state.claim.Execution.State = domain.ExecutionPreparing
	return nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubPrepareFailed(context.Context, store.ScaleSetID, int64) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil || state.claim.State != store.GitHubClaimAcquired {
		return store.ErrGitHubClaimState
	}
	state.claim.State = store.GitHubClaimPrepareFailed
	state.claim.Execution.State = domain.ExecutionFailed
	return nil
}

func (state *runnerCoordinatorFakeStore) BeginGitHubJITAttempt(
	_ context.Context,
	scaleSetID store.ScaleSetID,
	claimKey int64,
	controllerEpoch domain.ControllerEpoch,
	name string,
) (store.GitHubJITAttempt, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil || state.claim.State != store.GitHubClaimPreparing {
		if state.attempt.Attempt > 0 {
			return state.attempt, true, nil
		}
		return store.GitHubJITAttempt{}, false, store.ErrGitHubClaimState
	}
	state.attempt = store.GitHubJITAttempt{
		ScaleSetID: scaleSetID, ClaimKey: claimKey,
		Attempt: state.attempt.Attempt + 1, ControllerEpoch: controllerEpoch,
		RunnerName: name, State: store.GitHubJITIntent,
	}
	state.claim.State = store.GitHubClaimJITIntent
	state.claim.CurrentAttempt = state.attempt.Attempt
	return state.attempt, false, nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubJITGenerationAmbiguous(_ context.Context, attempt store.GitHubJITAttempt) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.attempt.State = store.GitHubJITGenerationAmbiguous
	state.claim.State = store.GitHubClaimJITGenerationAmbiguous
	return nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubJITGenerated(
	_ context.Context,
	attempt store.GitHubJITAttempt,
	runnerID int,
	digest string,
	commandID domain.CommandID,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.attempt = attempt
	state.attempt.State = store.GitHubJITGenerated
	state.attempt.RunnerID = runnerID
	state.attempt.JITDigest = digest
	state.attempt.StartCommandID = commandID
	state.claim.State = store.GitHubClaimJITGenerated
	return nil
}

func (state *runnerCoordinatorFakeStore) BeginGitHubStartDispatch(_ context.Context, attempt store.GitHubJITAttempt) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.attempt = attempt
	state.attempt.State = store.GitHubJITStartDispatching
	state.claim.State = store.GitHubClaimStartDispatching
	return nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubStartAmbiguous(_ context.Context, attempt store.GitHubJITAttempt) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.attempt = attempt
	state.attempt.State = store.GitHubJITStartAmbiguous
	state.claim.State = store.GitHubClaimStartAmbiguous
	return nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubRunning(_ context.Context, attempt store.GitHubJITAttempt) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.attempt = attempt
	state.attempt.State = store.GitHubJITStarted
	state.claim.State = store.GitHubClaimRunning
	state.claim.Execution.State = domain.ExecutionRunning
	return nil
}

func (state *runnerCoordinatorFakeStore) CurrentGitHubJITAttempt(context.Context, store.ScaleSetID, int64) (store.GitHubJITAttempt, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.attempt, state.attempt.Attempt > 0, nil
}

func (state *runnerCoordinatorFakeStore) IssuedAgentCommand(
	_ context.Context,
	commandID domain.CommandID,
) (store.IssuedAgentCommand, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	issued, found := state.issued[commandID]
	return issued, found, nil
}

func (state *runnerCoordinatorFakeStore) AdoptAgentSnapshotObservation(
	_ context.Context,
	nodeID domain.NodeID,
	expectedState domain.ExecutionState,
	observation store.ObservationSnapshot,
	_ string,
	_ domain.ControllerEpoch,
) (domain.ExecutionSnapshot, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil ||
		state.claim.Execution.Slot.NodeID != nodeID ||
		state.claim.Execution.ID != observation.ExecutionID ||
		state.claim.Execution.State != expectedState ||
		!domain.CanReachExecutionState(
			state.claim.Execution.State, observation.State) {
		return domain.ExecutionSnapshot{}, false, store.ErrGitHubClaimState
	}
	changed := state.claim.Execution.State != observation.State
	state.claim.Execution.State = observation.State
	return state.claim.Execution, changed, nil
}

func (state *runnerCoordinatorFakeStore) FailDesiredExecutionFromSnapshot(
	_ context.Context,
	nodeID domain.NodeID,
	executionID domain.ExecutionID,
	expectedState domain.ExecutionState,
	_ string,
	_ domain.ControllerEpoch,
) (domain.ExecutionSnapshot, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil ||
		state.claim.Execution.Slot.NodeID != nodeID ||
		state.claim.Execution.ID != executionID ||
		state.claim.Execution.State != expectedState {
		return domain.ExecutionSnapshot{}, store.ErrGitHubClaimState
	}
	state.claim.Execution.State = domain.ExecutionFailed
	return state.claim.Execution, nil
}

func (state *runnerCoordinatorFakeStore) NextGitHubReconciliationFence(
	context.Context,
	store.ScaleSetID,
) (store.GitHubReconciliationFence, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil {
		return store.GitHubReconciliationFence{}, false, nil
	}
	if state.claim.State == store.GitHubClaimReconciliationRequired &&
		state.attempt.State == store.GitHubJITReconciledAbsent {
		return store.GitHubReconciliationFence{}, false, nil
	}
	switch state.claim.State {
	case store.GitHubClaimAcquireAmbiguous,
		store.GitHubClaimJITIntent,
		store.GitHubClaimJITGenerationAmbiguous,
		store.GitHubClaimJITGenerated,
		store.GitHubClaimStartDispatching,
		store.GitHubClaimStartAmbiguous,
		store.GitHubClaimReconciliationRequired:
	default:
		return store.GitHubReconciliationFence{}, false, nil
	}
	fence := store.GitHubReconciliationFence{Claim: *state.claim}
	if state.attempt.Attempt > 0 {
		attempt := state.attempt
		fence.Attempt = &attempt
	}
	return fence, true, nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubJITAgentAccepted(
	_ context.Context,
	attempt store.GitHubJITAttempt,
	_ domain.ControllerEpoch,
	_ string,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.attempt = attempt
	state.attempt.State = store.GitHubJITAgentAccepted
	state.claim.State = store.GitHubClaimReconciliationRequired
	return nil
}

func (state *runnerCoordinatorFakeStore) ReconcileGitHubJITPrunedHistory(
	_ context.Context,
	attempt store.GitHubJITAttempt,
	_ domain.ControllerEpoch,
	_ string,
) (store.GitHubJITPrunedHistoryResult, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.prunedStarted {
		state.attempt = attempt
		state.attempt.State = store.GitHubJITStarted
		state.claim.State = store.GitHubClaimRunning
		return store.GitHubJITPrunedHistoryResult{Started: true}, nil
	}
	if state.prunedLostTerminal != "" {
		state.lostJITTerminal = state.prunedLostTerminal
		return store.GitHubJITPrunedHistoryResult{
			LostTerminal: state.prunedLostTerminal,
		}, nil
	}
	return store.GitHubJITPrunedHistoryResult{}, nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubJITObservedStarted(
	_ context.Context,
	attempt store.GitHubJITAttempt,
	_ domain.ControllerEpoch,
	observation store.ObservationSnapshot,
	_ string,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil ||
		state.claim.Execution.ID != observation.ExecutionID ||
		!domain.CanReachExecutionState(state.claim.Execution.State, observation.State) {
		return store.ErrGitHubClaimState
	}
	if lostJITTerminalObservation(observation.State) {
		if !state.durableTerminal && !state.durableStartProof {
			return store.ErrGitHubJITTerminalPending
		}
		if !state.durableStartProof {
			state.lostJITTerminal = observation.State
			return store.ErrGitHubJITStartNotProven
		}
	}
	state.claim.Execution.State = observation.State
	state.attempt = attempt
	state.attempt.State = store.GitHubJITStarted
	state.claim.State = store.GitHubClaimRunning
	return nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubJITRemovalPending(
	_ context.Context,
	attempt store.GitHubJITAttempt,
	_ domain.ControllerEpoch,
	_ string,
	_ uint64,
	providerAbsent bool,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.requeueIntent != nil &&
		state.requeueIntent.PickupProven &&
		!providerAbsent {
		return store.ErrGitHubClaimState
	}
	state.attempt = attempt
	state.attempt.State = store.GitHubJITRemovalPending
	state.claim.State = store.GitHubClaimReconciliationRequired
	if state.requeueIntent != nil {
		state.requeueIntent.Attempt = state.attempt
		state.requeueIntent.Claim = *state.claim
	}
	state.removalIssued = !providerAbsent
	if providerAbsent {
		state.runnerAbsences = 1
	} else {
		state.runnerAbsences = 0
	}
	return nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubJITReconciledAbsent(
	_ context.Context,
	attempt store.GitHubJITAttempt,
	_ domain.ControllerEpoch,
	_ string,
	_ uint64,
) (store.GitHubJITAbsenceResult, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.attempt.RunnerID == 0 &&
		(state.attempt.State == store.GitHubJITIntent ||
			state.attempt.State == store.GitHubJITGenerationAmbiguous) {
		state.generationAbsences++
		if state.generationAbsences == 1 {
			return store.GitHubJITAbsenceResult{},
				store.ErrGitHubJITAbsencePending
		}
	}
	if state.attempt.State == store.GitHubJITRemovalPending {
		state.runnerAbsences++
		if state.removalIssued && state.runnerAbsences < 2 {
			return store.GitHubJITAbsenceResult{},
				store.ErrGitHubJITAbsencePending
		}
	}
	state.attempt = attempt
	state.attempt.State = store.GitHubJITReconciledAbsent
	state.attempt.RunnerID = 0
	state.attempt.JITDigest = ""
	state.attempt.StartCommandID = ""
	if state.requeueIntent != nil {
		intent := *state.requeueIntent
		state.requeueIntent = nil
		if intent.PickupProven {
			state.claim.State = store.GitHubClaimRunning
			state.capacity = 1
			return store.GitHubJITAbsenceResult{
				Claim: *state.claim,
			}, nil
		}
		state.claim.SourceMessageID = intent.SourceMessageID
		state.claim.Execution = intent.Replacement
		state.claim.State = store.GitHubClaimPending
		state.capacity = 0
		state.acquireAttempt.Attempt++
		state.acquireAttempt.EvidenceMessage = intent.SourceMessageID
		state.acquireAttempt.ControllerEpoch = state.controllerEpoch
		state.acquireDispatching = false
		state.acquireDone = false
		state.reconciledPending = true
		state.replacementCreates++
		replacement := intent.Replacement
		return store.GitHubJITAbsenceResult{
			Claim:                *state.claim,
			ReplacementExecution: &replacement,
			ReplacementClaimed:   true,
		}, nil
	}
	if state.lostJITTerminal != "" {
		state.claim.Execution.State = state.lostJITTerminal
		state.claim.State = store.GitHubClaimReconciliationRequired
		terminal := state.claim.Execution
		result := store.GitHubJITAbsenceResult{
			Claim:             *state.claim,
			TerminalExecution: &terminal,
		}
		switch state.lostJITTerminal {
		case domain.ExecutionReleased, domain.ExecutionFailed:
			state.capacity = 1
			result.AwaitingAvailability = true
		case domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
			state.capacity = 0
			result.CleanupBlocked = true
		}
		return result, nil
	}
	state.claim.State = store.GitHubClaimPreparing
	return store.GitHubJITAbsenceResult{Claim: *state.claim}, nil
}

func cloneRunnerCoordinatorClaim(claim *store.GitHubJobClaim) *store.GitHubJobClaim {
	if claim == nil {
		return nil
	}
	copyClaim := *claim
	return &copyClaim
}

type runnerCoordinatorFakeAgent struct {
	mu               sync.Mutex
	prepareState     domain.ExecutionState
	prepareCode      domain.ExecutionErrorCode
	prepareErr       error
	startState       domain.ExecutionState
	startCode        domain.ExecutionErrorCode
	startErr         error
	cancelState      domain.ExecutionState
	cancelCode       domain.ExecutionErrorCode
	cancelErr        error
	prepareCalls     int
	startCalls       int
	cancelCalls      int
	afterPrepare     func()
	afterStart       func()
	delivered        string
	snapshot         AgentSnapshot
	snapshotOnline   bool
	readinessContext context.Context
	readinessCancel  context.CancelFunc
	onSnapshotChange func(AgentSnapshot)
}

func (agent *runnerCoordinatorFakeAgent) SendPrepare(
	_ context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	_ bool,
) (transport.ExecutionUpdate, error) {
	agent.mu.Lock()
	agent.prepareCalls++
	if agent.prepareErr != nil {
		agent.mu.Unlock()
		return transport.ExecutionUpdate{}, agent.prepareErr
	}
	update := transport.ExecutionUpdate{
		NodeID: nodeID, CommandID: metadata.CommandID,
		ExecutionID: metadata.ExecutionID, State: agent.prepareState,
		ErrorCode: agent.prepareCode,
	}
	afterPrepare := agent.afterPrepare
	agent.mu.Unlock()
	if afterPrepare != nil {
		afterPrepare()
	}
	return update, nil
}

func (agent *runnerCoordinatorFakeAgent) SendStart(
	_ context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	_ bool,
	jit runner.JITConfig,
) (transport.ExecutionUpdate, error) {
	agent.mu.Lock()
	agent.startCalls++
	if agent.startErr != nil {
		agent.mu.Unlock()
		return transport.ExecutionUpdate{}, agent.startErr
	}
	if err := jit.Deliver(func(value string) error {
		agent.delivered = value
		return nil
	}); err != nil {
		agent.mu.Unlock()
		return transport.ExecutionUpdate{}, err
	}
	update := transport.ExecutionUpdate{
		NodeID: nodeID, CommandID: metadata.CommandID,
		ExecutionID: metadata.ExecutionID, State: agent.startState,
		ErrorCode: agent.startCode,
	}
	afterStart := agent.afterStart
	agent.mu.Unlock()
	if afterStart != nil {
		afterStart()
	}
	return update, nil
}

func (agent *runnerCoordinatorFakeAgent) ReplayPrepare(
	ctx context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	disableUpdate bool,
	_ string,
) (transport.ExecutionUpdate, error) {
	return agent.SendPrepare(ctx, nodeID, metadata, disableUpdate)
}

func (agent *runnerCoordinatorFakeAgent) SendReconciliationCancel(
	_ context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	_ string,
) (transport.ExecutionUpdate, error) {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	agent.cancelCalls++
	if agent.cancelErr != nil {
		return transport.ExecutionUpdate{}, agent.cancelErr
	}
	return transport.ExecutionUpdate{
		NodeID: nodeID, CommandID: metadata.CommandID,
		ExecutionID: metadata.ExecutionID, State: agent.cancelState,
		ErrorCode: agent.cancelCode,
	}, nil
}

func (agent *runnerCoordinatorFakeAgent) Readiness(
	domain.NodeID,
) (AgentSnapshot, bool, context.Context) {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	changed := agent.readinessContext
	if changed == nil {
		changed = context.Background()
	}
	return cloneAgentSnapshot(agent.snapshot), agent.snapshotOnline, changed
}

func (agent *runnerCoordinatorFakeAgent) setSnapshotOnline(online bool) {
	agent.mu.Lock()
	if agent.readinessCancel != nil {
		agent.readinessCancel()
	}
	agent.readinessContext, agent.readinessCancel = context.WithCancel(context.Background())
	agent.snapshotOnline = online
	snapshot := cloneAgentSnapshot(agent.snapshot)
	onSnapshotChange := agent.onSnapshotChange
	agent.mu.Unlock()
	if online && onSnapshotChange != nil {
		onSnapshotChange(snapshot)
	}
}

func (agent *runnerCoordinatorFakeAgent) setNativeRunnerReady(ready bool) {
	agent.mu.Lock()
	if agent.readinessCancel != nil {
		agent.readinessCancel()
	}
	agent.readinessContext, agent.readinessCancel = context.WithCancel(context.Background())
	agent.snapshot.NativeRunnerReady = ready
	snapshot := cloneAgentSnapshot(agent.snapshot)
	onSnapshotChange := agent.onSnapshotChange
	agent.mu.Unlock()
	if onSnapshotChange != nil {
		onSnapshotChange(snapshot)
	}
}

func waitControllerRunnerCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("controller runner condition was not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

type runnerCoordinatorFakeLifecycle struct {
	mu             sync.Mutex
	jit            *runnerCoordinatorFakeJIT
	nilJIT         bool
	generateErrors []error
	generate       func(context.Context, github.JITRequest) (runner.JITConfig, github.RunnerReference, error)
	generateCalls  int
	observedRunner *github.RunnerReference
	getErr         error
	query          func(context.Context, github.RunnerQuery) (*github.RunnerReference, error)
	getCalls       int
	removeErr      error
	remove         func(context.Context, github.RunnerReference) error
	removeCalls    int
}

func newRunnerCoordinatorFakeLifecycle() *runnerCoordinatorFakeLifecycle {
	return &runnerCoordinatorFakeLifecycle{
		jit: &runnerCoordinatorFakeJIT{value: "opaque-jit-canary"},
	}
}

func (lifecycle *runnerCoordinatorFakeLifecycle) GenerateJITConfig(
	ctx context.Context,
	request github.JITRequest,
) (runner.JITConfig, github.RunnerReference, error) {
	lifecycle.mu.Lock()
	lifecycle.generateCalls++
	generate := lifecycle.generate
	if generate != nil {
		lifecycle.mu.Unlock()
		return generate(ctx, request)
	}
	if len(lifecycle.generateErrors) > 0 {
		err := lifecycle.generateErrors[0]
		lifecycle.generateErrors = lifecycle.generateErrors[1:]
		if err != nil {
			lifecycle.mu.Unlock()
			return nil, github.RunnerReference{}, err
		}
	}
	lifecycle.jit = &runnerCoordinatorFakeJIT{value: "opaque-jit-canary"}
	if lifecycle.nilJIT {
		lifecycle.mu.Unlock()
		return nil, github.RunnerReference{
			ID: 81, Name: request.Name, ScaleSetID: request.ScaleSetID,
		}, nil
	}
	jit := lifecycle.jit
	lifecycle.mu.Unlock()
	return jit, github.RunnerReference{
		ID: 81, Name: request.Name, ScaleSetID: request.ScaleSetID,
	}, nil
}

func (lifecycle *runnerCoordinatorFakeLifecycle) QueryRunner(
	ctx context.Context,
	query github.RunnerQuery,
) (*github.RunnerReference, error) {
	lifecycle.mu.Lock()
	lifecycle.getCalls++
	queryFunc := lifecycle.query
	getErr := lifecycle.getErr
	observedRunner := lifecycle.observedRunner
	lifecycle.mu.Unlock()
	if queryFunc != nil {
		return queryFunc(ctx, query)
	}
	if getErr != nil {
		return nil, getErr
	}
	if observedRunner == nil {
		return nil, nil
	}
	copyReference := *observedRunner
	if copyReference.Name != query.Name ||
		copyReference.ScaleSetID != query.ScaleSetID ||
		(query.ExpectedID > 0 && copyReference.ID != query.ExpectedID) {
		return nil, github.ErrInvalidPreviewResponse
	}
	return &copyReference, nil
}

func (lifecycle *runnerCoordinatorFakeLifecycle) RemoveRunner(
	ctx context.Context,
	reference github.RunnerReference,
) error {
	lifecycle.mu.Lock()
	lifecycle.removeCalls++
	removeFunc := lifecycle.remove
	removeErr := lifecycle.removeErr
	lifecycle.mu.Unlock()
	if removeFunc != nil {
		return removeFunc(ctx, reference)
	}
	return removeErr
}

type runnerCoordinatorFakeJIT struct {
	mu        sync.Mutex
	value     string
	delivered bool
}

func (jit *runnerCoordinatorFakeJIT) Digest() string {
	jit.mu.Lock()
	defer jit.mu.Unlock()
	return domain.PayloadDigest([]byte(jit.value))
}

func (jit *runnerCoordinatorFakeJIT) Deliver(deliver func(string) error) error {
	jit.mu.Lock()
	defer jit.mu.Unlock()
	if jit.delivered || jit.value == "" || deliver == nil {
		return errors.New("fake JIT already consumed")
	}
	jit.delivered = true
	value := jit.value
	jit.value = ""
	return deliver(value)
}
