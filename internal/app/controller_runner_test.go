package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
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
	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stateStore.commits != 1 || stateStore.replays != 1 || session.deleteCalls != 2 {
		t.Fatalf("commits/replays/deletes = %d/%d/%d", stateStore.commits, stateStore.replays, session.deleteCalls)
	}
	if stateStore.claim.RunnerRequestID != 7001 {
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
		message := message
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
	runnerRequestID := stateStore.claim.RunnerRequestID
	storedDigest := stateStore.message.Digest
	stateStore.mu.Unlock()
	if executionID != deterministicExecutionID(first.ScaleSetID, first.Jobs[0].RunnerRequestID) ||
		runnerRequestID != first.Jobs[0].RunnerRequestID {
		t.Fatalf("stable replay execution/request = %s/%d", executionID, runnerRequestID)
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
		stateStore.claim.RunnerRequestID != runnerRequestID {
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
		RepositoryName: "tewake", OwnerName: "example-org",
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

func TestControllerRunnerJITAmbiguityRequiresSnapshotReconciliationBeforeRegeneration(t *testing.T) {
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
	if err := restarted.ReconcileJITAttempt(context.Background(), 7001); err != nil {
		t.Fatal(err)
	}
	if stateStore.claim.State != store.GitHubClaimPreparing {
		t.Fatalf("reconciled claim state = %q", stateStore.claim.State)
	}
	drove, err := restarted.DriveNext(context.Background())
	if err != nil || !drove {
		t.Fatalf("post-reconcile DriveNext = (%t, %v)", drove, err)
	}
	if lifecycle.generateCalls != 2 || agent.startCalls != 1 {
		t.Fatalf("post-reconcile generation/start = %d/%d", lifecycle.generateCalls, agent.startCalls)
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
	// TWK-007 may remove an unaccepted registration.
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
	t.Helper()
	if fakeAgent, ok := agent.(*runnerCoordinatorFakeAgent); ok &&
		fakeAgent.snapshot.NodeID == "" && !fakeAgent.snapshotOnline {
		fakeAgent.snapshot = AgentSnapshot{
			NodeID:            "00000000000000000000000000000001",
			OS:                domain.OSLinux,
			Arch:              domain.ArchAMD64,
			NativeRunnerReady: true,
		}
		fakeAgent.snapshotOnline = true
	}
	coordinator, err := NewControllerRunnerCoordinator(
		stateStore, session, agent, lifecycle,
		ControllerRunnerConfig{
			ScaleSetID:      7,
			TargetID:        "target-1",
			NodeID:          "00000000000000000000000000000001",
			ControllerEpoch: controllerEpoch,
			DisableUpdate:   true,
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func testControllerRunnerMessage() *github.Message {
	return &github.Message{
		ScaleSetID: 7,
		ID:         41,
		Statistics: github.Statistics{TotalAvailableJobs: 1},
		Jobs: []github.JobMessage{
			{Type: github.MessageTypeJobAvailable, RunnerRequestID: 7001, RepositoryName: "tewake", OwnerName: "example-org", JobID: "job-1", WorkflowRunID: 51},
			{Type: github.MessageTypeJobAssigned, RunnerRequestID: 7002, RepositoryName: "tewake", OwnerName: "example-org", JobID: "job-2", WorkflowRunID: 52},
			{Type: github.MessageTypeJobStarted, RunnerRequestID: 7003, RunnerID: 91, RunnerName: "existing-runner", RepositoryName: "tewake", OwnerName: "example-org", JobID: "job-3", WorkflowRunID: 53},
			{Type: github.MessageTypeJobCompleted, RunnerRequestID: 7004, RunnerID: 92, RunnerName: "complete-runner", Result: "succeeded", RepositoryName: "tewake", OwnerName: "example-org", JobID: "job-4", WorkflowRunID: 54},
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

func (session *runnerCoordinatorFakeSession) AcquireJobs(context.Context, []int64) ([]int64, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.acquireCalls++
	if session.afterAcquire != nil {
		session.afterAcquire()
	}
	return append([]int64(nil), session.acquired...), session.acquireErr
}

type runnerCoordinatorFakeStore struct {
	mu       sync.Mutex
	capacity int
	demand   *store.GitHubSessionDemand
	message  *store.GitHubQueueMessage
	claim    *store.GitHubJobClaim
	attempt  store.GitHubJITAttempt
	commits  int
	replays  int
	events   int
}

func newRunnerCoordinatorFakeStore() *runnerCoordinatorFakeStore {
	return &runnerCoordinatorFakeStore{capacity: 1}
}

func (state *runnerCoordinatorFakeStore) RecordGitHubSessionDemand(_ context.Context, demand store.GitHubSessionDemand) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	copyDemand := demand
	state.demand = &copyDemand
	return nil
}

func (state *runnerCoordinatorFakeStore) CommitGitHubQueueMessage(
	_ context.Context,
	message store.GitHubQueueMessage,
	binding store.SingleSlotBinding,
) (store.GitHubMessageCommit, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	replayed := false
	if state.message != nil {
		if state.message.ScaleSetID != message.ScaleSetID ||
			state.message.MessageID != message.MessageID ||
			state.message.Digest != message.Digest {
			return store.GitHubMessageCommit{}, store.ErrReplayMismatch
		}
		state.replays++
		replayed = true
	} else {
		copyMessage := message
		copyMessage.Jobs = append([]store.GitHubJobEvent(nil), message.Jobs...)
		state.message = &copyMessage
		state.commits++
		state.events = len(message.Jobs)
	}
	hasAvailable := false
	unclaimedAvailable := false
	var resolved *store.GitHubJobClaim
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
		if state.claim != nil && state.claim.RunnerRequestID == event.RunnerRequestID {
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
		resolved = state.claim
	}
	return store.GitHubMessageCommit{
		Replayed:           replayed,
		Claim:              cloneRunnerCoordinatorClaim(resolved),
		UnclaimedAvailable: hasAvailable && (resolved == nil || unclaimedAvailable),
	}, nil
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

func (state *runnerCoordinatorFakeStore) GitHubClaim(context.Context, store.ScaleSetID, int64) (store.GitHubJobClaim, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil {
		return store.GitHubJobClaim{}, false, nil
	}
	return *cloneRunnerCoordinatorClaim(state.claim), true, nil
}

func (state *runnerCoordinatorFakeStore) BeginGitHubAcquire(context.Context, store.ScaleSetID, int64) error {
	return state.setClaimState(store.GitHubClaimPending, store.GitHubClaimAcquireAmbiguous)
}

func (state *runnerCoordinatorFakeStore) MarkGitHubAcquired(context.Context, store.ScaleSetID, int64) error {
	return state.setClaimState(store.GitHubClaimAcquireAmbiguous, store.GitHubClaimAcquired)
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
	runnerRequestID int64,
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
		ScaleSetID: scaleSetID, RunnerRequestID: runnerRequestID,
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

func (state *runnerCoordinatorFakeStore) MarkGitHubJITAgentAccepted(
	_ context.Context,
	attempt store.GitHubJITAttempt,
	_ domain.ControllerEpoch,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.attempt = attempt
	state.attempt.State = store.GitHubJITAgentAccepted
	state.claim.State = store.GitHubClaimReconciliationRequired
	return nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubJITRemovalPending(
	_ context.Context,
	attempt store.GitHubJITAttempt,
	_ domain.ControllerEpoch,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.attempt = attempt
	state.attempt.State = store.GitHubJITRemovalPending
	state.claim.State = store.GitHubClaimReconciliationRequired
	return nil
}

func (state *runnerCoordinatorFakeStore) MarkGitHubJITReconciledAbsent(
	_ context.Context,
	attempt store.GitHubJITAttempt,
	_ domain.ControllerEpoch,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.attempt = attempt
	state.attempt.State = store.GitHubJITReconciledAbsent
	state.attempt.RunnerID = 0
	state.attempt.JITDigest = ""
	state.attempt.StartCommandID = ""
	state.claim.State = store.GitHubClaimPreparing
	return nil
}

func (state *runnerCoordinatorFakeStore) setClaimState(expected, next store.GitHubClaimState) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.claim == nil || state.claim.State != expected {
		return store.ErrGitHubClaimState
	}
	state.claim.State = next
	return nil
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
	prepareCalls     int
	startCalls       int
	afterPrepare     func()
	delivered        string
	snapshot         AgentSnapshot
	snapshotOnline   bool
	readinessContext context.Context
	readinessCancel  context.CancelFunc
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
	defer agent.mu.Unlock()
	agent.startCalls++
	if agent.startErr != nil {
		return transport.ExecutionUpdate{}, agent.startErr
	}
	if err := jit.Deliver(func(value string) error {
		agent.delivered = value
		return nil
	}); err != nil {
		return transport.ExecutionUpdate{}, err
	}
	return transport.ExecutionUpdate{
		NodeID: nodeID, CommandID: metadata.CommandID,
		ExecutionID: metadata.ExecutionID, State: agent.startState,
		ErrorCode: agent.startCode,
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
	agent.mu.Unlock()
}

func (agent *runnerCoordinatorFakeAgent) setNativeRunnerReady(ready bool) {
	agent.mu.Lock()
	if agent.readinessCancel != nil {
		agent.readinessCancel()
	}
	agent.readinessContext, agent.readinessCancel = context.WithCancel(context.Background())
	agent.snapshot.NativeRunnerReady = ready
	agent.mu.Unlock()
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
	generateCalls  int
	observedRunner *github.RunnerReference
	getErr         error
	getCalls       int
	removeErr      error
	removeCalls    int
}

func newRunnerCoordinatorFakeLifecycle() *runnerCoordinatorFakeLifecycle {
	return &runnerCoordinatorFakeLifecycle{
		jit: &runnerCoordinatorFakeJIT{value: "opaque-jit-canary"},
	}
}

func (lifecycle *runnerCoordinatorFakeLifecycle) GenerateJITConfig(
	_ context.Context,
	request github.JITRequest,
) (runner.JITConfig, github.RunnerReference, error) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.generateCalls++
	if len(lifecycle.generateErrors) > 0 {
		err := lifecycle.generateErrors[0]
		lifecycle.generateErrors = lifecycle.generateErrors[1:]
		if err != nil {
			return nil, github.RunnerReference{}, err
		}
	}
	lifecycle.jit = &runnerCoordinatorFakeJIT{value: "opaque-jit-canary"}
	if lifecycle.nilJIT {
		return nil, github.RunnerReference{
			ID: 81, Name: request.Name, ScaleSetID: request.ScaleSetID,
		}, nil
	}
	return lifecycle.jit, github.RunnerReference{
		ID: 81, Name: request.Name, ScaleSetID: request.ScaleSetID,
	}, nil
}

func (lifecycle *runnerCoordinatorFakeLifecycle) GetRunnerByName(
	context.Context,
	string,
) (*github.RunnerReference, error) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.getCalls++
	if lifecycle.observedRunner == nil {
		return nil, lifecycle.getErr
	}
	copyReference := *lifecycle.observedRunner
	return &copyReference, lifecycle.getErr
}

func (lifecycle *runnerCoordinatorFakeLifecycle) RemoveRunner(
	context.Context,
	github.RunnerReference,
) error {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.removeCalls++
	return lifecycle.removeErr
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
