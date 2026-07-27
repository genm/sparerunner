package app

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/reconcile"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/store"
)

func TestControllerRunnerUnknownSessionUsesZeroProbeBeforeNewClaim(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	stateStore.runtimeFreshness.Session = store.GitHubScaleSetSessionHealth{
		ScaleSetID: 7,
		Freshness:  store.RuntimeFreshnessUnknown,
	}
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		&runnerCoordinatorFakeAgent{},
		newRunnerCoordinatorFakeLifecycle(),
	)

	if _, err := coordinator.PollOnce(context.Background()); !errors.Is(
		err,
		ErrGitHubAvailableUnclaimed,
	) {
		t.Fatalf("unknown-session probe error = %v", err)
	}
	if stateStore.claim != nil || session.deleteCalls != 0 {
		t.Fatalf("zero probe claim/delete = %#v/%d", stateStore.claim, session.deleteCalls)
	}
	if stateStore.runtimeFreshness.Session.Freshness != store.RuntimeFreshnessFresh {
		t.Fatalf("session after successful probe = %#v", stateStore.runtimeFreshness.Session)
	}

	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(session.pollCapacities) != 2 ||
		session.pollCapacities[0] != 0 ||
		session.pollCapacities[1] != 1 ||
		stateStore.claim == nil ||
		session.deleteCalls != 1 {
		t.Fatalf(
			"probe/replay capacities/claim/delete = %v/%#v/%d",
			session.pollCapacities,
			stateStore.claim,
			session.deleteCalls,
		)
	}
}

func TestControllerRunnerProviderFailuresKeepLastKnownDemandAndBlockClaims(t *testing.T) {
	tests := []struct {
		name  string
		cause error
		class store.GitHubObservationFailureClass
	}{
		{
			name: "provider 503",
			cause: &github.ProviderHTTPStatusError{
				StatusCode: http.StatusServiceUnavailable,
				Err:        errors.New("provider unavailable"),
			},
			class: store.GitHubObservationProvider5xx,
		},
		{
			name: "provider permission",
			cause: &github.ProviderHTTPStatusError{
				StatusCode: http.StatusForbidden,
				Err:        errors.New("provider denied"),
			},
			class: store.GitHubObservationProviderAuth,
		},
		{
			name:  "timeout",
			cause: context.DeadlineExceeded,
			class: store.GitHubObservationTimeout,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			stateStore := newRunnerCoordinatorFakeStore()
			session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
			session.poll = func(context.Context, int) (*github.Message, error) {
				return nil, testCase.cause
			}
			coordinator := newControllerRunnerForTest(
				t,
				stateStore,
				session,
				&runnerCoordinatorFakeAgent{},
				newRunnerCoordinatorFakeLifecycle(),
			)

			if _, err := coordinator.PollOnce(context.Background()); err == nil {
				t.Fatal("provider failure was hidden")
			} else {
				var providerFailure *github.ProviderFailure
				if !errors.As(err, &providerFailure) {
					t.Fatalf("error = %#v, want ProviderFailure", err)
				}
			}
			if stateStore.demand == nil ||
				stateStore.runtimeFreshness.Session.Freshness != store.RuntimeFreshnessStale ||
				len(stateStore.sessionFailures) != 1 ||
				stateStore.sessionFailures[0] != testCase.class ||
				stateStore.claim != nil ||
				session.deleteCalls != 0 {
				t.Fatalf(
					"demand/session/failures/claim/delete = %#v/%#v/%v/%#v/%d",
					stateStore.demand,
					stateStore.runtimeFreshness.Session,
					stateStore.sessionFailures,
					stateStore.claim,
					session.deleteCalls,
				)
			}
		})
	}
}

func TestControllerRunnerInvalidJobIdentityMarksSessionStaleBeforeHealthyCommit(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	message := testControllerRunnerMessage()
	message.Jobs[0].RunnerRequestID = 0
	session := newRunnerCoordinatorFakeSession(message)
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		&runnerCoordinatorFakeAgent{},
		newRunnerCoordinatorFakeLifecycle(),
	)
	if _, err := coordinator.PollOnce(context.Background()); err == nil {
		t.Fatal("invalid job identity was accepted")
	} else {
		var providerFailure *github.ProviderFailure
		if !errors.As(err, &providerFailure) ||
			providerFailure.Operation != github.ProviderValidateResponse {
			t.Fatalf("invalid job error = %#v", err)
		}
	}
	if stateStore.sessionSuccesses != 0 ||
		len(stateStore.sessionFailures) != 1 ||
		stateStore.sessionFailures[0] != store.GitHubObservationInvalidResponse ||
		stateStore.runtimeFreshness.Session.Freshness != store.RuntimeFreshnessStale ||
		stateStore.claim != nil ||
		session.deleteCalls != 0 {
		t.Fatalf("invalid job state = success:%d failures:%v session:%#v claim:%#v delete:%d",
			stateStore.sessionSuccesses,
			stateStore.sessionFailures,
			stateStore.runtimeFreshness.Session,
			stateStore.claim,
			session.deleteCalls)
	}
}

func TestControllerRunnerRunRetriesProviderFailureThroughZeroCapacityRecovery(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(nil)
	var pollCalls atomic.Int32
	thirdPoll := make(chan struct{})
	session.poll = func(ctx context.Context, capacity int) (*github.Message, error) {
		switch pollCalls.Add(1) {
		case 1:
			return nil, &github.ProviderHTTPStatusError{
				StatusCode: http.StatusServiceUnavailable,
				Err:        errors.New("provider unavailable"),
			}
		case 2:
			if capacity != 0 {
				return nil, errors.New("recovery probe advertised nonzero capacity")
			}
			return nil, nil
		default:
			close(thirdPoll)
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		&runnerCoordinatorFakeAgent{},
		newRunnerCoordinatorFakeLifecycle(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- coordinator.Run(ctx)
	}()
	select {
	case <-thirdPoll:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("coordinator did not retry through a successful zero-capacity probe")
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
	if len(session.pollCapacities) != 3 ||
		session.pollCapacities[0] != 1 ||
		session.pollCapacities[1] != 0 ||
		session.pollCapacities[2] != 1 ||
		len(stateStore.sessionFailures) != 1 ||
		stateStore.runtimeFreshness.Session.Freshness != store.RuntimeFreshnessFresh {
		t.Fatalf(
			"capacities/failures/session = %v/%v/%#v",
			session.pollCapacities,
			stateStore.sessionFailures,
			stateStore.runtimeFreshness.Session,
		)
	}
}

func TestControllerRunnerAgentVersionMismatchAdvertisesNoCapacity(t *testing.T) {
	stateStore := newRunnerCoordinatorFakeStore()
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	agent := &runnerCoordinatorFakeAgent{
		snapshot: AgentSnapshot{
			NodeID:            "00000000000000000000000000000001",
			OS:                domain.OSLinux,
			Arch:              domain.ArchAMD64,
			RunnerVersion:     "2.335.0",
			NativeRunnerReady: true,
		},
		snapshotOnline: true,
	}
	coordinator := newControllerRunnerForTest(
		t,
		stateStore,
		session,
		agent,
		newRunnerCoordinatorFakeLifecycle(),
	)
	if _, err := coordinator.PollOnce(context.Background()); !errors.Is(
		err,
		ErrGitHubAvailableUnclaimed,
	) {
		t.Fatalf("version-mismatch poll error = %v", err)
	}
	if len(session.pollCapacities) != 1 ||
		session.pollCapacities[0] != 0 ||
		stateStore.claim != nil ||
		session.acquireCalls != 0 {
		t.Fatalf(
			"capacity/claim/acquire = %v/%#v/%d",
			session.pollCapacities,
			stateStore.claim,
			session.acquireCalls,
		)
	}
}

func TestControllerRunnerPinnedReleaseEvidenceControlsAdmission(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name      string
		release   store.GitHubRunnerReleaseState
		wantCap   int
		wantErr   error
		wantClaim bool
	}{
		{
			name: "current fresh release",
			release: store.GitHubRunnerReleaseState{
				Freshness:                store.RuntimeFreshnessFresh,
				LatestVersion:            runner.OfficialRunnerVersion,
				LatestReleasedAtUnixNano: now.Add(-2 * time.Hour).UnixNano(),
				ObservedAtUnixNano:       now.Add(-time.Hour).UnixNano(),
				Generation:               1,
			},
			wantCap:   1,
			wantClaim: true,
		},
		{
			name: "last-known release became stale",
			release: store.GitHubRunnerReleaseState{
				Freshness:                store.RuntimeFreshnessStale,
				LatestVersion:            runner.OfficialRunnerVersion,
				LatestReleasedAtUnixNano: now.Add(-2 * time.Hour).UnixNano(),
				ObservedAtUnixNano:       now.Add(-time.Hour).UnixNano(),
				FailureClass:             store.GitHubObservationProvider5xx,
				FailureAtUnixNano:        now.UnixNano(),
				Generation:               2,
			},
			wantCap: 0,
			wantErr: ErrGitHubAvailableUnclaimed,
		},
		{
			name: "fresh observation expired",
			release: store.GitHubRunnerReleaseState{
				Freshness:                store.RuntimeFreshnessFresh,
				LatestVersion:            runner.OfficialRunnerVersion,
				LatestReleasedAtUnixNano: now.Add(-31 * 24 * time.Hour).UnixNano(),
				ObservedAtUnixNano:       now.Add(-reconcile.GitHubRunnerUpdateWindow).UnixNano(),
				Generation:               1,
			},
			wantCap: 0,
			wantErr: ErrGitHubAvailableUnclaimed,
		},
		{
			name: "pinned version support window expired",
			release: store.GitHubRunnerReleaseState{
				Freshness:                store.RuntimeFreshnessFresh,
				LatestVersion:            "2.337.0",
				LatestReleasedAtUnixNano: now.Add(-reconcile.GitHubRunnerUpdateWindow).UnixNano(),
				ObservedAtUnixNano:       now.Add(-time.Hour).UnixNano(),
				Generation:               1,
			},
			wantCap: 0,
			wantErr: ErrGitHubAvailableUnclaimed,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			stateStore := newRunnerCoordinatorFakeStore()
			stateStore.runtimeFreshness.Profile.VersionPolicy = domain.RunnerVersionPinned
			stateStore.runtimeFreshness.Release = testCase.release
			session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
			coordinator := newControllerRunnerForPinnedTest(
				t,
				stateStore,
				session,
				&runnerCoordinatorFakeAgent{},
			)
			_, err := coordinator.PollOnce(context.Background())
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("poll error = %v, want %v", err, testCase.wantErr)
			}
			if len(session.pollCapacities) != 1 ||
				session.pollCapacities[0] != testCase.wantCap ||
				(stateStore.claim != nil) != testCase.wantClaim {
				t.Fatalf(
					"capacity/claim = %v/%#v",
					session.pollCapacities,
					stateStore.claim,
				)
			}
		})
	}
}

func TestControllerRunnerRecoveredClaimsCannotBypassPinnedFreshness(t *testing.T) {
	now := time.Now().UTC()
	degradedReleases := []struct {
		name    string
		release store.GitHubRunnerReleaseState
	}{
		{
			name: "stale",
			release: store.GitHubRunnerReleaseState{
				Freshness:                store.RuntimeFreshnessStale,
				LatestVersion:            runner.OfficialRunnerVersion,
				LatestReleasedAtUnixNano: now.Add(-2 * time.Hour).UnixNano(),
				ObservedAtUnixNano:       now.Add(-time.Hour).UnixNano(),
				FailureClass:             store.GitHubObservationProvider5xx,
				FailureAtUnixNano:        now.UnixNano(),
				Generation:               2,
			},
		},
		{
			name: "expired",
			release: store.GitHubRunnerReleaseState{
				Freshness:                store.RuntimeFreshnessFresh,
				LatestVersion:            runner.OfficialRunnerVersion,
				LatestReleasedAtUnixNano: now.Add(-31 * 24 * time.Hour).UnixNano(),
				ObservedAtUnixNano:       now.Add(-reconcile.GitHubRunnerUpdateWindow).UnixNano(),
				Generation:               1,
			},
		},
	}
	claims := []struct {
		name       string
		claimState store.GitHubClaimState
		execution  domain.ExecutionState
	}{
		{
			name:       "acquired",
			claimState: store.GitHubClaimAcquired,
			execution:  domain.ExecutionReserved,
		},
		{
			name:       "preparing",
			claimState: store.GitHubClaimPreparing,
			execution:  domain.ExecutionPreparing,
		},
	}

	for _, degraded := range degradedReleases {
		for _, claim := range claims {
			t.Run(degraded.name+"/"+claim.name, func(t *testing.T) {
				stateStore := newRunnerCoordinatorFakeStore()
				stateStore.runtimeFreshness.Profile.VersionPolicy =
					domain.RunnerVersionPinned
				stateStore.runtimeFreshness.Release = store.GitHubRunnerReleaseState{
					Freshness:                store.RuntimeFreshnessFresh,
					LatestVersion:            runner.OfficialRunnerVersion,
					LatestReleasedAtUnixNano: now.Add(-2 * time.Hour).UnixNano(),
					ObservedAtUnixNano:       now.Add(-time.Hour).UnixNano(),
					Generation:               1,
				}
				session := newRunnerCoordinatorFakeSession(
					testControllerRunnerMessage())
				agent := &runnerCoordinatorFakeAgent{
					prepareState: domain.ExecutionPreparing,
					startState:   domain.ExecutionRunning,
				}
				coordinator := newControllerRunnerForPinnedTest(
					t, stateStore, session, agent)
				if _, err := coordinator.PollOnce(context.Background()); err != nil {
					t.Fatal(err)
				}
				stateStore.mu.Lock()
				stateStore.claim.State = claim.claimState
				stateStore.claim.Execution.State = claim.execution
				stateStore.acquireDone = true
				stateStore.runtimeFreshness.Release = degraded.release
				stateStore.mu.Unlock()

				drove, err := coordinator.DriveNext(context.Background())
				if !drove || !errors.Is(err, ErrControllerRunnerAdmission) {
					t.Fatalf("degraded recovered drive = (%t, %v)", drove, err)
				}
				lifecycle := coordinator.lifecycle.(*runnerCoordinatorFakeLifecycle)
				if session.acquireCalls != 0 || agent.prepareCalls != 0 ||
					agent.startCalls != 0 || lifecycle.generateCalls != 0 {
					t.Fatalf(
						"degraded recovery crossed admission: acquire=%d prepare=%d generate=%d start=%d",
						session.acquireCalls,
						agent.prepareCalls,
						lifecycle.generateCalls,
						agent.startCalls,
					)
				}
				if stateStore.claim.State != claim.claimState ||
					stateStore.claim.Execution.State != claim.execution {
					t.Fatalf("degraded recovery mutated claim = %#v", stateStore.claim)
				}
			})
		}
	}
}

func TestControllerRunnerPinnedFreshnessDeadlineInterruptsPollWithoutStale(t *testing.T) {
	now := time.Now().UTC()
	stateStore := newRunnerCoordinatorFakeStore()
	stateStore.runtimeFreshness.Profile.VersionPolicy = domain.RunnerVersionPinned
	stateStore.runtimeFreshness.Release = store.GitHubRunnerReleaseState{
		Freshness:                store.RuntimeFreshnessFresh,
		LatestVersion:            runner.OfficialRunnerVersion,
		LatestReleasedAtUnixNano: now.Add(-31 * 24 * time.Hour).UnixNano(),
		ObservedAtUnixNano: now.
			Add(-reconcile.GitHubRunnerUpdateWindow).
			Add(500 * time.Millisecond).
			UnixNano(),
		Generation: 1,
	}
	session := newRunnerCoordinatorFakeSession(nil)
	started := make(chan int, 1)
	session.poll = func(ctx context.Context, capacity int) (*github.Message, error) {
		started <- capacity
		<-ctx.Done()
		return nil, ctx.Err()
	}
	coordinator := newControllerRunnerForPinnedTest(
		t,
		stateStore,
		session,
		&runnerCoordinatorFakeAgent{},
	)
	result := make(chan error, 1)
	go func() {
		_, err := coordinator.PollAndDriveOnce(context.Background())
		result <- err
	}()
	select {
	case capacity := <-started:
		if capacity != 1 {
			t.Fatalf("initial capacity = %d, want 1", capacity)
		}
	case <-time.After(time.Second):
		t.Fatal("pinned long poll did not start")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("deadline interruption = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pinned freshness deadline did not interrupt the long poll")
	}
	if len(stateStore.sessionFailures) != 0 ||
		stateStore.runtimeFreshness.Session.Freshness != store.RuntimeFreshnessFresh {
		t.Fatalf(
			"deadline marked provider stale = %v/%#v",
			stateStore.sessionFailures,
			stateStore.runtimeFreshness.Session,
		)
	}
}

func newControllerRunnerForPinnedTest(
	t *testing.T,
	stateStore *runnerCoordinatorFakeStore,
	session *runnerCoordinatorFakeSession,
	agent *runnerCoordinatorFakeAgent,
) *ControllerRunnerCoordinator {
	t.Helper()
	const controllerEpoch domain.ControllerEpoch = 3
	if agent.snapshot.NodeID == "" {
		agent.snapshot = AgentSnapshot{
			NodeID:            "00000000000000000000000000000001",
			OS:                domain.OSLinux,
			Arch:              domain.ArchAMD64,
			RunnerVersion:     runner.OfficialRunnerVersion,
			NativeRunnerReady: true,
		}
	}
	agent.snapshotOnline = true
	stateStore.mu.Lock()
	stateStore.controllerEpoch = controllerEpoch
	stateStore.runtimeFreshness.Profile.VersionPolicy = domain.RunnerVersionPinned
	stateStore.mu.Unlock()
	stateStore.setPollAgentSnapshot(t, agent.snapshot, controllerEpoch)
	agent.onSnapshotChange = func(snapshot AgentSnapshot) {
		stateStore.setPollAgentSnapshotWithoutTest(snapshot, controllerEpoch)
	}
	coordinator, err := NewControllerRunnerCoordinator(
		stateStore,
		session,
		agent,
		newRunnerCoordinatorFakeLifecycle(),
		ControllerRunnerConfig{
			ScaleSetID:      7,
			TargetID:        "target-1",
			Scope:           "owner/repo",
			ScopeKind:       domain.TargetRepository,
			RunnerProfileID: "profile-1",
			VersionPolicy:   domain.RunnerVersionPinned,
			NodeID:          "00000000000000000000000000000001",
			ControllerEpoch: controllerEpoch,
			Reconciler:      acceptingControllerRunnerReconciler{},
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}
