package app

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/store"
)

type runnerReleaseObserverFunc func(context.Context) (github.RunnerRelease, error)

func (observe runnerReleaseObserverFunc) Latest(ctx context.Context) (github.RunnerRelease, error) {
	return observe(ctx)
}

type runnerReleaseStoreFake struct {
	successVersion     string
	successPublishedAt int64
	successErr         error
	failureClass       store.GitHubObservationFailureClass
	failureErr         error
	successCalls       int
	failureCalls       int
}

func (state *runnerReleaseStoreFake) RecordGitHubRunnerReleaseSuccess(
	_ context.Context,
	version string,
	publishedAt int64,
) (store.GitHubRunnerReleaseState, error) {
	state.successCalls++
	state.successVersion = version
	state.successPublishedAt = publishedAt
	return store.GitHubRunnerReleaseState{
		Freshness:                store.RuntimeFreshnessFresh,
		LatestVersion:            version,
		LatestReleasedAtUnixNano: publishedAt,
	}, state.successErr
}

func (state *runnerReleaseStoreFake) RecordGitHubRunnerReleaseFailure(
	_ context.Context,
	class store.GitHubObservationFailureClass,
) (store.GitHubRunnerReleaseState, error) {
	state.failureCalls++
	state.failureClass = class
	return store.GitHubRunnerReleaseState{
		Freshness:    store.RuntimeFreshnessUnknown,
		FailureClass: class,
	}, state.failureErr
}

func TestRefreshGitHubRunnerReleasePersistsExactProviderEvidence(t *testing.T) {
	publishedAt := time.Date(2026, time.July, 26, 1, 2, 3, 0, time.UTC)
	observedAt := publishedAt.Add(time.Hour)
	stateStore := &runnerReleaseStoreFake{}
	state, err := RefreshGitHubRunnerRelease(
		context.Background(),
		stateStore,
		runnerReleaseObserverFunc(func(context.Context) (github.RunnerRelease, error) {
			return github.RunnerRelease{
				Version:     "2.336.0",
				PublishedAt: publishedAt,
				ObservedAt:  observedAt,
			}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if state.Freshness != store.RuntimeFreshnessFresh ||
		stateStore.successCalls != 1 ||
		stateStore.failureCalls != 0 ||
		stateStore.successVersion != "2.336.0" ||
		stateStore.successPublishedAt != publishedAt.UnixNano() {
		t.Fatalf("state/store = %#v/%#v", state, stateStore)
	}
}

func TestRefreshGitHubRunnerReleasePersistsSafeFailureWithoutLeakingCause(t *testing.T) {
	canary := errors.New("provider-body-token-canary")
	stateStore := &runnerReleaseStoreFake{}
	state, err := RefreshGitHubRunnerRelease(
		context.Background(),
		stateStore,
		runnerReleaseObserverFunc(func(context.Context) (github.RunnerRelease, error) {
			return github.RunnerRelease{}, &github.ProviderHTTPStatusError{
				StatusCode: http.StatusServiceUnavailable,
				Err:        canary,
			}
		}),
	)
	if !errors.Is(err, ErrGitHubRunnerReleaseObservation) ||
		!errors.Is(err, canary) ||
		strings.Contains(err.Error(), canary.Error()) {
		t.Fatalf("error = %#v", err)
	}
	if state.Freshness != store.RuntimeFreshnessUnknown ||
		state.FailureClass != store.GitHubObservationProvider5xx ||
		stateStore.failureCalls != 1 ||
		stateStore.successCalls != 0 {
		t.Fatalf("state/store = %#v/%#v", state, stateStore)
	}
}

func TestRefreshGitHubRunnerReleaseDoesNotPersistIntentionalCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stateStore := &runnerReleaseStoreFake{}
	_, err := RefreshGitHubRunnerRelease(
		ctx,
		stateStore,
		runnerReleaseObserverFunc(func(context.Context) (github.RunnerRelease, error) {
			return github.RunnerRelease{}, context.Canceled
		}),
	)
	if !errors.Is(err, context.Canceled) ||
		stateStore.failureCalls != 0 ||
		stateStore.successCalls != 0 {
		t.Fatalf("error/store = %#v/%#v", err, stateStore)
	}
}

func TestRefreshGitHubRunnerReleaseFailsClosedOnInvalidSuccess(t *testing.T) {
	stateStore := &runnerReleaseStoreFake{}
	_, err := RefreshGitHubRunnerRelease(
		context.Background(),
		stateStore,
		runnerReleaseObserverFunc(func(context.Context) (github.RunnerRelease, error) {
			return github.RunnerRelease{
				Version:     "2.336.0",
				PublishedAt: time.Unix(200, 0),
				ObservedAt:  time.Unix(100, 0),
			}, nil
		}),
	)
	if !errors.Is(err, ErrGitHubRunnerReleaseObservation) ||
		stateStore.failureClass != store.GitHubObservationInvalidResponse ||
		stateStore.failureCalls != 1 ||
		stateStore.successCalls != 0 {
		t.Fatalf("error/store = %#v/%#v", err, stateStore)
	}
}

func TestRefreshGitHubRunnerReleaseSurfacesPersistenceFailure(t *testing.T) {
	canary := errors.New("sqlite unavailable")
	stateStore := &runnerReleaseStoreFake{failureErr: canary}
	_, err := RefreshGitHubRunnerRelease(
		context.Background(),
		stateStore,
		runnerReleaseObserverFunc(func(context.Context) (github.RunnerRelease, error) {
			return github.RunnerRelease{}, errors.New("provider unavailable")
		}),
	)
	if !errors.Is(err, ErrGitHubRunnerReleaseStore) ||
		!errors.Is(err, canary) ||
		errors.Is(err, ErrGitHubRunnerReleaseObservation) {
		t.Fatalf("error = %#v", err)
	}
}
