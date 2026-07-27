package app

import (
	"context"
	"errors"
	"strings"

	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/store"
)

var (
	ErrGitHubRunnerReleaseConfig      = errors.New("GitHub runner release observation is not configured")
	ErrGitHubRunnerReleaseObservation = errors.New("GitHub runner release observation failed")
	ErrGitHubRunnerReleaseStore       = errors.New("GitHub runner release state persistence failed")
)

type githubRunnerReleaseStore interface {
	RecordGitHubRunnerReleaseSuccess(
		context.Context,
		string,
		int64,
	) (store.GitHubRunnerReleaseState, error)
	RecordGitHubRunnerReleaseFailure(
		context.Context,
		store.GitHubObservationFailureClass,
	) (store.GitHubRunnerReleaseState, error)
}

type githubRunnerReleaseObserver interface {
	Latest(context.Context) (github.RunnerRelease, error)
}

// RefreshGitHubRunnerRelease records either one exact successful observation or
// one allowlisted failure. An intentional caller cancellation is not provider
// evidence and therefore does not make the last-known release stale.
func RefreshGitHubRunnerRelease(
	ctx context.Context,
	stateStore githubRunnerReleaseStore,
	observer githubRunnerReleaseObserver,
) (store.GitHubRunnerReleaseState, error) {
	if ctx == nil || stateStore == nil || observer == nil {
		return store.GitHubRunnerReleaseState{}, ErrGitHubRunnerReleaseConfig
	}
	release, err := observer.Latest(ctx)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return store.GitHubRunnerReleaseState{},
				safeControllerReleaseError(ErrGitHubRunnerReleaseObservation, err)
		}
		return recordGitHubRunnerReleaseFailure(ctx, stateStore, err)
	}
	if strings.TrimSpace(release.Version) != release.Version ||
		release.Version == "" ||
		len(release.Version) > 64 ||
		release.PublishedAt.IsZero() ||
		release.ObservedAt.IsZero() ||
		release.ObservedAt.Before(release.PublishedAt) {
		return recordGitHubRunnerReleaseFailure(
			ctx,
			stateStore,
			github.ErrInvalidRunnerReleaseResponse,
		)
	}
	state, err := stateStore.RecordGitHubRunnerReleaseSuccess(
		ctx,
		release.Version,
		release.PublishedAt.UnixNano(),
	)
	if err != nil {
		return store.GitHubRunnerReleaseState{},
			safeControllerReleaseError(ErrGitHubRunnerReleaseStore, err)
	}
	return state, nil
}

func recordGitHubRunnerReleaseFailure(
	ctx context.Context,
	stateStore githubRunnerReleaseStore,
	observationErr error,
) (store.GitHubRunnerReleaseState, error) {
	state, err := stateStore.RecordGitHubRunnerReleaseFailure(
		ctx,
		ClassifyGitHubObservationFailure(observationErr),
	)
	if err != nil {
		return store.GitHubRunnerReleaseState{},
			safeControllerReleaseError(ErrGitHubRunnerReleaseStore, err)
	}
	return state,
		safeControllerReleaseError(ErrGitHubRunnerReleaseObservation, observationErr)
}

type controllerReleaseError struct {
	public error
	cause  error
}

func safeControllerReleaseError(public, cause error) error {
	if cause == nil {
		return public
	}
	return &controllerReleaseError{public: public, cause: cause}
}

func (failure *controllerReleaseError) Error() string {
	if failure == nil || failure.public == nil {
		return "GitHub runner release operation failed"
	}
	return failure.public.Error()
}

func (failure *controllerReleaseError) Unwrap() []error {
	if failure == nil {
		return nil
	}
	return []error{failure.public, failure.cause}
}
