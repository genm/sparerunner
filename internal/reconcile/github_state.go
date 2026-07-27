package reconcile

import (
	"context"
	"sync"
	"time"
)

type GitHubFailureKind string

const (
	GitHubFailureServer     GitHubFailureKind = "server"
	GitHubFailureTimeout    GitHubFailureKind = "timeout"
	GitHubFailureTransport  GitHubFailureKind = "transport"
	GitHubFailurePermission GitHubFailureKind = "permission"
	GitHubFailureUnknown    GitHubFailureKind = "unknown"
)

// GitHubFailure deliberately retains only a safe classification. Raw response
// bodies and errors may contain authorization or provider details and do not
// belong in last-known state.
type GitHubFailure struct {
	Kind       GitHubFailureKind
	StatusCode int
}

func (failure GitHubFailure) Validate() error {
	switch failure.Kind {
	case GitHubFailureServer:
		if failure.StatusCode < 500 || failure.StatusCode > 599 {
			return invalid("invalid_github_failure", "github_failure.status_code", "server failures require a 5xx status")
		}
	case GitHubFailureTimeout, GitHubFailureTransport:
		if failure.StatusCode != 0 {
			return invalid("invalid_github_failure", "github_failure.status_code", "non-HTTP failures must not carry a status")
		}
	case GitHubFailurePermission:
		if failure.StatusCode != 401 && failure.StatusCode != 403 {
			return invalid("invalid_github_failure", "github_failure.status_code", "permission failures require a 401 or 403 status")
		}
	case GitHubFailureUnknown:
		if failure.StatusCode != 0 {
			return invalid("invalid_github_failure", "github_failure.status_code", "unknown failures must not carry an unverified HTTP status")
		}
	default:
		return invalid("invalid_github_failure", "github_failure.kind", "is not a safe GitHub failure classification")
	}
	return nil
}

type GitHubState[T any] struct {
	Value      T
	HasValue   bool
	ObservedAt time.Time
	Stale      bool
	StaleSince time.Time
	FailureAt  time.Time
	Failure    GitHubFailure
}

// AllowsNewDesired is false for both never-observed and stale provider state.
// Existing Controller/Agent executions remain represented separately.
func (state GitHubState[T]) AllowsNewDesired() bool {
	return state.HasValue && !state.Stale
}

type GitHubObserver[T any] interface {
	ObserveGitHub(context.Context) (T, error)
}

type GitHubObserverFunc[T any] func(context.Context) (T, error)

func (observe GitHubObserverFunc[T]) ObserveGitHub(ctx context.Context) (T, error) {
	return observe(ctx)
}

type GitHubFailureClassifier func(error) (GitHubFailure, bool)

// LastKnown keeps an external observation and its staleness metadata under one
// lock. clone is mandatory so callers cannot mutate retained slice/map state.
type LastKnown[T any] struct {
	mu       sync.RWMutex
	clone    func(T) T
	snapshot GitHubState[T]
}

func NewLastKnown[T any](clone func(T) T) (*LastKnown[T], error) {
	if clone == nil {
		return nil, invalid("github_clone_required", "github_state.clone", "must not be nil")
	}
	return &LastKnown[T]{clone: clone}, nil
}

func (state *LastKnown[T]) RecordSuccess(value T, observedAt time.Time) error {
	if state == nil || state.clone == nil {
		return invalid("github_state_unavailable", "github_state", "is not initialized")
	}
	if observedAt.IsZero() {
		return invalid("invalid_github_observation_time", "github_state.observed_at", "must not be zero")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.snapshot.HasValue && observedAt.Before(state.snapshot.ObservedAt) {
		return invalid("github_observation_regressed", "github_state.observed_at", "must not precede the retained observation")
	}
	if state.snapshot.Stale && observedAt.Before(state.snapshot.FailureAt) {
		return invalid("github_observation_precedes_failure", "github_state.observed_at", "cannot clear a later retained failure")
	}
	state.snapshot = GitHubState[T]{
		Value:      state.clone(value),
		HasValue:   true,
		ObservedAt: observedAt,
	}
	return nil
}

func (state *LastKnown[T]) RecordFailure(failure GitHubFailure, failedAt time.Time) error {
	if state == nil || state.clone == nil {
		return invalid("github_state_unavailable", "github_state", "is not initialized")
	}
	if err := failure.Validate(); err != nil {
		return err
	}
	if failedAt.IsZero() {
		return invalid("invalid_github_failure_time", "github_state.failed_at", "must not be zero")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.snapshot.HasValue && failedAt.Before(state.snapshot.ObservedAt) {
		return invalid("github_failure_precedes_observation", "github_state.failed_at", "must not precede the retained observation")
	}
	if state.snapshot.Stale && failedAt.Before(state.snapshot.FailureAt) {
		return invalid("github_failure_regressed", "github_state.failed_at", "must not precede the retained failure")
	}
	if !state.snapshot.Stale {
		state.snapshot.StaleSince = failedAt
	} else if failedAt.Before(state.snapshot.StaleSince) {
		state.snapshot.StaleSince = failedAt
	}
	if !state.snapshot.Stale || !failedAt.Before(state.snapshot.FailureAt) {
		state.snapshot.FailureAt = failedAt
		state.snapshot.Failure = failure
	}
	state.snapshot.Stale = true
	return nil
}

// Refresh replaces last-known state only after a complete successful read. A
// failure marks retained state stale and returns the original error so
// operators and retry policy keep the degraded signal. An unclassified error
// is retained only as a safe "unknown" class; it never leaves admission open.
func (state *LastKnown[T]) Refresh(
	ctx context.Context,
	observer GitHubObserver[T],
	classify GitHubFailureClassifier,
	observedAt time.Time,
) error {
	if observer == nil || classify == nil {
		return invalid("github_refresh_dependency_required", "github_state.refresh", "observer and failure classifier must be present")
	}
	value, err := observer.ObserveGitHub(ctx)
	if err == nil {
		return state.RecordSuccess(value, observedAt)
	}
	failure, classified := classify(err)
	if !classified {
		failure = GitHubFailure{Kind: GitHubFailureUnknown}
	}
	if markErr := state.RecordFailure(failure, observedAt); markErr != nil {
		return markErr
	}
	return err
}

func (state *LastKnown[T]) Snapshot() GitHubState[T] {
	if state == nil || state.clone == nil {
		return GitHubState[T]{}
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	snapshot := state.snapshot
	if snapshot.HasValue {
		snapshot.Value = state.clone(snapshot.Value)
	}
	return snapshot
}
