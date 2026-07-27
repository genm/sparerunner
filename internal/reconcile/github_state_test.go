package reconcile

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestLastKnownGitHubStateRetainsValueAndMarksStaleOn5xx(t *testing.T) {
	cache, err := NewLastKnown(func(value []string) []string {
		return append([]string(nil), value...)
	})
	if err != nil {
		t.Fatal(err)
	}
	firstObserved := time.Unix(100, 0).UTC()
	if err := cache.RecordSuccess([]string{"target-a", "target-b"}, firstObserved); err != nil {
		t.Fatal(err)
	}
	failureAt := firstObserved.Add(time.Minute)
	wantErr := &statusError{status: http.StatusServiceUnavailable}
	err = cache.Refresh(
		context.Background(),
		GitHubObserverFunc[[]string](func(context.Context) ([]string, error) {
			return nil, wantErr
		}),
		func(err error) (GitHubFailure, bool) {
			var status *statusError
			if !errors.As(err, &status) {
				return GitHubFailure{}, false
			}
			return GitHubFailure{Kind: GitHubFailureServer, StatusCode: status.status}, true
		},
		failureAt,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("refresh error = %v", err)
	}
	snapshot := cache.Snapshot()
	if !snapshot.HasValue || !snapshot.Stale ||
		snapshot.ObservedAt != firstObserved ||
		snapshot.StaleSince != failureAt ||
		snapshot.FailureAt != failureAt ||
		snapshot.Failure.Kind != GitHubFailureServer ||
		snapshot.Failure.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("stale state = %#v", snapshot)
	}
	if snapshot.AllowsNewDesired() {
		t.Fatal("stale GitHub state allowed speculative desired execution")
	}
	if len(snapshot.Value) != 2 || snapshot.Value[0] != "target-a" {
		t.Fatalf("last-known value = %#v", snapshot.Value)
	}
	snapshot.Value[0] = "mutated"
	if cache.Snapshot().Value[0] != "target-a" {
		t.Fatal("caller mutated cached GitHub authority")
	}
}

func TestLastKnownGitHubStateRecoversOnlyOnSuccessfulObservation(t *testing.T) {
	cache, err := NewLastKnown(func(value int) int { return value })
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(200, 0).UTC()
	if err := cache.RecordFailure(GitHubFailure{Kind: GitHubFailureTimeout}, at); err != nil {
		t.Fatal(err)
	}
	degraded := cache.Snapshot()
	if degraded.HasValue || !degraded.Stale {
		t.Fatalf("initial failure synthesized healthy empty state: %#v", degraded)
	}
	if err := cache.RecordSuccess(7, at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	recovered := cache.Snapshot()
	if !recovered.HasValue || recovered.Stale || recovered.Value != 7 ||
		recovered.Failure.Kind != "" || !recovered.StaleSince.IsZero() ||
		!recovered.FailureAt.IsZero() {
		t.Fatalf("recovered state = %#v", recovered)
	}
	if !recovered.AllowsNewDesired() {
		t.Fatal("fresh GitHub state remained admission-blocked")
	}
}

func TestLastKnownGitHubStateDoesNotLetDelayedSuccessClearLaterFailure(t *testing.T) {
	cache, err := NewLastKnown(func(value int) int { return value })
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.RecordSuccess(1, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if err := cache.RecordFailure(
		GitHubFailure{Kind: GitHubFailureServer, StatusCode: http.StatusBadGateway},
		time.Unix(200, 0),
	); err != nil {
		t.Fatal(err)
	}
	if err := cache.RecordSuccess(2, time.Unix(150, 0)); !hasCode(err, "github_observation_precedes_failure") {
		t.Fatalf("delayed success = %v", err)
	}
	snapshot := cache.Snapshot()
	if snapshot.Value != 1 || !snapshot.Stale || snapshot.FailureAt != time.Unix(200, 0) {
		t.Fatalf("delayed success cleared failure: %#v", snapshot)
	}
}

func TestLastKnownGitHubStateFailsClosedOnUnclassifiedPermissionFailure(t *testing.T) {
	cache, err := NewLastKnown(func(value string) string { return value })
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.RecordSuccess("known", time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	want := errors.New("permission denied")
	err = cache.Refresh(
		context.Background(),
		GitHubObserverFunc[string](func(context.Context) (string, error) {
			return "", want
		}),
		func(error) (GitHubFailure, bool) { return GitHubFailure{}, false },
		time.Unix(2, 0),
	)
	if !errors.Is(err, want) {
		t.Fatalf("unclassified error = %v", err)
	}
	snapshot := cache.Snapshot()
	if snapshot.Value != "known" || !snapshot.Stale ||
		snapshot.Failure.Kind != GitHubFailureUnknown ||
		snapshot.AllowsNewDesired() {
		t.Fatalf("unclassified failure did not fail closed: %#v", snapshot)
	}
}

func TestLastKnownGitHubStateRejectsDelayedFailureAfterNewerSuccess(t *testing.T) {
	cache, err := NewLastKnown(func(value string) string { return value })
	if err != nil {
		t.Fatal(err)
	}
	newer := time.Unix(200, 0)
	if err := cache.RecordSuccess("fresh", newer); err != nil {
		t.Fatal(err)
	}
	if err := cache.RecordFailure(
		GitHubFailure{Kind: GitHubFailureServer, StatusCode: http.StatusBadGateway},
		time.Unix(100, 0),
	); !hasCode(err, "github_failure_precedes_observation") {
		t.Fatalf("delayed failure = %v", err)
	}
	snapshot := cache.Snapshot()
	if snapshot.Value != "fresh" || snapshot.Stale || snapshot.ObservedAt != newer {
		t.Fatalf("delayed failure degraded newer success: %#v", snapshot)
	}
}

func TestLastKnownGitHubStateConcurrentReadsAndFailures(t *testing.T) {
	cache, err := NewLastKnown(func(value []int) []int {
		return append([]int(nil), value...)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.RecordSuccess([]int{1}, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	const workers = 64
	done := make(chan struct{}, workers*2)
	for index := 0; index < workers; index++ {
		go func(index int) {
			_ = cache.RecordFailure(
				GitHubFailure{Kind: GitHubFailureServer, StatusCode: http.StatusBadGateway},
				time.Unix(int64(index+2), 0),
			)
			done <- struct{}{}
		}(index)
		go func() {
			_ = cache.Snapshot()
			done <- struct{}{}
		}()
	}
	for index := 0; index < workers*2; index++ {
		<-done
	}
	if cache.Snapshot().Value[0] != 1 {
		t.Fatal("concurrent failure replaced last-known value")
	}
}

type statusError struct{ status int }

func (err *statusError) Error() string { return http.StatusText(err.status) }
