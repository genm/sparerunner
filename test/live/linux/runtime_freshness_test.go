//go:build !windows

package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/runner"
	"github.com/genm/sparerunner/internal/store"
)

type liveRunnerReleaseObserverFunc func(context.Context) (github.RunnerRelease, error)

func (observe liveRunnerReleaseObserverFunc) Latest(ctx context.Context) (github.RunnerRelease, error) {
	return observe(ctx)
}

func openLiveRuntimeStore(
	t *testing.T,
	now time.Time,
) *store.ControllerStore {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "controller")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	stateStore, err := store.OpenController(
		context.Background(),
		filepath.Join(directory, "controller.db"),
		store.Options{Now: func() time.Time { return now }},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stateStore.Close(); err != nil {
			t.Error(err)
		}
	})
	return stateStore
}

func TestConfigureLiveGitHubRuntimePersistsAutoUpdateBindingWithoutReleaseRead(t *testing.T) {
	stateStore := openLiveRuntimeStore(
		t,
		time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC),
	)
	observer := liveRunnerReleaseObserverFunc(func(context.Context) (github.RunnerRelease, error) {
		t.Fatal("auto-update profile read the global runner release")
		return github.RunnerRelease{}, nil
	})
	profileID, policy, err := configureLiveGitHubRuntime(
		context.Background(),
		stateStore,
		liveConfig{},
		"target-live-auto",
		81,
		observer,
	)
	if err != nil {
		t.Fatal(err)
	}
	if profileID == "" || policy != domain.RunnerVersionAutoUpdate {
		t.Fatalf("profile/policy = %q/%q", profileID, policy)
	}
	profile, found, err := stateStore.ReadRunnerProfile(context.Background(), profileID)
	if err != nil || !found ||
		profile.VersionPolicy != domain.RunnerVersionAutoUpdate ||
		profile.RunnerVersion != runner.OfficialRunnerVersion ||
		profile.Revision != 1 {
		t.Fatalf("profile = (%#v, %t, %v)", profile, found, err)
	}
	binding, found, err := stateStore.ReadGitHubTargetRuntimeBinding(
		context.Background(),
		"target-live-auto",
	)
	if err != nil || !found || binding.ProfileID != profileID || binding.ScaleSetID != 81 {
		t.Fatalf("binding = (%#v, %t, %v)", binding, found, err)
	}
	release, err := stateStore.ReadGitHubRunnerReleaseState(context.Background())
	if err != nil || release.Freshness != store.RuntimeFreshnessUnknown {
		t.Fatalf("auto-update release = (%#v, %v)", release, err)
	}
}

func TestConfigureLiveGitHubRuntimePersistsPinnedReleaseAndExactReplay(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	publishedAt := now.Add(-time.Hour)
	stateStore := openLiveRuntimeStore(t, now)
	observer := liveRunnerReleaseObserverFunc(func(context.Context) (github.RunnerRelease, error) {
		return github.RunnerRelease{
			Version:     runner.OfficialRunnerVersion,
			PublishedAt: publishedAt,
			ObservedAt:  now,
		}, nil
	})
	config := liveConfig{GitHub: githubConfig{DisableUpdate: true}}
	for range 2 {
		profileID, policy, err := configureLiveGitHubRuntime(
			context.Background(),
			stateStore,
			config,
			"target-live-pinned",
			82,
			observer,
		)
		if err != nil {
			t.Fatal(err)
		}
		if profileID == "" || policy != domain.RunnerVersionPinned {
			t.Fatalf("profile/policy = %q/%q", profileID, policy)
		}
	}
	release, err := stateStore.ReadGitHubRunnerReleaseState(context.Background())
	if err != nil ||
		release.Freshness != store.RuntimeFreshnessFresh ||
		release.LatestVersion != runner.OfficialRunnerVersion ||
		release.LatestReleasedAtUnixNano != publishedAt.UnixNano() {
		t.Fatalf("pinned release = (%#v, %v)", release, err)
	}
}

func TestConfigureLiveGitHubRuntimeRecordsReleaseFailureAndFailsPreflight(t *testing.T) {
	now := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	stateStore := openLiveRuntimeStore(t, now)
	observer := liveRunnerReleaseObserverFunc(func(context.Context) (github.RunnerRelease, error) {
		return github.RunnerRelease{}, &github.ProviderHTTPStatusError{
			StatusCode: http.StatusServiceUnavailable,
			Err:        errors.New("provider unavailable"),
		}
	})
	_, _, err := configureLiveGitHubRuntime(
		context.Background(),
		stateStore,
		liveConfig{GitHub: githubConfig{DisableUpdate: true}},
		"target-live-failed-release",
		83,
		observer,
	)
	if !errors.Is(err, errGitHubClientPreflight) {
		t.Fatalf("error = %v, want GitHub preflight failure", err)
	}
	release, readErr := stateStore.ReadGitHubRunnerReleaseState(context.Background())
	if readErr != nil ||
		release.Freshness != store.RuntimeFreshnessUnknown ||
		release.FailureClass != store.GitHubObservationProvider5xx ||
		release.FailureAtUnixNano == 0 {
		t.Fatalf("failed release = (%#v, %v)", release, readErr)
	}
}
