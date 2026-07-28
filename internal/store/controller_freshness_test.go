package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/domain"
)

func TestRuntimeFreshnessPersistsAcrossCloseAndReopen(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(100, 0)
	path := filepath.Join(privateTestDir(t), "runtime-freshness.db")
	open := func() *ControllerStore {
		t.Helper()
		store, err := OpenController(ctx, path, Options{Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store := open()
	policy := RunnerProfileUpdatePolicy{
		ProfileID:     "linux",
		VersionPolicy: domain.RunnerVersionPinned,
		RunnerVersion: "2.336.0",
		Revision:      1,
	}
	if replayed, err := store.ConfigureRunnerProfile(ctx, policy); err != nil || replayed {
		t.Fatalf("configure profile = replayed %t, err %v", replayed, err)
	}
	binding := GitHubTargetRuntimeBinding{TargetID: "target-1", ScaleSetID: 44, ProfileID: policy.ProfileID}
	if replayed, err := store.ConfigureGitHubTargetRuntimeBinding(ctx, binding); err != nil || replayed {
		t.Fatalf("configure binding = replayed %t, err %v", replayed, err)
	}
	unknown, err := store.ReadGitHubScaleSetSessionHealth(ctx, binding.ScaleSetID)
	if err != nil || unknown.Freshness != RuntimeFreshnessUnknown || unknown.TransitionGeneration != 0 {
		t.Fatalf("never-observed session health = %#v, %v", unknown, err)
	}
	initialAggregate, err := store.ReadGitHubRuntimeFreshness(ctx, binding)
	if err != nil || initialAggregate.Release.Freshness != RuntimeFreshnessUnknown || initialAggregate.Session.Freshness != RuntimeFreshnessUnknown {
		t.Fatalf("never-observed runtime freshness aggregate = %#v, %v", initialAggregate, err)
	}
	release, err := store.RecordGitHubRunnerReleaseSuccess(ctx, "2.336.0", now.Add(-time.Second).UnixNano())
	if err != nil || release.Freshness != RuntimeFreshnessFresh || release.Generation != 1 {
		t.Fatalf("release success = %#v, %v", release, err)
	}
	health, err := store.RecordGitHubScaleSetSessionSuccess(ctx, binding.ScaleSetID)
	if err != nil || health.Freshness != RuntimeFreshnessFresh || health.TransitionGeneration != 1 {
		t.Fatalf("session success = %#v, %v", health, err)
	}
	aggregate, err := store.ReadGitHubRuntimeFreshness(ctx, binding)
	if err != nil || aggregate.Binding != binding || aggregate.Profile != policy || aggregate.Release != release || aggregate.Session != health {
		t.Fatalf("runtime freshness aggregate = %#v, %v", aggregate, err)
	}
	mismatched := binding
	mismatched.ScaleSetID++
	if _, err := store.ReadGitHubRuntimeFreshness(ctx, mismatched); !errors.Is(err, ErrRuntimeFreshnessBindingMismatch) {
		t.Fatalf("runtime freshness binding mismatch = %v", err)
	}
	if _, err := store.ReadGitHubRuntimeFreshness(ctx, GitHubTargetRuntimeBinding{TargetID: "missing-target", ScaleSetID: 99, ProfileID: policy.ProfileID}); !errors.Is(err, ErrRuntimeFreshnessBindingMissing) {
		t.Fatalf("runtime freshness missing binding = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	now = now.Add(time.Second)
	store = open()
	defer store.Close()
	gotPolicy, found, err := store.ReadRunnerProfile(ctx, policy.ProfileID)
	if err != nil || !found || gotPolicy != policy {
		t.Fatalf("reopened profile = %#v, found %t, err %v", gotPolicy, found, err)
	}
	gotBinding, found, err := store.ReadGitHubTargetRuntimeBinding(ctx, binding.TargetID)
	if err != nil || !found || gotBinding != binding {
		t.Fatalf("reopened binding = %#v, found %t, err %v", gotBinding, found, err)
	}
	gotRelease, err := store.ReadGitHubRunnerReleaseState(ctx)
	if err != nil || gotRelease != release {
		t.Fatalf("reopened release = %#v, err %v", gotRelease, err)
	}
	gotHealth, err := store.ReadGitHubScaleSetSessionHealth(ctx, binding.ScaleSetID)
	if err != nil || gotHealth != health {
		t.Fatalf("reopened session health = %#v, err %v", gotHealth, err)
	}
}

func TestRuntimeFreshnessConfigurationRejectsConflictsAndStaleRevisions(t *testing.T) {
	ctx := context.Background()
	store := openController(t, "runtime-freshness-conflicts.db")
	defer store.Close()
	policy := RunnerProfileUpdatePolicy{
		ProfileID:     "linux",
		VersionPolicy: domain.RunnerVersionAutoUpdate,
		RunnerVersion: "2.336.0",
		Revision:      1,
	}
	if replayed, err := store.ConfigureRunnerProfile(ctx, policy); err != nil || replayed {
		t.Fatalf("initial profile = replayed %t, err %v", replayed, err)
	}
	if replayed, err := store.ConfigureRunnerProfile(ctx, policy); err != nil || !replayed {
		t.Fatalf("exact profile replay = replayed %t, err %v", replayed, err)
	}
	changed := policy
	changed.RunnerVersion = "2.337.0"
	if _, err := store.ConfigureRunnerProfile(ctx, changed); !errors.Is(err, ErrRuntimeFreshnessConflict) {
		t.Fatalf("same-revision changed profile error = %v", err)
	}
	changed.Revision = 3
	if _, err := store.ConfigureRunnerProfile(ctx, changed); !errors.Is(err, ErrRuntimeFreshnessConflict) {
		t.Fatalf("skipped profile revision error = %v", err)
	}
	changed.Revision = 2
	if replayed, err := store.ConfigureRunnerProfile(ctx, changed); err != nil || replayed {
		t.Fatalf("next profile revision = replayed %t, err %v", replayed, err)
	}
	binding := GitHubTargetRuntimeBinding{TargetID: "target-1", ScaleSetID: 77, ProfileID: policy.ProfileID}
	if replayed, err := store.ConfigureGitHubTargetRuntimeBinding(ctx, binding); err != nil || replayed {
		t.Fatalf("initial binding = replayed %t, err %v", replayed, err)
	}
	if replayed, err := store.ConfigureGitHubTargetRuntimeBinding(ctx, binding); err != nil || !replayed {
		t.Fatalf("exact binding replay = replayed %t, err %v", replayed, err)
	}
	changedBinding := binding
	changedBinding.ScaleSetID = 78
	if _, err := store.ConfigureGitHubTargetRuntimeBinding(ctx, changedBinding); !errors.Is(err, ErrRuntimeFreshnessConflict) {
		t.Fatalf("changed target binding error = %v", err)
	}
	if _, err := store.ConfigureGitHubTargetRuntimeBinding(ctx, GitHubTargetRuntimeBinding{TargetID: "target-2", ScaleSetID: binding.ScaleSetID, ProfileID: policy.ProfileID}); !errors.Is(err, ErrRuntimeFreshnessConflict) {
		t.Fatalf("duplicate scale-set binding error = %v", err)
	}
}

func TestRuntimeFreshnessConcurrentBindingReplaySerializesAtStoreBoundary(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "runtime-freshness-concurrent.db")
	first, err := OpenController(ctx, path, Options{Now: func() time.Time { return time.Unix(150, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenController(ctx, path, Options{Now: func() time.Time { return time.Unix(150, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	policy := RunnerProfileUpdatePolicy{ProfileID: "linux", VersionPolicy: domain.RunnerVersionPinned, RunnerVersion: "2.336.0", Revision: 1}
	if _, err := first.ConfigureRunnerProfile(ctx, policy); err != nil {
		t.Fatal(err)
	}
	binding := GitHubTargetRuntimeBinding{TargetID: "target-concurrent", ScaleSetID: 88, ProfileID: policy.ProfileID}
	type result struct {
		replayed bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, controller := range []*ControllerStore{first, second} {
		go func(controller *ControllerStore) {
			<-start
			replayed, err := controller.ConfigureGitHubTargetRuntimeBinding(ctx, binding)
			results <- result{replayed: replayed, err: err}
		}(controller)
	}
	close(start)
	firstResult, secondResult := <-results, <-results
	if firstResult.err != nil || secondResult.err != nil {
		t.Fatalf("concurrent binding results = %#v, %#v", firstResult, secondResult)
	}
	if firstResult.replayed == secondResult.replayed {
		t.Fatalf("concurrent binding must have one writer and one exact replay: %#v, %#v", firstResult, secondResult)
	}
	got, found, err := first.ReadGitHubTargetRuntimeBinding(ctx, binding.TargetID)
	if err != nil || !found || got != binding {
		t.Fatalf("concurrent durable binding = %#v, found %t, err %v", got, found, err)
	}
}

func TestRuntimeFreshnessRetainsLastKnownReleaseAndTracksHealthTransitions(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(200, 0)
	path := filepath.Join(privateTestDir(t), "runtime-freshness-state.db")
	store, err := OpenController(ctx, path, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	// Capture the variable, not the initial receiver: this test replaces store
	// after reopen and Windows will retain the database handle otherwise.
	defer func() {
		if store == nil {
			return
		}
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close runtime freshness store: %v", closeErr)
		}
	}()
	unknown, err := store.ReadGitHubRunnerReleaseState(ctx)
	if err != nil || unknown.Freshness != RuntimeFreshnessUnknown || unknown.Generation != 0 {
		t.Fatalf("never-observed release = %#v, %v", unknown, err)
	}
	unknown, err = store.RecordGitHubRunnerReleaseFailure(ctx, GitHubObservationTimeout)
	if err != nil || unknown.Freshness != RuntimeFreshnessUnknown || unknown.FailureClass != GitHubObservationTimeout || unknown.FailureAtUnixNano != now.UnixNano() || unknown.Generation != 1 {
		t.Fatalf("release failure before success = %#v, %v", unknown, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenController(ctx, path, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	reopenedUnknown, err := store.ReadGitHubRunnerReleaseState(ctx)
	if err != nil || reopenedUnknown != unknown {
		t.Fatalf("reopened never-success failure = %#v, %v", reopenedUnknown, err)
	}
	now = now.Add(time.Second)
	release, err := store.RecordGitHubRunnerReleaseSuccess(ctx, "2.336.0", time.Unix(150, 0).UnixNano())
	if err != nil || release.Freshness != RuntimeFreshnessFresh || release.Generation != 2 {
		t.Fatalf("release recovery = %#v, %v", release, err)
	}
	now = now.Add(time.Second)
	freshAgain, err := store.RecordGitHubRunnerReleaseSuccess(ctx, "2.336.0", time.Unix(150, 0).UnixNano())
	if err != nil || freshAgain.Generation != release.Generation || freshAgain.ObservedAtUnixNano <= release.ObservedAtUnixNano {
		t.Fatalf("same release observation = %#v, %v", freshAgain, err)
	}
	if _, err := store.RecordGitHubRunnerReleaseSuccess(ctx, "2.335.0", time.Unix(149, 0).UnixNano()); err == nil {
		t.Fatal("regressed release publication time was accepted")
	}
	// A wall-clock rollback cannot make a later durable failure older.
	now = time.Unix(1, 0)
	staleBeforeRecovery, err := store.RecordGitHubRunnerReleaseFailure(ctx, GitHubObservationProvider5xx)
	if err != nil || staleBeforeRecovery.Freshness != RuntimeFreshnessStale || staleBeforeRecovery.Generation != freshAgain.Generation+1 || staleBeforeRecovery.FailureAtUnixNano <= freshAgain.ObservedAtUnixNano {
		t.Fatalf("clock-rollback release failure = %#v, %v", staleBeforeRecovery, err)
	}
	now = time.Unix(203, 0)
	changedRelease, err := store.RecordGitHubRunnerReleaseSuccess(ctx, "2.337.0", time.Unix(200, 0).UnixNano())
	if err != nil || changedRelease.Generation != staleBeforeRecovery.Generation+1 || changedRelease.ObservedAtUnixNano <= staleBeforeRecovery.FailureAtUnixNano {
		t.Fatalf("changed release observation = %#v, %v", changedRelease, err)
	}
	stale, err := store.RecordGitHubRunnerReleaseFailure(ctx, GitHubObservationProvider429)
	if err != nil || stale.Freshness != RuntimeFreshnessStale || stale.Generation != changedRelease.Generation+1 || stale.LatestVersion != changedRelease.LatestVersion || stale.LatestReleasedAtUnixNano != changedRelease.LatestReleasedAtUnixNano || stale.FailureAtUnixNano <= changedRelease.ObservedAtUnixNano {
		t.Fatalf("stale release retention = %#v, %v", stale, err)
	}
	staleAgain, err := store.RecordGitHubRunnerReleaseFailure(ctx, GitHubObservationTimeout)
	if err != nil || staleAgain.Generation != stale.Generation || staleAgain.LatestVersion != stale.LatestVersion || staleAgain.FailureClass != GitHubObservationTimeout || staleAgain.FailureAtUnixNano <= stale.FailureAtUnixNano {
		t.Fatalf("repeated stale release failure = %#v, %v", staleAgain, err)
	}

	const scaleSetID ScaleSetID = 91
	health, err := store.RecordGitHubScaleSetSessionFailure(ctx, scaleSetID, GitHubObservationNetwork)
	if err != nil || health.Freshness != RuntimeFreshnessUnknown || health.TransitionGeneration != 1 || health.FailureAtUnixNano <= 0 {
		t.Fatalf("unknown session failure = %#v, %v", health, err)
	}
	healthAgain, err := store.RecordGitHubScaleSetSessionFailure(ctx, scaleSetID, GitHubObservationTimeout)
	if err != nil || healthAgain.TransitionGeneration != health.TransitionGeneration || healthAgain.FailureClass != GitHubObservationTimeout || healthAgain.FailureAtUnixNano <= health.FailureAtUnixNano {
		t.Fatalf("repeated unknown session failure = %#v, %v", healthAgain, err)
	}
	now = now.Add(time.Second)
	health, err = store.RecordGitHubScaleSetSessionSuccess(ctx, scaleSetID)
	if err != nil || health.Freshness != RuntimeFreshnessFresh || health.TransitionGeneration != 2 {
		t.Fatalf("session recovery = %#v, %v", health, err)
	}
	now = now.Add(time.Second)
	healthAgain, err = store.RecordGitHubScaleSetSessionSuccess(ctx, scaleSetID)
	if err != nil || healthAgain.TransitionGeneration != health.TransitionGeneration || healthAgain.LastSuccessAtUnixNano <= health.LastSuccessAtUnixNano {
		t.Fatalf("repeated fresh session success = %#v, %v", healthAgain, err)
	}
	now = now.Add(time.Second)
	health, err = store.RecordGitHubScaleSetSessionFailure(ctx, scaleSetID, GitHubObservationProvider5xx)
	if err != nil || health.Freshness != RuntimeFreshnessStale || health.TransitionGeneration != 3 {
		t.Fatalf("session stale = %#v, %v", health, err)
	}
	healthAgain, err = store.RecordGitHubScaleSetSessionFailure(ctx, scaleSetID, GitHubObservationTimeout)
	if err != nil || healthAgain.TransitionGeneration != health.TransitionGeneration || healthAgain.FailureClass != GitHubObservationTimeout || healthAgain.FailureAtUnixNano <= health.FailureAtUnixNano {
		t.Fatalf("repeated stale session failure = %#v, %v", healthAgain, err)
	}
}

func TestRuntimeFreshnessRejectsInvalidInputAndMigratesExistingControllerData(t *testing.T) {
	ctx := context.Background()
	store := openController(t, "runtime-freshness-invalid.db")
	for _, policy := range []RunnerProfileUpdatePolicy{
		{ProfileID: " linux", VersionPolicy: domain.RunnerVersionPinned, RunnerVersion: "2.336.0", Revision: 1},
		{ProfileID: "linux", VersionPolicy: domain.RunnerVersionPinned, RunnerVersion: " 2.336.0", Revision: 1},
		{ProfileID: "linux", VersionPolicy: domain.RunnerVersionPinned, RunnerVersion: strings.Repeat("1", 65), Revision: 1},
	} {
		if _, err := store.ConfigureRunnerProfile(ctx, policy); err == nil {
			t.Fatalf("non-canonical runner profile was accepted: %#v", policy)
		}
	}
	if _, err := store.ConfigureRunnerProfile(ctx, RunnerProfileUpdatePolicy{ProfileID: "linux", VersionPolicy: domain.RunnerVersionPinned, RunnerVersion: "2.336.0", Revision: 0}); err == nil {
		t.Fatal("zero profile revision was accepted")
	}
	for _, binding := range []GitHubTargetRuntimeBinding{
		{TargetID: " target", ScaleSetID: 1, ProfileID: "linux"},
		{TargetID: "target", ScaleSetID: 1, ProfileID: "linux "},
	} {
		if _, err := store.ConfigureGitHubTargetRuntimeBinding(ctx, binding); err == nil {
			t.Fatalf("non-canonical target binding was accepted: %#v", binding)
		}
	}
	if _, err := store.ConfigureGitHubTargetRuntimeBinding(ctx, GitHubTargetRuntimeBinding{TargetID: "target", ScaleSetID: 0, ProfileID: "linux"}); err == nil {
		t.Fatal("zero scale-set binding was accepted")
	}
	if _, err := store.RecordGitHubRunnerReleaseSuccess(ctx, "", time.Now().UnixNano()); err == nil {
		t.Fatal("empty release version was accepted")
	}
	for _, version := range []string{" 2.336.0", "2.336.0 ", strings.Repeat("1", 65)} {
		if _, err := store.RecordGitHubRunnerReleaseSuccess(ctx, version, time.Now().UnixNano()); err == nil {
			t.Fatalf("non-canonical release version %q was accepted", version)
		}
	}
	if _, err := store.RecordGitHubRunnerReleaseSuccess(ctx, "2.336.0", 0); err == nil {
		t.Fatal("zero release time was accepted")
	}
	if _, err := store.RecordGitHubRunnerReleaseSuccess(ctx, "2.336.0", -1); err == nil {
		t.Fatal("negative release time was accepted")
	}
	if _, err := store.RecordGitHubRunnerReleaseSuccess(ctx, "2.336.0", time.Now().Add(time.Hour).UnixNano()); err == nil {
		t.Fatal("future release time was accepted")
	}
	if _, err := store.RecordGitHubScaleSetSessionFailure(ctx, 1, "raw provider error"); err == nil {
		t.Fatal("raw failure value was accepted")
	}
	if state, err := store.RecordGitHubScaleSetSessionFailure(
		ctx, 2, GitHubObservationProviderAuth,
	); err != nil || state.FailureClass != GitHubObservationProviderAuth {
		t.Fatalf("allowlisted provider auth failure = %#v, %v", state, err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO github_runner_release_state(
		singleton, freshness, failure_class, failure_at_unix_nano, generation
	) VALUES (1, 'unknown', 'timeout', 0, 1)`); err == nil {
		t.Fatal("release state CHECK accepted unknown failure without a failure timestamp")
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO github_scale_set_session_health(
		scale_set_id, freshness, last_success_at_unix_nano, failure_class, failure_at_unix_nano, transition_generation
	) VALUES (12, 'fresh', 1, 'timeout', 2, 1)`); err == nil {
		t.Fatal("session health CHECK accepted fresh state with a failure")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(privateTestDir(t), "existing-controller.db")
	dsn, err := sqliteDSN(path, false)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(sqliteDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := configure(ctx, db); err != nil {
		t.Fatal(err)
	}
	migrations, err := loadMigrations("controller", controllerMigrations)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyLoadedMigrations(ctx, db, "controller", migrations[:5], func() time.Time { return time.Unix(300, 0) }, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO github_session_demand(
		scale_set_id, session_id, total_available_jobs, total_acquired_jobs, total_assigned_jobs,
		total_running_jobs, total_registered_runners, total_busy_runners, total_idle_runners, observed_at_unix_nano
	) VALUES (11, 'existing-session', 1, 0, 0, 0, 1, 0, 1, 300000000000)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	migrated, err := OpenController(ctx, path, Options{Now: func() time.Time { return time.Unix(301, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	var sessionID string
	if err := migrated.db.QueryRowContext(ctx, `SELECT session_id FROM github_session_demand WHERE scale_set_id = 11`).Scan(&sessionID); err != nil || sessionID != "existing-session" {
		t.Fatalf("existing data after migration = %q, %v", sessionID, err)
	}
	columns := tableColumns(t, migrated.db, "agent_session_snapshots")
	if !freshnessContainsString(columns, "runner_version") {
		t.Fatalf("agent snapshot runner_version migration missing from %v", columns)
	}
	releaseColumns := tableColumns(t, migrated.db, "github_runner_release_state")
	if !freshnessContainsString(releaseColumns, "latest_released_at_unix_nano") || !freshnessContainsString(releaseColumns, "failure_at_unix_nano") {
		t.Fatalf("runtime release migration columns missing from %v", releaseColumns)
	}
}

func freshnessContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
