package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/genm/sparerunner/internal/domain"
)

// RuntimeFreshness is deliberately tri-state. Unknown means no successful
// observation exists, so callers cannot interpret an empty value as healthy.
type RuntimeFreshness string

const (
	RuntimeFreshnessUnknown RuntimeFreshness = "unknown"
	RuntimeFreshnessFresh   RuntimeFreshness = "fresh"
	RuntimeFreshnessStale   RuntimeFreshness = "stale"
)

// GitHubObservationFailureClass is an allowlist for provider failures. Raw
// errors can contain URLs, headers, and response fragments, so they never cross
// this persistence boundary.
type GitHubObservationFailureClass string

const (
	GitHubObservationTimeout         GitHubObservationFailureClass = "timeout"
	GitHubObservationNetwork         GitHubObservationFailureClass = "network"
	GitHubObservationProviderAuth    GitHubObservationFailureClass = "provider_auth"
	GitHubObservationProvider429     GitHubObservationFailureClass = "provider_429"
	GitHubObservationProvider5xx     GitHubObservationFailureClass = "provider_5xx"
	GitHubObservationInvalidResponse GitHubObservationFailureClass = "invalid_response"
)

type RunnerProfileUpdatePolicy struct {
	ProfileID     domain.RunnerProfileID
	VersionPolicy domain.RunnerVersionPolicy
	RunnerVersion string
	Revision      uint64
}

type GitHubTargetRuntimeBinding struct {
	TargetID   domain.TargetID
	ScaleSetID ScaleSetID
	ProfileID  domain.RunnerProfileID
}

type GitHubRunnerReleaseState struct {
	Freshness                RuntimeFreshness
	LatestVersion            string
	LatestReleasedAtUnixNano int64
	ObservedAtUnixNano       int64
	FailureClass             GitHubObservationFailureClass
	FailureAtUnixNano        int64
	Generation               uint64
}

type GitHubScaleSetSessionHealth struct {
	ScaleSetID            ScaleSetID
	Freshness             RuntimeFreshness
	LastSuccessAtUnixNano int64
	FailureClass          GitHubObservationFailureClass
	FailureAtUnixNano     int64
	TransitionGeneration  uint64
}

// GitHubRuntimeFreshness is the poll-start authority read from one SQLite
// snapshot. Binding, profile, release, and session state cannot be torn across
// independent reads while a configuration writer is committing.
type GitHubRuntimeFreshness struct {
	Binding GitHubTargetRuntimeBinding
	Profile RunnerProfileUpdatePolicy
	Release GitHubRunnerReleaseState
	Session GitHubScaleSetSessionHealth
}

var (
	ErrRuntimeFreshnessConflict        = errors.New("runtime freshness configuration conflict")
	ErrRuntimeFreshnessState           = errors.New("runtime freshness state is invalid")
	ErrRuntimeFreshnessBindingMissing  = errors.New("runtime freshness target binding is missing")
	ErrRuntimeFreshnessBindingMismatch = errors.New("runtime freshness target binding does not match")
)

// ConfigureRunnerProfile creates a profile policy at revision one, accepts an
// exact replay, or advances exactly one revision. Skipping or reusing a revision
// with changed data is rejected so a stale configuration writer cannot win.
func (s *ControllerStore) ConfigureRunnerProfile(ctx context.Context, policy RunnerProfileUpdatePolicy) (bool, error) {
	if err := s.requireReady(); err != nil {
		return false, err
	}
	if err := validateRunnerProfileUpdatePolicy(policy); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var existing RunnerProfileUpdatePolicy
	err = tx.QueryRowContext(ctx, `SELECT profile_id, version_policy, runner_version, revision
		FROM runner_profile_update_policies WHERE profile_id = ?`, policy.ProfileID).
		Scan(&existing.ProfileID, &existing.VersionPolicy, &existing.RunnerVersion, &existing.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		if policy.Revision != 1 {
			return false, fmt.Errorf("%w: initial profile revision must be one", ErrRuntimeFreshnessConflict)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO runner_profile_update_policies(
			profile_id, version_policy, runner_version, revision
		) VALUES (?, ?, ?, ?)`, policy.ProfileID, policy.VersionPolicy, policy.RunnerVersion, policy.Revision); err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if existing == policy {
		return true, tx.Commit()
	}
	if policy.Revision != existing.Revision+1 {
		return false, fmt.Errorf("%w: profile revision is not the next revision", ErrRuntimeFreshnessConflict)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runner_profile_update_policies
		SET version_policy = ?, runner_version = ?, revision = ?
		WHERE profile_id = ? AND revision = ?`,
		policy.VersionPolicy, policy.RunnerVersion, policy.Revision,
		policy.ProfileID, existing.Revision); err != nil {
		return false, err
	}
	return false, tx.Commit()
}

func (s *ControllerStore) ReadRunnerProfile(ctx context.Context, profileID domain.RunnerProfileID) (RunnerProfileUpdatePolicy, bool, error) {
	if err := s.requireReady(); err != nil {
		return RunnerProfileUpdatePolicy{}, false, err
	}
	if !canonicalRuntimeIdentifier(string(profileID)) {
		return RunnerProfileUpdatePolicy{}, false, errors.New("runner profile ID is required")
	}
	var policy RunnerProfileUpdatePolicy
	err := s.db.QueryRowContext(ctx, `SELECT profile_id, version_policy, runner_version, revision
		FROM runner_profile_update_policies WHERE profile_id = ?`, profileID).
		Scan(&policy.ProfileID, &policy.VersionPolicy, &policy.RunnerVersion, &policy.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return RunnerProfileUpdatePolicy{}, false, nil
	}
	if err != nil {
		return RunnerProfileUpdatePolicy{}, false, err
	}
	if err := validateRunnerProfileUpdatePolicy(policy); err != nil {
		return RunnerProfileUpdatePolicy{}, false, fmt.Errorf("%w: stored runner profile: %v", ErrRuntimeFreshnessState, err)
	}
	return policy, true, nil
}

// ConfigureGitHubTargetRuntimeBinding is exact replay only. Changing either
// side of a persisted target-to-scale-set route must be an explicit replacement
// operation in a future configuration contract, never an accidental upsert.
func (s *ControllerStore) ConfigureGitHubTargetRuntimeBinding(ctx context.Context, binding GitHubTargetRuntimeBinding) (bool, error) {
	if err := s.requireReady(); err != nil {
		return false, err
	}
	if err := validateGitHubTargetRuntimeBinding(binding); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var existing GitHubTargetRuntimeBinding
	err = tx.QueryRowContext(ctx, `SELECT target_id, scale_set_id, profile_id
		FROM github_target_runtime_bindings WHERE target_id = ?`, binding.TargetID).
		Scan(&existing.TargetID, &existing.ScaleSetID, &existing.ProfileID)
	if err == nil {
		if existing != binding {
			return false, fmt.Errorf("%w: target binding differs from durable mapping", ErrRuntimeFreshnessConflict)
		}
		return true, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	var profileID domain.RunnerProfileID
	if err := tx.QueryRowContext(ctx, `SELECT profile_id FROM runner_profile_update_policies WHERE profile_id = ?`, binding.ProfileID).Scan(&profileID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("%w: runner profile is not configured", ErrRuntimeFreshnessConflict)
		}
		return false, err
	}
	var boundTargetID domain.TargetID
	if err := tx.QueryRowContext(ctx, `SELECT target_id FROM github_target_runtime_bindings WHERE scale_set_id = ?`, binding.ScaleSetID).Scan(&boundTargetID); err == nil {
		return false, fmt.Errorf("%w: scale set is already bound to target %q", ErrRuntimeFreshnessConflict, boundTargetID)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_target_runtime_bindings(target_id, scale_set_id, profile_id)
		VALUES (?, ?, ?)`, binding.TargetID, binding.ScaleSetID, binding.ProfileID); err != nil {
		return false, err
	}
	return false, tx.Commit()
}

func (s *ControllerStore) ReadGitHubTargetRuntimeBinding(ctx context.Context, targetID domain.TargetID) (GitHubTargetRuntimeBinding, bool, error) {
	if err := s.requireReady(); err != nil {
		return GitHubTargetRuntimeBinding{}, false, err
	}
	if !canonicalRuntimeIdentifier(string(targetID)) {
		return GitHubTargetRuntimeBinding{}, false, errors.New("GitHub target ID is required")
	}
	var binding GitHubTargetRuntimeBinding
	err := s.db.QueryRowContext(ctx, `SELECT target_id, scale_set_id, profile_id
		FROM github_target_runtime_bindings WHERE target_id = ?`, targetID).
		Scan(&binding.TargetID, &binding.ScaleSetID, &binding.ProfileID)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubTargetRuntimeBinding{}, false, nil
	}
	if err != nil {
		return GitHubTargetRuntimeBinding{}, false, err
	}
	if err := validateGitHubTargetRuntimeBinding(binding); err != nil {
		return GitHubTargetRuntimeBinding{}, false, fmt.Errorf("%w: stored GitHub target binding: %v", ErrRuntimeFreshnessState, err)
	}
	return binding, true, nil
}

// ReadGitHubRuntimeFreshness returns all poll-start authority from one SQLite
// read transaction. Callers supply the expected complete binding so a target
// remap, missing configuration, or profile mismatch fails before a poll can use
// a torn combination of release and session state.
func (s *ControllerStore) ReadGitHubRuntimeFreshness(ctx context.Context, expected GitHubTargetRuntimeBinding) (GitHubRuntimeFreshness, error) {
	if err := s.requireReady(); err != nil {
		return GitHubRuntimeFreshness{}, err
	}
	if err := validateGitHubTargetRuntimeBinding(expected); err != nil {
		return GitHubRuntimeFreshness{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return GitHubRuntimeFreshness{}, err
	}
	defer tx.Rollback()
	result, err := readGitHubRuntimeFreshness(ctx, tx, expected)
	if err != nil {
		return GitHubRuntimeFreshness{}, err
	}
	if err := tx.Commit(); err != nil {
		return GitHubRuntimeFreshness{}, err
	}
	return result, nil
}

func readGitHubRuntimeFreshness(
	ctx context.Context,
	queryer freshnessQueryer,
	expected GitHubTargetRuntimeBinding,
) (GitHubRuntimeFreshness, error) {
	var binding GitHubTargetRuntimeBinding
	err := queryer.QueryRowContext(ctx, `SELECT target_id, scale_set_id, profile_id
		FROM github_target_runtime_bindings WHERE target_id = ?`, expected.TargetID).
		Scan(&binding.TargetID, &binding.ScaleSetID, &binding.ProfileID)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubRuntimeFreshness{}, fmt.Errorf("%w: target %q", ErrRuntimeFreshnessBindingMissing, expected.TargetID)
	}
	if err != nil {
		return GitHubRuntimeFreshness{}, err
	}
	if err := validateGitHubTargetRuntimeBinding(binding); err != nil {
		return GitHubRuntimeFreshness{}, fmt.Errorf("%w: stored target binding: %v", ErrRuntimeFreshnessState, err)
	}
	if binding != expected {
		return GitHubRuntimeFreshness{}, fmt.Errorf("%w: expected %+v, stored %+v", ErrRuntimeFreshnessBindingMismatch, expected, binding)
	}
	var profile RunnerProfileUpdatePolicy
	err = queryer.QueryRowContext(ctx, `SELECT profile_id, version_policy, runner_version, revision
		FROM runner_profile_update_policies WHERE profile_id = ?`, binding.ProfileID).
		Scan(&profile.ProfileID, &profile.VersionPolicy, &profile.RunnerVersion, &profile.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubRuntimeFreshness{}, fmt.Errorf("%w: profile %q", ErrRuntimeFreshnessBindingMissing, binding.ProfileID)
	}
	if err != nil {
		return GitHubRuntimeFreshness{}, err
	}
	if err := validateRunnerProfileUpdatePolicy(profile); err != nil {
		return GitHubRuntimeFreshness{}, fmt.Errorf("%w: stored runner profile: %v", ErrRuntimeFreshnessState, err)
	}
	release, releaseFound, err := readGitHubRunnerReleaseState(ctx, queryer)
	if err != nil {
		return GitHubRuntimeFreshness{}, err
	}
	if !releaseFound {
		release = GitHubRunnerReleaseState{Freshness: RuntimeFreshnessUnknown}
	}
	session, sessionFound, err := readGitHubScaleSetSessionHealth(ctx, queryer, binding.ScaleSetID)
	if err != nil {
		return GitHubRuntimeFreshness{}, err
	}
	if !sessionFound {
		session = GitHubScaleSetSessionHealth{ScaleSetID: binding.ScaleSetID, Freshness: RuntimeFreshnessUnknown}
	}
	return GitHubRuntimeFreshness{Binding: binding, Profile: profile, Release: release, Session: session}, nil
}

func (s *ControllerStore) RecordGitHubRunnerReleaseSuccess(ctx context.Context, latestVersion string, latestReleasedAtUnixNano int64) (GitHubRunnerReleaseState, error) {
	if err := s.requireReady(); err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	if !canonicalRunnerVersion(latestVersion) {
		return GitHubRunnerReleaseState{}, errors.New("GitHub runner release version is invalid")
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	if err := validateGitHubReleaseTime(latestReleasedAtUnixNano, now); err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	defer tx.Rollback()
	previous, found, err := readGitHubRunnerReleaseState(ctx, tx)
	if err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	if !found {
		state := GitHubRunnerReleaseState{Freshness: RuntimeFreshnessFresh, LatestVersion: latestVersion, LatestReleasedAtUnixNano: latestReleasedAtUnixNano, ObservedAtUnixNano: now, Generation: 1}
		if err := insertGitHubRunnerReleaseState(ctx, tx, state); err != nil {
			return GitHubRunnerReleaseState{}, err
		}
		return state, tx.Commit()
	}
	if previous.LatestReleasedAtUnixNano > latestReleasedAtUnixNano {
		return GitHubRunnerReleaseState{}, errors.New("GitHub runner release time regressed")
	}
	now, err = monotonicFreshnessTimestamp(now, maxFreshnessTimestamp(previous.ObservedAtUnixNano, previous.FailureAtUnixNano), "GitHub runner release observation")
	if err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	state := GitHubRunnerReleaseState{Freshness: RuntimeFreshnessFresh, LatestVersion: latestVersion, LatestReleasedAtUnixNano: latestReleasedAtUnixNano, ObservedAtUnixNano: now, Generation: previous.Generation}
	if previous.Freshness != RuntimeFreshnessFresh || previous.LatestVersion != latestVersion || previous.LatestReleasedAtUnixNano != latestReleasedAtUnixNano {
		state.Generation++
	}
	if err := updateGitHubRunnerReleaseState(ctx, tx, state); err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	return state, tx.Commit()
}

func (s *ControllerStore) RecordGitHubRunnerReleaseFailure(ctx context.Context, class GitHubObservationFailureClass) (GitHubRunnerReleaseState, error) {
	if err := s.requireReady(); err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	if err := validateGitHubObservationFailureClass(class); err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	defer tx.Rollback()
	previous, found, err := readGitHubRunnerReleaseState(ctx, tx)
	if err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	if !found {
		state := GitHubRunnerReleaseState{Freshness: RuntimeFreshnessUnknown, FailureClass: class, FailureAtUnixNano: now, Generation: 1}
		if err := insertGitHubRunnerReleaseState(ctx, tx, state); err != nil {
			return GitHubRunnerReleaseState{}, err
		}
		return state, tx.Commit()
	}
	state := previous
	state.FailureClass = class
	now, err = monotonicFreshnessTimestamp(now, maxFreshnessTimestamp(previous.ObservedAtUnixNano, previous.FailureAtUnixNano), "GitHub runner release failure")
	if err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	state.FailureAtUnixNano = now
	if previous.Freshness == RuntimeFreshnessFresh {
		state.Freshness = RuntimeFreshnessStale
		state.Generation++
	}
	if err := updateGitHubRunnerReleaseState(ctx, tx, state); err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	return state, tx.Commit()
}

func (s *ControllerStore) ReadGitHubRunnerReleaseState(ctx context.Context) (GitHubRunnerReleaseState, error) {
	if err := s.requireReady(); err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	state, found, err := readGitHubRunnerReleaseState(ctx, s.db)
	if err != nil {
		return GitHubRunnerReleaseState{}, err
	}
	if !found {
		return GitHubRunnerReleaseState{Freshness: RuntimeFreshnessUnknown}, nil
	}
	return state, nil
}

func (s *ControllerStore) RecordGitHubScaleSetSessionSuccess(ctx context.Context, scaleSetID ScaleSetID) (GitHubScaleSetSessionHealth, error) {
	if err := s.requireReady(); err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	if err := validateScaleSetID(scaleSetID); err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	defer tx.Rollback()
	previous, found, err := readGitHubScaleSetSessionHealth(ctx, tx, scaleSetID)
	if err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	if !found {
		state := GitHubScaleSetSessionHealth{ScaleSetID: scaleSetID, Freshness: RuntimeFreshnessFresh, LastSuccessAtUnixNano: now, TransitionGeneration: 1}
		if err := insertGitHubScaleSetSessionHealth(ctx, tx, state); err != nil {
			return GitHubScaleSetSessionHealth{}, err
		}
		return state, tx.Commit()
	}
	now, err = monotonicFreshnessTimestamp(now, maxFreshnessTimestamp(previous.LastSuccessAtUnixNano, previous.FailureAtUnixNano), "GitHub scale-set session success")
	if err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	state := previous
	state.Freshness = RuntimeFreshnessFresh
	state.LastSuccessAtUnixNano = now
	state.FailureClass = ""
	state.FailureAtUnixNano = 0
	if previous.Freshness != RuntimeFreshnessFresh {
		state.TransitionGeneration++
	}
	if err := updateGitHubScaleSetSessionHealth(ctx, tx, state); err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	return state, tx.Commit()
}

func (s *ControllerStore) RecordGitHubScaleSetSessionFailure(ctx context.Context, scaleSetID ScaleSetID, class GitHubObservationFailureClass) (GitHubScaleSetSessionHealth, error) {
	if err := s.requireReady(); err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	if err := validateScaleSetID(scaleSetID); err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	if err := validateGitHubObservationFailureClass(class); err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	defer tx.Rollback()
	previous, found, err := readGitHubScaleSetSessionHealth(ctx, tx, scaleSetID)
	if err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	if !found {
		state := GitHubScaleSetSessionHealth{ScaleSetID: scaleSetID, Freshness: RuntimeFreshnessUnknown, FailureClass: class, FailureAtUnixNano: now, TransitionGeneration: 1}
		if err := insertGitHubScaleSetSessionHealth(ctx, tx, state); err != nil {
			return GitHubScaleSetSessionHealth{}, err
		}
		return state, tx.Commit()
	}
	state := previous
	state.FailureClass = class
	now, err = monotonicFreshnessTimestamp(now, maxFreshnessTimestamp(previous.LastSuccessAtUnixNano, previous.FailureAtUnixNano), "GitHub scale-set session failure")
	if err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	state.FailureAtUnixNano = now
	if previous.Freshness == RuntimeFreshnessFresh {
		state.Freshness = RuntimeFreshnessStale
		state.TransitionGeneration++
	}
	if err := updateGitHubScaleSetSessionHealth(ctx, tx, state); err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	return state, tx.Commit()
}

func (s *ControllerStore) ReadGitHubScaleSetSessionHealth(ctx context.Context, scaleSetID ScaleSetID) (GitHubScaleSetSessionHealth, error) {
	if err := s.requireReady(); err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	if err := validateScaleSetID(scaleSetID); err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	state, found, err := readGitHubScaleSetSessionHealth(ctx, s.db, scaleSetID)
	if err != nil {
		return GitHubScaleSetSessionHealth{}, err
	}
	if !found {
		return GitHubScaleSetSessionHealth{ScaleSetID: scaleSetID, Freshness: RuntimeFreshnessUnknown}, nil
	}
	return state, nil
}

func validateRunnerProfileUpdatePolicy(policy RunnerProfileUpdatePolicy) error {
	if !canonicalRuntimeIdentifier(string(policy.ProfileID)) ||
		!canonicalRunnerVersion(policy.RunnerVersion) {
		return errors.New("runner profile policy requires profile ID and runner version")
	}
	if policy.Revision == 0 || policy.Revision > maxSQLiteInteger {
		return errors.New("runner profile policy revision is invalid")
	}
	switch policy.VersionPolicy {
	case domain.RunnerVersionAutoUpdate, domain.RunnerVersionPinned:
		return nil
	default:
		return errors.New("runner profile policy version policy is invalid")
	}
}

func validateGitHubTargetRuntimeBinding(binding GitHubTargetRuntimeBinding) error {
	if !canonicalRuntimeIdentifier(string(binding.TargetID)) ||
		!canonicalRuntimeIdentifier(string(binding.ProfileID)) {
		return errors.New("GitHub target runtime binding requires target and runner profile IDs")
	}
	return validateScaleSetID(binding.ScaleSetID)
}

func canonicalRuntimeIdentifier(value string) bool {
	return value != "" && strings.TrimSpace(value) == value
}

func canonicalRunnerVersion(value string) bool {
	return canonicalRuntimeIdentifier(value) && len(value) <= 64
}

func validateScaleSetID(scaleSetID ScaleSetID) error {
	if scaleSetID == 0 || uint64(scaleSetID) > maxSQLiteInteger {
		return errors.New("GitHub scale set ID is invalid")
	}
	return nil
}

func validateGitHubObservationFailureClass(class GitHubObservationFailureClass) error {
	switch class {
	case GitHubObservationTimeout, GitHubObservationNetwork,
		GitHubObservationProviderAuth, GitHubObservationProvider429,
		GitHubObservationProvider5xx, GitHubObservationInvalidResponse:
		return nil
	default:
		return errors.New("GitHub observation failure class is invalid")
	}
}

func monotonicFreshnessTimestamp(candidate, previous int64, subject string) (int64, error) {
	if candidate > previous {
		return candidate, nil
	}
	if previous == int64(maxSQLiteInteger) {
		return 0, fmt.Errorf("%s timestamp is exhausted", subject)
	}
	return previous + 1, nil
}

func maxFreshnessTimestamp(first, second int64) int64 {
	if first > second {
		return first
	}
	return second
}

func validateGitHubReleaseTime(releasedAt, observedAt int64) error {
	if releasedAt <= 0 || releasedAt > int64(maxSQLiteInteger) {
		return errors.New("GitHub runner release time is invalid")
	}
	if releasedAt > observedAt {
		return errors.New("GitHub runner release time is in the future")
	}
	return nil
}

type freshnessQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readGitHubRunnerReleaseState(ctx context.Context, q freshnessQueryer) (GitHubRunnerReleaseState, bool, error) {
	var state GitHubRunnerReleaseState
	var version sql.NullString
	var releasedAt, observedAt, failureAt sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT freshness, latest_version, latest_released_at_unix_nano, observed_at_unix_nano, failure_class, failure_at_unix_nano, generation
		FROM github_runner_release_state WHERE singleton = 1`).
		Scan(&state.Freshness, &version, &releasedAt, &observedAt, &state.FailureClass, &failureAt, &state.Generation)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubRunnerReleaseState{}, false, nil
	}
	if err != nil {
		return GitHubRunnerReleaseState{}, false, err
	}
	state.LatestVersion, state.LatestReleasedAtUnixNano, state.ObservedAtUnixNano, state.FailureAtUnixNano = version.String, releasedAt.Int64, observedAt.Int64, failureAt.Int64
	if err := validateGitHubRunnerReleaseState(state); err != nil {
		return GitHubRunnerReleaseState{}, false, fmt.Errorf("%w: stored GitHub runner release: %v", ErrRuntimeFreshnessState, err)
	}
	return state, true, nil
}

func insertGitHubRunnerReleaseState(ctx context.Context, tx *sql.Tx, state GitHubRunnerReleaseState) error {
	if err := validateGitHubRunnerReleaseState(state); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO github_runner_release_state(
		singleton, latest_version, latest_released_at_unix_nano, observed_at_unix_nano, freshness, failure_class, failure_at_unix_nano, generation
	) VALUES (1, NULLIF(?, ''), NULLIF(?, 0), NULLIF(?, 0), ?, ?, NULLIF(?, 0), ?)`,
		state.LatestVersion, state.LatestReleasedAtUnixNano, state.ObservedAtUnixNano, state.Freshness, state.FailureClass, state.FailureAtUnixNano, state.Generation)
	return err
}

func updateGitHubRunnerReleaseState(ctx context.Context, tx *sql.Tx, state GitHubRunnerReleaseState) error {
	if err := validateGitHubRunnerReleaseState(state); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_runner_release_state
		SET latest_version = NULLIF(?, ''), latest_released_at_unix_nano = NULLIF(?, 0), observed_at_unix_nano = NULLIF(?, 0),
			freshness = ?, failure_class = ?, failure_at_unix_nano = NULLIF(?, 0), generation = ?
		WHERE singleton = 1`, state.LatestVersion, state.LatestReleasedAtUnixNano, state.ObservedAtUnixNano,
		state.Freshness, state.FailureClass, state.FailureAtUnixNano, state.Generation)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("%w: GitHub runner release row disappeared", ErrRuntimeFreshnessState)
	}
	return nil
}

func validateGitHubRunnerReleaseState(state GitHubRunnerReleaseState) error {
	if state.Generation == 0 || state.Generation > maxSQLiteInteger {
		return errors.New("GitHub runner release generation is invalid")
	}
	if state.LatestVersion != "" && !canonicalRunnerVersion(state.LatestVersion) {
		return errors.New("GitHub runner release version is invalid")
	}
	if state.FailureClass != "" {
		if err := validateGitHubObservationFailureClass(state.FailureClass); err != nil {
			return err
		}
	}
	switch state.Freshness {
	case RuntimeFreshnessUnknown:
		if state.LatestVersion != "" || state.LatestReleasedAtUnixNano != 0 || state.ObservedAtUnixNano != 0 || state.FailureClass == "" || state.FailureAtUnixNano <= 0 {
			return errors.New("unknown GitHub runner release has an observation")
		}
	case RuntimeFreshnessFresh:
		if state.LatestVersion == "" || state.LatestReleasedAtUnixNano <= 0 || state.ObservedAtUnixNano < state.LatestReleasedAtUnixNano || state.FailureClass != "" || state.FailureAtUnixNano != 0 {
			return errors.New("fresh GitHub runner release is incomplete")
		}
	case RuntimeFreshnessStale:
		if state.LatestVersion == "" || state.LatestReleasedAtUnixNano <= 0 || state.ObservedAtUnixNano < state.LatestReleasedAtUnixNano || state.FailureClass == "" || state.FailureAtUnixNano <= 0 {
			return errors.New("stale GitHub runner release is incomplete")
		}
	default:
		return errors.New("GitHub runner release freshness is invalid")
	}
	return nil
}

func readGitHubScaleSetSessionHealth(ctx context.Context, q freshnessQueryer, scaleSetID ScaleSetID) (GitHubScaleSetSessionHealth, bool, error) {
	var state GitHubScaleSetSessionHealth
	var lastSuccessAt, failureAt sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT scale_set_id, freshness, last_success_at_unix_nano, failure_class, failure_at_unix_nano, transition_generation
		FROM github_scale_set_session_health WHERE scale_set_id = ?`, scaleSetID).
		Scan(&state.ScaleSetID, &state.Freshness, &lastSuccessAt, &state.FailureClass, &failureAt, &state.TransitionGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubScaleSetSessionHealth{}, false, nil
	}
	if err != nil {
		return GitHubScaleSetSessionHealth{}, false, err
	}
	state.LastSuccessAtUnixNano, state.FailureAtUnixNano = lastSuccessAt.Int64, failureAt.Int64
	if err := validateGitHubScaleSetSessionHealth(state); err != nil {
		return GitHubScaleSetSessionHealth{}, false, fmt.Errorf("%w: stored GitHub scale-set session health: %v", ErrRuntimeFreshnessState, err)
	}
	return state, true, nil
}

func insertGitHubScaleSetSessionHealth(ctx context.Context, tx *sql.Tx, state GitHubScaleSetSessionHealth) error {
	if err := validateGitHubScaleSetSessionHealth(state); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO github_scale_set_session_health(
		scale_set_id, freshness, last_success_at_unix_nano, failure_class, failure_at_unix_nano, transition_generation
	) VALUES (?, ?, NULLIF(?, 0), ?, NULLIF(?, 0), ?)`, state.ScaleSetID, state.Freshness,
		state.LastSuccessAtUnixNano, state.FailureClass, state.FailureAtUnixNano, state.TransitionGeneration)
	return err
}

func updateGitHubScaleSetSessionHealth(ctx context.Context, tx *sql.Tx, state GitHubScaleSetSessionHealth) error {
	if err := validateGitHubScaleSetSessionHealth(state); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_scale_set_session_health
		SET freshness = ?, last_success_at_unix_nano = NULLIF(?, 0), failure_class = ?,
			failure_at_unix_nano = NULLIF(?, 0), transition_generation = ?
		WHERE scale_set_id = ?`, state.Freshness, state.LastSuccessAtUnixNano,
		state.FailureClass, state.FailureAtUnixNano, state.TransitionGeneration, state.ScaleSetID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("%w: GitHub scale-set session health row disappeared", ErrRuntimeFreshnessState)
	}
	return nil
}

func validateGitHubScaleSetSessionHealth(state GitHubScaleSetSessionHealth) error {
	if err := validateScaleSetID(state.ScaleSetID); err != nil {
		return err
	}
	if state.TransitionGeneration == 0 || state.TransitionGeneration > maxSQLiteInteger {
		return errors.New("GitHub scale-set session health generation is invalid")
	}
	if state.FailureClass != "" {
		if err := validateGitHubObservationFailureClass(state.FailureClass); err != nil {
			return err
		}
	}
	switch state.Freshness {
	case RuntimeFreshnessUnknown:
		if state.LastSuccessAtUnixNano != 0 || state.FailureClass == "" || state.FailureAtUnixNano <= 0 {
			return errors.New("unknown GitHub scale-set session health has timestamps")
		}
	case RuntimeFreshnessFresh:
		if state.LastSuccessAtUnixNano <= 0 || state.FailureClass != "" || state.FailureAtUnixNano != 0 {
			return errors.New("fresh GitHub scale-set session health is incomplete")
		}
	case RuntimeFreshnessStale:
		if state.LastSuccessAtUnixNano <= 0 || state.FailureClass == "" || state.FailureAtUnixNano <= 0 {
			return errors.New("stale GitHub scale-set session health is incomplete")
		}
	default:
		return errors.New("GitHub scale-set session health freshness is invalid")
	}
	return nil
}
