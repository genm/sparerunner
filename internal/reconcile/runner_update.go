package reconcile

import (
	"strings"
	"time"

	"github.com/genm/tewake/internal/domain"
)

// GitHubRunnerUpdateWindow is the provider-owned support window for a
// self-hosted runner with automatic updates disabled. It is a product contract,
// not an execution timeout or an arbitrary local quota.
const GitHubRunnerUpdateWindow = 30 * 24 * time.Hour

type RunnerUpdateState string

const (
	RunnerUpdateManaged RunnerUpdateState = "managed"
	RunnerUpdateCurrent RunnerUpdateState = "current"
	RunnerUpdateDue     RunnerUpdateState = "update_due"
	RunnerUpdateExpired RunnerUpdateState = "expired"
	RunnerUpdateUnknown RunnerUpdateState = "unknown"
)

// RunnerReleaseObservation is last-known GitHub release evidence. Stale
// evidence cannot prove a pinned runner remains within the support window.
type RunnerReleaseObservation struct {
	HasValue         bool
	PinnedVersion    string
	LatestVersion    string
	LatestReleasedAt time.Time
	ObservedAt       time.Time
	Stale            bool
}

// RunnerUpdateStatus is the deterministic admission result. Deadline is set
// only when a pinned version differs from the latest observed version.
type RunnerUpdateStatus struct {
	State            RunnerUpdateState
	PinnedVersion    string
	LatestVersion    string
	LatestReleasedAt time.Time
	Deadline         time.Time
	ObservedAt       time.Time
	FreshUntil       time.Time
}

func ManagedRunnerUpdate() RunnerUpdateStatus {
	return RunnerUpdateStatus{State: RunnerUpdateManaged}
}

func (status RunnerUpdateStatus) AllowsAdmissionAt(now time.Time) bool {
	switch status.State {
	case RunnerUpdateManaged:
		return true
	case RunnerUpdateCurrent:
		return !now.IsZero() &&
			!now.Before(status.ObservedAt) &&
			now.Before(status.FreshUntil)
	case RunnerUpdateDue:
		return !now.IsZero() &&
			!now.Before(status.ObservedAt) &&
			now.Before(status.Deadline) &&
			now.Before(status.FreshUntil)
	default:
		return false
	}
}

func (status RunnerUpdateStatus) MatchesPolicy(policy domain.RunnerVersionPolicy) bool {
	switch policy {
	case domain.RunnerVersionAutoUpdate:
		return status.State == RunnerUpdateManaged
	case domain.RunnerVersionPinned:
		return status.State != RunnerUpdateManaged &&
			status.State != "" &&
			status.Validate() == nil
	default:
		return false
	}
}

func (status RunnerUpdateStatus) Validate() error {
	switch status.State {
	case RunnerUpdateManaged:
		if status.PinnedVersion != "" || status.LatestVersion != "" ||
			!status.LatestReleasedAt.IsZero() || !status.Deadline.IsZero() ||
			!status.ObservedAt.IsZero() || !status.FreshUntil.IsZero() {
			return invalid("invalid_runner_update_status", "runner_update", "managed status must not carry pinned release evidence")
		}
		return nil
	case RunnerUpdateCurrent:
		if !validVersion(status.PinnedVersion) || status.PinnedVersion != status.LatestVersion ||
			status.LatestReleasedAt.IsZero() || status.ObservedAt.IsZero() ||
			status.ObservedAt.Before(status.LatestReleasedAt) ||
			!status.Deadline.IsZero() ||
			!status.FreshUntil.Equal(status.ObservedAt.Add(GitHubRunnerUpdateWindow)) {
			return invalid("invalid_runner_update_status", "runner_update", "current status requires equal versions and a successful observation")
		}
		return nil
	case RunnerUpdateDue, RunnerUpdateExpired:
		if !validVersion(status.PinnedVersion) || !validVersion(status.LatestVersion) ||
			status.PinnedVersion == status.LatestVersion ||
			status.LatestReleasedAt.IsZero() || status.ObservedAt.IsZero() ||
			status.ObservedAt.Before(status.LatestReleasedAt) ||
			!status.Deadline.Equal(status.LatestReleasedAt.Add(GitHubRunnerUpdateWindow)) ||
			!status.FreshUntil.Equal(status.ObservedAt.Add(GitHubRunnerUpdateWindow)) {
			return invalid("invalid_runner_update_status", "runner_update", "mismatched pinned versions require observation and deadline evidence")
		}
		return nil
	case RunnerUpdateUnknown:
		if !validVersion(status.PinnedVersion) {
			return invalid("invalid_runner_update_status", "runner_update", "unknown status requires a pinned version")
		}
		// A never-observed release is an explicit unknown state. It carries no
		// fabricated latest release or timestamp and therefore cannot admit.
		if status.LatestVersion == "" {
			if !status.LatestReleasedAt.IsZero() || !status.Deadline.IsZero() ||
				!status.ObservedAt.IsZero() || !status.FreshUntil.IsZero() {
				return invalid("invalid_runner_update_status", "runner_update", "never-observed status must not carry release evidence")
			}
			return nil
		}
		if !validVersion(status.LatestVersion) ||
			status.LatestReleasedAt.IsZero() || status.ObservedAt.IsZero() ||
			status.ObservedAt.Before(status.LatestReleasedAt) ||
			!status.FreshUntil.Equal(status.ObservedAt.Add(GitHubRunnerUpdateWindow)) {
			return invalid("invalid_runner_update_status", "runner_update", "unknown status requires retained release identity and observation time")
		}
		if status.PinnedVersion == status.LatestVersion && !status.Deadline.IsZero() {
			return invalid("invalid_runner_update_status", "runner_update.deadline", "current versions must not carry an update deadline")
		}
		if status.PinnedVersion != status.LatestVersion &&
			!status.Deadline.Equal(status.LatestReleasedAt.Add(GitHubRunnerUpdateWindow)) {
			return invalid("invalid_runner_update_status", "runner_update.deadline", "must preserve the provider support window")
		}
		return nil
	default:
		return invalid("invalid_runner_update_status", "runner_update.state", "is not a known runner update state")
	}
}

// EvaluateRunnerUpdate enforces GitHub's update window when the profile disables
// official runner auto-update. A stale or missing release observation fails
// closed for new runner admission but does not terminate an existing process.
func EvaluateRunnerUpdate(
	now time.Time,
	policy domain.RunnerVersionPolicy,
	release RunnerReleaseObservation,
) (RunnerUpdateStatus, error) {
	if now.IsZero() {
		return RunnerUpdateStatus{}, invalid("invalid_runner_update_time", "runner_update.now", "must not be zero")
	}
	switch policy {
	case domain.RunnerVersionAutoUpdate:
		return ManagedRunnerUpdate(), nil
	case domain.RunnerVersionPinned:
	default:
		return RunnerUpdateStatus{}, invalid("invalid_runner_update_policy", "runner_update.policy", "must be auto_update or pinned")
	}
	if !validVersion(release.PinnedVersion) {
		return RunnerUpdateStatus{}, invalid("missing_runner_release_evidence", "runner_update.pinned_version", "pinned version must be configured")
	}
	if !release.HasValue {
		status := RunnerUpdateStatus{
			State:         RunnerUpdateUnknown,
			PinnedVersion: release.PinnedVersion,
		}
		return status, status.Validate()
	}
	if !validVersion(release.LatestVersion) {
		return RunnerUpdateStatus{}, invalid("missing_runner_release_evidence", "runner_update.version", "pinned and latest versions must be observed")
	}
	if release.LatestReleasedAt.IsZero() || release.ObservedAt.IsZero() {
		return RunnerUpdateStatus{}, invalid("missing_runner_release_evidence", "runner_update.observed_at", "release and observation times must be present")
	}
	if release.ObservedAt.Before(release.LatestReleasedAt) {
		return RunnerUpdateStatus{}, invalid("invalid_runner_release_evidence", "runner_update.observed_at", "cannot precede the observed release")
	}
	if now.Before(release.LatestReleasedAt) {
		return RunnerUpdateStatus{}, invalid("invalid_runner_release_evidence", "runner_update.latest_released_at", "cannot be in the future")
	}
	// A future observation could extend freshness after a controller clock
	// rollback, so reject it instead of admitting from unverifiable evidence.
	if now.Before(release.ObservedAt) {
		return RunnerUpdateStatus{}, invalid("invalid_runner_release_evidence", "runner_update.observed_at", "cannot be in the future")
	}

	status := RunnerUpdateStatus{
		PinnedVersion:    release.PinnedVersion,
		LatestVersion:    release.LatestVersion,
		LatestReleasedAt: release.LatestReleasedAt,
		ObservedAt:       release.ObservedAt,
		FreshUntil:       release.ObservedAt.Add(GitHubRunnerUpdateWindow),
	}
	if release.PinnedVersion != release.LatestVersion {
		status.Deadline = release.LatestReleasedAt.Add(GitHubRunnerUpdateWindow)
	}
	if release.Stale {
		status.State = RunnerUpdateUnknown
		return status, nil
	}
	if release.PinnedVersion != release.LatestVersion &&
		!now.Before(status.Deadline) {
		status.State = RunnerUpdateExpired
		return status, nil
	}
	if !now.Before(status.FreshUntil) {
		status.State = RunnerUpdateUnknown
		return status, nil
	}
	if release.PinnedVersion == release.LatestVersion {
		status.State = RunnerUpdateCurrent
		return status, nil
	}
	status.State = RunnerUpdateDue
	return status, nil
}

func validVersion(version string) bool {
	return strings.TrimSpace(version) == version && version != ""
}
