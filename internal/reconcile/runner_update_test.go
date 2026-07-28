package reconcile

import (
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/domain"
)

func TestEvaluateRunnerUpdatePolicy(t *testing.T) {
	released := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	observed := released.Add(time.Hour)
	tests := []struct {
		name      string
		now       time.Time
		policy    domain.RunnerVersionPolicy
		release   RunnerReleaseObservation
		wantState RunnerUpdateState
		wantReady bool
	}{
		{
			name:      "official auto update is managed",
			now:       released,
			policy:    domain.RunnerVersionAutoUpdate,
			wantState: RunnerUpdateManaged,
			wantReady: true,
		},
		{
			name:   "pinned version is current",
			now:    released.Add(time.Hour),
			policy: domain.RunnerVersionPinned,
			release: RunnerReleaseObservation{
				HasValue:         true,
				PinnedVersion:    "2.336.0",
				LatestVersion:    "2.336.0",
				LatestReleasedAt: released,
				ObservedAt:       observed,
			},
			wantState: RunnerUpdateCurrent,
			wantReady: true,
		},
		{
			name:   "new release remains supported before deadline",
			now:    released.Add(GitHubRunnerUpdateWindow - time.Nanosecond),
			policy: domain.RunnerVersionPinned,
			release: RunnerReleaseObservation{
				HasValue:         true,
				PinnedVersion:    "2.335.0",
				LatestVersion:    "2.336.0",
				LatestReleasedAt: released,
				ObservedAt:       observed,
			},
			wantState: RunnerUpdateDue,
			wantReady: true,
		},
		{
			name:   "deadline is fail closed",
			now:    released.Add(GitHubRunnerUpdateWindow),
			policy: domain.RunnerVersionPinned,
			release: RunnerReleaseObservation{
				HasValue:         true,
				PinnedVersion:    "2.335.0",
				LatestVersion:    "2.336.0",
				LatestReleasedAt: released,
				ObservedAt:       observed,
			},
			wantState: RunnerUpdateExpired,
			wantReady: false,
		},
		{
			name:   "stale release observation is unknown",
			now:    released.Add(time.Hour),
			policy: domain.RunnerVersionPinned,
			release: RunnerReleaseObservation{
				HasValue:         true,
				PinnedVersion:    "2.336.0",
				LatestVersion:    "2.336.0",
				LatestReleasedAt: released,
				ObservedAt:       observed,
				Stale:            true,
			},
			wantState: RunnerUpdateUnknown,
			wantReady: false,
		},
		{
			name:   "unchanged release cannot remain fresh past provider window",
			now:    observed.Add(GitHubRunnerUpdateWindow),
			policy: domain.RunnerVersionPinned,
			release: RunnerReleaseObservation{
				HasValue:         true,
				PinnedVersion:    "2.336.0",
				LatestVersion:    "2.336.0",
				LatestReleasedAt: released,
				ObservedAt:       observed,
			},
			wantState: RunnerUpdateUnknown,
			wantReady: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, err := EvaluateRunnerUpdate(test.now, test.policy, test.release)
			if err != nil {
				t.Fatal(err)
			}
			if status.State != test.wantState || status.AllowsAdmissionAt(test.now) != test.wantReady {
				t.Fatalf("status = %#v, admission=%t", status, status.AllowsAdmissionAt(test.now))
			}
		})
	}
}

func TestEvaluatePinnedRunnerRejectsMissingOrImpossibleReleaseEvidence(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	tests := []struct {
		name    string
		release RunnerReleaseObservation
	}{
		{
			name: "future release timestamp",
			release: RunnerReleaseObservation{
				HasValue:         true,
				PinnedVersion:    "2.336.0",
				LatestVersion:    "2.336.0",
				LatestReleasedAt: now.Add(time.Hour),
				ObservedAt:       now.Add(2 * time.Hour),
			},
		},
		{
			name: "observation predates release",
			release: RunnerReleaseObservation{
				HasValue:         true,
				PinnedVersion:    "2.336.0",
				LatestVersion:    "2.336.0",
				LatestReleasedAt: now,
				ObservedAt:       now.Add(-time.Hour),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := EvaluateRunnerUpdate(now, domain.RunnerVersionPinned, test.release); err == nil {
				t.Fatalf("invalid release evidence accepted: %#v", test.release)
			}
		})
	}
}

func TestEvaluatePinnedRunnerObservationClockBoundary(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	release := RunnerReleaseObservation{
		HasValue:         true,
		PinnedVersion:    "2.336.0",
		LatestVersion:    "2.336.0",
		LatestReleasedAt: now.Add(-time.Hour),
		ObservedAt:       now,
	}

	status, err := EvaluateRunnerUpdate(now, domain.RunnerVersionPinned, release)
	if err != nil {
		t.Fatalf("current observation rejected: %v", err)
	}
	if status.State != RunnerUpdateCurrent || !status.AllowsAdmissionAt(now) {
		t.Fatalf("current observation status = %#v", status)
	}

	release.ObservedAt = now.Add(time.Nanosecond)
	if _, err := EvaluateRunnerUpdate(now, domain.RunnerVersionPinned, release); err == nil {
		t.Fatal("future observation accepted after controller clock rollback")
	}
}

func TestRunnerUpdateAdmissionRejectsFutureObservation(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	observed := now.Add(time.Hour)
	status := RunnerUpdateStatus{
		State:            RunnerUpdateCurrent,
		PinnedVersion:    "2.336.0",
		LatestVersion:    "2.336.0",
		LatestReleasedAt: now,
		ObservedAt:       observed,
		FreshUntil:       observed.Add(GitHubRunnerUpdateWindow),
	}
	if err := status.Validate(); err != nil {
		t.Fatal(err)
	}
	if status.AllowsAdmissionAt(now) {
		t.Fatal("future release observation admitted new capacity")
	}
}

func TestEvaluatePinnedRunnerWithoutObservationIsExplicitUnknown(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	status, err := EvaluateRunnerUpdate(now, domain.RunnerVersionPinned, RunnerReleaseObservation{
		PinnedVersion: "2.336.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.State != RunnerUpdateUnknown || status.PinnedVersion != "2.336.0" ||
		status.LatestVersion != "" || status.AllowsAdmissionAt(now) {
		t.Fatalf("never-observed pinned status = %#v", status)
	}
}
