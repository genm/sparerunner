package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/runner"
)

func TestGitHubPollAuthorityRejectsStateChangedAfterPollStart(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *ControllerStore, SingleSlotBinding)
	}{
		{
			name: "session became stale",
			mutate: func(t *testing.T, controller *ControllerStore, binding SingleSlotBinding) {
				t.Helper()
				if _, err := controller.RecordGitHubScaleSetSessionFailure(
					context.Background(),
					binding.PollAuthority.Binding.ScaleSetID,
					GitHubObservationProvider5xx,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "Agent snapshot revision changed",
			mutate: func(t *testing.T, controller *ControllerStore, binding SingleSlotBinding) {
				t.Helper()
				epoch, err := controller.EnrollmentEpoch(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if err := controller.RecordAgentSnapshot(context.Background(), NodeAgentSnapshot{
					NodeID:            binding.NodeID,
					OS:                domain.OSLinux,
					Architecture:      domain.ArchAMD64,
					RunnerVersion:     runner.OfficialRunnerVersion,
					NativeRunnerReady: false,
					Journal: AgentSnapshot{
						MaxControllerEpoch: epoch,
					},
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "Agent disconnected",
			mutate: func(t *testing.T, controller *ControllerStore, binding SingleSlotBinding) {
				t.Helper()
				if err := controller.RecordAgentDisconnect(
					context.Background(),
					binding.NodeID,
					binding.PollAuthority.Agent.SnapshotDigest,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "runner profile revision changed",
			mutate: func(t *testing.T, controller *ControllerStore, binding SingleSlotBinding) {
				t.Helper()
				profile := binding.PollAuthority.Binding.ProfileID
				if _, err := controller.ConfigureRunnerProfile(
					context.Background(),
					RunnerProfileUpdatePolicy{
						ProfileID:     profile,
						VersionPolicy: domain.RunnerVersionAutoUpdate,
						RunnerVersion: "2.335.0",
						Revision:      2,
					},
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "controller epoch advanced",
			mutate: func(t *testing.T, controller *ControllerStore, _ SingleSlotBinding) {
				t.Helper()
				if _, err := controller.AdvanceEpoch(context.Background()); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for index, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			controller := openController(t, "github-poll-authority-state-change.db")
			defer controller.Close()
			nodeID, _ := enrollControllerAgentNode(t, controller, 3)
			binding := SingleSlotBinding{
				TargetID:     "target-authority",
				NodeID:       domain.NodeID(nodeID),
				Slot:         0,
				ClaimEnabled: true,
			}
			const scaleSetID ScaleSetID = 71
			enableGitHubClaimForTest(
				t,
				controller,
				&binding,
				scaleSetID,
				domain.ArchAMD64,
			)

			testCase.mutate(t, controller, binding)
			commit, err := controller.CommitGitHubQueueMessage(
				context.Background(),
				githubQueueMessageForTest(
					scaleSetID,
					MessageID(710+index),
					int64(7100+index),
				),
				binding,
			)
			if err != nil {
				t.Fatal(err)
			}
			if commit.Claim != nil || !commit.UnclaimedAvailable {
				t.Fatalf("state-changed commit = %#v", commit)
			}
			assertCount(t, controller.db, "SELECT count(*) FROM github_queue_messages", 1)
			assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 0)
			assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 0)
		})
	}
}

func TestGitHubPinnedPollAuthorityClosesAtExactAdmissionDeadline(t *testing.T) {
	base := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	deadline := base.Add(time.Hour)
	tests := []struct {
		name      string
		commitAt  time.Time
		wantClaim bool
	}{
		{name: "one nanosecond before", commitAt: deadline.Add(-time.Nanosecond), wantClaim: true},
		{name: "at deadline", commitAt: deadline, wantClaim: false},
	}

	for index, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			now := base
			path := filepath.Join(privateTestDir(t), "github-pinned-deadline.db")
			controller, err := OpenController(
				context.Background(),
				path,
				Options{Now: func() time.Time { return now }},
			)
			if err != nil {
				t.Fatal(err)
			}
			defer controller.Close()
			nodeID, epoch := enrollControllerAgentNode(t, controller, 4)
			binding := SingleSlotBinding{
				TargetID:     "target-pinned",
				NodeID:       domain.NodeID(nodeID),
				Slot:         0,
				ClaimEnabled: true,
			}
			if err := controller.RecordAgentSnapshot(context.Background(), NodeAgentSnapshot{
				NodeID:            binding.NodeID,
				OS:                domain.OSLinux,
				Architecture:      domain.ArchAMD64,
				RunnerVersion:     runner.OfficialRunnerVersion,
				NativeRunnerReady: true,
				Journal: AgentSnapshot{
					MaxControllerEpoch: epoch,
				},
			}); err != nil {
				t.Fatal(err)
			}
			profileID := domain.RunnerProfileID("profile-pinned")
			if _, err := controller.ConfigureRunnerProfile(
				context.Background(),
				RunnerProfileUpdatePolicy{
					ProfileID:     profileID,
					VersionPolicy: domain.RunnerVersionPinned,
					RunnerVersion: runner.OfficialRunnerVersion,
					Revision:      1,
				},
			); err != nil {
				t.Fatal(err)
			}
			runtimeBinding := GitHubTargetRuntimeBinding{
				TargetID:   binding.TargetID,
				ScaleSetID: 72,
				ProfileID:  profileID,
			}
			if _, err := controller.ConfigureGitHubTargetRuntimeBinding(
				context.Background(),
				runtimeBinding,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := controller.RecordGitHubRunnerReleaseSuccess(
				context.Background(),
				runner.OfficialRunnerVersion,
				base.Add(-24*time.Hour).UnixNano(),
			); err != nil {
				t.Fatal(err)
			}
			if _, err := controller.RecordGitHubScaleSetSessionSuccess(
				context.Background(),
				runtimeBinding.ScaleSetID,
			); err != nil {
				t.Fatal(err)
			}
			pollState, err := controller.ReadGitHubPollState(
				context.Background(),
				runtimeBinding,
				binding.NodeID,
			)
			if err != nil {
				t.Fatal(err)
			}
			pollState.ClaimAuthority.AdvertisedCapacity = 1
			pollState.ClaimAuthority.AdmissionDeadlineUnixNano = deadline.UnixNano()
			binding.PollAuthority = pollState.ClaimAuthority

			now = testCase.commitAt
			commit, err := controller.CommitGitHubQueueMessage(
				context.Background(),
				githubQueueMessageForTest(
					runtimeBinding.ScaleSetID,
					MessageID(720+index),
					int64(7200+index),
				),
				binding,
			)
			if err != nil {
				t.Fatal(err)
			}
			if (commit.Claim != nil) != testCase.wantClaim ||
				commit.UnclaimedAvailable == testCase.wantClaim {
				t.Fatalf("deadline commit = %#v, want claim %t", commit, testCase.wantClaim)
			}
		})
	}
}

func TestGitHubZeroCapacityReplayAcknowledgesExistingDurableClaim(t *testing.T) {
	ctx := context.Background()
	controller := openController(t, "github-zero-capacity-replay.db")
	defer controller.Close()
	nodeID, _ := enrollControllerAgentNode(t, controller, 5)
	binding := SingleSlotBinding{
		TargetID:     "target-zero-replay",
		NodeID:       domain.NodeID(nodeID),
		Slot:         0,
		ClaimEnabled: true,
	}
	const scaleSetID ScaleSetID = 73
	enableGitHubClaimForTest(t, controller, &binding, scaleSetID, domain.ArchAMD64)
	message := githubQueueMessageForTest(scaleSetID, 730, 7300)
	first, err := controller.CommitGitHubQueueMessage(ctx, message, binding)
	if err != nil || first.Claim == nil || first.UnclaimedAvailable {
		t.Fatalf("initial claim = (%#v, %v)", first, err)
	}
	if _, err := controller.RecordGitHubScaleSetSessionFailure(
		ctx,
		scaleSetID,
		GitHubObservationProvider5xx,
	); err != nil {
		t.Fatal(err)
	}

	zeroCapacity := binding
	zeroCapacity.ClaimEnabled = false
	zeroCapacity.PollAuthority = GitHubPollClaimAuthority{}
	replayed, err := controller.CommitGitHubQueueMessage(ctx, message, zeroCapacity)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.Claim == nil ||
		replayed.Claim.ClaimKey != first.Claim.ClaimKey ||
		replayed.UnclaimedAvailable {
		t.Fatalf("zero-capacity replay = %#v", replayed)
	}
	assertCount(t, controller.db, "SELECT count(*) FROM github_job_claims", 1)
	assertCount(t, controller.db, "SELECT count(*) FROM slot_reservations", 1)
}
