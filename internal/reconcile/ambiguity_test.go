package reconcile

import (
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

func TestResolveGitHubStartAmbiguityUsesExactAgentCommandBeforeProviderRemoval(t *testing.T) {
	command := domain.Command{
		ID:              "start-a",
		ControllerEpoch: 1,
		ExecutionID:     "execution-a",
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start")),
	}
	fence := startFenceForTest(command)
	issued := &IssuedCommand{NodeID: "node-a", Type: domain.CommandStart, Command: command}
	agent := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{command},
	}
	resolution, err := ResolveGitHubFence(fence, issued, agent, GitHubReconciliationObservation{
		ObservedAt:     time.Unix(100, 0),
		RunnerObserved: true,
		Runner: &GitHubRunnerIdentity{
			ScaleSetID: 7,
			ID:         9,
			Name:       "tewake-a",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Kind != AmbiguityConfirmAgentAccepted ||
		resolution.StartCommandID != command.ID ||
		!resolution.PersistBeforeRetry {
		t.Fatalf("resolution = %#v", resolution)
	}
}

func TestResolveGitHubJITRequiresRemoveThenFreshAbsence(t *testing.T) {
	command := domain.Command{
		ID:              "start-a",
		ControllerEpoch: 1,
		ExecutionID:     "execution-a",
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start")),
	}
	fence := startFenceForTest(command)
	issued := &IssuedCommand{NodeID: "node-a", Type: domain.CommandStart, Command: command}
	agent := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		MaxControllerEpoch: 1,
	}
	present, err := ResolveGitHubFence(fence, issued, agent, GitHubReconciliationObservation{
		ObservedAt:      time.Unix(100, 0),
		ScaleSetID:      7,
		RunnerRequestID: 8,
		RunnerObserved:  true,
		RunnerName:      "tewake-a",
		Runner: &GitHubRunnerIdentity{
			ScaleSetID: 7,
			ID:         9,
			Name:       "tewake-a",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if present.Kind != AmbiguityRemoveRunner || present.Runner == nil {
		t.Fatalf("present resolution = %#v", present)
	}
	absent, err := ResolveGitHubFence(fence, issued, agent, GitHubReconciliationObservation{
		ObservedAt:      time.Unix(101, 0),
		ScaleSetID:      7,
		RunnerRequestID: 8,
		RunnerObserved:  true,
		RunnerName:      "tewake-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if absent.Kind != AmbiguityMarkRunnerAbsent || !absent.PersistBeforeRetry {
		t.Fatalf("absent resolution = %#v", absent)
	}
}

func TestResolveGitHubFenceHoldsOnStaleOrUnobservedState(t *testing.T) {
	fence := GitHubFence{
		ExecutionID:     "execution-a",
		ScaleSetID:      7,
		RunnerRequestID: 8,
		ClaimState:      store.GitHubClaimAcquireAmbiguous,
	}
	agent := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		MaxControllerEpoch: 1,
	}
	for _, observation := range []GitHubReconciliationObservation{
		{
			ObservedAt:      time.Unix(1, 0),
			Stale:           true,
			ScaleSetID:      7,
			RunnerRequestID: 8,
			JobObserved:     true,
			JobState:        GitHubJobAvailable,
		},
		{},
	} {
		resolution, err := ResolveGitHubFence(fence, nil, agent, observation)
		if err != nil {
			t.Fatal(err)
		}
		if resolution.Kind != AmbiguityHold {
			t.Fatalf("unsafe observation resolved ambiguity: %#v", resolution)
		}
	}
	for _, state := range []GitHubJobObservationState{
		GitHubJobAvailable,
		GitHubJobAssigned,
		GitHubJobRunning,
		GitHubJobCompleted,
	} {
		fresh, err := ResolveGitHubFence(fence, nil, agent, GitHubReconciliationObservation{
			ObservedAt:      time.Unix(2, 0),
			ScaleSetID:      7,
			RunnerRequestID: 8,
			JobObserved:     true,
			JobState:        state,
		})
		if err != nil {
			t.Fatal(err)
		}
		if fresh.Kind != AmbiguityHold {
			t.Fatalf("inferred acquisition observation resolved ambiguity: %#v", fresh)
		}
	}
}

func TestResolveGitHubFenceRejectsDifferentRunnerIdentity(t *testing.T) {
	command := domain.Command{
		ID:              "start-a",
		ControllerEpoch: 1,
		ExecutionID:     "execution-a",
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start")),
	}
	fence := startFenceForTest(command)
	issued := &IssuedCommand{NodeID: "node-a", Type: domain.CommandStart, Command: command}
	agent := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		MaxControllerEpoch: 1,
	}
	if _, err := ResolveGitHubFence(fence, issued, agent, GitHubReconciliationObservation{
		ObservedAt:      time.Unix(100, 0),
		ScaleSetID:      7,
		RunnerRequestID: 8,
		RunnerObserved:  true,
		RunnerName:      "tewake-a",
		Runner: &GitHubRunnerIdentity{
			ScaleSetID: 7,
			ID:         10,
			Name:       "tewake-a",
		},
	}); !hasCode(err, "github_runner_identity_mismatch") {
		t.Fatalf("different runner identity = %v", err)
	}
}

func TestResolveGitHubFenceRejectsReportedStartWithoutExactControllerAuthority(t *testing.T) {
	command := domain.Command{
		ID:              "start-a",
		ControllerEpoch: 1,
		ExecutionID:     "execution-a",
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start")),
	}
	fence := startFenceForTest(command)
	agent := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{command},
	}
	if _, err := ResolveGitHubFence(
		fence,
		nil,
		agent,
		GitHubReconciliationObservation{},
	); !hasCode(err, "start_command_authority_required") {
		t.Fatalf("unowned start accepted = %v", err)
	}
	changed := command
	changed.PayloadDigest = domain.PayloadDigest([]byte("different"))
	if _, err := ResolveGitHubFence(
		fence,
		&IssuedCommand{NodeID: "node-a", Type: domain.CommandStart, Command: changed},
		agent,
		GitHubReconciliationObservation{},
	); !hasCode(err, "agent_command_authority_mismatch") {
		t.Fatalf("changed start accepted = %v", err)
	}
}

func startFenceForTest(command domain.Command) GitHubFence {
	return GitHubFence{
		ExecutionID:     command.ExecutionID,
		ScaleSetID:      7,
		RunnerRequestID: 8,
		ClaimState:      store.GitHubClaimStartAmbiguous,
		Attempt: &store.GitHubJITAttempt{
			ScaleSetID:      7,
			RunnerRequestID: 8,
			Attempt:         1,
			ControllerEpoch: command.ControllerEpoch,
			RunnerName:      "tewake-a",
			State:           store.GitHubJITStartAmbiguous,
			RunnerID:        9,
			JITDigest:       domain.PayloadDigest([]byte("jit")),
			StartCommandID:  command.ID,
		},
	}
}

func TestResolveGitHubFenceRejectsUnboundAbsenceAndWrongClaimObservation(t *testing.T) {
	command := domain.Command{
		ID:              "start-a",
		ControllerEpoch: 1,
		ExecutionID:     "execution-a",
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start")),
	}
	fence := startFenceForTest(command)
	agent := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		MaxControllerEpoch: 1,
	}
	issued := &IssuedCommand{NodeID: "node-a", Type: domain.CommandStart, Command: command}
	if _, err := ResolveGitHubFence(fence, issued, agent, GitHubReconciliationObservation{
		ObservedAt:      time.Unix(100, 0),
		ScaleSetID:      7,
		RunnerRequestID: 8,
		RunnerObserved:  true,
		RunnerName:      "another-runner",
	}); !hasCode(err, "github_runner_query_identity_mismatch") {
		t.Fatalf("unbound runner absence = %v", err)
	}

	acquire := GitHubFence{
		ExecutionID:     "execution-a",
		ScaleSetID:      7,
		RunnerRequestID: 8,
		ClaimState:      store.GitHubClaimAcquireAmbiguous,
	}
	resolution, err := ResolveGitHubFence(acquire, nil, agent, GitHubReconciliationObservation{
		ObservedAt:      time.Unix(100, 0),
		ScaleSetID:      9,
		RunnerRequestID: 8,
		JobObserved:     true,
		JobState:        GitHubJobAvailable,
	})
	if err != nil || resolution.Kind != AmbiguityHold {
		t.Fatalf("inferred claim observation changed durable acquisition = (%#v, %v)", resolution, err)
	}
}
