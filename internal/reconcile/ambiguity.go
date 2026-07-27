package reconcile

import (
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

type GitHubJobObservationState string

const (
	GitHubJobUnknown   GitHubJobObservationState = "unknown"
	GitHubJobAvailable GitHubJobObservationState = "available"
	GitHubJobAssigned  GitHubJobObservationState = "assigned"
	GitHubJobRunning   GitHubJobObservationState = "running"
	GitHubJobCompleted GitHubJobObservationState = "completed"
)

type GitHubRunnerIdentity struct {
	ScaleSetID store.ScaleSetID
	ID         int
	Name       string
}

// GitHubReconciliationObservation is a fresh provider read, never a message
// inferred from an earlier process. RunnerObserved distinguishes proven absence
// from an API response that did not query runner registration.
type GitHubReconciliationObservation struct {
	ObservedAt      time.Time
	Stale           bool
	ScaleSetID      store.ScaleSetID
	RunnerRequestID int64
	JobObserved     bool
	JobState        GitHubJobObservationState
	RunnerObserved  bool
	RunnerName      string
	Runner          *GitHubRunnerIdentity
}

type AmbiguityResolutionKind string

const (
	AmbiguityHold                  AmbiguityResolutionKind = "hold"
	AmbiguityConfirmAgentAccepted  AmbiguityResolutionKind = "confirm_agent_accepted"
	AmbiguityAwaitAgentObservation AmbiguityResolutionKind = "await_agent_observation"
	AmbiguityRemoveRunner          AmbiguityResolutionKind = "remove_runner"
	AmbiguityMarkRunnerAbsent      AmbiguityResolutionKind = "mark_runner_absent"
)

// AmbiguityResolution never grants capacity itself. The owning store must
// durably apply the result, and a later Controller projection must no longer
// contain the GitHubFence before any retry or reservation advertisement.
type AmbiguityResolution struct {
	Kind               AmbiguityResolutionKind
	ExecutionID        domain.ExecutionID
	StartCommandID     domain.CommandID
	Runner             *GitHubRunnerIdentity
	PersistBeforeRetry bool
}

// ResolveGitHubFence combines fresh Agent journal authority with an explicit
// provider observation. It does not perform removal, retry, or a store update.
func ResolveGitHubFence(
	fence GitHubFence,
	issuedStart *IssuedCommand,
	agent transport.AgentSnapshot,
	observation GitHubReconciliationObservation,
) (AmbiguityResolution, error) {
	if err := validateGitHubFence(fence); err != nil {
		return AmbiguityResolution{}, err
	}
	if err := agent.Validate(); err != nil {
		return AmbiguityResolution{}, invalid("invalid_agent_snapshot", "agent_snapshot", "failed typed validation")
	}
	result := AmbiguityResolution{
		Kind:               AmbiguityHold,
		ExecutionID:        fence.ExecutionID,
		PersistBeforeRetry: true,
	}
	if fence.Attempt != nil {
		result.StartCommandID = fence.Attempt.StartCommandID
		if fence.Attempt.StartCommandID != "" {
			accepted, err := exactAgentStartAccepted(
				fence, issuedStart, agent.NodeID, agent.Commands)
			if err != nil {
				return AmbiguityResolution{}, err
			}
			switch fence.Attempt.State {
			case store.GitHubJITAgentAccepted, store.GitHubJITStarted:
				result.Kind = AmbiguityAwaitAgentObservation
				return result, nil
			case store.GitHubJITRemovalPending:
				if accepted {
					return AmbiguityResolution{}, invalid("agent_start_after_removal", "agent_snapshot.commands", "contradicts the durable runner-removal fence")
				}
			}
			if accepted {
				result.Kind = AmbiguityConfirmAgentAccepted
				return result, nil
			}
		} else if issuedStart != nil {
			return AmbiguityResolution{}, invalid("unexpected_start_authority", "issued_start", "fence has no start command")
		}
	} else if issuedStart != nil {
		return AmbiguityResolution{}, invalid("unexpected_start_authority", "issued_start", "acquire ambiguity has no start command")
	}
	if observation.Stale || observation.ObservedAt.IsZero() {
		return result, nil
	}
	if fence.ClaimState == store.GitHubClaimAcquireAmbiguous {
		// Acquire recovery is owned by the durable queue-message transaction.
		// A newly committed JobAvailable message can re-arm one exact attempt;
		// inferred job state or a replay cannot adopt or retry an external write.
		return result, nil
	}
	if !observation.RunnerObserved {
		return result, nil
	}
	if fence.Attempt == nil {
		return AmbiguityResolution{}, invalid("github_attempt_required", "github_fence.attempt", "runner observation requires a durable JIT attempt")
	}
	if observation.ScaleSetID != fence.ScaleSetID ||
		observation.RunnerRequestID != fence.RunnerRequestID ||
		observation.RunnerName != fence.Attempt.RunnerName {
		return AmbiguityResolution{}, invalid("github_runner_query_identity_mismatch", "github_observation", "was not produced by the exact durable runner query")
	}
	if observation.Runner == nil {
		result.Kind = AmbiguityMarkRunnerAbsent
		return result, nil
	}
	runner := *observation.Runner
	if runner.ScaleSetID == 0 || runner.ID <= 0 || runner.Name == "" {
		return AmbiguityResolution{}, invalid("invalid_github_runner_observation", "github_observation.runner", "contains incomplete runner identity")
	}
	if runner.ScaleSetID != fence.Attempt.ScaleSetID ||
		runner.Name != fence.Attempt.RunnerName ||
		(fence.Attempt.RunnerID > 0 && runner.ID != fence.Attempt.RunnerID) {
		return AmbiguityResolution{}, invalid("github_runner_identity_mismatch", "github_observation.runner", "does not match durable JIT identity")
	}
	result.Kind = AmbiguityRemoveRunner
	result.Runner = &runner
	return result, nil
}

func exactAgentStartAccepted(
	fence GitHubFence,
	issued *IssuedCommand,
	nodeID domain.NodeID,
	commands []domain.Command,
) (bool, error) {
	if issued == nil {
		for _, command := range commands {
			if command.ID == fence.Attempt.StartCommandID {
				return false, invalid("start_command_authority_required", "issued_start", "Agent reported a start command without exact Controller authority")
			}
		}
		return false, nil
	}
	if issued.NodeID != nodeID || issued.Type != domain.CommandStart ||
		issued.Command.ID != fence.Attempt.StartCommandID ||
		issued.Command.ExecutionID != fence.ExecutionID ||
		issued.Command.ControllerEpoch != fence.Attempt.ControllerEpoch ||
		issued.Command.ExpectedState != domain.ExecutionPreparing {
		return false, invalid("start_command_authority_mismatch", "issued_start", "does not match the fence and Agent node")
	}
	if err := issued.Command.Validate(); err != nil {
		return false, err
	}
	for _, command := range commands {
		if command.ID != issued.Command.ID {
			continue
		}
		if command != issued.Command {
			return false, invalid("agent_command_authority_mismatch", "agent_snapshot.commands", "start command differs from Controller authority")
		}
		return true, nil
	}
	return false, nil
}
