package app

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

var (
	ErrControllerRunnerConfig       = errors.New("controller runner coordinator configuration is invalid")
	ErrGitHubAcquireAmbiguous       = errors.New("GitHub job acquisition is ambiguous")
	ErrGitHubAvailableUnclaimed     = errors.New("GitHub available job has no eligible durable slot claim")
	ErrGitHubPrepareFailed          = errors.New("Agent runner preparation failed")
	ErrGitHubStartAmbiguous         = errors.New("Agent runner start is ambiguous")
	ErrGitHubReconciliationRequired = errors.New("GitHub runner reconciliation is required")
)

const controllerRunnerWorkFolder = "_work"

type controllerRunnerStore interface {
	RecordGitHubSessionDemand(context.Context, store.GitHubSessionDemand) error
	CommitGitHubQueueMessage(context.Context, store.GitHubQueueMessage, store.SingleSlotBinding) (store.GitHubMessageCommit, error)
	GitHubSingleSlotCapacity(context.Context, store.SingleSlotBinding) (int, error)
	NextActionableGitHubClaim(context.Context, store.ScaleSetID) (store.GitHubJobClaim, bool, error)
	GitHubClaim(context.Context, store.ScaleSetID, int64) (store.GitHubJobClaim, bool, error)
	BeginGitHubAcquire(context.Context, store.ScaleSetID, int64) error
	MarkGitHubAcquired(context.Context, store.ScaleSetID, int64) error
	MarkGitHubPreparing(context.Context, store.ScaleSetID, int64) error
	MarkGitHubPrepareFailed(context.Context, store.ScaleSetID, int64) error
	BeginGitHubJITAttempt(context.Context, store.ScaleSetID, int64, domain.ControllerEpoch, string) (store.GitHubJITAttempt, bool, error)
	MarkGitHubJITGenerationAmbiguous(context.Context, store.GitHubJITAttempt) error
	MarkGitHubJITGenerated(context.Context, store.GitHubJITAttempt, int, string, domain.CommandID) error
	BeginGitHubStartDispatch(context.Context, store.GitHubJITAttempt) error
	MarkGitHubStartAmbiguous(context.Context, store.GitHubJITAttempt) error
	MarkGitHubRunning(context.Context, store.GitHubJITAttempt) error
	CurrentGitHubJITAttempt(context.Context, store.ScaleSetID, int64) (store.GitHubJITAttempt, bool, error)
	MarkGitHubJITAgentAccepted(context.Context, store.GitHubJITAttempt, domain.ControllerEpoch) error
	MarkGitHubJITRemovalPending(context.Context, store.GitHubJITAttempt, domain.ControllerEpoch) error
	MarkGitHubJITReconciledAbsent(context.Context, store.GitHubJITAttempt, domain.ControllerEpoch) error
}

type controllerRunnerMessageSession interface {
	github.MessageSource
	AcquireJobs(context.Context, []int64) ([]int64, error)
}

type controllerRunnerAgent interface {
	SendPrepare(context.Context, domain.NodeID, transport.CommandMetadata, bool) (transport.ExecutionUpdate, error)
	SendStart(context.Context, domain.NodeID, transport.CommandMetadata, bool, runner.JITConfig) (transport.ExecutionUpdate, error)
	Readiness(domain.NodeID) (AgentSnapshot, bool, context.Context)
}

// ControllerRunnerLifecycle makes JIT generation testable without exposing the
// opaque body. The production adapter is NewGitHubClientRunnerLifecycle.
type ControllerRunnerLifecycle interface {
	GenerateJITConfig(context.Context, github.JITRequest) (runner.JITConfig, github.RunnerReference, error)
	GetRunnerByName(context.Context, string) (*github.RunnerReference, error)
	RemoveRunner(context.Context, github.RunnerReference) error
}

type githubClientRunnerLifecycle struct {
	client *github.Client
}

func NewGitHubClientRunnerLifecycle(client *github.Client) (ControllerRunnerLifecycle, error) {
	if client == nil {
		return nil, ErrControllerRunnerConfig
	}
	return githubClientRunnerLifecycle{client: client}, nil
}

func (lifecycle githubClientRunnerLifecycle) GenerateJITConfig(
	ctx context.Context,
	request github.JITRequest,
) (runner.JITConfig, github.RunnerReference, error) {
	config, err := lifecycle.client.GenerateJITConfig(ctx, request)
	if err != nil {
		return nil, github.RunnerReference{}, err
	}
	return config, config.Runner(), nil
}

func (lifecycle githubClientRunnerLifecycle) GetRunnerByName(
	ctx context.Context,
	name string,
) (*github.RunnerReference, error) {
	return lifecycle.client.GetRunnerByName(ctx, name)
}

func (lifecycle githubClientRunnerLifecycle) RemoveRunner(
	ctx context.Context,
	reference github.RunnerReference,
) error {
	return lifecycle.client.RemoveRunner(ctx, reference)
}

type ControllerRunnerConfig struct {
	ScaleSetID      github.ScaleSetID
	TargetID        domain.TargetID
	NodeID          domain.NodeID
	ControllerEpoch domain.ControllerEpoch
	DisableUpdate   bool
}

type ControllerRunnerCoordinator struct {
	store     controllerRunnerStore
	session   controllerRunnerMessageSession
	agents    controllerRunnerAgent
	lifecycle ControllerRunnerLifecycle
	config    ControllerRunnerConfig
	binding   store.SingleSlotBinding
	poller    *github.Poller
}

// NewControllerRunnerCoordinator creates the production-callable TWK-007
// single-slot vertical. Task twk-010 replaces the fixed binding with the full
// node-affined scheduler without changing the durable GitHub claim boundary.
func NewControllerRunnerCoordinator(
	stateStore controllerRunnerStore,
	session controllerRunnerMessageSession,
	agents controllerRunnerAgent,
	lifecycle ControllerRunnerLifecycle,
	config ControllerRunnerConfig,
	logger *slog.Logger,
) (*ControllerRunnerCoordinator, error) {
	if stateStore == nil || session == nil || agents == nil || lifecycle == nil ||
		config.ScaleSetID <= 0 || config.TargetID == "" || config.NodeID == "" ||
		config.ControllerEpoch.Validate() != nil {
		return nil, ErrControllerRunnerConfig
	}
	coordinator := &ControllerRunnerCoordinator{
		store:     stateStore,
		session:   session,
		agents:    agents,
		lifecycle: lifecycle,
		config:    config,
		binding: store.SingleSlotBinding{
			TargetID: config.TargetID,
			NodeID:   config.NodeID,
			Slot:     0,
		},
	}
	poller, err := github.NewPoller(session, coordinator, logger)
	if err != nil {
		return nil, err
	}
	coordinator.poller = poller
	return coordinator, nil
}

var _ github.DurableMessageHandler = (*ControllerRunnerCoordinator)(nil)

func (coordinator *ControllerRunnerCoordinator) CommitSessionDemand(
	ctx context.Context,
	snapshot github.SessionSnapshot,
) error {
	if snapshot.ScaleSetID != coordinator.config.ScaleSetID {
		return github.ErrInvalidSession
	}
	statistics := snapshot.Statistics
	return coordinator.store.RecordGitHubSessionDemand(ctx, store.GitHubSessionDemand{
		ScaleSetID:             store.ScaleSetID(snapshot.ScaleSetID),
		SessionID:              snapshot.ID,
		TotalAvailableJobs:     statistics.TotalAvailableJobs,
		TotalAcquiredJobs:      statistics.TotalAcquiredJobs,
		TotalAssignedJobs:      statistics.TotalAssignedJobs,
		TotalRunningJobs:       statistics.TotalRunningJobs,
		TotalRegisteredRunners: statistics.TotalRegisteredRunners,
		TotalBusyRunners:       statistics.TotalBusyRunners,
		TotalIdleRunners:       statistics.TotalIdleRunners,
	})
}

func (coordinator *ControllerRunnerCoordinator) CommitMessage(
	ctx context.Context,
	message github.Message,
) error {
	if message.ScaleSetID != coordinator.config.ScaleSetID || message.ID <= 0 {
		return github.ErrInvalidSession
	}
	digest, err := controllerGitHubMessageDigest(message)
	if err != nil {
		return errors.New("encode GitHub queue message identity")
	}
	record := store.GitHubQueueMessage{
		ScaleSetID: store.ScaleSetID(message.ScaleSetID),
		MessageID:  store.MessageID(message.ID),
		Digest:     digest,
		Jobs:       make([]store.GitHubJobEvent, 0, len(message.Jobs)),
	}
	for _, job := range message.Jobs {
		eventType, err := controllerStoreGitHubEventType(job.Type)
		if err != nil {
			return err
		}
		event := store.GitHubJobEvent{
			Type:            eventType,
			RunnerRequestID: job.RunnerRequestID,
			RunnerID:        job.RunnerID,
			RunnerName:      job.RunnerName,
			Result:          job.Result,
			RepositoryName:  job.RepositoryName,
			OwnerName:       job.OwnerName,
			JobID:           job.JobID,
			WorkflowRunID:   job.WorkflowRunID,
		}
		if job.Type == github.MessageTypeJobAvailable {
			event.ExecutionID = deterministicExecutionID(message.ScaleSetID, job.RunnerRequestID)
		}
		record.Jobs = append(record.Jobs, event)
	}
	binding := coordinator.binding
	if snapshot, online, _ := coordinator.agents.Readiness(coordinator.config.NodeID); online {
		binding.ClaimEnabled = snapshot.NativeRunnerReady
	}
	commit, err := coordinator.store.CommitGitHubQueueMessage(ctx, record, binding)
	if err == nil && commit.UnclaimedAvailable {
		return ErrGitHubAvailableUnclaimed
	}
	return err
}

// controllerGitHubMessageDigest is deliberately independent of the public
// github.Message JSON shape. Statistics are volatile demand observations, not
// queue-message identity, and future adapter fields must be reviewed here
// explicitly before they can affect exact replay.
func controllerGitHubMessageDigest(message github.Message) (string, error) {
	type jobIdentity struct {
		Type            github.MessageType `json:"type"`
		RunnerRequestID int64              `json:"runnerRequestId"`
		RunnerID        int                `json:"runnerId"`
		RunnerName      string             `json:"runnerName"`
		Result          string             `json:"result"`
		RepositoryName  string             `json:"repositoryName"`
		OwnerName       string             `json:"ownerName"`
		JobID           string             `json:"jobId"`
		WorkflowRunID   int64              `json:"workflowRunId"`
	}
	type messageIdentity struct {
		ScaleSetID github.ScaleSetID `json:"scaleSetId"`
		MessageID  int               `json:"messageId"`
		Jobs       []jobIdentity     `json:"jobs"`
	}
	identity := messageIdentity{
		ScaleSetID: message.ScaleSetID,
		MessageID:  message.ID,
		Jobs:       make([]jobIdentity, len(message.Jobs)),
	}
	for index, job := range message.Jobs {
		identity.Jobs[index] = jobIdentity{
			Type:            job.Type,
			RunnerRequestID: job.RunnerRequestID,
			RunnerID:        job.RunnerID,
			RunnerName:      job.RunnerName,
			Result:          job.Result,
			RepositoryName:  job.RepositoryName,
			OwnerName:       job.OwnerName,
			JobID:           job.JobID,
			WorkflowRunID:   job.WorkflowRunID,
		}
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	digest := domain.PayloadDigest(encoded)
	clear(encoded)
	return digest, nil
}

// PollOnce commits one GitHub message before acknowledgement. Capacity is read
// from the same durable single slot used by CommitMessage and is zero whenever
// its node is draining, quarantined, revoked, missing, or already reserved.
func (coordinator *ControllerRunnerCoordinator) PollOnce(ctx context.Context) (*github.Message, error) {
	message, _, err := coordinator.pollOnce(ctx)
	return message, err
}

func (coordinator *ControllerRunnerCoordinator) pollOnce(
	ctx context.Context,
) (*github.Message, bool, error) {
	capacity, err := coordinator.store.GitHubSingleSlotCapacity(ctx, coordinator.binding)
	if err != nil {
		return nil, false, err
	}
	snapshot, online, readinessChanged := coordinator.agents.Readiness(coordinator.config.NodeID)
	if capacity > 0 && (!online || !snapshot.NativeRunnerReady) {
		capacity = 0
	}
	if readinessChanged == nil {
		message, err := coordinator.poller.PollOnce(ctx, capacity)
		return message, false, err
	}

	pollContext, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(readinessChanged, cancel)
	defer func() {
		stop()
		cancel()
	}()
	message, err := coordinator.poller.PollOnce(pollContext, capacity)
	if err != nil && ctx.Err() == nil && readinessChanged.Err() != nil &&
		errors.Is(err, context.Canceled) {
		// A readiness transition intentionally interrupts the upstream long
		// poll. Re-evaluate capacity without advancing or driving a claim.
		return nil, true, nil
	}
	if err == nil && ctx.Err() == nil && readinessChanged.Err() != nil {
		return message, true, nil
	}
	return message, false, err
}

func (coordinator *ControllerRunnerCoordinator) PollAndDriveOnce(ctx context.Context) (*github.Message, error) {
	message, readinessChanged, err := coordinator.pollOnce(ctx)
	if err != nil {
		return nil, err
	}
	if readinessChanged {
		return nil, nil
	}
	_, err = coordinator.DriveNext(ctx)
	return message, err
}

// Run owns the poll-first single-target acceptance loop. The first operation
// remains a poll so a restart cannot drive a commit-before-ack message ahead of
// its upstream redelivery. Once that poll has completed or was intentionally
// interrupted by a readiness change, an existing durable claim may be retried
// directly without waiting through another GitHub long poll.
func (coordinator *ControllerRunnerCoordinator) Run(ctx context.Context) error {
	if coordinator == nil {
		return ErrControllerRunnerConfig
	}
	driveBeforePoll := false
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		if driveBeforePoll {
			drove, err := coordinator.DriveNext(ctx)
			switch {
			case err == nil:
				driveBeforePoll = false
				if drove {
					continue
				}
			case errors.Is(err, ErrAgentOffline):
				if err := coordinator.waitForRunnerReadiness(ctx); err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return err
				}
				continue
			default:
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}

		_, readinessChanged, err := coordinator.pollOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if readinessChanged {
			// The transition-owning poll iteration never drives. The next
			// iteration may retry an already durable claim immediately.
			driveBeforePoll = true
			continue
		}
		_, err = coordinator.DriveNext(ctx)
		if errors.Is(err, ErrAgentOffline) {
			if err := coordinator.waitForRunnerReadiness(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			driveBeforePoll = true
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (coordinator *ControllerRunnerCoordinator) waitForRunnerReadiness(ctx context.Context) error {
	snapshot, online, changed := coordinator.agents.Readiness(coordinator.config.NodeID)
	if online && snapshot.NativeRunnerReady {
		return nil
	}
	if changed == nil {
		return ErrAgentOffline
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed.Done():
		return nil
	}
}

// DriveNext advances at most one durable single-slot claim. A pre-existing
// Pending claim is sufficient after restart; no in-memory PollOnce result is
// required.
func (coordinator *ControllerRunnerCoordinator) DriveNext(ctx context.Context) (bool, error) {
	claim, found, err := coordinator.store.NextActionableGitHubClaim(
		ctx, store.ScaleSetID(coordinator.config.ScaleSetID))
	if err != nil || !found {
		return false, err
	}
	switch claim.State {
	case store.GitHubClaimPending:
		if err := coordinator.acquire(ctx, claim); err != nil {
			return true, err
		}
		claim.State = store.GitHubClaimAcquired
		fallthrough
	case store.GitHubClaimAcquired:
		prepared, err := coordinator.prepare(ctx, claim)
		if err != nil || !prepared {
			return true, err
		}
		claim.State = store.GitHubClaimPreparing
		fallthrough
	case store.GitHubClaimPreparing:
		return true, coordinator.generateAndStart(ctx, claim)
	default:
		return true, store.ErrGitHubClaimState
	}
}

func (coordinator *ControllerRunnerCoordinator) acquire(ctx context.Context, claim store.GitHubJobClaim) error {
	if err := coordinator.requireRunnerReadiness(); err != nil {
		return err
	}
	if err := coordinator.store.BeginGitHubAcquire(ctx, claim.ScaleSetID, claim.RunnerRequestID); err != nil {
		return err
	}
	acquired, err := coordinator.session.AcquireJobs(ctx, []int64{claim.RunnerRequestID})
	if err != nil || len(acquired) != 1 || acquired[0] != claim.RunnerRequestID {
		// BeginGitHubAcquire records the ambiguous state before the network call,
		// so a crash, transport error, or empty response all remain non-runnable.
		return ErrGitHubAcquireAmbiguous
	}
	return coordinator.store.MarkGitHubAcquired(ctx, claim.ScaleSetID, claim.RunnerRequestID)
}

func (coordinator *ControllerRunnerCoordinator) requireRunnerReadiness() error {
	snapshot, online, _ := coordinator.agents.Readiness(coordinator.config.NodeID)
	if !online || !snapshot.NativeRunnerReady {
		return ErrAgentOffline
	}
	return nil
}

func (coordinator *ControllerRunnerCoordinator) prepare(ctx context.Context, claim store.GitHubJobClaim) (bool, error) {
	if err := coordinator.requireRunnerReadiness(); err != nil {
		return false, err
	}
	metadata := transport.CommandMetadata{
		CommandID:       deterministicCommandID("prepare", claim.Execution.ID, ""),
		ControllerEpoch: coordinator.config.ControllerEpoch,
		ExecutionID:     claim.Execution.ID,
		ExpectedState:   domain.ExecutionReserved,
	}
	update, err := coordinator.agents.SendPrepare(
		ctx, coordinator.config.NodeID, metadata, coordinator.config.DisableUpdate)
	if err != nil {
		// Prepare is non-secret and exact-idempotent. Keep the claim Acquired so
		// reconnect can resend the same deterministic command.
		return false, err
	}
	if err := validateControllerRunnerUpdate(update, coordinator.config.NodeID, metadata); err != nil {
		return false, err
	}
	switch update.State {
	case domain.ExecutionPreparing:
		if err := coordinator.store.MarkGitHubPreparing(ctx, claim.ScaleSetID, claim.RunnerRequestID); err != nil {
			return false, err
		}
		return true, nil
	case domain.ExecutionFailed:
		if err := coordinator.store.MarkGitHubPrepareFailed(ctx, claim.ScaleSetID, claim.RunnerRequestID); err != nil {
			return false, err
		}
		return false, ErrGitHubPrepareFailed
	default:
		return false, ErrGitHubPrepareFailed
	}
}

func (coordinator *ControllerRunnerCoordinator) generateAndStart(ctx context.Context, claim store.GitHubJobClaim) error {
	if err := coordinator.requireRunnerReadiness(); err != nil {
		return err
	}
	runnerName := deterministicRunnerName(claim.ScaleSetID, claim.RunnerRequestID)
	attempt, replayed, err := coordinator.store.BeginGitHubJITAttempt(
		ctx, claim.ScaleSetID, claim.RunnerRequestID,
		coordinator.config.ControllerEpoch, runnerName)
	if err != nil {
		return err
	}
	if replayed {
		// The opaque body is not durable. Any surviving attempt row means a
		// previous generation may have crossed GitHub and must be reconciled.
		return ErrGitHubReconciliationRequired
	}
	jitConfig, reference, err := coordinator.lifecycle.GenerateJITConfig(ctx, github.JITRequest{
		ScaleSetID: coordinator.config.ScaleSetID,
		Name:       runnerName,
		WorkFolder: controllerRunnerWorkFolder,
	})
	if err != nil {
		if markErr := coordinator.store.MarkGitHubJITGenerationAmbiguous(ctx, attempt); markErr != nil {
			return errors.Join(ErrGitHubReconciliationRequired, markErr)
		}
		return ErrGitHubReconciliationRequired
	}
	if jitConfig == nil {
		if markErr := coordinator.store.MarkGitHubJITGenerationAmbiguous(ctx, attempt); markErr != nil {
			return errors.Join(ErrGitHubReconciliationRequired, markErr)
		}
		return ErrGitHubReconciliationRequired
	}
	jitDigest := jitConfig.Digest()
	if reference.ID <= 0 || reference.Name != runnerName ||
		reference.ScaleSetID != coordinator.config.ScaleSetID ||
		!controllerLowerSHA256(jitDigest) {
		if markErr := coordinator.store.MarkGitHubJITGenerationAmbiguous(ctx, attempt); markErr != nil {
			return errors.Join(ErrGitHubReconciliationRequired, markErr)
		}
		return ErrGitHubReconciliationRequired
	}
	startCommandID := deterministicCommandID("start", claim.Execution.ID, jitDigest)
	if err := coordinator.store.MarkGitHubJITGenerated(
		ctx, attempt, reference.ID, jitDigest, startCommandID); err != nil {
		return errors.Join(ErrGitHubReconciliationRequired, err)
	}
	attempt.RunnerID = reference.ID
	attempt.JITDigest = jitDigest
	attempt.StartCommandID = startCommandID
	attempt.State = store.GitHubJITGenerated
	if err := coordinator.store.BeginGitHubStartDispatch(ctx, attempt); err != nil {
		return errors.Join(ErrGitHubReconciliationRequired, err)
	}
	attempt.State = store.GitHubJITStartDispatching
	metadata := transport.CommandMetadata{
		CommandID:       startCommandID,
		ControllerEpoch: coordinator.config.ControllerEpoch,
		ExecutionID:     claim.Execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
	}
	update, err := coordinator.agents.SendStart(
		ctx, coordinator.config.NodeID, metadata, coordinator.config.DisableUpdate, jitConfig)
	if err != nil {
		if markErr := coordinator.store.MarkGitHubStartAmbiguous(ctx, attempt); markErr != nil {
			return errors.Join(ErrGitHubStartAmbiguous, markErr)
		}
		return ErrGitHubStartAmbiguous
	}
	if err := validateControllerRunnerUpdate(update, coordinator.config.NodeID, metadata); err != nil ||
		update.State != domain.ExecutionRunning {
		if markErr := coordinator.store.MarkGitHubStartAmbiguous(ctx, attempt); markErr != nil {
			return errors.Join(ErrGitHubStartAmbiguous, markErr)
		}
		return ErrGitHubStartAmbiguous
	}
	if err := coordinator.store.MarkGitHubRunning(ctx, attempt); err != nil {
		return errors.Join(ErrGitHubReconciliationRequired, err)
	}
	return nil
}

// ReconcileJITAttempt is the only path that permits a later JIT attempt. It
// requires a freshly activated Agent snapshot, then either proves the exact
// start command was accepted or removes and re-reads the deterministic GitHub
// runner registration before marking it absent.
func (coordinator *ControllerRunnerCoordinator) ReconcileJITAttempt(
	ctx context.Context,
	runnerRequestID int64,
) error {
	attempt, found, err := coordinator.store.CurrentGitHubJITAttempt(
		ctx, store.ScaleSetID(coordinator.config.ScaleSetID), runnerRequestID)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubReconciliationRequired
		}
		return err
	}
	if attempt.State == store.GitHubJITStarted || attempt.State == store.GitHubJITAgentAccepted {
		return nil
	}
	if coordinator.config.ControllerEpoch <= attempt.ControllerEpoch {
		// An intent may still be executing in its owning process. Only a later
		// durable Controller epoch can reconcile that crash window.
		return ErrGitHubReconciliationRequired
	}
	switch attempt.State {
	case store.GitHubJITIntent, store.GitHubJITGenerationAmbiguous,
		store.GitHubJITGenerated, store.GitHubJITRemovalPending:
	default:
		// start_dispatching/start_ambiguous require a post-ambiguity Agent
		// session watermark, which is owned by twk-011.
		return ErrGitHubReconciliationRequired
	}
	claim, found, err := coordinator.store.GitHubClaim(ctx, attempt.ScaleSetID, runnerRequestID)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubReconciliationRequired
		}
		return err
	}
	snapshot, online, _ := coordinator.agents.Readiness(coordinator.config.NodeID)
	if !online || snapshot.NodeID != coordinator.config.NodeID {
		return ErrGitHubReconciliationRequired
	}
	if attempt.StartCommandID != "" {
		for _, command := range snapshot.Commands {
			if command.ID == attempt.StartCommandID {
				// A Generated attempt is ordered before start dispatch. Seeing
				// that command is contradictory evidence, never absence.
				return ErrGitHubReconciliationRequired
			}
		}
	}
	for _, observation := range snapshot.Observations {
		if observation.ExecutionID == claim.Execution.ID &&
			observation.State != domain.ExecutionPreparing {
			// An advanced runtime without its exact start replay identity is
			// contradictory evidence; never remove or regenerate automatically.
			return ErrGitHubReconciliationRequired
		}
	}
	reference, err := coordinator.lifecycle.GetRunnerByName(ctx, attempt.RunnerName)
	if err != nil {
		return ErrGitHubReconciliationRequired
	}
	if reference == nil {
		return coordinator.store.MarkGitHubJITReconciledAbsent(
			ctx, attempt, coordinator.config.ControllerEpoch)
	}
	if reference.Name != attempt.RunnerName ||
		reference.ScaleSetID != coordinator.config.ScaleSetID ||
		reference.ID <= 0 {
		return ErrGitHubReconciliationRequired
	}
	if attempt.RunnerID > 0 && reference.ID != attempt.RunnerID {
		// A deterministic name is not sufficient authority to delete a runner
		// once the exact registration ID was durably recorded.
		return ErrGitHubReconciliationRequired
	}
	attempt.RunnerID = reference.ID
	if err := coordinator.store.MarkGitHubJITRemovalPending(
		ctx, attempt, coordinator.config.ControllerEpoch); err != nil {
		return err
	}
	if err := coordinator.lifecycle.RemoveRunner(ctx, *reference); err != nil {
		return ErrGitHubReconciliationRequired
	}
	// Even a successful DELETE is followed by a later read. A crash or preview
	// inconsistency cannot be mistaken for proven absence.
	return ErrGitHubReconciliationRequired
}

func controllerStoreGitHubEventType(messageType github.MessageType) (store.GitHubJobEventType, error) {
	switch messageType {
	case github.MessageTypeJobAvailable:
		return store.GitHubJobAvailable, nil
	case github.MessageTypeJobAssigned:
		return store.GitHubJobAssigned, nil
	case github.MessageTypeJobStarted:
		return store.GitHubJobStarted, nil
	case github.MessageTypeJobCompleted:
		return store.GitHubJobCompleted, nil
	default:
		return "", errors.New("unsupported GitHub job event type")
	}
}

func validateControllerRunnerUpdate(
	update transport.ExecutionUpdate,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
) error {
	if err := update.Validate(); err != nil ||
		update.NodeID != nodeID ||
		update.CommandID != metadata.CommandID ||
		update.ExecutionID != metadata.ExecutionID {
		return errors.New("Agent execution update does not match runner command")
	}
	return nil
}

func deterministicExecutionID(scaleSetID github.ScaleSetID, runnerRequestID int64) domain.ExecutionID {
	return domain.ExecutionID("twk-exec-" + deterministicControllerToken(
		fmt.Sprintf("execution\x00%d\x00%d", scaleSetID, runnerRequestID)))
}

func deterministicRunnerName(scaleSetID store.ScaleSetID, runnerRequestID int64) string {
	return "tewake-" + deterministicControllerToken(
		fmt.Sprintf("runner\x00%d\x00%d", scaleSetID, runnerRequestID))
}

func deterministicCommandID(kind string, executionID domain.ExecutionID, discriminator string) domain.CommandID {
	return domain.CommandID("twk-" + kind + "-" + deterministicControllerToken(
		kind+"\x00"+string(executionID)+"\x00"+discriminator))
}

func deterministicControllerToken(value string) string {
	digest := sha256.Sum256([]byte(value))
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(digest[:]))
}

func controllerLowerSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') &&
			!(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}
