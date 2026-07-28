package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/genm/sparerunner/internal/domain"
)

type GitHubJobEventType string

const (
	GitHubJobAvailable GitHubJobEventType = "JobAvailable"
	GitHubJobAssigned  GitHubJobEventType = "JobAssigned"
	GitHubJobStarted   GitHubJobEventType = "JobStarted"
	GitHubJobCompleted GitHubJobEventType = "JobCompleted"
)

const (
	GitHubJobResultSucceeded = "succeeded"
	GitHubJobResultFailed    = "failed"
	GitHubJobResultCanceled  = "canceled"
)

type GitHubSessionDemand struct {
	ScaleSetID             ScaleSetID
	SessionID              string
	TotalAvailableJobs     int
	TotalAcquiredJobs      int
	TotalAssignedJobs      int
	TotalRunningJobs       int
	TotalRegisteredRunners int
	TotalBusyRunners       int
	TotalIdleRunners       int
}

type GitHubJobEvent struct {
	Type GitHubJobEventType
	// RunnerRequestID is provider identity carried by the message itself, never a
	// claim key. JobAssigned legitimately arrives with it unset.
	RunnerRequestID int64
	RunnerID        int
	RunnerName      string
	Result          string
	RepositoryName  string
	OwnerName       string
	JobID           string
	WorkflowRunID   int64
	ExecutionID     domain.ExecutionID
}

type GitHubQueueMessage struct {
	ScaleSetID ScaleSetID
	MessageID  MessageID
	Digest     string
	Jobs       []GitHubJobEvent
}

type SingleSlotBinding struct {
	TargetID      domain.TargetID
	NodeID        domain.NodeID
	Slot          int
	ClaimEnabled  bool
	PollAuthority GitHubPollClaimAuthority
}

// GitHubAgentPollAuthority identifies the exact authenticated Agent snapshot
// that was current when a GitHub long poll began. A missing snapshot is an
// explicit zero value with HasSnapshot false, never an inferred ready state.
type GitHubAgentPollAuthority struct {
	NodeID                    domain.NodeID
	HasSnapshot               bool
	Revision                  uint64
	SnapshotDigest            string
	AcceptedByControllerEpoch domain.ControllerEpoch
	RunnerVersion             string
	NativeRunnerReady         bool
}

// GitHubPollClaimAuthority is immutable for one poll iteration. Volatile
// observation timestamps are deliberately excluded: transition generations,
// profile revision, and Agent snapshot revision are the mutation authority,
// while AdmissionDeadlineUnixNano closes the exact runner-update deadline.
type GitHubPollClaimAuthority struct {
	Binding                     GitHubTargetRuntimeBinding
	ProfileRevision             uint64
	VersionPolicy               domain.RunnerVersionPolicy
	RunnerVersion               string
	ReleaseGeneration           uint64
	SessionTransitionGeneration uint64
	Agent                       GitHubAgentPollAuthority
	ControllerEpoch             domain.ControllerEpoch
	AdmissionDeadlineUnixNano   int64
	AdvertisedCapacity          int
}

// GitHubPollState is read from one SQLite snapshot before a provider poll.
// Runtime contains the evidence needed for admission evaluation; ClaimAuthority
// is later compared inside the queue-message transaction.
type GitHubPollState struct {
	Runtime        GitHubRuntimeFreshness
	ClaimAuthority GitHubPollClaimAuthority
}

type GitHubClaimState string

const (
	GitHubClaimPending                GitHubClaimState = "pending"
	GitHubClaimAcquireAmbiguous       GitHubClaimState = "acquire_ambiguous"
	GitHubClaimAcquired               GitHubClaimState = "acquired"
	GitHubClaimPreparing              GitHubClaimState = "preparing"
	GitHubClaimPrepareFailed          GitHubClaimState = "prepare_failed"
	GitHubClaimJITIntent              GitHubClaimState = "jit_intent"
	GitHubClaimJITGenerationAmbiguous GitHubClaimState = "jit_generation_ambiguous"
	GitHubClaimJITGenerated           GitHubClaimState = "jit_generated"
	GitHubClaimStartDispatching       GitHubClaimState = "start_dispatching"
	GitHubClaimStartAmbiguous         GitHubClaimState = "start_ambiguous"
	GitHubClaimRunning                GitHubClaimState = "running"
	GitHubClaimReconciliationRequired GitHubClaimState = "reconciliation_required"
)

// GitHubClaimOrigin records which half of the scale-set protocol created a
// claim. It is durable because the two halves have genuinely different
// identity: an offered job carries a provider request ID that AcquireJobs must
// be called with, while an assigned job carries none at all and is only ever
// counted.
type GitHubClaimOrigin string

const (
	// GitHubClaimFromJobAvailable is the claim-this-offered-job handshake.
	GitHubClaimFromJobAvailable GitHubClaimOrigin = "job_available"
	// GitHubClaimFromAssignedDemand is a runner created because
	// Statistics.TotalAssignedJobs exceeded the durable active count. GitHub
	// matches an already-assigned job to the ephemeral runner once it registers,
	// so no job identity is needed to start one.
	GitHubClaimFromAssignedDemand GitHubClaimOrigin = "assigned_demand"
)

// GitHubJobClaim is one runner lifecycle. ClaimKey is its SpareRunner-owned durable
// identity and the key every child table references; it equals RunnerRequestID
// for an offered job and is negative for assigned demand, so the two namespaces
// can never collide. RunnerRequestID is provider correlation only and is zero
// when GitHub never issued one — it must never be synthesized, because
// AcquireJobs is keyed on it. SourceMessageID is likewise zero for a claim
// created by an empty long poll, which carries no queue message at all.
type GitHubJobClaim struct {
	ScaleSetID      ScaleSetID
	ClaimKey        int64
	Origin          GitHubClaimOrigin
	RunnerRequestID int64
	SourceMessageID MessageID
	Execution       domain.ExecutionSnapshot
	State           GitHubClaimState
	CurrentAttempt  int
}

// validateGitHubClaimIdentity mirrors the durable CHECK constraint in Go so a
// claim can never be handed to the lifecycle with an identity the database
// would have refused.
func validateGitHubClaimIdentity(claim GitHubJobClaim) error {
	switch claim.Origin {
	case GitHubClaimFromJobAvailable:
		if claim.ClaimKey <= 0 || claim.RunnerRequestID != claim.ClaimKey ||
			claim.SourceMessageID == 0 {
			return ErrGitHubClaimState
		}
	case GitHubClaimFromAssignedDemand:
		if claim.ClaimKey >= 0 || claim.RunnerRequestID != 0 {
			return ErrGitHubClaimState
		}
	default:
		return ErrGitHubClaimState
	}
	return nil
}

type GitHubAcquireAttempt struct {
	ScaleSetID      ScaleSetID
	ClaimKey        int64
	Attempt         int
	EvidenceMessage MessageID
	ControllerEpoch domain.ControllerEpoch
}

type githubAcquireAttemptState string

const (
	githubAcquirePending           githubAcquireAttemptState = "pending"
	githubAcquireReconciledPending githubAcquireAttemptState = "reconciled_pending"
	githubAcquireDispatching       githubAcquireAttemptState = "dispatching"
	githubAcquireAcquired          githubAcquireAttemptState = "acquired"
)

type githubAcquireAttemptRecord struct {
	GitHubAcquireAttempt
	State githubAcquireAttemptState
}

func (record githubAcquireAttemptRecord) public() GitHubAcquireAttempt {
	return record.GitHubAcquireAttempt
}

type GitHubMessageCommit struct {
	Replayed           bool
	Claim              *GitHubJobClaim
	RequeueIntent      *GitHubUnpickedRequeueIntent
	UnclaimedAvailable bool
	// AssignedDemand reports the assigned-job reconciliation performed inside
	// this same transaction. Its Created claim, when present, is already durable
	// by the time the caller may acknowledge the message.
	AssignedDemand GitHubAssignedDemandResult
}

// GitHubUnpickedRequeueIntent binds one fresh JobAvailable event to the exact
// started-but-terminal runner attempt it supersedes. Replacement is only an
// immutable proposed identity until the old provider registration is proven
// absent; the intent itself fences that slot from every other claim.
type GitHubUnpickedRequeueIntent struct {
	Claim             GitHubJobClaim
	Attempt           GitHubJITAttempt
	Replacement       domain.ExecutionSnapshot
	SourceMessageID   MessageID
	SourceEventIndex  int
	ControllerEpoch   domain.ControllerEpoch
	CreatedAtUnixNano int64
	UpdatedAtUnixNano int64
	PickupProven      bool
}

type GitHubJITAttemptState string

const (
	GitHubJITIntent              GitHubJITAttemptState = "intent"
	GitHubJITGenerationAmbiguous GitHubJITAttemptState = "generation_ambiguous"
	GitHubJITGenerated           GitHubJITAttemptState = "generated"
	GitHubJITStartDispatching    GitHubJITAttemptState = "start_dispatching"
	GitHubJITStartAmbiguous      GitHubJITAttemptState = "start_ambiguous"
	GitHubJITStarted             GitHubJITAttemptState = "started"
	GitHubJITAgentAccepted       GitHubJITAttemptState = "agent_accepted"
	GitHubJITRemovalPending      GitHubJITAttemptState = "removal_pending"
	GitHubJITReconciledAbsent    GitHubJITAttemptState = "reconciled_absent"
)

type GitHubJITAttempt struct {
	ScaleSetID      ScaleSetID
	ClaimKey        int64
	Attempt         int
	ControllerEpoch domain.ControllerEpoch
	RunnerName      string
	State           GitHubJITAttemptState
	RunnerID        int
	JITDigest       string
	StartCommandID  domain.CommandID
}

// GitHubJITAbsenceResult describes the durable state created after an exact
// provider runner has remained absent across the confirmation interval. Most
// ambiguities resume the already-prepared execution. Lost-JIT recovery instead
// retains a terminal dormant claim; only a later fresh JobAvailable message may
// create a new execution and acquisition attempt.
type GitHubJITAbsenceResult struct {
	Claim                GitHubJobClaim
	TerminalExecution    *domain.ExecutionSnapshot
	ReplacementExecution *domain.ExecutionSnapshot
	ReplacementClaimed   bool
	AwaitingAvailability bool
	CleanupBlocked       bool
}

type githubRunnerRemovalKind uint8

const (
	githubRunnerRemovalAmbiguity githubRunnerRemovalKind = iota
	githubRunnerRemovalLostJIT
	githubRunnerRemovalUnpickedRequeue
)

// GitHubJITPrunedHistoryResult classifies an exact Start after the Agent has
// acknowledged and pruned its terminal command/runtime journal. Started means
// durable Running/Cleaning history closed the JIT ambiguity atomically.
// LostTerminal is set only when an exact terminal Start update exists without
// that runtime history; the caller must retain the fence and reconcile the
// exact provider runner.
type GitHubJITPrunedHistoryResult struct {
	Started      bool
	LostTerminal domain.ExecutionState
}

var (
	ErrGitHubClaimState         = errors.New("GitHub job claim state conflict")
	ErrGitHubJITState           = errors.New("GitHub JIT attempt state conflict")
	ErrGitHubJITStartNotProven  = errors.New("GitHub JIT start has no durable running history")
	ErrGitHubJITTerminalPending = errors.New(
		"GitHub JIT terminal update is not yet durable",
	)
	ErrGitHubJITAbsencePending      = errors.New("GitHub runner absence confirmation is pending")
	ErrGitHubRequeueTerminalPending = errors.New(
		"fresh GitHub availability is waiting for exact terminal runner authority",
	)
	ErrGitHubRecoveryAvailabilityPending = errors.New(
		"fresh GitHub availability is waiting for current recovery admission",
	)
)

// GitHubRunnerAbsenceConfirmationDelay is the durable minimum separation
// between two exact absence reads. The orchestration loop uses the same value
// so it neither busy-polls GitHub nor falls back to an unrelated long poll.
const GitHubRunnerAbsenceConfirmationDelay = 2 * time.Second

func (s *ControllerStore) RecordGitHubSessionDemand(ctx context.Context, demand GitHubSessionDemand) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if err := validateGitHubSessionDemand(demand); err != nil {
		return err
	}
	observedAt, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO github_session_demand(
		scale_set_id, session_id, total_available_jobs, total_acquired_jobs,
		total_assigned_jobs, total_running_jobs, total_registered_runners,
		total_busy_runners, total_idle_runners, observed_at_unix_nano
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(scale_set_id) DO UPDATE SET
		session_id=excluded.session_id,
		total_available_jobs=excluded.total_available_jobs,
		total_acquired_jobs=excluded.total_acquired_jobs,
		total_assigned_jobs=excluded.total_assigned_jobs,
		total_running_jobs=excluded.total_running_jobs,
		total_registered_runners=excluded.total_registered_runners,
		total_busy_runners=excluded.total_busy_runners,
		total_idle_runners=excluded.total_idle_runners,
		observed_at_unix_nano=CASE
			WHEN github_session_demand.observed_at_unix_nano >= excluded.observed_at_unix_nano
			THEN github_session_demand.observed_at_unix_nano + 1
			ELSE excluded.observed_at_unix_nano
		END`,
		demand.ScaleSetID, demand.SessionID, demand.TotalAvailableJobs,
		demand.TotalAcquiredJobs, demand.TotalAssignedJobs, demand.TotalRunningJobs,
		demand.TotalRegisteredRunners, demand.TotalBusyRunners,
		demand.TotalIdleRunners, observedAt)
	return err
}

func validateGitHubSessionDemand(demand GitHubSessionDemand) error {
	if demand.ScaleSetID == 0 || uint64(demand.ScaleSetID) > maxSQLiteInteger || demand.SessionID == "" {
		return errors.New("GitHub session demand identity is invalid")
	}
	values := []int{
		demand.TotalAvailableJobs,
		demand.TotalAcquiredJobs,
		demand.TotalAssignedJobs,
		demand.TotalRunningJobs,
		demand.TotalRegisteredRunners,
		demand.TotalBusyRunners,
		demand.TotalIdleRunners,
	}
	for _, value := range values {
		if value < 0 {
			return errors.New("GitHub session demand contains a negative statistic")
		}
	}
	if demand.TotalRunningJobs > demand.TotalAssignedJobs {
		return errors.New("GitHub running demand exceeds assigned demand")
	}
	return nil
}

// ReadGitHubPollState captures configuration, provider freshness, Controller
// epoch, and current Agent snapshot from one SQLite read transaction. The
// caller may combine it with in-memory liveness, but may not reconstruct this
// durable authority from independent reads.
func (s *ControllerStore) ReadGitHubPollState(
	ctx context.Context,
	binding GitHubTargetRuntimeBinding,
	nodeID domain.NodeID,
) (GitHubPollState, error) {
	if err := s.requireReady(); err != nil {
		return GitHubPollState{}, err
	}
	if err := validateGitHubTargetRuntimeBinding(binding); err != nil {
		return GitHubPollState{}, err
	}
	if !canonicalRuntimeIdentifier(string(nodeID)) {
		return GitHubPollState{}, errors.New("GitHub poll state requires node ID")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return GitHubPollState{}, err
	}
	defer tx.Rollback()
	state, err := readGitHubPollState(ctx, tx, binding, nodeID)
	if err != nil {
		return GitHubPollState{}, err
	}
	if err := tx.Commit(); err != nil {
		return GitHubPollState{}, err
	}
	return state, nil
}

func readGitHubPollState(
	ctx context.Context,
	queryer interface {
		freshnessQueryer
	},
	binding GitHubTargetRuntimeBinding,
	nodeID domain.NodeID,
) (GitHubPollState, error) {
	runtimeState, err := readGitHubRuntimeFreshness(ctx, queryer, binding)
	if err != nil {
		return GitHubPollState{}, err
	}
	controllerEpoch, err := readUintMetadata(ctx, queryer, "controller_epoch")
	if err != nil {
		return GitHubPollState{}, err
	}
	agent := GitHubAgentPollAuthority{NodeID: nodeID}
	var ready int
	err = queryer.QueryRowContext(ctx, `SELECT
			a.revision, a.snapshot_digest, a.accepted_by_controller_epoch,
			s.runner_version, s.native_runner_ready
		FROM agent_snapshot_authority a
		JOIN agent_session_snapshots s ON s.node_id = a.node_id
		WHERE a.node_id = ?`, nodeID).
		Scan(
			&agent.Revision,
			&agent.SnapshotDigest,
			&agent.AcceptedByControllerEpoch,
			&agent.RunnerVersion,
			&ready,
		)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	} else if err == nil {
		agent.HasSnapshot = true
		if ready < 0 || ready > 1 {
			return GitHubPollState{}, errors.New("stored Agent runner readiness is invalid")
		}
		agent.NativeRunnerReady = ready == 1
		if err := validateGitHubAgentPollAuthority(agent); err != nil {
			return GitHubPollState{}, err
		}
	}
	if err != nil {
		return GitHubPollState{}, err
	}
	authority := GitHubPollClaimAuthority{
		Binding:                     binding,
		ProfileRevision:             runtimeState.Profile.Revision,
		VersionPolicy:               runtimeState.Profile.VersionPolicy,
		RunnerVersion:               runtimeState.Profile.RunnerVersion,
		SessionTransitionGeneration: runtimeState.Session.TransitionGeneration,
		Agent:                       agent,
		ControllerEpoch:             domain.ControllerEpoch(controllerEpoch),
	}
	if runtimeState.Profile.VersionPolicy == domain.RunnerVersionPinned {
		authority.ReleaseGeneration = runtimeState.Release.Generation
	}
	return GitHubPollState{
		Runtime:        runtimeState,
		ClaimAuthority: authority,
	}, nil
}

func validateGitHubAgentPollAuthority(authority GitHubAgentPollAuthority) error {
	if !canonicalRuntimeIdentifier(string(authority.NodeID)) {
		return errors.New("GitHub Agent poll authority requires node ID")
	}
	if !authority.HasSnapshot {
		if authority.Revision != 0 || authority.SnapshotDigest != "" ||
			authority.AcceptedByControllerEpoch != 0 ||
			authority.RunnerVersion != "" || authority.NativeRunnerReady {
			return errors.New("missing Agent poll authority carries snapshot evidence")
		}
		return nil
	}
	if authority.Revision == 0 ||
		authority.Revision > maxSQLiteInteger ||
		!isLowerSHA256(authority.SnapshotDigest) ||
		authority.AcceptedByControllerEpoch.Validate() != nil ||
		!canonicalRunnerVersion(authority.RunnerVersion) {
		return errors.New("GitHub Agent poll authority is invalid")
	}
	return nil
}

func validateGitHubPollClaimAuthority(authority GitHubPollClaimAuthority) error {
	if err := validateGitHubTargetRuntimeBinding(authority.Binding); err != nil {
		return err
	}
	if authority.ProfileRevision == 0 ||
		authority.ProfileRevision > maxSQLiteInteger ||
		!canonicalRunnerVersion(authority.RunnerVersion) ||
		authority.ControllerEpoch.Validate() != nil ||
		authority.SessionTransitionGeneration == 0 ||
		authority.SessionTransitionGeneration > maxSQLiteInteger ||
		authority.AdvertisedCapacity != 1 {
		return errors.New("GitHub poll claim authority is incomplete")
	}
	if err := validateGitHubAgentPollAuthority(authority.Agent); err != nil {
		return err
	}
	if !authority.Agent.HasSnapshot ||
		!authority.Agent.NativeRunnerReady ||
		authority.Agent.AcceptedByControllerEpoch != authority.ControllerEpoch ||
		authority.Agent.RunnerVersion != authority.RunnerVersion {
		return errors.New("GitHub poll claim Agent authority is not ready")
	}
	switch authority.VersionPolicy {
	case domain.RunnerVersionAutoUpdate:
		if authority.ReleaseGeneration != 0 ||
			authority.AdmissionDeadlineUnixNano != 0 {
			return errors.New("managed runner poll authority carries release deadline")
		}
	case domain.RunnerVersionPinned:
		if authority.ReleaseGeneration == 0 ||
			authority.ReleaseGeneration > maxSQLiteInteger ||
			authority.AdmissionDeadlineUnixNano <= 0 {
			return errors.New("pinned runner poll authority lacks release evidence")
		}
	default:
		return errors.New("GitHub poll claim version policy is invalid")
	}
	return nil
}

// CommitGitHubQueueMessage exact-deduplicates the queue message independently
// from executions. If the concrete slot is free, the first unclaimed
// JobAvailable event receives the only reservation in the same transaction.
func (s *ControllerStore) CommitGitHubQueueMessage(
	ctx context.Context,
	message GitHubQueueMessage,
	binding SingleSlotBinding,
) (GitHubMessageCommit, error) {
	if err := s.requireReady(); err != nil {
		return GitHubMessageCommit{}, err
	}
	if !s.ManagementAuditHealthy() {
		return GitHubMessageCommit{}, ErrManagementAuditPersistence
	}
	if err := validateGitHubQueueMessage(message); err != nil {
		return GitHubMessageCommit{}, err
	}
	if binding.TargetID == "" || binding.NodeID == "" || binding.Slot != 0 {
		return GitHubMessageCommit{}, errors.New("single-slot GitHub binding must name target, node, and slot zero")
	}
	committedAt, err := storeUnixNano(s.now())
	if err != nil {
		return GitHubMessageCommit{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return GitHubMessageCommit{}, err
	}
	defer tx.Rollback()
	if !s.ManagementAuditHealthy() {
		return GitHubMessageCommit{}, ErrManagementAuditPersistence
	}
	controllerEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return GitHubMessageCommit{}, err
	}

	var digest string
	replayed := false
	err = tx.QueryRowContext(ctx, `SELECT message_digest FROM github_queue_messages
		WHERE scale_set_id = ? AND message_id = ?`,
		message.ScaleSetID, message.MessageID).Scan(&digest)
	if err == nil {
		if digest != message.Digest {
			return GitHubMessageCommit{}, fmt.Errorf("%w: GitHub queue message %d", ErrReplayMismatch, message.MessageID)
		}
		replayed = true
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return GitHubMessageCommit{}, err
	}
	if !replayed {
		if _, err := tx.ExecContext(ctx, `INSERT INTO github_queue_messages(
			scale_set_id, message_id, message_digest, committed_at_unix_nano
		) VALUES (?, ?, ?, ?)`, message.ScaleSetID, message.MessageID,
			message.Digest, committedAt); err != nil {
			return GitHubMessageCommit{}, err
		}
		for index, job := range message.Jobs {
			if _, err := tx.ExecContext(ctx, `INSERT INTO github_message_jobs(
				scale_set_id, message_id, event_index, event_type, runner_request_id,
				runner_id, runner_name, result, repository_name, owner_name, job_id,
				workflow_run_id
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				message.ScaleSetID, message.MessageID, index, job.Type,
				job.RunnerRequestID, job.RunnerID, job.RunnerName, job.Result,
				job.RepositoryName, job.OwnerName, job.JobID, job.WorkflowRunID); err != nil {
				return GitHubMessageCommit{}, err
			}
		}
	}
	claimed, requeueIntent, unclaimedAvailable, err := resolveGitHubAvailableClaim(
		ctx,
		tx,
		message,
		binding,
		committedAt,
		domain.ControllerEpoch(controllerEpoch),
		!replayed,
	)
	if err != nil {
		return GitHubMessageCommit{}, err
	}
	// Assigned demand is reconciled inside the transaction that commits this
	// message, so any execution it creates is durable before the message can be
	// acknowledged. A crash between the two redelivers the message and reaches
	// the same count instead of losing the work.
	demand := GitHubAssignedDemandResult{}
	if claimed == nil {
		demand, err = reconcileGitHubAssignedDemand(
			ctx, tx, message.ScaleSetID, binding, committedAt)
		if err != nil {
			return GitHubMessageCommit{}, err
		}
	}
	// Take the audit gate only after all SQLite work is complete. Taking it
	// before BeginTx would invert the single-connection DB/audit lock order when
	// another transaction is degrading audit authority. A completed degradation
	// therefore precedes this check, or waits until this commit has completed.
	s.auditGate.RLock()
	defer s.auditGate.RUnlock()
	if !s.ManagementAuditHealthy() {
		return GitHubMessageCommit{}, ErrManagementAuditPersistence
	}
	if s.beforeGitHubQueueCommit != nil {
		s.beforeGitHubQueueCommit()
	}
	if err := tx.Commit(); err != nil {
		return GitHubMessageCommit{}, err
	}
	return GitHubMessageCommit{
		Replayed:           replayed,
		Claim:              claimed,
		RequeueIntent:      requeueIntent,
		UnclaimedAvailable: unclaimedAvailable,
		AssignedDemand:     demand,
	}, nil
}

func resolveGitHubAvailableClaim(
	ctx context.Context,
	tx *sql.Tx,
	message GitHubQueueMessage,
	binding SingleSlotBinding,
	committedAt int64,
	controllerEpoch domain.ControllerEpoch,
	freshMessage bool,
) (*GitHubJobClaim, *GitHubUnpickedRequeueIntent, bool, error) {
	var resolved *GitHubJobClaim
	var resolvedRequeue *GitHubUnpickedRequeueIntent
	unclaimed := make([]GitHubJobEvent, 0)
	seen := make(map[int64]struct{})
	for eventIndex, job := range message.Jobs {
		if job.Type != GitHubJobAvailable {
			continue
		}
		if _, duplicate := seen[job.RunnerRequestID]; duplicate {
			continue
		}
		seen[job.RunnerRequestID] = struct{}{}
		existing, found, err := loadGitHubClaim(ctx, tx, message.ScaleSetID, job.RunnerRequestID)
		if err != nil {
			return nil, nil, true, err
		}
		if !found {
			unclaimed = append(unclaimed, job)
			continue
		}
		if existing.Execution.TargetID != binding.TargetID ||
			existing.Execution.Slot.NodeID != binding.NodeID ||
			existing.Execution.Slot.Index != binding.Slot {
			return nil, nil, true, fmt.Errorf("%w: GitHub available job binding changed", ErrReplayMismatch)
		}
		intent, hasIntent, err := loadGitHubUnpickedRequeueIntent(
			ctx,
			tx,
			existing.ScaleSetID,
			existing.ClaimKey,
		)
		if err != nil {
			return nil, nil, true, err
		}
		if hasIntent {
			if err := requireCompatibleGitHubRequeueEvent(
				ctx,
				tx,
				intent,
				job,
			); err != nil {
				return nil, nil, true, err
			}
			existing = intent.Claim
			value := intent
			resolvedRequeue = &value
		} else {
			requeueReadiness, attempt, err := unpickedRunnerRequeueReadiness(
				ctx,
				tx,
				existing,
			)
			if err != nil {
				return nil, nil, true, err
			}
			if freshMessage &&
				requeueReadiness == githubUnpickedRequeueAwaitTerminal {
				// Do not commit the message dedupe row. The Poller will withhold
				// ACK, and redelivery remains first-seen authority after the
				// exact terminal outbox catches up with the Agent snapshot.
				return nil, nil, true, ErrGitHubRequeueTerminalPending
			}
			if freshMessage &&
				requeueReadiness == githubUnpickedRequeueReady {
				if err := requireGitHubFreshRecoveryAdmission(
					ctx,
					tx,
					binding,
					committedAt,
				); err != nil {
					return nil, nil, true, err
				}
				intent, err = createGitHubUnpickedRequeueIntent(
					ctx,
					tx,
					existing,
					attempt,
					job,
					message.MessageID,
					eventIndex,
					controllerEpoch,
					committedAt,
				)
				if err != nil {
					return nil, nil, true, err
				}
				existing = intent.Claim
				value := intent
				resolvedRequeue = &value
			}
		}
		awaitingAvailability, err := lostJITClaimAwaitingAvailability(
			ctx,
			tx,
			existing,
		)
		if err != nil {
			return nil, nil, true, err
		}
		if freshMessage && awaitingAvailability {
			if err := requireGitHubFreshRecoveryAdmission(
				ctx,
				tx,
				binding,
				committedAt,
			); err != nil {
				return nil, nil, true, err
			}
			existing, err = rearmLostJITFromMessage(
				ctx,
				tx,
				existing,
				job,
				message.MessageID,
				controllerEpoch,
				committedAt,
			)
			if err != nil {
				return nil, nil, true, err
			}
		}
		if freshMessage && existing.State == GitHubClaimAcquireAmbiguous {
			existing, err = rearmGitHubAcquireFromMessage(
				ctx,
				tx,
				existing,
				message.MessageID,
				controllerEpoch,
				committedAt,
			)
			if err != nil {
				return nil, nil, true, err
			}
		}
		if resolved == nil {
			value := existing
			resolved = &value
		}
	}
	if len(unclaimed) == 0 {
		return resolved, resolvedRequeue, false, nil
	}
	if len(unclaimed) > 1 {
		// The SPR-007 vertical owns one concrete slot and may not partially
		// acknowledge a message containing more independent availability than it
		// can durably claim. The pinned poll requests capacity one, so this is a
		// fail-closed preview-contract violation.
		return resolved, resolvedRequeue, true, nil
	}
	if !binding.ClaimEnabled {
		return resolved, resolvedRequeue, true, nil
	}
	slotFree, err := githubSlotAvailable(ctx, tx, binding)
	if err != nil || !slotFree {
		return resolved, resolvedRequeue, true, err
	}
	authorityCurrent, err := githubPollClaimAuthorityCurrent(
		ctx,
		tx,
		binding,
		committedAt,
	)
	if err != nil || !authorityCurrent {
		return resolved, resolvedRequeue, true, err
	}
	job := unclaimed[0]
	claim, err := insertGitHubSlotClaim(ctx, tx, githubSlotClaimRequest{
		ScaleSetID:      message.ScaleSetID,
		Origin:          GitHubClaimFromJobAvailable,
		ClaimKey:        job.RunnerRequestID,
		RunnerRequestID: job.RunnerRequestID,
		SourceMessageID: message.MessageID,
		ExecutionID:     job.ExecutionID,
		State:           GitHubClaimPending,
		Binding:         binding,
		CommittedAt:     committedAt,
	})
	if err != nil {
		return nil, nil, true, err
	}
	// An offered job still needs the acquire handshake before it may run; the
	// assigned-demand path deliberately has no equivalent row because there is
	// nothing to acquire.
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_acquire_attempts(
		scale_set_id, claim_key, attempt, evidence_message_id,
		controller_epoch, state, created_at_unix_nano, updated_at_unix_nano
	) VALUES (?, ?, 1, ?, ?, 'pending', ?, ?)`,
		message.ScaleSetID,
		job.RunnerRequestID,
		message.MessageID,
		controllerEpoch,
		committedAt,
		committedAt,
	); err != nil {
		return nil, nil, true, err
	}
	return &claim, resolvedRequeue, false, nil
}

// githubSlotClaimRequest is the complete input needed to reserve the single
// slot and create one durable claim. Both protocol halves go through it so the
// reservation, execution, and claim rows can never drift apart.
type githubSlotClaimRequest struct {
	ScaleSetID      ScaleSetID
	Origin          GitHubClaimOrigin
	ClaimKey        int64
	RunnerRequestID int64
	SourceMessageID MessageID
	ExecutionID     domain.ExecutionID
	State           GitHubClaimState
	Binding         SingleSlotBinding
	CommittedAt     int64
}

func insertGitHubSlotClaim(
	ctx context.Context,
	tx *sql.Tx,
	request githubSlotClaimRequest,
) (GitHubJobClaim, error) {
	binding := request.Binding
	execution := domain.ExecutionSnapshot{
		ID:       request.ExecutionID,
		TargetID: binding.TargetID,
		Slot:     domain.SlotKey{NodeID: binding.NodeID, Index: binding.Slot},
		State:    domain.ExecutionReserved,
	}
	if err := execution.Validate(); err != nil {
		return GitHubJobClaim{}, err
	}
	claim := GitHubJobClaim{
		ScaleSetID:      request.ScaleSetID,
		ClaimKey:        request.ClaimKey,
		Origin:          request.Origin,
		RunnerRequestID: request.RunnerRequestID,
		SourceMessageID: request.SourceMessageID,
		Execution:       execution,
		State:           request.State,
	}
	if err := validateGitHubClaimIdentity(claim); err != nil {
		return GitHubJobClaim{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO executions(
			id, target_id, node_id, slot_index, state, created_at_unix_nano
		) VALUES (?, ?, ?, ?, ?, ?)`, execution.ID, binding.TargetID,
		binding.NodeID, binding.Slot, execution.State,
		request.CommittedAt); err != nil {
		return GitHubJobClaim{}, mapAssignmentError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO slot_reservations(
			node_id, slot_index, target_id, execution_id
		) VALUES (?, ?, ?, ?)`, binding.NodeID, binding.Slot,
		binding.TargetID, execution.ID); err != nil {
		return GitHubJobClaim{}, mapAssignmentError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_job_claims(
			scale_set_id, claim_key, origin, runner_request_id, source_message_id,
			execution_id, state, current_jit_attempt, created_at_unix_nano,
			updated_at_unix_nano
		) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`,
		request.ScaleSetID,
		request.ClaimKey,
		request.Origin,
		nullableGitHubInt64(request.RunnerRequestID),
		nullableGitHubInt64(int64(request.SourceMessageID)),
		execution.ID,
		request.State,
		request.CommittedAt,
		request.CommittedAt,
	); err != nil {
		return GitHubJobClaim{}, err
	}
	return claim, nil
}

// GitHubAssignedDemandResult reports one reconciliation of durable active
// executions against GitHub's assigned-job statistic. Observed is false when no
// statistics have ever been recorded for the scale set: demand is never
// invented, so that case creates nothing and is not a shortfall. Unserved is
// demand that every gate refused this round; it is returned rather than
// swallowed so the caller can log it.
type GitHubAssignedDemandResult struct {
	Observed bool
	Desired  int
	Active   int
	Created  *GitHubJobClaim
	Unserved int
}

// ReconcileGitHubAssignedDemand raises durable executions toward the assigned
// job count recorded for this scale set. It exists because GitHub's scale-set
// protocol drives runner creation from Statistics.TotalAssignedJobs, not from
// JobAvailable: an assigned job is never offered, so waiting for an offer left
// the workflow queued forever. The reference listener re-evaluates desired
// count even when a long poll returns no message, which is exactly what this
// entry point serves.
func (s *ControllerStore) ReconcileGitHubAssignedDemand(
	ctx context.Context,
	scaleSetID ScaleSetID,
	binding SingleSlotBinding,
) (GitHubAssignedDemandResult, error) {
	if err := s.requireReady(); err != nil {
		return GitHubAssignedDemandResult{}, err
	}
	if !s.ManagementAuditHealthy() {
		return GitHubAssignedDemandResult{}, ErrManagementAuditPersistence
	}
	if scaleSetID == 0 || uint64(scaleSetID) > maxSQLiteInteger {
		return GitHubAssignedDemandResult{}, errors.New("GitHub scale set ID is invalid")
	}
	if binding.TargetID == "" || binding.NodeID == "" || binding.Slot != 0 {
		return GitHubAssignedDemandResult{}, errors.New(
			"single-slot GitHub binding must name target, node, and slot zero")
	}
	observedAt, err := storeUnixNano(s.now())
	if err != nil {
		return GitHubAssignedDemandResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return GitHubAssignedDemandResult{}, err
	}
	defer tx.Rollback()
	result, err := reconcileGitHubAssignedDemand(
		ctx, tx, scaleSetID, binding, observedAt)
	if err != nil {
		return GitHubAssignedDemandResult{}, err
	}
	if result.Created == nil {
		// Nothing durable changed; do not take the audit gate or commit a
		// write transaction for a pure read.
		return result, nil
	}
	s.auditGate.RLock()
	defer s.auditGate.RUnlock()
	if !s.ManagementAuditHealthy() {
		return GitHubAssignedDemandResult{}, ErrManagementAuditPersistence
	}
	if err := tx.Commit(); err != nil {
		return GitHubAssignedDemandResult{}, err
	}
	return result, nil
}

// reconcileGitHubAssignedDemand converges durable state to a count rather than
// creating one execution per message seen. That is what makes it safe under
// redelivery and restart: the same statistic reached twice reads the same
// durable active count inside the same transaction and creates nothing the
// second time.
func reconcileGitHubAssignedDemand(
	ctx context.Context,
	tx *sql.Tx,
	scaleSetID ScaleSetID,
	binding SingleSlotBinding,
	committedAt int64,
) (GitHubAssignedDemandResult, error) {
	desired, observed, err := githubAssignedDemand(ctx, tx, scaleSetID)
	if err != nil || !observed {
		return GitHubAssignedDemandResult{}, err
	}
	active, err := githubActiveClaimCount(ctx, tx, scaleSetID)
	if err != nil {
		return GitHubAssignedDemandResult{}, err
	}
	result := GitHubAssignedDemandResult{
		Observed: true,
		Desired:  desired,
		Active:   active,
	}
	missing := desired - active
	if missing <= 0 {
		return result, nil
	}
	result.Unserved = missing
	// Assigned demand raises the desired count; it may never bypass a gate. All
	// three below are the same predicates the JobAvailable claim boundary uses,
	// called here rather than duplicated, so node capacity, target exclusions,
	// availability intent, admission, and runner-update policy stay identical
	// for both halves of the protocol.
	if !binding.ClaimEnabled {
		return result, nil
	}
	slotFree, err := githubSlotAvailable(ctx, tx, binding)
	if err != nil || !slotFree {
		return result, err
	}
	authorityCurrent, err := githubPollClaimAuthorityCurrent(
		ctx, tx, binding, committedAt)
	if err != nil || !authorityCurrent {
		return result, err
	}
	claimKey, err := allocateGitHubDemandClaimKey(ctx, tx, scaleSetID)
	if err != nil {
		return GitHubAssignedDemandResult{}, err
	}
	claim, err := insertGitHubSlotClaim(ctx, tx, githubSlotClaimRequest{
		ScaleSetID:  scaleSetID,
		Origin:      GitHubClaimFromAssignedDemand,
		ClaimKey:    claimKey,
		ExecutionID: githubDemandExecutionID(scaleSetID, claimKey),
		// There is nothing to acquire: GitHub already assigned the job and will
		// match it to whichever ephemeral runner registers. The claim therefore
		// enters the shared lifecycle at the state the acquire handshake would
		// have left it in, and reuses prepare -> JIT -> start unchanged.
		State:       GitHubClaimAcquired,
		Binding:     binding,
		CommittedAt: committedAt,
	})
	if err != nil {
		return GitHubAssignedDemandResult{}, err
	}
	result.Created = &claim
	result.Unserved = missing - 1
	return result, nil
}

func githubAssignedDemand(
	ctx context.Context,
	tx *sql.Tx,
	scaleSetID ScaleSetID,
) (int, bool, error) {
	var assigned int
	err := tx.QueryRowContext(ctx, `SELECT total_assigned_jobs
		FROM github_session_demand WHERE scale_set_id = ?`,
		scaleSetID).Scan(&assigned)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return assigned, true, nil
}

// githubActiveClaimCount counts the runner lifecycles that are still consuming
// assigned demand. A claim whose execution reached a terminal state has
// released its slot and no longer serves a job, except while an unpicked
// requeue intent still owns a replacement for it.
func githubActiveClaimCount(
	ctx context.Context,
	tx *sql.Tx,
	scaleSetID ScaleSetID,
) (int, error) {
	var active int
	err := tx.QueryRowContext(ctx, `SELECT count(*)
		FROM github_job_claims c
		JOIN executions e ON e.id = c.execution_id
		WHERE c.scale_set_id = ?
			AND (
				e.state NOT IN ('released', 'failed', 'quarantined')
				OR EXISTS (
					SELECT 1 FROM github_unpicked_requeue_intents intent
					WHERE intent.scale_set_id = c.scale_set_id
						AND intent.claim_key = c.claim_key
				)
			)`, scaleSetID).Scan(&active)
	return active, err
}

// allocateGitHubDemandClaimKey hands out the next key below every key this
// scale set has ever used. Claim rows are never deleted, so the minimum is
// monotonic and needs no separate allocator. Keys stay negative so they can
// never be mistaken for, or collide with, a GitHub runner request ID.
func allocateGitHubDemandClaimKey(
	ctx context.Context,
	tx *sql.Tx,
	scaleSetID ScaleSetID,
) (int64, error) {
	var lowest int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(min(claim_key), 0)
		FROM github_job_claims WHERE scale_set_id = ?`,
		scaleSetID).Scan(&lowest); err != nil {
		return 0, err
	}
	if lowest > 0 {
		lowest = 0
	}
	key := lowest - 1
	if key >= 0 {
		return 0, errors.New("GitHub demand claim key allocation overflowed")
	}
	return key, nil
}

func githubDemandExecutionID(
	scaleSetID ScaleSetID,
	claimKey int64,
) domain.ExecutionID {
	digest := sha256.Sum256(fmt.Appendf(
		nil, "execution\x00assigned_demand\x00%d\x00%d", scaleSetID, claimKey))
	return domain.ExecutionID("spr-exec-" + strings.ToLower(
		base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(digest[:])))
}

// nullableGitHubInt64 keeps "GitHub never issued one" out of the value domain.
// Zero is not a valid request or message ID, so it is stored as SQL NULL rather
// than as a number the schema would have to carve an exception for.
func nullableGitHubInt64(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

// requireGitHubFreshRecoveryAdmission protects fresh-only recovery transitions.
// A temporary admission miss must abort the surrounding transaction so GitHub
// redelivery remains first-seen authority after the slot becomes eligible.
func requireGitHubFreshRecoveryAdmission(
	ctx context.Context,
	tx *sql.Tx,
	binding SingleSlotBinding,
	committedAt int64,
) error {
	if !binding.ClaimEnabled {
		return ErrGitHubRecoveryAvailabilityPending
	}
	slotFree, err := githubSlotAvailable(ctx, tx, binding)
	if err != nil {
		return err
	}
	if !slotFree {
		return ErrGitHubRecoveryAvailabilityPending
	}
	authorityCurrent, err := githubPollClaimAuthorityCurrent(
		ctx,
		tx,
		binding,
		committedAt,
	)
	if err != nil {
		return err
	}
	if !authorityCurrent {
		return ErrGitHubRecoveryAvailabilityPending
	}
	return nil
}

type githubUnpickedRequeueReadiness uint8

const (
	githubUnpickedRequeueNone githubUnpickedRequeueReadiness = iota
	githubUnpickedRequeueAwaitTerminal
	githubUnpickedRequeueReady
)

func unpickedRunnerRequeueReadiness(
	ctx context.Context,
	tx *sql.Tx,
	claim GitHubJobClaim,
) (githubUnpickedRequeueReadiness, GitHubJITAttempt, error) {
	if (claim.State != GitHubClaimRunning &&
		claim.State != GitHubClaimReconciliationRequired) ||
		claim.CurrentAttempt < 1 {
		return githubUnpickedRequeueNone, GitHubJITAttempt{}, nil
	}
	attempt, found, err := loadGitHubJITAttempt(
		ctx,
		tx,
		claim.ScaleSetID,
		claim.ClaimKey,
		claim.CurrentAttempt,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubJITState
		}
		return githubUnpickedRequeueNone, GitHubJITAttempt{}, err
	}
	if attempt.State != GitHubJITStarted {
		return githubUnpickedRequeueNone, GitHubJITAttempt{}, nil
	}
	started, err := exactStartHasDurableRuntimeHistory(ctx, tx, attempt, claim)
	if err != nil {
		return githubUnpickedRequeueNone, GitHubJITAttempt{}, err
	}
	if !started {
		return githubUnpickedRequeueNone, GitHubJITAttempt{},
			ErrGitHubJITStartNotProven
	}
	pickedUp, err := githubJITAttemptPickupProven(ctx, tx, attempt)
	if err != nil || pickedUp {
		return githubUnpickedRequeueNone, GitHubJITAttempt{}, err
	}
	switch claim.Execution.State {
	case domain.ExecutionRunning, domain.ExecutionCleaning,
		domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
		return githubUnpickedRequeueAwaitTerminal, attempt, nil
	case domain.ExecutionReleased, domain.ExecutionFailed:
	default:
		return githubUnpickedRequeueNone, GitHubJITAttempt{},
			ErrGitHubClaimState
	}
	terminal, terminalFound, err := exactStartDurableTerminalUpdate(
		ctx,
		tx,
		attempt,
		claim,
	)
	if err != nil {
		return githubUnpickedRequeueNone, GitHubJITAttempt{}, err
	}
	if !started || !terminalFound || terminal != claim.Execution.State {
		return githubUnpickedRequeueNone, GitHubJITAttempt{},
			ErrGitHubClaimState
	}
	return githubUnpickedRequeueReady, attempt, nil
}

func githubJITAttemptPickupProven(
	ctx context.Context,
	queryer controllerAgentQueryer,
	attempt GitHubJITAttempt,
) (bool, error) {
	if attempt.ScaleSetID == 0 || attempt.ClaimKey == 0 ||
		attempt.Attempt < 1 || attempt.RunnerID <= 0 ||
		strings.TrimSpace(attempt.RunnerName) == "" {
		return false, ErrGitHubJITState
	}
	// GitHub may omit ClaimKey from lifecycle messages. Runner ID is
	// provider-owned and unique within a scale set; validate that durable
	// identity first so a zero-request fallback can never authorize two
	// attempts or silently choose one.
	var identityCount, pickedUp int
	if err := queryer.QueryRowContext(ctx, `SELECT
			(
				SELECT count(*) FROM github_jit_attempts
				WHERE scale_set_id = ? AND runner_id = ?
					AND runner_name = ?
			),
			EXISTS (
				SELECT 1 FROM github_message_jobs
				WHERE scale_set_id = ?
					AND runner_request_id IN (0, ?)
					AND (
						event_type = 'JobStarted'
						OR (
							event_type = 'JobCompleted'
							AND result IN ('succeeded', 'failed')
						)
					)
					AND runner_id = ? AND runner_name = ?
			)`,
		attempt.ScaleSetID,
		attempt.RunnerID,
		attempt.RunnerName,
		attempt.ScaleSetID,
		attempt.ClaimKey,
		attempt.RunnerID,
		attempt.RunnerName,
	).Scan(&identityCount, &pickedUp); err != nil {
		return false, err
	}
	if identityCount != 1 {
		return false, ErrGitHubJITState
	}
	return pickedUp == 1, nil
}

func createGitHubUnpickedRequeueIntent(
	ctx context.Context,
	tx *sql.Tx,
	claim GitHubJobClaim,
	attempt GitHubJITAttempt,
	job GitHubJobEvent,
	sourceMessageID MessageID,
	sourceEventIndex int,
	controllerEpoch domain.ControllerEpoch,
	now int64,
) (GitHubUnpickedRequeueIntent, error) {
	readiness, currentAttempt, err := unpickedRunnerRequeueReadiness(
		ctx,
		tx,
		claim,
	)
	if err != nil || readiness != githubUnpickedRequeueReady ||
		currentAttempt != attempt {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return GitHubUnpickedRequeueIntent{}, err
	}
	if job.Type != GitHubJobAvailable ||
		job.RunnerRequestID != claim.ClaimKey ||
		job.ExecutionID == "" ||
		job.ExecutionID == claim.Execution.ID ||
		sourceMessageID == 0 ||
		sourceEventIndex < 0 ||
		controllerEpoch.Validate() != nil {
		return GitHubUnpickedRequeueIntent{}, ErrGitHubClaimState
	}
	currentAcquire, found, err := loadCurrentGitHubAcquireAttempt(
		ctx,
		tx,
		claim.ScaleSetID,
		claim.ClaimKey,
	)
	if err != nil || !found || currentAcquire.State != githubAcquireAcquired {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return GitHubUnpickedRequeueIntent{}, err
	}
	replacement := domain.ExecutionSnapshot{
		ID:       job.ExecutionID,
		TargetID: claim.Execution.TargetID,
		Slot:     claim.Execution.Slot,
		State:    domain.ExecutionReserved,
	}
	if err := replacement.Validate(); err != nil {
		return GitHubUnpickedRequeueIntent{}, err
	}
	var replacementExists int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT count(*) FROM executions WHERE id = ?`,
		replacement.ID,
	).Scan(&replacementExists); err != nil {
		return GitHubUnpickedRequeueIntent{}, err
	}
	if replacementExists != 0 {
		return GitHubUnpickedRequeueIntent{}, fmt.Errorf(
			"%w: requeue replacement execution identity already exists",
			ErrReplayMismatch,
		)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_unpicked_requeue_intents(
			scale_set_id, claim_key, jit_attempt, old_execution_id,
			replacement_execution_id, source_message_id, source_event_index,
			controller_epoch, created_at_unix_nano, updated_at_unix_nano
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		claim.ScaleSetID,
		claim.ClaimKey,
		attempt.Attempt,
		claim.Execution.ID,
		replacement.ID,
		sourceMessageID,
		sourceEventIndex,
		controllerEpoch,
		now,
		now,
	); err != nil {
		return GitHubUnpickedRequeueIntent{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_job_claims SET state = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ?
			AND execution_id = ? AND current_jit_attempt = ? AND state = ?`,
		GitHubClaimReconciliationRequired,
		now,
		now,
		claim.ScaleSetID,
		claim.ClaimKey,
		claim.Execution.ID,
		claim.CurrentAttempt,
		claim.State,
	)
	if err != nil {
		return GitHubUnpickedRequeueIntent{}, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return GitHubUnpickedRequeueIntent{}, ErrGitHubClaimState
	}
	claim.State = GitHubClaimReconciliationRequired
	return GitHubUnpickedRequeueIntent{
		Claim:             claim,
		Attempt:           attempt,
		Replacement:       replacement,
		SourceMessageID:   sourceMessageID,
		SourceEventIndex:  sourceEventIndex,
		ControllerEpoch:   controllerEpoch,
		CreatedAtUnixNano: now,
		UpdatedAtUnixNano: now,
	}, nil
}

func requireCompatibleGitHubRequeueEvent(
	ctx context.Context,
	queryer controllerAgentQueryer,
	intent GitHubUnpickedRequeueIntent,
	job GitHubJobEvent,
) error {
	if job.Type != GitHubJobAvailable ||
		job.RunnerRequestID != intent.Claim.ClaimKey {
		return ErrGitHubClaimState
	}
	var event GitHubJobEvent
	if err := queryer.QueryRowContext(ctx, `SELECT event_type, runner_request_id,
			runner_id, runner_name, result, repository_name, owner_name, job_id,
			workflow_run_id
		FROM github_message_jobs
		WHERE scale_set_id = ? AND message_id = ? AND event_index = ?`,
		intent.Claim.ScaleSetID,
		intent.SourceMessageID,
		intent.SourceEventIndex,
	).Scan(
		&event.Type,
		&event.RunnerRequestID,
		&event.RunnerID,
		&event.RunnerName,
		&event.Result,
		&event.RepositoryName,
		&event.OwnerName,
		&event.JobID,
		&event.WorkflowRunID,
	); err != nil {
		return err
	}
	if event.Type != GitHubJobAvailable ||
		event.RunnerRequestID != job.RunnerRequestID ||
		event.RepositoryName != job.RepositoryName ||
		event.OwnerName != job.OwnerName ||
		event.JobID != job.JobID ||
		event.WorkflowRunID != job.WorkflowRunID {
		return fmt.Errorf(
			"%w: fresh GitHub availability differs from pending requeue authority",
			ErrReplayMismatch,
		)
	}
	return nil
}

func loadGitHubUnpickedRequeueIntent(
	ctx context.Context,
	queryer controllerAgentQueryer,
	scaleSetID ScaleSetID,
	claimKey int64,
) (GitHubUnpickedRequeueIntent, bool, error) {
	var result GitHubUnpickedRequeueIntent
	var attemptNumber int
	var oldExecutionID domain.ExecutionID
	var replacementExecutionID domain.ExecutionID
	err := queryer.QueryRowContext(ctx, `SELECT jit_attempt, old_execution_id,
			replacement_execution_id, source_message_id, source_event_index,
			controller_epoch, created_at_unix_nano, updated_at_unix_nano
		FROM github_unpicked_requeue_intents
		WHERE scale_set_id = ? AND claim_key = ?`,
		scaleSetID,
		claimKey,
	).Scan(
		&attemptNumber,
		&oldExecutionID,
		&replacementExecutionID,
		&result.SourceMessageID,
		&result.SourceEventIndex,
		&result.ControllerEpoch,
		&result.CreatedAtUnixNano,
		&result.UpdatedAtUnixNano,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubUnpickedRequeueIntent{}, false, nil
	}
	if err != nil {
		return GitHubUnpickedRequeueIntent{}, false, err
	}
	claim, found, err := loadGitHubClaim(
		ctx,
		queryer,
		scaleSetID,
		claimKey,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return GitHubUnpickedRequeueIntent{}, false, err
	}
	attempt, found, err := loadGitHubJITAttempt(
		ctx,
		queryer,
		scaleSetID,
		claimKey,
		attemptNumber,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubJITState
		}
		return GitHubUnpickedRequeueIntent{}, false, err
	}
	replacement := domain.ExecutionSnapshot{
		ID:       replacementExecutionID,
		TargetID: claim.Execution.TargetID,
		Slot:     claim.Execution.Slot,
		State:    domain.ExecutionReserved,
	}
	if err := replacement.Validate(); err != nil {
		return GitHubUnpickedRequeueIntent{}, false, err
	}
	if claim.Execution.ID != oldExecutionID ||
		claim.State != GitHubClaimReconciliationRequired ||
		claim.CurrentAttempt != attemptNumber ||
		attempt.ScaleSetID != scaleSetID ||
		attempt.ClaimKey != claimKey ||
		(attempt.State != GitHubJITStarted &&
			attempt.State != GitHubJITRemovalPending) ||
		result.SourceMessageID == 0 ||
		result.SourceEventIndex < 0 ||
		result.ControllerEpoch.Validate() != nil ||
		result.CreatedAtUnixNano <= 0 ||
		result.UpdatedAtUnixNano < result.CreatedAtUnixNano {
		return GitHubUnpickedRequeueIntent{}, false, ErrGitHubClaimState
	}
	result.Claim = claim
	result.Attempt = attempt
	result.Replacement = replacement
	result.PickupProven, err = githubJITAttemptPickupProven(
		ctx,
		queryer,
		attempt,
	)
	if err != nil {
		return GitHubUnpickedRequeueIntent{}, false, err
	}
	return result, true, nil
}

func (s *ControllerStore) GitHubUnpickedRequeueIntent(
	ctx context.Context,
	scaleSetID ScaleSetID,
	claimKey int64,
) (GitHubUnpickedRequeueIntent, bool, error) {
	if err := s.requireReady(); err != nil {
		return GitHubUnpickedRequeueIntent{}, false, err
	}
	if scaleSetID == 0 || uint64(scaleSetID) > maxSQLiteInteger ||
		claimKey <= 0 ||
		uint64(claimKey) > maxSQLiteInteger {
		return GitHubUnpickedRequeueIntent{}, false, errors.New(
			"GitHub unpicked requeue identity is invalid",
		)
	}
	return loadGitHubUnpickedRequeueIntent(
		ctx,
		s.db,
		scaleSetID,
		claimKey,
	)
}

func lostJITClaimAwaitingAvailability(
	ctx context.Context,
	tx *sql.Tx,
	claim GitHubJobClaim,
) (bool, error) {
	if claim.State != GitHubClaimReconciliationRequired ||
		(claim.Execution.State != domain.ExecutionReleased &&
			claim.Execution.State != domain.ExecutionFailed) ||
		claim.CurrentAttempt < 1 {
		return false, nil
	}
	attempt, found, err := loadGitHubJITAttempt(
		ctx,
		tx,
		claim.ScaleSetID,
		claim.ClaimKey,
		claim.CurrentAttempt,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubJITState
		}
		return false, err
	}
	return attempt.State == GitHubJITReconciledAbsent &&
		attempt.RunnerID == 0 &&
		attempt.JITDigest == "" &&
		attempt.StartCommandID == "", nil
}

// rearmLostJITFromMessage is deliberately reachable only from a newly
// committed JobAvailable message whose poll authority is still current. Exact
// provider runner absence releases local capacity, but does not prove that the
// external job still wants a runner; this fresh message is that authority.
func rearmLostJITFromMessage(
	ctx context.Context,
	tx *sql.Tx,
	claim GitHubJobClaim,
	job GitHubJobEvent,
	evidenceMessageID MessageID,
	controllerEpoch domain.ControllerEpoch,
	now int64,
) (GitHubJobClaim, error) {
	if claim.State != GitHubClaimReconciliationRequired ||
		(claim.Execution.State != domain.ExecutionReleased &&
			claim.Execution.State != domain.ExecutionFailed) ||
		job.Type != GitHubJobAvailable ||
		job.RunnerRequestID != claim.ClaimKey ||
		job.ExecutionID == "" ||
		job.ExecutionID == claim.Execution.ID {
		return GitHubJobClaim{}, ErrGitHubClaimState
	}
	attempt, found, err := loadGitHubJITAttempt(
		ctx,
		tx,
		claim.ScaleSetID,
		claim.ClaimKey,
		claim.CurrentAttempt,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubJITState
		}
		return GitHubJobClaim{}, err
	}
	if attempt.State != GitHubJITReconciledAbsent ||
		attempt.RunnerID != 0 ||
		attempt.JITDigest != "" ||
		attempt.StartCommandID != "" {
		return GitHubJobClaim{}, ErrGitHubJITState
	}
	var oldReservations int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT count(*) FROM slot_reservations WHERE execution_id = ?`,
		claim.Execution.ID,
	).Scan(&oldReservations); err != nil {
		return GitHubJobClaim{}, err
	}
	if oldReservations != 0 {
		return GitHubJobClaim{}, errors.New(
			"dormant lost-JIT execution still owns a slot",
		)
	}
	nextExecution := domain.ExecutionSnapshot{
		ID:       job.ExecutionID,
		TargetID: claim.Execution.TargetID,
		Slot:     claim.Execution.Slot,
		State:    domain.ExecutionReserved,
	}
	if err := nextExecution.Validate(); err != nil {
		return GitHubJobClaim{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO executions(
		id, target_id, node_id, slot_index, state, created_at_unix_nano
	) VALUES (?, ?, ?, ?, ?, ?)`,
		nextExecution.ID,
		nextExecution.TargetID,
		nextExecution.Slot.NodeID,
		nextExecution.Slot.Index,
		nextExecution.State,
		now,
	); err != nil {
		return GitHubJobClaim{}, mapAssignmentError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO slot_reservations(
		node_id, slot_index, target_id, execution_id
	) VALUES (?, ?, ?, ?)`,
		nextExecution.Slot.NodeID,
		nextExecution.Slot.Index,
		nextExecution.TargetID,
		nextExecution.ID,
	); err != nil {
		return GitHubJobClaim{}, mapAssignmentError(err)
	}
	currentAcquire, found, err := loadCurrentGitHubAcquireAttempt(
		ctx,
		tx,
		claim.ScaleSetID,
		claim.ClaimKey,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return GitHubJobClaim{}, err
	}
	if currentAcquire.State != githubAcquireAcquired {
		return GitHubJobClaim{}, ErrGitHubClaimState
	}
	nextAcquire := currentAcquire.Attempt + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_acquire_attempts(
		scale_set_id, claim_key, attempt, evidence_message_id,
		controller_epoch, state, created_at_unix_nano, updated_at_unix_nano
	) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)`,
		claim.ScaleSetID,
		claim.ClaimKey,
		nextAcquire,
		evidenceMessageID,
		controllerEpoch,
		now,
		now,
	); err != nil {
		return GitHubJobClaim{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_job_claims SET
		source_message_id = ?, execution_id = ?, state = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ?
			AND execution_id = ? AND current_jit_attempt = ? AND state = ?`,
		evidenceMessageID,
		nextExecution.ID,
		GitHubClaimPending,
		now,
		now,
		claim.ScaleSetID,
		claim.ClaimKey,
		claim.Execution.ID,
		claim.CurrentAttempt,
		GitHubClaimReconciliationRequired,
	)
	if err != nil {
		return GitHubJobClaim{}, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return GitHubJobClaim{}, ErrGitHubClaimState
	}
	claim.SourceMessageID = evidenceMessageID
	claim.Execution = nextExecution
	claim.State = GitHubClaimPending
	return claim, nil
}

func githubPollClaimAuthorityCurrent(
	ctx context.Context,
	tx *sql.Tx,
	binding SingleSlotBinding,
	nowUnixNano int64,
) (bool, error) {
	authority := binding.PollAuthority
	if err := validateGitHubPollClaimAuthority(authority); err != nil {
		return false, err
	}
	if authority.Binding.TargetID != binding.TargetID ||
		authority.Agent.NodeID != binding.NodeID {
		return false, errors.New("GitHub poll authority binding changed")
	}
	current, err := readGitHubPollState(
		ctx,
		tx,
		authority.Binding,
		authority.Agent.NodeID,
	)
	if err != nil {
		if errors.Is(err, ErrRuntimeFreshnessBindingMissing) ||
			errors.Is(err, ErrRuntimeFreshnessBindingMismatch) {
			return false, nil
		}
		return false, err
	}
	currentAuthority := current.ClaimAuthority
	if currentAuthority.Binding != authority.Binding ||
		currentAuthority.ProfileRevision != authority.ProfileRevision ||
		currentAuthority.VersionPolicy != authority.VersionPolicy ||
		currentAuthority.RunnerVersion != authority.RunnerVersion ||
		currentAuthority.SessionTransitionGeneration !=
			authority.SessionTransitionGeneration ||
		currentAuthority.Agent != authority.Agent ||
		currentAuthority.ControllerEpoch != authority.ControllerEpoch ||
		current.Runtime.Session.Freshness != RuntimeFreshnessFresh {
		return false, nil
	}
	switch authority.VersionPolicy {
	case domain.RunnerVersionAutoUpdate:
		return true, nil
	case domain.RunnerVersionPinned:
		if currentAuthority.ReleaseGeneration != authority.ReleaseGeneration ||
			current.Runtime.Release.Freshness != RuntimeFreshnessFresh ||
			nowUnixNano >= authority.AdmissionDeadlineUnixNano {
			return false, nil
		}
		return true, nil
	default:
		return false, nil
	}
}

func rearmGitHubAcquireFromMessage(
	ctx context.Context,
	tx *sql.Tx,
	claim GitHubJobClaim,
	evidenceMessageID MessageID,
	controllerEpoch domain.ControllerEpoch,
	now int64,
) (GitHubJobClaim, error) {
	if claim.State != GitHubClaimAcquireAmbiguous {
		return claim, nil
	}
	current, found, err := loadCurrentGitHubAcquireAttempt(
		ctx,
		tx,
		claim.ScaleSetID,
		claim.ClaimKey,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return GitHubJobClaim{}, err
	}
	if current.State != githubAcquireDispatching {
		return GitHubJobClaim{}, ErrGitHubClaimState
	}
	nextAttempt := current.Attempt + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_acquire_attempts(
		scale_set_id, claim_key, attempt, evidence_message_id,
		controller_epoch, state, created_at_unix_nano, updated_at_unix_nano
	) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)`,
		claim.ScaleSetID,
		claim.ClaimKey,
		nextAttempt,
		evidenceMessageID,
		controllerEpoch,
		now,
		now,
	); err != nil {
		return GitHubJobClaim{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_job_claims SET state = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ? AND state = ?`,
		GitHubClaimPending,
		now,
		now,
		claim.ScaleSetID,
		claim.ClaimKey,
		GitHubClaimAcquireAmbiguous,
	)
	if err != nil {
		return GitHubJobClaim{}, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return GitHubJobClaim{}, ErrGitHubClaimState
	}
	claim.State = GitHubClaimPending
	return claim, nil
}

func validateGitHubAcquireAttempt(attempt GitHubAcquireAttempt) error {
	if attempt.ScaleSetID == 0 ||
		uint64(attempt.ScaleSetID) > maxSQLiteInteger ||
		attempt.ClaimKey <= 0 ||
		uint64(attempt.ClaimKey) > maxSQLiteInteger ||
		attempt.Attempt <= 0 ||
		attempt.EvidenceMessage == 0 ||
		uint64(attempt.EvidenceMessage) > maxSQLiteInteger ||
		attempt.ControllerEpoch.Validate() != nil {
		return errors.New("GitHub acquire attempt identity is invalid")
	}
	return nil
}

func loadCurrentGitHubAcquireAttempt(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	scaleSetID ScaleSetID,
	claimKey int64,
) (githubAcquireAttemptRecord, bool, error) {
	var record githubAcquireAttemptRecord
	err := queryer.QueryRowContext(ctx, `SELECT scale_set_id, claim_key,
		attempt, evidence_message_id, controller_epoch, state
		FROM github_acquire_attempts
		WHERE scale_set_id = ? AND claim_key = ?
		ORDER BY attempt DESC LIMIT 1`,
		scaleSetID,
		claimKey,
	).Scan(
		&record.ScaleSetID,
		&record.ClaimKey,
		&record.Attempt,
		&record.EvidenceMessage,
		&record.ControllerEpoch,
		&record.State,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return githubAcquireAttemptRecord{}, false, nil
	}
	if err != nil {
		return githubAcquireAttemptRecord{}, false, err
	}
	if err := validateGitHubAcquireAttempt(record.public()); err != nil {
		return githubAcquireAttemptRecord{}, false, err
	}
	switch record.State {
	case githubAcquirePending, githubAcquireReconciledPending,
		githubAcquireDispatching, githubAcquireAcquired:
	default:
		return githubAcquireAttemptRecord{}, false, errors.New("stored GitHub acquire attempt state is invalid")
	}
	return record, true, nil
}

func validateGitHubQueueMessage(message GitHubQueueMessage) error {
	if message.ScaleSetID == 0 || message.MessageID == 0 ||
		uint64(message.ScaleSetID) > maxSQLiteInteger ||
		uint64(message.MessageID) > maxSQLiteInteger ||
		!isLowerSHA256(message.Digest) {
		return errors.New("GitHub queue message identity is invalid")
	}
	for _, job := range message.Jobs {
		if job.RunnerRequestID < 0 || uint64(job.RunnerRequestID) > maxSQLiteInteger ||
			job.WorkflowRunID < 0 {
			return errors.New("GitHub job event identity is invalid")
		}
		switch job.Type {
		case GitHubJobAvailable:
			if job.RunnerRequestID == 0 ||
				job.RunnerID != 0 || job.RunnerName != "" ||
				job.Result != "" || job.ExecutionID == "" {
				return errors.New("available GitHub job event is invalid")
			}
		case GitHubJobAssigned:
			// Live GitHub omits ClaimKey on JobAssigned, and no durable
			// decision reads it: assignment is stored as evidence, while claims
			// key off JobAvailable and pickup off runner identity. Requiring one
			// made the store reject the same messages the adapter did.
			if job.RunnerID != 0 || job.RunnerName != "" || job.Result != "" {
				return errors.New("assigned GitHub job event is invalid")
			}
		case GitHubJobStarted:
			if job.RunnerID <= 0 || job.RunnerName == "" ||
				job.Result != "" {
				return errors.New("started GitHub job event is invalid")
			}
		case GitHubJobCompleted:
			if !validGitHubJobCompletionResult(job.Result) {
				return errors.New("completed GitHub job event is invalid")
			}
			// A cancellation that happened before any runner picked the job up
			// carries no runner identity. It is stored as evidence only: the
			// pickup query requires succeeded/failed plus an exact non-zero
			// runner ID and name, so this row can never become pickup proof.
			if job.Result == GitHubJobResultCanceled &&
				job.RunnerID == 0 && job.RunnerName == "" {
				break
			}
			if job.RunnerID <= 0 || job.RunnerName == "" {
				return errors.New("completed GitHub job event is invalid")
			}
		default:
			return errors.New("GitHub job event type is invalid")
		}
	}
	return nil
}

func validGitHubJobCompletionResult(result string) bool {
	switch result {
	case GitHubJobResultSucceeded,
		GitHubJobResultFailed,
		GitHubJobResultCanceled:
		return true
	default:
		return false
	}
}

// githubSlotAvailable is the single slot-availability predicate behind
// advertised capacity, the claim boundary, and fresh-recovery admission. The
// node-owner exclusion clause lives here precisely so all three stay consistent
// by construction instead of by three parallel checks.
func githubSlotAvailable(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, binding SingleSlotBinding) (bool, error) {
	var count int
	if err := queryer.QueryRowContext(ctx, `SELECT count(*)
		FROM node_administrative_states a
			WHERE a.node_id = ? AND a.administrative_state = 'active'
				AND NOT EXISTS (
					SELECT 1 FROM slot_reservations r
					WHERE r.node_id = a.node_id AND r.slot_index = ?
				)
				AND NOT EXISTS (
					SELECT 1 FROM github_unpicked_requeue_intents intent
					JOIN executions old_execution
						ON old_execution.id = intent.old_execution_id
					WHERE old_execution.node_id = a.node_id
						AND old_execution.slot_index = ?
				)
				AND NOT EXISTS (
					SELECT 1 FROM node_target_exclusions x
					WHERE x.node_id = a.node_id AND x.target_id = ?
				)`,
		binding.NodeID, binding.Slot, binding.Slot, binding.TargetID,
	).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func requireActiveGitHubClaimLease(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	claim GitHubJobClaim,
) error {
	if err := claim.Execution.Validate(); err != nil {
		return ErrGitHubClaimState
	}
	var count int
	if err := queryer.QueryRowContext(ctx, `SELECT count(*)
		FROM node_administrative_states a
		JOIN slot_reservations r
			ON r.node_id = a.node_id
			AND r.slot_index = ?
			AND r.target_id = ?
			AND r.execution_id = ?
		WHERE a.node_id = ? AND a.administrative_state = 'active'`,
		claim.Execution.Slot.Index,
		claim.Execution.TargetID,
		claim.Execution.ID,
		claim.Execution.Slot.NodeID,
	).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return ErrGitHubClaimState
	}
	return nil
}

func (s *ControllerStore) GitHubSingleSlotCapacity(ctx context.Context, binding SingleSlotBinding) (int, error) {
	if err := s.requireReady(); err != nil {
		return 0, err
	}
	if binding.TargetID == "" || binding.NodeID == "" || binding.Slot != 0 {
		return 0, errors.New("single-slot GitHub binding must name target, node, and slot zero")
	}
	free, err := githubSlotAvailable(ctx, s.db, binding)
	if err != nil || !free {
		return 0, err
	}
	return 1, nil
}

func (s *ControllerStore) NextActionableGitHubClaim(ctx context.Context, scaleSetID ScaleSetID) (GitHubJobClaim, bool, error) {
	if err := s.requireReady(); err != nil {
		return GitHubJobClaim{}, false, err
	}
	if scaleSetID == 0 || uint64(scaleSetID) > maxSQLiteInteger {
		return GitHubJobClaim{}, false, errors.New("GitHub scale set ID is invalid")
	}
	row := s.db.QueryRowContext(ctx, githubClaimSelect+`
		WHERE c.scale_set_id = ? AND c.state IN ('pending', 'acquired', 'preparing')
		ORDER BY c.created_at_unix_nano, c.claim_key LIMIT 1`, scaleSetID)
	claim, err := scanGitHubClaim(row)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubJobClaim{}, false, nil
	}
	return claim, err == nil, err
}

// GitHubPendingClaimDispatchReady distinguishes a normal Pending claim whose
// source message may still need commit-before-ack redelivery from a replacement
// claim created only after the source message was ACKed and the old provider
// registration passed every absence fence. Only the latter may drive before
// the first post-restart long poll.
func (s *ControllerStore) GitHubPendingClaimDispatchReady(
	ctx context.Context,
	claim GitHubJobClaim,
) (bool, error) {
	if err := s.requireReady(); err != nil {
		return false, err
	}
	if claim.ScaleSetID == 0 ||
		uint64(claim.ScaleSetID) > maxSQLiteInteger ||
		claim.ClaimKey <= 0 ||
		uint64(claim.ClaimKey) > maxSQLiteInteger ||
		claim.SourceMessageID == 0 ||
		claim.State != GitHubClaimPending ||
		claim.Execution.State != domain.ExecutionReserved ||
		claim.CurrentAttempt < 0 {
		return false, ErrGitHubClaimState
	}
	if err := claim.Execution.Validate(); err != nil {
		return false, ErrGitHubClaimState
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	current, found, err := loadGitHubClaim(
		ctx,
		tx,
		claim.ScaleSetID,
		claim.ClaimKey,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return false, err
	}
	if current != claim {
		return false, ErrGitHubClaimState
	}
	acquire, found, err := loadCurrentGitHubAcquireAttempt(
		ctx,
		tx,
		claim.ScaleSetID,
		claim.ClaimKey,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return false, err
	}
	if acquire.EvidenceMessage != claim.SourceMessageID {
		return false, ErrGitHubClaimState
	}
	switch acquire.State {
	case githubAcquirePending:
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	case githubAcquireReconciledPending:
	default:
		return false, ErrGitHubClaimState
	}
	if acquire.Attempt < 2 {
		return false, ErrGitHubClaimState
	}
	if claim.CurrentAttempt < 1 {
		return false, ErrGitHubClaimState
	}
	attempt, found, err := loadGitHubJITAttempt(
		ctx,
		tx,
		claim.ScaleSetID,
		claim.ClaimKey,
		claim.CurrentAttempt,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubJITState
		}
		return false, err
	}
	if attempt.State != GitHubJITReconciledAbsent ||
		attempt.RunnerID != 0 ||
		attempt.JITDigest != "" ||
		attempt.StartCommandID != "" {
		return false, ErrGitHubJITState
	}
	var sourceAvailable int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM github_message_jobs
			WHERE scale_set_id = ? AND message_id = ?
				AND event_type = 'JobAvailable' AND runner_request_id = ?
		)`,
		claim.ScaleSetID,
		claim.SourceMessageID,
		claim.ClaimKey,
	).Scan(&sourceAvailable); err != nil {
		return false, err
	}
	if sourceAvailable != 1 {
		return false, ErrGitHubClaimState
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *ControllerStore) GitHubClaim(ctx context.Context, scaleSetID ScaleSetID, claimKey int64) (GitHubJobClaim, bool, error) {
	if err := s.requireReady(); err != nil {
		return GitHubJobClaim{}, false, err
	}
	return loadGitHubClaim(ctx, s.db, scaleSetID, claimKey)
}

func (s *ControllerStore) BeginGitHubAcquire(
	ctx context.Context,
	scaleSetID ScaleSetID,
	claimKey int64,
) (GitHubAcquireAttempt, error) {
	if err := s.requireReady(); err != nil {
		return GitHubAcquireAttempt{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return GitHubAcquireAttempt{}, err
	}
	defer tx.Rollback()
	claim, found, err := loadGitHubClaim(ctx, tx, scaleSetID, claimKey)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return GitHubAcquireAttempt{}, err
	}
	if claim.State != GitHubClaimPending ||
		claim.Execution.State != domain.ExecutionReserved {
		return GitHubAcquireAttempt{}, ErrGitHubClaimState
	}
	if err := requireActiveGitHubClaimLease(ctx, tx, claim); err != nil {
		return GitHubAcquireAttempt{}, err
	}
	attempt, found, err := loadCurrentGitHubAcquireAttempt(
		ctx,
		tx,
		scaleSetID,
		claimKey,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return GitHubAcquireAttempt{}, err
	}
	if attempt.State != githubAcquirePending &&
		attempt.State != githubAcquireReconciledPending {
		return GitHubAcquireAttempt{}, ErrGitHubClaimState
	}
	pendingState := attempt.State
	currentEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return GitHubAcquireAttempt{}, err
	}
	attempt.ControllerEpoch = domain.ControllerEpoch(currentEpoch)
	now, err := storeUnixNano(s.now())
	if err != nil {
		return GitHubAcquireAttempt{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_acquire_attempts SET
		state = 'dispatching', controller_epoch = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?
			AND evidence_message_id = ? AND state = ?`,
		attempt.ControllerEpoch,
		now,
		now,
		attempt.ScaleSetID,
		attempt.ClaimKey,
		attempt.Attempt,
		attempt.EvidenceMessage,
		pendingState,
	)
	if err != nil {
		return GitHubAcquireAttempt{}, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return GitHubAcquireAttempt{}, ErrGitHubClaimState
	}
	result, err = tx.ExecContext(ctx, `UPDATE github_job_claims SET state = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ? AND state = ?`,
		GitHubClaimAcquireAmbiguous,
		now,
		now,
		scaleSetID,
		claimKey,
		GitHubClaimPending,
	)
	if err != nil {
		return GitHubAcquireAttempt{}, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return GitHubAcquireAttempt{}, ErrGitHubClaimState
	}
	if err := tx.Commit(); err != nil {
		return GitHubAcquireAttempt{}, err
	}
	return attempt.public(), nil
}

func (s *ControllerStore) MarkGitHubAcquired(
	ctx context.Context,
	attempt GitHubAcquireAttempt,
) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if err := validateGitHubAcquireAttempt(attempt); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	currentEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return err
	}
	if domain.ControllerEpoch(currentEpoch) != attempt.ControllerEpoch {
		return ErrStaleControllerEpoch
	}
	current, found, err := loadCurrentGitHubAcquireAttempt(
		ctx,
		tx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return err
	}
	if current.public() != attempt {
		return ErrGitHubClaimState
	}
	claim, found, err := loadGitHubClaim(
		ctx,
		tx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return err
	}
	if current.State == githubAcquireAcquired &&
		claim.State == GitHubClaimAcquired {
		return tx.Commit()
	}
	if current.State != githubAcquireDispatching ||
		claim.State != GitHubClaimAcquireAmbiguous {
		return ErrGitHubClaimState
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_acquire_attempts SET
		state = 'acquired',
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?
			AND evidence_message_id = ? AND controller_epoch = ?
			AND state = 'dispatching'`,
		now,
		now,
		attempt.ScaleSetID,
		attempt.ClaimKey,
		attempt.Attempt,
		attempt.EvidenceMessage,
		attempt.ControllerEpoch,
	)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrGitHubClaimState
	}
	result, err = tx.ExecContext(ctx, `UPDATE github_job_claims SET state = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ? AND state = ?`,
		GitHubClaimAcquired,
		now,
		now,
		attempt.ScaleSetID,
		attempt.ClaimKey,
		GitHubClaimAcquireAmbiguous,
	)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrGitHubClaimState
	}
	return tx.Commit()
}

func (s *ControllerStore) MarkGitHubPreparing(ctx context.Context, scaleSetID ScaleSetID, claimKey int64) error {
	return s.transitionGitHubClaimWithExecution(ctx, scaleSetID, claimKey,
		[]GitHubClaimState{GitHubClaimAcquired}, GitHubClaimPreparing,
		domain.ExecutionPreparing)
}

func (s *ControllerStore) MarkGitHubPrepareFailed(ctx context.Context, scaleSetID ScaleSetID, claimKey int64) error {
	return s.transitionGitHubClaimWithExecution(ctx, scaleSetID, claimKey,
		[]GitHubClaimState{GitHubClaimAcquired}, GitHubClaimPrepareFailed,
		domain.ExecutionFailed)
}

func (s *ControllerStore) BeginGitHubJITAttempt(
	ctx context.Context,
	scaleSetID ScaleSetID,
	claimKey int64,
	controllerEpoch domain.ControllerEpoch,
	runnerName string,
) (GitHubJITAttempt, bool, error) {
	if err := s.requireReady(); err != nil {
		return GitHubJITAttempt{}, false, err
	}
	if runnerName == "" || controllerEpoch.Validate() != nil {
		return GitHubJITAttempt{}, false, errors.New("GitHub JIT runner name is required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return GitHubJITAttempt{}, false, err
	}
	defer tx.Rollback()
	currentEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return GitHubJITAttempt{}, false, err
	}
	if domain.ControllerEpoch(currentEpoch) != controllerEpoch {
		return GitHubJITAttempt{}, false, ErrStaleControllerEpoch
	}
	claim, found, err := loadGitHubClaim(ctx, tx, scaleSetID, claimKey)
	if err != nil || !found {
		if err == nil {
			err = sql.ErrNoRows
		}
		return GitHubJITAttempt{}, false, err
	}
	if claim.State != GitHubClaimPreparing {
		if claim.CurrentAttempt > 0 {
			attempt, attemptFound, attemptErr := loadGitHubJITAttempt(ctx, tx, scaleSetID, claimKey, claim.CurrentAttempt)
			if attemptErr != nil {
				return GitHubJITAttempt{}, false, attemptErr
			}
			if attemptFound {
				if attempt.RunnerName != runnerName {
					return GitHubJITAttempt{}, false, fmt.Errorf("%w: GitHub JIT runner name changed", ErrReplayMismatch)
				}
				return attempt, true, nil
			}
		}
		return GitHubJITAttempt{}, false, ErrGitHubClaimState
	}
	if claim.Execution.State != domain.ExecutionPreparing {
		return GitHubJITAttempt{}, false, ErrGitHubClaimState
	}
	if err := requireActiveGitHubClaimLease(ctx, tx, claim); err != nil {
		return GitHubJITAttempt{}, false, err
	}
	attemptNumber := claim.CurrentAttempt + 1
	now, err := storeUnixNano(s.now())
	if err != nil {
		return GitHubJITAttempt{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_jit_attempts(
		scale_set_id, claim_key, attempt, controller_epoch, runner_name, state, runner_id,
		jit_digest, start_command_id, created_at_unix_nano, updated_at_unix_nano
	) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, '', ?, ?)`, scaleSetID,
		claimKey, attemptNumber, controllerEpoch, runnerName, GitHubJITIntent, now, now); err != nil {
		return GitHubJITAttempt{}, false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_job_claims SET
		state = ?, current_jit_attempt = ?, updated_at_unix_nano = ?
		WHERE scale_set_id = ? AND claim_key = ? AND state = ?`,
		GitHubClaimJITIntent, attemptNumber, now, scaleSetID,
		claimKey, GitHubClaimPreparing)
	if err != nil {
		return GitHubJITAttempt{}, false, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return GitHubJITAttempt{}, false, ErrGitHubClaimState
	}
	if err := tx.Commit(); err != nil {
		return GitHubJITAttempt{}, false, err
	}
	return GitHubJITAttempt{
		ScaleSetID: scaleSetID, ClaimKey: claimKey,
		Attempt: attemptNumber, ControllerEpoch: controllerEpoch,
		RunnerName: runnerName, State: GitHubJITIntent,
	}, false, nil
}

func (s *ControllerStore) MarkGitHubJITGenerationAmbiguous(ctx context.Context, attempt GitHubJITAttempt) error {
	return s.transitionGitHubJIT(ctx, attempt, GitHubJITIntent,
		GitHubJITGenerationAmbiguous, GitHubClaimJITGenerationAmbiguous, 0, "", "", false)
}

func (s *ControllerStore) MarkGitHubJITGenerated(
	ctx context.Context,
	attempt GitHubJITAttempt,
	runnerID int,
	jitDigest string,
	startCommandID domain.CommandID,
) error {
	if runnerID <= 0 || !isLowerSHA256(jitDigest) || startCommandID == "" {
		return errors.New("generated GitHub JIT identity is invalid")
	}
	return s.transitionGitHubJIT(ctx, attempt, GitHubJITIntent,
		GitHubJITGenerated, GitHubClaimJITGenerated, runnerID, jitDigest, startCommandID, false)
}

func (s *ControllerStore) BeginGitHubStartDispatch(ctx context.Context, attempt GitHubJITAttempt) error {
	return s.transitionGitHubJIT(ctx, attempt, GitHubJITGenerated,
		GitHubJITStartDispatching, GitHubClaimStartDispatching,
		attempt.RunnerID, attempt.JITDigest, attempt.StartCommandID, true)
}

func (s *ControllerStore) MarkGitHubStartAmbiguous(ctx context.Context, attempt GitHubJITAttempt) error {
	return s.transitionGitHubJIT(ctx, attempt, GitHubJITStartDispatching,
		GitHubJITStartAmbiguous, GitHubClaimStartAmbiguous,
		attempt.RunnerID, attempt.JITDigest, attempt.StartCommandID, false)
}

func (s *ControllerStore) MarkGitHubRunning(ctx context.Context, attempt GitHubJITAttempt) error {
	return s.markGitHubStarted(ctx, attempt)
}

func (s *ControllerStore) MarkGitHubJITAgentAccepted(
	ctx context.Context,
	attempt GitHubJITAttempt,
	reconciliationEpoch domain.ControllerEpoch,
	snapshotDigest string,
) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, claim, err := loadGitHubJITReconciliationContext(
		ctx,
		tx,
		attempt,
		reconciliationEpoch,
		[]GitHubJITAttemptState{
			GitHubJITStartDispatching,
			GitHubJITStartAmbiguous,
			GitHubJITAgentAccepted,
		},
		false,
	)
	if err != nil {
		return err
	}
	if !sameGitHubJITIdentity(current, attempt) {
		return ErrGitHubJITState
	}
	if err := requireCurrentAgentSnapshotAuthority(
		ctx,
		tx,
		claim.Execution.Slot.NodeID,
		snapshotDigest,
		reconciliationEpoch,
	); err != nil {
		return err
	}
	accepted, err := exactCurrentAgentStartAccepted(
		ctx,
		tx,
		current,
		claim,
		snapshotDigest,
	)
	if err != nil {
		return err
	}
	if !accepted {
		return errors.New("current Agent snapshot does not contain the exact issued Start")
	}
	if err := requireNoAdvancedCurrentAgentObservation(
		ctx,
		tx,
		claim,
		snapshotDigest,
	); err != nil {
		return err
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	if current.State != GitHubJITAgentAccepted {
		if err := transitionExactGitHubJITAndClaim(
			ctx,
			tx,
			current,
			claim,
			GitHubJITAgentAccepted,
			GitHubClaimReconciliationRequired,
			current.RunnerID,
			current.JITDigest,
			current.StartCommandID,
			now,
		); err != nil {
			return err
		}
	}
	if err := upsertGitHubJITSnapshotAuthority(
		ctx,
		tx,
		current,
		snapshotDigest,
		reconciliationEpoch,
		"agent_accepted",
		0,
		now,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// ReconcileGitHubJITPrunedHistory handles reconnect after an Agent has
// acknowledged a terminal update and pruned the exact Start command and
// execution observation from its current snapshot. Historical rows are accepted
// only when the current authenticated snapshot has no competing membership for
// the execution.
func (s *ControllerStore) ReconcileGitHubJITPrunedHistory(
	ctx context.Context,
	attempt GitHubJITAttempt,
	reconciliationEpoch domain.ControllerEpoch,
	snapshotDigest string,
) (GitHubJITPrunedHistoryResult, error) {
	if err := s.requireReady(); err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	defer tx.Rollback()
	current, claim, err := loadGitHubJITReconciliationContext(
		ctx,
		tx,
		attempt,
		reconciliationEpoch,
		[]GitHubJITAttemptState{
			GitHubJITStartDispatching,
			GitHubJITStartAmbiguous,
			GitHubJITAgentAccepted,
			GitHubJITStarted,
		},
		false,
	)
	if err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	if !sameGitHubJITIdentity(current, attempt) {
		return GitHubJITPrunedHistoryResult{}, ErrGitHubJITState
	}
	if err := requireCurrentAgentSnapshotAuthority(
		ctx,
		tx,
		claim.Execution.Slot.NodeID,
		snapshotDigest,
		reconciliationEpoch,
	); err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	accepted, err := exactCurrentAgentStartAccepted(
		ctx,
		tx,
		current,
		claim,
		snapshotDigest,
	)
	if err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	if accepted {
		return GitHubJITPrunedHistoryResult{}, nil
	}
	started, err := exactStartHasDurableRuntimeHistory(
		ctx,
		tx,
		current,
		claim,
	)
	if err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	terminalState, terminalFound, err := exactStartDurableTerminalUpdate(
		ctx,
		tx,
		current,
		claim,
	)
	if err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	if !started && !terminalFound {
		return GitHubJITPrunedHistoryResult{}, nil
	}
	if err := requirePrunedAgentExecutionMembership(
		ctx,
		tx,
		claim,
		snapshotDigest,
	); err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	if terminalFound && claim.Execution.State != terminalState {
		return GitHubJITPrunedHistoryResult{}, errors.New(
			"pruned exact terminal Start update conflicts with desired execution",
		)
	}
	if !started {
		return GitHubJITPrunedHistoryResult{LostTerminal: terminalState}, nil
	}
	if current.State == GitHubJITStarted &&
		claim.State == GitHubClaimRunning {
		return GitHubJITPrunedHistoryResult{Started: true}, tx.Commit()
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	if err := transitionExactGitHubJITAndClaim(
		ctx,
		tx,
		current,
		claim,
		GitHubJITStarted,
		GitHubClaimRunning,
		current.RunnerID,
		current.JITDigest,
		current.StartCommandID,
		now,
	); err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM github_jit_snapshot_authority
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		current.ScaleSetID,
		current.ClaimKey,
		current.Attempt,
	); err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return GitHubJITPrunedHistoryResult{}, err
	}
	return GitHubJITPrunedHistoryResult{Started: true}, nil
}

// MarkGitHubJITObservedStarted resolves a start-dispatch ambiguity only when
// the exact issued Start, current Agent command membership, current observation,
// desired-state advance, and JIT fence transition all commit atomically.
func (s *ControllerStore) MarkGitHubJITObservedStarted(
	ctx context.Context,
	attempt GitHubJITAttempt,
	reconciliationEpoch domain.ControllerEpoch,
	observation ObservationSnapshot,
	snapshotDigest string,
) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	switch observation.State {
	case domain.ExecutionRunning, domain.ExecutionCleaning,
		domain.ExecutionReleased, domain.ExecutionFailed,
		domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
	default:
		return errors.New("Agent observation does not prove that the JIT runner started")
	}
	if observation.ExecutionID == "" || observation.ObservedAtUnixNano <= 0 {
		return errors.New("Agent start observation identity is invalid")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	currentEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return err
	}
	if domain.ControllerEpoch(currentEpoch) != reconciliationEpoch ||
		reconciliationEpoch <= attempt.ControllerEpoch {
		return ErrStaleControllerEpoch
	}
	current, claim, err := loadGitHubJITReconciliationContext(
		ctx,
		tx,
		attempt,
		reconciliationEpoch,
		[]GitHubJITAttemptState{
			GitHubJITStartDispatching,
			GitHubJITStartAmbiguous,
			GitHubJITAgentAccepted,
			GitHubJITStarted,
			GitHubJITRemovalPending,
		},
		false,
	)
	if err != nil {
		return err
	}
	if !sameGitHubJITIdentity(current, attempt) {
		return ErrGitHubJITState
	}
	if claim.Execution.ID != observation.ExecutionID {
		return ErrGitHubClaimState
	}
	if err := requireCurrentAgentSnapshotAuthority(
		ctx,
		tx,
		claim.Execution.Slot.NodeID,
		snapshotDigest,
		reconciliationEpoch,
	); err != nil {
		return err
	}
	accepted, err := exactCurrentAgentStartAccepted(
		ctx,
		tx,
		current,
		claim,
		snapshotDigest,
	)
	if err != nil {
		return err
	}
	if !accepted {
		return errors.New("current Agent snapshot does not contain the exact issued Start")
	}
	var persisted ObservationSnapshot
	if err := tx.QueryRowContext(ctx, `SELECT execution_id, state,
		agent_observed_at_unix_nano
		FROM agent_current_snapshot_observations
		WHERE node_id = ? AND execution_id = ? AND snapshot_digest = ?`,
		claim.Execution.Slot.NodeID, observation.ExecutionID, snapshotDigest,
	).Scan(&persisted.ExecutionID, &persisted.State,
		&persisted.ObservedAtUnixNano); err != nil {
		return err
	}
	if persisted != observation {
		return fmt.Errorf("%w: Agent start observation differs from current snapshot authority", ErrReplayMismatch)
	}
	terminalObservation := lostJITTerminalState(observation.State)
	terminalUpdateDurable := false
	if terminalObservation {
		startProven, err := exactStartHasDurableRuntimeHistory(
			ctx,
			tx,
			current,
			claim,
		)
		if err != nil {
			return err
		}
		terminalState, found, err := exactStartDurableTerminalUpdate(
			ctx,
			tx,
			current,
			claim,
		)
		if err != nil {
			return err
		}
		if !found {
			if !startProven {
				// Reconnect snapshots arrive before pending Agent outbox
				// updates. The snapshot alone cannot distinguish a genuinely
				// lost JIT from a fast runner whose Running update is still in
				// flight, so provider cleanup must wait for exact durable
				// history.
				return ErrGitHubJITTerminalPending
			}
			// Running/Cleaning already proves the Start. Only close the JIT
			// ambiguity here; the later terminal outbox update exclusively owns
			// terminal desired-state and lease mutation. Snapshot cleanup
			// evidence may independently quarantine the Node earlier.
		} else {
			terminalUpdateDurable = true
			if terminalState != observation.State ||
				claim.Execution.State != terminalState {
				return errors.New(
					"exact terminal Start update conflicts with current Agent snapshot",
				)
			}
		}
		// A terminal update with no exact Running/Cleaning history is startup
		// recovery after accepted Start but before JIT delivery. It must use
		// lost-JIT provider cleanup and must never be labelled Running.
		if terminalUpdateDurable && !startProven {
			return ErrGitHubJITStartNotProven
		}
		if current.State == GitHubJITRemovalPending {
			// Provider deletion may already have crossed the process boundary.
			// A late start proof contradicts that fence and must not revive it.
			return ErrGitHubJITState
		}
	} else if current.State == GitHubJITRemovalPending {
		return ErrGitHubJITState
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	if claim.Execution.State != observation.State && !terminalObservation {
		if !domain.CanReachExecutionState(claim.Execution.State, observation.State) {
			return errors.New("Agent start observation cannot advance desired execution")
		}
		result, err := tx.ExecContext(ctx, `UPDATE executions SET state = ?
			WHERE id = ? AND state = ?`,
			observation.State,
			claim.Execution.ID,
			claim.Execution.State,
		)
		if err != nil {
			return err
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return errors.New("Agent start observation lost desired-state compare-and-swap")
		}
		claim.Execution.State = observation.State
	}
	nextClaimState := GitHubClaimRunning
	if observation.State == domain.ExecutionCleaning {
		nextClaimState = GitHubClaimReconciliationRequired
	}
	if current.State != GitHubJITStarted || claim.State != nextClaimState {
		if err := transitionExactGitHubJITAndClaim(
			ctx,
			tx,
			current,
			claim,
			GitHubJITStarted,
			nextClaimState,
			current.RunnerID,
			current.JITDigest,
			current.StartCommandID,
			now,
		); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM github_jit_snapshot_authority
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		current.ScaleSetID,
		current.ClaimKey,
		current.Attempt,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *ControllerStore) MarkGitHubJITRemovalPending(
	ctx context.Context,
	attempt GitHubJITAttempt,
	reconciliationEpoch domain.ControllerEpoch,
	snapshotDigest string,
	githubSessionGeneration uint64,
	providerAbsent bool,
) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, claim, err := loadGitHubJITReconciliationContext(
		ctx,
		tx,
		attempt,
		reconciliationEpoch,
		[]GitHubJITAttemptState{
			GitHubJITIntent,
			GitHubJITGenerationAmbiguous,
			GitHubJITGenerated,
			GitHubJITStartDispatching,
			GitHubJITStartAmbiguous,
			GitHubJITAgentAccepted,
			GitHubJITStarted,
			GitHubJITRemovalPending,
		},
		true,
	)
	if err != nil {
		return err
	}
	if reconciliationEpoch == attempt.ControllerEpoch {
		if _, found, err := loadGitHubUnpickedRequeueIntent(
			ctx,
			tx,
			current.ScaleSetID,
			current.ClaimKey,
		); err != nil {
			return err
		} else if !found {
			return ErrStaleControllerEpoch
		}
	}
	if err := requireGitHubSessionTransitionGeneration(
		ctx,
		tx,
		current.ScaleSetID,
		githubSessionGeneration,
	); err != nil {
		return err
	}
	if err := requireCurrentAgentSnapshotAuthority(
		ctx,
		tx,
		claim.Execution.Slot.NodeID,
		snapshotDigest,
		reconciliationEpoch,
	); err != nil {
		return err
	}
	_, removalKind, err := classifyGitHubRunnerRemovalAuthority(
		ctx,
		tx,
		current,
		claim,
		snapshotDigest,
	)
	if err != nil {
		return err
	}
	if reconciliationEpoch == attempt.ControllerEpoch &&
		removalKind != githubRunnerRemovalUnpickedRequeue {
		return ErrStaleControllerEpoch
	}
	if removalKind == githubRunnerRemovalUnpickedRequeue && !providerAbsent {
		intent, found, err := loadGitHubUnpickedRequeueIntent(
			ctx,
			tx,
			current.ScaleSetID,
			current.ClaimKey,
		)
		if err != nil || !found {
			if err == nil {
				err = ErrGitHubClaimState
			}
			return err
		}
		if intent.PickupProven {
			return errors.New(
				"exact GitHub pickup evidence forbids deleting the terminal runner",
			)
		}
	}
	runnerID := current.RunnerID
	switch current.State {
	case GitHubJITIntent, GitHubJITGenerationAmbiguous:
		if current.RunnerID != 0 || current.JITDigest != "" ||
			current.StartCommandID != "" || attempt.RunnerID <= 0 ||
			attempt.JITDigest != "" || attempt.StartCommandID != "" {
			return ErrGitHubJITState
		}
		runnerID = attempt.RunnerID
	default:
		if !sameGitHubJITIdentity(current, attempt) {
			return ErrGitHubJITState
		}
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	if current.State != GitHubJITRemovalPending {
		if err := transitionExactGitHubJITAndClaim(
			ctx,
			tx,
			current,
			claim,
			GitHubJITRemovalPending,
			GitHubClaimReconciliationRequired,
			runnerID,
			current.JITDigest,
			current.StartCommandID,
			now,
		); err != nil {
			return err
		}
	}
	current.RunnerID = runnerID
	decision := "runner_removal_issued"
	if providerAbsent {
		decision = "runner_absence_pending"
	}
	switch removalKind {
	case githubRunnerRemovalLostJIT:
		decision = "lost_jit_removal_issued"
		if providerAbsent {
			decision = "lost_jit_absence_pending"
		}
	case githubRunnerRemovalUnpickedRequeue:
		decision = "unpicked_requeue_removal_issued"
		if providerAbsent {
			decision = "unpicked_requeue_absence_pending"
		}
	}
	if err := upsertGitHubJITSnapshotAuthority(
		ctx,
		tx,
		current,
		snapshotDigest,
		reconciliationEpoch,
		decision,
		githubSessionGeneration,
		now,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *ControllerStore) MarkGitHubJITReconciledAbsent(
	ctx context.Context,
	attempt GitHubJITAttempt,
	reconciliationEpoch domain.ControllerEpoch,
	snapshotDigest string,
	githubSessionGeneration uint64,
) (GitHubJITAbsenceResult, error) {
	if err := s.requireReady(); err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	defer tx.Rollback()
	current, claim, err := loadGitHubJITReconciliationContext(
		ctx,
		tx,
		attempt,
		reconciliationEpoch,
		[]GitHubJITAttemptState{
			GitHubJITIntent,
			GitHubJITGenerationAmbiguous,
			GitHubJITRemovalPending,
		},
		true,
	)
	if err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	if reconciliationEpoch == attempt.ControllerEpoch {
		if _, found, err := loadGitHubUnpickedRequeueIntent(
			ctx,
			tx,
			current.ScaleSetID,
			current.ClaimKey,
		); err != nil {
			return GitHubJITAbsenceResult{}, err
		} else if !found {
			return GitHubJITAbsenceResult{}, ErrStaleControllerEpoch
		}
	}
	if err := requireGitHubSessionTransitionGeneration(
		ctx,
		tx,
		current.ScaleSetID,
		githubSessionGeneration,
	); err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	if !sameGitHubJITIdentity(current, attempt) {
		return GitHubJITAbsenceResult{}, ErrGitHubJITState
	}
	if err := requireCurrentAgentSnapshotAuthority(
		ctx,
		tx,
		claim.Execution.Slot.NodeID,
		snapshotDigest,
		reconciliationEpoch,
	); err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	terminalState, removalKind, err := classifyGitHubRunnerRemovalAuthority(
		ctx,
		tx,
		current,
		claim,
		snapshotDigest,
	)
	if err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	if reconciliationEpoch == attempt.ControllerEpoch &&
		removalKind != githubRunnerRemovalUnpickedRequeue {
		return GitHubJITAbsenceResult{}, ErrStaleControllerEpoch
	}
	lostJIT := removalKind == githubRunnerRemovalLostJIT
	unpickedRequeue := removalKind == githubRunnerRemovalUnpickedRequeue
	var decision string
	var authorityDigest string
	var authorityEpoch domain.ControllerEpoch
	var authoritySessionGeneration uint64
	var firstObservedAt int64
	authorityErr := tx.QueryRowContext(ctx, `SELECT decision, snapshot_digest,
		controller_epoch, github_session_generation, updated_at_unix_nano
		FROM github_jit_snapshot_authority
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		current.ScaleSetID,
		current.ClaimKey,
		current.Attempt,
	).Scan(
		&decision,
		&authorityDigest,
		&authorityEpoch,
		&authoritySessionGeneration,
		&firstObservedAt,
	)
	if authorityErr != nil && !errors.Is(authorityErr, sql.ErrNoRows) {
		return GitHubJITAbsenceResult{}, authorityErr
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	if current.State == GitHubJITIntent ||
		current.State == GitHubJITGenerationAmbiguous {
		if current.RunnerID != 0 || current.JITDigest != "" ||
			current.StartCommandID != "" {
			return GitHubJITAbsenceResult{}, ErrGitHubJITState
		}
		if errors.Is(authorityErr, sql.ErrNoRows) ||
			decision != "generation_absence_pending" ||
			authorityDigest != snapshotDigest ||
			authorityEpoch != reconciliationEpoch {
			if err := upsertGitHubJITSnapshotAuthority(
				ctx,
				tx,
				current,
				snapshotDigest,
				reconciliationEpoch,
				"generation_absence_pending",
				githubSessionGeneration,
				now,
			); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			if err := tx.Commit(); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			return GitHubJITAbsenceResult{}, ErrGitHubJITAbsencePending
		}
	} else {
		if authorityErr != nil || authorityEpoch != reconciliationEpoch {
			return GitHubJITAbsenceResult{}, ErrGitHubJITState
		}
		if authorityDigest != snapshotDigest {
			rotatableDecision := (lostJIT &&
				(decision == "lost_jit_removal_issued" ||
					decision == "lost_jit_absence_pending")) ||
				(unpickedRequeue &&
					(decision == "unpicked_requeue_removal_issued" ||
						decision == "unpicked_requeue_absence_pending"))
			if !rotatableDecision {
				return GitHubJITAbsenceResult{}, ErrGitHubJITState
			}
			// ACK pruning legitimately replaces the full Agent snapshot after
			// the exact terminal Start update is durable. This nil provider
			// read becomes the first absence under the new snapshot authority.
			if err := upsertGitHubJITSnapshotAuthority(
				ctx,
				tx,
				current,
				snapshotDigest,
				reconciliationEpoch,
				map[bool]string{
					true:  "unpicked_requeue_absence_pending",
					false: "lost_jit_absence_pending",
				}[unpickedRequeue],
				githubSessionGeneration,
				now,
			); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			if err := tx.Commit(); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			return GitHubJITAbsenceResult{}, ErrGitHubJITAbsencePending
		}
		switch decision {
		case "runner_removal_issued":
			if lostJIT || unpickedRequeue {
				return GitHubJITAbsenceResult{}, ErrGitHubJITState
			}
			// A successful DELETE does not prove absence. Persist this first
			// exact nil read as its own authority and require another read after
			// the minimum interval.
			if err := upsertGitHubJITSnapshotAuthority(
				ctx,
				tx,
				current,
				snapshotDigest,
				reconciliationEpoch,
				"runner_absence_pending",
				githubSessionGeneration,
				now,
			); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			if err := tx.Commit(); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			return GitHubJITAbsenceResult{}, ErrGitHubJITAbsencePending
		case "runner_absence_pending":
			if lostJIT || unpickedRequeue {
				return GitHubJITAbsenceResult{}, ErrGitHubJITState
			}
		case "lost_jit_removal_issued":
			if !lostJIT {
				return GitHubJITAbsenceResult{}, ErrGitHubJITState
			}
			if err := upsertGitHubJITSnapshotAuthority(
				ctx,
				tx,
				current,
				snapshotDigest,
				reconciliationEpoch,
				"lost_jit_absence_pending",
				githubSessionGeneration,
				now,
			); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			if err := tx.Commit(); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			return GitHubJITAbsenceResult{}, ErrGitHubJITAbsencePending
		case "lost_jit_absence_pending":
			if !lostJIT {
				return GitHubJITAbsenceResult{}, ErrGitHubJITState
			}
		case "unpicked_requeue_removal_issued":
			if !unpickedRequeue {
				return GitHubJITAbsenceResult{}, ErrGitHubJITState
			}
			if err := upsertGitHubJITSnapshotAuthority(
				ctx,
				tx,
				current,
				snapshotDigest,
				reconciliationEpoch,
				"unpicked_requeue_absence_pending",
				githubSessionGeneration,
				now,
			); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			if err := tx.Commit(); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			return GitHubJITAbsenceResult{}, ErrGitHubJITAbsencePending
		case "unpicked_requeue_absence_pending":
			if !unpickedRequeue {
				return GitHubJITAbsenceResult{}, ErrGitHubJITState
			}
		default:
			return GitHubJITAbsenceResult{}, ErrGitHubJITState
		}
	}
	if authoritySessionGeneration != githubSessionGeneration {
		// A provider failure/recovery transition invalidates every prior absence
		// observation. This successful nil read becomes the first observation
		// under the caller's exact pre-query session authority.
		if err := upsertGitHubJITSnapshotAuthority(
			ctx,
			tx,
			current,
			snapshotDigest,
			reconciliationEpoch,
			decision,
			githubSessionGeneration,
			now,
		); err != nil {
			return GitHubJITAbsenceResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return GitHubJITAbsenceResult{}, err
		}
		return GitHubJITAbsenceResult{}, ErrGitHubJITAbsencePending
	}
	if now < firstObservedAt ||
		time.Duration(now-firstObservedAt) < GitHubRunnerAbsenceConfirmationDelay {
		return GitHubJITAbsenceResult{}, ErrGitHubJITAbsencePending
	}
	if unpickedRequeue {
		return finalizeGitHubUnpickedRequeue(
			ctx,
			tx,
			current,
			claim,
			now,
		)
	}
	nextClaimState := GitHubClaimPreparing
	if lostJIT {
		nextClaimState = GitHubClaimReconciliationRequired
		switch claim.Execution.State {
		case domain.ExecutionPreparing:
			result, err := tx.ExecContext(ctx, `UPDATE executions SET state = ?
				WHERE id = ? AND state = ?`,
				terminalState,
				claim.Execution.ID,
				domain.ExecutionPreparing,
			)
			if err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			if affected, rowsErr := result.RowsAffected(); rowsErr != nil ||
				affected != 1 {
				return GitHubJITAbsenceResult{}, errors.New(
					"lost-JIT cleanup lost the old execution compare-and-swap",
				)
			}
			switch terminalState {
			case domain.ExecutionReleased, domain.ExecutionFailed:
				result, err = tx.ExecContext(
					ctx,
					`DELETE FROM slot_reservations WHERE execution_id = ?`,
					claim.Execution.ID,
				)
				if err != nil {
					return GitHubJITAbsenceResult{}, err
				}
				if affected, rowsErr := result.RowsAffected(); rowsErr != nil ||
					affected != 1 {
					return GitHubJITAbsenceResult{}, errors.New(
						"lost-JIT cleanup did not release exactly one old slot",
					)
				}
			case domain.ExecutionCleanupFailed:
				if err := quarantineNode(
					ctx,
					tx,
					claim.Execution.Slot.NodeID,
				); err != nil {
					return GitHubJITAbsenceResult{}, err
				}
			case domain.ExecutionQuarantined:
				if err := quarantineNode(
					ctx,
					tx,
					claim.Execution.Slot.NodeID,
				); err != nil {
					return GitHubJITAbsenceResult{}, err
				}
				result, err = tx.ExecContext(
					ctx,
					`DELETE FROM slot_reservations WHERE execution_id = ?`,
					claim.Execution.ID,
				)
				if err != nil {
					return GitHubJITAbsenceResult{}, err
				}
				if affected, rowsErr := result.RowsAffected(); rowsErr != nil ||
					affected != 1 {
					return GitHubJITAbsenceResult{}, errors.New(
						"quarantined lost-JIT cleanup did not release exactly one old slot",
					)
				}
			}
			claim.Execution.State = terminalState
		case terminalState:
			var reservations int
			if err := tx.QueryRowContext(
				ctx,
				`SELECT count(*) FROM slot_reservations WHERE execution_id = ?`,
				claim.Execution.ID,
			).Scan(&reservations); err != nil {
				return GitHubJITAbsenceResult{}, err
			}
			switch terminalState {
			case domain.ExecutionReleased, domain.ExecutionFailed,
				domain.ExecutionQuarantined:
				if reservations != 0 {
					return GitHubJITAbsenceResult{}, errors.New(
						"terminal lost-JIT execution still owns a slot",
					)
				}
				if terminalState == domain.ExecutionQuarantined {
					if err := quarantineNode(
						ctx,
						tx,
						claim.Execution.Slot.NodeID,
					); err != nil {
						return GitHubJITAbsenceResult{}, err
					}
				}
			case domain.ExecutionCleanupFailed:
				if reservations != 1 {
					return GitHubJITAbsenceResult{}, errors.New(
						"cleanup-blocked lost-JIT execution lost its slot",
					)
				}
				if err := quarantineNode(
					ctx,
					tx,
					claim.Execution.Slot.NodeID,
				); err != nil {
					return GitHubJITAbsenceResult{}, err
				}
			}
		default:
			return GitHubJITAbsenceResult{}, ErrGitHubClaimState
		}
	}
	if err := transitionExactGitHubJITAndClaim(
		ctx,
		tx,
		current,
		claim,
		GitHubJITReconciledAbsent,
		nextClaimState,
		0,
		"",
		"",
		now,
	); err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM github_jit_snapshot_authority
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		current.ScaleSetID,
		current.ClaimKey,
		current.Attempt,
	); err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	claim.State = nextClaimState
	result := GitHubJITAbsenceResult{Claim: claim}
	if lostJIT {
		terminal := claim.Execution
		result.TerminalExecution = &terminal
		switch terminal.State {
		case domain.ExecutionReleased, domain.ExecutionFailed:
			result.AwaitingAvailability = true
		case domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
			result.CleanupBlocked = true
		default:
			return GitHubJITAbsenceResult{}, ErrGitHubClaimState
		}
	}
	return result, nil
}

func finalizeGitHubUnpickedRequeue(
	ctx context.Context,
	tx *sql.Tx,
	current GitHubJITAttempt,
	claim GitHubJobClaim,
	now int64,
) (GitHubJITAbsenceResult, error) {
	intent, found, err := loadGitHubUnpickedRequeueIntent(
		ctx,
		tx,
		current.ScaleSetID,
		current.ClaimKey,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return GitHubJITAbsenceResult{}, err
	}
	if intent.Claim != claim ||
		intent.Attempt != current ||
		current.State != GitHubJITRemovalPending ||
		claim.State != GitHubClaimReconciliationRequired ||
		(claim.Execution.State != domain.ExecutionReleased &&
			claim.Execution.State != domain.ExecutionFailed) {
		return GitHubJITAbsenceResult{}, ErrGitHubClaimState
	}
	var slotLeaseCount int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM slot_reservations
		WHERE node_id = ? AND slot_index = ?`,
		intent.Replacement.Slot.NodeID,
		intent.Replacement.Slot.Index,
	).Scan(&slotLeaseCount); err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	if slotLeaseCount != 0 {
		return GitHubJITAbsenceResult{}, errors.New(
			"unpicked requeue slot was consumed before provider cleanup completed",
		)
	}
	pickedUp, err := githubJITAttemptPickupProven(ctx, tx, current)
	if err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_jit_attempts SET
		state = ?, runner_id = NULL, jit_digest = NULL, start_command_id = '',
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?
			AND controller_epoch = ? AND runner_name = ? AND state = ?
			AND runner_id = ? AND jit_digest = ? AND start_command_id = ?`,
		GitHubJITReconciledAbsent,
		now,
		now,
		current.ScaleSetID,
		current.ClaimKey,
		current.Attempt,
		current.ControllerEpoch,
		current.RunnerName,
		GitHubJITRemovalPending,
		current.RunnerID,
		current.JITDigest,
		current.StartCommandID,
	)
	if err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return GitHubJITAbsenceResult{}, ErrGitHubJITState
	}

	replacement := intent.Replacement
	if pickedUp {
		result, err = tx.ExecContext(ctx, `UPDATE github_job_claims SET state = ?,
			updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
				THEN updated_at_unix_nano + 1 ELSE ? END
			WHERE scale_set_id = ? AND claim_key = ?
				AND execution_id = ? AND current_jit_attempt = ? AND state = ?`,
			GitHubClaimRunning,
			now,
			now,
			claim.ScaleSetID,
			claim.ClaimKey,
			claim.Execution.ID,
			claim.CurrentAttempt,
			GitHubClaimReconciliationRequired,
		)
		if err != nil {
			return GitHubJITAbsenceResult{}, err
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return GitHubJITAbsenceResult{}, ErrGitHubClaimState
		}
		claim.State = GitHubClaimRunning
	} else {
		currentAcquire, found, err := loadCurrentGitHubAcquireAttempt(
			ctx,
			tx,
			claim.ScaleSetID,
			claim.ClaimKey,
		)
		if err != nil || !found || currentAcquire.State != githubAcquireAcquired {
			if err == nil {
				err = ErrGitHubClaimState
			}
			return GitHubJITAbsenceResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO executions(
				id, target_id, node_id, slot_index, state,
				created_at_unix_nano
			) VALUES (?, ?, ?, ?, ?, ?)`,
			replacement.ID,
			replacement.TargetID,
			replacement.Slot.NodeID,
			replacement.Slot.Index,
			replacement.State,
			now,
		); err != nil {
			return GitHubJITAbsenceResult{}, mapAssignmentError(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO slot_reservations(
				node_id, slot_index, target_id, execution_id
			) VALUES (?, ?, ?, ?)`,
			replacement.Slot.NodeID,
			replacement.Slot.Index,
			replacement.TargetID,
			replacement.ID,
		); err != nil {
			return GitHubJITAbsenceResult{}, mapAssignmentError(err)
		}
		controllerEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
		if err != nil {
			return GitHubJITAbsenceResult{}, err
		}
		nextAcquire := currentAcquire.Attempt + 1
		if _, err := tx.ExecContext(ctx, `INSERT INTO github_acquire_attempts(
			scale_set_id, claim_key, attempt, evidence_message_id,
			controller_epoch, state, created_at_unix_nano, updated_at_unix_nano
		) VALUES (?, ?, ?, ?, ?, 'reconciled_pending', ?, ?)`,
			claim.ScaleSetID,
			claim.ClaimKey,
			nextAcquire,
			intent.SourceMessageID,
			controllerEpoch,
			now,
			now,
		); err != nil {
			return GitHubJITAbsenceResult{}, err
		}
		result, err = tx.ExecContext(ctx, `UPDATE github_job_claims SET
			source_message_id = ?, execution_id = ?, state = ?,
			updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
				THEN updated_at_unix_nano + 1 ELSE ? END
			WHERE scale_set_id = ? AND claim_key = ?
				AND execution_id = ? AND current_jit_attempt = ? AND state = ?`,
			intent.SourceMessageID,
			replacement.ID,
			GitHubClaimPending,
			now,
			now,
			claim.ScaleSetID,
			claim.ClaimKey,
			claim.Execution.ID,
			claim.CurrentAttempt,
			GitHubClaimReconciliationRequired,
		)
		if err != nil {
			return GitHubJITAbsenceResult{}, err
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return GitHubJITAbsenceResult{}, ErrGitHubClaimState
		}
		claim.SourceMessageID = intent.SourceMessageID
		claim.Execution = replacement
		claim.State = GitHubClaimPending
	}
	deleteIntent, err := tx.ExecContext(ctx, `DELETE FROM github_unpicked_requeue_intents
		WHERE scale_set_id = ? AND claim_key = ?
			AND jit_attempt = ? AND old_execution_id = ?
			AND replacement_execution_id = ? AND source_message_id = ?
			AND source_event_index = ?`,
		current.ScaleSetID,
		current.ClaimKey,
		current.Attempt,
		intent.Claim.Execution.ID,
		intent.Replacement.ID,
		intent.SourceMessageID,
		intent.SourceEventIndex,
	)
	if err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	if affected, err := deleteIntent.RowsAffected(); err != nil || affected != 1 {
		return GitHubJITAbsenceResult{}, ErrGitHubClaimState
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM github_jit_snapshot_authority
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?`,
		current.ScaleSetID,
		current.ClaimKey,
		current.Attempt,
	); err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return GitHubJITAbsenceResult{}, err
	}
	resultValue := GitHubJITAbsenceResult{
		Claim:              claim,
		ReplacementClaimed: !pickedUp,
	}
	if !pickedUp {
		resultValue.ReplacementExecution = &replacement
	}
	return resultValue, nil
}

func loadGitHubJITReconciliationContext(
	ctx context.Context,
	tx *sql.Tx,
	attempt GitHubJITAttempt,
	reconciliationEpoch domain.ControllerEpoch,
	allowedStates []GitHubJITAttemptState,
	allowOwningEpoch bool,
) (GitHubJITAttempt, GitHubJobClaim, error) {
	currentEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return GitHubJITAttempt{}, GitHubJobClaim{}, err
	}
	if domain.ControllerEpoch(currentEpoch) != reconciliationEpoch ||
		reconciliationEpoch < attempt.ControllerEpoch ||
		(!allowOwningEpoch && reconciliationEpoch == attempt.ControllerEpoch) {
		return GitHubJITAttempt{}, GitHubJobClaim{}, ErrStaleControllerEpoch
	}
	current, found, err := loadGitHubJITAttempt(
		ctx,
		tx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
		attempt.Attempt,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubJITState
		}
		return GitHubJITAttempt{}, GitHubJobClaim{}, err
	}
	if current.ScaleSetID != attempt.ScaleSetID ||
		current.ClaimKey != attempt.ClaimKey ||
		current.Attempt != attempt.Attempt ||
		current.ControllerEpoch != attempt.ControllerEpoch ||
		current.RunnerName != attempt.RunnerName {
		return GitHubJITAttempt{}, GitHubJobClaim{}, ErrGitHubJITState
	}
	allowed := false
	for _, state := range allowedStates {
		allowed = allowed || current.State == state
	}
	if !allowed {
		return GitHubJITAttempt{}, GitHubJobClaim{}, ErrGitHubJITState
	}
	claim, found, err := loadGitHubClaim(
		ctx,
		tx,
		attempt.ScaleSetID,
		attempt.ClaimKey,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return GitHubJITAttempt{}, GitHubJobClaim{}, err
	}
	if claim.CurrentAttempt != current.Attempt ||
		!gitHubClaimMatchesJITState(claim.State, current.State) {
		return GitHubJITAttempt{}, GitHubJobClaim{}, ErrGitHubClaimState
	}
	return current, claim, nil
}

func sameGitHubJITIdentity(left, right GitHubJITAttempt) bool {
	return left.ScaleSetID == right.ScaleSetID &&
		left.ClaimKey == right.ClaimKey &&
		left.Attempt == right.Attempt &&
		left.ControllerEpoch == right.ControllerEpoch &&
		left.RunnerName == right.RunnerName &&
		left.RunnerID == right.RunnerID &&
		left.JITDigest == right.JITDigest &&
		left.StartCommandID == right.StartCommandID
}

func exactCurrentAgentStartAccepted(
	ctx context.Context,
	tx *sql.Tx,
	attempt GitHubJITAttempt,
	claim GitHubJobClaim,
	snapshotDigest string,
) (bool, error) {
	if attempt.StartCommandID == "" {
		return false, nil
	}
	issued, found, err := loadIssuedAgentCommand(
		ctx,
		tx,
		attempt.StartCommandID,
	)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubJITState
		}
		return false, err
	}
	if issued.Type != domain.CommandStart ||
		issued.NodeID != claim.Execution.Slot.NodeID ||
		issued.Command.ID != attempt.StartCommandID ||
		issued.Command.ControllerEpoch != attempt.ControllerEpoch ||
		issued.Command.ExecutionID != claim.Execution.ID ||
		issued.Command.ExpectedState != domain.ExecutionPreparing {
		return false, ErrGitHubJITState
	}
	var accepted domain.Command
	err = tx.QueryRowContext(ctx, `SELECT command.command_id,
		command.controller_epoch, command.execution_id,
		command.expected_state, command.payload_digest
		FROM agent_current_snapshot_commands current
		JOIN agent_snapshot_commands command
			ON command.node_id = current.node_id
			AND command.command_id = current.command_id
		WHERE current.node_id = ? AND current.command_id = ?
			AND current.snapshot_digest = ?`,
		issued.NodeID,
		issued.Command.ID,
		snapshotDigest,
	).Scan(
		&accepted.ID,
		&accepted.ControllerEpoch,
		&accepted.ExecutionID,
		&accepted.ExpectedState,
		&accepted.PayloadDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if accepted != issued.Command {
		return false, fmt.Errorf(
			"%w: Agent start command differs from Controller authority",
			ErrReplayMismatch,
		)
	}
	return true, nil
}

// exactStartHasDurableRuntimeHistory accepts only a Running/Cleaning update
// committed for the exact issued Start command and execution owned by this JIT
// attempt. A current Released snapshot alone is deliberately insufficient:
// Agent recovery emits that state when a decoded JIT value was lost before
// runner startup.
func exactStartHasDurableRuntimeHistory(
	ctx context.Context,
	tx *sql.Tx,
	attempt GitHubJITAttempt,
	claim GitHubJobClaim,
) (bool, error) {
	if attempt.StartCommandID == "" {
		return false, nil
	}
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM agent_execution_updates updates
		JOIN agent_commands command
			ON command.command_id = updates.command_id
			AND command.node_id = updates.node_id
			AND command.execution_id = updates.execution_id
		WHERE updates.node_id = ? AND updates.command_id = ?
			AND updates.execution_id = ?
			AND updates.state IN ('running', 'cleaning')
			AND command.command_type = 'start'
			AND command.controller_epoch = ?
			AND command.expected_state = 'preparing'
		)`,
		claim.Execution.Slot.NodeID,
		attempt.StartCommandID,
		claim.Execution.ID,
		attempt.ControllerEpoch,
	).Scan(&exists); err != nil {
		return false, err
	}
	if exists != 0 && exists != 1 {
		return false, errors.New("exact Agent Start runtime history is invalid")
	}
	return exists == 1, nil
}

func exactStartTerminalWithoutRuntimeHistory(
	ctx context.Context,
	tx *sql.Tx,
	attempt GitHubJITAttempt,
	claim GitHubJobClaim,
) (domain.ExecutionState, bool, error) {
	if attempt.StartCommandID == "" {
		return "", false, nil
	}
	started, err := exactStartHasDurableRuntimeHistory(
		ctx,
		tx,
		attempt,
		claim,
	)
	if err != nil || started {
		return "", false, err
	}
	return exactStartDurableTerminalUpdate(ctx, tx, attempt, claim)
}

func exactStartDurableTerminalUpdate(
	ctx context.Context,
	tx *sql.Tx,
	attempt GitHubJITAttempt,
	claim GitHubJobClaim,
) (domain.ExecutionState, bool, error) {
	if attempt.StartCommandID == "" {
		return "", false, nil
	}
	var state domain.ExecutionState
	err := tx.QueryRowContext(ctx, `SELECT updates.state
		FROM agent_execution_updates updates
		JOIN agent_commands command
			ON command.command_id = updates.command_id
			AND command.node_id = updates.node_id
			AND command.execution_id = updates.execution_id
		WHERE updates.node_id = ? AND updates.command_id = ?
			AND updates.execution_id = ?
			AND updates.state IN (
				'released', 'failed', 'cleanup_failed', 'quarantined'
			)
			AND command.command_type = 'start'
			AND command.controller_epoch = ?
			AND command.expected_state = 'preparing'
		ORDER BY updates.received_at_unix_nano DESC,
			updates.message_id DESC LIMIT 1`,
		claim.Execution.Slot.NodeID,
		attempt.StartCommandID,
		claim.Execution.ID,
		attempt.ControllerEpoch,
	).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return state, true, nil
}

func requirePrunedAgentExecutionMembership(
	ctx context.Context,
	tx *sql.Tx,
	claim GitHubJobClaim,
	snapshotDigest string,
) error {
	var commandCount int
	if err := tx.QueryRowContext(ctx, `SELECT count(*)
		FROM agent_current_snapshot_commands current
		JOIN agent_snapshot_commands command
			ON command.node_id = current.node_id
			AND command.command_id = current.command_id
		WHERE current.node_id = ? AND current.snapshot_digest = ?
			AND command.execution_id = ?`,
		claim.Execution.Slot.NodeID,
		snapshotDigest,
		claim.Execution.ID,
	).Scan(&commandCount); err != nil {
		return err
	}
	var observationCount int
	if err := tx.QueryRowContext(ctx, `SELECT count(*)
		FROM agent_current_snapshot_observations
		WHERE node_id = ? AND snapshot_digest = ? AND execution_id = ?`,
		claim.Execution.Slot.NodeID,
		snapshotDigest,
		claim.Execution.ID,
	).Scan(&observationCount); err != nil {
		return err
	}
	if commandCount != 0 || observationCount != 0 {
		return errors.New(
			"durable Start history conflicts with current Agent execution membership",
		)
	}
	return nil
}

func classifyGitHubRunnerRemovalAuthority(
	ctx context.Context,
	tx *sql.Tx,
	attempt GitHubJITAttempt,
	claim GitHubJobClaim,
	snapshotDigest string,
) (domain.ExecutionState, githubRunnerRemovalKind, error) {
	intent, found, err := loadGitHubUnpickedRequeueIntent(
		ctx,
		tx,
		claim.ScaleSetID,
		claim.ClaimKey,
	)
	if err != nil {
		return "", githubRunnerRemovalAmbiguity, err
	}
	if found {
		if intent.Claim != claim ||
			intent.Attempt != attempt ||
			(claim.Execution.State != domain.ExecutionReleased &&
				claim.Execution.State != domain.ExecutionFailed) {
			return "", githubRunnerRemovalAmbiguity, ErrGitHubClaimState
		}
		started, err := exactStartHasDurableRuntimeHistory(
			ctx,
			tx,
			attempt,
			claim,
		)
		if err != nil {
			return "", githubRunnerRemovalAmbiguity, err
		}
		terminal, terminalFound, err := exactStartDurableTerminalUpdate(
			ctx,
			tx,
			attempt,
			claim,
		)
		if err != nil {
			return "", githubRunnerRemovalAmbiguity, err
		}
		if !started || !terminalFound || terminal != claim.Execution.State {
			return "", githubRunnerRemovalAmbiguity, ErrGitHubClaimState
		}
		var observation ObservationSnapshot
		observationErr := tx.QueryRowContext(ctx, `SELECT execution_id, state,
				agent_observed_at_unix_nano
			FROM agent_current_snapshot_observations
			WHERE node_id = ? AND execution_id = ? AND snapshot_digest = ?`,
			claim.Execution.Slot.NodeID,
			claim.Execution.ID,
			snapshotDigest,
		).Scan(
			&observation.ExecutionID,
			&observation.State,
			&observation.ObservedAtUnixNano,
		)
		switch {
		case observationErr == nil:
			if observation.ExecutionID != claim.Execution.ID ||
				observation.State != claim.Execution.State ||
				observation.ObservedAtUnixNano <= 0 {
				return "", githubRunnerRemovalAmbiguity, errors.New(
					"unpicked requeue conflicts with current Agent execution",
				)
			}
		case errors.Is(observationErr, sql.ErrNoRows):
			if err := requirePrunedAgentExecutionMembership(
				ctx,
				tx,
				claim,
				snapshotDigest,
			); err != nil {
				return "", githubRunnerRemovalAmbiguity, err
			}
		default:
			return "", githubRunnerRemovalAmbiguity, observationErr
		}
		return claim.Execution.State, githubRunnerRemovalUnpickedRequeue, nil
	}
	terminal, lostJIT, err := classifyAgentRunnerRemovalAuthority(
		ctx,
		tx,
		attempt,
		claim,
		snapshotDigest,
	)
	if err != nil {
		return "", githubRunnerRemovalAmbiguity, err
	}
	if lostJIT {
		return terminal, githubRunnerRemovalLostJIT, nil
	}
	return terminal, githubRunnerRemovalAmbiguity, nil
}

func classifyAgentRunnerRemovalAuthority(
	ctx context.Context,
	tx *sql.Tx,
	attempt GitHubJITAttempt,
	claim GitHubJobClaim,
	snapshotDigest string,
) (terminalState domain.ExecutionState, lostJIT bool, err error) {
	accepted, err := exactCurrentAgentStartAccepted(
		ctx,
		tx,
		attempt,
		claim,
		snapshotDigest,
	)
	if err != nil {
		return "", false, err
	}
	if !accepted {
		started, err := exactStartHasDurableRuntimeHistory(
			ctx,
			tx,
			attempt,
			claim,
		)
		if err != nil {
			return "", false, err
		}
		if started {
			return "", false, errors.New(
				"durably started pruned Agent runner forbids provider removal",
			)
		}
		state, found, err := exactStartTerminalWithoutRuntimeHistory(
			ctx,
			tx,
			attempt,
			claim,
		)
		if err != nil {
			return "", false, err
		}
		if !found {
			if err := requireNoAdvancedCurrentAgentObservation(
				ctx,
				tx,
				claim,
				snapshotDigest,
			); err != nil {
				return "", false, err
			}
			return "", false, nil
		}
		if err := requirePrunedAgentExecutionMembership(
			ctx,
			tx,
			claim,
			snapshotDigest,
		); err != nil {
			return "", false, err
		}
		if claim.Execution.State != state {
			return "", false, errors.New(
				"exact terminal Start update conflicts with desired execution",
			)
		}
		return state, true, nil
	}
	var observation ObservationSnapshot
	err = tx.QueryRowContext(ctx, `SELECT execution_id, state,
		agent_observed_at_unix_nano
		FROM agent_current_snapshot_observations
		WHERE node_id = ? AND execution_id = ? AND snapshot_digest = ?`,
		claim.Execution.Slot.NodeID,
		claim.Execution.ID,
		snapshotDigest,
	).Scan(
		&observation.ExecutionID,
		&observation.State,
		&observation.ObservedAtUnixNano,
	)
	if err != nil {
		return "", false, err
	}
	if observation.ExecutionID != claim.Execution.ID ||
		!lostJITTerminalState(observation.State) ||
		observation.ObservedAtUnixNano <= 0 {
		return "", false, errors.New(
			"accepted Agent Start forbids provider runner removal without exact lost-JIT cleanup",
		)
	}
	startProven, err := exactStartHasDurableRuntimeHistory(
		ctx,
		tx,
		attempt,
		claim,
	)
	if err != nil {
		return "", false, err
	}
	if startProven || attempt.State == GitHubJITStarted {
		return "", false, errors.New(
			"durably started Agent runner forbids provider removal as lost JIT",
		)
	}
	var found bool
	terminalState, found, err = exactStartTerminalWithoutRuntimeHistory(
		ctx,
		tx,
		attempt,
		claim,
	)
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", false, ErrGitHubJITTerminalPending
	}
	if terminalState != observation.State {
		return "", false, errors.New(
			"exact terminal Start update conflicts with current Agent snapshot",
		)
	}
	switch claim.Execution.State {
	case domain.ExecutionPreparing:
	case observation.State:
	default:
		return "", false, errors.New(
			"lost-JIT cleanup conflicts with durable execution history",
		)
	}
	return observation.State, true, nil
}

func lostJITTerminalState(state domain.ExecutionState) bool {
	switch state {
	case domain.ExecutionReleased, domain.ExecutionFailed,
		domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
		return true
	default:
		return false
	}
}

func requireNoAdvancedCurrentAgentObservation(
	ctx context.Context,
	tx *sql.Tx,
	claim GitHubJobClaim,
	snapshotDigest string,
) error {
	var state domain.ExecutionState
	err := tx.QueryRowContext(ctx, `SELECT state
		FROM agent_current_snapshot_observations
		WHERE node_id = ? AND execution_id = ? AND snapshot_digest = ?`,
		claim.Execution.Slot.NodeID,
		claim.Execution.ID,
		snapshotDigest,
	).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if state != domain.ExecutionPreparing {
		return errors.New("current Agent snapshot still reports an advanced local runtime")
	}
	return nil
}

func transitionExactGitHubJITAndClaim(
	ctx context.Context,
	tx *sql.Tx,
	current GitHubJITAttempt,
	claim GitHubJobClaim,
	nextAttemptState GitHubJITAttemptState,
	nextClaimState GitHubClaimState,
	runnerID int,
	jitDigest string,
	startCommandID domain.CommandID,
	now int64,
) error {
	var currentRunner any
	var currentDigest any
	var nextRunner any
	var nextDigest any
	if current.RunnerID > 0 {
		currentRunner = current.RunnerID
	}
	if current.JITDigest != "" {
		currentDigest = current.JITDigest
	}
	if runnerID > 0 {
		nextRunner = runnerID
	}
	if jitDigest != "" {
		nextDigest = jitDigest
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_jit_attempts SET
		state = ?, runner_id = ?, jit_digest = ?, start_command_id = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?
			AND controller_epoch = ? AND runner_name = ? AND state = ?
			AND runner_id IS ? AND jit_digest IS ? AND start_command_id = ?`,
		nextAttemptState,
		nextRunner,
		nextDigest,
		startCommandID,
		now,
		now,
		current.ScaleSetID,
		current.ClaimKey,
		current.Attempt,
		current.ControllerEpoch,
		current.RunnerName,
		current.State,
		currentRunner,
		currentDigest,
		current.StartCommandID,
	)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrGitHubJITState
	}
	result, err = tx.ExecContext(ctx, `UPDATE github_job_claims SET state = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ?
			AND current_jit_attempt = ? AND state = ?`,
		nextClaimState,
		now,
		now,
		current.ScaleSetID,
		current.ClaimKey,
		current.Attempt,
		claim.State,
	)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrGitHubClaimState
	}
	return nil
}

func upsertGitHubJITSnapshotAuthority(
	ctx context.Context,
	tx *sql.Tx,
	attempt GitHubJITAttempt,
	snapshotDigest string,
	controllerEpoch domain.ControllerEpoch,
	decision string,
	githubSessionGeneration uint64,
	now int64,
) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO github_jit_snapshot_authority(
		scale_set_id, claim_key, attempt, snapshot_digest,
		controller_epoch, decision, updated_at_unix_nano,
		github_session_generation
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(scale_set_id, claim_key, attempt) DO UPDATE SET
		snapshot_digest=excluded.snapshot_digest,
		controller_epoch=excluded.controller_epoch,
		decision=excluded.decision,
		updated_at_unix_nano=excluded.updated_at_unix_nano,
		github_session_generation=excluded.github_session_generation`,
		attempt.ScaleSetID,
		attempt.ClaimKey,
		attempt.Attempt,
		snapshotDigest,
		controllerEpoch,
		decision,
		now,
		githubSessionGeneration,
	)
	return err
}

func requireGitHubSessionTransitionGeneration(
	ctx context.Context,
	queryer freshnessQueryer,
	scaleSetID ScaleSetID,
	expected uint64,
) error {
	if expected == 0 || expected > maxSQLiteInteger {
		return ErrGitHubJITAbsencePending
	}
	health, found, err := readGitHubScaleSetSessionHealth(
		ctx,
		queryer,
		scaleSetID,
	)
	if err != nil {
		return err
	}
	if !found || health.TransitionGeneration != expected {
		return ErrGitHubJITAbsencePending
	}
	return nil
}

func (s *ControllerStore) CurrentGitHubJITAttempt(ctx context.Context, scaleSetID ScaleSetID, claimKey int64) (GitHubJITAttempt, bool, error) {
	if err := s.requireReady(); err != nil {
		return GitHubJITAttempt{}, false, err
	}
	var attempt int
	err := s.db.QueryRowContext(ctx, `SELECT current_jit_attempt FROM github_job_claims
		WHERE scale_set_id = ? AND claim_key = ?`,
		scaleSetID, claimKey).Scan(&attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubJITAttempt{}, false, nil
	}
	if err != nil {
		return GitHubJITAttempt{}, false, err
	}
	if attempt == 0 {
		return GitHubJITAttempt{}, false, nil
	}
	return loadGitHubJITAttempt(ctx, s.db, scaleSetID, claimKey, attempt)
}

// NextGitHubReconciliationFence returns the oldest durable provider ambiguity
// for one scale set from a single read transaction. It never reconstructs JIT
// configuration; only claim, runner identity, and digests cross this boundary.
func (s *ControllerStore) NextGitHubReconciliationFence(
	ctx context.Context,
	scaleSetID ScaleSetID,
) (GitHubReconciliationFence, bool, error) {
	if err := s.requireReady(); err != nil {
		return GitHubReconciliationFence{}, false, err
	}
	if scaleSetID == 0 || uint64(scaleSetID) > maxSQLiteInteger {
		return GitHubReconciliationFence{}, false, errors.New("GitHub scale set ID is invalid")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return GitHubReconciliationFence{}, false, err
	}
	defer tx.Rollback()
	epoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return GitHubReconciliationFence{}, false, err
	}
	fences, err := readGitHubReconciliationFences(
		ctx, tx, domain.ControllerEpoch(epoch))
	if err != nil {
		return GitHubReconciliationFence{}, false, err
	}
	for _, fence := range fences {
		if fence.Claim.ScaleSetID == scaleSetID {
			return fence, true, nil
		}
	}
	return GitHubReconciliationFence{}, false, nil
}

func (s *ControllerStore) transitionGitHubClaimWithExecution(
	ctx context.Context,
	scaleSetID ScaleSetID,
	claimKey int64,
	expected []GitHubClaimState,
	next GitHubClaimState,
	executionState domain.ExecutionState,
) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	claim, found, err := loadGitHubClaim(ctx, tx, scaleSetID, claimKey)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return err
	}
	expectedState := false
	for _, state := range expected {
		expectedState = expectedState || claim.State == state
	}
	if !expectedState || claim.Execution.State != executionState {
		return ErrGitHubClaimState
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_job_claims SET state = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
		THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ? AND state = ?`,
		next, now, now, scaleSetID, claimKey, claim.State)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrGitHubClaimState
	}
	return tx.Commit()
}

func (s *ControllerStore) transitionGitHubJIT(
	ctx context.Context,
	attempt GitHubJITAttempt,
	expected GitHubJITAttemptState,
	next GitHubJITAttemptState,
	claimState GitHubClaimState,
	runnerID int,
	jitDigest string,
	startCommandID domain.CommandID,
	requireAdmission bool,
) error {
	return s.transitionGitHubJITAny(ctx, GitHubJITAttempt{
		ScaleSetID: attempt.ScaleSetID, ClaimKey: attempt.ClaimKey,
		Attempt: attempt.Attempt, ControllerEpoch: attempt.ControllerEpoch,
		RunnerName: attempt.RunnerName, RunnerID: runnerID,
		JITDigest: jitDigest, StartCommandID: startCommandID,
	}, []GitHubJITAttemptState{expected}, next, claimState,
		attempt.ControllerEpoch, false, requireAdmission)
}

func (s *ControllerStore) markGitHubStarted(
	ctx context.Context,
	attempt GitHubJITAttempt,
) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	currentEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return err
	}
	if domain.ControllerEpoch(currentEpoch) != attempt.ControllerEpoch {
		return ErrStaleControllerEpoch
	}
	current, found, err := loadGitHubJITAttempt(
		ctx, tx, attempt.ScaleSetID, attempt.ClaimKey, attempt.Attempt)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubJITState
		}
		return err
	}
	if current.State != GitHubJITStartDispatching ||
		current.RunnerName != attempt.RunnerName ||
		current.RunnerID != attempt.RunnerID ||
		current.JITDigest != attempt.JITDigest ||
		current.StartCommandID != attempt.StartCommandID {
		return ErrGitHubJITState
	}
	claim, found, err := loadGitHubClaim(
		ctx, tx, attempt.ScaleSetID, attempt.ClaimKey)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return err
	}
	if claim.State != GitHubClaimStartDispatching ||
		claim.CurrentAttempt != attempt.Attempt {
		return ErrGitHubClaimState
	}
	startProven, err := exactStartHasDurableRuntimeHistory(
		ctx,
		tx,
		current,
		claim,
	)
	if err != nil {
		return err
	}
	if !startProven {
		return ErrGitHubJITStartNotProven
	}

	// The Agent commits Running before it returns the Start result. A very short
	// job may then commit its terminal cleanup update before this Controller
	// transaction begins. Preserve that newer execution authority and retain a
	// reconciliation fence until a later exact Agent snapshot proves the start.
	claimState := GitHubClaimRunning
	switch claim.Execution.State {
	case domain.ExecutionRunning:
	case domain.ExecutionCleaning, domain.ExecutionReleased, domain.ExecutionFailed,
		domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
		claimState = GitHubClaimReconciliationRequired
	default:
		return ErrGitHubClaimState
	}

	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_jit_attempts SET
		state = ?, updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ?
			AND state = ?`,
		GitHubJITStarted, now, now, attempt.ScaleSetID, attempt.ClaimKey,
		attempt.Attempt, GitHubJITStartDispatching)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrGitHubJITState
	}
	result, err = tx.ExecContext(ctx, `UPDATE github_job_claims SET state = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ?
			AND current_jit_attempt = ? AND state = ?`,
		claimState, now, now, attempt.ScaleSetID, attempt.ClaimKey,
		attempt.Attempt, GitHubClaimStartDispatching)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrGitHubClaimState
	}
	return tx.Commit()
}

func (s *ControllerStore) transitionGitHubJITAny(
	ctx context.Context,
	attempt GitHubJITAttempt,
	expected []GitHubJITAttemptState,
	next GitHubJITAttemptState,
	claimState GitHubClaimState,
	authorityEpoch domain.ControllerEpoch,
	reconciliation bool,
	requireAdmission bool,
) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	currentEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return err
	}
	if domain.ControllerEpoch(currentEpoch) != authorityEpoch ||
		(!reconciliation && authorityEpoch != attempt.ControllerEpoch) ||
		(reconciliation && authorityEpoch <= attempt.ControllerEpoch) {
		return ErrStaleControllerEpoch
	}
	current, found, err := loadGitHubJITAttempt(ctx, tx, attempt.ScaleSetID,
		attempt.ClaimKey, attempt.Attempt)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubJITState
		}
		return err
	}
	allowed := false
	for _, state := range expected {
		allowed = allowed || current.State == state
	}
	if !allowed || current.RunnerName != attempt.RunnerName ||
		current.ControllerEpoch != attempt.ControllerEpoch {
		return ErrGitHubJITState
	}
	claim, found, err := loadGitHubClaim(
		ctx, tx, attempt.ScaleSetID, attempt.ClaimKey)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return err
	}
	if claim.CurrentAttempt != attempt.Attempt ||
		!gitHubClaimMatchesJITState(claim.State, current.State) {
		return ErrGitHubClaimState
	}
	if requireAdmission {
		if claim.Execution.State != domain.ExecutionPreparing {
			return ErrGitHubClaimState
		}
		if err := requireActiveGitHubClaimLease(ctx, tx, claim); err != nil {
			return err
		}
	}
	if (current.RunnerID != 0 && current.RunnerID != attempt.RunnerID) ||
		(current.JITDigest != "" && current.JITDigest != attempt.JITDigest) ||
		(current.StartCommandID != "" && current.StartCommandID != attempt.StartCommandID) {
		// Once GitHub has issued JIT identity, later dispatch and reconciliation
		// transitions may preserve or clear it, but must never replace it with
		// caller-supplied identity from a different runner attempt.
		return ErrGitHubJITState
	}
	runnerID := attempt.RunnerID
	jitDigest := attempt.JITDigest
	startCommandID := attempt.StartCommandID
	if next == GitHubJITReconciledAbsent {
		runnerID = 0
		jitDigest = ""
		startCommandID = ""
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	var nullableRunner any
	var nullableDigest any
	if runnerID > 0 {
		nullableRunner = runnerID
	}
	if jitDigest != "" {
		nullableDigest = jitDigest
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_jit_attempts SET
		state = ?, runner_id = ?, jit_digest = ?, start_command_id = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND claim_key = ? AND attempt = ? AND state = ?`,
		next, nullableRunner, nullableDigest, startCommandID, now, now,
		attempt.ScaleSetID, attempt.ClaimKey, attempt.Attempt, current.State)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrGitHubJITState
	}
	result, err = tx.ExecContext(ctx, `UPDATE github_job_claims SET state = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
			WHERE scale_set_id = ? AND claim_key = ?
				AND current_jit_attempt = ? AND state = ?`,
		claimState, now, now, attempt.ScaleSetID, attempt.ClaimKey,
		attempt.Attempt, claim.State)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrGitHubClaimState
	}
	return tx.Commit()
}

func gitHubClaimMatchesJITState(
	claimState GitHubClaimState,
	attemptState GitHubJITAttemptState,
) bool {
	switch attemptState {
	case GitHubJITIntent:
		return claimState == GitHubClaimJITIntent
	case GitHubJITGenerationAmbiguous:
		return claimState == GitHubClaimJITGenerationAmbiguous
	case GitHubJITGenerated:
		return claimState == GitHubClaimJITGenerated
	case GitHubJITStartDispatching:
		return claimState == GitHubClaimStartDispatching
	case GitHubJITStartAmbiguous:
		return claimState == GitHubClaimStartAmbiguous
	case GitHubJITStarted:
		return claimState == GitHubClaimRunning ||
			claimState == GitHubClaimReconciliationRequired
	case GitHubJITAgentAccepted, GitHubJITRemovalPending:
		return claimState == GitHubClaimReconciliationRequired
	case GitHubJITReconciledAbsent:
		return claimState == GitHubClaimPreparing ||
			claimState == GitHubClaimReconciliationRequired
	default:
		return false
	}
}

func readGitHubReconciliationFences(
	ctx context.Context,
	tx *sql.Tx,
	controllerEpoch domain.ControllerEpoch,
) ([]GitHubReconciliationFence, error) {
	rows, err := tx.QueryContext(ctx, githubClaimSelect+`
		WHERE c.state IN (
			'acquire_ambiguous',
			'jit_intent',
			'jit_generation_ambiguous',
			'jit_generated',
			'start_dispatching',
			'start_ambiguous',
			'reconciliation_required'
		)
		AND NOT (
			c.state = 'reconciliation_required'
			AND EXISTS (
				SELECT 1 FROM github_jit_attempts current_attempt
				WHERE current_attempt.scale_set_id = c.scale_set_id
					AND current_attempt.claim_key = c.claim_key
					AND current_attempt.attempt = c.current_jit_attempt
					AND current_attempt.state = 'reconciled_absent'
			)
		)
		ORDER BY c.scale_set_id, c.claim_key`)
	if err != nil {
		return nil, err
	}
	var claims []GitHubJobClaim
	for rows.Next() {
		claim, err := scanGitHubClaim(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		// An assigned-demand claim has a negative key and, when it was created by
		// an empty long poll, no source message at all. scanGitHubClaim has
		// already proven the identity is one of the two legal shapes.
		if claim.ScaleSetID == 0 || claim.ClaimKey == 0 ||
			(claim.Origin == GitHubClaimFromJobAvailable &&
				claim.SourceMessageID == 0) {
			rows.Close()
			return nil, errors.New("stored GitHub reconciliation claim identity is invalid")
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	result := make([]GitHubReconciliationFence, 0, len(claims))
	for _, claim := range claims {
		fence := GitHubReconciliationFence{Claim: claim}
		if claim.State == GitHubClaimAcquireAmbiguous {
			if claim.CurrentAttempt != 0 {
				return nil, errors.New("stored acquire ambiguity unexpectedly owns a JIT attempt")
			}
			result = append(result, fence)
			continue
		}
		if claim.CurrentAttempt < 1 {
			return nil, errors.New("stored GitHub reconciliation claim has no JIT attempt")
		}
		attempt, found, err := loadGitHubJITAttempt(
			ctx,
			tx,
			claim.ScaleSetID,
			claim.ClaimKey,
			claim.CurrentAttempt,
		)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.New("stored GitHub reconciliation JIT attempt is missing")
		}
		if err := validateRestartGitHubJITAttempt(
			attempt,
			claim.State,
			controllerEpoch,
		); err != nil {
			return nil, err
		}
		fence.Attempt = &attempt
		result = append(result, fence)
	}
	return result, nil
}

func validateRestartGitHubJITAttempt(
	attempt GitHubJITAttempt,
	claimState GitHubClaimState,
	controllerEpoch domain.ControllerEpoch,
) error {
	if attempt.ScaleSetID == 0 ||
		attempt.ClaimKey == 0 ||
		attempt.Attempt < 1 ||
		attempt.ControllerEpoch == 0 ||
		attempt.ControllerEpoch > controllerEpoch ||
		strings.TrimSpace(attempt.RunnerName) == "" {
		return errors.New("stored GitHub reconciliation JIT identity is invalid")
	}
	if !gitHubClaimMatchesJITState(claimState, attempt.State) {
		return errors.New("stored GitHub reconciliation claim and JIT states are inconsistent")
	}
	switch attempt.State {
	case GitHubJITIntent, GitHubJITGenerationAmbiguous:
		if attempt.RunnerID != 0 || attempt.JITDigest != "" ||
			attempt.StartCommandID != "" {
			return errors.New("stored pre-generation JIT attempt carries runner material")
		}
	case GitHubJITGenerated,
		GitHubJITStartDispatching,
		GitHubJITStartAmbiguous,
		GitHubJITStarted,
		GitHubJITAgentAccepted:
		if attempt.RunnerID <= 0 ||
			!isLowerSHA256(attempt.JITDigest) ||
			attempt.StartCommandID == "" {
			return errors.New("stored generated JIT attempt identity is incomplete")
		}
	case GitHubJITRemovalPending:
		if attempt.RunnerID <= 0 {
			return errors.New("stored runner-removal JIT attempt has no runner identity")
		}
	default:
		return errors.New("stored JIT attempt is not a reconciliation fence")
	}
	return nil
}

const githubClaimSelect = `SELECT c.scale_set_id, c.claim_key, c.origin,
	c.runner_request_id, c.source_message_id, e.id, e.target_id, e.node_id,
	e.slot_index, e.state, c.state, c.current_jit_attempt
	FROM github_job_claims c JOIN executions e ON e.id = c.execution_id`

func loadGitHubClaim(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	scaleSetID ScaleSetID,
	claimKey int64,
) (GitHubJobClaim, bool, error) {
	row := queryer.QueryRowContext(ctx, githubClaimSelect+`
		WHERE c.scale_set_id = ? AND c.claim_key = ?`,
		scaleSetID, claimKey)
	claim, err := scanGitHubClaim(row)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubJobClaim{}, false, nil
	}
	return claim, err == nil, err
}

type rowScanner interface {
	Scan(...any) error
}

func scanGitHubClaim(row rowScanner) (GitHubJobClaim, error) {
	var claim GitHubJobClaim
	var runnerRequestID sql.NullInt64
	var sourceMessageID sql.NullInt64
	err := row.Scan(&claim.ScaleSetID, &claim.ClaimKey, &claim.Origin,
		&runnerRequestID, &sourceMessageID, &claim.Execution.ID,
		&claim.Execution.TargetID, &claim.Execution.Slot.NodeID,
		&claim.Execution.Slot.Index, &claim.Execution.State, &claim.State,
		&claim.CurrentAttempt)
	if err != nil {
		return GitHubJobClaim{}, err
	}
	if runnerRequestID.Valid {
		claim.RunnerRequestID = runnerRequestID.Int64
	}
	if sourceMessageID.Valid {
		claim.SourceMessageID = MessageID(sourceMessageID.Int64)
	}
	if err := validateGitHubClaimIdentity(claim); err != nil {
		return GitHubJobClaim{}, err
	}
	if err := claim.Execution.Validate(); err != nil {
		return GitHubJobClaim{}, err
	}
	return claim, nil
}

func loadGitHubJITAttempt(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	scaleSetID ScaleSetID,
	claimKey int64,
	attempt int,
) (GitHubJITAttempt, bool, error) {
	var result GitHubJITAttempt
	var runnerID sql.NullInt64
	var jitDigest sql.NullString
	err := queryer.QueryRowContext(ctx, `SELECT scale_set_id, claim_key,
		attempt, controller_epoch, runner_name, state, runner_id, jit_digest, start_command_id
		FROM github_jit_attempts WHERE scale_set_id = ? AND claim_key = ?
			AND attempt = ?`, scaleSetID, claimKey, attempt).Scan(
		&result.ScaleSetID, &result.ClaimKey, &result.Attempt,
		&result.ControllerEpoch, &result.RunnerName, &result.State, &runnerID, &jitDigest,
		&result.StartCommandID)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubJITAttempt{}, false, nil
	}
	if err != nil {
		return GitHubJITAttempt{}, false, err
	}
	if runnerID.Valid {
		result.RunnerID = int(runnerID.Int64)
	}
	if jitDigest.Valid {
		result.JITDigest = jitDigest.String
	}
	return result, true, nil
}
