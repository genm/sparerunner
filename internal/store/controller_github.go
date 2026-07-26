package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/genm/tewake/internal/domain"
)

type GitHubJobEventType string

const (
	GitHubJobAvailable GitHubJobEventType = "JobAvailable"
	GitHubJobAssigned  GitHubJobEventType = "JobAssigned"
	GitHubJobStarted   GitHubJobEventType = "JobStarted"
	GitHubJobCompleted GitHubJobEventType = "JobCompleted"
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
	Type            GitHubJobEventType
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
	TargetID     domain.TargetID
	NodeID       domain.NodeID
	Slot         int
	ClaimEnabled bool
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

type GitHubJobClaim struct {
	ScaleSetID      ScaleSetID
	RunnerRequestID int64
	SourceMessageID MessageID
	Execution       domain.ExecutionSnapshot
	State           GitHubClaimState
	CurrentAttempt  int
}

type GitHubMessageCommit struct {
	Replayed           bool
	Claim              *GitHubJobClaim
	UnclaimedAvailable bool
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
	RunnerRequestID int64
	Attempt         int
	ControllerEpoch domain.ControllerEpoch
	RunnerName      string
	State           GitHubJITAttemptState
	RunnerID        int
	JITDigest       string
	StartCommandID  domain.CommandID
}

var (
	ErrGitHubClaimState = errors.New("GitHub job claim state conflict")
	ErrGitHubJITState   = errors.New("GitHub JIT attempt state conflict")
)

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
	claimed, unclaimedAvailable, err := resolveGitHubAvailableClaim(
		ctx, tx, message, binding, committedAt)
	if err != nil {
		return GitHubMessageCommit{}, err
	}
	if err := tx.Commit(); err != nil {
		return GitHubMessageCommit{}, err
	}
	return GitHubMessageCommit{
		Replayed:           replayed,
		Claim:              claimed,
		UnclaimedAvailable: unclaimedAvailable,
	}, nil
}

func resolveGitHubAvailableClaim(
	ctx context.Context,
	tx *sql.Tx,
	message GitHubQueueMessage,
	binding SingleSlotBinding,
	committedAt int64,
) (*GitHubJobClaim, bool, error) {
	var resolved *GitHubJobClaim
	unclaimed := make([]GitHubJobEvent, 0)
	seen := make(map[int64]struct{})
	for _, job := range message.Jobs {
		if job.Type != GitHubJobAvailable {
			continue
		}
		if _, duplicate := seen[job.RunnerRequestID]; duplicate {
			continue
		}
		seen[job.RunnerRequestID] = struct{}{}
		existing, found, err := loadGitHubClaim(ctx, tx, message.ScaleSetID, job.RunnerRequestID)
		if err != nil {
			return nil, true, err
		}
		if !found {
			unclaimed = append(unclaimed, job)
			continue
		}
		if existing.Execution.TargetID != binding.TargetID ||
			existing.Execution.Slot.NodeID != binding.NodeID ||
			existing.Execution.Slot.Index != binding.Slot {
			return nil, true, fmt.Errorf("%w: GitHub available job binding changed", ErrReplayMismatch)
		}
		if resolved == nil {
			value := existing
			resolved = &value
		}
	}
	if len(unclaimed) == 0 {
		return resolved, false, nil
	}
	if len(unclaimed) > 1 {
		// The TWK-007 vertical owns one concrete slot and may not partially
		// acknowledge a message containing more independent availability than it
		// can durably claim. The pinned poll requests capacity one, so this is a
		// fail-closed preview-contract violation.
		return resolved, true, nil
	}
	if !binding.ClaimEnabled {
		return resolved, true, nil
	}
	slotFree, err := githubSlotAvailable(ctx, tx, binding)
	if err != nil || !slotFree {
		return resolved, true, err
	}
	job := unclaimed[0]
	execution := domain.ExecutionSnapshot{
		ID:       job.ExecutionID,
		TargetID: binding.TargetID,
		Slot:     domain.SlotKey{NodeID: binding.NodeID, Index: binding.Slot},
		State:    domain.ExecutionReserved,
	}
	if err := execution.Validate(); err != nil {
		return nil, true, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO executions(
			id, target_id, node_id, slot_index, state, created_at_unix_nano
		) VALUES (?, ?, ?, ?, ?, ?)`, execution.ID, binding.TargetID,
		binding.NodeID, binding.Slot, execution.State, committedAt); err != nil {
		return nil, true, mapAssignmentError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO slot_reservations(
			node_id, slot_index, target_id, execution_id
		) VALUES (?, ?, ?, ?)`, binding.NodeID, binding.Slot,
		binding.TargetID, execution.ID); err != nil {
		return nil, true, mapAssignmentError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO github_job_claims(
			scale_set_id, runner_request_id, source_message_id, execution_id,
			state, current_jit_attempt, created_at_unix_nano, updated_at_unix_nano
		) VALUES (?, ?, ?, ?, ?, 0, ?, ?)`, message.ScaleSetID,
		job.RunnerRequestID, message.MessageID, execution.ID,
		GitHubClaimPending, committedAt, committedAt); err != nil {
		return nil, true, err
	}
	return &GitHubJobClaim{
		ScaleSetID:      message.ScaleSetID,
		RunnerRequestID: job.RunnerRequestID,
		SourceMessageID: message.MessageID,
		Execution:       execution,
		State:           GitHubClaimPending,
	}, false, nil
}

func validateGitHubQueueMessage(message GitHubQueueMessage) error {
	if message.ScaleSetID == 0 || message.MessageID == 0 ||
		uint64(message.ScaleSetID) > maxSQLiteInteger ||
		uint64(message.MessageID) > maxSQLiteInteger ||
		!isLowerSHA256(message.Digest) {
		return errors.New("GitHub queue message identity is invalid")
	}
	for _, job := range message.Jobs {
		if job.RunnerRequestID <= 0 || uint64(job.RunnerRequestID) > maxSQLiteInteger ||
			job.WorkflowRunID < 0 {
			return errors.New("GitHub job event identity is invalid")
		}
		switch job.Type {
		case GitHubJobAvailable:
			if job.RunnerID != 0 || job.RunnerName != "" || job.ExecutionID == "" {
				return errors.New("available GitHub job event is invalid")
			}
		case GitHubJobAssigned:
			if job.RunnerID != 0 || job.RunnerName != "" {
				return errors.New("assigned GitHub job event is invalid")
			}
		case GitHubJobStarted, GitHubJobCompleted:
			if job.RunnerID <= 0 || job.RunnerName == "" {
				return errors.New("started or completed GitHub job event is invalid")
			}
		default:
			return errors.New("GitHub job event type is invalid")
		}
	}
	return nil
}

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
			)`, binding.NodeID, binding.Slot).Scan(&count); err != nil {
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
		ORDER BY c.created_at_unix_nano, c.runner_request_id LIMIT 1`, scaleSetID)
	claim, err := scanGitHubClaim(row)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubJobClaim{}, false, nil
	}
	return claim, err == nil, err
}

func (s *ControllerStore) GitHubClaim(ctx context.Context, scaleSetID ScaleSetID, runnerRequestID int64) (GitHubJobClaim, bool, error) {
	if err := s.requireReady(); err != nil {
		return GitHubJobClaim{}, false, err
	}
	return loadGitHubClaim(ctx, s.db, scaleSetID, runnerRequestID)
}

func (s *ControllerStore) BeginGitHubAcquire(ctx context.Context, scaleSetID ScaleSetID, runnerRequestID int64) error {
	return s.transitionGitHubClaim(ctx, scaleSetID, runnerRequestID,
		[]GitHubClaimState{GitHubClaimPending}, GitHubClaimAcquireAmbiguous, true)
}

func (s *ControllerStore) MarkGitHubAcquired(ctx context.Context, scaleSetID ScaleSetID, runnerRequestID int64) error {
	return s.transitionGitHubClaim(ctx, scaleSetID, runnerRequestID,
		[]GitHubClaimState{GitHubClaimAcquireAmbiguous}, GitHubClaimAcquired, false)
}

func (s *ControllerStore) MarkGitHubPreparing(ctx context.Context, scaleSetID ScaleSetID, runnerRequestID int64) error {
	return s.transitionGitHubClaimWithExecution(ctx, scaleSetID, runnerRequestID,
		[]GitHubClaimState{GitHubClaimAcquired}, GitHubClaimPreparing,
		domain.ExecutionPreparing)
}

func (s *ControllerStore) MarkGitHubPrepareFailed(ctx context.Context, scaleSetID ScaleSetID, runnerRequestID int64) error {
	return s.transitionGitHubClaimWithExecution(ctx, scaleSetID, runnerRequestID,
		[]GitHubClaimState{GitHubClaimAcquired}, GitHubClaimPrepareFailed,
		domain.ExecutionFailed)
}

func (s *ControllerStore) BeginGitHubJITAttempt(
	ctx context.Context,
	scaleSetID ScaleSetID,
	runnerRequestID int64,
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
	claim, found, err := loadGitHubClaim(ctx, tx, scaleSetID, runnerRequestID)
	if err != nil || !found {
		if err == nil {
			err = sql.ErrNoRows
		}
		return GitHubJITAttempt{}, false, err
	}
	if claim.State != GitHubClaimPreparing {
		if claim.CurrentAttempt > 0 {
			attempt, attemptFound, attemptErr := loadGitHubJITAttempt(ctx, tx, scaleSetID, runnerRequestID, claim.CurrentAttempt)
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
		scale_set_id, runner_request_id, attempt, controller_epoch, runner_name, state, runner_id,
		jit_digest, start_command_id, created_at_unix_nano, updated_at_unix_nano
	) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, '', ?, ?)`, scaleSetID,
		runnerRequestID, attemptNumber, controllerEpoch, runnerName, GitHubJITIntent, now, now); err != nil {
		return GitHubJITAttempt{}, false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE github_job_claims SET
		state = ?, current_jit_attempt = ?, updated_at_unix_nano = ?
		WHERE scale_set_id = ? AND runner_request_id = ? AND state = ?`,
		GitHubClaimJITIntent, attemptNumber, now, scaleSetID,
		runnerRequestID, GitHubClaimPreparing)
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
		ScaleSetID: scaleSetID, RunnerRequestID: runnerRequestID,
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
) error {
	return s.transitionGitHubJITAny(ctx, attempt,
		[]GitHubJITAttemptState{GitHubJITGenerated, GitHubJITStartDispatching, GitHubJITStartAmbiguous},
		GitHubJITAgentAccepted, GitHubClaimReconciliationRequired,
		reconciliationEpoch, true, false)
}

func (s *ControllerStore) MarkGitHubJITRemovalPending(
	ctx context.Context,
	attempt GitHubJITAttempt,
	reconciliationEpoch domain.ControllerEpoch,
) error {
	return s.transitionGitHubJITAny(ctx, attempt,
		[]GitHubJITAttemptState{
			GitHubJITIntent, GitHubJITGenerationAmbiguous, GitHubJITGenerated,
			GitHubJITStartDispatching, GitHubJITStartAmbiguous, GitHubJITRemovalPending,
		},
		GitHubJITRemovalPending, GitHubClaimReconciliationRequired,
		reconciliationEpoch, true, false)
}

func (s *ControllerStore) MarkGitHubJITReconciledAbsent(
	ctx context.Context,
	attempt GitHubJITAttempt,
	reconciliationEpoch domain.ControllerEpoch,
) error {
	return s.transitionGitHubJITAny(ctx, attempt,
		[]GitHubJITAttemptState{
			GitHubJITIntent, GitHubJITGenerationAmbiguous, GitHubJITGenerated,
			GitHubJITStartDispatching, GitHubJITStartAmbiguous, GitHubJITRemovalPending,
		},
		GitHubJITReconciledAbsent, GitHubClaimPreparing,
		reconciliationEpoch, true, false)
}

func (s *ControllerStore) CurrentGitHubJITAttempt(ctx context.Context, scaleSetID ScaleSetID, runnerRequestID int64) (GitHubJITAttempt, bool, error) {
	if err := s.requireReady(); err != nil {
		return GitHubJITAttempt{}, false, err
	}
	var attempt int
	err := s.db.QueryRowContext(ctx, `SELECT current_jit_attempt FROM github_job_claims
		WHERE scale_set_id = ? AND runner_request_id = ?`,
		scaleSetID, runnerRequestID).Scan(&attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return GitHubJITAttempt{}, false, nil
	}
	if err != nil {
		return GitHubJITAttempt{}, false, err
	}
	if attempt == 0 {
		return GitHubJITAttempt{}, false, nil
	}
	return loadGitHubJITAttempt(ctx, s.db, scaleSetID, runnerRequestID, attempt)
}

func (s *ControllerStore) transitionGitHubClaim(
	ctx context.Context,
	scaleSetID ScaleSetID,
	runnerRequestID int64,
	expected []GitHubClaimState,
	next GitHubClaimState,
	requireAdmission bool,
) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if len(expected) == 0 {
		return ErrGitHubClaimState
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	claim, found, err := loadGitHubClaim(ctx, tx, scaleSetID, runnerRequestID)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubClaimState
		}
		return err
	}
	allowed := false
	for _, state := range expected {
		allowed = allowed || claim.State == state
	}
	if !allowed {
		return ErrGitHubClaimState
	}
	if requireAdmission {
		if claim.Execution.State != domain.ExecutionReserved {
			return ErrGitHubClaimState
		}
		if err := requireActiveGitHubClaimLease(ctx, tx, claim); err != nil {
			return err
		}
	}
	now, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(expected)), ",")
	query := `UPDATE github_job_claims SET state = ?, updated_at_unix_nano =
		CASE WHEN updated_at_unix_nano >= ? THEN updated_at_unix_nano + 1 ELSE ? END
		WHERE scale_set_id = ? AND runner_request_id = ? AND state IN (` + placeholders + `)`
	args := []any{next, now, now, scaleSetID, runnerRequestID}
	for _, state := range expected {
		args = append(args, state)
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrGitHubClaimState
	}
	return tx.Commit()
}

func (s *ControllerStore) transitionGitHubClaimWithExecution(
	ctx context.Context,
	scaleSetID ScaleSetID,
	runnerRequestID int64,
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
	claim, found, err := loadGitHubClaim(ctx, tx, scaleSetID, runnerRequestID)
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
		WHERE scale_set_id = ? AND runner_request_id = ? AND state = ?`,
		next, now, now, scaleSetID, runnerRequestID, claim.State)
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
		ScaleSetID: attempt.ScaleSetID, RunnerRequestID: attempt.RunnerRequestID,
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
		ctx, tx, attempt.ScaleSetID, attempt.RunnerRequestID, attempt.Attempt)
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
		ctx, tx, attempt.ScaleSetID, attempt.RunnerRequestID)
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

	// The Agent commits Running before it returns the Start result. A very short
	// job may then commit its terminal cleanup update before this Controller
	// transaction begins. Preserve that newer execution authority and record only
	// that the JIT runner did start; never regress the claim to a false Running
	// projection. CleanupFailed/Quarantined continue to retain their slot lease.
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
		WHERE scale_set_id = ? AND runner_request_id = ? AND attempt = ?
			AND state = ?`,
		GitHubJITStarted, now, now, attempt.ScaleSetID, attempt.RunnerRequestID,
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
		WHERE scale_set_id = ? AND runner_request_id = ?
			AND current_jit_attempt = ? AND state = ?`,
		claimState, now, now, attempt.ScaleSetID, attempt.RunnerRequestID,
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
		attempt.RunnerRequestID, attempt.Attempt)
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
		ctx, tx, attempt.ScaleSetID, attempt.RunnerRequestID)
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
	if next != GitHubJITReconciledAbsent &&
		((current.RunnerID != 0 && current.RunnerID != attempt.RunnerID) ||
			(current.JITDigest != "" && current.JITDigest != attempt.JITDigest) ||
			(current.StartCommandID != "" && current.StartCommandID != attempt.StartCommandID)) {
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
		WHERE scale_set_id = ? AND runner_request_id = ? AND attempt = ? AND state = ?`,
		next, nullableRunner, nullableDigest, startCommandID, now, now,
		attempt.ScaleSetID, attempt.RunnerRequestID, attempt.Attempt, current.State)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrGitHubJITState
	}
	result, err = tx.ExecContext(ctx, `UPDATE github_job_claims SET state = ?,
		updated_at_unix_nano = CASE WHEN updated_at_unix_nano >= ?
			THEN updated_at_unix_nano + 1 ELSE ? END
			WHERE scale_set_id = ? AND runner_request_id = ?
				AND current_jit_attempt = ? AND state = ?`,
		claimState, now, now, attempt.ScaleSetID, attempt.RunnerRequestID,
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
		return claimState == GitHubClaimPreparing
	default:
		return false
	}
}

const githubClaimSelect = `SELECT c.scale_set_id, c.runner_request_id,
	c.source_message_id, e.id, e.target_id, e.node_id, e.slot_index, e.state,
	c.state, c.current_jit_attempt
	FROM github_job_claims c JOIN executions e ON e.id = c.execution_id`

func loadGitHubClaim(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	scaleSetID ScaleSetID,
	runnerRequestID int64,
) (GitHubJobClaim, bool, error) {
	row := queryer.QueryRowContext(ctx, githubClaimSelect+`
		WHERE c.scale_set_id = ? AND c.runner_request_id = ?`,
		scaleSetID, runnerRequestID)
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
	err := row.Scan(&claim.ScaleSetID, &claim.RunnerRequestID,
		&claim.SourceMessageID, &claim.Execution.ID, &claim.Execution.TargetID,
		&claim.Execution.Slot.NodeID, &claim.Execution.Slot.Index,
		&claim.Execution.State, &claim.State, &claim.CurrentAttempt)
	if err != nil {
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
	runnerRequestID int64,
	attempt int,
) (GitHubJITAttempt, bool, error) {
	var result GitHubJITAttempt
	var runnerID sql.NullInt64
	var jitDigest sql.NullString
	err := queryer.QueryRowContext(ctx, `SELECT scale_set_id, runner_request_id,
		attempt, controller_epoch, runner_name, state, runner_id, jit_digest, start_command_id
		FROM github_jit_attempts WHERE scale_set_id = ? AND runner_request_id = ?
			AND attempt = ?`, scaleSetID, runnerRequestID, attempt).Scan(
		&result.ScaleSetID, &result.RunnerRequestID, &result.Attempt,
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
