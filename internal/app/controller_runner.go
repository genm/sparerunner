package app

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/reconcile"
	"github.com/genm/sparerunner/internal/runner"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

var (
	ErrControllerRunnerConfig       = errors.New("controller runner coordinator configuration is invalid")
	ErrGitHubAcquireAmbiguous       = errors.New("GitHub job acquisition is ambiguous")
	ErrGitHubAvailableUnclaimed     = errors.New("GitHub available job has no eligible durable slot claim")
	ErrGitHubPrepareFailed          = errors.New("Agent runner preparation failed")
	ErrGitHubStartAmbiguous         = errors.New("Agent runner start is ambiguous")
	ErrGitHubReconciliationRequired = errors.New("GitHub runner reconciliation is required")
	ErrGitHubReconciliationPending  = errors.New("GitHub runner reconciliation needs another observation")
	ErrGitHubSessionFailureStore    = errors.New("GitHub session failure state could not be persisted")
	ErrControllerRunnerAdmission    = errors.New("controller runner admission is unavailable")
	// ErrControllerRunnerNoCandidate reports that the Target currently has no
	// node the coordinator could even evaluate for capacity. It is a normal,
	// recoverable state (an empty or fully removed fleet), never a fatal one.
	ErrControllerRunnerNoCandidate = errors.New("controller runner target has no candidate node")
)

const (
	controllerRunnerWorkFolder   = "_work"
	controllerRunnerRetryInitial = 250 * time.Millisecond
	controllerRunnerRetryMaximum = 5 * time.Second
)

type controllerRunnerStore interface {
	ManagementAuditHealthy() bool
	ManagementAuditChange() <-chan struct{}
	ReadManagementConfiguration(context.Context) (store.ManagementConfiguration, error)
	RecordGitHubSessionDemand(context.Context, store.GitHubSessionDemand) error
	RecordGitHubScaleSetSessionSuccess(context.Context, store.ScaleSetID) (store.GitHubScaleSetSessionHealth, error)
	RecordGitHubScaleSetSessionFailure(context.Context, store.ScaleSetID, store.GitHubObservationFailureClass) (store.GitHubScaleSetSessionHealth, error)
	ReadGitHubScaleSetSessionHealth(context.Context, store.ScaleSetID) (store.GitHubScaleSetSessionHealth, error)
	ReadGitHubPollState(context.Context, store.GitHubTargetRuntimeBinding, domain.NodeID) (store.GitHubPollState, error)
	CommitGitHubQueueMessage(context.Context, store.GitHubQueueMessage, store.SingleSlotBinding) (store.GitHubMessageCommit, error)
	GitHubSingleSlotCapacity(context.Context, store.SingleSlotBinding) (int, error)
	NextActionableGitHubClaim(context.Context, store.ScaleSetID) (store.GitHubJobClaim, bool, error)
	GitHubPendingClaimDispatchReady(context.Context, store.GitHubJobClaim) (bool, error)
	GitHubClaim(context.Context, store.ScaleSetID, int64) (store.GitHubJobClaim, bool, error)
	BeginGitHubAcquire(context.Context, store.ScaleSetID, int64) (store.GitHubAcquireAttempt, error)
	MarkGitHubAcquired(context.Context, store.GitHubAcquireAttempt) error
	MarkGitHubPreparing(context.Context, store.ScaleSetID, int64) error
	MarkGitHubPrepareFailed(context.Context, store.ScaleSetID, int64) error
	BeginGitHubJITAttempt(context.Context, store.ScaleSetID, int64, domain.ControllerEpoch, string) (store.GitHubJITAttempt, bool, error)
	MarkGitHubJITGenerationAmbiguous(context.Context, store.GitHubJITAttempt) error
	MarkGitHubJITGenerated(context.Context, store.GitHubJITAttempt, int, string, domain.CommandID) error
	BeginGitHubStartDispatch(context.Context, store.GitHubJITAttempt) error
	MarkGitHubStartAmbiguous(context.Context, store.GitHubJITAttempt) error
	MarkGitHubRunning(context.Context, store.GitHubJITAttempt) error
	CurrentGitHubJITAttempt(context.Context, store.ScaleSetID, int64) (store.GitHubJITAttempt, bool, error)
	GitHubUnpickedRequeueIntent(context.Context, store.ScaleSetID, int64) (store.GitHubUnpickedRequeueIntent, bool, error)
	IssuedAgentCommand(context.Context, domain.CommandID) (store.IssuedAgentCommand, bool, error)
	AdoptAgentSnapshotObservation(context.Context, domain.NodeID, domain.ExecutionState, store.ObservationSnapshot, string, domain.ControllerEpoch) (domain.ExecutionSnapshot, bool, error)
	FailDesiredExecutionFromSnapshot(context.Context, domain.NodeID, domain.ExecutionID, domain.ExecutionState, string, domain.ControllerEpoch) (domain.ExecutionSnapshot, error)
	MarkGitHubJITAgentAccepted(context.Context, store.GitHubJITAttempt, domain.ControllerEpoch, string) error
	ReconcileGitHubJITPrunedHistory(context.Context, store.GitHubJITAttempt, domain.ControllerEpoch, string) (store.GitHubJITPrunedHistoryResult, error)
	MarkGitHubJITObservedStarted(context.Context, store.GitHubJITAttempt, domain.ControllerEpoch, store.ObservationSnapshot, string) error
	MarkGitHubJITRemovalPending(context.Context, store.GitHubJITAttempt, domain.ControllerEpoch, string, uint64, bool) error
	MarkGitHubJITReconciledAbsent(context.Context, store.GitHubJITAttempt, domain.ControllerEpoch, string, uint64) (store.GitHubJITAbsenceResult, error)
	NextGitHubReconciliationFence(context.Context, store.ScaleSetID) (store.GitHubReconciliationFence, bool, error)
}

type controllerRunnerMessageSession interface {
	github.MessageSource
	AcquireJobs(context.Context, []int64) ([]int64, error)
}

type controllerRunnerAgent interface {
	SendPrepare(context.Context, domain.NodeID, transport.CommandMetadata, bool) (transport.ExecutionUpdate, error)
	SendStart(context.Context, domain.NodeID, transport.CommandMetadata, bool, runner.JITConfig) (transport.ExecutionUpdate, error)
	ReplayPrepare(context.Context, domain.NodeID, transport.CommandMetadata, bool, string) (transport.ExecutionUpdate, error)
	SendReconciliationCancel(context.Context, domain.NodeID, transport.CommandMetadata, string) (transport.ExecutionUpdate, error)
	Readiness(domain.NodeID) (AgentSnapshot, bool, context.Context)
}

type controllerRunnerReconciler interface {
	Admission(domain.NodeID) (reconcile.NodeAdmission, error)
	ApplyGitHubClaim(store.GitHubJobClaim) error
	ApplyGitHubFence(reconcile.GitHubFence) error
	ClearGitHubFence(reconcile.GitHubFence) error
	ApplyDesiredExecution(domain.ExecutionSnapshot) error
}

// ControllerRunnerLifecycle makes JIT generation testable without exposing the
// opaque body. The production adapter is NewGitHubClientRunnerLifecycle.
type ControllerRunnerLifecycle interface {
	GenerateJITConfig(context.Context, github.JITRequest) (runner.JITConfig, github.RunnerReference, error)
	QueryRunner(context.Context, github.RunnerQuery) (*github.RunnerReference, error)
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

func (lifecycle githubClientRunnerLifecycle) QueryRunner(
	ctx context.Context,
	query github.RunnerQuery,
) (*github.RunnerReference, error) {
	return lifecycle.client.QueryRunner(ctx, query)
}

func (lifecycle githubClientRunnerLifecycle) RemoveRunner(
	ctx context.Context,
	reference github.RunnerReference,
) error {
	return lifecycle.client.RemoveRunner(ctx, reference)
}

type ControllerRunnerConfig struct {
	ScaleSetID github.ScaleSetID
	TargetID   domain.TargetID
	// Scope and ScopeKind name the GitHub org/repo this coordinator serves. They
	// travel on prepare and start purely so a node owner's desktop surface can
	// say which scope a job belongs to; the Agent enforces on TargetID alone.
	Scope           string
	ScopeKind       domain.TargetScopeKind
	RunnerProfileID domain.RunnerProfileID
	VersionPolicy   domain.RunnerVersionPolicy
	// NodeID pins this coordinator to exactly one node. It is optional: an
	// empty value makes every configured node a candidate and the binding is
	// resolved per operation instead. The live acceptance rig sets it so its
	// evidence stays about one known machine.
	NodeID          domain.NodeID
	ControllerEpoch domain.ControllerEpoch
	Reconciler      controllerRunnerReconciler
}

type ControllerRunnerCoordinator struct {
	store                  controllerRunnerStore
	session                controllerRunnerMessageSession
	agents                 controllerRunnerAgent
	lifecycle              ControllerRunnerLifecycle
	config                 ControllerRunnerConfig
	poller                 *github.Poller
	logger                 *slog.Logger
	pollMu                 sync.Mutex
	driveMu                sync.Mutex
	finiteOperationContext func(context.Context) (context.Context, context.CancelFunc)
	reconciliationRetry    time.Duration
	authorityMu            sync.RWMutex
	activePollScope        controllerRunnerPollScope
	hasActivePollScope     bool
}

// controllerRunnerPollScope is the exact node selection one poll iteration
// committed to. CommitMessage runs inside that poll, so it must bind the same
// node the advertised authority was read for; claimable is false whenever no
// candidate node actually had durable capacity.
type controllerRunnerPollScope struct {
	authority store.GitHubPollClaimAuthority
	nodeID    domain.NodeID
	claimable bool
}

// controllerRunnerCandidate is one node's complete admission evidence for a
// single poll iteration. Capacity is already gated here, so demand and
// selection can never disagree about whether a node is usable.
type controllerRunnerCandidate struct {
	nodeID           domain.NodeID
	pollState        store.GitHubPollState
	capacity         int
	admission        reconcile.NodeAdmission
	readinessChanged context.Context
	recheckAt        time.Time
	runnerRecheckAt  time.Time
}

// NewControllerRunnerCoordinator creates the production-callable single-slot
// vertical for exactly one Target. A GitHub scale set exposes one message
// queue, so exactly one coordinator, session, and poller may exist per Target;
// the owning node is therefore resolved per operation rather than pinned.
func NewControllerRunnerCoordinator(
	stateStore controllerRunnerStore,
	session controllerRunnerMessageSession,
	agents controllerRunnerAgent,
	lifecycle ControllerRunnerLifecycle,
	config ControllerRunnerConfig,
	logger *slog.Logger,
) (*ControllerRunnerCoordinator, error) {
	if stateStore == nil || session == nil || agents == nil || lifecycle == nil ||
		config.ScaleSetID <= 0 || config.TargetID == "" ||
		config.RunnerProfileID == "" ||
		// Target identity is mandatory on the command wire, so a coordinator that
		// cannot name its own scope must never be constructed. Failing here keeps
		// the refusal at configuration time instead of at every dispatch.
		commandTargetFor(config).Validate() != nil ||
		config.ControllerEpoch.Validate() != nil || config.Reconciler == nil ||
		(config.VersionPolicy != domain.RunnerVersionAutoUpdate &&
			config.VersionPolicy != domain.RunnerVersionPinned) {
		return nil, ErrControllerRunnerConfig
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	coordinator := &ControllerRunnerCoordinator{
		store:                  stateStore,
		session:                session,
		agents:                 agents,
		lifecycle:              lifecycle,
		config:                 config,
		logger:                 logger,
		finiteOperationContext: github.WithFiniteOperationTimeout,
		reconciliationRetry:    store.GitHubRunnerAbsenceConfirmationDelay,
	}
	poller, err := github.NewPoller(session, coordinator, logger)
	if err != nil {
		return nil, err
	}
	coordinator.poller = poller
	return coordinator, nil
}

var _ github.DurableMessageHandler = (*ControllerRunnerCoordinator)(nil)

// commandTargetFor is the single place the coordinator's configured Target
// becomes command wire identity, so prepare, start, and Prepare replay cannot
// drift apart and produce different payload digests for the same execution.
func commandTargetFor(config ControllerRunnerConfig) transport.CommandTarget {
	return transport.CommandTarget{
		TargetID:  config.TargetID,
		Scope:     config.Scope,
		ScopeKind: config.ScopeKind,
	}
}

func (coordinator *ControllerRunnerCoordinator) commandTarget() transport.CommandTarget {
	return commandTargetFor(coordinator.config)
}

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

func (coordinator *ControllerRunnerCoordinator) CommitSessionHealthy(
	ctx context.Context,
	snapshot github.SessionSnapshot,
) error {
	if snapshot.ScaleSetID != coordinator.config.ScaleSetID {
		return github.ErrInvalidSession
	}
	_, err := coordinator.store.RecordGitHubScaleSetSessionSuccess(
		ctx,
		store.ScaleSetID(snapshot.ScaleSetID),
	)
	return err
}

func (coordinator *ControllerRunnerCoordinator) CommitMessage(
	ctx context.Context,
	message github.Message,
) error {
	// Queue evidence and provider cleanup share one lifecycle lock. This closes
	// the in-process window where an exact JobStarted event could commit after
	// the last pickup check but before RemoveRunner is issued.
	coordinator.driveMu.Lock()
	defer coordinator.driveMu.Unlock()
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
			event.ExecutionID = deterministicExecutionID(
				message.ScaleSetID,
				message.ID,
				job.RunnerRequestID,
			)
		}
		record.Jobs = append(record.Jobs, event)
	}
	binding := store.SingleSlotBinding{TargetID: coordinator.config.TargetID, Slot: 0}
	scope, hasScope := coordinator.currentPollScope()
	existingNode, hasExisting, err := coordinator.existingClaimNode(ctx, record)
	if err != nil {
		return err
	}
	switch {
	case hasExisting:
		binding.NodeID = existingNode
	case hasScope:
		binding.NodeID = scope.nodeID
	default:
		return ErrControllerRunnerNoCandidate
	}
	if hasScope {
		binding.PollAuthority = scope.authority
	}
	// A new claim may only be created for the node this poll actually selected
	// and advertised authority for. When the binding came from an existing
	// claim on a different node, the commit stays a pure replay.
	if hasScope && scope.claimable && scope.nodeID == binding.NodeID {
		admission, err := coordinator.config.Reconciler.Admission(binding.NodeID)
		if err != nil {
			return err
		}
		if snapshot, online, _ := coordinator.agents.Readiness(binding.NodeID); online {
			binding.ClaimEnabled = scope.authority.AdvertisedCapacity > 0 &&
				snapshot.NativeRunnerReady &&
				snapshot.RunnerVersion == runner.OfficialRunnerVersion &&
				admission.AllowsNewCapacity
		}
	}
	// Audit health may change while the provider long poll is returning. Check
	// again at the durable queue boundary so that message evidence is not
	// committed and acknowledged after management admission has failed closed.
	if !coordinator.store.ManagementAuditHealthy() {
		return ErrControllerRunnerAdmission
	}
	commit, err := coordinator.store.CommitGitHubQueueMessage(ctx, record, binding)
	if err != nil {
		if errors.Is(err, store.ErrManagementAuditPersistence) {
			return ErrControllerRunnerAdmission
		}
		if errors.Is(err, store.ErrGitHubRequeueTerminalPending) ||
			errors.Is(err, store.ErrGitHubRecoveryAvailabilityPending) {
			return errors.Join(ErrGitHubAvailableUnclaimed, err)
		}
		return err
	}
	if commit.Claim != nil {
		if err := coordinator.config.Reconciler.ApplyGitHubClaim(*commit.Claim); err != nil {
			return err
		}
	}
	if commit.RequeueIntent != nil {
		if err := coordinator.applyAttemptFence(
			commit.RequeueIntent.Claim,
			commit.RequeueIntent.Attempt,
			store.GitHubClaimReconciliationRequired,
		); err != nil {
			return err
		}
	}
	if commit.UnclaimedAvailable {
		return ErrGitHubAvailableUnclaimed
	}
	return nil
}

func (coordinator *ControllerRunnerCoordinator) currentPollScope() (
	controllerRunnerPollScope,
	bool,
) {
	coordinator.authorityMu.RLock()
	defer coordinator.authorityMu.RUnlock()
	return coordinator.activePollScope, coordinator.hasActivePollScope
}

func (coordinator *ControllerRunnerCoordinator) setPollScope(
	scope controllerRunnerPollScope,
) {
	coordinator.authorityMu.Lock()
	coordinator.activePollScope = scope
	coordinator.hasActivePollScope = true
	coordinator.authorityMu.Unlock()
}

func (coordinator *ControllerRunnerCoordinator) clearPollScope() {
	coordinator.authorityMu.Lock()
	coordinator.activePollScope = controllerRunnerPollScope{}
	coordinator.hasActivePollScope = false
	coordinator.authorityMu.Unlock()
}

// candidateNodes lists, in a stable order, every node this Target may bind a
// claim to. Ordering is the tie-break authority for selection: a restart or a
// redelivered message must resolve the same node, so the order can never come
// from a query plan or a map iteration.
func (coordinator *ControllerRunnerCoordinator) candidateNodes(
	ctx context.Context,
) ([]domain.NodeID, error) {
	if coordinator.config.NodeID != "" {
		return []domain.NodeID{coordinator.config.NodeID}, nil
	}
	configuration, err := coordinator.store.ReadManagementConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	nodes := make([]domain.NodeID, 0, len(configuration.Nodes))
	for _, node := range configuration.Nodes {
		if node.NodeID == "" {
			continue
		}
		nodes = append(nodes, node.NodeID)
	}
	slices.Sort(nodes)
	return slices.Compact(nodes), nil
}

// evaluateCandidates reads each candidate node's durable capacity and gates it
// with the same admission evidence a claim would need. Demand is the honest sum
// over those gated capacities; selected is the index of the first usable node in
// candidate order, or -1 when the Target currently has none.
func (coordinator *ControllerRunnerCoordinator) evaluateCandidates(
	ctx context.Context,
	now time.Time,
	auditHealthy bool,
) (candidates []controllerRunnerCandidate, demand int, selected int, err error) {
	nodes, err := coordinator.candidateNodes(ctx)
	if err != nil {
		return nil, 0, -1, err
	}
	selected = -1
	candidates = make([]controllerRunnerCandidate, 0, len(nodes))
	for _, nodeID := range nodes {
		candidate, err := coordinator.evaluateCandidate(ctx, nodeID, now, auditHealthy)
		if err != nil {
			return nil, 0, -1, err
		}
		demand += candidate.capacity
		if candidate.capacity > 0 && selected < 0 {
			selected = len(candidates)
		}
		candidates = append(candidates, candidate)
	}
	return candidates, demand, selected, nil
}

func (coordinator *ControllerRunnerCoordinator) evaluateCandidate(
	ctx context.Context,
	nodeID domain.NodeID,
	now time.Time,
	auditHealthy bool,
) (controllerRunnerCandidate, error) {
	pollState, err := coordinator.store.ReadGitHubPollState(
		ctx,
		coordinator.runtimeBinding(),
		nodeID,
	)
	if err != nil {
		return controllerRunnerCandidate{}, err
	}
	capacity, err := coordinator.store.GitHubSingleSlotCapacity(
		ctx,
		store.SingleSlotBinding{
			TargetID: coordinator.config.TargetID,
			NodeID:   nodeID,
			Slot:     0,
		},
	)
	if err != nil {
		return controllerRunnerCandidate{}, err
	}
	if !auditHealthy {
		capacity = 0
	}
	snapshot, online, readinessChanged := coordinator.agents.Readiness(nodeID)
	admission, err := coordinator.config.Reconciler.Admission(nodeID)
	if err != nil {
		return controllerRunnerCandidate{}, err
	}
	runnerUpdate, err := coordinator.evaluateRunnerUpdate(now, pollState.Runtime)
	if err != nil {
		return controllerRunnerCandidate{}, err
	}
	runnerRecheckAt := runnerUpdateRecheckAt(now, runnerUpdate)
	snapshotDigest := ""
	if online {
		snapshotDigest, err = transport.AgentSnapshotDigest(snapshot)
		if err != nil {
			return controllerRunnerCandidate{}, err
		}
	}
	durableAgent := pollState.ClaimAuthority.Agent
	durableAgentReady := durableAgent.HasSnapshot &&
		durableAgent.NativeRunnerReady &&
		durableAgent.AcceptedByControllerEpoch == coordinator.config.ControllerEpoch &&
		durableAgent.RunnerVersion == runner.OfficialRunnerVersion
	inMemoryAgentReady := online &&
		snapshot.NodeID == nodeID &&
		snapshot.NativeRunnerReady &&
		snapshot.RunnerVersion == runner.OfficialRunnerVersion &&
		snapshotDigest == durableAgent.SnapshotDigest
	providerReady := pollState.Runtime.Session.Freshness == store.RuntimeFreshnessFresh &&
		runnerUpdate.AllowsAdmissionAt(now)
	if capacity > 0 && (!durableAgentReady || !inMemoryAgentReady ||
		!providerReady || !admission.AllowsNewCapacity) {
		capacity = 0
	}
	return controllerRunnerCandidate{
		nodeID:           nodeID,
		pollState:        pollState,
		capacity:         capacity,
		admission:        admission,
		readinessChanged: readinessChanged,
		recheckAt: earliestControllerRunnerTime(
			admission.RecheckAt,
			runnerRecheckAt,
		),
		runnerRecheckAt: runnerRecheckAt,
	}, nil
}

// existingClaimNode returns the node that already owns a durable claim for one
// of this message's available jobs. Redelivery must reuse that node: the store
// rejects a claim whose binding moved, so re-running selection after a crash
// would turn an ordinary commit-before-ack replay into a replay mismatch.
func (coordinator *ControllerRunnerCoordinator) existingClaimNode(
	ctx context.Context,
	record store.GitHubQueueMessage,
) (domain.NodeID, bool, error) {
	for _, job := range record.Jobs {
		if job.Type != store.GitHubJobAvailable {
			continue
		}
		claim, found, err := coordinator.store.GitHubClaim(
			ctx,
			record.ScaleSetID,
			job.RunnerRequestID,
		)
		if err != nil {
			return "", false, err
		}
		if found && claim.Execution.Slot.NodeID != "" {
			return claim.Execution.Slot.NodeID, true, nil
		}
	}
	return "", false, nil
}

func (coordinator *ControllerRunnerCoordinator) runtimeBinding() store.GitHubTargetRuntimeBinding {
	return store.GitHubTargetRuntimeBinding{
		TargetID:   coordinator.config.TargetID,
		ScaleSetID: store.ScaleSetID(coordinator.config.ScaleSetID),
		ProfileID:  coordinator.config.RunnerProfileID,
	}
}

func (coordinator *ControllerRunnerCoordinator) disableUpdate() bool {
	return coordinator.config.VersionPolicy == domain.RunnerVersionPinned
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
	coordinator.pollMu.Lock()
	defer coordinator.pollMu.Unlock()

	auditHealthy := coordinator.store.ManagementAuditHealthy()
	auditChanged := coordinator.store.ManagementAuditChange()
	pollNow := time.Now()
	candidates, demand, selected, err := coordinator.evaluateCandidates(
		ctx, pollNow, auditHealthy)
	if err != nil {
		return nil, false, err
	}
	if len(candidates) == 0 {
		// Advertising demand for a Target with no node at all would be a lie,
		// and there is no binding a durable claim could name. Report it and let
		// the caller wait for a configuration change instead of hot looping.
		return nil, false, ErrControllerRunnerNoCandidate
	}
	// The poll authority is always one concrete node's evidence, because the
	// store requires an advertised capacity of exactly one at the claim
	// boundary. Only the capacity reported upstream to GitHub is the fleet sum.
	authorityIndex := selected
	if authorityIndex < 0 {
		authorityIndex = 0
	}
	chosen := candidates[authorityIndex]
	authority := chosen.pollState.ClaimAuthority
	authority.AdvertisedCapacity = chosen.capacity
	if coordinator.config.VersionPolicy == domain.RunnerVersionPinned &&
		!chosen.runnerRecheckAt.IsZero() {
		authority.AdmissionDeadlineUnixNano = chosen.runnerRecheckAt.UnixNano()
	}
	coordinator.setPollScope(controllerRunnerPollScope{
		authority: authority,
		nodeID:    chosen.nodeID,
		claimable: selected >= 0,
	})
	defer coordinator.clearPollScope()

	// Every candidate's readiness matters, not just the selected one: a node
	// that becomes usable while another one owns the current poll must still be
	// able to interrupt the long poll and raise advertised demand.
	recheckAt := time.Time{}
	watched := false
	for _, candidate := range candidates {
		if candidate.readinessChanged != nil || candidate.admission.Change != nil {
			watched = true
		}
		recheckAt = earliestControllerRunnerTime(recheckAt, candidate.recheckAt)
	}
	if !watched && (!auditHealthy || auditChanged == nil) && recheckAt.IsZero() {
		message, err := coordinator.poller.PollOnce(ctx, demand)
		if err != nil {
			err = coordinator.persistProviderFailure(ctx, err)
		}
		return message, false, err
	}

	// One line per poll naming the advertised demand and the node that would
	// own a claim. A fleet that silently advertises zero is the single hardest
	// state to diagnose from the outside, so it must be visible without a
	// debug build.
	coordinator.logger.Info(
		"github_poll_demand",
		slog.String("component", "github"),
		slog.String("target_id", string(coordinator.config.TargetID)),
		slog.Int("demand", demand),
		slog.Int("candidates", len(candidates)),
		slog.String("selected_node", string(chosen.nodeID)),
		slog.Bool("claimable", selected >= 0),
	)
	pollContext, cancel := context.WithCancel(ctx)
	stopReadiness := make([]func() bool, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.readinessChanged != nil {
			stopReadiness = append(
				stopReadiness,
				context.AfterFunc(candidate.readinessChanged, cancel),
			)
		}
		if candidate.admission.Change != nil {
			go func(change <-chan struct{}) {
				select {
				case <-change:
					cancel()
				case <-pollContext.Done():
				}
			}(candidate.admission.Change)
		}
	}
	if auditHealthy && auditChanged != nil {
		go func() {
			select {
			case <-auditChanged:
				cancel()
			case <-pollContext.Done():
			}
		}()
	}
	var deadlineTimer *time.Timer
	if !recheckAt.IsZero() {
		deadlineTimer = time.AfterFunc(time.Until(recheckAt), cancel)
	}
	defer func() {
		for _, stop := range stopReadiness {
			stop()
		}
		if deadlineTimer != nil {
			deadlineTimer.Stop()
		}
		cancel()
	}()
	message, err := coordinator.poller.PollOnce(pollContext, demand)
	invalidated := (auditHealthy && auditChanged != nil && channelClosed(auditChanged)) ||
		(!recheckAt.IsZero() && !time.Now().Before(recheckAt))
	for _, candidate := range candidates {
		if invalidated {
			break
		}
		invalidated = (candidate.readinessChanged != nil &&
			candidate.readinessChanged.Err() != nil) ||
			(candidate.admission.Change != nil &&
				channelClosed(candidate.admission.Change))
	}
	if err != nil && ctx.Err() == nil && invalidated &&
		errors.Is(err, context.Canceled) {
		// A readiness transition intentionally interrupts the upstream long
		// poll. Re-evaluate capacity without advancing or driving a claim.
		return nil, true, nil
	}
	if err == nil && ctx.Err() == nil && invalidated {
		return message, true, nil
	}
	if err != nil {
		err = coordinator.persistProviderFailure(ctx, err)
	}
	return message, false, err
}

func (coordinator *ControllerRunnerCoordinator) evaluateRunnerUpdate(
	now time.Time,
	freshness store.GitHubRuntimeFreshness,
) (reconcile.RunnerUpdateStatus, error) {
	profile := freshness.Profile
	if profile.ProfileID != coordinator.config.RunnerProfileID ||
		profile.VersionPolicy != coordinator.config.VersionPolicy ||
		profile.RunnerVersion != runner.OfficialRunnerVersion {
		return reconcile.RunnerUpdateStatus{}, ErrControllerRunnerConfig
	}
	release := freshness.Release
	return reconcile.EvaluateRunnerUpdate(
		now,
		profile.VersionPolicy,
		reconcile.RunnerReleaseObservation{
			HasValue:         release.LatestVersion != "",
			PinnedVersion:    profile.RunnerVersion,
			LatestVersion:    release.LatestVersion,
			LatestReleasedAt: unixNanoTime(release.LatestReleasedAtUnixNano),
			ObservedAt:       unixNanoTime(release.ObservedAtUnixNano),
			Stale:            release.Freshness != store.RuntimeFreshnessFresh,
		},
	)
}

func unixNanoTime(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}

func runnerUpdateRecheckAt(
	now time.Time,
	status reconcile.RunnerUpdateStatus,
) time.Time {
	if !status.AllowsAdmissionAt(now) {
		return time.Time{}
	}
	return earliestControllerRunnerTime(status.FreshUntil, status.Deadline)
}

func earliestControllerRunnerTime(first, second time.Time) time.Time {
	switch {
	case first.IsZero():
		return second
	case second.IsZero():
		return first
	case first.Before(second):
		return first
	default:
		return second
	}
}

func (coordinator *ControllerRunnerCoordinator) persistProviderFailure(
	ctx context.Context,
	err error,
) error {
	var providerFailure *github.ProviderFailure
	if !errors.As(err, &providerFailure) ||
		errors.Is(err, context.Canceled) {
		return err
	}
	class := ClassifyGitHubObservationFailure(providerFailure)
	if _, persistErr := coordinator.store.RecordGitHubScaleSetSessionFailure(
		ctx,
		store.ScaleSetID(coordinator.config.ScaleSetID),
		class,
	); persistErr != nil {
		return errors.Join(ErrGitHubSessionFailureStore, persistErr)
	}
	attributes := []any{
		slog.String("component", "github"),
		slog.String("operation", string(providerFailure.Operation)),
		slog.String("failure_class", string(class)),
	}
	var statusFailure *github.ProviderHTTPStatusError
	if class != store.GitHubObservationNetwork &&
		class != store.GitHubObservationTimeout &&
		errors.As(providerFailure, &statusFailure) {
		attributes = append(
			attributes,
			slog.Int("status_code", statusFailure.StatusCode),
		)
	}
	coordinator.logger.Warn("github_session_stale", attributes...)
	return err
}

func (coordinator *ControllerRunnerCoordinator) finiteProviderOperation(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	if coordinator == nil || coordinator.finiteOperationContext == nil {
		return github.WithFiniteOperationTimeout(ctx)
	}
	return coordinator.finiteOperationContext(ctx)
}

func (coordinator *ControllerRunnerCoordinator) recordFiniteProviderFailure(
	ctx context.Context,
	operation github.ProviderOperation,
	cause error,
) error {
	if cause == nil {
		cause = github.ErrInvalidPreviewResponse
	}
	return coordinator.persistProviderFailure(ctx, &github.ProviderFailure{
		Operation: operation,
		Err:       cause,
	})
}

// ClassifyGitHubObservationFailure maps immediate provider errors to the
// secret-free persistence allowlist shared by session and release observations.
// Callers must exclude intentional local cancellation before persisting it.
func ClassifyGitHubObservationFailure(
	failure error,
) store.GitHubObservationFailureClass {
	if failure == nil {
		return store.GitHubObservationInvalidResponse
	}
	if errors.Is(failure, context.DeadlineExceeded) {
		return store.GitHubObservationTimeout
	}
	// A concrete terminal network error owns the classification even if the
	// official client recorded an earlier HTTP response during token refresh.
	// Otherwise a stale 401/503 can mislabel the final failed request.
	var networkError net.Error
	if errors.As(failure, &networkError) {
		if networkError.Timeout() {
			return store.GitHubObservationTimeout
		}
		return store.GitHubObservationNetwork
	}
	var statusFailure *github.ProviderHTTPStatusError
	if errors.As(failure, &statusFailure) {
		switch {
		case statusFailure.StatusCode == 401 ||
			statusFailure.StatusCode == 403:
			return store.GitHubObservationProviderAuth
		case statusFailure.StatusCode == 429:
			return store.GitHubObservationProvider429
		case statusFailure.StatusCode >= 500 &&
			statusFailure.StatusCode <= 599:
			return store.GitHubObservationProvider5xx
		default:
			return store.GitHubObservationInvalidResponse
		}
	}
	return store.GitHubObservationInvalidResponse
}

func (coordinator *ControllerRunnerCoordinator) PollAndDriveOnce(ctx context.Context) (*github.Message, error) {
	message, readinessChanged, err := coordinator.pollOnce(ctx)
	if err != nil {
		return nil, err
	}
	if readinessChanged {
		return nil, nil
	}
	if message != nil {
		intent, found, err := coordinator.pendingUnpickedRequeue(ctx)
		if err != nil {
			return message, err
		}
		if found && store.MessageID(message.ID) == intent.SourceMessageID {
			return message, ErrGitHubReconciliationPending
		}
	}
	_, err = coordinator.DriveNext(ctx)
	return message, err
}

// Run owns the single-target acceptance loop. A restart drives already-acquired
// or preparing work and prior-epoch reconciliation before long polling, so it
// does not consume most of GitHub's runner pickup window. Pending work remains
// poll-first because commit-before-ack redelivery is its upstream authority.
func (coordinator *ControllerRunnerCoordinator) Run(ctx context.Context) error {
	if coordinator == nil {
		return ErrControllerRunnerConfig
	}
	driveBeforePoll, err := coordinator.recoveryWorkBeforeInitialPoll(ctx)
	if err != nil {
		return err
	}
	providerFailureAttempts := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if driveBeforePoll {
			_, unpickedRequeue, err :=
				coordinator.pendingUnpickedRequeue(ctx)
			if err != nil {
				return err
			}
			if unpickedRequeue {
				// Every destructive/removal-absence step is separated by one
				// zero-capacity long poll. This lets a late exact JobStarted or
				// JobCompleted become durable before replacement authority is
				// created. The lifecycle mutex serializes that commit with the
				// following provider operation.
				driveBeforePoll = false
			}
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
			case errors.Is(err, ErrControllerRunnerAdmission):
				// Recovery work remains durable, but stale provider or
				// runner-release authority cannot create another runner.
				// Return to a zero-capacity poll so a fresh GitHub session
				// can recover without bypassing pinned release policy.
				driveBeforePoll = false
				continue
			case errors.Is(err, ErrGitHubReconciliationPending):
				if !coordinator.waitForReconciliationRetry(ctx) {
					return nil
				}
				driveBeforePoll = true
				continue
			case errors.Is(err, ErrGitHubSessionFailureStore):
				return err
			default:
				if ctx.Err() != nil {
					return nil
				}
				var providerFailure *github.ProviderFailure
				if errors.As(err, &providerFailure) {
					providerFailureAttempts++
					if !coordinator.waitForProviderRetry(
						ctx, providerFailureAttempts,
					) {
						return nil
					}
					driveBeforePoll = true
					continue
				}
				return err
			}
		}

		message, readinessChanged, err := coordinator.pollOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			switch {
			case errors.Is(err, ErrGitHubSessionFailureStore):
				return err
			case errors.Is(err, ErrGitHubAvailableUnclaimed):
				if !coordinator.waitForProviderRetry(ctx, 1) {
					return nil
				}
				continue
			case errors.Is(err, ErrControllerRunnerNoCandidate):
				// An empty candidate set is a normal configuration state, so it
				// backs off like any other recoverable stall rather than
				// terminating the Target's coordinator.
				providerFailureAttempts++
				if !coordinator.waitForProviderRetry(
					ctx,
					providerFailureAttempts,
				) {
					return nil
				}
				continue
			default:
				var providerFailure *github.ProviderFailure
				if !errors.As(err, &providerFailure) {
					return err
				}
				providerFailureAttempts++
				if !coordinator.waitForProviderRetry(
					ctx,
					providerFailureAttempts,
				) {
					return nil
				}
				continue
			}
		}
		providerFailureAttempts = 0
		if readinessChanged {
			// The transition-owning poll iteration never drives. The next
			// iteration may retry an already durable claim immediately.
			driveBeforePoll = true
			continue
		}
		if message != nil {
			intent, found, err :=
				coordinator.pendingUnpickedRequeue(ctx)
			if err != nil {
				return err
			}
			if found &&
				store.MessageID(message.ID) == intent.SourceMessageID {
				// The poll that first commits and ACKs the replacement
				// availability never also deletes the old runner. A later
				// zero-capacity poll gets the first chance to ingest pickup
				// evidence for that exact registration.
				driveBeforePoll = false
				continue
			}
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
		if errors.Is(err, ErrControllerRunnerAdmission) {
			driveBeforePoll = false
			continue
		}
		if errors.Is(err, ErrGitHubReconciliationPending) {
			if !coordinator.waitForReconciliationRetry(ctx) {
				return nil
			}
			driveBeforePoll = true
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, ErrGitHubSessionFailureStore) {
				return err
			}
			var providerFailure *github.ProviderFailure
			if errors.As(err, &providerFailure) {
				providerFailureAttempts++
				if !coordinator.waitForProviderRetry(
					ctx, providerFailureAttempts,
				) {
					return nil
				}
				driveBeforePoll = true
				continue
			}
			return err
		}
		// Reconciliation may atomically replace a terminal execution with a
		// durable Pending claim only after the old provider registration is
		// proven absent. That claim has already paid the required zero-capacity
		// observation fences, so dispatch it before another potentially long
		// GitHub poll consumes the runner pickup window.
		driveBeforePoll, err = coordinator.actionableClaimBeforePoll(ctx)
		if err != nil {
			return err
		}
	}
}

func (coordinator *ControllerRunnerCoordinator) pendingUnpickedRequeue(
	ctx context.Context,
) (store.GitHubUnpickedRequeueIntent, bool, error) {
	fence, found, err := coordinator.store.NextGitHubReconciliationFence(
		ctx,
		store.ScaleSetID(coordinator.config.ScaleSetID),
	)
	if err != nil || !found {
		return store.GitHubUnpickedRequeueIntent{}, false, err
	}
	intent, found, err := coordinator.store.GitHubUnpickedRequeueIntent(
		ctx,
		fence.Claim.ScaleSetID,
		fence.Claim.RunnerRequestID,
	)
	if err != nil || !found {
		return store.GitHubUnpickedRequeueIntent{}, false, err
	}
	if fence.Attempt == nil ||
		*fence.Attempt != intent.Attempt ||
		fence.Claim != intent.Claim {
		return store.GitHubUnpickedRequeueIntent{},
			false,
			ErrGitHubReconciliationRequired
	}
	return intent, true, nil
}

func (coordinator *ControllerRunnerCoordinator) recoveryWorkBeforeInitialPoll(
	ctx context.Context,
) (bool, error) {
	fence, found, err := coordinator.store.NextGitHubReconciliationFence(
		ctx, store.ScaleSetID(coordinator.config.ScaleSetID))
	if err != nil {
		return false, err
	}
	if found {
		return fence.Claim.State != store.GitHubClaimAcquireAmbiguous, nil
	}
	claim, found, err := coordinator.store.NextActionableGitHubClaim(
		ctx, store.ScaleSetID(coordinator.config.ScaleSetID))
	if err != nil || !found {
		return false, err
	}
	switch claim.State {
	case store.GitHubClaimAcquired, store.GitHubClaimPreparing:
		return true, nil
	case store.GitHubClaimPending:
		return coordinator.store.GitHubPendingClaimDispatchReady(ctx, claim)
	default:
		return false, store.ErrGitHubClaimState
	}
}

func (coordinator *ControllerRunnerCoordinator) actionableClaimBeforePoll(
	ctx context.Context,
) (bool, error) {
	claim, found, err := coordinator.store.NextActionableGitHubClaim(
		ctx,
		store.ScaleSetID(coordinator.config.ScaleSetID),
	)
	if err != nil || !found {
		return false, err
	}
	switch claim.State {
	case store.GitHubClaimPending,
		store.GitHubClaimAcquired,
		store.GitHubClaimPreparing:
		return true, nil
	default:
		return false, store.ErrGitHubClaimState
	}
}

func (coordinator *ControllerRunnerCoordinator) waitForProviderRetry(
	ctx context.Context,
	attempt int,
) bool {
	if attempt < 1 {
		attempt = 1
	}
	delay := controllerRunnerRetryInitial
	for range attempt - 1 {
		if delay >= controllerRunnerRetryMaximum/2 {
			delay = controllerRunnerRetryMaximum
			break
		}
		delay *= 2
	}
	coordinator.logger.Info(
		"github_session_retry_scheduled",
		slog.String("component", "github"),
		slog.Duration("delay", delay),
	)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (coordinator *ControllerRunnerCoordinator) waitForReconciliationRetry(
	ctx context.Context,
) bool {
	delay := coordinator.reconciliationRetry
	if delay <= 0 {
		delay = store.GitHubRunnerAbsenceConfirmationDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// waitForRunnerReadiness blocks until any candidate node of this Target could
// carry recovery work. Waiting on a single node would stall a Target whose work
// can legitimately move to a different machine.
func (coordinator *ControllerRunnerCoordinator) waitForRunnerReadiness(ctx context.Context) error {
	nodes, err := coordinator.candidateNodes(ctx)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return ErrAgentOffline
	}
	watchContext, cancel := context.WithCancel(ctx)
	defer cancel()
	watching := false
	for _, nodeID := range nodes {
		snapshot, online, changed := coordinator.agents.Readiness(nodeID)
		admission, err := coordinator.config.Reconciler.Admission(nodeID)
		if err != nil {
			return err
		}
		if online && snapshot.NativeRunnerReady &&
			snapshot.RunnerVersion == runner.OfficialRunnerVersion &&
			admission.AllowsRecovery {
			return nil
		}
		if changed != nil {
			stop := context.AfterFunc(changed, cancel)
			defer stop()
			watching = true
		}
		if admission.Change != nil {
			watching = true
			go func(change <-chan struct{}) {
				select {
				case <-change:
					cancel()
				case <-watchContext.Done():
				}
			}(admission.Change)
		}
	}
	if !watching {
		return ErrAgentOffline
	}
	<-watchContext.Done()
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// DriveNext advances at most one durable single-slot claim. A pre-existing
// Pending claim is sufficient after restart; no in-memory PollOnce result is
// required.
func (coordinator *ControllerRunnerCoordinator) DriveNext(ctx context.Context) (bool, error) {
	coordinator.driveMu.Lock()
	defer coordinator.driveMu.Unlock()
	return coordinator.driveNext(ctx)
}

// reconciliationNodes names the nodes whose pending reconciliation actions this
// Target owns. A durable fence or claim identifies its node exactly, so those
// actions are driven against the machine that owns the work rather than a
// coordinator-wide one. Only when this Target holds no claim at all does it fall
// back to its candidates, which keeps a pinned single-node deployment identical.
func (coordinator *ControllerRunnerCoordinator) reconciliationNodes(
	ctx context.Context,
) ([]domain.NodeID, error) {
	fence, found, err := coordinator.store.NextGitHubReconciliationFence(
		ctx, store.ScaleSetID(coordinator.config.ScaleSetID))
	if err != nil {
		return nil, err
	}
	if found && fence.Claim.Execution.Slot.NodeID != "" {
		return []domain.NodeID{fence.Claim.Execution.Slot.NodeID}, nil
	}
	claim, found, err := coordinator.store.NextActionableGitHubClaim(
		ctx, store.ScaleSetID(coordinator.config.ScaleSetID))
	if err != nil {
		return nil, err
	}
	if found && claim.Execution.Slot.NodeID != "" {
		return []domain.NodeID{claim.Execution.Slot.NodeID}, nil
	}
	return coordinator.candidateNodes(ctx)
}

func (coordinator *ControllerRunnerCoordinator) driveNext(ctx context.Context) (bool, error) {
	reconciliationNodes, err := coordinator.reconciliationNodes(ctx)
	if err != nil {
		return false, err
	}
	for _, nodeID := range reconciliationNodes {
		handled, blocked, err := coordinator.driveNodeReconciliationAction(ctx, nodeID)
		if err != nil || handled || blocked {
			return handled, err
		}
	}
	fence, found, err := coordinator.store.NextGitHubReconciliationFence(
		ctx, store.ScaleSetID(coordinator.config.ScaleSetID))
	if err != nil {
		return false, err
	}
	if found {
		if fence.Claim.State == store.GitHubClaimAcquireAmbiguous {
			return false, nil
		}
		err := coordinator.reconcileJITAttempt(ctx, fence.Claim.RunnerRequestID)
		if errors.Is(err, ErrGitHubReconciliationRequired) {
			var providerFailure *github.ProviderFailure
			if !errors.As(err, &providerFailure) {
				_, unpickedRequeue, intentErr :=
					coordinator.store.GitHubUnpickedRequeueIntent(
						ctx,
						fence.Claim.ScaleSetID,
						fence.Claim.RunnerRequestID,
					)
				if intentErr != nil {
					return true, intentErr
				}
				if fence.Attempt != nil &&
					(coordinator.config.ControllerEpoch >
						fence.Attempt.ControllerEpoch ||
						unpickedRequeue) {
					return true, ErrGitHubReconciliationPending
				}
				return false, nil
			}
		}
		return true, err
	}
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
	if err := coordinator.requireRunnerAdmission(
		ctx, claim.Execution.Slot.NodeID); err != nil {
		return err
	}
	attempt, err := coordinator.store.BeginGitHubAcquire(
		ctx,
		claim.ScaleSetID,
		claim.RunnerRequestID,
	)
	if err != nil {
		return err
	}
	acquireFence := reconcile.GitHubFence{
		ExecutionID:     claim.Execution.ID,
		ScaleSetID:      claim.ScaleSetID,
		RunnerRequestID: claim.RunnerRequestID,
		ClaimState:      store.GitHubClaimAcquireAmbiguous,
	}
	if err := coordinator.config.Reconciler.ApplyGitHubFence(acquireFence); err != nil {
		return err
	}
	operationContext, cancelOperation := coordinator.finiteProviderOperation(ctx)
	acquired, err := coordinator.session.AcquireJobs(
		operationContext, []int64{claim.RunnerRequestID})
	cancelOperation()
	if err != nil || len(acquired) != 1 || acquired[0] != claim.RunnerRequestID {
		// BeginGitHubAcquire records the ambiguous state before the network call,
		// so a crash, transport error, or empty response all remain non-runnable.
		if err == nil {
			err = github.ErrInvalidPreviewResponse
		}
		providerErr := coordinator.recordFiniteProviderFailure(
			ctx, github.ProviderAcquireJobs, err)
		return errors.Join(ErrGitHubAcquireAmbiguous, providerErr)
	}
	if err := coordinator.store.MarkGitHubAcquired(ctx, attempt); err != nil {
		return err
	}
	return coordinator.config.Reconciler.ClearGitHubFence(acquireFence)
}

// requireRunnerAdmission proves that one exact node may still carry the claim
// it is being asked to advance. The node comes from the claim's own execution
// slot, so a coordinator that serves several nodes can never send a command to
// a machine that does not own the work.
func (coordinator *ControllerRunnerCoordinator) requireRunnerAdmission(
	ctx context.Context,
	nodeID domain.NodeID,
) error {
	if nodeID == "" {
		return ErrControllerRunnerConfig
	}
	if !coordinator.store.ManagementAuditHealthy() {
		return ErrControllerRunnerAdmission
	}
	snapshot, online, _ := coordinator.agents.Readiness(nodeID)
	admission, err := coordinator.config.Reconciler.Admission(nodeID)
	if err != nil {
		return err
	}
	if !online || !snapshot.NativeRunnerReady ||
		snapshot.RunnerVersion != runner.OfficialRunnerVersion ||
		!admission.AllowsRecovery {
		return ErrAgentOffline
	}
	pollState, err := coordinator.store.ReadGitHubPollState(
		ctx,
		coordinator.runtimeBinding(),
		nodeID,
	)
	if err != nil {
		return err
	}
	now := time.Now()
	runnerUpdate, err := coordinator.evaluateRunnerUpdate(now, pollState.Runtime)
	if err != nil {
		return err
	}
	if pollState.Runtime.Session.Freshness != store.RuntimeFreshnessFresh ||
		!runnerUpdate.AllowsAdmissionAt(now) {
		return ErrControllerRunnerAdmission
	}
	snapshotDigest, err := transport.AgentSnapshotDigest(snapshot)
	if err != nil {
		return err
	}
	durableAgent := pollState.ClaimAuthority.Agent
	if pollState.ClaimAuthority.ControllerEpoch != coordinator.config.ControllerEpoch ||
		!durableAgent.HasSnapshot ||
		!durableAgent.NativeRunnerReady ||
		durableAgent.AcceptedByControllerEpoch != coordinator.config.ControllerEpoch ||
		durableAgent.RunnerVersion != runner.OfficialRunnerVersion ||
		durableAgent.SnapshotDigest != snapshotDigest {
		return ErrAgentOffline
	}
	return nil
}

func channelClosed(channel <-chan struct{}) bool {
	if channel == nil {
		return false
	}
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

func (coordinator *ControllerRunnerCoordinator) prepare(ctx context.Context, claim store.GitHubJobClaim) (bool, error) {
	nodeID := claim.Execution.Slot.NodeID
	if err := coordinator.requireRunnerAdmission(ctx, nodeID); err != nil {
		return false, err
	}
	metadata := transport.CommandMetadata{
		CommandID:       deterministicCommandID("prepare", claim.Execution.ID, ""),
		ControllerEpoch: coordinator.config.ControllerEpoch,
		ExecutionID:     claim.Execution.ID,
		ExpectedState:   domain.ExecutionReserved,
		Target:          coordinator.commandTarget(),
	}
	update, err := coordinator.agents.SendPrepare(
		ctx, nodeID, metadata, coordinator.disableUpdate())
	if err != nil {
		// Prepare is non-secret and exact-idempotent. Keep the claim Acquired so
		// reconnect can resend the same deterministic command.
		return false, err
	}
	if err := validateControllerRunnerUpdate(update, nodeID, metadata); err != nil {
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
	nodeID := claim.Execution.Slot.NodeID
	if err := coordinator.requireRunnerAdmission(ctx, nodeID); err != nil {
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
		claimState, stateErr := claimStateForAttempt(attempt.State)
		if stateErr != nil {
			return stateErr
		}
		if err := coordinator.applyAttemptFence(claim, attempt, claimState); err != nil {
			return err
		}
		// The opaque body is not durable. Any surviving attempt row means a
		// previous generation may have crossed GitHub and must be reconciled.
		return ErrGitHubReconciliationRequired
	}
	if err := coordinator.applyAttemptFence(claim, attempt, store.GitHubClaimJITIntent); err != nil {
		return err
	}
	operationContext, cancelOperation := coordinator.finiteProviderOperation(ctx)
	jitConfig, reference, err := coordinator.lifecycle.GenerateJITConfig(operationContext, github.JITRequest{
		ScaleSetID: coordinator.config.ScaleSetID,
		Name:       runnerName,
		WorkFolder: controllerRunnerWorkFolder,
	})
	cancelOperation()
	if err != nil {
		if markErr := coordinator.store.MarkGitHubJITGenerationAmbiguous(ctx, attempt); markErr != nil {
			return errors.Join(ErrGitHubReconciliationRequired, markErr)
		}
		attempt.State = store.GitHubJITGenerationAmbiguous
		if fenceErr := coordinator.applyAttemptFence(
			claim, attempt, store.GitHubClaimJITGenerationAmbiguous); fenceErr != nil {
			return errors.Join(ErrGitHubReconciliationRequired, fenceErr)
		}
		providerErr := coordinator.recordFiniteProviderFailure(
			ctx, github.ProviderGenerateJIT, err)
		return errors.Join(ErrGitHubReconciliationRequired, providerErr)
	}
	if jitConfig == nil {
		if markErr := coordinator.store.MarkGitHubJITGenerationAmbiguous(ctx, attempt); markErr != nil {
			return errors.Join(ErrGitHubReconciliationRequired, markErr)
		}
		attempt.State = store.GitHubJITGenerationAmbiguous
		if fenceErr := coordinator.applyAttemptFence(
			claim, attempt, store.GitHubClaimJITGenerationAmbiguous); fenceErr != nil {
			return errors.Join(ErrGitHubReconciliationRequired, fenceErr)
		}
		providerErr := coordinator.recordFiniteProviderFailure(
			ctx, github.ProviderGenerateJIT, github.ErrInvalidPreviewResponse)
		return errors.Join(ErrGitHubReconciliationRequired, providerErr)
	}
	jitDigest := jitConfig.Digest()
	if reference.ID <= 0 || reference.Name != runnerName ||
		reference.ScaleSetID != coordinator.config.ScaleSetID ||
		!controllerLowerSHA256(jitDigest) {
		if markErr := coordinator.store.MarkGitHubJITGenerationAmbiguous(ctx, attempt); markErr != nil {
			return errors.Join(ErrGitHubReconciliationRequired, markErr)
		}
		attempt.State = store.GitHubJITGenerationAmbiguous
		if fenceErr := coordinator.applyAttemptFence(
			claim, attempt, store.GitHubClaimJITGenerationAmbiguous); fenceErr != nil {
			return errors.Join(ErrGitHubReconciliationRequired, fenceErr)
		}
		providerErr := coordinator.recordFiniteProviderFailure(
			ctx, github.ProviderGenerateJIT, github.ErrInvalidPreviewResponse)
		return errors.Join(ErrGitHubReconciliationRequired, providerErr)
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
	if err := coordinator.applyAttemptFence(
		claim, attempt, store.GitHubClaimJITGenerated); err != nil {
		return errors.Join(ErrGitHubReconciliationRequired, err)
	}
	if err := coordinator.store.BeginGitHubStartDispatch(ctx, attempt); err != nil {
		return errors.Join(ErrGitHubReconciliationRequired, err)
	}
	attempt.State = store.GitHubJITStartDispatching
	startDispatchFence := githubFenceForAttempt(
		claim, attempt, store.GitHubClaimStartDispatching)
	if err := coordinator.config.Reconciler.ApplyGitHubFence(startDispatchFence); err != nil {
		return errors.Join(ErrGitHubReconciliationRequired, err)
	}
	metadata := transport.CommandMetadata{
		CommandID:       startCommandID,
		ControllerEpoch: coordinator.config.ControllerEpoch,
		ExecutionID:     claim.Execution.ID,
		ExpectedState:   domain.ExecutionPreparing,
		Target:          coordinator.commandTarget(),
	}
	update, err := coordinator.agents.SendStart(
		ctx, nodeID, metadata, coordinator.disableUpdate(), jitConfig)
	if err != nil {
		if markErr := coordinator.store.MarkGitHubStartAmbiguous(ctx, attempt); markErr != nil {
			return errors.Join(ErrGitHubStartAmbiguous, markErr)
		}
		attempt.State = store.GitHubJITStartAmbiguous
		if fenceErr := coordinator.applyAttemptFence(
			claim, attempt, store.GitHubClaimStartAmbiguous); fenceErr != nil {
			return errors.Join(ErrGitHubStartAmbiguous, fenceErr)
		}
		return ErrGitHubStartAmbiguous
	}
	if err := validateControllerRunnerUpdate(update, nodeID, metadata); err != nil ||
		update.State != domain.ExecutionRunning {
		if markErr := coordinator.store.MarkGitHubStartAmbiguous(ctx, attempt); markErr != nil {
			return errors.Join(ErrGitHubStartAmbiguous, markErr)
		}
		attempt.State = store.GitHubJITStartAmbiguous
		if fenceErr := coordinator.applyAttemptFence(
			claim, attempt, store.GitHubClaimStartAmbiguous); fenceErr != nil {
			return errors.Join(ErrGitHubStartAmbiguous, fenceErr)
		}
		return ErrGitHubStartAmbiguous
	}
	if err := coordinator.store.MarkGitHubRunning(ctx, attempt); err != nil {
		return errors.Join(ErrGitHubReconciliationRequired, err)
	}
	return coordinator.config.Reconciler.ClearGitHubFence(startDispatchFence)
}

func (coordinator *ControllerRunnerCoordinator) applyAttemptFence(
	claim store.GitHubJobClaim,
	attempt store.GitHubJITAttempt,
	claimState store.GitHubClaimState,
) error {
	return coordinator.config.Reconciler.ApplyGitHubFence(
		githubFenceForAttempt(claim, attempt, claimState))
}

func githubFenceForAttempt(
	claim store.GitHubJobClaim,
	attempt store.GitHubJITAttempt,
	claimState store.GitHubClaimState,
) reconcile.GitHubFence {
	// The process projection stores an immutable value token. Keep the caller's
	// later state-machine mutations from rewriting the expected CAS token.
	attemptToken := attempt
	return reconcile.GitHubFence{
		ExecutionID:     claim.Execution.ID,
		ScaleSetID:      claim.ScaleSetID,
		RunnerRequestID: claim.RunnerRequestID,
		ClaimState:      claimState,
		Attempt:         &attemptToken,
	}
}

func claimStateForAttempt(
	state store.GitHubJITAttemptState,
) (store.GitHubClaimState, error) {
	switch state {
	case store.GitHubJITIntent:
		return store.GitHubClaimJITIntent, nil
	case store.GitHubJITGenerationAmbiguous:
		return store.GitHubClaimJITGenerationAmbiguous, nil
	case store.GitHubJITGenerated:
		return store.GitHubClaimJITGenerated, nil
	case store.GitHubJITStartDispatching:
		return store.GitHubClaimStartDispatching, nil
	case store.GitHubJITStartAmbiguous:
		return store.GitHubClaimStartAmbiguous, nil
	case store.GitHubJITStarted, store.GitHubJITAgentAccepted,
		store.GitHubJITRemovalPending:
		return store.GitHubClaimReconciliationRequired, nil
	default:
		return "", ErrGitHubReconciliationRequired
	}
}

// ReconcileJITAttempt is the only path that permits a later JIT attempt. It
// requires a freshly activated Agent snapshot, then either proves the exact
// start command was accepted or removes and re-reads the deterministic GitHub
// runner registration before marking it absent.
func (coordinator *ControllerRunnerCoordinator) ReconcileJITAttempt(
	ctx context.Context,
	runnerRequestID int64,
) error {
	coordinator.driveMu.Lock()
	defer coordinator.driveMu.Unlock()
	return coordinator.reconcileJITAttempt(ctx, runnerRequestID)
}

func (coordinator *ControllerRunnerCoordinator) reconcileJITAttempt(
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
	requeueIntent, unpickedRequeue, err :=
		coordinator.store.GitHubUnpickedRequeueIntent(
			ctx,
			attempt.ScaleSetID,
			runnerRequestID,
		)
	if err != nil {
		return errors.Join(ErrGitHubReconciliationRequired, err)
	}
	if unpickedRequeue && requeueIntent.Attempt != attempt {
		return ErrGitHubReconciliationRequired
	}
	if coordinator.config.ControllerEpoch < attempt.ControllerEpoch ||
		(coordinator.config.ControllerEpoch == attempt.ControllerEpoch &&
			!unpickedRequeue) {
		// An intent may still be executing in its owning process. Only a later
		// durable Controller epoch can reconcile that crash window. The narrow
		// exception is a committed fresh-availability cleanup intent: it is
		// created only after the same process has observed the old execution
		// terminal and must be actionable without requiring a restart.
		return ErrGitHubReconciliationRequired
	}
	switch attempt.State {
	case store.GitHubJITIntent, store.GitHubJITGenerationAmbiguous,
		store.GitHubJITGenerated, store.GitHubJITStartDispatching,
		store.GitHubJITStartAmbiguous, store.GitHubJITAgentAccepted,
		store.GitHubJITStarted, store.GitHubJITRemovalPending:
	default:
		return ErrGitHubReconciliationRequired
	}
	claim, found, err := coordinator.store.GitHubClaim(ctx, attempt.ScaleSetID, runnerRequestID)
	if err != nil || !found {
		if err == nil {
			err = ErrGitHubReconciliationRequired
		}
		return err
	}
	if unpickedRequeue && requeueIntent.Claim != claim {
		return ErrGitHubReconciliationRequired
	}
	// Reconciliation authority is the snapshot of the node that owns this claim,
	// never a coordinator-wide one.
	claimNodeID := claim.Execution.Slot.NodeID
	snapshot, online, _ := coordinator.agents.Readiness(claimNodeID)
	if claimNodeID == "" || !online || snapshot.NodeID != claimNodeID {
		return ErrGitHubReconciliationRequired
	}
	snapshotDigest, err := transport.AgentSnapshotDigest(snapshot)
	if err != nil {
		return errors.Join(ErrGitHubReconciliationRequired, err)
	}
	fence := githubFenceForAttempt(claim, attempt, claim.State)
	var issuedStart *reconcile.IssuedCommand
	if attempt.StartCommandID != "" {
		issued, issuedFound, issuedErr := coordinator.store.IssuedAgentCommand(
			ctx, attempt.StartCommandID)
		if issuedErr != nil {
			return issuedErr
		}
		if issuedFound {
			issuedStart = &reconcile.IssuedCommand{
				NodeID:  issued.NodeID,
				Type:    issued.Type,
				Command: issued.Command,
			}
		}
	}
	observation, observed := controllerRunnerObservation(
		snapshot, claim.Execution.ID)
	lostJITTerminal := false
	if !unpickedRequeue &&
		jitAttemptMayHaveAcceptedStart(attempt.State) &&
		attempt.StartCommandID != "" {
		history, err := coordinator.store.ReconcileGitHubJITPrunedHistory(
			ctx,
			attempt,
			coordinator.config.ControllerEpoch,
			snapshotDigest,
		)
		if err != nil {
			return errors.Join(ErrGitHubReconciliationRequired, err)
		}
		switch {
		case history.Started:
			return coordinator.config.Reconciler.ClearGitHubFence(fence)
		case lostJITTerminalObservation(history.LostTerminal):
			lostJITTerminal = true
		case history.LostTerminal != "":
			return ErrGitHubReconciliationRequired
		}
	}
	if !unpickedRequeue &&
		observed && lostJITTerminalObservation(observation.State) &&
		attempt.StartCommandID != "" && !lostJITTerminal {
		durableObservation := store.ObservationSnapshot{
			ExecutionID:        observation.ExecutionID,
			State:              observation.State,
			ObservedAtUnixNano: observation.ObservedAtUnixNano,
		}
		err := coordinator.store.MarkGitHubJITObservedStarted(
			ctx,
			attempt,
			coordinator.config.ControllerEpoch,
			durableObservation,
			snapshotDigest,
		)
		switch {
		case err == nil:
			return coordinator.config.Reconciler.ClearGitHubFence(fence)
		case errors.Is(err, store.ErrGitHubJITStartNotProven):
			// Agent startup recovery can report a terminal state after accepting
			// Start but before the opaque JIT value reached runner.Manager. Keep
			// the exact fence and reconcile the provider runner; do not label
			// this execution Running or generate another JIT.
			lostJITTerminal = true
		default:
			return errors.Join(ErrGitHubReconciliationRequired, err)
		}
	}
	var resolution reconcile.AmbiguityResolution
	providerCleanupRequired := unpickedRequeue || lostJITTerminal
	if !providerCleanupRequired {
		resolution, err = reconcile.ResolveGitHubFence(
			fence,
			issuedStart,
			snapshot,
			reconcile.GitHubReconciliationObservation{},
		)
		if err != nil {
			return errors.Join(ErrGitHubReconciliationRequired, err)
		}
	}
	switch {
	case providerCleanupRequired:
		// Provider reconciliation below owns the direct terminal path.
	case resolution.Kind == reconcile.AmbiguityConfirmAgentAccepted ||
		resolution.Kind == reconcile.AmbiguityAwaitAgentObservation:
		if observed {
			switch observation.State {
			case domain.ExecutionRunning, domain.ExecutionCleaning:
				durableObservation := store.ObservationSnapshot{
					ExecutionID:        observation.ExecutionID,
					State:              observation.State,
					ObservedAtUnixNano: observation.ObservedAtUnixNano,
				}
				if err := coordinator.store.MarkGitHubJITObservedStarted(
					ctx,
					attempt,
					coordinator.config.ControllerEpoch,
					durableObservation,
					snapshotDigest,
				); err != nil {
					return errors.Join(ErrGitHubReconciliationRequired, err)
				}
				return coordinator.config.Reconciler.ClearGitHubFence(fence)
			}
		}
		if resolution.Kind == reconcile.AmbiguityConfirmAgentAccepted {
			if err := coordinator.store.MarkGitHubJITAgentAccepted(
				ctx,
				attempt,
				coordinator.config.ControllerEpoch,
				snapshotDigest,
			); err != nil {
				return errors.Join(ErrGitHubReconciliationRequired, err)
			}
			attempt.State = store.GitHubJITAgentAccepted
			if err := coordinator.applyAttemptFence(
				claim, attempt, store.GitHubClaimReconciliationRequired); err != nil {
				return errors.Join(ErrGitHubReconciliationRequired, err)
			}
		}
		return ErrGitHubReconciliationRequired
	}
	if !providerCleanupRequired && observed &&
		observation.State != domain.ExecutionPreparing {
		// A local runtime that advanced without exact accepted Start authority
		// can never be treated as proof that the provider runner is disposable.
		return ErrGitHubReconciliationRequired
	}
	if attempt.RunnerName != deterministicRunnerName(
		attempt.ScaleSetID,
		attempt.RunnerRequestID,
	) {
		return ErrGitHubReconciliationRequired
	}
	sessionHealth, err := coordinator.store.ReadGitHubScaleSetSessionHealth(
		ctx,
		attempt.ScaleSetID,
	)
	if err != nil || sessionHealth.TransitionGeneration == 0 {
		if err == nil {
			err = store.ErrRuntimeFreshnessState
		}
		return errors.Join(ErrGitHubReconciliationRequired, err)
	}
	githubSessionGeneration := sessionHealth.TransitionGeneration
	operationContext, cancelOperation := coordinator.finiteProviderOperation(ctx)
	reference, err := coordinator.lifecycle.QueryRunner(operationContext, github.RunnerQuery{
		ScaleSetID:      github.ScaleSetID(attempt.ScaleSetID),
		RunnerRequestID: attempt.RunnerRequestID,
		Name:            attempt.RunnerName,
		ExpectedID:      attempt.RunnerID,
	})
	cancelOperation()
	if err != nil {
		providerErr := coordinator.recordFiniteProviderFailure(
			ctx, github.ProviderQueryRunner, err)
		return errors.Join(ErrGitHubReconciliationRequired, providerErr)
	}
	providerObservation := reconcile.GitHubReconciliationObservation{
		ObservedAt:      time.Now(),
		ScaleSetID:      attempt.ScaleSetID,
		RunnerRequestID: attempt.RunnerRequestID,
		RunnerObserved:  true,
		RunnerName:      attempt.RunnerName,
	}
	if reference != nil {
		providerObservation.Runner = &reconcile.GitHubRunnerIdentity{
			ScaleSetID: store.ScaleSetID(reference.ScaleSetID),
			ID:         reference.ID,
			Name:       reference.Name,
		}
	}
	if providerCleanupRequired {
		switch {
		case reference == nil:
			resolution = reconcile.AmbiguityResolution{
				Kind: reconcile.AmbiguityMarkRunnerAbsent,
			}
		case reference.ScaleSetID != coordinator.config.ScaleSetID ||
			reference.ID != attempt.RunnerID ||
			reference.Name != attempt.RunnerName:
			return ErrGitHubReconciliationRequired
		default:
			resolution = reconcile.AmbiguityResolution{
				Kind: reconcile.AmbiguityRemoveRunner,
				Runner: &reconcile.GitHubRunnerIdentity{
					ScaleSetID: store.ScaleSetID(reference.ScaleSetID),
					ID:         reference.ID,
					Name:       reference.Name,
				},
			}
		}
	} else {
		resolution, err = reconcile.ResolveGitHubFence(
			fence, issuedStart, snapshot, providerObservation)
		if err != nil {
			return errors.Join(ErrGitHubReconciliationRequired, err)
		}
	}
	switch resolution.Kind {
	case reconcile.AmbiguityMarkRunnerAbsent:
		if attempt.RunnerID == 0 {
			result, err := coordinator.store.MarkGitHubJITReconciledAbsent(
				ctx,
				attempt,
				coordinator.config.ControllerEpoch,
				snapshotDigest,
				githubSessionGeneration,
			)
			if err != nil {
				return errors.Join(ErrGitHubReconciliationRequired, err)
			}
			return coordinator.finishGitHubJITAbsence(result, fence)
		}
		if attempt.State != store.GitHubJITRemovalPending {
			if err := coordinator.store.MarkGitHubJITRemovalPending(
				ctx,
				attempt,
				coordinator.config.ControllerEpoch,
				snapshotDigest,
				githubSessionGeneration,
				true,
			); err != nil {
				return errors.Join(ErrGitHubReconciliationRequired, err)
			}
			attempt.State = store.GitHubJITRemovalPending
			if err := coordinator.applyAttemptFence(
				claim,
				attempt,
				store.GitHubClaimReconciliationRequired,
			); err != nil {
				return errors.Join(ErrGitHubReconciliationRequired, err)
			}
			// The absence observation that authorizes RemovalPending cannot
			// also prove the removal fence held across a later provider read.
			// A subsequent reconciliation iteration must observe absence again
			// while the same Agent snapshot authority remains current.
			return ErrGitHubReconciliationRequired
		}
		result, err := coordinator.store.MarkGitHubJITReconciledAbsent(
			ctx,
			attempt,
			coordinator.config.ControllerEpoch,
			snapshotDigest,
			githubSessionGeneration,
		)
		if err != nil {
			return errors.Join(ErrGitHubReconciliationRequired, err)
		}
		return coordinator.finishGitHubJITAbsence(result, fence)
	case reconcile.AmbiguityRemoveRunner:
		if resolution.Runner == nil || reference == nil {
			return ErrGitHubReconciliationRequired
		}
		if unpickedRequeue {
			latestIntent, found, err :=
				coordinator.store.GitHubUnpickedRequeueIntent(
					ctx,
					attempt.ScaleSetID,
					attempt.RunnerRequestID,
				)
			if err != nil || !found ||
				latestIntent.Attempt != attempt ||
				latestIntent.Claim != claim {
				if err == nil {
					err = ErrGitHubReconciliationRequired
				}
				return errors.Join(ErrGitHubReconciliationRequired, err)
			}
			if latestIntent.PickupProven {
				return ErrGitHubReconciliationRequired
			}
		}
		attempt.RunnerID = resolution.Runner.ID
		if err := coordinator.store.MarkGitHubJITRemovalPending(
			ctx,
			attempt,
			coordinator.config.ControllerEpoch,
			snapshotDigest,
			githubSessionGeneration,
			false,
		); err != nil {
			return errors.Join(ErrGitHubReconciliationRequired, err)
		}
		attempt.State = store.GitHubJITRemovalPending
		if err := coordinator.applyAttemptFence(
			claim, attempt, store.GitHubClaimReconciliationRequired); err != nil {
			return errors.Join(ErrGitHubReconciliationRequired, err)
		}
		operationContext, cancelOperation := coordinator.finiteProviderOperation(ctx)
		err := coordinator.lifecycle.RemoveRunner(operationContext, *reference)
		cancelOperation()
		if err != nil {
			providerErr := coordinator.recordFiniteProviderFailure(
				ctx, github.ProviderRemoveRunner, err)
			return errors.Join(ErrGitHubReconciliationRequired, providerErr)
		}
		// Even a successful DELETE is followed by a later read. A crash or
		// preview inconsistency cannot be mistaken for proven absence.
		return ErrGitHubReconciliationRequired
	default:
		return ErrGitHubReconciliationRequired
	}
}

func (coordinator *ControllerRunnerCoordinator) finishGitHubJITAbsence(
	result store.GitHubJITAbsenceResult,
	fence reconcile.GitHubFence,
) error {
	if result.ReplacementExecution != nil || result.ReplacementClaimed {
		if result.ReplacementExecution == nil ||
			!result.ReplacementClaimed ||
			result.Claim.State != store.GitHubClaimPending ||
			result.Claim.Execution != *result.ReplacementExecution ||
			result.ReplacementExecution.State != domain.ExecutionReserved ||
			result.ReplacementExecution.ID == fence.ExecutionID {
			return ErrGitHubReconciliationRequired
		}
		if err := coordinator.config.Reconciler.ApplyGitHubClaim(
			result.Claim,
		); err != nil {
			return err
		}
	} else if result.Claim.State == store.GitHubClaimRunning {
		// A late exact JobStarted/JobCompleted proves the old registration was
		// consumed. Once GitHub also reports that registration absent, discard
		// only the pending intent and keep the terminal local execution as the
		// claim identity; never create or dispatch the replacement.
		if result.Claim.Execution.ID != fence.ExecutionID ||
			(result.Claim.Execution.State != domain.ExecutionReleased &&
				result.Claim.Execution.State != domain.ExecutionFailed) {
			return ErrGitHubReconciliationRequired
		}
	}
	if result.TerminalExecution != nil {
		if result.AwaitingAvailability == result.CleanupBlocked ||
			result.Claim.State != store.GitHubClaimReconciliationRequired ||
			result.Claim.Execution != *result.TerminalExecution {
			return ErrGitHubReconciliationRequired
		}
		if err := coordinator.config.Reconciler.ApplyDesiredExecution(
			*result.TerminalExecution,
		); err != nil {
			return err
		}
	} else if result.AwaitingAvailability || result.CleanupBlocked {
		return ErrGitHubReconciliationRequired
	}
	return coordinator.config.Reconciler.ClearGitHubFence(fence)
}

func controllerRunnerObservation(
	snapshot AgentSnapshot,
	executionID domain.ExecutionID,
) (transport.AgentExecutionObservation, bool) {
	for _, observation := range snapshot.Observations {
		if observation.ExecutionID == executionID {
			return observation, true
		}
	}
	return transport.AgentExecutionObservation{}, false
}

func lostJITTerminalObservation(state domain.ExecutionState) bool {
	switch state {
	case domain.ExecutionReleased, domain.ExecutionFailed,
		domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
		return true
	default:
		return false
	}
}

func jitAttemptMayHaveAcceptedStart(state store.GitHubJITAttemptState) bool {
	switch state {
	case store.GitHubJITStartDispatching,
		store.GitHubJITStartAmbiguous,
		store.GitHubJITAgentAccepted,
		store.GitHubJITStarted:
		return true
	default:
		return false
	}
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

func deterministicExecutionID(
	scaleSetID github.ScaleSetID,
	messageID int,
	runnerRequestID int64,
) domain.ExecutionID {
	return domain.ExecutionID("twk-exec-" + deterministicControllerToken(
		fmt.Sprintf(
			"execution\x00%d\x00%d\x00%d",
			scaleSetID,
			messageID,
			runnerRequestID,
		),
	))
}

func deterministicRunnerName(scaleSetID store.ScaleSetID, runnerRequestID int64) string {
	return "sparerunner-" + deterministicControllerToken(
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
