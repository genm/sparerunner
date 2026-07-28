package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/genm/sparerunner/internal/app"
	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/runner"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

const (
	liveSubsystem      = "spr-007-linux-live"
	liveShutdownBudget = 10 * time.Second
	liveStatePollDelay = 200 * time.Millisecond
)

var (
	errGitHubClientPreflight = errors.New("GitHub client preflight failed")
	errScaleSetPreflight     = errors.New("GitHub scale set preflight failed")
	errControllerPreflight   = errors.New("controller preflight failed")
	errAgentPreflight        = errors.New("Linux Agent preflight failed")
	errControllerServe       = errors.New("controller serving failed")
	errCoordinatorRun        = errors.New("controller runner coordination failed")
	errAcceptanceOutcome     = errors.New("live acceptance outcome failed")
	errShutdown              = errors.New("live acceptance shutdown failed")
)

type pollFirstDriver interface {
	PollOnce(context.Context) (*github.Message, error)
	DriveNext(context.Context) (bool, error)
}

type replayTracker struct {
	expected *replayEvidence
	store    snapshotReader
	evidence *evidenceStore
	targetID domain.TargetID
	nodeID   domain.NodeID
	scaleSet github.ScaleSetID
	epoch    domain.ControllerEpoch
	observed bool
}

type acceptanceOutcome struct {
	execution        domain.ExecutionSnapshot
	nodeState        domain.NodeAdministrativeState
	reservationCount int
	job              completedJobObservation
}

func runLiveAcceptance(
	parent context.Context,
	config liveConfig,
	mode acceptanceMode,
	logger *slog.Logger,
) (result resultEvidence, returnedErr error) {
	started := time.Now().UTC()
	result = resultEvidence{
		Version:   evidenceVersion,
		Mode:      string(mode),
		Status:    "failed",
		NodeID:    config.NodeID,
		StartedAt: started.Format(time.RFC3339Nano),
	}
	evidence, err := openEvidenceStore(config.EvidenceDirectory)
	if err != nil {
		return result, err
	}
	provenance, err := loadEvidenceFile[provenanceEvidence](
		config.EvidenceDirectory,
		provenanceFileName,
	)
	if err != nil {
		return result, errEvidenceInvalid
	}
	result.ProvenanceCommitSHA = provenance.CommitSHA
	result.HarnessSHA256 = provenance.HarnessSHA256
	replay, hasReplay, err := evidence.validateScenarioStart(mode)
	if err != nil {
		return result, err
	}
	defer func() {
		result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if returnedErr == nil {
			result.Status = "passed"
		} else {
			result.Status = "failed"
			result.ErrorClass = classifyLiveError(returnedErr)
		}
		if writeErr := evidence.writeResult(result); writeErr != nil {
			if returnedErr == nil {
				returnedErr = errEvidenceInvalid
			} else {
				returnedErr = errors.Join(returnedErr, errEvidenceInvalid)
			}
		}
	}()

	privateProof, err := loadPrivateRepositoryProof(config, time.Now())
	if err != nil {
		return result, err
	}
	result.PrivateRepository = strings.ToLower(privateProof.Repository)
	privateKeyBytes, err := loadGitHubPrivateKey(config.GitHub.PrivateKeyFile)
	if err != nil {
		return result, err
	}
	privateKey := github.NewAppPrivateKey(string(privateKeyBytes))
	clear(privateKeyBytes)
	client, err := github.NewAppClient(github.AppClientConfig{
		GitHubConfigURL: config.GitHub.ConfigURL,
		ClientID:        config.GitHub.ClientID,
		InstallationID:  config.GitHub.InstallationID,
		PrivateKey:      privateKey,
		System:          "sparerunner",
		Version:         "live-acceptance",
		CommitSHA:       "unknown",
		Subsystem:       liveSubsystem,
	})
	if err != nil {
		return result, errGitHubClientPreflight
	}

	runContext, cancelRun := context.WithTimeout(parent, config.runTimeout())
	defer cancelRun()
	scaleSet, err := client.GetScaleSet(
		runContext,
		config.GitHub.RunnerGroupID,
		config.GitHub.ScaleSetName,
	)
	if err != nil {
		return result, errGitHubClientPreflight
	}
	if err := validateLiveScaleSet(config, scaleSet); err != nil {
		return result, err
	}
	targetID := stableTargetID(config, *scaleSet)
	// Commands carry the target's scope so a node owner's desktop surface can
	// name the repository a running job belongs to. The live rig is always
	// repository-scoped, and its config URL is already validated as such.
	targetScope, err := repositoryFromConfigURL(config.GitHub.ConfigURL)
	if err != nil {
		return result, err
	}
	result.TargetID = string(targetID)
	result.ScaleSetID = int(scaleSet.ID)

	listener, err := (&net.ListenConfig{}).Listen(runContext, "tcp", config.AgentListenAddress)
	if err != nil {
		return result, errControllerServe
	}
	listenerOwned := true
	defer func() {
		if listenerOwned {
			_ = listener.Close()
		}
	}()

	state, err := app.OpenController(runContext, config.ControllerStateDirectory, true)
	if err != nil {
		return result, errControllerPreflight
	}
	stateOwned := true
	defer func() {
		if stateOwned {
			_ = state.Close()
		}
	}()
	result.ControllerEpoch = state.Epoch

	controllerSnapshot, err := state.Store.Snapshot(runContext)
	if err != nil {
		return result, errControllerPreflight
	}
	if err := validateControllerPreflight(
		controllerSnapshot,
		domain.NodeID(config.NodeID),
		targetID,
		scaleSet.ID,
		replay,
		hasReplay,
	); err != nil {
		return result, err
	}
	runnerProfileID, versionPolicy, err := configureLiveGitHubRuntime(
		runContext,
		state.Store,
		config,
		targetID,
		scaleSet.ID,
		github.NewRunnerReleaseObserver(),
	)
	if err != nil {
		return result, err
	}

	serverContext, cancelServer := context.WithCancel(context.Background())
	serverResult := make(chan error, 1)
	go func() {
		serverResult <- app.ServeController(serverContext, state, app.ControllerServeOptions{
			AgentListener: listener,
		})
	}()
	listenerOwned = false

	nodeContext, cancelNode := context.WithTimeout(runContext, config.nodeReadyTimeout())
	agentSnapshot, serverDone, err := waitForFreshLinuxAgent(
		nodeContext,
		state,
		domain.NodeID(config.NodeID),
		domain.ControllerEpoch(state.Epoch),
		serverResult,
	)
	cancelNode()
	if err != nil {
		shutdownErr := shutdownLiveRuntime(
			nil, cancelServer, listener, state, serverResult, serverDone, nil, false)
		stateOwned = false
		if shutdownErr != nil {
			return result, errors.Join(err, shutdownErr)
		}
		return result, err
	}
	_ = agentSnapshot

	repository, _ := repositoryFromConfigURL(config.GitHub.ConfigURL)
	owner := strings.SplitN(repository, "/", 2)[0]
	messageSession, err := client.OpenMessageSession(runContext, scaleSet.ID, owner)
	if err != nil {
		shutdownErr := shutdownLiveRuntime(
			nil, cancelServer, listener, state, serverResult, serverDone, nil, false)
		stateOwned = false
		if shutdownErr != nil {
			return result, errors.Join(errGitHubClientPreflight, shutdownErr)
		}
		return result, errGitHubClientPreflight
	}
	var session liveMessageSession = messageSession
	if mode == modeCommitBeforeAck {
		session = &ackGateSession{
			delegate:        messageSession,
			store:           state.Store,
			evidence:        evidence,
			targetID:        targetID,
			nodeID:          domain.NodeID(config.NodeID),
			scaleSetID:      scaleSet.ID,
			controllerEpoch: domain.ControllerEpoch(state.Epoch),
		}
	}
	lifecycle, err := app.NewGitHubClientRunnerLifecycle(client)
	if err != nil {
		shutdownErr := shutdownLiveRuntime(
			session, cancelServer, listener, state, serverResult, serverDone, nil, false)
		stateOwned = false
		if shutdownErr != nil {
			return result, errors.Join(errGitHubClientPreflight, shutdownErr)
		}
		return result, errGitHubClientPreflight
	}
	coordinator, err := app.NewControllerRunnerCoordinator(
		state.Store,
		session,
		state.AgentBroker,
		lifecycle,
		app.ControllerRunnerConfig{
			ScaleSetID:      scaleSet.ID,
			TargetID:        targetID,
			Scope:           targetScope,
			ScopeKind:       domain.TargetRepository,
			RunnerProfileID: runnerProfileID,
			VersionPolicy:   versionPolicy,
			NodeID:          domain.NodeID(config.NodeID),
			ControllerEpoch: domain.ControllerEpoch(state.Epoch),
			Reconciler:      state.Reconciler,
		},
		logger,
	)
	if err != nil {
		shutdownErr := shutdownLiveRuntime(
			session, cancelServer, listener, state, serverResult, serverDone, nil, false)
		stateOwned = false
		if shutdownErr != nil {
			return result, errors.Join(errCoordinatorRun, shutdownErr)
		}
		return result, errCoordinatorRun
	}

	events := newObservedJobEvents()
	if mode == modeAgentRestart {
		events.onStarted = func(runnerRequestID int64, observedAt time.Time) error {
			return evidence.recordRestartStarted(
				scaleSet.ID,
				runnerRequestID,
				liveExecutionID(scaleSet.ID, runnerRequestID),
				observedAt,
			)
		}
	}
	tracker := &replayTracker{
		store:    state.Store,
		evidence: evidence,
		targetID: targetID,
		nodeID:   domain.NodeID(config.NodeID),
		scaleSet: scaleSet.ID,
		epoch:    domain.ControllerEpoch(state.Epoch),
	}
	if hasReplay {
		tracker.expected = &replay
		result.Mode = string(modeCommitBeforeAck)
	}
	coordinatorContext, cancelCoordinator := context.WithCancel(runContext)
	coordinatorResult := make(chan error, 1)
	go func() {
		coordinatorResult <- runPollFirst(
			coordinatorContext,
			coordinator,
			events,
			tracker,
		)
	}()

	type outcome struct {
		value acceptanceOutcome
		err   error
	}
	outcomeResult := make(chan outcome, 1)
	var outcomeDone chan struct{}
	if mode != modeCommitBeforeAck {
		outcomeDone = make(chan struct{})
		go func() {
			defer close(outcomeDone)
			completed, err := waitForAcceptanceOutcome(
				coordinatorContext,
				state.Store,
				mode,
				targetID,
				domain.NodeID(config.NodeID),
				events,
			)
			outcomeResult <- outcome{value: completed, err: err}
		}()
	}

	coordinatorDone := false
	var runErr error
	select {
	case coordinatorErr := <-coordinatorResult:
		coordinatorDone = true
		if coordinatorErr == nil && runContext.Err() != nil {
			runErr = runContext.Err()
		} else if coordinatorErr == nil {
			runErr = errCoordinatorRun
		} else {
			runErr = coordinatorErr
		}
	case serverErr := <-serverResult:
		serverDone = true
		if serverErr == nil {
			runErr = errControllerServe
		} else {
			runErr = errControllerServe
		}
	case completed := <-outcomeResult:
		if completed.err != nil {
			runErr = completed.err
		} else {
			result.ExecutionID = string(completed.value.execution.ID)
			result.ExecutionState = string(completed.value.execution.State)
			result.NodeState = string(completed.value.nodeState)
			result.ReservationCount = completed.value.reservationCount
			result.RunnerRequestID = completed.value.job.requestID
			result.ObservedEvents = completed.value.job.events
			result.AvailableObservedAt = completed.value.job.availableObservedAt.UTC().Format(time.RFC3339Nano)
			result.JobStartedObservedAt = completed.value.job.startedObservedAt.UTC().Format(time.RFC3339Nano)
			result.JobCompletedObservedAt = completed.value.job.completedObservedAt.UTC().Format(time.RFC3339Nano)
			latencyMillis := completed.value.job.availableToStartedLatency.Milliseconds()
			result.AvailableToStartedMillis = &latencyMillis
		}
	case <-runContext.Done():
		runErr = errAcceptanceOutcome
	}

	cancelCoordinator()
	var outcomeWaitErr error
	if outcomeDone != nil {
		select {
		case <-outcomeDone:
		case <-time.After(liveShutdownBudget):
			outcomeWaitErr = errShutdown
		}
	}
	shutdownErr := shutdownLiveRuntime(
		session,
		cancelServer,
		listener,
		state,
		serverResult,
		serverDone,
		coordinatorResult,
		coordinatorDone,
	)
	stateOwned = false
	if outcomeWaitErr != nil {
		if shutdownErr == nil {
			shutdownErr = outcomeWaitErr
		} else {
			shutdownErr = errors.Join(shutdownErr, outcomeWaitErr)
		}
	}
	if runErr != nil {
		if shutdownErr != nil {
			return result, errors.Join(runErr, shutdownErr)
		}
		return result, runErr
	}
	if shutdownErr != nil {
		return result, shutdownErr
	}
	if tracker.expected != nil && !tracker.observed {
		return result, errAcceptanceOutcome
	}
	return result, nil
}

func validateLiveScaleSet(config liveConfig, scaleSet *github.ScaleSet) error {
	if scaleSet == nil || scaleSet.ID <= 0 ||
		scaleSet.Name != liveScaleSetName ||
		scaleSet.Name != config.GitHub.ScaleSetName ||
		scaleSet.RunnerGroupID != config.GitHub.RunnerGroupID ||
		scaleSet.DisableUpdate != config.GitHub.DisableUpdate ||
		len(scaleSet.Labels) != 1 || scaleSet.Labels[0] != liveScaleSetName {
		return errScaleSetPreflight
	}
	return nil
}

func stableTargetID(config liveConfig, scaleSet github.ScaleSet) domain.TargetID {
	repository, _ := repositoryFromConfigURL(config.GitHub.ConfigURL)
	identity := fmt.Sprintf(
		"v1\x00%s\x00%d\x00%d\x00%d\x00%s",
		repository,
		config.GitHub.InstallationID,
		config.GitHub.RunnerGroupID,
		scaleSet.ID,
		scaleSet.Name,
	)
	digest := sha256.Sum256([]byte(identity))
	return domain.TargetID("spr-live-" + hex.EncodeToString(digest[:]))
}

type liveRunnerReleaseObserver interface {
	Latest(context.Context) (github.RunnerRelease, error)
}

func configureLiveGitHubRuntime(
	ctx context.Context,
	stateStore *store.ControllerStore,
	config liveConfig,
	targetID domain.TargetID,
	scaleSetID github.ScaleSetID,
	releaseObserver liveRunnerReleaseObserver,
) (domain.RunnerProfileID, domain.RunnerVersionPolicy, error) {
	if stateStore == nil || targetID == "" || scaleSetID <= 0 {
		return "", "", errControllerPreflight
	}
	versionPolicy := domain.RunnerVersionAutoUpdate
	if config.GitHub.DisableUpdate {
		versionPolicy = domain.RunnerVersionPinned
	}
	profileID := domain.RunnerProfileID("profile-" + string(targetID))
	if _, err := stateStore.ConfigureRunnerProfile(ctx, store.RunnerProfileUpdatePolicy{
		ProfileID:     profileID,
		VersionPolicy: versionPolicy,
		RunnerVersion: runner.OfficialRunnerVersion,
		Revision:      1,
	}); err != nil {
		return "", "", errControllerPreflight
	}
	if _, err := stateStore.ConfigureGitHubTargetRuntimeBinding(
		ctx,
		store.GitHubTargetRuntimeBinding{
			TargetID:   targetID,
			ScaleSetID: store.ScaleSetID(scaleSetID),
			ProfileID:  profileID,
		},
	); err != nil {
		return "", "", errControllerPreflight
	}
	if versionPolicy == domain.RunnerVersionPinned {
		if releaseObserver == nil {
			return "", "", errGitHubClientPreflight
		}
		if _, err := app.RefreshGitHubRunnerRelease(
			ctx,
			stateStore,
			releaseObserver,
		); err != nil {
			if errors.Is(err, app.ErrGitHubRunnerReleaseStore) {
				return "", "", errControllerPreflight
			}
			return "", "", errGitHubClientPreflight
		}
	}
	return profileID, versionPolicy, nil
}

func validateControllerPreflight(
	snapshot store.ControllerSnapshot,
	nodeID domain.NodeID,
	targetID domain.TargetID,
	scaleSetID github.ScaleSetID,
	replay replayEvidence,
	hasReplay bool,
) error {
	if snapshot.ControllerEpoch == 0 || nodeID == "" || targetID == "" || scaleSetID <= 0 {
		return errControllerPreflight
	}
	activeNode := false
	for _, node := range snapshot.Nodes {
		if node.NodeID != nodeID {
			continue
		}
		if activeNode || node.State != domain.NodeActive {
			return errControllerPreflight
		}
		activeNode = true
	}
	if !activeNode {
		return errControllerPreflight
	}
	if !hasReplay {
		if len(snapshot.Reservations) != 0 || len(snapshot.Executions) != 0 {
			return errControllerPreflight
		}
		return nil
	}
	if replay.Phase != "killed_before_ack" ||
		replay.TargetID != string(targetID) ||
		replay.NodeID != string(nodeID) ||
		replay.ScaleSetID != int(scaleSetID) ||
		replay.MessageID <= 0 || replay.ExecutionID == "" ||
		replay.CommitControllerEpoch == 0 ||
		replay.CommitControllerEpoch >= uint64(snapshot.ControllerEpoch) ||
		len(snapshot.Executions) != 1 || len(snapshot.Reservations) != 1 {
		return errControllerPreflight
	}
	execution := snapshot.Executions[0]
	reservation := snapshot.Reservations[0]
	if execution.ID != domain.ExecutionID(replay.ExecutionID) ||
		execution.TargetID != targetID ||
		execution.Slot != (domain.SlotKey{NodeID: nodeID, Index: 0}) ||
		execution.State != domain.ExecutionReserved ||
		reservation.Slot != execution.Slot ||
		reservation.Owner != (domain.SlotOwner{
			TargetID: targetID, ExecutionID: execution.ID,
		}) {
		return errControllerPreflight
	}
	return nil
}

func waitForFreshLinuxAgent(
	ctx context.Context,
	state *app.ControllerState,
	nodeID domain.NodeID,
	epoch domain.ControllerEpoch,
	serverResult <-chan error,
) (transport.AgentSnapshot, bool, error) {
	ticker := time.NewTicker(liveStatePollDelay)
	defer ticker.Stop()
	for {
		snapshot, online := state.AgentBroker.Snapshot(nodeID)
		if online {
			if err := validateFreshLinuxAgent(snapshot, nodeID, epoch); err != nil {
				return transport.AgentSnapshot{}, false, err
			}
			return snapshot, false, nil
		}
		select {
		case <-ctx.Done():
			return transport.AgentSnapshot{}, false, errAgentPreflight
		case <-ticker.C:
		case <-serverResult:
			return transport.AgentSnapshot{}, true, errControllerServe
		}
	}
}

func validateFreshLinuxAgent(
	snapshot transport.AgentSnapshot,
	nodeID domain.NodeID,
	epoch domain.ControllerEpoch,
) error {
	if snapshot.Validate() != nil ||
		snapshot.NodeID != nodeID ||
		snapshot.OS != domain.OSLinux ||
		(snapshot.Arch != domain.ArchAMD64 && snapshot.Arch != domain.ArchARM64) ||
		snapshot.RunnerVersion != runner.OfficialRunnerVersion ||
		!snapshot.NativeRunnerReady ||
		snapshot.MaxControllerEpoch > epoch ||
		len(snapshot.Commands) != 0 ||
		len(snapshot.Observations) != 0 ||
		len(snapshot.CleanupTombstones) != 0 {
		return errAgentPreflight
	}
	return nil
}

func runPollFirst(
	ctx context.Context,
	driver pollFirstDriver,
	events *observedJobEvents,
	replay *replayTracker,
) error {
	if driver == nil || events == nil || replay == nil {
		return errCoordinatorRun
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		message, err := driver.PollOnce(ctx)
		if message != nil {
			if observeErr := events.observe(message); observeErr != nil {
				return errCoordinatorRun
			}
			if observeErr := replay.observe(ctx, message, events); observeErr != nil {
				return observeErr
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, app.ErrGitHubAvailableUnclaimed) {
				timer := time.NewTimer(liveStatePollDelay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
				}
				continue
			}
			var providerFailure *github.ProviderFailure
			if errors.As(err, &providerFailure) {
				timer := time.NewTimer(liveStatePollDelay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
				}
				continue
			}
			return errors.Join(errCoordinatorRun, err)
		}
		if _, err := driver.DriveNext(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errors.Join(errCoordinatorRun, err)
		}
	}
}

func (tracker *replayTracker) observe(
	ctx context.Context,
	message *github.Message,
	events *observedJobEvents,
) error {
	if tracker.expected == nil || tracker.observed || message == nil {
		return nil
	}
	expected := tracker.expected
	if expected.Phase != "killed_before_ack" ||
		message.ScaleSetID != tracker.scaleSet ||
		message.ID != expected.MessageID {
		return errAcceptanceOutcome
	}
	availableAt, err := time.Parse(time.RFC3339Nano, expected.AvailableObservedAt)
	if err != nil {
		return errAcceptanceOutcome
	}
	availableMatches := 0
	for _, job := range message.Jobs {
		if job.Type == github.MessageTypeJobAvailable &&
			job.RunnerRequestID == expected.RunnerRequestID {
			availableMatches++
		}
	}
	if availableMatches != 1 || events.seedAvailable(expected.RunnerRequestID, availableAt) != nil {
		return errAcceptanceOutcome
	}
	snapshot, err := tracker.store.Snapshot(ctx)
	if err != nil || len(snapshot.Executions) != 1 {
		return errAcceptanceOutcome
	}
	execution := snapshot.Executions[0]
	if execution.ID != domain.ExecutionID(expected.ExecutionID) ||
		execution.TargetID != tracker.targetID ||
		execution.Slot != (domain.SlotKey{NodeID: tracker.nodeID, Index: 0}) {
		return errAcceptanceOutcome
	}
	if err := tracker.evidence.writeReplay(replayEvidence{
		Version:                   evidenceVersion,
		Phase:                     "redelivered_same_execution",
		TargetID:                  string(tracker.targetID),
		NodeID:                    string(tracker.nodeID),
		ScaleSetID:                int(tracker.scaleSet),
		MessageID:                 message.ID,
		RunnerRequestID:           expected.RunnerRequestID,
		ExecutionID:               string(execution.ID),
		AvailableObservedAt:       expected.AvailableObservedAt,
		CommitControllerEpoch:     expected.CommitControllerEpoch,
		CommitObservedAt:          expected.CommitObservedAt,
		KillExitStatus:            expected.KillExitStatus,
		KilledBeforeAckObservedAt: expected.KilledBeforeAckObservedAt,
		RedeliveryControllerEpoch: uint64(tracker.epoch),
		RedeliveryObservedAt:      time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return errAcceptanceOutcome
	}
	tracker.observed = true
	return nil
}

func waitForAcceptanceOutcome(
	ctx context.Context,
	reader snapshotReader,
	mode acceptanceMode,
	targetID domain.TargetID,
	nodeID domain.NodeID,
	events *observedJobEvents,
) (acceptanceOutcome, error) {
	ticker := time.NewTicker(liveStatePollDelay)
	defer ticker.Stop()
	for {
		snapshot, err := reader.Snapshot(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return acceptanceOutcome{}, errAcceptanceOutcome
			}
			return acceptanceOutcome{}, errAcceptanceOutcome
		}
		execution, found, err := acceptanceExecution(snapshot, targetID, nodeID)
		if err != nil {
			return acceptanceOutcome{}, err
		}
		if found {
			job, completed, err := events.completedSingleJob()
			if err != nil {
				return acceptanceOutcome{}, errAcceptanceOutcome
			}
			nodeState, ok := controllerNodeState(snapshot, nodeID)
			if !ok {
				return acceptanceOutcome{}, errAcceptanceOutcome
			}
			switch mode {
			case modeNormal, modeAgentRestart:
				if execution.State == domain.ExecutionFailed ||
					execution.State == domain.ExecutionCleanupFailed ||
					execution.State == domain.ExecutionQuarantined ||
					nodeState != domain.NodeActive {
					return acceptanceOutcome{}, errAcceptanceOutcome
				}
				if execution.State == domain.ExecutionReleased && completed {
					if len(snapshot.Reservations) != 0 {
						return acceptanceOutcome{}, errAcceptanceOutcome
					}
					return acceptanceOutcome{
						execution:        execution,
						nodeState:        nodeState,
						reservationCount: len(snapshot.Reservations),
						job:              job,
					}, nil
				}
			case modeCleanupFailure:
				if execution.State == domain.ExecutionReleased ||
					execution.State == domain.ExecutionFailed {
					return acceptanceOutcome{}, errAcceptanceOutcome
				}
				if (execution.State == domain.ExecutionCleanupFailed ||
					execution.State == domain.ExecutionQuarantined) &&
					nodeState == domain.NodeQuarantined && completed {
					if len(snapshot.Reservations) != 1 ||
						snapshot.Reservations[0].Slot != execution.Slot ||
						snapshot.Reservations[0].Owner.ExecutionID != execution.ID {
						return acceptanceOutcome{}, errAcceptanceOutcome
					}
					return acceptanceOutcome{
						execution:        execution,
						nodeState:        nodeState,
						reservationCount: len(snapshot.Reservations),
						job:              job,
					}, nil
				}
			default:
				return acceptanceOutcome{}, errAcceptanceOutcome
			}
		}
		select {
		case <-ctx.Done():
			return acceptanceOutcome{}, errAcceptanceOutcome
		case <-ticker.C:
		}
	}
}

func acceptanceExecution(
	snapshot store.ControllerSnapshot,
	targetID domain.TargetID,
	nodeID domain.NodeID,
) (domain.ExecutionSnapshot, bool, error) {
	if len(snapshot.Executions) == 0 {
		return domain.ExecutionSnapshot{}, false, nil
	}
	if len(snapshot.Executions) != 1 {
		return domain.ExecutionSnapshot{}, false, errAcceptanceOutcome
	}
	execution := snapshot.Executions[0]
	if execution.TargetID != targetID ||
		execution.Slot != (domain.SlotKey{NodeID: nodeID, Index: 0}) {
		return domain.ExecutionSnapshot{}, false, errAcceptanceOutcome
	}
	return execution, true, nil
}

func controllerNodeState(
	snapshot store.ControllerSnapshot,
	nodeID domain.NodeID,
) (domain.NodeAdministrativeState, bool) {
	for _, node := range snapshot.Nodes {
		if node.NodeID == nodeID {
			return node.State, true
		}
	}
	return "", false
}

func shutdownLiveRuntime(
	session liveMessageSession,
	cancelServer context.CancelFunc,
	listener net.Listener,
	state *app.ControllerState,
	serverResult <-chan error,
	serverDone bool,
	coordinatorResult <-chan error,
	coordinatorDone bool,
) error {
	var failures []error
	if session != nil {
		closeContext, cancel := context.WithTimeout(context.Background(), liveShutdownBudget)
		if err := session.Close(closeContext); err != nil {
			failures = append(failures, errShutdown)
		}
		cancel()
	}
	if coordinatorResult != nil && !coordinatorDone {
		select {
		case <-coordinatorResult:
		case <-time.After(liveShutdownBudget):
			failures = append(failures, errShutdown)
		}
	}
	if cancelServer != nil {
		cancelServer()
	}
	if listener != nil {
		_ = listener.Close()
	}
	if serverResult != nil && !serverDone {
		select {
		case <-serverResult:
		case <-time.After(liveShutdownBudget):
			failures = append(failures, errShutdown)
		}
	}
	if state != nil {
		if err := state.Close(); err != nil {
			failures = append(failures, errShutdown)
		}
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	return nil
}

func classifyLiveError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errConfigInvalid):
		return "config_invalid"
	case errors.Is(err, errPrivateProofInvalid):
		return "private_repository_unverified"
	case errors.Is(err, errCredentialUnavailable):
		return "credential_unavailable"
	case errors.Is(err, errGitHubClientPreflight):
		return "github_client_preflight_failed"
	case errors.Is(err, errScaleSetPreflight):
		return "scale_set_preflight_failed"
	case errors.Is(err, errControllerPreflight):
		return "controller_preflight_failed"
	case errors.Is(err, errAgentPreflight):
		return "agent_preflight_failed"
	case errors.Is(err, errControllerServe):
		return "controller_serve_failed"
	case errors.Is(err, errAckGateInvalid):
		return "commit_before_ack_gate_failed"
	case errors.Is(err, errCoordinatorRun):
		return "coordinator_failed"
	case errors.Is(err, errAcceptanceOutcome):
		return "acceptance_outcome_failed"
	case errors.Is(err, errShutdown):
		return "shutdown_failed"
	case errors.Is(err, errEvidenceInvalid):
		return "evidence_failed"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "unclassified_failure"
	}
}
