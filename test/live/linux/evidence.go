package main

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/store"
)

const (
	evidenceVersion                 = 1
	resultFileName                  = "result.json"
	replayFileName                  = "controller-replay.json"
	processBeforeName               = "processes-before.json"
	processAfterName                = "processes-after.json"
	processRunningBeforeRestartName = "processes-running-before-restart.json"
	processRunningAfterRestartName  = "processes-running-after-restart.json"
	filesystemName                  = "filesystem-after.json"
	restartStartedName              = "agent-restart-started.json"
	authorityFileName               = "authority.json"
	provenanceFileName              = "provenance.json"
	injectorFileName                = "injector.json"
)

var (
	errEvidenceInvalid = errors.New("live acceptance evidence is invalid")
	errAckGateInvalid  = errors.New("commit-before-ack gate precondition failed")
)

type resultEvidence struct {
	Version                  int      `json:"version"`
	Mode                     string   `json:"mode"`
	Status                   string   `json:"status"`
	ErrorClass               string   `json:"errorClass,omitempty"`
	TargetID                 string   `json:"targetId,omitempty"`
	PrivateRepository        string   `json:"privateRepository,omitempty"`
	NodeID                   string   `json:"nodeId,omitempty"`
	ScaleSetID               int      `json:"scaleSetId,omitempty"`
	ControllerEpoch          uint64   `json:"controllerEpoch,omitempty"`
	ExecutionID              string   `json:"executionId,omitempty"`
	ExecutionState           string   `json:"executionState,omitempty"`
	NodeState                string   `json:"nodeState,omitempty"`
	ReservationCount         int      `json:"reservationCount"`
	ClaimKey                 int64    `json:"runnerRequestId,omitempty"`
	ObservedEvents           []string `json:"observedEvents,omitempty"`
	AvailableObservedAt      string   `json:"availableObservedAt,omitempty"`
	JobStartedObservedAt     string   `json:"startedObservedAt,omitempty"`
	JobCompletedObservedAt   string   `json:"completedObservedAt,omitempty"`
	AvailableToStartedMillis *int64   `json:"availableToStartedMillis,omitempty"`
	ProvenanceCommitSHA      string   `json:"provenanceCommitSha,omitempty"`
	HarnessSHA256            string   `json:"harnessSha256,omitempty"`
	StartedAt                string   `json:"startedAt"`
	FinishedAt               string   `json:"finishedAt"`
}

type replayEvidence struct {
	Version                   int    `json:"version"`
	Phase                     string `json:"phase"`
	TargetID                  string `json:"targetId"`
	NodeID                    string `json:"nodeId"`
	ScaleSetID                int    `json:"scaleSetId"`
	MessageID                 int    `json:"messageId"`
	ClaimKey                  int64  `json:"runnerRequestId"`
	ExecutionID               string `json:"executionId"`
	AvailableObservedAt       string `json:"availableObservedAt"`
	CommitControllerEpoch     uint64 `json:"commitControllerEpoch"`
	CommitObservedAt          string `json:"commitObservedAt"`
	KillExitStatus            int    `json:"killExitStatus,omitempty"`
	KilledBeforeAckObservedAt string `json:"killedBeforeAckObservedAt,omitempty"`
	RedeliveryControllerEpoch uint64 `json:"redeliveryControllerEpoch,omitempty"`
	RedeliveryObservedAt      string `json:"redeliveryObservedAt,omitempty"`
}

type restartStartedEvidence struct {
	Version     int    `json:"version"`
	ScaleSetID  int    `json:"scaleSetId"`
	ClaimKey    int64  `json:"runnerRequestId"`
	ExecutionID string `json:"executionId"`
	ObservedAt  string `json:"observedAt"`
}

type evidenceStore struct {
	directory string
}

func openEvidenceStore(directory string) (*evidenceStore, error) {
	if !canonicalAbsolutePath(directory) {
		return nil, errEvidenceInvalid
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, errEvidenceInvalid
	}
	if err := trustedDirectory(directory, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o700 {
		return nil, errEvidenceInvalid
	}
	return &evidenceStore{directory: directory}, nil
}

func (evidence *evidenceStore) writeResult(result resultEvidence) error {
	if result.Version != evidenceVersion ||
		(result.Mode != string(modeNormal) &&
			result.Mode != string(modeCommitBeforeAck) &&
			result.Mode != string(modeCleanupFailure) &&
			result.Mode != string(modeAgentRestart)) ||
		(result.Status != "passed" && result.Status != "failed") ||
		result.StartedAt == "" || result.FinishedAt == "" {
		return errEvidenceInvalid
	}
	startedAt, err := time.Parse(time.RFC3339Nano, result.StartedAt)
	if err != nil || startedAt.IsZero() {
		return errEvidenceInvalid
	}
	finishedAt, err := time.Parse(time.RFC3339Nano, result.FinishedAt)
	if err != nil || finishedAt.Before(startedAt) {
		return errEvidenceInvalid
	}
	if result.AvailableToStartedMillis != nil &&
		(*result.AvailableToStartedMillis < 0 || *result.AvailableToStartedMillis > 60_000) {
		return errEvidenceInvalid
	}
	if result.Status == "passed" {
		if result.ErrorClass != "" ||
			result.TargetID == "" || result.PrivateRepository == "" ||
			result.NodeID == "" || result.ScaleSetID <= 0 ||
			result.ControllerEpoch == 0 ||
			result.ExecutionID == "" || result.ClaimKey <= 0 ||
			result.AvailableObservedAt == "" || result.JobStartedObservedAt == "" ||
			result.JobCompletedObservedAt == "" ||
			result.AvailableToStartedMillis == nil {
			return errEvidenceInvalid
		}
		availableAt, availableErr := time.Parse(time.RFC3339Nano, result.AvailableObservedAt)
		startedObservedAt, startedErr := time.Parse(
			time.RFC3339Nano,
			result.JobStartedObservedAt,
		)
		if availableErr != nil || startedErr != nil ||
			startedObservedAt.Before(availableAt) ||
			startedObservedAt.Sub(availableAt).Milliseconds() !=
				*result.AvailableToStartedMillis {
			return errEvidenceInvalid
		}
		completedObservedAt, completedErr := time.Parse(
			time.RFC3339Nano,
			result.JobCompletedObservedAt,
		)
		if completedErr != nil || completedObservedAt.Before(startedObservedAt) {
			return errEvidenceInvalid
		}
		switch acceptanceMode(result.Mode) {
		case modeNormal, modeCommitBeforeAck, modeAgentRestart:
			if result.ExecutionState != string(domain.ExecutionReleased) ||
				result.NodeState != string(domain.NodeActive) ||
				result.ReservationCount != 0 {
				return errEvidenceInvalid
			}
		case modeCleanupFailure:
			if (result.ExecutionState != string(domain.ExecutionCleanupFailed) &&
				result.ExecutionState != string(domain.ExecutionQuarantined)) ||
				result.NodeState != string(domain.NodeQuarantined) ||
				result.ReservationCount != 1 {
				return errEvidenceInvalid
			}
		}
	}
	return evidence.writeJSON(resultFileName, result)
}

func (evidence *evidenceStore) writeReplay(replay replayEvidence) error {
	if validateReplayEvidence(replay) != nil {
		return errEvidenceInvalid
	}
	return evidence.writeJSON(replayFileName, replay)
}

func (evidence *evidenceStore) loadReplay() (replayEvidence, bool, error) {
	path := filepath.Join(evidence.directory, replayFileName)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return replayEvidence{}, false, nil
	} else if err != nil {
		return replayEvidence{}, false, errEvidenceInvalid
	}
	var replay replayEvidence
	if err := decodeStrictRegularJSONFile(path, maxLiveConfigBytes, &replay, true); err != nil {
		return replayEvidence{}, false, errEvidenceInvalid
	}
	if validateReplayEvidence(replay) != nil {
		return replayEvidence{}, false, errEvidenceInvalid
	}
	return replay, true, nil
}

func validateReplayEvidence(replay replayEvidence) error {
	if replay.Version != evidenceVersion ||
		(replay.Phase != "committed_before_ack" &&
			replay.Phase != "killed_before_ack" &&
			replay.Phase != "redelivered_same_execution") ||
		replay.TargetID == "" || replay.NodeID == "" || replay.ScaleSetID <= 0 ||
		replay.MessageID <= 0 || replay.ClaimKey <= 0 ||
		replay.ExecutionID == "" || replay.CommitControllerEpoch == 0 {
		return errEvidenceInvalid
	}
	availableAt, err := time.Parse(time.RFC3339Nano, replay.AvailableObservedAt)
	if err != nil || availableAt.IsZero() {
		return errEvidenceInvalid
	}
	commitAt, err := time.Parse(time.RFC3339Nano, replay.CommitObservedAt)
	if err != nil || commitAt.Before(availableAt) {
		return errEvidenceInvalid
	}
	switch replay.Phase {
	case "committed_before_ack":
		if replay.KillExitStatus != 0 ||
			replay.KilledBeforeAckObservedAt != "" ||
			replay.RedeliveryControllerEpoch != 0 ||
			replay.RedeliveryObservedAt != "" {
			return errEvidenceInvalid
		}
	case "killed_before_ack":
		if replay.KillExitStatus != 137 ||
			replay.KilledBeforeAckObservedAt == "" ||
			replay.RedeliveryControllerEpoch != 0 ||
			replay.RedeliveryObservedAt != "" {
			return errEvidenceInvalid
		}
		killedAt, parseErr := time.Parse(time.RFC3339Nano, replay.KilledBeforeAckObservedAt)
		if parseErr != nil || killedAt.Before(commitAt) {
			return errEvidenceInvalid
		}
	case "redelivered_same_execution":
		if replay.KillExitStatus != 137 ||
			replay.KilledBeforeAckObservedAt == "" ||
			replay.RedeliveryControllerEpoch <= replay.CommitControllerEpoch ||
			replay.RedeliveryObservedAt == "" {
			return errEvidenceInvalid
		}
		killedAt, parseErr := time.Parse(time.RFC3339Nano, replay.KilledBeforeAckObservedAt)
		if parseErr != nil || killedAt.Before(commitAt) {
			return errEvidenceInvalid
		}
		redeliveredAt, parseErr := time.Parse(time.RFC3339Nano, replay.RedeliveryObservedAt)
		if parseErr != nil || redeliveredAt.Before(killedAt) {
			return errEvidenceInvalid
		}
	}
	return nil
}

func (evidence *evidenceStore) recordAckGateKill(observedAt time.Time) error {
	if observedAt.IsZero() {
		return errEvidenceInvalid
	}
	replay, found, err := evidence.loadReplay()
	if err != nil || !found || replay.Phase != "committed_before_ack" {
		return errEvidenceInvalid
	}
	replay.Phase = "killed_before_ack"
	replay.KillExitStatus = 137
	replay.KilledBeforeAckObservedAt = observedAt.UTC().Format(time.RFC3339Nano)
	return evidence.writeReplay(replay)
}

func (evidence *evidenceStore) recordRestartStarted(
	scaleSetID github.ScaleSetID,
	claimKey int64,
	executionID domain.ExecutionID,
	observedAt time.Time,
) error {
	if scaleSetID <= 0 || claimKey <= 0 || executionID == "" || observedAt.IsZero() {
		return errEvidenceInvalid
	}
	path := filepath.Join(evidence.directory, restartStartedName)
	var existing restartStartedEvidence
	if err := decodeStrictRegularJSONFile(path, maxLiveConfigBytes, &existing, true); err == nil {
		if existing.Version != evidenceVersion ||
			existing.ScaleSetID != int(scaleSetID) ||
			existing.ClaimKey != claimKey ||
			existing.ExecutionID != string(executionID) ||
			existing.ObservedAt == "" {
			return errEvidenceInvalid
		}
		return nil
	} else if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
		return errEvidenceInvalid
	}
	return evidence.writeJSON(restartStartedName, restartStartedEvidence{
		Version:     evidenceVersion,
		ScaleSetID:  int(scaleSetID),
		ClaimKey:    claimKey,
		ExecutionID: string(executionID),
		ObservedAt:  observedAt.UTC().Format(time.RFC3339Nano),
	})
}

func liveExecutionID(scaleSetID github.ScaleSetID, claimKey int64) domain.ExecutionID {
	value := "execution\x00" + strconv.Itoa(int(scaleSetID)) + "\x00" +
		strconv.FormatInt(claimKey, 10)
	digest := sha256.Sum256([]byte(value))
	token := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(digest[:])
	return domain.ExecutionID("spr-exec-" + strings.ToLower(token))
}

func (evidence *evidenceStore) validateScenarioStart(
	mode acceptanceMode,
) (replayEvidence, bool, error) {
	for _, name := range []string{
		resultFileName,
		processAfterName,
		processRunningBeforeRestartName,
		processRunningAfterRestartName,
		filesystemName,
		restartStartedName,
	} {
		if _, err := os.Lstat(filepath.Join(evidence.directory, name)); err == nil {
			return replayEvidence{}, false, errEvidenceInvalid
		} else if !errors.Is(err, os.ErrNotExist) {
			return replayEvidence{}, false, errEvidenceInvalid
		}
	}
	replay, found, err := evidence.loadReplay()
	if err != nil {
		return replayEvidence{}, false, err
	}
	if mode != modeNormal && found {
		return replayEvidence{}, false, errEvidenceInvalid
	}
	return replay, found, nil
}

func (evidence *evidenceStore) writeJSON(name string, value any) error {
	switch name {
	case resultFileName,
		replayFileName,
		processBeforeName,
		processAfterName,
		processRunningBeforeRestartName,
		processRunningAfterRestartName,
		filesystemName,
		restartStartedName:
	case authorityFileName:
	case provenanceFileName, injectorFileName:
	default:
		return errEvidenceInvalid
	}
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return errEvidenceInvalid
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(evidence.directory, ".sparerunner-live-evidence-")
	if err != nil {
		return errEvidenceInvalid
	}
	temporaryName := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return errEvidenceInvalid
	}
	if _, err := temporary.Write(payload); err != nil {
		return errEvidenceInvalid
	}
	if err := temporary.Sync(); err != nil {
		return errEvidenceInvalid
	}
	if err := temporary.Close(); err != nil {
		return errEvidenceInvalid
	}
	target := filepath.Join(evidence.directory, name)
	if err := os.Rename(temporaryName, target); err != nil {
		return errEvidenceInvalid
	}
	keep = true
	directory, err := os.Open(evidence.directory)
	if err != nil {
		return errEvidenceInvalid
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return errEvidenceInvalid
	}
	return nil
}

type snapshotReader interface {
	Snapshot(context.Context) (store.ControllerSnapshot, error)
}

type liveMessageSession interface {
	github.MessageSource
	AcquireJobs(context.Context, []int64) ([]int64, error)
	Close(context.Context) error
}

type ackGateSession struct {
	delegate        liveMessageSession
	store           snapshotReader
	evidence        *evidenceStore
	targetID        domain.TargetID
	nodeID          domain.NodeID
	scaleSetID      github.ScaleSetID
	controllerEpoch domain.ControllerEpoch
	mu              sync.Mutex
	polledMessageID int
	claimKey        int64
	availableAt     time.Time
	now             func() time.Time
}

func (gate *ackGateSession) Snapshot() (github.SessionSnapshot, error) {
	return gate.delegate.Snapshot()
}

func (gate *ackGateSession) Poll(
	ctx context.Context,
	lastAcknowledgedMessageID int,
	maxCapacity int,
) (*github.Message, error) {
	message, err := gate.delegate.Poll(ctx, lastAcknowledgedMessageID, maxCapacity)
	if err != nil || message == nil {
		return message, err
	}
	var claimKey int64
	availableCount := 0
	for _, job := range message.Jobs {
		if job.Type == github.MessageTypeJobAvailable {
			availableCount++
			claimKey = job.RunnerRequestID
		}
	}
	observedAt := time.Now()
	if gate.now != nil {
		observedAt = gate.now()
	}
	gate.mu.Lock()
	gate.polledMessageID = message.ID
	if availableCount == 1 && !observedAt.IsZero() {
		gate.claimKey = claimKey
		gate.availableAt = observedAt
	} else {
		gate.claimKey = 0
		gate.availableAt = time.Time{}
	}
	gate.mu.Unlock()
	return message, nil
}

func (gate *ackGateSession) AcquireJobs(ctx context.Context, requestIDs []int64) ([]int64, error) {
	return gate.delegate.AcquireJobs(ctx, requestIDs)
}

func (gate *ackGateSession) Close(ctx context.Context) error {
	return gate.delegate.Close(ctx)
}

func (gate *ackGateSession) DeleteMessage(ctx context.Context, messageID int) error {
	if gate == nil || gate.delegate == nil || gate.store == nil || gate.evidence == nil ||
		gate.targetID == "" || gate.nodeID == "" || gate.scaleSetID <= 0 ||
		gate.controllerEpoch == 0 || messageID <= 0 {
		return errAckGateInvalid
	}
	gate.mu.Lock()
	polledMessageID := gate.polledMessageID
	claimKey := gate.claimKey
	availableAt := gate.availableAt
	gate.mu.Unlock()
	if polledMessageID != messageID || claimKey <= 0 || availableAt.IsZero() {
		return errAckGateInvalid
	}
	snapshot, err := gate.store.Snapshot(ctx)
	if err != nil {
		return errAckGateInvalid
	}
	execution, err := exactActiveExecution(snapshot, gate.targetID, gate.nodeID)
	if err != nil || execution.State != domain.ExecutionReserved ||
		len(snapshot.Reservations) != 1 ||
		snapshot.Reservations[0].Slot != execution.Slot ||
		snapshot.Reservations[0].Owner.TargetID != execution.TargetID ||
		snapshot.Reservations[0].Owner.ExecutionID != execution.ID {
		return errAckGateInvalid
	}
	// Start the 60-second window when the available message first crosses this
	// process boundary. The later SQLite commit and pre-ack evidence write must
	// not erase time already spent durably reserving the slot.
	durableAvailableAt := availableAt.UTC().Format(time.RFC3339Nano)
	commitObservedAt := time.Now()
	if gate.now != nil {
		commitObservedAt = gate.now()
	}
	if commitObservedAt.IsZero() || commitObservedAt.Before(availableAt) {
		return errAckGateInvalid
	}
	if err := gate.evidence.writeReplay(replayEvidence{
		Version:               evidenceVersion,
		Phase:                 "committed_before_ack",
		TargetID:              string(gate.targetID),
		NodeID:                string(gate.nodeID),
		ScaleSetID:            int(gate.scaleSetID),
		MessageID:             messageID,
		ClaimKey:              claimKey,
		ExecutionID:           string(execution.ID),
		AvailableObservedAt:   durableAvailableAt,
		CommitControllerEpoch: uint64(gate.controllerEpoch),
		CommitObservedAt:      commitObservedAt.UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return errAckGateInvalid
	}
	// The driver observes the durable marker and SIGKILLs this exact process.
	// Returning or delegating here would acknowledge the message and invalidate
	// the crash-window acceptance case.
	<-ctx.Done()
	return ctx.Err()
}

func exactActiveExecution(
	snapshot store.ControllerSnapshot,
	targetID domain.TargetID,
	nodeID domain.NodeID,
) (domain.ExecutionSnapshot, error) {
	var found *domain.ExecutionSnapshot
	for index := range snapshot.Executions {
		execution := snapshot.Executions[index]
		if execution.TargetID != targetID || execution.Slot.NodeID != nodeID ||
			execution.Slot.Index != 0 {
			continue
		}
		if execution.State == domain.ExecutionReleased ||
			execution.State == domain.ExecutionFailed ||
			execution.State == domain.ExecutionQuarantined {
			continue
		}
		if found != nil {
			return domain.ExecutionSnapshot{}, errAckGateInvalid
		}
		copy := execution
		found = &copy
	}
	if found == nil {
		return domain.ExecutionSnapshot{}, errAckGateInvalid
	}
	return *found, nil
}

type observedJobEvents struct {
	mu         sync.Mutex
	now        func() time.Time
	byRequest  map[int64]map[github.MessageType]time.Time
	messageIDs map[int]struct{}
	onStarted  func(int64, time.Time) error
}

func newObservedJobEvents() *observedJobEvents {
	return &observedJobEvents{
		now:        time.Now,
		byRequest:  make(map[int64]map[github.MessageType]time.Time),
		messageIDs: make(map[int]struct{}),
	}
}

func (events *observedJobEvents) observe(message *github.Message) error {
	if message == nil {
		return nil
	}
	if message.ID <= 0 || message.ScaleSetID <= 0 {
		return errEvidenceInvalid
	}
	if events.now == nil {
		return errEvidenceInvalid
	}
	observedAt := events.now()
	if observedAt.IsZero() {
		return errEvidenceInvalid
	}
	events.mu.Lock()
	defer events.mu.Unlock()
	events.messageIDs[message.ID] = struct{}{}
	for _, job := range message.Jobs {
		switch job.Type {
		case github.MessageTypeJobAvailable, github.MessageTypeJobAssigned:
			if job.RunnerRequestID <= 0 {
				return errEvidenceInvalid
			}
		case github.MessageTypeJobStarted, github.MessageTypeJobCompleted:
		default:
			return errEvidenceInvalid
		}
		if job.Type == github.MessageTypeJobCompleted && job.Result != "succeeded" {
			return errEvidenceInvalid
		}
		requestID := job.RunnerRequestID
		if requestID < 0 {
			return errEvidenceInvalid
		}
		if requestID == 0 {
			// The live gate is deliberately one-job-only. GitHub lifecycle
			// events can omit runnerRequestId, so correlate them to the sole
			// positive availability identity and fail closed if that identity
			// is absent or ambiguous.
			if len(events.byRequest) != 1 {
				return errEvidenceInvalid
			}
			for existingRequestID := range events.byRequest {
				requestID = existingRequestID
			}
		}
		if events.byRequest[requestID] == nil {
			events.byRequest[requestID] = make(map[github.MessageType]time.Time)
		}
		// Redelivery never moves the start of the warm-up window forward.
		if _, exists := events.byRequest[requestID][job.Type]; !exists {
			events.byRequest[requestID][job.Type] = observedAt
			if job.Type == github.MessageTypeJobStarted && events.onStarted != nil {
				if err := events.onStarted(requestID, observedAt); err != nil {
					return errEvidenceInvalid
				}
			}
		}
	}
	return nil
}

func (events *observedJobEvents) seedAvailable(
	claimKey int64,
	availableAt time.Time,
) error {
	if events == nil || claimKey <= 0 || availableAt.IsZero() {
		return errEvidenceInvalid
	}
	events.mu.Lock()
	defer events.mu.Unlock()
	if events.byRequest[claimKey] == nil {
		events.byRequest[claimKey] = make(map[github.MessageType]time.Time)
	}
	existing, found := events.byRequest[claimKey][github.MessageTypeJobAvailable]
	if !found || availableAt.Before(existing) {
		events.byRequest[claimKey][github.MessageTypeJobAvailable] = availableAt
	}
	return nil
}

type completedJobObservation struct {
	requestID                 int64
	events                    []string
	availableObservedAt       time.Time
	startedObservedAt         time.Time
	completedObservedAt       time.Time
	availableToStartedLatency time.Duration
}

func (events *observedJobEvents) completedSingleJob() (completedJobObservation, bool, error) {
	events.mu.Lock()
	defer events.mu.Unlock()
	// An isolated one-job gate must never explain away events from a second
	// runner request merely because that request lacks JobAvailable.
	if len(events.byRequest) > 1 {
		return completedJobObservation{}, false, errEvidenceInvalid
	}
	var completed []int64
	availableCount := 0
	for requestID, observed := range events.byRequest {
		availableAt, available := observed[github.MessageTypeJobAvailable]
		if !available {
			continue
		}
		availableCount++
		startedAt, started := observed[github.MessageTypeJobStarted]
		if !started {
			continue
		}
		if _, ok := observed[github.MessageTypeJobCompleted]; !ok {
			continue
		}
		latency := startedAt.Sub(availableAt)
		if latency < 0 || latency > time.Minute {
			return completedJobObservation{}, false, errEvidenceInvalid
		}
		completed = append(completed, requestID)
	}
	if availableCount > 1 {
		return completedJobObservation{}, false, errEvidenceInvalid
	}
	if len(completed) == 0 {
		return completedJobObservation{}, false, nil
	}
	if len(completed) != 1 {
		return completedJobObservation{}, false, errEvidenceInvalid
	}
	requestID := completed[0]
	observed := events.byRequest[requestID]
	names := make([]string, 0, len(observed))
	for event := range observed {
		names = append(names, string(event))
	}
	sort.Strings(names)
	availableAt := observed[github.MessageTypeJobAvailable]
	startedAt := observed[github.MessageTypeJobStarted]
	completedAt := observed[github.MessageTypeJobCompleted]
	return completedJobObservation{
		requestID:                 requestID,
		events:                    names,
		availableObservedAt:       availableAt,
		startedObservedAt:         startedAt,
		completedObservedAt:       completedAt,
		availableToStartedLatency: startedAt.Sub(availableAt),
	}, true, nil
}

func decodeReplayEvidence(reader io.Reader) (replayEvidence, error) {
	var value replayEvidence
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return replayEvidence{}, err
	}
	return value, nil
}
