//go:build !windows

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/store"
)

func TestAckGateWritesDurableAllowlistBeforeRefusingDelete(t *testing.T) {
	evidence, err := openEvidenceStore(filepath.Join(t.TempDir(), "evidence"))
	if err != nil {
		t.Fatal(err)
	}
	nodeID := domain.NodeID("0123456789abcdef0123456789abcdef")
	targetID := domain.TargetID("target")
	execution := domain.ExecutionSnapshot{
		ID:       "execution",
		TargetID: targetID,
		Slot:     domain.SlotKey{NodeID: nodeID},
		State:    domain.ExecutionReserved,
	}
	delegate := &fakeLiveSession{message: &github.Message{
		ScaleSetID: 41,
		ID:         71,
		Jobs: []github.JobMessage{{
			Type: github.MessageTypeJobAvailable, RunnerRequestID: 91,
		}},
	}}
	gate := &ackGateSession{
		delegate: delegate,
		store: fixedSnapshotReader{snapshot: store.ControllerSnapshot{
			ControllerEpoch: 2,
			Executions:      []domain.ExecutionSnapshot{execution},
			Reservations: []store.SlotReservation{{
				Slot: execution.Slot,
				Owner: domain.SlotOwner{
					TargetID: targetID, ExecutionID: execution.ID,
				},
			}},
		}},
		evidence:        evidence,
		targetID:        targetID,
		nodeID:          nodeID,
		scaleSetID:      41,
		controllerEpoch: 2,
	}
	availableAt := time.Now().UTC().Add(-3 * time.Second)
	gate.now = func() time.Time { return availableAt }
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := gate.Poll(ctx, 0, 1); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- gate.DeleteMessage(ctx, 71)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		replay, found, readErr := evidence.loadReplay()
		if readErr == nil && found {
			if replay.Phase != "committed_before_ack" ||
				replay.MessageID != 71 ||
				replay.ClaimKey != 91 ||
				replay.AvailableObservedAt != availableAt.Format(time.RFC3339Nano) ||
				replay.CommitControllerEpoch != 2 ||
				replay.CommitObservedAt != availableAt.Format(time.RFC3339Nano) ||
				replay.ExecutionID != string(execution.ID) {
				t.Fatalf("replay evidence = %#v", replay)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replay evidence not written: %v", readErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if delegate.deleteCount() != 0 {
		t.Fatal("DeleteMessage reached GitHub before process kill")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteMessage() error = %v, want context.Canceled", err)
	}
	payload, err := os.ReadFile(filepath.Join(evidence.directory, replayFileName))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"jit-secret-canary", "authorization", "privateKey", "joinCode"} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("replay evidence contains forbidden field/value %q", forbidden)
		}
	}
}

func TestAckGateRejectsMissingReservationWithoutDelegating(t *testing.T) {
	evidence, err := openEvidenceStore(filepath.Join(t.TempDir(), "evidence"))
	if err != nil {
		t.Fatal(err)
	}
	delegate := &fakeLiveSession{message: &github.Message{
		ScaleSetID: 41,
		ID:         71,
		Jobs: []github.JobMessage{{
			Type: github.MessageTypeJobAvailable, RunnerRequestID: 91,
		}},
	}}
	gate := &ackGateSession{
		delegate:        delegate,
		store:           fixedSnapshotReader{snapshot: store.ControllerSnapshot{ControllerEpoch: 2}},
		evidence:        evidence,
		targetID:        "target",
		nodeID:          "0123456789abcdef0123456789abcdef",
		scaleSetID:      41,
		controllerEpoch: 2,
	}
	if _, err := gate.Poll(context.Background(), 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := gate.DeleteMessage(context.Background(), 71); !errors.Is(err, errAckGateInvalid) {
		t.Fatalf("DeleteMessage() error = %v, want errAckGateInvalid", err)
	}
	if delegate.deleteCount() != 0 {
		t.Fatal("invalid gate delegated DeleteMessage")
	}
	if _, found, err := evidence.loadReplay(); err != nil || found {
		t.Fatalf("loadReplay() found=%v error=%v, want no evidence", found, err)
	}
}

func TestEvidenceStoreRejectsUnallowlistedFileAndUnsafeDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "evidence")
	evidence, err := openEvidenceStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := evidence.writeJSON("debug.json", map[string]string{"secret": "jit-secret-canary"}); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("writeJSON() error = %v, want errEvidenceInvalid", err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := openEvidenceStore(directory); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("openEvidenceStore(0755) error = %v, want errEvidenceInvalid", err)
	}
}

func TestRestartStartedMarkerIsIdempotentOnlyForSameRunnerRequest(t *testing.T) {
	evidence, err := openEvidenceStore(filepath.Join(t.TempDir(), "evidence"))
	if err != nil {
		t.Fatal(err)
	}
	first := time.Now().UTC()
	executionID := liveExecutionID(41, 91)
	if err := evidence.recordRestartStarted(41, 91, executionID, first); err != nil {
		t.Fatal(err)
	}
	if err := evidence.recordRestartStarted(41, 91, executionID, first.Add(time.Second)); err != nil {
		t.Fatalf("same request redelivery error = %v", err)
	}
	marker, err := loadEvidenceFile[restartStartedEvidence](
		evidence.directory,
		restartStartedName,
	)
	if err != nil || marker.ObservedAt != first.Format(time.RFC3339Nano) {
		t.Fatalf("marker = %#v, error=%v", marker, err)
	}
	if err := evidence.recordRestartStarted(41, 92, liveExecutionID(41, 92), first); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("different request error = %v, want errEvidenceInvalid", err)
	}
}

func TestScenarioStartAllowsOnlyExactUnfinishedCrashReplay(t *testing.T) {
	evidence, err := openEvidenceStore(filepath.Join(t.TempDir(), "evidence"))
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Now().UTC().Format(time.RFC3339Nano)
	replay := replayEvidence{
		Version:               evidenceVersion,
		Phase:                 "committed_before_ack",
		TargetID:              "target",
		NodeID:                "0123456789abcdef0123456789abcdef",
		ScaleSetID:            41,
		MessageID:             71,
		ClaimKey:              91,
		ExecutionID:           "execution",
		AvailableObservedAt:   observedAt,
		CommitControllerEpoch: 2,
		CommitObservedAt:      observedAt,
	}
	if err := evidence.writeReplay(replay); err != nil {
		t.Fatal(err)
	}
	if err := evidence.recordAckGateKill(time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := evidence.validateScenarioStart(modeNormal)
	if err != nil || !found || loaded.ExecutionID != replay.ExecutionID ||
		loaded.Phase != "killed_before_ack" ||
		loaded.CommitControllerEpoch != 2 ||
		loaded.KillExitStatus != 137 {
		t.Fatalf("normal replay = %#v, found=%v, error=%v", loaded, found, err)
	}
	if _, _, err := evidence.validateScenarioStart(modeCleanupFailure); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("cleanup replay error = %v, want errEvidenceInvalid", err)
	}
	if err := os.WriteFile(filepath.Join(evidence.directory, resultFileName), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := evidence.validateScenarioStart(modeNormal); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("existing result error = %v, want errEvidenceInvalid", err)
	}
}

func TestResultEvidenceAlwaysEmitsWarmLatencyForPassingZeroMillis(t *testing.T) {
	evidence, err := openEvidenceStore(filepath.Join(t.TempDir(), "evidence"))
	if err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	result := resultEvidence{
		Version:                  evidenceVersion,
		Mode:                     string(modeNormal),
		Status:                   "passed",
		TargetID:                 "target",
		PrivateRepository:        "genm/tewake-private",
		NodeID:                   "0123456789abcdef0123456789abcdef",
		ScaleSetID:               41,
		ControllerEpoch:          2,
		ExecutionID:              "execution",
		ExecutionState:           string(domain.ExecutionReleased),
		NodeState:                string(domain.NodeActive),
		ClaimKey:                 91,
		AvailableObservedAt:      "2026-07-26T00:00:00Z",
		JobStartedObservedAt:     "2026-07-26T00:00:00Z",
		JobCompletedObservedAt:   "2026-07-26T00:00:00Z",
		AvailableToStartedMillis: &zero,
		StartedAt:                "2026-07-26T00:00:00Z",
		FinishedAt:               "2026-07-26T00:00:01Z",
	}
	if err := evidence.writeResult(result); err != nil {
		t.Fatalf("writeResult() error = %v", err)
	}
	payload, err := os.ReadFile(filepath.Join(evidence.directory, resultFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"availableToStartedMillis": 0`) {
		t.Fatalf("result evidence omitted zero-millisecond warm gate: %s", payload)
	}
}

func TestObservedJobEventsEnforcesSixtySecondWarmGateWithMonotonicTimes(t *testing.T) {
	base := time.Now()
	times := []time.Time{base, base.Add(60 * time.Second)}
	events := newObservedJobEvents()
	events.now = func() time.Time {
		value := times[0]
		times = times[1:]
		return value
	}
	if err := events.observe(&github.Message{
		ScaleSetID: 41,
		ID:         71,
		Jobs: []github.JobMessage{{
			Type: github.MessageTypeJobAvailable, RunnerRequestID: 91,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := events.observe(&github.Message{
		ScaleSetID: 41,
		ID:         72,
		Jobs: []github.JobMessage{
			{Type: github.MessageTypeJobStarted, RunnerRequestID: 0},
			{Type: github.MessageTypeJobCompleted, RunnerRequestID: 0, Result: "succeeded"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	completed, found, err := events.completedSingleJob()
	if err != nil || !found || completed.availableToStartedLatency != time.Minute {
		t.Fatalf("completedSingleJob() = %#v, found=%v, error=%v", completed, found, err)
	}

	tooSlow := newObservedJobEvents()
	times = []time.Time{base, base.Add(60*time.Second + time.Millisecond)}
	tooSlow.now = events.now
	if err := tooSlow.observe(&github.Message{
		ScaleSetID: 41, ID: 73,
		Jobs: []github.JobMessage{{
			Type: github.MessageTypeJobAvailable, RunnerRequestID: 92,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tooSlow.observe(&github.Message{
		ScaleSetID: 41, ID: 74,
		Jobs: []github.JobMessage{
			{Type: github.MessageTypeJobStarted, RunnerRequestID: 92},
			{Type: github.MessageTypeJobCompleted, RunnerRequestID: 92, Result: "succeeded"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tooSlow.completedSingleJob(); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("61s completedSingleJob() error = %v, want errEvidenceInvalid", err)
	}

	uncorrelated := newObservedJobEvents()
	if err := uncorrelated.observe(&github.Message{
		ScaleSetID: 41,
		ID:         75,
		Jobs: []github.JobMessage{{
			Type:            github.MessageTypeJobStarted,
			RunnerRequestID: 0,
		}},
	}); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("uncorrelated zero-request lifecycle error = %v", err)
	}

	failedJob := newObservedJobEvents()
	if err := failedJob.observe(&github.Message{
		ScaleSetID: 41,
		ID:         76,
		Jobs: []github.JobMessage{{
			Type: github.MessageTypeJobCompleted, RunnerRequestID: 93, Result: "failed",
		}},
	}); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("failed completion observe error = %v, want errEvidenceInvalid", err)
	}
}

func TestObservedJobEventsRejectsAnySecondRunnerRequest(t *testing.T) {
	events := newObservedJobEvents()
	if err := events.observe(&github.Message{
		ScaleSetID: 41,
		ID:         71,
		Jobs: []github.JobMessage{
			{Type: github.MessageTypeJobAvailable, RunnerRequestID: 91},
			{Type: github.MessageTypeJobStarted, RunnerRequestID: 91},
			{Type: github.MessageTypeJobCompleted, RunnerRequestID: 91, Result: "succeeded"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := events.observe(&github.Message{
		ScaleSetID: 41,
		ID:         72,
		Jobs: []github.JobMessage{
			{Type: github.MessageTypeJobAssigned, RunnerRequestID: 92},
			{Type: github.MessageTypeJobStarted, RunnerRequestID: 92},
			{Type: github.MessageTypeJobCompleted, RunnerRequestID: 92, Result: "succeeded"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := events.completedSingleJob(); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("completedSingleJob() error = %v, want errEvidenceInvalid", err)
	}
}

type fakeLiveSession struct {
	mu      sync.Mutex
	deletes int
	message *github.Message
}

func (session *fakeLiveSession) Poll(context.Context, int, int) (*github.Message, error) {
	return session.message, nil
}

func (session *fakeLiveSession) DeleteMessage(context.Context, int) error {
	session.mu.Lock()
	session.deletes++
	session.mu.Unlock()
	return nil
}

func (session *fakeLiveSession) Snapshot() (github.SessionSnapshot, error) {
	return github.SessionSnapshot{
		ScaleSetID: 41,
		ID:         "session",
	}, nil
}

func (session *fakeLiveSession) AcquireJobs(context.Context, []int64) ([]int64, error) {
	return []int64{1}, nil
}

func (session *fakeLiveSession) Close(context.Context) error {
	return nil
}

func (session *fakeLiveSession) deleteCount() int {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.deletes
}
