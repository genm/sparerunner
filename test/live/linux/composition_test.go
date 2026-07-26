//go:build !windows

package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

func TestValidateControllerPreflightAcceptsFreshAndExactCrashReplay(t *testing.T) {
	nodeID := domain.NodeID("0123456789abcdef0123456789abcdef")
	targetID := domain.TargetID("target")
	fresh := store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes: []store.NodeAdministration{{
			NodeID: nodeID,
			State:  domain.NodeActive,
		}},
	}
	if err := validateControllerPreflight(fresh, nodeID, targetID, 41, replayEvidence{}, false); err != nil {
		t.Fatalf("fresh preflight error = %v", err)
	}

	execution := domain.ExecutionSnapshot{
		ID:       "execution",
		TargetID: targetID,
		Slot:     domain.SlotKey{NodeID: nodeID, Index: 0},
		State:    domain.ExecutionReserved,
	}
	replayed := fresh
	replayed.ControllerEpoch = 3
	replayed.Executions = []domain.ExecutionSnapshot{execution}
	replayed.Reservations = []store.SlotReservation{{
		Slot: execution.Slot,
		Owner: domain.SlotOwner{
			TargetID: targetID, ExecutionID: execution.ID,
		},
	}}
	observedAt := time.Now().UTC().Format(time.RFC3339Nano)
	proof := replayEvidence{
		Version:                   evidenceVersion,
		Phase:                     "killed_before_ack",
		TargetID:                  string(targetID),
		NodeID:                    string(nodeID),
		ScaleSetID:                41,
		MessageID:                 71,
		RunnerRequestID:           91,
		ExecutionID:               string(execution.ID),
		AvailableObservedAt:       observedAt,
		CommitControllerEpoch:     2,
		CommitObservedAt:          observedAt,
		KillExitStatus:            137,
		KilledBeforeAckObservedAt: observedAt,
	}
	if err := validateControllerPreflight(replayed, nodeID, targetID, 41, proof, true); err != nil {
		t.Fatalf("replay preflight error = %v", err)
	}
}

func TestValidateControllerPreflightRejectsUnsafeState(t *testing.T) {
	nodeID := domain.NodeID("0123456789abcdef0123456789abcdef")
	targetID := domain.TargetID("target")
	base := store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes: []store.NodeAdministration{{
			NodeID: nodeID,
			State:  domain.NodeActive,
		}},
	}
	testCases := []struct {
		name   string
		mutate func(*store.ControllerSnapshot)
	}{
		{name: "node draining", mutate: func(snapshot *store.ControllerSnapshot) {
			snapshot.Nodes[0].State = domain.NodeDraining
		}},
		{name: "existing execution without replay proof", mutate: func(snapshot *store.ControllerSnapshot) {
			snapshot.Executions = []domain.ExecutionSnapshot{{
				ID: "old", TargetID: targetID,
				Slot: domain.SlotKey{NodeID: nodeID}, State: domain.ExecutionReleased,
			}}
		}},
		{name: "existing reservation without replay proof", mutate: func(snapshot *store.ControllerSnapshot) {
			snapshot.Reservations = []store.SlotReservation{{
				Slot: domain.SlotKey{NodeID: nodeID},
				Owner: domain.SlotOwner{
					TargetID: targetID, ExecutionID: "old",
				},
			}}
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			snapshot := base
			snapshot.Nodes = append([]store.NodeAdministration(nil), base.Nodes...)
			testCase.mutate(&snapshot)
			if err := validateControllerPreflight(
				snapshot, nodeID, targetID, 41, replayEvidence{}, false,
			); !errors.Is(err, errControllerPreflight) {
				t.Fatalf("preflight error = %v, want errControllerPreflight", err)
			}
		})
	}
}

func TestValidateFreshLinuxAgentRequiresNativeReadyEmptyJournal(t *testing.T) {
	nodeID := domain.NodeID("0123456789abcdef0123456789abcdef")
	valid := transport.AgentSnapshot{
		NodeID:            nodeID,
		OS:                domain.OSLinux,
		Arch:              domain.ArchARM64,
		NativeRunnerReady: true,
	}
	if err := validateFreshLinuxAgent(valid, nodeID, 2); err != nil {
		t.Fatalf("validateFreshLinuxAgent() error = %v", err)
	}
	testCases := []struct {
		name   string
		mutate func(*transport.AgentSnapshot)
	}{
		{name: "wrong OS", mutate: func(snapshot *transport.AgentSnapshot) { snapshot.OS = domain.OSMacOS }},
		{name: "runtime unavailable", mutate: func(snapshot *transport.AgentSnapshot) { snapshot.NativeRunnerReady = false }},
		{name: "future epoch", mutate: func(snapshot *transport.AgentSnapshot) { snapshot.MaxControllerEpoch = 3 }},
		{name: "existing journal", mutate: func(snapshot *transport.AgentSnapshot) {
			snapshot.Commands = []domain.Command{{}}
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			snapshot := valid
			testCase.mutate(&snapshot)
			if err := validateFreshLinuxAgent(snapshot, nodeID, 2); !errors.Is(err, errAgentPreflight) {
				t.Fatalf("validation error = %v, want errAgentPreflight", err)
			}
		})
	}
}

func TestRunPollFirstObservesCommittedMessageBeforeSurfacingDriveFailure(t *testing.T) {
	driverFailure := errors.New("drive failed")
	driver := &fakePollDriver{
		message: &github.Message{
			ScaleSetID: 41,
			ID:         71,
			Jobs: []github.JobMessage{{
				Type:            github.MessageTypeJobAvailable,
				RunnerRequestID: 91,
			}},
		},
		driveErr: driverFailure,
	}
	events := newObservedJobEvents()
	err := runPollFirst(context.Background(), driver, events, &replayTracker{})
	if !errors.Is(err, errCoordinatorRun) {
		t.Fatalf("runPollFirst() error = %v, want errCoordinatorRun", err)
	}
	if driver.calls != 1 {
		t.Fatalf("PollAndDriveOnce calls = %d, want 1", driver.calls)
	}
	events.mu.Lock()
	_, observed := events.byRequest[91][github.MessageTypeJobAvailable]
	events.mu.Unlock()
	if !observed {
		t.Fatal("JobAvailable was not observed before failure")
	}
}

func TestWaitForAcceptanceOutcomeRequiresGitHubCompletionAndReleasedSlot(t *testing.T) {
	nodeID := domain.NodeID("0123456789abcdef0123456789abcdef")
	targetID := domain.TargetID("target")
	execution := domain.ExecutionSnapshot{
		ID:       "execution",
		TargetID: targetID,
		Slot:     domain.SlotKey{NodeID: nodeID},
		State:    domain.ExecutionReleased,
	}
	reader := fixedSnapshotReader{snapshot: store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes: []store.NodeAdministration{{
			NodeID: nodeID, State: domain.NodeActive,
		}},
		Executions: []domain.ExecutionSnapshot{execution},
	}}
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
	outcome, err := waitForAcceptanceOutcome(
		context.Background(), reader, modeNormal, targetID, nodeID, events,
	)
	if err != nil {
		t.Fatalf("waitForAcceptanceOutcome() error = %v", err)
	}
	if outcome.execution.ID != execution.ID ||
		outcome.nodeState != domain.NodeActive ||
		outcome.reservationCount != 0 ||
		outcome.job.requestID != 91 ||
		len(outcome.job.events) != 3 ||
		outcome.job.availableToStartedLatency > time.Minute {
		t.Fatalf("outcome = %#v, want execution/91/three events within 60s", outcome)
	}
}

func TestReplayTrackerRequiresSameMessageAndExecution(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "evidence")
	evidence, err := openEvidenceStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := domain.NodeID("0123456789abcdef0123456789abcdef")
	targetID := domain.TargetID("target")
	availableAt := time.Now().UTC().Add(-5 * time.Second)
	expected := replayEvidence{
		Version:                   evidenceVersion,
		Phase:                     "killed_before_ack",
		TargetID:                  string(targetID),
		NodeID:                    string(nodeID),
		ScaleSetID:                41,
		MessageID:                 71,
		RunnerRequestID:           91,
		ExecutionID:               "execution",
		AvailableObservedAt:       availableAt.Format(time.RFC3339Nano),
		CommitControllerEpoch:     2,
		CommitObservedAt:          availableAt.Format(time.RFC3339Nano),
		KillExitStatus:            137,
		KilledBeforeAckObservedAt: availableAt.Add(time.Second).Format(time.RFC3339Nano),
	}
	tracker := &replayTracker{
		expected: &expected,
		store: fixedSnapshotReader{snapshot: store.ControllerSnapshot{
			ControllerEpoch: 3,
			Executions: []domain.ExecutionSnapshot{{
				ID: "execution", TargetID: targetID,
				Slot: domain.SlotKey{NodeID: nodeID}, State: domain.ExecutionRunning,
			}},
		}},
		evidence: evidence,
		targetID: targetID,
		nodeID:   nodeID,
		scaleSet: 41,
		epoch:    3,
	}
	events := newObservedJobEvents()
	if err := tracker.observe(context.Background(), &github.Message{
		ScaleSetID: 41, ID: 71,
		Jobs: []github.JobMessage{{
			Type: github.MessageTypeJobAvailable, RunnerRequestID: 91,
		}},
	}, events); err != nil {
		t.Fatalf("observe() error = %v", err)
	}
	if !tracker.observed {
		t.Fatal("replay was not marked observed")
	}
	replay, found, err := evidence.loadReplay()
	if err != nil || !found || replay.Phase != "redelivered_same_execution" ||
		replay.ExecutionID != expected.ExecutionID ||
		replay.RunnerRequestID != expected.RunnerRequestID ||
		replay.AvailableObservedAt != expected.AvailableObservedAt ||
		replay.CommitControllerEpoch != expected.CommitControllerEpoch ||
		replay.CommitObservedAt != expected.CommitObservedAt ||
		replay.KillExitStatus != 137 ||
		replay.KilledBeforeAckObservedAt != expected.KilledBeforeAckObservedAt ||
		replay.RedeliveryControllerEpoch != 3 ||
		replay.RedeliveryObservedAt == "" {
		t.Fatalf("replay = %#v, found=%v, err=%v", replay, found, err)
	}
}

func TestReplayTrackerPreservesWarmGateAcrossProcessBoundary(t *testing.T) {
	base := time.Now().UTC().Add(-time.Minute)
	for _, testCase := range []struct {
		name      string
		startedAt time.Time
		wantError bool
	}{
		{name: "exactly sixty seconds", startedAt: base.Add(time.Minute)},
		{name: "over sixty seconds", startedAt: base.Add(time.Minute + time.Millisecond), wantError: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			evidence, err := openEvidenceStore(filepath.Join(t.TempDir(), "evidence"))
			if err != nil {
				t.Fatal(err)
			}
			nodeID := domain.NodeID("0123456789abcdef0123456789abcdef")
			targetID := domain.TargetID("target")
			expected := replayEvidence{
				Version:                   evidenceVersion,
				Phase:                     "killed_before_ack",
				TargetID:                  string(targetID),
				NodeID:                    string(nodeID),
				ScaleSetID:                41,
				MessageID:                 71,
				RunnerRequestID:           91,
				ExecutionID:               "execution",
				AvailableObservedAt:       base.Format(time.RFC3339Nano),
				CommitControllerEpoch:     2,
				CommitObservedAt:          base.Format(time.RFC3339Nano),
				KillExitStatus:            137,
				KilledBeforeAckObservedAt: base.Add(time.Millisecond).Format(time.RFC3339Nano),
			}
			tracker := &replayTracker{
				expected: &expected,
				store: fixedSnapshotReader{snapshot: store.ControllerSnapshot{
					ControllerEpoch: 3,
					Executions: []domain.ExecutionSnapshot{{
						ID: "execution", TargetID: targetID,
						Slot: domain.SlotKey{NodeID: nodeID}, State: domain.ExecutionRunning,
					}},
				}},
				evidence: evidence,
				targetID: targetID,
				nodeID:   nodeID,
				scaleSet: 41,
				epoch:    3,
			}
			events := newObservedJobEvents()
			times := []time.Time{base.Add(30 * time.Second), testCase.startedAt}
			events.now = func() time.Time {
				value := times[0]
				times = times[1:]
				return value
			}
			redelivered := &github.Message{
				ScaleSetID: 41,
				ID:         71,
				Jobs: []github.JobMessage{{
					Type: github.MessageTypeJobAvailable, RunnerRequestID: 91,
				}},
			}
			if err := events.observe(redelivered); err != nil {
				t.Fatal(err)
			}
			if err := tracker.observe(context.Background(), redelivered, events); err != nil {
				t.Fatal(err)
			}
			if err := events.observe(&github.Message{
				ScaleSetID: 41,
				ID:         72,
				Jobs: []github.JobMessage{
					{Type: github.MessageTypeJobStarted, RunnerRequestID: 91},
					{Type: github.MessageTypeJobCompleted, RunnerRequestID: 91, Result: "succeeded"},
				},
			}); err != nil {
				t.Fatal(err)
			}
			completed, found, err := events.completedSingleJob()
			if testCase.wantError {
				if !errors.Is(err, errEvidenceInvalid) {
					t.Fatalf("completedSingleJob() error = %v, want errEvidenceInvalid", err)
				}
				return
			}
			if err != nil || !found || completed.availableToStartedLatency != time.Minute {
				t.Fatalf("completedSingleJob() = %#v, found=%v, error=%v", completed, found, err)
			}
		})
	}
}

type fakePollDriver struct {
	message  *github.Message
	pollErr  error
	driveErr error
	calls    int
	drives   int
}

func (driver *fakePollDriver) PollOnce(context.Context) (*github.Message, error) {
	driver.calls++
	return driver.message, driver.pollErr
}

func (driver *fakePollDriver) DriveNext(context.Context) (bool, error) {
	driver.drives++
	return true, driver.driveErr
}

type fixedSnapshotReader struct {
	snapshot store.ControllerSnapshot
	err      error
}

func (reader fixedSnapshotReader) Snapshot(context.Context) (store.ControllerSnapshot, error) {
	return reader.snapshot, reader.err
}
