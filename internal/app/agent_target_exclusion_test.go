package app

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/nodectl"
	"github.com/genm/sparerunner/internal/runner"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

const exclusionTestTargetID = domain.TargetID("target-1")

func exclusionTestTarget() transport.CommandTarget {
	return transport.CommandTarget{
		TargetID:  exclusionTestTargetID,
		Scope:     "owner/repo",
		ScopeKind: domain.TargetRepository,
	}
}

// startExclusionRuntime builds a runtime whose local runner is already prepared,
// which is the exact state a start command arrives in.
func startExclusionRuntime(
	t *testing.T,
	agentStore *store.AgentStore,
	manager *fakeRunnerLifecycle,
) *AgentCommandRuntime {
	t.Helper()
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := commandRuntime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return commandRuntime
}

func TestAgentStartForExcludedTargetFailsDurablyWithoutStartingAProcess(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	ctx := context.Background()
	// The owner excluded this Target before the controller dispatched the start.
	if err := agentStore.AddExclusion(ctx, exclusionTestTargetID, "cli"); err != nil {
		t.Fatal(err)
	}
	destroyed := 0
	manager := &fakeRunnerLifecycle{
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			t.Fatal("excluded target crossed the exec boundary")
			return runner.Snapshot{}, nil
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StatePrepared, Prepared: true}, nil
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			destroyed++
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	commandRuntime := startExclusionRuntime(t, agentStore, manager)

	metadata := transport.CommandMetadata{
		CommandID:       "excluded-start-command",
		ControllerEpoch: 1,
		ExecutionID:     "excluded-start-execution",
		ExpectedState:   domain.ExecutionPreparing,
		Target:          exclusionTestTarget(),
	}
	payload, err := transport.EncodeStartCommandPayload(
		metadata, runner.OfficialRunnerVersion, false, "excluded-start-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{
		ProtocolVersion: 1,
		MessageID:       string(metadata.CommandID),
		Type:            transport.MessageStart,
		Payload:         payload,
	}
	accepted, err := commandRuntime.Accept(ctx, &envelope)
	if err != nil {
		// The refusal is a classified execution failure, never a transport
		// rejection: rejecting would make the controller redeliver forever.
		t.Fatalf("excluded start was rejected at the transport boundary: %v", err)
	}
	update, execErr := accepted.Execute(ctx)
	if execErr == nil {
		t.Fatal("excluded start reported success")
	}
	if update.State != domain.ExecutionFailed ||
		update.ErrorCode != transport.ExecutionErrorTargetExcluded {
		t.Fatalf("excluded start update = %+v", update)
	}
	if manager.starts != 0 {
		t.Fatalf("runner start attempts = %d", manager.starts)
	}
	if destroyed != 1 {
		t.Fatalf("prepared root cleanups = %d, want the prepared workspace destroyed", destroyed)
	}

	// The durable outbox carries the refusal, so the controller releases the
	// slot and GitHub may reassign the job.
	pending, err := commandRuntime.PendingUpdates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range pending {
		if item.Update.ExecutionID == metadata.ExecutionID &&
			item.Update.ErrorCode == domain.ExecutionErrorTargetExcluded {
			found = true
		}
	}
	if !found {
		t.Fatalf("durable outbox missing the target_excluded failure: %+v", pending)
	}
	// The per-execution admission lock is released, so the slot is not stuck.
	release := commandRuntime.lockExecution(metadata.ExecutionID)
	release()
}

func TestAgentExclusionAfterPrepareStillRefusesTheStart(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	ctx := context.Background()
	prepared := false
	manager := &fakeRunnerLifecycle{
		prepare: func(_ context.Context, request runner.Preparation) (runner.Snapshot, error) {
			prepared = true
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StatePrepared, Prepared: true}, nil
		},
		start: func(context.Context, runner.Start) (runner.Snapshot, error) {
			t.Fatal("target excluded between prepare and start crossed the exec boundary")
			return runner.Snapshot{}, nil
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			if !prepared {
				return runner.Snapshot{}, runner.ErrExecutionNotFound
			}
			return runner.Snapshot{ExecutionID: executionID, State: runner.StatePrepared, Prepared: true}, nil
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	commandRuntime := startExclusionRuntime(t, agentStore, manager)

	prepareMetadata := transport.CommandMetadata{
		CommandID:       "late-exclusion-prepare",
		ControllerEpoch: 1,
		ExecutionID:     "late-exclusion-execution",
		ExpectedState:   domain.ExecutionReserved,
		Target:          exclusionTestTarget(),
	}
	preparePayload, err := transport.EncodePrepareCommandPayload(
		prepareMetadata, runner.OfficialRunnerVersion, false)
	if err != nil {
		t.Fatal(err)
	}
	prepareEnvelope := transport.Envelope{
		ProtocolVersion: 1,
		MessageID:       string(prepareMetadata.CommandID),
		Type:            transport.MessagePrepare,
		Payload:         preparePayload,
	}
	acceptedPrepare, err := commandRuntime.Accept(ctx, &prepareEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	prepareUpdate, err := acceptedPrepare.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if prepareUpdate.State != domain.ExecutionPreparing {
		t.Fatalf("prepare update = %+v", prepareUpdate)
	}

	// The owner excludes the Target in the window between prepare and start.
	// The start-path check is authoritative precisely because of this window.
	if err := agentStore.AddExclusion(ctx, exclusionTestTargetID, "tray"); err != nil {
		t.Fatal(err)
	}

	startMetadata := transport.CommandMetadata{
		CommandID:       "late-exclusion-start",
		ControllerEpoch: 1,
		ExecutionID:     "late-exclusion-execution",
		ExpectedState:   domain.ExecutionPreparing,
		Target:          exclusionTestTarget(),
	}
	startPayload, err := transport.EncodeStartCommandPayload(
		startMetadata, runner.OfficialRunnerVersion, false, "late-exclusion-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	startEnvelope := transport.Envelope{
		ProtocolVersion: 1,
		MessageID:       string(startMetadata.CommandID),
		Type:            transport.MessageStart,
		Payload:         startPayload,
	}
	acceptedStart, err := commandRuntime.Accept(ctx, &startEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	update, execErr := acceptedStart.Execute(ctx)
	if execErr == nil {
		t.Fatal("start after a late exclusion reported success")
	}
	if update.State != domain.ExecutionFailed ||
		update.ErrorCode != transport.ExecutionErrorTargetExcluded {
		t.Fatalf("late exclusion update = %+v", update)
	}
	if manager.starts != 0 {
		t.Fatalf("runner start attempts = %d", manager.starts)
	}
}

func TestAgentPrepareForExcludedTargetIsRefusedBeforeMaterialization(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	ctx := context.Background()
	if err := agentStore.AddExclusion(ctx, exclusionTestTargetID, "raycast"); err != nil {
		t.Fatal(err)
	}
	manager := &fakeRunnerLifecycle{
		prepare: func(context.Context, runner.Preparation) (runner.Snapshot, error) {
			t.Fatal("excluded target was materialized")
			return runner.Snapshot{}, nil
		},
		inspect: func(context.Context, string) (runner.Snapshot, error) {
			return runner.Snapshot{}, runner.ErrExecutionNotFound
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	commandRuntime := startExclusionRuntime(t, agentStore, manager)

	metadata := transport.CommandMetadata{
		CommandID:       "excluded-prepare-command",
		ControllerEpoch: 1,
		ExecutionID:     "excluded-prepare-execution",
		ExpectedState:   domain.ExecutionReserved,
		Target:          exclusionTestTarget(),
	}
	payload, err := transport.EncodePrepareCommandPayload(metadata, runner.OfficialRunnerVersion, false)
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{
		ProtocolVersion: 1,
		MessageID:       string(metadata.CommandID),
		Type:            transport.MessagePrepare,
		Payload:         payload,
	}
	accepted, err := commandRuntime.Accept(ctx, &envelope)
	if err != nil {
		t.Fatal(err)
	}
	update, execErr := accepted.Execute(ctx)
	if execErr == nil {
		t.Fatal("excluded prepare reported success")
	}
	if update.State != domain.ExecutionFailed ||
		update.ErrorCode != transport.ExecutionErrorTargetExcluded {
		t.Fatalf("excluded prepare update = %+v", update)
	}
	if manager.prepares != 0 {
		t.Fatalf("runner prepare attempts = %d", manager.prepares)
	}
}

func TestAgentStartForServedTargetIsUnaffectedByOtherExclusions(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	ctx := context.Background()
	// An exclusion only subtracts the Target it names.
	if err := agentStore.AddExclusion(ctx, "some-other-target", "cli"); err != nil {
		t.Fatal(err)
	}
	manager := &fakeRunnerLifecycle{
		start: func(_ context.Context, request runner.Start) (runner.Snapshot, error) {
			if err := request.JIT.Deliver(func(string) error { return nil }); err != nil {
				return runner.Snapshot{}, err
			}
			return runner.Snapshot{ExecutionID: request.ExecutionID, State: runner.StateRunning, Running: true}, nil
		},
		inspect: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StatePrepared, Prepared: true}, nil
		},
		wait: func(ctx context.Context, executionID string) (runner.Snapshot, error) {
			<-ctx.Done()
			return runner.Snapshot{}, ctx.Err()
		},
		destroy: func(_ context.Context, executionID string) (runner.Snapshot, error) {
			return runner.Snapshot{ExecutionID: executionID, State: runner.StateReleased}, nil
		},
	}
	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()
	commandRuntime, err := NewAgentCommandRuntime("node-1", agentStore, manager, currentRunnerPackage(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := commandRuntime.Start(lifetime); err != nil {
		t.Fatal(err)
	}

	metadata := transport.CommandMetadata{
		CommandID:       "served-start-command",
		ControllerEpoch: 1,
		ExecutionID:     "served-start-execution",
		ExpectedState:   domain.ExecutionPreparing,
		Target:          exclusionTestTarget(),
	}
	payload, err := transport.EncodeStartCommandPayload(
		metadata, runner.OfficialRunnerVersion, false, "served-start-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	envelope := transport.Envelope{
		ProtocolVersion: 1,
		MessageID:       string(metadata.CommandID),
		Type:            transport.MessageStart,
		Payload:         payload,
	}
	accepted, err := commandRuntime.Accept(lifetime, &envelope)
	if err != nil {
		t.Fatal(err)
	}
	update, err := accepted.Execute(lifetime)
	if err != nil {
		t.Fatal(err)
	}
	if update.State != domain.ExecutionRunning || update.ErrorCode != transport.ExecutionErrorNone {
		t.Fatalf("served start update = %+v", update)
	}

	// The execution's target attribution is durable, so a desktop surface can
	// name the scope this computer is working on.
	targets, err := agentStore.ExecutionTargets(lifetime)
	if err != nil {
		t.Fatal(err)
	}
	attribution, found := targets[metadata.ExecutionID]
	if !found || attribution.TargetID != exclusionTestTargetID ||
		attribution.Scope != "owner/repo" || attribution.ScopeKind != domain.TargetRepository {
		t.Fatalf("execution attribution = %+v (found=%t)", attribution, found)
	}
}

func TestAgentSnapshotAndHeartbeatCarryTheExclusionSet(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	ctx := context.Background()
	for _, targetID := range []domain.TargetID{"target-z", "target-a"} {
		if err := agentStore.AddExclusion(ctx, targetID, "cli"); err != nil {
			t.Fatal(err)
		}
	}
	availability, err := newAgentAvailability(ctx, agentStore, "node-1", false)
	if err != nil {
		t.Fatal(err)
	}
	excluded := availability.ExcludedTargets()
	if len(excluded) != 2 || excluded[0] != "target-a" || excluded[1] != "target-z" {
		t.Fatalf("reported exclusions = %v, want a sorted set", excluded)
	}

	state := &AgentState{Store: agentStore, NodeID: "node-1"}
	snapshot, err := buildAgentSnapshot(
		ctx, state, runner.OfficialRunnerVersion, true, domain.AvailabilityAccepting,
		&excluded, nil)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ExcludedTargets == nil || len(*snapshot.ExcludedTargets) != 2 ||
		(*snapshot.ExcludedTargets)[0] != "target-a" || (*snapshot.ExcludedTargets)[1] != "target-z" {
		t.Fatalf("snapshot exclusions = %v", snapshot.ExcludedTargets)
	}
	if _, err := transport.EncodeAgentHeartbeat(transport.AgentHeartbeat{
		NodeID:             "node-1",
		NativeRunnerReady:  true,
		AvailabilityIntent: domain.AvailabilityAccepting,
		ExcludedTargets:    &excluded,
	}); err != nil {
		t.Fatalf("heartbeat with exclusions: %v", err)
	}

	// A mutation refreshes the reported set without another agent restart.
	if _, err := availability.SetTargetExclusion(ctx, "target-a", false, nodectl.SourceTray); err != nil {
		t.Fatal(err)
	}
	excluded = availability.ExcludedTargets()
	if len(excluded) != 1 || excluded[0] != "target-z" {
		t.Fatalf("exclusions after include = %v", excluded)
	}
}

func TestAgentStatusJoinsLocalExclusionsAgainstTheControllerEcho(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	ctx := context.Background()
	availability, err := newAgentAvailability(ctx, agentStore, "node-1", false)
	if err != nil {
		t.Fatal(err)
	}
	availability.setEligibleTargets([]transport.EligibleTarget{
		{TargetID: "target-eligible", ScopeKind: domain.TargetRepository, Scope: "owner/repo", ScaleSetName: "s1"},
		{TargetID: "target-adopted", ScopeKind: domain.TargetOrganization, Scope: "owner", ScaleSetName: "s2", Excluded: true},
	}, true)

	// Excluding is subtractive, so it is locally effective at once even though
	// the controller has not adopted it yet: excluded, and still syncing.
	status, err := availability.SetTargetExclusion(ctx, "target-eligible", true, nodectl.SourceCLI)
	if err != nil {
		t.Fatal(err)
	}
	targets := status.Targets()
	if len(targets) != 2 {
		t.Fatalf("status targets = %+v", targets)
	}
	fresh := targets[0]
	if !fresh.LocallyExcluded || !fresh.Pending || fresh.Excluded {
		t.Fatalf("locally excluded target = %+v", fresh)
	}
	// The controller still adopts this one as excluded while the owner no
	// longer does, so re-inclusion is additive and stays pending.
	adopted := targets[1]
	if adopted.LocallyExcluded || !adopted.Pending || !adopted.Excluded {
		t.Fatalf("include-pending target = %+v", adopted)
	}
	if len(status.UnknownExclusions) != 0 {
		t.Fatalf("unexpected unknown exclusions: %v", status.UnknownExclusions)
	}

	// Excluding a Target the controller never listed is a safe no-op rendered
	// as not-currently-eligible, never an error.
	status, err = availability.SetTargetExclusion(ctx, "target-never-seen", true, nodectl.SourceRaycast)
	if err != nil {
		t.Fatalf("unknown target exclusion failed: %v", err)
	}
	if len(status.UnknownExclusions) != 1 || status.UnknownExclusions[0] != "target-never-seen" {
		t.Fatalf("unknown exclusions = %v", status.UnknownExclusions)
	}
}

func TestAgentTargetExclusionCapIsRejectedAsAnInvalidRequest(t *testing.T) {
	agentStore := openAgentCommandStore(t)
	defer agentStore.Close()
	ctx := context.Background()
	availability, err := newAgentAvailability(ctx, agentStore, "node-1", false)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < store.MaxTargetExclusions; index++ {
		targetID := domain.TargetID(fmt.Sprintf("target-%04d", index))
		if _, err := availability.SetTargetExclusion(ctx, targetID, true, nodectl.SourceCLI); err != nil {
			t.Fatal(err)
		}
	}
	_, err = availability.SetTargetExclusion(ctx, "target-overflow", true, nodectl.SourceCLI)
	if err == nil {
		t.Fatal("a full exclusion set accepted another entry")
	}
	// A full set is an actionable rejected request, not a degraded agent: the
	// local control endpoint reports it as invalid_request.
	if !errors.Is(err, nodectl.ErrInvalidRequest) {
		t.Fatalf("cap error = %v, want an invalid-request classification", err)
	}
}
