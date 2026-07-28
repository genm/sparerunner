package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/nodectl"
	"github.com/genm/tewake/internal/store"
)

func TestAvailabilityStopAppliesWhileDisconnectedAndResumeStaysPending(t *testing.T) {
	availability := newTestAvailability(t)

	// Stopping subtracts capacity, so it is effective locally with no
	// controller session at all.
	stopped, err := availability.SetIntent(
		context.Background(), domain.AvailabilityStopped, nodectl.SourceTray,
	)
	if err != nil {
		t.Fatalf("stop acceptance: %v", err)
	}
	if stopped.Intent != domain.AvailabilityStopped || stopped.PendingResume ||
		stopped.EffectiveAccepting() {
		t.Fatalf("stop was not effective offline: %+v", stopped)
	}
	if availability.Accepts() {
		t.Fatal("stopped node still reports local acceptance")
	}

	// Resuming adds capacity, so it must stay pending until the controller has
	// acknowledged it.
	resumed, err := availability.SetIntent(
		context.Background(), domain.AvailabilityAccepting, nodectl.SourceRaycast,
	)
	if err != nil {
		t.Fatalf("resume acceptance: %v", err)
	}
	if !resumed.PendingResume || resumed.EffectiveAccepting() {
		t.Fatalf("unconfirmed resume rendered as accepting: %+v", resumed)
	}

	availability.setConnected(true)
	pending, err := availability.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !pending.PendingResume || pending.EffectiveAccepting() {
		t.Fatalf("connection alone confirmed the resume: %+v", pending)
	}

	availability.confirm(domain.AvailabilityAccepting)
	confirmed, err := availability.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if confirmed.PendingResume || !confirmed.EffectiveAccepting() {
		t.Fatalf("confirmed resume did not become effective: %+v", confirmed)
	}

	// Losing the session withdraws the confirmation rather than retaining a
	// stale acceptance.
	availability.setConnected(false)
	offline, err := availability.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !offline.PendingResume || offline.EffectiveAccepting() {
		t.Fatalf("disconnected node still reports acceptance: %+v", offline)
	}
}

func TestAvailabilityStatusReportsRunningExecutionsFromTheJournal(t *testing.T) {
	availability := newTestAvailability(t)
	ctx := context.Background()
	if err := availability.store.RecordObservation(ctx, store.Observation{
		ExecutionID: "execution-running",
		State:       domain.ExecutionRunning,
	}); err != nil {
		t.Fatalf("record running observation: %v", err)
	}
	if err := availability.store.RecordObservation(ctx, store.Observation{
		ExecutionID: "execution-done",
		State:       domain.ExecutionReleased,
	}); err != nil {
		t.Fatalf("record released observation: %v", err)
	}
	status, err := availability.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(status.RunningExecutions) != 1 ||
		status.RunningExecutions[0].ExecutionID != "execution-running" {
		t.Fatalf("unexpected running executions: %+v", status.RunningExecutions)
	}
}

// TestAvailabilityStatusReportsSharedRunnerIdentityWithoutGatingCapacity pins
// both halves of the contract: the isolation mode reaches every node-local
// observer unchanged from the serve flag, and it changes no admission decision.
func TestAvailabilityStatusReportsSharedRunnerIdentityWithoutGatingCapacity(t *testing.T) {
	ctx := context.Background()
	for _, shared := range []bool{false, true} {
		availability := newTestAvailabilityWithSharedRunnerIdentity(t, shared)
		availability.setNativeReady(true)
		availability.setConnected(true)
		availability.confirm(availability.Intent())
		status, err := availability.Status(ctx)
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		if status.SharedRunnerIdentity != shared {
			t.Fatalf("status.SharedRunnerIdentity = %t, want %t", status.SharedRunnerIdentity, shared)
		}
		// Observation only: the weaker mode must neither grant nor withhold
		// capacity relative to the isolated one.
		if !status.NativeRunnerReady || !status.EffectiveAccepting() {
			t.Fatalf(
				"sharedRunnerIdentity=%t changed admission: ready=%t accepting=%t",
				shared, status.NativeRunnerReady, status.EffectiveAccepting(),
			)
		}
		if !availability.Accepts() {
			t.Fatalf("sharedRunnerIdentity=%t changed the local admission gate", shared)
		}
	}
}

func newTestAvailability(t *testing.T) *agentAvailability {
	t.Helper()
	return newTestAvailabilityWithSharedRunnerIdentity(t, false)
}

func newTestAvailabilityWithSharedRunnerIdentity(
	t *testing.T,
	sharedRunnerIdentity bool,
) *agentAvailability {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create private directory: %v", err)
	}
	agentStore, err := store.OpenAgent(
		context.Background(), filepath.Join(directory, "agent.db"), store.Options{},
	)
	if err != nil {
		t.Fatalf("open agent store: %v", err)
	}
	t.Cleanup(func() { _ = agentStore.Close() })
	availability, err := newAgentAvailability(
		context.Background(), agentStore, "node-1", sharedRunnerIdentity)
	if err != nil {
		t.Fatalf("open availability: %v", err)
	}
	return availability
}
