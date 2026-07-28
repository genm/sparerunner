package fault_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/app"
	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/reconcile"
	"github.com/genm/sparerunner/internal/runner"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

const (
	runtimeFaultScaleSetID      = github.ScaleSetID(707)
	runtimeFaultRunnerRequestID = int64(70701)
	runtimeFaultTargetID        = domain.TargetID("target-runtime-fault")
	runtimeFaultProfileID       = domain.RunnerProfileID("profile-runtime-fault")
	runtimeFaultAcceptedNodeID  = domain.NodeID("00000000000000000000000000000031")
	runtimeFaultAcceptedExecID  = domain.ExecutionID("runtime-fault-accepted-start")
	runtimeFaultPrepareID       = domain.CommandID("runtime-fault-prepare-command")
	runtimeFaultStartID         = domain.CommandID("runtime-fault-start-command")
	runtimeFaultAcceptedJIT     = "opaque-accepted-before-execute-jit"
)

func TestRuntimeFaultControllerCommitHelper(t *testing.T) {
	path := os.Getenv("SPARERUNNER_RUNTIME_FAULT_CONTROLLER_DB")
	if path == "" {
		return
	}
	nodeID := domain.NodeID(os.Getenv("SPARERUNNER_RUNTIME_FAULT_NODE_ID"))
	controller := openController(t, path)
	restart, err := controller.RestartSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	projection, err := reconcile.RestoreRestart(restart, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := runtimeFaultEmptyAgentSnapshot(nodeID)
	if _, err := projection.ReconcileAgentSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	agent := &runtimeFaultControllerAgent{
		nodeID:     nodeID,
		controller: controller,
		projection: projection,
		snapshot:   snapshot,
		online:     true,
	}
	session := &runtimeFaultPreAckCrashSession{message: runtimeFaultMessage()}
	coordinator, err := app.NewControllerRunnerCoordinator(
		controller,
		session,
		agent,
		&runtimeFaultGitHubLifecycle{},
		app.ControllerRunnerConfig{
			ScaleSetID:      runtimeFaultScaleSetID,
			TargetID:        runtimeFaultTargetID,
			Scope:           "owner/repo",
			ScopeKind:       domain.TargetRepository,
			RunnerProfileID: runtimeFaultProfileID,
			VersionPolicy:   domain.RunnerVersionAutoUpdate,
			NodeID:          nodeID,
			ControllerEpoch: restart.Controller.ControllerEpoch,
			Reconciler:      projection,
		},
		slog.New(slog.DiscardHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Fatal("fault helper returned from the pre-ack boundary")
}

func TestRuntimeFaultAgentAcceptedStartHelper(t *testing.T) {
	path := os.Getenv("SPARERUNNER_RUNTIME_FAULT_AGENT_DB")
	if path == "" {
		return
	}
	runtimeRoot := os.Getenv("SPARERUNNER_RUNTIME_FAULT_RUNTIME_ROOT")
	agentStore := openAgent(t, path)
	supervisor := newRuntimeFaultSupervisor()
	manager := runtimeFaultNewManager(t, agentStore, runtimeRoot, supervisor)
	commandRuntime, cancel := runtimeFaultNewCommandRuntime(
		t,
		runtimeFaultAcceptedNodeID,
		agentStore,
		manager,
	)
	defer cancel()

	prepareMetadata := transport.CommandMetadata{
		CommandID:       runtimeFaultPrepareID,
		ControllerEpoch: 1,
		ExecutionID:     runtimeFaultAcceptedExecID,
		ExpectedState:   domain.ExecutionReserved,
		Target:          transport.CommandTarget{TargetID: "target-1", Scope: "owner/repo", ScopeKind: domain.TargetRepository},
	}
	preparePayload, err := transport.EncodePrepareCommandPayload(
		prepareMetadata,
		runner.OfficialRunnerVersion,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	prepare, err := commandRuntime.Accept(context.Background(), &transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       string(prepareMetadata.CommandID),
		Type:            transport.MessagePrepare,
		Payload:         preparePayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepareUpdate, err := prepare.Execute(context.Background())
	if err != nil || prepareUpdate.State != domain.ExecutionPreparing {
		t.Fatalf("Prepare result = (%#v, %v)", prepareUpdate, err)
	}
	pending, err := commandRuntime.PendingUpdates(context.Background())
	if err != nil || len(pending) != 1 ||
		pending[0].Update.CommandID != runtimeFaultPrepareID {
		t.Fatalf("Prepare outbox = (%#v, %v)", pending, err)
	}
	if err := commandRuntime.AcknowledgeUpdate(
		context.Background(),
		pending[0].MessageID,
	); err != nil {
		t.Fatal(err)
	}
	record := runtimeFaultSingleRecord(t, agentStore)
	if record.State != runner.StatePrepared || record.PID != 0 ||
		record.JITDigest != "" || record.Containment != (runner.ContainmentRef{}) {
		t.Fatalf("prepared runtime before Start acceptance = %#v", record)
	}

	startMetadata := transport.CommandMetadata{
		CommandID:       runtimeFaultStartID,
		ControllerEpoch: 1,
		ExecutionID:     runtimeFaultAcceptedExecID,
		ExpectedState:   domain.ExecutionPreparing,
		Target:          transport.CommandTarget{TargetID: "target-1", Scope: "owner/repo", ScopeKind: domain.TargetRepository},
	}
	startPayload, err := transport.EncodeStartCommandPayload(
		startMetadata,
		runner.OfficialRunnerVersion,
		false,
		runtimeFaultAcceptedJIT,
	)
	if err != nil {
		t.Fatal(err)
	}
	acceptedStart, err := commandRuntime.Accept(
		context.Background(),
		&transport.Envelope{
			ProtocolVersion: transport.ProtocolVersion,
			MessageID:       string(startMetadata.CommandID),
			Type:            transport.MessageStart,
			Payload:         startPayload,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer acceptedStart.Discard()
	journal, err := agentStore.Snapshot(context.Background())
	if err != nil || len(journal.Commands) != 2 ||
		journal.Commands[1].ID != runtimeFaultStartID ||
		len(journal.Observations) != 1 ||
		journal.Observations[0].State != domain.ExecutionPreparing {
		t.Fatalf("accepted Start journal = (%#v, %v)", journal, err)
	}
	pending, err = commandRuntime.PendingUpdates(context.Background())
	if err != nil || len(pending) != 0 {
		t.Fatalf("pre-kill outbox = (%#v, %v)", pending, err)
	}
	if attempts, starts, deliveries := supervisor.counts(); attempts != 0 ||
		starts != 0 || deliveries != 0 {
		t.Fatalf(
			"Start executed before crash boundary: attempts=%d starts=%d deliveries=%d",
			attempts,
			starts,
			deliveries,
		)
	}

	fmt.Fprintln(os.Stdout, "READY start-accepted")
	if err := os.Stdout.Sync(); err != nil {
		t.Fatal(err)
	}
	// The accepted command retains the only decoded JIT value in memory. The
	// parent SIGKILLs here, before Execute can call runner.Manager.EnsureRunning.
	_, _ = io.Copy(io.Discard, os.Stdin)
	t.Fatal("fault helper stdin closed before SIGKILL")
}

func TestControllerKillAfterQueueCommitBeforeAckCreatesOneRuntime(t *testing.T) {
	harness := newRuntimeFaultHarness(t, 21)
	defer harness.close()
	harness.killControllerAfterQueueCommit(t)
	if generated, _, _ := harness.lifecycle.counts(); generated != 0 {
		t.Fatalf("JIT generation crossed the pre-ack kill boundary: %d", generated)
	}
	if _, starts, deliveries := harness.supervisor.counts(); starts != 0 || deliveries != 0 {
		t.Fatalf("runtime crossed the pre-ack kill boundary: starts=%d JIT=%d", starts, deliveries)
	}

	// The helper was SIGKILLed inside DeleteMessage, after the queue/claim commit
	// and before an acknowledgement could return. A new process epoch must
	// consume the redelivery without creating another claim or runtime.
	harness.restartController(t)
	claim, found, err := harness.controller.GitHubClaim(
		context.Background(),
		store.ScaleSetID(runtimeFaultScaleSetID),
		runtimeFaultRunnerRequestID,
	)
	if err != nil || !found || claim.State != store.GitHubClaimPending {
		t.Fatalf("claim recovered after pre-ack kill = (%#v, %t, %v)", claim, found, err)
	}
	restarted := harness.newCoordinator(t)
	message, err := restarted.PollOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if message == nil || message.ID != harness.message.ID {
		t.Fatalf("redelivered message = %#v", message)
	}
	drove, err := restarted.DriveNext(context.Background())
	if err != nil || !drove {
		t.Fatalf("restart drive = (%t, %v)", drove, err)
	}

	claim, found, err = harness.controller.GitHubClaim(
		context.Background(),
		store.ScaleSetID(runtimeFaultScaleSetID),
		runtimeFaultRunnerRequestID,
	)
	if err != nil || !found || claim.State != store.GitHubClaimRunning {
		t.Fatalf("post-restart claim = (%#v, %t, %v)", claim, found, err)
	}
	generated, queries, removals := harness.lifecycle.counts()
	startRequests, starts, deliveries := harness.supervisor.counts()
	if generated != 1 || queries != 0 || removals != 0 ||
		harness.session.acquireCount() != 1 ||
		harness.agent.startCount() != 1 ||
		startRequests != 1 || starts != 1 || deliveries != 1 {
		t.Fatalf(
			"generation/acquire/dispatch/runtime = jit %d query %d remove %d acquire %d dispatch %d attempts %d starts %d deliveries %d",
			generated,
			queries,
			removals,
			harness.session.acquireCount(),
			harness.agent.startCount(),
			startRequests,
			starts,
			deliveries,
		)
	}
	records, err := harness.agentStore.RunnerJournalRecords(context.Background())
	if err != nil || len(records) != 1 ||
		records[0].ExecutionID != string(claim.Execution.ID) ||
		records[0].State != runner.StateRunning ||
		records[0].PID <= 0 ||
		records[0].Containment.FenceToken == "" {
		t.Fatalf("single runtime evidence = %#v, %v", records, err)
	}
	snapshot, err := harness.controller.Snapshot(context.Background())
	if err != nil || len(snapshot.Executions) != 1 || len(snapshot.Reservations) != 1 {
		t.Fatalf("single durable execution = %#v, %v", snapshot, err)
	}
}

func TestAgentSIGKILLAfterAcceptedStartCleansPreparedRuntimeWithoutStarting(t *testing.T) {
	directory := privateFaultDir(t)
	agentPath := filepath.Join(directory, "accepted-start-agent.db")
	runtimeRoot := filepath.Join(directory, "accepted-start-runtime")
	runtimeFaultKillAcceptedStartHelper(t, agentPath, runtimeRoot)
	runtimeFaultAssertSecretAbsentInSQLiteFiles(
		t,
		agentPath,
		[]byte(runtimeFaultAcceptedJIT),
	)

	agentStore := openAgent(t, agentPath)
	defer agentStore.Close()
	prepared := runtimeFaultSingleRecord(t, agentStore)
	if prepared.State != runner.StatePrepared || prepared.PID != 0 ||
		prepared.JITDigest != "" ||
		prepared.Containment != (runner.ContainmentRef{}) {
		t.Fatalf("post-kill prepared journal = %#v", prepared)
	}
	pending, err := agentStore.PendingExecutionUpdates(context.Background())
	if err != nil || len(pending) != 0 {
		t.Fatalf("post-kill outbox = (%#v, %v)", pending, err)
	}

	supervisor := newRuntimeFaultSupervisor()
	manager := runtimeFaultNewManager(t, agentStore, runtimeRoot, supervisor)
	commandRuntime, cancel := runtimeFaultNewCommandRuntime(
		t,
		runtimeFaultAcceptedNodeID,
		agentStore,
		manager,
	)
	defer cancel()
	if !commandRuntime.Ready(context.Background()) {
		t.Fatal("Agent startup recovery left the runtime unavailable")
	}
	released := runtimeFaultSingleRecord(t, agentStore)
	if released.State != runner.StateReleased || released.PID != 0 ||
		released.JITDigest != "" ||
		released.Containment != (runner.ContainmentRef{}) ||
		released.Revision <= prepared.Revision {
		t.Fatalf("lost-JIT cleanup journal = %#v", released)
	}
	if _, err := os.Lstat(filepath.Join(
		runtimeRoot,
		"executions",
		prepared.RootName,
	)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prepared workspace survived lost-JIT cleanup: %v", err)
	}
	pending, err = commandRuntime.PendingUpdates(context.Background())
	if err != nil || len(pending) != 1 {
		t.Fatalf("lost-JIT reconciliation outbox = (%#v, %v)", pending, err)
	}
	update := pending[0].Update
	if update.CommandID != runtimeFaultStartID ||
		update.ExecutionID != runtimeFaultAcceptedExecID ||
		update.State != domain.ExecutionReleased ||
		!update.Replayed ||
		update.ErrorCode != domain.ExecutionErrorNone {
		t.Fatalf("lost-JIT reconciliation update = %#v", update)
	}
	if attempts, starts, deliveries := supervisor.counts(); attempts != 0 ||
		starts != 0 || deliveries != 0 {
		t.Fatalf(
			"startup regenerated JIT or started runner: attempts=%d starts=%d deliveries=%d",
			attempts,
			starts,
			deliveries,
		)
	}

	// A valid accepted-Start crash always has the Prepared journal created by
	// Prepare. This differs from
	// TestAgentStartupRecoveryTreatsAcceptedStartWithoutRuntimeAsJournalCorruption,
	// whose missing record is corruption and must remain fail-closed.
	secondManager := runtimeFaultNewManager(t, agentStore, runtimeRoot, supervisor)
	secondRuntime, secondCancel := runtimeFaultNewCommandRuntime(
		t,
		runtimeFaultAcceptedNodeID,
		agentStore,
		secondManager,
	)
	defer secondCancel()
	secondPending, err := secondRuntime.PendingUpdates(context.Background())
	if err != nil || len(secondPending) != 1 ||
		secondPending[0] != pending[0] {
		t.Fatalf("replayed startup duplicated terminal outbox = (%#v, %v)", secondPending, err)
	}
	if attempts, starts, deliveries := supervisor.counts(); attempts != 0 ||
		starts != 0 || deliveries != 0 {
		t.Fatalf(
			"replayed startup regenerated JIT or runner: attempts=%d starts=%d deliveries=%d",
			attempts,
			starts,
			deliveries,
		)
	}

	runtimeFaultProveControllerLostJITCleanup(
		t,
		agentStore,
		commandRuntime,
		pending[0],
	)
}

// runtimeFaultProveControllerLostJITCleanup connects the real Agent SIGKILL
// evidence above to the production Controller/store/provider reconciliation
// boundary. In particular, the reconnect snapshot arrives before the terminal
// outbox, so it may suppress the slot but may not authorize a provider query.
func runtimeFaultProveControllerLostJITCleanup(
	t *testing.T,
	agentStore *store.AgentStore,
	commandRuntime *app.AgentCommandRuntime,
	terminal store.PendingExecutionUpdate,
) {
	t.Helper()
	ctx := context.Background()
	directory := privateFaultDir(t)
	controllerPath := filepath.Join(directory, "lost-jit-controller.db")
	now := time.Unix(1_707_000_000, 0).UTC()
	openAt := func() *store.ControllerStore {
		controller, err := store.OpenController(ctx, controllerPath, store.Options{
			Now: func() time.Time { return now },
		})
		if err != nil {
			if controller != nil {
				_ = controller.Close()
			}
			t.Fatal(err)
		}
		return controller
	}
	controller := openAt()
	defer func() {
		if controller != nil {
			_ = controller.Close()
		}
	}()
	enrollmentEpoch, err := controller.EnrollmentEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := enrollFaultNode(t, controller, enrollmentEpoch, 0x31)
	if nodeID != runtimeFaultAcceptedNodeID {
		t.Fatalf("lost-JIT Controller node = %q", nodeID)
	}
	owningEpoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	emptySnapshot := runtimeFaultEmptyAgentSnapshot(nodeID)
	if err := controller.RecordAgentSnapshot(
		ctx,
		runtimeFaultControllerSnapshot(emptySnapshot),
	); err != nil {
		t.Fatal(err)
	}
	binding := store.GitHubTargetRuntimeBinding{
		TargetID:   runtimeFaultTargetID,
		ScaleSetID: store.ScaleSetID(runtimeFaultScaleSetID),
		ProfileID:  runtimeFaultProfileID,
	}
	if _, err := controller.ConfigureRunnerProfile(ctx, store.RunnerProfileUpdatePolicy{
		ProfileID:     runtimeFaultProfileID,
		VersionPolicy: domain.RunnerVersionAutoUpdate,
		RunnerVersion: runner.OfficialRunnerVersion,
		Revision:      1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ConfigureGitHubTargetRuntimeBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.RecordGitHubScaleSetSessionSuccess(
		ctx,
		store.ScaleSetID(runtimeFaultScaleSetID),
	); err != nil {
		t.Fatal(err)
	}
	pollState, err := controller.ReadGitHubPollState(ctx, binding, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	pollState.ClaimAuthority.AdvertisedCapacity = 1
	commit, err := controller.CommitGitHubQueueMessage(
		ctx,
		store.GitHubQueueMessage{
			ScaleSetID: store.ScaleSetID(runtimeFaultScaleSetID),
			MessageID:  707,
			Digest:     runtimeFaultDigest("lost-jit-message"),
			Jobs: []store.GitHubJobEvent{{
				Type:            store.GitHubJobAvailable,
				RunnerRequestID: runtimeFaultRunnerRequestID,
				RepositoryName:  "sparerunner-private",
				OwnerName:       "runtime-fault-owner",
				JobID:           "runtime-fault-job",
				WorkflowRunID:   707001,
				ExecutionID:     runtimeFaultAcceptedExecID,
			}},
		},
		store.SingleSlotBinding{
			TargetID:      runtimeFaultTargetID,
			NodeID:        nodeID,
			Slot:          0,
			ClaimEnabled:  true,
			PollAuthority: pollState.ClaimAuthority,
		},
	)
	if err != nil || commit.Claim == nil ||
		commit.Claim.Execution.ID != runtimeFaultAcceptedExecID {
		t.Fatalf("lost-JIT queue commit = (%#v, %v)", commit, err)
	}
	acquire, err := controller.BeginGitHubAcquire(
		ctx,
		store.ScaleSetID(runtimeFaultScaleSetID),
		runtimeFaultRunnerRequestID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkGitHubAcquired(ctx, acquire); err != nil {
		t.Fatal(err)
	}

	prepareMetadata := transport.CommandMetadata{
		CommandID:       runtimeFaultPrepareID,
		ControllerEpoch: owningEpoch,
		ExecutionID:     runtimeFaultAcceptedExecID,
		ExpectedState:   domain.ExecutionReserved,
		Target:          transport.CommandTarget{TargetID: "target-1", Scope: "owner/repo", ScopeKind: domain.TargetRepository},
	}
	preparePayload, err := transport.EncodePrepareCommandPayload(
		prepareMetadata,
		runner.OfficialRunnerVersion,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	prepareDigest := transport.PayloadDigest(
		transport.MessagePrepare,
		preparePayload,
	)
	if _, err := controller.CommitAgentCommand(ctx, store.IssuedAgentCommand{
		NodeID: nodeID,
		Type:   domain.CommandPrepare,
		Command: domain.Command{
			ID:              prepareMetadata.CommandID,
			ControllerEpoch: prepareMetadata.ControllerEpoch,
			ExecutionID:     prepareMetadata.ExecutionID,
			ExpectedState:   prepareMetadata.ExpectedState,
			PayloadDigest:   hex.EncodeToString(prepareDigest[:]),
		},
	}); err != nil {
		t.Fatal(err)
	}
	runtimeFaultRecordControllerUpdate(t, controller, transport.ExecutionUpdate{
		NodeID:      nodeID,
		CommandID:   runtimeFaultPrepareID,
		ExecutionID: runtimeFaultAcceptedExecID,
		State:       domain.ExecutionPreparing,
	}, runtimeFaultDigest("lost-jit-prepare-update"))
	if err := controller.MarkGitHubPreparing(
		ctx,
		store.ScaleSetID(runtimeFaultScaleSetID),
		runtimeFaultRunnerRequestID,
	); err != nil {
		t.Fatal(err)
	}

	runnerName := runtimeFaultRunnerName(
		store.ScaleSetID(runtimeFaultScaleSetID),
		runtimeFaultRunnerRequestID,
	)
	attempt, replayed, err := controller.BeginGitHubJITAttempt(
		ctx,
		store.ScaleSetID(runtimeFaultScaleSetID),
		runtimeFaultRunnerRequestID,
		owningEpoch,
		runnerName,
	)
	if err != nil || replayed {
		t.Fatalf("lost-JIT begin attempt = (%#v, %t, %v)", attempt, replayed, err)
	}
	jitDigest := runtimeFaultDigest(runtimeFaultAcceptedJIT)
	const runnerID = 7071
	if err := controller.MarkGitHubJITGenerated(
		ctx,
		attempt,
		runnerID,
		jitDigest,
		runtimeFaultStartID,
	); err != nil {
		t.Fatal(err)
	}
	attempt.RunnerID = runnerID
	attempt.JITDigest = jitDigest
	attempt.StartCommandID = runtimeFaultStartID
	attempt.State = store.GitHubJITGenerated
	if err := controller.BeginGitHubStartDispatch(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	startMetadata := transport.CommandMetadata{
		CommandID:       runtimeFaultStartID,
		ControllerEpoch: owningEpoch,
		ExecutionID:     runtimeFaultAcceptedExecID,
		ExpectedState:   domain.ExecutionPreparing,
		Target:          transport.CommandTarget{TargetID: "target-1", Scope: "owner/repo", ScopeKind: domain.TargetRepository},
	}
	startPayload, err := transport.EncodeStartCommandPayload(
		startMetadata,
		runner.OfficialRunnerVersion,
		false,
		runtimeFaultAcceptedJIT,
	)
	if err != nil {
		t.Fatal(err)
	}
	startDigest := transport.PayloadDigest(transport.MessageStart, startPayload)
	if _, err := controller.CommitAgentCommand(ctx, store.IssuedAgentCommand{
		NodeID: nodeID,
		Type:   domain.CommandStart,
		Command: domain.Command{
			ID:              startMetadata.CommandID,
			ControllerEpoch: startMetadata.ControllerEpoch,
			ExecutionID:     startMetadata.ExecutionID,
			ExpectedState:   startMetadata.ExpectedState,
			PayloadDigest:   hex.EncodeToString(startDigest[:]),
		},
	}); err != nil {
		t.Fatal(err)
	}
	attempt.State = store.GitHubJITStartDispatching
	if err := controller.MarkGitHubStartAmbiguous(ctx, attempt); err != nil {
		t.Fatal(err)
	}

	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	controller = openAt()
	reconciliationEpoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restart, err := controller.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := reconcile.RestoreRestart(
		restart,
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	terminalSnapshot := runtimeFaultAgentSnapshot(t, nodeID, agentStore)
	if err := controller.RecordAgentSnapshot(
		ctx,
		runtimeFaultControllerSnapshot(terminalSnapshot),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := projection.ReconcileAgentSnapshot(terminalSnapshot); err != nil {
		t.Fatal(err)
	}
	agent := &runtimeFaultControllerAgent{
		nodeID:     nodeID,
		controller: controller,
		projection: projection,
		snapshot:   terminalSnapshot,
		online:     true,
	}
	lifecycle := &runtimeFaultGitHubLifecycle{
		runner: &github.RunnerReference{
			ID:         runnerID,
			Name:       runnerName,
			ScaleSetID: runtimeFaultScaleSetID,
		},
	}
	coordinator, err := app.NewControllerRunnerCoordinator(
		controller,
		newRuntimeFaultSession(runtimeFaultMessage()),
		agent,
		lifecycle,
		app.ControllerRunnerConfig{
			ScaleSetID:      runtimeFaultScaleSetID,
			TargetID:        runtimeFaultTargetID,
			Scope:           "owner/repo",
			ScopeKind:       domain.TargetRepository,
			RunnerProfileID: runtimeFaultProfileID,
			VersionPolicy:   domain.RunnerVersionAutoUpdate,
			NodeID:          nodeID,
			ControllerEpoch: reconciliationEpoch,
			Reconciler:      projection,
		},
		slog.New(slog.DiscardHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ReconcileJITAttempt(
		ctx,
		runtimeFaultRunnerRequestID,
	); !errors.Is(err, app.ErrGitHubReconciliationRequired) {
		t.Fatalf("snapshot-before-outbox reconciliation = %v", err)
	}
	if _, queries, removals := lifecycle.counts(); queries != 0 || removals != 0 {
		t.Fatalf(
			"provider touched before terminal outbox: queries=%d removals=%d",
			queries,
			removals,
		)
	}

	terminalUpdate := transport.ExecutionUpdate{
		NodeID:      terminal.Update.NodeID,
		CommandID:   terminal.Update.CommandID,
		ExecutionID: terminal.Update.ExecutionID,
		State:       terminal.Update.State,
		Replayed:    terminal.Update.Replayed,
		ErrorCode:   terminal.Update.ErrorCode,
	}
	runtimeFaultRecordControllerUpdate(
		t,
		controller,
		terminalUpdate,
		terminal.MessageID,
	)
	if err := projection.ApplyExecutionUpdate(terminalUpdate); err != nil {
		t.Fatal(err)
	}
	if err := commandRuntime.AcknowledgeUpdate(ctx, terminal.MessageID); err != nil {
		t.Fatal(err)
	}
	prunedSnapshot := runtimeFaultAgentSnapshot(t, nodeID, agentStore)
	if len(prunedSnapshot.Commands) != 0 ||
		len(prunedSnapshot.Observations) != 0 ||
		len(prunedSnapshot.CleanupTombstones) != 0 {
		t.Fatalf("terminal ACK did not prune exact Agent lineage: %#v", prunedSnapshot)
	}
	if err := controller.RecordAgentSnapshot(
		ctx,
		runtimeFaultControllerSnapshot(prunedSnapshot),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := projection.ReconcileAgentSnapshot(prunedSnapshot); err != nil {
		t.Fatal(err)
	}
	agent.activate(controller, projection, nil, prunedSnapshot)

	for iteration := 0; iteration < 3; iteration++ {
		err := coordinator.ReconcileJITAttempt(ctx, runtimeFaultRunnerRequestID)
		if iteration < 2 {
			if !errors.Is(err, app.ErrGitHubReconciliationRequired) {
				t.Fatalf("lost-JIT provider pass %d = %v", iteration+1, err)
			}
		} else if err != nil {
			t.Fatalf("lost-JIT final absence = %v", err)
		}
		if iteration == 1 {
			now = now.Add(store.GitHubRunnerAbsenceConfirmationDelay + time.Millisecond)
		}
	}
	_, queries, removals := lifecycle.counts()
	if queries != 3 || removals != 1 {
		t.Fatalf(
			"exact provider cleanup = queries %d removals %d, want 3/1",
			queries,
			removals,
		)
	}
	queryHistory, removalHistory := lifecycle.history()
	wantQuery := github.RunnerQuery{
		ScaleSetID:      runtimeFaultScaleSetID,
		RunnerRequestID: runtimeFaultRunnerRequestID,
		Name:            runnerName,
		ExpectedID:      runnerID,
	}
	for index, query := range queryHistory {
		if query != wantQuery {
			t.Fatalf("provider query %d = %#v, want %#v", index, query, wantQuery)
		}
	}
	if len(removalHistory) != 1 || removalHistory[0] != (github.RunnerReference{
		ID:         runnerID,
		Name:       runnerName,
		ScaleSetID: runtimeFaultScaleSetID,
	}) {
		t.Fatalf("provider removal history = %#v", removalHistory)
	}
	fence, found, err := controller.NextGitHubReconciliationFence(
		ctx,
		store.ScaleSetID(runtimeFaultScaleSetID),
	)
	if err != nil || found {
		t.Fatalf("dormant lost-JIT fence = (%#v, %t, %v)", fence, found, err)
	}
	if err := coordinator.ReconcileJITAttempt(
		ctx,
		runtimeFaultRunnerRequestID,
	); !errors.Is(err, app.ErrGitHubReconciliationRequired) {
		t.Fatalf("dormant reconciliation replay = %v", err)
	}
	if _, replayQueries, replayRemovals := lifecycle.counts(); replayQueries != queries || replayRemovals != removals {
		t.Fatalf(
			"dormant replay touched provider: queries %d removals %d",
			replayQueries,
			replayRemovals,
		)
	}
	runtimeFaultAssertSecretAbsentInSQLiteFiles(
		t,
		controllerPath,
		[]byte(runtimeFaultAcceptedJIT),
	)
}

func runtimeFaultRecordControllerUpdate(
	t *testing.T,
	controller *store.ControllerStore,
	update transport.ExecutionUpdate,
	messageID string,
) {
	t.Helper()
	encoded, err := transport.EncodeExecutionUpdate(update)
	if err != nil {
		t.Fatal(err)
	}
	digest := transport.PayloadDigest(transport.MessageExecutionUpdate, encoded)
	if _, err := controller.RecordAgentExecutionUpdate(
		context.Background(),
		store.AgentExecutionUpdate{
			NodeID:        update.NodeID,
			MessageID:     messageID,
			CommandID:     update.CommandID,
			ExecutionID:   update.ExecutionID,
			State:         update.State,
			Replayed:      update.Replayed,
			ErrorCode:     update.ErrorCode,
			PayloadDigest: hex.EncodeToString(digest[:]),
		},
	); err != nil {
		t.Fatal(err)
	}
}

func runtimeFaultDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func runtimeFaultRunnerName(
	scaleSetID store.ScaleSetID,
	runnerRequestID int64,
) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"runner\x00%d\x00%d",
		scaleSetID,
		runnerRequestID,
	)))
	return "sparerunner-" + strings.ToLower(base32.StdEncoding.WithPadding(
		base32.NoPadding,
	).EncodeToString(digest[:]))
}

// TestAgentKillReplaysAcceptedStartWithoutCreatingAnotherRuntime exercises the
// abrupt SIGKILL/WAL boundary in this package. This complementary test recreates
// the production AgentCommandRuntime and runner.Manager over that same durable
// journal shape, proving that actual runtime recovery adopts its exact lineage.
func TestAgentManagerRestartAfterAcceptedStartAdoptsExactRuntimeWithoutAnotherJIT(t *testing.T) {
	harness := newRuntimeFaultHarness(t, 22)
	defer harness.close()
	harness.agent.failStartResponse = true

	coordinator := harness.newCoordinator(t)
	if _, err := coordinator.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	drove, err := coordinator.DriveNext(context.Background())
	if !drove || !errors.Is(err, app.ErrGitHubStartAmbiguous) {
		t.Fatalf("ambiguous Start drive = (%t, %v)", drove, err)
	}
	claim, found, claimErr := harness.controller.GitHubClaim(
		context.Background(),
		store.ScaleSetID(runtimeFaultScaleSetID),
		runtimeFaultRunnerRequestID,
	)
	if claimErr != nil || !found || claim.State != store.GitHubClaimStartAmbiguous {
		t.Fatalf("ambiguous claim = (%#v, %t, %v)", claim, found, claimErr)
	}
	before := runtimeFaultSingleRecord(t, harness.agentStore)
	if before.State != runner.StateRunning || before.PID <= 0 ||
		before.Containment.FenceToken == "" {
		t.Fatalf("pre-kill runtime lineage = %#v", before)
	}
	if harness.agent.startCount() != 1 {
		t.Fatalf("pre-kill Start dispatches = %d", harness.agent.startCount())
	}
	aliveBefore, _ := harness.supervisor.recoveryObservation()

	// The Controller did not receive the Running update. The Agent journal did,
	// so recreating both AgentCommandRuntime and runner.Manager must Recover the
	// exact PID/containment instead of accepting or generating another Start.
	harness.restartAgent(t)
	after := runtimeFaultSingleRecord(t, harness.agentStore)
	if after != before {
		t.Fatalf("runtime lineage changed across Agent restart:\nbefore=%#v\nafter=%#v", before, after)
	}
	aliveAfter, observed := harness.supervisor.recoveryObservation()
	wantProcess := runner.Process{PID: after.PID, Containment: after.Containment}
	if aliveAfter != aliveBefore+1 || observed != wantProcess {
		t.Fatalf(
			"Manager Recover observation = calls %d->%d process %#v, want %#v",
			aliveBefore,
			aliveAfter,
			observed,
			wantProcess,
		)
	}
	harness.activateCurrentAgentSnapshot(t)
	admission, err := harness.projection.Admission(harness.nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if !runtimeFaultContainsAction(admission.Actions, reconcile.ActionConfirmAgentStartAccepted) ||
		runtimeFaultContainsAction(admission.Actions, reconcile.ActionIssuePrepare) {
		t.Fatalf("Agent reconnect actions = %#v", admission.Actions)
	}

	// The original Controller epoch keeps the ambiguous intent fenced. This
	// drive may not regenerate JIT or resend Start merely to regain liveness.
	for attempt := 0; attempt < 3; attempt++ {
		drove, driveErr := coordinator.DriveNext(context.Background())
		runtimeFaultAssertOneStart(t, harness)
		if driveErr != nil {
			t.Fatalf("same-epoch reconciliation %d: %v", attempt, driveErr)
		}
		if !drove {
			break
		}
	}
	claim, found, claimErr = harness.controller.GitHubClaim(
		context.Background(),
		store.ScaleSetID(runtimeFaultScaleSetID),
		runtimeFaultRunnerRequestID,
	)
	if claimErr != nil || !found || claim.State != store.GitHubClaimStartAmbiguous {
		t.Fatalf("same-epoch ambiguity fence = (%#v, %t, %v)", claim, found, claimErr)
	}

	// A later Controller epoch owns ambiguity resolution. The fresh Agent
	// snapshot proves that the exact accepted Start and Running runtime survived;
	// convergence therefore still requires no provider generation or Agent Start.
	harness.restartController(t)
	restarted := harness.newCoordinator(t)
	for attempt := 0; attempt < 4; attempt++ {
		drove, driveErr := restarted.DriveNext(context.Background())
		if driveErr != nil {
			t.Fatalf("post-reconnect drive %d: %v", attempt, driveErr)
		}
		if !drove {
			break
		}
	}
	claim, found, claimErr = harness.controller.GitHubClaim(
		context.Background(),
		store.ScaleSetID(runtimeFaultScaleSetID),
		runtimeFaultRunnerRequestID,
	)
	if claimErr != nil || !found || claim.State != store.GitHubClaimRunning {
		t.Fatalf("reconciled claim = (%#v, %t, %v)", claim, found, claimErr)
	}
	runtimeFaultAssertOneStart(t, harness)
	finalRecord := runtimeFaultSingleRecord(t, harness.agentStore)
	if finalRecord != before {
		t.Fatalf("final runtime lineage changed:\nbefore=%#v\nafter=%#v", before, finalRecord)
	}
}

type runtimeFaultHarness struct {
	controllerPath string
	agentPath      string
	runtimeRoot    string
	nodeID         domain.NodeID

	controller *store.ControllerStore
	projection *reconcile.Controller
	epoch      domain.ControllerEpoch
	agentStore *store.AgentStore

	supervisor *runtimeFaultSupervisor
	runtime    *app.AgentCommandRuntime
	cancel     context.CancelFunc
	agent      *runtimeFaultControllerAgent
	session    *runtimeFaultSession
	lifecycle  *runtimeFaultGitHubLifecycle
	message    *github.Message
}

func newRuntimeFaultHarness(t *testing.T, seed byte) *runtimeFaultHarness {
	t.Helper()
	ctx := context.Background()
	directory := privateFaultDir(t)
	harness := &runtimeFaultHarness{
		controllerPath: filepath.Join(directory, "controller.db"),
		agentPath:      filepath.Join(directory, "agent.db"),
		runtimeRoot:    filepath.Join(directory, "runtime"),
		supervisor:     newRuntimeFaultSupervisor(),
	}
	harness.controller = openController(t, harness.controllerPath)
	enrollmentEpoch, err := harness.controller.EnrollmentEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	harness.nodeID = enrollFaultNode(t, harness.controller, enrollmentEpoch, seed)
	harness.projection, err = reconcile.Start(ctx, harness.controller, reconcile.Config{
		Nodes: []reconcile.NodeDefinition{faultNodeDefinition(harness.nodeID, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	harness.epoch = harness.projection.Epoch()
	harness.agentStore = openAgent(t, harness.agentPath)
	harness.runtime, harness.cancel = harness.newAgentRuntime(t)
	harness.message = runtimeFaultMessage()
	harness.session = newRuntimeFaultSession(harness.message)
	harness.lifecycle = &runtimeFaultGitHubLifecycle{}
	harness.agent = &runtimeFaultControllerAgent{
		nodeID:     harness.nodeID,
		controller: harness.controller,
		projection: harness.projection,
		runtime:    harness.runtime,
	}
	harness.activateCurrentAgentSnapshot(t)
	if _, err := harness.controller.ConfigureRunnerProfile(ctx, store.RunnerProfileUpdatePolicy{
		ProfileID:     runtimeFaultProfileID,
		VersionPolicy: domain.RunnerVersionAutoUpdate,
		RunnerVersion: runner.OfficialRunnerVersion,
		Revision:      1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.controller.ConfigureGitHubTargetRuntimeBinding(
		ctx,
		store.GitHubTargetRuntimeBinding{
			TargetID:   runtimeFaultTargetID,
			ScaleSetID: store.ScaleSetID(runtimeFaultScaleSetID),
			ProfileID:  runtimeFaultProfileID,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.controller.RecordGitHubScaleSetSessionSuccess(
		ctx,
		store.ScaleSetID(runtimeFaultScaleSetID),
	); err != nil {
		t.Fatal(err)
	}
	return harness
}

func (harness *runtimeFaultHarness) newAgentRuntime(
	t *testing.T,
) (*app.AgentCommandRuntime, context.CancelFunc) {
	t.Helper()
	manager := runtimeFaultNewManager(
		t,
		harness.agentStore,
		harness.runtimeRoot,
		harness.supervisor,
	)
	return runtimeFaultNewCommandRuntime(
		t,
		harness.nodeID,
		harness.agentStore,
		manager,
	)
}

func runtimeFaultNewManager(
	t *testing.T,
	agentStore *store.AgentStore,
	runtimeRoot string,
	supervisor *runtimeFaultSupervisor,
) *runner.Manager {
	t.Helper()
	manager, err := runner.NewManager(runner.Options{
		RuntimeRoot: runtimeRoot,
		Cache:       runtimeFaultPackageCache{},
		Journal:     agentStore.RunnerJournal(),
		Supervisor:  supervisor,
		Cleaner:     runtimeFaultCleaner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func runtimeFaultNewCommandRuntime(
	t *testing.T,
	nodeID domain.NodeID,
	agentStore *store.AgentStore,
	manager *runner.Manager,
) (*app.AgentCommandRuntime, context.CancelFunc) {
	t.Helper()
	pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	commandRuntime, err := app.NewAgentCommandRuntime(
		string(nodeID),
		agentStore,
		manager,
		pkg,
	)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	if err := commandRuntime.Start(lifetime); err != nil {
		cancel()
		t.Fatal(err)
	}
	return commandRuntime, cancel
}

func (harness *runtimeFaultHarness) newCoordinator(
	t *testing.T,
) *app.ControllerRunnerCoordinator {
	t.Helper()
	coordinator, err := app.NewControllerRunnerCoordinator(
		harness.controller,
		harness.session,
		harness.agent,
		harness.lifecycle,
		app.ControllerRunnerConfig{
			ScaleSetID:      runtimeFaultScaleSetID,
			TargetID:        runtimeFaultTargetID,
			Scope:           "owner/repo",
			ScopeKind:       domain.TargetRepository,
			RunnerProfileID: runtimeFaultProfileID,
			VersionPolicy:   domain.RunnerVersionAutoUpdate,
			NodeID:          harness.nodeID,
			ControllerEpoch: harness.epoch,
			Reconciler:      harness.projection,
		},
		slog.New(slog.DiscardHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func (harness *runtimeFaultHarness) activateCurrentAgentSnapshot(t *testing.T) {
	t.Helper()
	snapshot := runtimeFaultAgentSnapshot(t, harness.nodeID, harness.agentStore)
	if err := harness.controller.RecordAgentSnapshot(
		context.Background(),
		runtimeFaultControllerSnapshot(snapshot),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.projection.ReconcileAgentSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	harness.agent.activate(
		harness.controller,
		harness.projection,
		harness.runtime,
		snapshot,
	)
}

func (harness *runtimeFaultHarness) restartController(t *testing.T) {
	t.Helper()
	if harness.controller != nil {
		if err := harness.controller.Close(); err != nil {
			t.Fatal(err)
		}
	}
	harness.controller = openController(t, harness.controllerPath)
	epoch, err := harness.controller.AdvanceEpoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restart, err := harness.controller.RestartSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	projection, err := reconcile.RestoreRestart(restart, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	harness.epoch = epoch
	harness.projection = projection
	if _, err := harness.controller.RecordGitHubScaleSetSessionSuccess(
		context.Background(),
		store.ScaleSetID(runtimeFaultScaleSetID),
	); err != nil {
		t.Fatal(err)
	}
	harness.activateCurrentAgentSnapshot(t)
}

func (harness *runtimeFaultHarness) killControllerAfterQueueCommit(t *testing.T) {
	t.Helper()
	if harness.controller == nil {
		t.Fatal("runtime fault Controller is unavailable")
	}
	if err := harness.controller.Close(); err != nil {
		t.Fatal(err)
	}
	harness.controller = nil

	command := exec.Command(
		os.Args[0],
		"-test.run=^TestRuntimeFaultControllerCommitHelper$",
	)
	command.Env = append(
		os.Environ(),
		"SPARERUNNER_RUNTIME_FAULT_CONTROLLER_DB="+harness.controllerPath,
		"SPARERUNNER_RUNTIME_FAULT_NODE_ID="+string(harness.nodeID),
	)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
			return
		}
		ready <- ""
	}()
	select {
	case line := <-ready:
		if line != "READY queue-committed" {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf(
				"Controller helper failed before pre-ack boundary: line=%q stderr=%q",
				line,
				stderr.String(),
			)
		}
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("Controller helper did not reach pre-ack boundary: %s", stderr.String())
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = stdin.Close()
	if err := command.Wait(); err == nil {
		t.Fatal("Controller helper exited gracefully instead of being killed")
	}
}

func runtimeFaultKillAcceptedStartHelper(
	t *testing.T,
	agentPath string,
	runtimeRoot string,
) {
	t.Helper()
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestRuntimeFaultAgentAcceptedStartHelper$",
	)
	command.Env = append(
		os.Environ(),
		"SPARERUNNER_RUNTIME_FAULT_AGENT_DB="+agentPath,
		"SPARERUNNER_RUNTIME_FAULT_RUNTIME_ROOT="+runtimeRoot,
	)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
			return
		}
		ready <- ""
	}()
	select {
	case line := <-ready:
		if line != "READY start-accepted" {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf(
				"Agent helper failed before accepted-Start boundary: line=%q stderr=%q",
				line,
				stderr.String(),
			)
		}
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("Agent helper did not reach accepted-Start boundary: %s", stderr.String())
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = stdin.Close()
	if err := command.Wait(); err == nil {
		t.Fatal("Agent helper exited gracefully instead of being killed")
	}
}

func runtimeFaultAssertSecretAbsentInSQLiteFiles(
	t *testing.T,
	databasePath string,
	secret []byte,
) {
	t.Helper()
	for _, path := range []string{
		databasePath,
		databasePath + "-wal",
		databasePath + "-shm",
	} {
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatalf("read SQLite file %q: %v", path, err)
		}
		if bytes.Contains(data, secret) {
			t.Fatalf("accepted Start JIT secret persisted in SQLite file %q", path)
		}
	}
}

func (harness *runtimeFaultHarness) restartAgent(t *testing.T) {
	t.Helper()
	harness.cancel()
	harness.supervisor.waitForNoWaiters(t)
	if err := harness.agentStore.Close(); err != nil {
		t.Fatal(err)
	}
	harness.agentStore = openAgent(t, harness.agentPath)
	harness.runtime, harness.cancel = harness.newAgentRuntime(t)
}

func (harness *runtimeFaultHarness) close() {
	if harness.cancel != nil {
		harness.cancel()
	}
	if harness.supervisor != nil {
		harness.supervisor.waitForNoWaitersBestEffort()
	}
	if harness.agentStore != nil {
		_ = harness.agentStore.Close()
	}
	if harness.controller != nil {
		_ = harness.controller.Close()
	}
}

type runtimeFaultControllerAgent struct {
	mu                sync.Mutex
	nodeID            domain.NodeID
	controller        *store.ControllerStore
	projection        *reconcile.Controller
	runtime           *app.AgentCommandRuntime
	snapshot          app.AgentSnapshot
	online            bool
	starts            int
	failStartResponse bool
}

func (agent *runtimeFaultControllerAgent) activate(
	controller *store.ControllerStore,
	projection *reconcile.Controller,
	commandRuntime *app.AgentCommandRuntime,
	snapshot app.AgentSnapshot,
) {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	agent.controller = controller
	agent.projection = projection
	agent.runtime = commandRuntime
	agent.snapshot = snapshot
	agent.online = true
}

func (agent *runtimeFaultControllerAgent) Readiness(
	nodeID domain.NodeID,
) (app.AgentSnapshot, bool, context.Context) {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	if nodeID != agent.nodeID {
		return app.AgentSnapshot{}, false, nil
	}
	return runtimeFaultCloneSnapshot(agent.snapshot), agent.online, nil
}

func (agent *runtimeFaultControllerAgent) SendPrepare(
	ctx context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	disableUpdate bool,
) (transport.ExecutionUpdate, error) {
	payload, err := transport.EncodePrepareCommandPayload(
		metadata,
		runner.OfficialRunnerVersion,
		disableUpdate,
	)
	if err != nil {
		return transport.ExecutionUpdate{}, err
	}
	return agent.execute(ctx, nodeID, transport.MessagePrepare, metadata, payload, false)
}

func (agent *runtimeFaultControllerAgent) SendStart(
	ctx context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	disableUpdate bool,
	jit runner.JITConfig,
) (transport.ExecutionUpdate, error) {
	var value string
	if jit == nil {
		return transport.ExecutionUpdate{}, transport.ErrCommandSecret
	}
	if err := jit.Deliver(func(delivered string) error {
		value = delivered
		return nil
	}); err != nil {
		return transport.ExecutionUpdate{}, err
	}
	payload, err := transport.EncodeStartCommandPayload(
		metadata,
		runner.OfficialRunnerVersion,
		disableUpdate,
		value,
	)
	value = ""
	if err != nil {
		return transport.ExecutionUpdate{}, err
	}
	agent.mu.Lock()
	agent.starts++
	failResponse := agent.failStartResponse
	agent.failStartResponse = false
	agent.mu.Unlock()
	return agent.execute(ctx, nodeID, transport.MessageStart, metadata, payload, failResponse)
}

func (agent *runtimeFaultControllerAgent) ReplayPrepare(
	context.Context,
	domain.NodeID,
	transport.CommandMetadata,
	bool,
	string,
) (transport.ExecutionUpdate, error) {
	return transport.ExecutionUpdate{}, errors.New("unexpected Prepare replay in runtime fault harness")
}

func (agent *runtimeFaultControllerAgent) SendReconciliationCancel(
	context.Context,
	domain.NodeID,
	transport.CommandMetadata,
	string,
) (transport.ExecutionUpdate, error) {
	return transport.ExecutionUpdate{}, errors.New("unexpected Cancel in runtime fault harness")
}

func (agent *runtimeFaultControllerAgent) execute(
	ctx context.Context,
	nodeID domain.NodeID,
	messageType transport.MessageType,
	metadata transport.CommandMetadata,
	payload []byte,
	dropControllerResponse bool,
) (transport.ExecutionUpdate, error) {
	agent.mu.Lock()
	controller := agent.controller
	projection := agent.projection
	commandRuntime := agent.runtime
	agent.mu.Unlock()
	if nodeID != agent.nodeID || controller == nil || projection == nil || commandRuntime == nil {
		return transport.ExecutionUpdate{}, app.ErrAgentOffline
	}
	payloadDigest := transport.PayloadDigest(messageType, payload)
	commandType := domain.CommandPrepare
	if messageType == transport.MessageStart {
		commandType = domain.CommandStart
	}
	issued := store.IssuedAgentCommand{
		NodeID: nodeID,
		Type:   commandType,
		Command: domain.Command{
			ID:              metadata.CommandID,
			ControllerEpoch: metadata.ControllerEpoch,
			ExecutionID:     metadata.ExecutionID,
			ExpectedState:   metadata.ExpectedState,
			PayloadDigest:   hex.EncodeToString(payloadDigest[:]),
		},
	}
	if _, err := controller.CommitAgentCommand(ctx, issued); err != nil {
		return transport.ExecutionUpdate{}, err
	}
	if err := projection.ApplyIssuedCommand(reconcile.IssuedCommand{
		NodeID:  issued.NodeID,
		Type:    issued.Type,
		Command: issued.Command,
	}); err != nil {
		return transport.ExecutionUpdate{}, err
	}
	envelope := &transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       string(metadata.CommandID),
		Type:            messageType,
		Payload:         append([]byte(nil), payload...),
	}
	accepted, err := commandRuntime.Accept(ctx, envelope)
	if err != nil {
		return transport.ExecutionUpdate{}, err
	}
	update, executeErr := accepted.Execute(ctx)
	if executeErr != nil {
		return update, executeErr
	}
	if dropControllerResponse {
		return transport.ExecutionUpdate{}, app.ErrAgentDisconnected
	}
	encoded, err := transport.EncodeExecutionUpdate(update)
	if err != nil {
		return transport.ExecutionUpdate{}, err
	}
	updateDigest := transport.PayloadDigest(transport.MessageExecutionUpdate, encoded)
	messageID := sha256.Sum256(encoded)
	if _, err := controller.RecordAgentExecutionUpdate(ctx, store.AgentExecutionUpdate{
		NodeID:        update.NodeID,
		MessageID:     hex.EncodeToString(messageID[:]),
		CommandID:     update.CommandID,
		ExecutionID:   update.ExecutionID,
		State:         update.State,
		Replayed:      update.Replayed,
		ErrorCode:     update.ErrorCode,
		PayloadDigest: hex.EncodeToString(updateDigest[:]),
	}); err != nil {
		return transport.ExecutionUpdate{}, err
	}
	if err := projection.ApplyExecutionUpdate(update); err != nil {
		return transport.ExecutionUpdate{}, err
	}
	return update, nil
}

func (agent *runtimeFaultControllerAgent) startCount() int {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	return agent.starts
}

type runtimeFaultSession struct {
	mu       sync.Mutex
	message  *github.Message
	acquires int
}

func newRuntimeFaultSession(message *github.Message) *runtimeFaultSession {
	return &runtimeFaultSession{message: message}
}

func (session *runtimeFaultSession) Snapshot() (github.SessionSnapshot, error) {
	return github.SessionSnapshot{
		ScaleSetID: runtimeFaultScaleSetID,
		ID:         "runtime-fault-session",
		Statistics: github.Statistics{},
	}, nil
}

func (session *runtimeFaultSession) Poll(
	context.Context,
	int,
	int,
) (*github.Message, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	copyMessage := *session.message
	copyMessage.Jobs = append([]github.JobMessage(nil), session.message.Jobs...)
	return &copyMessage, nil
}

func (session *runtimeFaultSession) DeleteMessage(
	_ context.Context,
	_ int,
) error {
	return nil
}

func (session *runtimeFaultSession) AcquireJobs(
	_ context.Context,
	requests []int64,
) ([]int64, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.acquires++
	if len(requests) != 1 || requests[0] != runtimeFaultRunnerRequestID {
		return nil, errors.New("unexpected AcquireJobs identity")
	}
	return append([]int64(nil), requests...), nil
}

func (session *runtimeFaultSession) acquireCount() int {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.acquires
}

type runtimeFaultPreAckCrashSession struct {
	message *github.Message
}

func (*runtimeFaultPreAckCrashSession) Snapshot() (github.SessionSnapshot, error) {
	return github.SessionSnapshot{
		ScaleSetID: runtimeFaultScaleSetID,
		ID:         "runtime-fault-pre-ack-session",
		Statistics: github.Statistics{},
	}, nil
}

func (session *runtimeFaultPreAckCrashSession) Poll(
	context.Context,
	int,
	int,
) (*github.Message, error) {
	copyMessage := *session.message
	copyMessage.Jobs = append([]github.JobMessage(nil), session.message.Jobs...)
	return &copyMessage, nil
}

func (*runtimeFaultPreAckCrashSession) DeleteMessage(
	context.Context,
	int,
) error {
	fmt.Fprintln(os.Stdout, "READY queue-committed")
	if err := os.Stdout.Sync(); err != nil {
		return err
	}
	// Keep the acknowledgement call in flight until the parent sends SIGKILL.
	// No Controller Close or deferred cleanup can run after this boundary.
	_, _ = io.Copy(io.Discard, os.Stdin)
	return errors.New("fault helper stdin closed before SIGKILL")
}

func (*runtimeFaultPreAckCrashSession) AcquireJobs(
	context.Context,
	[]int64,
) ([]int64, error) {
	return nil, errors.New("AcquireJobs crossed the pre-ack kill boundary")
}

type runtimeFaultGitHubLifecycle struct {
	mu             sync.Mutex
	generated      int
	queries        int
	removals       int
	runner         *github.RunnerReference
	queryHistory   []github.RunnerQuery
	removalHistory []github.RunnerReference
}

func (lifecycle *runtimeFaultGitHubLifecycle) GenerateJITConfig(
	_ context.Context,
	request github.JITRequest,
) (runner.JITConfig, github.RunnerReference, error) {
	lifecycle.mu.Lock()
	lifecycle.generated++
	lifecycle.mu.Unlock()
	return &runtimeFaultJIT{value: "opaque-runtime-fault-jit"}, github.RunnerReference{
		ID:         7071,
		Name:       request.Name,
		ScaleSetID: request.ScaleSetID,
	}, nil
}

func (lifecycle *runtimeFaultGitHubLifecycle) QueryRunner(
	_ context.Context,
	query github.RunnerQuery,
) (*github.RunnerReference, error) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.queries++
	lifecycle.queryHistory = append(lifecycle.queryHistory, query)
	if lifecycle.runner == nil {
		return nil, nil
	}
	runner := *lifecycle.runner
	return &runner, nil
}

func (lifecycle *runtimeFaultGitHubLifecycle) RemoveRunner(
	_ context.Context,
	reference github.RunnerReference,
) error {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.removals++
	lifecycle.removalHistory = append(lifecycle.removalHistory, reference)
	if lifecycle.runner != nil && *lifecycle.runner == reference {
		lifecycle.runner = nil
	}
	return nil
}

func (lifecycle *runtimeFaultGitHubLifecycle) counts() (int, int, int) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	return lifecycle.generated, lifecycle.queries, lifecycle.removals
}

func (lifecycle *runtimeFaultGitHubLifecycle) history() (
	[]github.RunnerQuery,
	[]github.RunnerReference,
) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	return append([]github.RunnerQuery(nil), lifecycle.queryHistory...),
		append([]github.RunnerReference(nil), lifecycle.removalHistory...)
}

type runtimeFaultJIT struct {
	mu        sync.Mutex
	value     string
	delivered bool
}

func (jit *runtimeFaultJIT) Digest() string {
	jit.mu.Lock()
	defer jit.mu.Unlock()
	digest := sha256.Sum256([]byte(jit.value))
	return hex.EncodeToString(digest[:])
}

func (jit *runtimeFaultJIT) Deliver(deliver func(string) error) error {
	jit.mu.Lock()
	defer jit.mu.Unlock()
	if jit.delivered || jit.value == "" || deliver == nil {
		return transport.ErrCommandSecret
	}
	jit.delivered = true
	value := jit.value
	jit.value = ""
	return deliver(value)
}

type runtimeFaultPackageCache struct{}

func (runtimeFaultPackageCache) Ensure(
	context.Context,
	runner.Package,
) (runner.PreparedPackage, error) {
	return runtimeFaultPreparedPackage{}, nil
}

type runtimeFaultPreparedPackage struct{}

func (runtimeFaultPreparedPackage) Materialize(root *os.Root) error {
	file, err := root.OpenFile(
		"runner.marker",
		os.O_CREATE|os.O_EXCL|os.O_WRONLY,
		0o600,
	)
	if err != nil {
		return err
	}
	if _, err := file.WriteString("bounded runtime fault runner\n"); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func (runtimeFaultPreparedPackage) Close() error { return nil }

type runtimeFaultCleaner struct{}

func (runtimeFaultCleaner) StrongWorkspaceOwnership() bool { return true }
func (runtimeFaultCleaner) WorkspaceBackend() string       { return "runtime-fault-v1" }

func (runtimeFaultCleaner) ValidateRuntimeRoot(
	_ context.Context,
	root string,
) error {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (runtimeFaultCleaner) PrepareWorkspace(
	_ context.Context,
	_ *os.Root,
	name string,
) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{
		Backend: "runtime-fault-v1",
		OwnerID: "workspace:" + name,
	}, nil
}

func (runtimeFaultCleaner) WorkspaceRef(
	_ context.Context,
	_ *os.Root,
	name string,
) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{
		Backend: "runtime-fault-v1",
		OwnerID: "workspace:" + name,
	}, nil
}

func (runtimeFaultCleaner) RemoveAndVerify(
	_ context.Context,
	root *os.Root,
	name string,
) error {
	if _, err := root.Lstat(name); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := root.RemoveAll(name); err != nil {
		return runner.ErrCleanupFailed
	}
	if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		return runner.ErrCleanupFailed
	}
	return nil
}

type runtimeFaultSupervisor struct {
	mu            sync.Mutex
	nextPID       int
	startRequests int
	starts        int
	jitDeliveries int
	aliveChecks   int
	lastAlive     runner.Process
	waiters       int
	running       map[string]runtimeFaultProcess
}

type runtimeFaultProcess struct {
	process runner.Process
	done    chan struct{}
}

func newRuntimeFaultSupervisor() *runtimeFaultSupervisor {
	return &runtimeFaultSupervisor{
		nextPID: 41000,
		running: make(map[string]runtimeFaultProcess),
	}
}

func (*runtimeFaultSupervisor) StrongDescendantOwnership() bool { return true }
func (*runtimeFaultSupervisor) WorkspaceBackend() string        { return "runtime-fault-v1" }

func (*runtimeFaultSupervisor) PrepareContainment(
	_ context.Context,
	executionID string,
) (runner.ContainmentRef, error) {
	return runner.ContainmentRef{
		Backend: "runtime-fault-v1",
		OwnerID: "runtime:" + executionID,
	}, nil
}

func (supervisor *runtimeFaultSupervisor) Start(
	ctx context.Context,
	request runner.StartRequest,
) (runner.Process, error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.startRequests++
	if _, exists := supervisor.running[request.Containment.FenceToken]; exists {
		return runner.Process{Containment: request.Containment}, runner.ErrStartFenced
	}
	if err := request.VerifyWorkspaceAtExec(ctx); err != nil {
		return runner.Process{Containment: request.Containment}, err
	}
	if err := request.DeliverJIT(func(value string) error {
		if value == "" {
			return errors.New("empty runtime fault JIT")
		}
		supervisor.jitDeliveries++
		return nil
	}); err != nil {
		return runner.Process{Containment: request.Containment}, err
	}
	supervisor.nextPID++
	process := runner.Process{
		PID:         supervisor.nextPID,
		Containment: request.Containment,
	}
	supervisor.running[request.Containment.FenceToken] = runtimeFaultProcess{
		process: process,
		done:    make(chan struct{}),
	}
	supervisor.starts++
	return process, nil
}

func (supervisor *runtimeFaultSupervisor) Stop(
	_ context.Context,
	process runner.Process,
) error {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	current, found := supervisor.running[process.Containment.FenceToken]
	if !found {
		return nil
	}
	if current.process != process {
		return runner.ErrStartFenced
	}
	delete(supervisor.running, process.Containment.FenceToken)
	close(current.done)
	return nil
}

func (supervisor *runtimeFaultSupervisor) Alive(
	process runner.Process,
) (bool, error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.aliveChecks++
	supervisor.lastAlive = process
	current, found := supervisor.running[process.Containment.FenceToken]
	if !found {
		return false, nil
	}
	if current.process != process {
		return false, runner.ErrStartFenced
	}
	return true, nil
}

func (supervisor *runtimeFaultSupervisor) Wait(
	ctx context.Context,
	process runner.Process,
) error {
	supervisor.mu.Lock()
	current, found := supervisor.running[process.Containment.FenceToken]
	if !found || current.process != process {
		supervisor.mu.Unlock()
		return nil
	}
	supervisor.waiters++
	done := current.done
	supervisor.mu.Unlock()
	defer func() {
		supervisor.mu.Lock()
		supervisor.waiters--
		supervisor.mu.Unlock()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (supervisor *runtimeFaultSupervisor) FinalizeCleanup(
	ctx context.Context,
	process runner.Process,
	root *os.Root,
	name string,
	_ runner.WorkspaceRef,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if alive, err := supervisor.Alive(process); err != nil || alive {
		return runner.ErrCleanupFailed
	}
	if err := root.RemoveAll(name); err != nil {
		return runner.ErrCleanupFailed
	}
	if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (*runtimeFaultSupervisor) GarbageCollectCleanup(
	context.Context,
	runner.Process,
) error {
	return nil
}

func (supervisor *runtimeFaultSupervisor) counts() (int, int, int) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.startRequests, supervisor.starts, supervisor.jitDeliveries
}

func (supervisor *runtimeFaultSupervisor) recoveryObservation() (int, runner.Process) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.aliveChecks, supervisor.lastAlive
}

func (supervisor *runtimeFaultSupervisor) waitForNoWaiters(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		supervisor.mu.Lock()
		waiters := supervisor.waiters
		supervisor.mu.Unlock()
		if waiters == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime waiters survived Agent shutdown: %d", waiters)
		}
		time.Sleep(time.Millisecond)
	}
}

func (supervisor *runtimeFaultSupervisor) waitForNoWaitersBestEffort() {
	deadline := time.Now().Add(time.Second)
	for {
		supervisor.mu.Lock()
		waiters := supervisor.waiters
		supervisor.mu.Unlock()
		if waiters == 0 || time.Now().After(deadline) {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func runtimeFaultMessage() *github.Message {
	return &github.Message{
		ScaleSetID: runtimeFaultScaleSetID,
		ID:         707,
		Statistics: github.Statistics{TotalAvailableJobs: 1},
		Jobs: []github.JobMessage{{
			Type:            github.MessageTypeJobAvailable,
			RunnerRequestID: runtimeFaultRunnerRequestID,
			RepositoryName:  "sparerunner-private",
			OwnerName:       "runtime-fault-owner",
			JobID:           "runtime-fault-job",
			WorkflowRunID:   707001,
		}},
	}
}

func runtimeFaultEmptyAgentSnapshot(nodeID domain.NodeID) app.AgentSnapshot {
	return app.AgentSnapshot{
		NodeID:            nodeID,
		OS:                domain.OSLinux,
		Arch:              domain.ArchAMD64,
		RunnerVersion:     runner.OfficialRunnerVersion,
		NativeRunnerReady: true,
	}
}

func runtimeFaultAgentSnapshot(
	t *testing.T,
	nodeID domain.NodeID,
	agentStore *store.AgentStore,
) app.AgentSnapshot {
	t.Helper()
	journal, err := agentStore.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := app.AgentSnapshot{
		NodeID:             nodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		RunnerVersion:      runner.OfficialRunnerVersion,
		NativeRunnerReady:  true,
		MaxControllerEpoch: journal.MaxControllerEpoch,
		Commands:           append([]domain.Command(nil), journal.Commands...),
	}
	for _, observation := range journal.Observations {
		snapshot.Observations = append(
			snapshot.Observations,
			transport.AgentExecutionObservation{
				ExecutionID:        observation.ExecutionID,
				State:              observation.State,
				ObservedAtUnixNano: observation.ObservedAtUnixNano,
			},
		)
	}
	for _, tombstone := range journal.CleanupTombstones {
		snapshot.CleanupTombstones = append(
			snapshot.CleanupTombstones,
			transport.AgentCleanupTombstone{
				ExecutionID:        tombstone.ExecutionID,
				FailureCode:        tombstone.FailureCode,
				RecordedAtUnixNano: tombstone.RecordedAtUnixNano,
			},
		)
	}
	return snapshot
}

func runtimeFaultControllerSnapshot(snapshot app.AgentSnapshot) store.NodeAgentSnapshot {
	journal := store.AgentSnapshot{
		MaxControllerEpoch: snapshot.MaxControllerEpoch,
		Commands:           append([]domain.Command(nil), snapshot.Commands...),
	}
	for _, observation := range snapshot.Observations {
		journal.Observations = append(journal.Observations, store.ObservationSnapshot{
			ExecutionID:        observation.ExecutionID,
			State:              observation.State,
			ObservedAtUnixNano: observation.ObservedAtUnixNano,
		})
	}
	for _, tombstone := range snapshot.CleanupTombstones {
		journal.CleanupTombstones = append(
			journal.CleanupTombstones,
			store.CleanupTombstoneSnapshot{
				ExecutionID:        tombstone.ExecutionID,
				FailureCode:        tombstone.FailureCode,
				RecordedAtUnixNano: tombstone.RecordedAtUnixNano,
			},
		)
	}
	return store.NodeAgentSnapshot{
		NodeID:            snapshot.NodeID,
		OS:                snapshot.OS,
		Architecture:      snapshot.Arch,
		RunnerVersion:     snapshot.RunnerVersion,
		NativeRunnerReady: snapshot.NativeRunnerReady,
		Journal:           journal,
	}
}

func runtimeFaultCloneSnapshot(snapshot app.AgentSnapshot) app.AgentSnapshot {
	snapshot.Commands = append([]domain.Command(nil), snapshot.Commands...)
	snapshot.Observations = append(
		[]transport.AgentExecutionObservation(nil),
		snapshot.Observations...,
	)
	snapshot.CleanupTombstones = append(
		[]transport.AgentCleanupTombstone(nil),
		snapshot.CleanupTombstones...,
	)
	return snapshot
}

func runtimeFaultSingleRecord(
	t *testing.T,
	agentStore *store.AgentStore,
) runner.VersionedRecord {
	t.Helper()
	records, err := agentStore.RunnerJournalRecords(context.Background())
	if err != nil || len(records) != 1 {
		t.Fatalf("runner journal = %#v, %v", records, err)
	}
	return records[0]
}

func runtimeFaultContainsAction(
	actions []reconcile.Action,
	kind reconcile.ActionKind,
) bool {
	for _, action := range actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}

func runtimeFaultAssertOneStart(t *testing.T, harness *runtimeFaultHarness) {
	t.Helper()
	generated, queries, removals := harness.lifecycle.counts()
	startRequests, starts, deliveries := harness.supervisor.counts()
	if generated != 1 || queries != 0 || removals != 0 ||
		harness.agent.startCount() != 1 ||
		startRequests != 1 || starts != 1 || deliveries != 1 {
		t.Fatalf(
			"recovery duplicated runner: jit=%d queries=%d removals=%d dispatch=%d attempts=%d starts=%d deliveries=%d",
			generated,
			queries,
			removals,
			harness.agent.startCount(),
			startRequests,
			starts,
			deliveries,
		)
	}
}
