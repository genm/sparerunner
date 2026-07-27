package fault_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/enroll"
	"github.com/genm/tewake/internal/reconcile"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

const (
	faultExecutionID = domain.ExecutionID("execution-fault-a")
	faultTargetID    = domain.TargetID("target-fault-a")
)

func TestReconcileFaultHelperProcess(t *testing.T) {
	mode := os.Getenv("TEWAKE_FAULT_HELPER")
	if mode == "" {
		return
	}
	ctx := context.Background()
	switch mode {
	case "controller":
		controller, err := store.OpenController(ctx, os.Getenv("TEWAKE_FAULT_DB"), store.Options{})
		if err != nil {
			t.Fatal(err)
		}
		nodeID := domain.NodeID(os.Getenv("TEWAKE_FAULT_NODE"))
		recovered, err := reconcile.Start(ctx, controller, reconcile.Config{
			Nodes:    []reconcile.NodeDefinition{faultNodeDefinition(nodeID, 1)},
			Commands: []reconcile.IssuedCommand{faultPrepareAuthority(nodeID)},
		})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(os.Stdout, "READY epoch=%d\n", recovered.Epoch())
	case "agent":
		agent, err := store.OpenAgent(ctx, os.Getenv("TEWAKE_FAULT_DB"), store.Options{})
		if err != nil {
			t.Fatal(err)
		}
		command := faultStartCommand()
		if replayed, err := agent.RecordCommand(ctx, command); err != nil || replayed {
			t.Fatalf("record command = (%t, %v)", replayed, err)
		}
		if err := agent.RecordObservation(ctx, store.Observation{
			ExecutionID: command.ExecutionID,
			State:       domain.ExecutionRunning,
		}); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, "READY agent")
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
	if err := os.Stdout.Sync(); err != nil {
		t.Fatal(err)
	}
	// The parent keeps stdin open until SIGKILL. No store Close or test cleanup
	// runs, exercising committed WAL recovery rather than a graceful restart.
	_, _ = io.Copy(io.Discard, os.Stdin)
}

func TestControllerKillRestoresOneExecutionAndKeepsItZeroCapacity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateFaultDir(t), "controller.db")
	controller := openController(t, path)
	epoch, err := controller.AdvanceEpoch(ctx)
	if err != nil || epoch != 1 {
		t.Fatalf("initial epoch = %d, %v", epoch, err)
	}
	nodeID := enrollFaultNode(t, controller, epoch, 1)
	execution := faultExecution(nodeID, domain.ExecutionReserved)
	if _, replayed, err := controller.Assign(ctx, store.Assignment{
		ScaleSetID:    7,
		MessageID:     8,
		MessageDigest: domain.PayloadDigest([]byte("message-fault-a")),
		Execution:     execution,
	}); err != nil || replayed {
		t.Fatalf("assignment = (%t, %v)", replayed, err)
	}
	prepare := faultPrepareAuthority(nodeID)
	if replayed, err := controller.CommitAgentCommand(ctx, store.IssuedAgentCommand{
		NodeID:  prepare.NodeID,
		Type:    prepare.Type,
		Command: prepare.Command,
	}); err != nil || replayed {
		t.Fatalf("prepare authority = (%t, %v)", replayed, err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}

	killFaultHelper(t, "controller", path, nodeID)

	controller = openController(t, path)
	defer controller.Close()
	activeEpoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restartSnapshot, err := controller.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := reconcile.RestoreRestart(restartSnapshot, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Epoch() != activeEpoch || recovered.Epoch() != 3 {
		t.Fatalf("post-kill epoch = %d", recovered.Epoch())
	}
	fleet := recovered.FleetSnapshot()
	if len(fleet.Reservations) != 1 || len(fleet.SuppressedReservations) != 1 ||
		len(fleet.Nodes) != 0 || len(fleet.Statuses) != 1 {
		t.Fatalf("startup recovery = %#v", fleet)
	}
	result, err := recovered.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             nodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
		Commands:           []domain.Command{faultPrepareCommand()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Actions) != 1 ||
		result.Actions[0].Kind != reconcile.ActionReplayCommand ||
		result.Actions[0].CommandID != faultPrepareCommand().ID ||
		len(result.SuppressedReservations) != 1 {
		t.Fatalf("post-kill command recovery = %#v", result)
	}
	for _, action := range result.Actions {
		if action.Kind == reconcile.ActionIssuePrepare {
			t.Fatal("controller kill produced a second command identity")
		}
	}
	snapshot, err := controller.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Executions) != 1 || len(snapshot.Reservations) != 1 ||
		snapshot.Executions[0].ID != faultExecutionID {
		t.Fatalf("durable authority duplicated after kill: %#v", snapshot)
	}
}

func TestAgentKillReplaysAcceptedStartWithoutCreatingAnotherRuntime(t *testing.T) {
	path := filepath.Join(privateFaultDir(t), "agent.db")
	agent := openAgent(t, path)
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	killFaultHelper(t, "agent", path, "")

	agent = openAgent(t, path)
	defer agent.Close()
	journal, err := agent.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(journal.Commands) != 1 || len(journal.Observations) != 1 ||
		journal.Observations[0].State != domain.ExecutionRunning {
		t.Fatalf("agent kill lost journal authority: %#v", journal)
	}

	nodeID := domain.NodeID("00000000000000000000000000000001")
	execution := faultExecution(nodeID, domain.ExecutionPreparing)
	command := faultStartCommand()
	controller, err := reconcile.Restore(2, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: nodeID, State: domain.NodeActive}},
		Reservations: []store.SlotReservation{{
			Slot:  execution.Slot,
			Owner: domain.SlotOwner{TargetID: execution.TargetID, ExecutionID: execution.ID},
		}},
		Executions: []domain.ExecutionSnapshot{execution},
	}, reconcile.Config{
		Nodes: []reconcile.NodeDefinition{faultNodeDefinition(nodeID, 1)},
		Commands: []reconcile.IssuedCommand{{
			NodeID:  nodeID,
			Type:    domain.CommandStart,
			Command: command,
		}},
		GitHubFences: []reconcile.GitHubFence{{
			ExecutionID:     execution.ID,
			ScaleSetID:      7,
			RunnerRequestID: 8,
			ClaimState:      store.GitHubClaimStartAmbiguous,
			Attempt: &store.GitHubJITAttempt{
				ScaleSetID:      7,
				RunnerRequestID: 8,
				Attempt:         1,
				ControllerEpoch: 1,
				RunnerName:      "tewake-fault-a",
				State:           store.GitHubJITStartAmbiguous,
				RunnerID:        9,
				JITDigest:       domain.PayloadDigest([]byte("jit-fault-a")),
				StartCommandID:  command.ID,
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := controller.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             nodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: journal.MaxControllerEpoch,
		Commands:           journal.Commands,
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        journal.Observations[0].ExecutionID,
			State:              journal.Observations[0].State,
			ObservedAtUnixNano: journal.Observations[0].ObservedAtUnixNano,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Scheduler.ActiveExecutions) != 1 ||
		!containsFaultAction(result.Actions, reconcile.ActionConfirmAgentStartAccepted) ||
		containsFaultAction(result.Actions, reconcile.ActionIssuePrepare) ||
		containsFaultAction(result.Actions, reconcile.ActionObserveGitHubRunner) ||
		len(result.SuppressedReservations) != 1 {
		t.Fatalf("agent kill reconciliation = %#v", result)
	}
}

func TestOfflineNodeDoesNotBlockAnotherNodeAfterRealControllerRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateFaultDir(t), "controller.db")
	controller := openController(t, path)
	epoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nodeA := enrollFaultNode(t, controller, epoch, 1)
	nodeB := enrollFaultNode(t, controller, epoch, 2)
	for _, nodeID := range []domain.NodeID{nodeA, nodeB} {
		if err := controller.RecordAgentSnapshot(ctx, store.NodeAgentSnapshot{
			NodeID:            nodeID,
			OS:                domain.OSLinux,
			Architecture:      domain.ArchAMD64,
			NativeRunnerReady: true,
			Journal: store.AgentSnapshot{
				MaxControllerEpoch: epoch,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	controller = openController(t, path)
	defer controller.Close()
	if _, err := controller.AdvanceEpoch(ctx); err != nil {
		t.Fatal(err)
	}
	restartSnapshot, err := controller.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := reconcile.RestoreRestart(restartSnapshot, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             nodeA,
		OS:                 domain.OSWindows,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: epoch,
	}); err == nil {
		t.Fatal("invalid node A snapshot unexpectedly reconciled")
	}
	if _, err := recovered.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             nodeB,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: epoch,
	}); err != nil {
		t.Fatal(err)
	}
	fleet := recovered.FleetSnapshot()
	if fleet.Nodes[0].Node.ID != nodeA || fleet.Nodes[0].Reconciled ||
		fleet.Nodes[1].Node.ID != nodeB || !fleet.Nodes[1].Reconciled {
		t.Fatalf("independent post-restart capacity = %#v", fleet.Nodes)
	}
}

func TestCleanupTombstoneSurvivesBothJournalsAndKeepsNodeQuarantined(t *testing.T) {
	ctx := context.Background()
	directory := privateFaultDir(t)
	controllerPath := filepath.Join(directory, "controller.db")
	agentPath := filepath.Join(directory, "agent.db")
	controller := openController(t, controllerPath)
	epoch, err := controller.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := enrollFaultNode(t, controller, epoch, 1)
	execution := faultExecution(nodeID, domain.ExecutionReserved)
	if _, replayed, err := controller.Assign(ctx, store.Assignment{
		ScaleSetID:    7,
		MessageID:     8,
		MessageDigest: domain.PayloadDigest([]byte("cleanup-message")),
		Execution:     execution,
	}); err != nil || replayed {
		t.Fatalf("assignment = (%t, %v)", replayed, err)
	}

	agent := openAgent(t, agentPath)
	if err := agent.RecordCleanupTombstone(ctx, store.CleanupTombstone{
		ExecutionID: execution.ID,
		FailureCode: store.CleanupWorkspaceRemoval,
	}); err != nil {
		t.Fatal(err)
	}
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	agent = openAgent(t, agentPath)
	journal, err := agent.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	if len(journal.CleanupTombstones) != 1 {
		t.Fatalf("agent tombstone after restart = %#v", journal)
	}
	if err := controller.RecordAgentSnapshot(ctx, store.NodeAgentSnapshot{
		NodeID:            nodeID,
		OS:                domain.OSLinux,
		Architecture:      domain.ArchAMD64,
		NativeRunnerReady: true,
		Journal:           journal,
	}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}

	controller = openController(t, controllerPath)
	defer controller.Close()
	if _, err := controller.AdvanceEpoch(ctx); err != nil {
		t.Fatal(err)
	}
	restartSnapshot, err := controller.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := reconcile.RestoreRestart(restartSnapshot, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	startup := recovered.FleetSnapshot()
	if startup.Nodes[0].Node.AdministrativeState != domain.NodeQuarantined ||
		len(startup.SuppressedReservations) != 1 {
		t.Fatalf("controller tombstone after restart = %#v", startup)
	}
	result, err := recovered.ReconcileAgentSnapshot(transport.AgentSnapshot{
		NodeID:             nodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: journal.MaxControllerEpoch,
		CleanupTombstones: []transport.AgentCleanupTombstone{{
			ExecutionID:        journal.CleanupTombstones[0].ExecutionID,
			FailureCode:        journal.CleanupTombstones[0].FailureCode,
			RecordedAtUnixNano: journal.CleanupTombstones[0].RecordedAtUnixNano,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status.Phase != reconcile.NodeQuarantined ||
		result.Scheduler.Node.AdministrativeState != domain.NodeQuarantined ||
		result.Scheduler.Reconciled || len(result.SuppressedReservations) != 1 {
		t.Fatalf("reconciled tombstone = %#v", result)
	}
}

func TestGitHub5xxFaultRetainsLastKnownAndDisablesFreshness(t *testing.T) {
	cache, err := reconcile.NewLastKnown(func(value []string) []string {
		return append([]string(nil), value...)
	})
	if err != nil {
		t.Fatal(err)
	}
	first := time.Unix(100, 0).UTC()
	if err := cache.RecordSuccess([]string{"target-a"}, first); err != nil {
		t.Fatal(err)
	}
	want := &faultHTTPError{status: http.StatusServiceUnavailable}
	err = cache.Refresh(
		context.Background(),
		reconcile.GitHubObserverFunc[[]string](func(context.Context) ([]string, error) {
			return nil, want
		}),
		func(err error) (reconcile.GitHubFailure, bool) {
			var status *faultHTTPError
			if !errors.As(err, &status) {
				return reconcile.GitHubFailure{}, false
			}
			return reconcile.GitHubFailure{
				Kind:       reconcile.GitHubFailureServer,
				StatusCode: status.status,
			}, true
		},
		first.Add(time.Minute),
	)
	if !errors.Is(err, want) {
		t.Fatalf("5xx = %v", err)
	}
	snapshot := cache.Snapshot()
	if !snapshot.HasValue || !snapshot.Stale || len(snapshot.Value) != 1 ||
		snapshot.Value[0] != "target-a" {
		t.Fatalf("5xx state = %#v", snapshot)
	}
}

func killFaultHelper(
	t *testing.T,
	mode string,
	path string,
	nodeID domain.NodeID,
) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestReconcileFaultHelperProcess$")
	command.Env = append(os.Environ(),
		"TEWAKE_FAULT_HELPER="+mode,
		"TEWAKE_FAULT_DB="+path,
		"TEWAKE_FAULT_NODE="+string(nodeID),
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
		if !strings.HasPrefix(line, "READY ") {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf("helper failed before commit: line=%q stderr=%q", line, stderr.String())
		}
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("helper did not reach durable boundary: %s", stderr.String())
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = stdin.Close()
	if err := command.Wait(); err == nil {
		t.Fatal("fault helper exited gracefully instead of being killed")
	}
}

func privateFaultDir(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func openController(t *testing.T, path string) *store.ControllerStore {
	t.Helper()
	controller, err := store.OpenController(context.Background(), path, store.Options{})
	if err != nil {
		if controller != nil {
			_ = controller.Close()
		}
		t.Fatal(err)
	}
	return controller
}

func openAgent(t *testing.T, path string) *store.AgentStore {
	t.Helper()
	agent, err := store.OpenAgent(context.Background(), path, store.Options{})
	if err != nil {
		if agent != nil {
			_ = agent.Close()
		}
		t.Fatal(err)
	}
	return agent
}

func enrollFaultNode(
	t *testing.T,
	controller *store.ControllerStore,
	epoch domain.ControllerEpoch,
	seed byte,
) domain.NodeID {
	t.Helper()
	var token enroll.TokenRecord
	token.ID[0] = seed
	token.SecretDigest[0] = seed + 1
	token.Epoch = uint64(epoch)
	if err := controller.CreateToken(context.Background(), token); err != nil {
		t.Fatal(err)
	}
	nodeID := domain.NodeID(fmt.Sprintf("%032x", seed))
	now := time.Now().UTC()
	var publicKeyDigest [32]byte
	publicKeyDigest[0] = seed
	node := enroll.NodeRecord{
		NodeID: string(nodeID),
		Credential: enroll.Credential{
			NodeID:    string(nodeID),
			Serial:    fmt.Sprintf("%032x", seed),
			Epoch:     1,
			NotBefore: now.Add(-time.Minute),
			NotAfter:  now.Add(time.Hour),
		},
		PublicKeyDigest:  publicKeyDigest,
		CertificateDER:   []byte{seed},
		CACertificateDER: []byte{seed + 1},
	}
	if err := controller.ConsumeEnrollment(context.Background(), token, node); err != nil {
		t.Fatal(err)
	}
	return nodeID
}

func faultNodeDefinition(nodeID domain.NodeID, maxRunners int) reconcile.NodeDefinition {
	return reconcile.NodeDefinition{
		Node: domain.Node{
			ID:                  nodeID,
			DisplayName:         string(nodeID),
			OS:                  domain.OSLinux,
			Architecture:        domain.ArchAMD64,
			MaxRunners:          maxRunners,
			AdministrativeState: domain.NodeActive,
			ObservedState:       domain.NodeOffline,
		},
		RunnerVersionPolicy: domain.RunnerVersionAutoUpdate,
		RunnerUpdate:        reconcile.ManagedRunnerUpdate(),
	}
}

func faultExecution(
	nodeID domain.NodeID,
	state domain.ExecutionState,
) domain.ExecutionSnapshot {
	return domain.ExecutionSnapshot{
		ID:       faultExecutionID,
		TargetID: faultTargetID,
		Slot:     domain.SlotKey{NodeID: nodeID, Index: 0},
		State:    state,
	}
}

func faultPrepareCommand() domain.Command {
	return domain.Command{
		ID:              "prepare-fault-a",
		ControllerEpoch: 1,
		ExecutionID:     faultExecutionID,
		ExpectedState:   domain.ExecutionReserved,
		PayloadDigest:   domain.PayloadDigest([]byte("prepare-fault-a")),
	}
}

func faultPrepareAuthority(nodeID domain.NodeID) reconcile.IssuedCommand {
	return reconcile.IssuedCommand{
		NodeID:  nodeID,
		Type:    domain.CommandPrepare,
		Command: faultPrepareCommand(),
	}
}

func faultStartCommand() domain.Command {
	return domain.Command{
		ID:              "start-fault-a",
		ControllerEpoch: 1,
		ExecutionID:     faultExecutionID,
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   domain.PayloadDigest([]byte("start-fault-a")),
	}
}

func containsFaultAction(actions []reconcile.Action, kind reconcile.ActionKind) bool {
	for _, action := range actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}

type faultHTTPError struct{ status int }

func (err *faultHTTPError) Error() string { return http.StatusText(err.status) }
