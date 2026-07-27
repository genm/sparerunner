package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/reconcile"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

// disconnectLinearizationStore forces a disconnect after CommitMessage has
// read broker readiness but before the real SQLite claim transaction begins.
// WithoutCancel is intentional: cancellation is an additional fast-path guard,
// while this test must prove the durable revision CAS independently.
type disconnectLinearizationStore struct {
	*store.ControllerStore
	beforeCommit func(store.SingleSlotBinding)
	once         sync.Once
}

func (state *disconnectLinearizationStore) CommitGitHubQueueMessage(
	ctx context.Context,
	message store.GitHubQueueMessage,
	binding store.SingleSlotBinding,
) (store.GitHubMessageCommit, error) {
	state.once.Do(func() {
		if state.beforeCommit != nil {
			state.beforeCommit(binding)
		}
	})
	return state.ControllerStore.CommitGitHubQueueMessage(
		context.WithoutCancel(ctx),
		message,
		binding,
	)
}

func TestControllerRunnerDisconnectAfterReadinessBeforeClaimTransactionRejectsStaleAuthority(
	t *testing.T,
) {
	const (
		nodeID     domain.NodeID          = "00000000000000000000000000000001"
		targetID   domain.TargetID        = "target-disconnect-linearization"
		profileID  domain.RunnerProfileID = "profile-disconnect-linearization"
		scaleSetID store.ScaleSetID       = 7
	)
	ctx := context.Background()
	privateDirectory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(privateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	controllerStore, err := store.OpenController(
		ctx,
		filepath.Join(privateDirectory, "controller.db"),
		store.Options{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer controllerStore.Close()

	epoch, err := controllerStore.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	enrollOfflineCapacityNode(t, controllerStore, nodeID, epoch, 1)
	restart, err := controllerStore.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := reconcile.RestoreRestart(restart, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controllerStore.ConfigureRunnerProfile(
		ctx,
		store.RunnerProfileUpdatePolicy{
			ProfileID:     profileID,
			VersionPolicy: domain.RunnerVersionAutoUpdate,
			RunnerVersion: runner.OfficialRunnerVersion,
			Revision:      1,
		},
	); err != nil {
		t.Fatal(err)
	}
	runtimeBinding := store.GitHubTargetRuntimeBinding{
		TargetID:   targetID,
		ScaleSetID: scaleSetID,
		ProfileID:  profileID,
	}
	if _, err := controllerStore.ConfigureGitHubTargetRuntimeBinding(
		ctx,
		runtimeBinding,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := controllerStore.RecordGitHubScaleSetSessionSuccess(
		ctx,
		scaleSetID,
	); err != nil {
		t.Fatal(err)
	}

	consumers := newStoreBackedAgentConsumers(controllerStore, projection)
	snapshotConsumer, err := reconcile.NewSnapshotConsumer(
		controllerStore,
		projection,
	)
	if err != nil {
		t.Fatal(err)
	}
	consumers.Snapshot = snapshotConsumer
	broker := NewAgentBroker(epoch, consumers)
	defer broker.Close()
	agentSnapshot := AgentSnapshot{
		NodeID:             nodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		RunnerVersion:      runner.OfficialRunnerVersion,
		NativeRunnerReady:  true,
		MaxControllerEpoch: epoch,
	}
	agentSession, serveResult := startReadyBrokerSessionWithSnapshot(
		t,
		broker,
		agentSnapshot,
	)
	beforeDisconnect, err := controllerStore.ReadGitHubPollState(
		ctx,
		runtimeBinding,
		nodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !beforeDisconnect.ClaimAuthority.Agent.NativeRunnerReady {
		t.Fatalf("precondition Agent authority = %#v", beforeDisconnect.ClaimAuthority.Agent)
	}

	stateStore := &disconnectLinearizationStore{
		ControllerStore: controllerStore,
	}
	var authorityAtCommit store.GitHubAgentPollAuthority
	var afterDisconnect store.GitHubPollState
	stateStore.beforeCommit = func(binding store.SingleSlotBinding) {
		authorityAtCommit = binding.PollAuthority.Agent
		agentSession.disconnect()
		if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
			t.Fatalf("serve result during claim linearization = %v", err)
		}
		var readErr error
		afterDisconnect, readErr = controllerStore.ReadGitHubPollState(
			context.Background(),
			runtimeBinding,
			nodeID,
		)
		if readErr != nil {
			t.Fatal(readErr)
		}
	}
	githubSession := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	coordinator, err := NewControllerRunnerCoordinator(
		stateStore,
		githubSession,
		broker,
		newRunnerCoordinatorFakeLifecycle(),
		ControllerRunnerConfig{
			ScaleSetID:      github.ScaleSetID(scaleSetID),
			TargetID:        targetID,
			RunnerProfileID: profileID,
			VersionPolicy:   domain.RunnerVersionAutoUpdate,
			NodeID:          nodeID,
			ControllerEpoch: epoch,
			Reconciler:      projection,
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	if message, err := coordinator.PollOnce(ctx); !errors.Is(
		err,
		ErrGitHubAvailableUnclaimed,
	) || message != nil {
		t.Fatalf("poll across disconnect = (%#v, %v)", message, err)
	}
	if authorityAtCommit != beforeDisconnect.ClaimAuthority.Agent {
		t.Fatalf(
			"claim carried Agent authority %#v, want poll authority %#v",
			authorityAtCommit,
			beforeDisconnect.ClaimAuthority.Agent,
		)
	}
	if afterDisconnect.ClaimAuthority.Agent.Revision !=
		beforeDisconnect.ClaimAuthority.Agent.Revision+1 ||
		afterDisconnect.ClaimAuthority.Agent.NativeRunnerReady ||
		afterDisconnect.ClaimAuthority.Agent.SnapshotDigest !=
			beforeDisconnect.ClaimAuthority.Agent.SnapshotDigest {
		t.Fatalf(
			"disconnect authority transition = before %#v after %#v",
			beforeDisconnect.ClaimAuthority.Agent,
			afterDisconnect.ClaimAuthority.Agent,
		)
	}
	if _, found, err := controllerStore.GitHubClaim(
		ctx,
		scaleSetID,
		testControllerRunnerMessage().Jobs[0].RunnerRequestID,
	); err != nil || found {
		t.Fatalf("claim after stale authority = found:%t error:%v", found, err)
	}
	capacity, err := controllerStore.GitHubSingleSlotCapacity(
		ctx,
		store.SingleSlotBinding{
			TargetID: targetID,
			NodeID:   nodeID,
			Slot:     0,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if capacity != 1 || githubSession.deleteCalls != 0 ||
		githubSession.acquireCalls != 0 {
		t.Fatalf(
			"post-disconnect slot/ack/acquire = %d/%d/%d",
			capacity,
			githubSession.deleteCalls,
			githubSession.acquireCalls,
		)
	}
	admission, err := projection.Admission(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if admission.AllowsNewCapacity {
		t.Fatalf("disconnect projection retained admission: %#v", admission)
	}

	// The full journal identity remains last-known evidence; only liveness and
	// its revision change on disconnect.
	wantDigest, err := transport.AgentSnapshotDigest(agentSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if afterDisconnect.ClaimAuthority.Agent.SnapshotDigest != wantDigest {
		t.Fatalf(
			"disconnect replaced last-known journal digest = %q, want %q",
			afterDisconnect.ClaimAuthority.Agent.SnapshotDigest,
			wantDigest,
		)
	}
}
