package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/enroll"
	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/reconcile"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

func TestControllerRunnerOfflineNodeDoesNotBlockFreshNodeCapacityAdvertisement(
	t *testing.T,
) {
	const (
		nodeA domain.NodeID = "00000000000000000000000000000001"
		nodeB domain.NodeID = "00000000000000000000000000000002"

		scaleSetID store.ScaleSetID       = 7
		targetID   domain.TargetID        = "target-offline-capacity"
		profileID  domain.RunnerProfileID = "profile-offline-capacity"
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

	firstEpoch, err := controllerStore.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	enrollOfflineCapacityNode(t, controllerStore, nodeA, firstEpoch, 1)
	enrollOfflineCapacityNode(t, controllerStore, nodeB, firstEpoch, 2)

	oldSnapshotA := AgentSnapshot{
		NodeID:             nodeA,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		RunnerVersion:      runner.OfficialRunnerVersion,
		NativeRunnerReady:  true,
		MaxControllerEpoch: firstEpoch,
	}
	oldSnapshotB := oldSnapshotA
	oldSnapshotB.NodeID = nodeB
	recordOfflineCapacitySnapshot(t, controllerStore, oldSnapshotA)
	recordOfflineCapacitySnapshot(t, controllerStore, oldSnapshotB)

	secondEpoch, err := controllerStore.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restartSnapshot, err := controllerStore.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := reconcile.RestoreRestart(restartSnapshot, time.Now)
	if err != nil {
		t.Fatal(err)
	}

	// Node A did reconcile in this Controller epoch before its authenticated
	// session disappeared. The disconnect revokes only its exact durable
	// readiness generation; it must not turn another node's capacity into a
	// cluster-wide barrier.
	freshSnapshotA := oldSnapshotA
	freshSnapshotA.MaxControllerEpoch = secondEpoch
	recordOfflineCapacitySnapshot(t, controllerStore, freshSnapshotA)
	if _, err := projection.ReconcileAgentSnapshot(freshSnapshotA); err != nil {
		t.Fatal(err)
	}
	freshDigestA, err := transport.AgentSnapshotDigest(freshSnapshotA)
	if err != nil {
		t.Fatal(err)
	}
	if err := controllerStore.RecordAgentDisconnect(
		ctx,
		nodeA,
		freshDigestA,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Disconnect(nodeA); err != nil {
		t.Fatal(err)
	}

	freshSnapshotB := oldSnapshotB
	freshSnapshotB.MaxControllerEpoch = secondEpoch
	recordOfflineCapacitySnapshot(t, controllerStore, freshSnapshotB)
	if _, err := projection.ReconcileAgentSnapshot(freshSnapshotB); err != nil {
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
	if _, err := controllerStore.ConfigureGitHubTargetRuntimeBinding(
		ctx,
		store.GitHubTargetRuntimeBinding{
			TargetID:   targetID,
			ScaleSetID: scaleSetID,
			ProfileID:  profileID,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := controllerStore.RecordGitHubScaleSetSessionSuccess(
		ctx,
		scaleSetID,
	); err != nil {
		t.Fatal(err)
	}

	offlineSession := newRunnerCoordinatorFakeSession(nil)
	offlineAgent := &runnerCoordinatorFakeAgent{
		snapshot:       oldSnapshotA,
		snapshotOnline: false,
	}
	offlineCoordinator := newOfflineCapacityCoordinator(
		t,
		controllerStore,
		offlineSession,
		offlineAgent,
		projection,
		nodeA,
		secondEpoch,
		targetID,
		profileID,
		scaleSetID,
	)
	if message, err := offlineCoordinator.PollOnce(ctx); err != nil || message != nil {
		t.Fatalf("offline node poll = (%#v, %v)", message, err)
	}

	freshSession := newRunnerCoordinatorFakeSession(nil)
	freshAgent := &runnerCoordinatorFakeAgent{
		snapshot:       freshSnapshotB,
		snapshotOnline: true,
	}
	freshCoordinator := newOfflineCapacityCoordinator(
		t,
		controllerStore,
		freshSession,
		freshAgent,
		projection,
		nodeB,
		secondEpoch,
		targetID,
		profileID,
		scaleSetID,
	)
	freshAdmission, err := projection.Admission(nodeB)
	if err != nil {
		t.Fatal(err)
	}
	if !freshAdmission.AllowsNewCapacity {
		t.Fatalf("fresh node projection admission = %#v", freshAdmission)
	}
	freshPollState, err := controllerStore.ReadGitHubPollState(
		ctx,
		store.GitHubTargetRuntimeBinding{
			TargetID:   targetID,
			ScaleSetID: scaleSetID,
			ProfileID:  profileID,
		},
		nodeB,
	)
	if err != nil {
		t.Fatal(err)
	}
	freshDigest, err := transport.AgentSnapshotDigest(freshSnapshotB)
	if err != nil {
		t.Fatal(err)
	}
	freshSlotCapacity, err := controllerStore.GitHubSingleSlotCapacity(
		ctx,
		store.SingleSlotBinding{TargetID: targetID, NodeID: nodeB, Slot: 0},
	)
	if err != nil {
		t.Fatal(err)
	}
	if freshSlotCapacity != 1 ||
		freshPollState.ClaimAuthority.Agent.SnapshotDigest != freshDigest ||
		freshPollState.ClaimAuthority.Agent.AcceptedByControllerEpoch != secondEpoch {
		t.Fatalf(
			"fresh durable poll authority = capacity %d, state %#v, digest %q",
			freshSlotCapacity,
			freshPollState,
			freshDigest,
		)
	}
	if message, err := freshCoordinator.PollOnce(ctx); err != nil || message != nil {
		t.Fatalf("fresh node poll = (%#v, %v)", message, err)
	}

	if got := append([]int(nil), offlineSession.pollCapacities...); len(got) != 1 ||
		got[0] != 0 {
		t.Fatalf("offline node advertised capacities = %v, want [0]", got)
	}
	if got := append([]int(nil), freshSession.pollCapacities...); len(got) != 1 ||
		got[0] != 1 {
		t.Fatalf(
			"fresh node advertised capacities after offline node poll = %v, want [1]",
			got,
		)
	}
}

func enrollOfflineCapacityNode(
	t *testing.T,
	controllerStore *store.ControllerStore,
	nodeID domain.NodeID,
	epoch domain.ControllerEpoch,
	seed byte,
) {
	t.Helper()
	var token enroll.TokenRecord
	token.ID[len(token.ID)-1] = seed
	token.SecretDigest[len(token.SecretDigest)-1] = seed
	token.Epoch = uint64(epoch)
	if err := controllerStore.CreateToken(context.Background(), token); err != nil {
		t.Fatal(err)
	}
	var publicKeyDigest [32]byte
	publicKeyDigest[len(publicKeyDigest)-1] = seed
	now := time.Now()
	if err := controllerStore.ConsumeEnrollment(
		context.Background(),
		token,
		enroll.NodeRecord{
			NodeID: string(nodeID),
			Credential: enroll.Credential{
				NodeID:    string(nodeID),
				Serial:    "a" + string(rune('0'+seed)),
				Epoch:     uint64(epoch),
				NotBefore: now.Add(-time.Minute),
				NotAfter:  now.Add(time.Hour),
			},
			PublicKeyDigest:  publicKeyDigest,
			CertificateDER:   []byte{seed},
			CACertificateDER: []byte{seed + 1},
		},
	); err != nil {
		t.Fatal(err)
	}
}

func recordOfflineCapacitySnapshot(
	t *testing.T,
	controllerStore *store.ControllerStore,
	snapshot AgentSnapshot,
) {
	t.Helper()
	if err := controllerStore.RecordAgentSnapshot(
		context.Background(),
		store.NodeAgentSnapshot{
			NodeID:            snapshot.NodeID,
			OS:                snapshot.OS,
			Architecture:      snapshot.Arch,
			RunnerVersion:     snapshot.RunnerVersion,
			NativeRunnerReady: snapshot.NativeRunnerReady,
			Journal: store.AgentSnapshot{
				MaxControllerEpoch: snapshot.MaxControllerEpoch,
			},
		},
	); err != nil {
		t.Fatal(err)
	}
}

func newOfflineCapacityCoordinator(
	t *testing.T,
	controllerStore *store.ControllerStore,
	session *runnerCoordinatorFakeSession,
	agent *runnerCoordinatorFakeAgent,
	projection *reconcile.Controller,
	nodeID domain.NodeID,
	epoch domain.ControllerEpoch,
	targetID domain.TargetID,
	profileID domain.RunnerProfileID,
	scaleSetID store.ScaleSetID,
) *ControllerRunnerCoordinator {
	t.Helper()
	coordinator, err := NewControllerRunnerCoordinator(
		controllerStore,
		session,
		agent,
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
	return coordinator
}
