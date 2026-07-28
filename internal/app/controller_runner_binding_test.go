package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
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

const (
	bindingNodeA      domain.NodeID          = "00000000000000000000000000000001"
	bindingNodeB      domain.NodeID          = "00000000000000000000000000000002"
	bindingScaleSetID store.ScaleSetID       = 7
	bindingTargetID   domain.TargetID        = "target-binding"
	bindingProfileID  domain.RunnerProfileID = "profile-binding"
)

// TestControllerRunnerAdvertisesSummedDemandAndSelectsLowestCandidateNode proves
// the coordinator is no longer pinned to one node: demand is the honest sum over
// every candidate, and the durable claim lands on the deterministically first
// candidate in NodeID order.
func TestControllerRunnerAdvertisesSummedDemandAndSelectsLowestCandidateNode(t *testing.T) {
	ctx := context.Background()
	fixture := newBindingFixture(t)
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	coordinator := fixture.coordinator(t, session)

	if _, err := coordinator.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := session.pollCapacities; len(got) != 1 || got[0] != 2 {
		t.Fatalf("advertised capacities = %v, want [2] summed over both nodes", got)
	}
	claim, found, err := fixture.store.GitHubClaim(ctx, bindingScaleSetID, 7001)
	if err != nil || !found {
		t.Fatalf("claim = (%#v, %t, %v)", claim, found, err)
	}
	if claim.Execution.Slot.NodeID != bindingNodeA {
		t.Fatalf(
			"claim node = %q, want the deterministically first candidate %q",
			claim.Execution.Slot.NodeID,
			bindingNodeA,
		)
	}
}

// TestControllerRunnerExcludedNodeLeavesCandidacyAndSelectionMovesOn proves that
// per-node exclusion is honoured through the store's own slot predicate: the
// excluded node contributes no demand and can never be selected.
func TestControllerRunnerExcludedNodeLeavesCandidacyAndSelectionMovesOn(t *testing.T) {
	ctx := context.Background()
	fixture := newBindingFixture(t)
	fixture.excludeTarget(t, bindingNodeA)

	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	coordinator := fixture.coordinator(t, session)
	if _, err := coordinator.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := session.pollCapacities; len(got) != 1 || got[0] != 1 {
		t.Fatalf("advertised capacities = %v, want [1] with one node excluded", got)
	}
	claim, found, err := fixture.store.GitHubClaim(ctx, bindingScaleSetID, 7001)
	if err != nil || !found {
		t.Fatalf("claim = (%#v, %t, %v)", claim, found, err)
	}
	if claim.Execution.Slot.NodeID != bindingNodeB {
		t.Fatalf("claim node = %q, want the only unexcluded node %q",
			claim.Execution.Slot.NodeID, bindingNodeB)
	}
}

// TestControllerRunnerReplayKeepsTheOriginalClaimNodeAcrossRestart is the
// correctness case that makes deterministic selection load-bearing. After the
// claim exists, its node has no free slot, so a naive re-selection would bind
// the redelivered message to the other node and the store would reject it as a
// replay mismatch.
func TestControllerRunnerReplayKeepsTheOriginalClaimNodeAcrossRestart(t *testing.T) {
	ctx := context.Background()
	fixture := newBindingFixture(t)
	first := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	if _, err := fixture.coordinator(t, first).PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	claim, found, err := fixture.store.GitHubClaim(ctx, bindingScaleSetID, 7001)
	if err != nil || !found || claim.Execution.Slot.NodeID != bindingNodeA {
		t.Fatalf("initial claim = (%#v, %t, %v)", claim, found, err)
	}
	if capacity, err := fixture.store.GitHubSingleSlotCapacity(ctx,
		store.SingleSlotBinding{TargetID: bindingTargetID, NodeID: bindingNodeA, Slot: 0},
	); err != nil || capacity != 0 {
		t.Fatalf("claimed node capacity = (%d, %v), want 0", capacity, err)
	}

	// A new coordinator over the same durable state is exactly what a restart
	// produces; the queue message is redelivered because the crash happened
	// between commit and acknowledgement.
	replaySession := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	if _, err := fixture.coordinator(t, replaySession).PollOnce(ctx); err != nil {
		t.Fatalf("redelivered message after restart: %v", err)
	}
	replayed, found, err := fixture.store.GitHubClaim(ctx, bindingScaleSetID, 7001)
	if err != nil || !found {
		t.Fatalf("replayed claim = (%#v, %t, %v)", replayed, found, err)
	}
	if replayed.Execution.Slot.NodeID != bindingNodeA ||
		replayed.Execution.ID != claim.Execution.ID {
		t.Fatalf("replayed claim moved: %#v, want %#v", replayed, claim)
	}
}

// TestControllerRunnerDispatchesToTheClaimOwningNode proves admission and
// dispatch read the node from the claim's own execution slot. A coordinator-wide
// node would have sent these commands to the wrong machine.
func TestControllerRunnerDispatchesToTheClaimOwningNode(t *testing.T) {
	ctx := context.Background()
	fixture := newBindingFixture(t)
	fixture.excludeTarget(t, bindingNodeA)
	session := newRunnerCoordinatorFakeSession(testControllerRunnerMessage())
	coordinator := fixture.coordinator(t, session)
	if _, err := coordinator.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	// DriveNext acquires the claim and dispatches Prepare. It cannot advance
	// past Prepare here on purpose: this fixture has no AgentBroker, so the
	// durable execution never leaves Reserved and the store fails the next
	// transition closed. The dispatch target is the evidence under test.
	if _, err := coordinator.DriveNext(ctx); err != nil &&
		!errors.Is(err, store.ErrGitHubClaimState) {
		t.Fatal(err)
	}
	prepared, started := fixture.agent.dispatched()
	if len(prepared) == 0 {
		t.Fatal("no Prepare was dispatched")
	}
	for _, nodeID := range append(append([]domain.NodeID(nil), prepared...), started...) {
		if nodeID != bindingNodeB {
			t.Fatalf(
				"command dispatched to %q, want the claim-owning node %q (prepare=%v start=%v)",
				nodeID, bindingNodeB, prepared, started,
			)
		}
	}
	// The admission read that authorized this dispatch must have been the claim
	// node's, so the excluded node must never have been asked to run anything.
	if fixture.agent.saw(bindingNodeA) {
		t.Fatalf("excluded node %q received a command", bindingNodeA)
	}
}

type bindingFixture struct {
	store      *store.ControllerStore
	projection *reconcile.Controller
	agent      *multiNodeFakeAgent
	epoch      domain.ControllerEpoch
}

func newBindingFixture(t *testing.T) *bindingFixture {
	t.Helper()
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
	t.Cleanup(func() { _ = controllerStore.Close() })

	firstEpoch, err := controllerStore.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	enrollOfflineCapacityNode(t, controllerStore, bindingNodeA, firstEpoch, 1)
	enrollOfflineCapacityNode(t, controllerStore, bindingNodeB, firstEpoch, 2)
	baseSnapshot := AgentSnapshot{
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		RunnerVersion:      runner.OfficialRunnerVersion,
		NativeRunnerReady:  true,
		MaxControllerEpoch: firstEpoch,
	}
	for _, nodeID := range []domain.NodeID{bindingNodeA, bindingNodeB} {
		snapshot := baseSnapshot
		snapshot.NodeID = nodeID
		recordOfflineCapacitySnapshot(t, controllerStore, snapshot)
	}

	epoch, err := controllerStore.AdvanceEpoch(ctx)
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
	agent := &multiNodeFakeAgent{
		snapshots: make(map[domain.NodeID]AgentSnapshot, 2),
	}
	for _, nodeID := range []domain.NodeID{bindingNodeA, bindingNodeB} {
		snapshot := baseSnapshot
		snapshot.NodeID = nodeID
		snapshot.MaxControllerEpoch = epoch
		recordOfflineCapacitySnapshot(t, controllerStore, snapshot)
		if _, err := projection.ReconcileAgentSnapshot(snapshot); err != nil {
			t.Fatal(err)
		}
		agent.snapshots[nodeID] = snapshot
	}

	if _, err := controllerStore.ConfigureRunnerProfile(
		ctx,
		store.RunnerProfileUpdatePolicy{
			ProfileID:     bindingProfileID,
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
			TargetID:   bindingTargetID,
			ScaleSetID: bindingScaleSetID,
			ProfileID:  bindingProfileID,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := controllerStore.RecordGitHubScaleSetSessionSuccess(
		ctx, bindingScaleSetID,
	); err != nil {
		t.Fatal(err)
	}
	return &bindingFixture{
		store:      controllerStore,
		projection: projection,
		agent:      agent,
		epoch:      epoch,
	}
}

func (fixture *bindingFixture) excludeTarget(t *testing.T, nodeID domain.NodeID) {
	t.Helper()
	digest, err := transport.AgentSnapshotDigest(fixture.agent.snapshots[nodeID])
	if err != nil {
		t.Fatal(err)
	}
	exclusions := []domain.TargetID{bindingTargetID}
	if err := fixture.store.RecordNodeOwnerState(
		context.Background(), nodeID, digest, "", &exclusions, nil,
	); err != nil {
		t.Fatal(err)
	}
}

// coordinator builds an unpinned coordinator: NodeID is intentionally empty so
// candidate resolution, not configuration, decides which node is bound.
func (fixture *bindingFixture) coordinator(
	t *testing.T,
	session *runnerCoordinatorFakeSession,
) *ControllerRunnerCoordinator {
	t.Helper()
	coordinator, err := NewControllerRunnerCoordinator(
		fixture.store,
		session,
		fixture.agent,
		newRunnerCoordinatorFakeLifecycle(),
		ControllerRunnerConfig{
			ScaleSetID:      github.ScaleSetID(bindingScaleSetID),
			TargetID:        bindingTargetID,
			Scope:           "example-org/tewake",
			ScopeKind:       domain.TargetRepository,
			RunnerProfileID: bindingProfileID,
			VersionPolicy:   domain.RunnerVersionAutoUpdate,
			ControllerEpoch: fixture.epoch,
			Reconciler:      fixture.projection,
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

// multiNodeFakeAgent answers readiness per node, which a single-snapshot fake
// cannot do and which every multi-node binding assertion depends on.
type multiNodeFakeAgent struct {
	mu           sync.Mutex
	snapshots    map[domain.NodeID]AgentSnapshot
	prepareNodes []domain.NodeID
	startNodes   []domain.NodeID
}

func (agent *multiNodeFakeAgent) saw(nodeID domain.NodeID) bool {
	prepared, started := agent.dispatched()
	return slices.Contains(prepared, nodeID) || slices.Contains(started, nodeID)
}

func (agent *multiNodeFakeAgent) dispatched() ([]domain.NodeID, []domain.NodeID) {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	return append([]domain.NodeID(nil), agent.prepareNodes...),
		append([]domain.NodeID(nil), agent.startNodes...)
}

func (agent *multiNodeFakeAgent) SendPrepare(
	_ context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	_ bool,
) (transport.ExecutionUpdate, error) {
	agent.mu.Lock()
	agent.prepareNodes = append(agent.prepareNodes, nodeID)
	agent.mu.Unlock()
	return transport.ExecutionUpdate{
		NodeID:      nodeID,
		CommandID:   metadata.CommandID,
		ExecutionID: metadata.ExecutionID,
		State:       domain.ExecutionPreparing,
	}, nil
}

func (agent *multiNodeFakeAgent) SendStart(
	_ context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	_ bool,
	_ runner.JITConfig,
) (transport.ExecutionUpdate, error) {
	agent.mu.Lock()
	agent.startNodes = append(agent.startNodes, nodeID)
	agent.mu.Unlock()
	return transport.ExecutionUpdate{
		NodeID:      nodeID,
		CommandID:   metadata.CommandID,
		ExecutionID: metadata.ExecutionID,
		State:       domain.ExecutionRunning,
	}, nil
}

func (agent *multiNodeFakeAgent) ReplayPrepare(
	ctx context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	disableUpdate bool,
	_ string,
) (transport.ExecutionUpdate, error) {
	return agent.SendPrepare(ctx, nodeID, metadata, disableUpdate)
}

func (agent *multiNodeFakeAgent) SendReconciliationCancel(
	_ context.Context,
	nodeID domain.NodeID,
	metadata transport.CommandMetadata,
	_ string,
) (transport.ExecutionUpdate, error) {
	return transport.ExecutionUpdate{
		NodeID:      nodeID,
		CommandID:   metadata.CommandID,
		ExecutionID: metadata.ExecutionID,
		State:       domain.ExecutionCleaning,
	}, nil
}

func (agent *multiNodeFakeAgent) Readiness(
	nodeID domain.NodeID,
) (AgentSnapshot, bool, context.Context) {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	snapshot, online := agent.snapshots[nodeID]
	return cloneAgentSnapshot(snapshot), online, context.Background()
}
