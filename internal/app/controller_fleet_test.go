package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/reconcile"
	"github.com/genm/sparerunner/internal/runner"
	"github.com/genm/sparerunner/internal/store"
)

const (
	fleetNodeID      domain.NodeID          = "00000000000000000000000000000011"
	fleetProfileID   domain.RunnerProfileID = "profile-fleet"
	fleetBoundTarget domain.TargetID        = "target-fleet-bound"
	fleetOpenTarget  domain.TargetID        = "target-fleet-unbound"
	fleetScaleSetID  store.ScaleSetID       = 61
	fleetOtherSetID  store.ScaleSetID       = 62
)

// TestControllerFleetRunsOneCoordinatorPerBoundTarget proves the fleet derives
// its coordinator set from durable state: a Target whose provisioning is not
// committed gets no session at all, and the bound Target gets exactly one.
func TestControllerFleetRunsOneCoordinatorPerBoundTarget(t *testing.T) {
	fixture := newFleetFixture(t, fleetBoundTarget, fleetOpenTarget)
	fixture.commitProvisioning(t, fleetBoundTarget, fleetScaleSetID)

	provider := newFleetFakeProvider()
	fleet := fixture.fleet(t, provider)
	stop := fixture.run(t, fleet)
	defer stop()

	provider.waitForOpen(t, fleetBoundTarget, 1)
	time.Sleep(50 * time.Millisecond)
	if opened := provider.openCount(fleetOpenTarget); opened != 0 {
		t.Fatalf("uncommitted target opened %d sessions, want 0", opened)
	}
	if opened := provider.openCount(fleetBoundTarget); opened != 1 {
		t.Fatalf("bound target opened %d sessions, want exactly 1", opened)
	}
}

// TestControllerFleetConfigurationChangeStopsRemovedTargetSession proves the
// fleet reacts to the same management invalidation signal the management UI
// uses, and that a removed Target's provider session is actually closed.
func TestControllerFleetConfigurationChangeStopsRemovedTargetSession(t *testing.T) {
	fixture := newFleetFixture(t, fleetBoundTarget, fleetOpenTarget)
	fixture.commitProvisioning(t, fleetBoundTarget, fleetScaleSetID)
	fixture.commitProvisioning(t, fleetOpenTarget, fleetOtherSetID)

	provider := newFleetFakeProvider()
	fleet := fixture.fleet(t, provider)
	// Only the invalidation signal may drive this test, never the safety-net
	// resync.
	fleet.resyncInterval = time.Hour
	stop := fixture.run(t, fleet)
	defer stop()

	provider.waitForOpen(t, fleetBoundTarget, 1)
	provider.waitForOpen(t, fleetOpenTarget, 1)

	fixture.removeTarget(t, fleetOpenTarget)
	fixture.publishManagementChange(t)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if provider.closeCount(fleetOpenTarget) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"removed target session closes = %d, want 1",
				provider.closeCount(fleetOpenTarget),
			)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if closed := provider.closeCount(fleetBoundTarget); closed != 0 {
		t.Fatalf("retained target session was closed %d times", closed)
	}
}

// TestControllerFleetRetriesFailingTargetWithoutStoppingOthers proves a Target
// that cannot open a session is retried with backoff rather than terminating
// the fleet or its healthy siblings.
func TestControllerFleetRetriesFailingTargetWithoutStoppingOthers(t *testing.T) {
	fixture := newFleetFixture(t, fleetBoundTarget, fleetOpenTarget)
	fixture.commitProvisioning(t, fleetBoundTarget, fleetScaleSetID)
	fixture.commitProvisioning(t, fleetOpenTarget, fleetOtherSetID)

	provider := newFleetFakeProvider()
	provider.failFor = fleetOpenTarget
	fleet := fixture.fleet(t, provider)
	fleet.retryInitial = 5 * time.Millisecond
	fleet.retryMaximum = 20 * time.Millisecond
	stop := fixture.run(t, fleet)
	defer stop()

	// The failing Target keeps retrying...
	provider.waitForOpen(t, fleetOpenTarget, 3)
	// ...while the healthy one is opened exactly once and stays open.
	provider.waitForOpen(t, fleetBoundTarget, 1)
	if opened := provider.openCount(fleetBoundTarget); opened != 1 {
		t.Fatalf("healthy target opened %d sessions, want 1", opened)
	}
	if closed := provider.closeCount(fleetBoundTarget); closed != 0 {
		t.Fatalf("healthy target session closed %d times during sibling failure", closed)
	}
}

// TestControllerFleetShutdownClosesEverySession proves shutdown is complete:
// every live session is closed and Run returns.
func TestControllerFleetShutdownClosesEverySession(t *testing.T) {
	fixture := newFleetFixture(t, fleetBoundTarget, fleetOpenTarget)
	fixture.commitProvisioning(t, fleetBoundTarget, fleetScaleSetID)
	fixture.commitProvisioning(t, fleetOpenTarget, fleetOtherSetID)

	provider := newFleetFakeProvider()
	fleet := fixture.fleet(t, provider)
	runContext, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(runContext) }()
	provider.waitForOpen(t, fleetBoundTarget, 1)
	provider.waitForOpen(t, fleetOpenTarget, 1)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("fleet shutdown = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fleet shutdown did not return")
	}
	for _, targetID := range []domain.TargetID{fleetBoundTarget, fleetOpenTarget} {
		if closed := provider.closeCount(targetID); closed != 1 {
			t.Fatalf("target %q closed %d sessions on shutdown, want 1", targetID, closed)
		}
	}
}

type fleetFixture struct {
	state    *ControllerState
	targets  []domain.TargetID
	snapshot AgentSnapshot
}

func newFleetFixture(t *testing.T, targetIDs ...domain.TargetID) *fleetFixture {
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
	enrollOfflineCapacityNode(t, controllerStore, fleetNodeID, firstEpoch, 3)
	snapshot := AgentSnapshot{
		NodeID:             fleetNodeID,
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		RunnerVersion:      runner.OfficialRunnerVersion,
		NativeRunnerReady:  true,
		MaxControllerEpoch: firstEpoch,
	}
	recordOfflineCapacitySnapshot(t, controllerStore, snapshot)
	epoch, err := controllerStore.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.MaxControllerEpoch = epoch
	recordOfflineCapacitySnapshot(t, controllerStore, snapshot)
	restartSnapshot, err := controllerStore.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := reconcile.RestoreRestart(restartSnapshot, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projection.ReconcileAgentSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}

	fixture := &fleetFixture{
		state: &ControllerState{
			Store:       controllerStore,
			Reconciler:  projection,
			AgentBroker: NewAgentBroker(epoch, newStoreBackedAgentConsumers(controllerStore)),
			Epoch:       uint64(epoch),
		},
		targets:  targetIDs,
		snapshot: snapshot,
	}
	fixture.applyConfiguration(t, targetIDs...)
	return fixture
}

func (fixture *fleetFixture) applyConfiguration(
	t *testing.T,
	targetIDs ...domain.TargetID,
) {
	t.Helper()
	ctx := context.Background()
	current, err := fixture.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	operatingSystem := domain.OSLinux
	architecture := domain.ArchAMD64
	profile := domain.RunnerProfile{
		ID:                      fleetProfileID,
		Label:                   "tewake-fleet",
		OS:                      &operatingSystem,
		Architecture:            &architecture,
		MinAvailableMemoryBytes: 4 << 30,
		VersionPolicy:           domain.RunnerVersionAutoUpdate,
		Runtime:                 domain.RuntimeNative,
	}
	desired := store.DesiredManagementConfiguration{
		Nodes: []store.ManagementNodeConfiguration{{
			NodeID:      fleetNodeID,
			DisplayName: "Fleet Desk",
			MaxRunners:  1,
		}},
		RunnerProfiles: []domain.RunnerProfile{profile},
	}
	verified := store.ManagementVerifiedAuthorities{
		RunnerProfiles: []store.ManagementRunnerProfile{{
			Profile:       profile,
			RunnerVersion: runner.OfficialRunnerVersion,
		}},
	}
	for _, targetID := range targetIDs {
		scaleSetID := fleetScaleSetIDFor(targetID)
		desired.GitHubTargets = append(desired.GitHubTargets, store.DesiredManagementGitHubTarget{
			ID:              targetID,
			InstallationID:  "41",
			ScopeKind:       domain.TargetOrganization,
			Scope:           "example-org",
			ScaleSetName:    string(targetID),
			RunnerProfileID: fleetProfileID,
		})
		verified.GitHubTargets = append(verified.GitHubTargets, store.ManagementGitHubTarget{
			Target: domain.GitHubTarget{
				ID:                    targetID,
				InstallationID:        "41",
				ScopeKind:             domain.TargetOrganization,
				Scope:                 "example-org",
				Visibility:            domain.TargetPrivate,
				RunnerGroupAccessSafe: true,
				ScaleSetName:          string(targetID),
				RunnerProfileID:       fleetProfileID,
			},
			ScaleSetID: scaleSetID,
		})
	}
	if _, err := fixture.state.Store.ApplyManagementConfiguration(
		ctx,
		current.Revision,
		desired,
		verified,
		store.AuditRecord{
			Actor:        store.AuditActorSingleAdmin,
			Action:       store.AuditActionConfigurationApplied,
			Outcome:      store.AuditOutcomeSucceeded,
			ResourceKind: store.AuditResourceConfiguration,
			RequestID:    "req_00000000000000000000000000000000",
		},
	); err != nil {
		t.Fatal(err)
	}
}

func (fixture *fleetFixture) removeTarget(t *testing.T, removed domain.TargetID) {
	t.Helper()
	remaining := make([]domain.TargetID, 0, len(fixture.targets))
	for _, targetID := range fixture.targets {
		if targetID != removed {
			remaining = append(remaining, targetID)
		}
	}
	fixture.targets = remaining
	fixture.applyConfiguration(t, remaining...)
}

// publishManagementChange fires the same in-memory projection signal a real
// configuration mutation publishes, which is what the fleet subscribes to.
func (fixture *fleetFixture) publishManagementChange(t *testing.T) {
	t.Helper()
	if _, err := fixture.state.Reconciler.ReconcileAgentSnapshot(
		fixture.snapshot,
	); err != nil {
		t.Fatal(err)
	}
}

func (fixture *fleetFixture) commitProvisioning(
	t *testing.T,
	targetID domain.TargetID,
	scaleSetID store.ScaleSetID,
) {
	t.Helper()
	ctx := context.Background()
	if err := fixture.state.Store.BeginGitHubTargetProvisioning(
		ctx,
		store.GitHubTargetProvisioningIntent{
			TargetID:       string(targetID),
			InstallationID: 41,
			ScopeKind:      "organization",
			Scope:          "example-org",
			ScaleSetName:   string(targetID),
			ProfileID:      string(fleetProfileID),
		},
	); err != nil {
		t.Fatal(err)
	}
	identifier := int64(scaleSetID)
	groupID := int64(9)
	if err := fixture.state.Store.UpdateGitHubTargetProvisioning(
		ctx, string(targetID), "ready", "", &identifier, &groupID,
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.state.Store.UpdateGitHubTargetProvisioning(
		ctx, string(targetID), "committed", "", &identifier, nil,
	); err != nil {
		t.Fatal(err)
	}
}

func (fixture *fleetFixture) fleet(
	t *testing.T,
	provider ControllerFleetProvider,
) *ControllerFleet {
	t.Helper()
	fleet, err := NewControllerFleet(fixture.state, provider, nil)
	if err != nil {
		t.Fatal(err)
	}
	fleet.resyncInterval = 25 * time.Millisecond
	fleet.retryInitial = 5 * time.Millisecond
	fleet.retryMaximum = 20 * time.Millisecond
	return fleet
}

func (fixture *fleetFixture) run(t *testing.T, fleet *ControllerFleet) func() {
	t.Helper()
	runContext, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(runContext) }()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fleet did not shut down")
		}
	}
}

func fleetScaleSetIDFor(targetID domain.TargetID) store.ScaleSetID {
	if targetID == fleetBoundTarget {
		return fleetScaleSetID
	}
	return fleetOtherSetID
}

var errFleetProviderRefused = errors.New("fleet provider refused")

type fleetFakeProvider struct {
	mu      sync.Mutex
	opens   map[domain.TargetID]int
	closes  map[domain.TargetID]int
	failFor domain.TargetID
}

func newFleetFakeProvider() *fleetFakeProvider {
	return &fleetFakeProvider{
		opens:  make(map[domain.TargetID]int),
		closes: make(map[domain.TargetID]int),
	}
}

func (provider *fleetFakeProvider) Open(
	_ context.Context,
	target ControllerFleetTarget,
) (ControllerFleetSession, ControllerRunnerLifecycle, error) {
	provider.mu.Lock()
	provider.opens[target.TargetID]++
	refused := provider.failFor == target.TargetID
	provider.mu.Unlock()
	if refused {
		return nil, nil, errFleetProviderRefused
	}
	return &fleetFakeSession{provider: provider, targetID: target.TargetID,
			scaleSetID: target.ScaleSetID},
		newRunnerCoordinatorFakeLifecycle(), nil
}

func (provider *fleetFakeProvider) openCount(targetID domain.TargetID) int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.opens[targetID]
}

func (provider *fleetFakeProvider) closeCount(targetID domain.TargetID) int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.closes[targetID]
}

func (provider *fleetFakeProvider) waitForOpen(
	t *testing.T,
	targetID domain.TargetID,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if provider.openCount(targetID) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"target %q opened %d sessions, want at least %d",
				targetID, provider.openCount(targetID), want,
			)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// fleetFakeSession blocks in Poll until its context ends, which is what a real
// long poll does and what makes "one live session per Target" observable.
type fleetFakeSession struct {
	provider   *fleetFakeProvider
	targetID   domain.TargetID
	scaleSetID github.ScaleSetID
}

func (session *fleetFakeSession) Snapshot() (github.SessionSnapshot, error) {
	return github.SessionSnapshot{
		ScaleSetID: session.scaleSetID,
		ID:         "session-" + string(session.targetID),
	}, nil
}

func (session *fleetFakeSession) Poll(
	ctx context.Context,
	_ int,
	_ int,
) (*github.Message, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (session *fleetFakeSession) DeleteMessage(context.Context, int) error {
	return nil
}

func (session *fleetFakeSession) AcquireJobs(
	_ context.Context,
	requestIDs []int64,
) ([]int64, error) {
	return requestIDs, nil
}

func (session *fleetFakeSession) Close(context.Context) error {
	session.provider.mu.Lock()
	session.provider.closes[session.targetID]++
	session.provider.mu.Unlock()
	return nil
}
