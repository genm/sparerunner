package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

type recordingControllerAgentStore struct {
	commands         []store.IssuedAgentCommand
	snapshots        []store.NodeAgentSnapshot
	readinessNode    domain.NodeID
	readinessDigest  string
	readiness        bool
	disconnectNode   domain.NodeID
	disconnectDigest string
	updates          []store.AgentExecutionUpdate
	err              error

	mu                     sync.Mutex
	configuration          store.ManagementConfiguration
	revisionReads          int
	fullConfigurationReads int
}

func (recording *recordingControllerAgentStore) ManagementConfigurationRevision(context.Context) (uint64, error) {
	recording.mu.Lock()
	defer recording.mu.Unlock()
	recording.revisionReads++
	if recording.err != nil {
		return 0, recording.err
	}
	return recording.configuration.Revision, nil
}

func (recording *recordingControllerAgentStore) ReadManagementConfiguration(context.Context) (store.ManagementConfiguration, error) {
	recording.mu.Lock()
	defer recording.mu.Unlock()
	recording.fullConfigurationReads++
	if recording.err != nil {
		return store.ManagementConfiguration{}, recording.err
	}
	return recording.configuration, nil
}

func (recording *recordingControllerAgentStore) counts() (revisionReads, fullReads int) {
	recording.mu.Lock()
	defer recording.mu.Unlock()
	return recording.revisionReads, recording.fullConfigurationReads
}

func (recording *recordingControllerAgentStore) setConfiguration(configuration store.ManagementConfiguration) {
	recording.mu.Lock()
	defer recording.mu.Unlock()
	recording.configuration = configuration
}

func (recording *recordingControllerAgentStore) CommitAgentCommand(_ context.Context, command store.IssuedAgentCommand) (bool, error) {
	recording.commands = append(recording.commands, command)
	return false, recording.err
}

func (recording *recordingControllerAgentStore) ReplayAgentCommand(
	_ context.Context,
	command store.IssuedAgentCommand,
	_ string,
) (bool, error) {
	recording.commands = append(recording.commands, command)
	return true, recording.err
}

func (recording *recordingControllerAgentStore) CommitAgentReconciliationCommand(
	_ context.Context,
	command store.IssuedAgentCommand,
	_ string,
) (bool, error) {
	recording.commands = append(recording.commands, command)
	return false, recording.err
}

func (recording *recordingControllerAgentStore) AgentCommandIsReconciliation(
	context.Context,
	domain.CommandID,
) (bool, error) {
	return false, recording.err
}

func (recording *recordingControllerAgentStore) RecordAgentSnapshot(_ context.Context, snapshot store.NodeAgentSnapshot) error {
	recording.snapshots = append(recording.snapshots, snapshot)
	return recording.err
}

func (recording *recordingControllerAgentStore) RecordAgentReadiness(
	_ context.Context,
	nodeID domain.NodeID,
	snapshotDigest string,
	ready bool,
) error {
	recording.readinessNode = nodeID
	recording.readinessDigest = snapshotDigest
	recording.readiness = ready
	return recording.err
}

func (recording *recordingControllerAgentStore) RecordAgentDisconnect(
	_ context.Context,
	nodeID domain.NodeID,
	snapshotDigest string,
) error {
	recording.disconnectNode = nodeID
	recording.disconnectDigest = snapshotDigest
	return recording.err
}

func (recording *recordingControllerAgentStore) RecordAgentExecutionUpdate(_ context.Context, update store.AgentExecutionUpdate) (bool, error) {
	recording.updates = append(recording.updates, update)
	return false, recording.err
}

func TestStoreBackedAgentConsumersMapOnlyNonSecretDurableFields(t *testing.T) {
	recording := &recordingControllerAgentStore{}
	consumers := newStoreBackedAgentConsumers(recording)
	var commandDigest [32]byte
	commandDigest[0], commandDigest[31] = 0x01, 0xfe
	command := AgentCommandRecord{
		NodeID: "node-agent",
		Kind:   transport.MessageStart,
		Metadata: transport.CommandMetadata{
			CommandID:       "command-agent",
			ControllerEpoch: 7,
			ExecutionID:     "execution-agent",
			ExpectedState:   domain.ExecutionPreparing,
		},
		PayloadDigest: commandDigest,
	}
	if err := consumers.Commands.HandleAgentCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	wantCommand := store.IssuedAgentCommand{
		NodeID: "node-agent",
		Type:   domain.CommandStart,
		Command: domain.Command{
			ID:              "command-agent",
			ControllerEpoch: 7,
			ExecutionID:     "execution-agent",
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   "01" + strings.Repeat("00", 30) + "fe",
		},
	}
	if !reflect.DeepEqual(recording.commands, []store.IssuedAgentCommand{wantCommand}) {
		t.Fatalf("mapped command = %+v, want %+v", recording.commands, wantCommand)
	}

	snapshot := AgentSnapshot{
		NodeID:             "node-agent",
		OS:                 "linux",
		Arch:               "arm64",
		NativeRunnerReady:  true,
		MaxControllerEpoch: 7,
		Commands:           []domain.Command{wantCommand.Command},
		Observations: []transport.AgentExecutionObservation{{
			ExecutionID:        "execution-agent",
			State:              domain.ExecutionRunning,
			ObservedAtUnixNano: 10,
		}},
		CleanupTombstones: []transport.AgentCleanupTombstone{{
			ExecutionID:        "execution-old",
			FailureCode:        domain.CleanupProcessResidue,
			RecordedAtUnixNano: 9,
		}},
	}
	if err := consumers.Snapshot.HandleAgentSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if len(recording.snapshots) != 1 {
		t.Fatalf("snapshot calls = %d, want 1", len(recording.snapshots))
	}
	gotSnapshot := recording.snapshots[0]
	if gotSnapshot.NodeID != "node-agent" || gotSnapshot.OS != domain.OSLinux ||
		gotSnapshot.Architecture != domain.ArchARM64 ||
		!gotSnapshot.NativeRunnerReady ||
		!reflect.DeepEqual(gotSnapshot.Journal.Commands, snapshot.Commands) ||
		len(gotSnapshot.Journal.Observations) != 1 ||
		len(gotSnapshot.Journal.CleanupTombstones) != 1 {
		t.Fatalf("mapped snapshot = %+v", gotSnapshot)
	}
	// The adapter owns a copy rather than retaining broker slice storage.
	snapshot.Commands[0].PayloadDigest = strings.Repeat("f", 64)
	if gotSnapshot.Journal.Commands[0].PayloadDigest == snapshot.Commands[0].PayloadDigest {
		t.Fatal("snapshot command slice aliases the broker payload")
	}
	readinessDigest := strings.Repeat("a", 64)
	if err := consumers.Readiness.HandleAgentReadiness(
		context.Background(), "node-agent", readinessDigest, true,
	); err != nil {
		t.Fatal(err)
	}
	if recording.readinessNode != "node-agent" ||
		recording.readinessDigest != readinessDigest || !recording.readiness {
		t.Fatalf("mapped readiness = (%q, %q, %t)",
			recording.readinessNode, recording.readinessDigest, recording.readiness)
	}
	disconnect := AgentDisconnectRecord{
		NodeID:         "node-agent",
		SnapshotDigest: strings.Repeat("b", 64),
	}
	if err := consumers.Disconnects.HandleAgentDisconnect(
		context.Background(),
		disconnect,
	); err != nil {
		t.Fatal(err)
	}
	if recording.disconnectNode != disconnect.NodeID ||
		recording.disconnectDigest != disconnect.SnapshotDigest {
		t.Fatalf(
			"mapped disconnect = node:%s digest:%s",
			recording.disconnectNode,
			recording.disconnectDigest,
		)
	}

	var updateDigest [32]byte
	updateDigest[0] = 0xab
	update := AgentExecutionUpdateRecord{
		MessageID: "update-agent",
		Update: transport.ExecutionUpdate{
			NodeID:      "node-agent",
			CommandID:   "command-agent",
			ExecutionID: "execution-agent",
			State:       domain.ExecutionRunning,
			Replayed:    true,
		},
		PayloadDigest: updateDigest,
	}
	if err := consumers.ExecutionUpdates.HandleExecutionUpdate(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if len(recording.updates) != 1 || recording.updates[0].PayloadDigest != "ab"+strings.Repeat("00", 31) ||
		recording.updates[0].MessageID != update.MessageID || recording.updates[0].ErrorCode != domain.ExecutionErrorNone {
		t.Fatalf("mapped update = %+v", recording.updates)
	}
}

func TestStoreBackedAgentConsumersFailClosedOnUnsupportedKindAndStoreFailure(t *testing.T) {
	recording := &recordingControllerAgentStore{}
	consumers := newStoreBackedAgentConsumers(recording)
	if err := consumers.Commands.HandleAgentCommand(context.Background(), AgentCommandRecord{
		NodeID: "node-agent",
		Kind:   transport.MessageHeartbeat,
	}); err == nil {
		t.Fatal("unsupported message kind reached durable command storage")
	}
	if len(recording.commands) != 0 {
		t.Fatal("unsupported message kind mutated durable storage")
	}

	sentinel := errors.New("durable store unavailable")
	recording.err = sentinel
	if err := consumers.ExecutionUpdates.HandleExecutionUpdate(context.Background(), AgentExecutionUpdateRecord{
		MessageID: "update-agent",
		Update: transport.ExecutionUpdate{
			NodeID:      "node-agent",
			CommandID:   "command-agent",
			ExecutionID: "execution-agent",
			State:       domain.ExecutionRunning,
		},
	}); !errors.Is(err, sentinel) {
		t.Fatalf("store failure = %v, want sentinel", err)
	}

	nilConsumers := newStoreBackedAgentConsumers(nil)
	if err := nilConsumers.Snapshot.HandleAgentSnapshot(context.Background(), AgentSnapshot{}); !errors.Is(err, ErrAgentSnapshotConsumerRequired) {
		t.Fatalf("nil store snapshot = %v", err)
	}
	if err := nilConsumers.Readiness.HandleAgentReadiness(
		context.Background(), "node-agent", strings.Repeat("a", 64), false,
	); !errors.Is(err, ErrAgentReadinessConsumerRequired) {
		t.Fatalf("nil store readiness = %v", err)
	}
	if err := nilConsumers.Disconnects.HandleAgentDisconnect(
		context.Background(),
		AgentDisconnectRecord{
			NodeID:         "node-agent",
			SnapshotDigest: strings.Repeat("a", 64),
		},
	); !errors.Is(err, ErrAgentReadinessConsumerRequired) {
		t.Fatalf("nil store disconnect = %v", err)
	}
}

func operatingSystemPtr(value domain.OperatingSystem) *domain.OperatingSystem { return &value }
func architecturePtr(value domain.Architecture) *domain.Architecture          { return &value }

func eligibilityTestConfiguration(revision uint64) store.ManagementConfiguration {
	return store.ManagementConfiguration{
		Revision: revision,
		RunnerProfiles: []store.ManagementRunnerProfile{
			{Profile: domain.RunnerProfile{
				ID: "profile-linux-amd64", Label: "linux-amd64",
				OS: operatingSystemPtr(domain.OSLinux), Architecture: architecturePtr(domain.ArchAMD64),
			}},
			{Profile: domain.RunnerProfile{
				ID: "profile-windows-arm64", Label: "windows-arm64",
				OS: operatingSystemPtr(domain.OSWindows), Architecture: architecturePtr(domain.ArchARM64),
			}},
			{Profile: domain.RunnerProfile{ID: "profile-any", Label: "any"}},
		},
		GitHubTargets: []store.ManagementGitHubTarget{
			{Target: domain.GitHubTarget{
				ID: "target-b", ScopeKind: domain.TargetRepository, Scope: "owner/b",
				ScaleSetName: "scale-b", RunnerProfileID: "profile-linux-amd64",
			}},
			{Target: domain.GitHubTarget{
				ID: "target-a", ScopeKind: domain.TargetOrganization, Scope: "owner",
				ScaleSetName: "scale-a", RunnerProfileID: "profile-any",
			}},
			{Target: domain.GitHubTarget{
				ID: "target-windows-only", ScopeKind: domain.TargetRepository, Scope: "owner/c",
				ScaleSetName: "scale-c", RunnerProfileID: "profile-windows-arm64",
			}},
		},
	}
}

func TestEligibleTargetsFiltersByPlatformAndOrdersDeterministically(t *testing.T) {
	recording := &recordingControllerAgentStore{}
	recording.setConfiguration(eligibilityTestConfiguration(1))
	consumers := newStoreBackedAgentConsumers(recording)

	eligible, err := consumers.Eligibility.EligibleTargets(
		context.Background(), domain.OSLinux, domain.ArchAMD64,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(eligible) != 2 || eligible[0].TargetID != "target-a" || eligible[1].TargetID != "target-b" {
		t.Fatalf("eligible targets = %#v, want [target-a target-b] in that order", eligible)
	}
	for _, target := range eligible {
		if target.Excluded {
			t.Fatalf("controller adoption of exclusion has not landed yet: %#v", target)
		}
	}

	// A windows/arm64 node sees the unconstrained target plus the target whose
	// profile is constrained to exactly its platform, and not the
	// linux/amd64-constrained target.
	windowsEligible, err := consumers.Eligibility.EligibleTargets(
		context.Background(), domain.OSWindows, domain.ArchARM64,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(windowsEligible) != 2 ||
		windowsEligible[0].TargetID != "target-a" ||
		windowsEligible[1].TargetID != "target-windows-only" {
		t.Fatalf("windows/arm64 eligible targets = %#v, want [target-a target-windows-only]", windowsEligible)
	}
}

func TestEligibleTargetsCacheAvoidsFullReadUntilRevisionChanges(t *testing.T) {
	recording := &recordingControllerAgentStore{}
	recording.setConfiguration(eligibilityTestConfiguration(1))
	consumers := newStoreBackedAgentConsumers(recording)

	for i := 0; i < 3; i++ {
		if _, err := consumers.Eligibility.EligibleTargets(
			context.Background(), domain.OSLinux, domain.ArchAMD64,
		); err != nil {
			t.Fatal(err)
		}
	}
	revisionReads, fullReads := recording.counts()
	if revisionReads != 3 {
		t.Fatalf("revision reads = %d, want 3 (once per call)", revisionReads)
	}
	if fullReads != 1 {
		t.Fatalf("full configuration reads = %d, want 1 (cached across unchanged revision)", fullReads)
	}

	// A revision bump forces exactly one more full read, and the result
	// reflects the new configuration rather than the stale cache.
	recording.setConfiguration(eligibilityTestConfiguration(2))
	if eligible, err := consumers.Eligibility.EligibleTargets(
		context.Background(), domain.OSLinux, domain.ArchAMD64,
	); err != nil || len(eligible) != 2 {
		t.Fatalf("eligible targets after revision bump = %#v, err = %v", eligible, err)
	}
	revisionReads, fullReads = recording.counts()
	if revisionReads != 4 {
		t.Fatalf("revision reads = %d, want 4", revisionReads)
	}
	if fullReads != 2 {
		t.Fatalf("full configuration reads = %d, want 2 (recomputed once after the bump)", fullReads)
	}
}

// TestEligibleTargetsCacheIsSharedAcrossConsumerCopies guards the pointer
// discipline the brief calls out: storeBackedAgentConsumers is copied by
// value into every AgentConsumers field, so a cache embedded directly in the
// struct (rather than referenced through a pointer) would be silently
// duplicated per copy instead of shared, defeating the cache entirely.
func TestEligibleTargetsCacheIsSharedAcrossConsumerCopies(t *testing.T) {
	recording := &recordingControllerAgentStore{}
	recording.setConfiguration(eligibilityTestConfiguration(1))
	agentConsumers := newStoreBackedAgentConsumers(recording)

	if _, err := agentConsumers.Eligibility.EligibleTargets(
		context.Background(), domain.OSLinux, domain.ArchAMD64,
	); err != nil {
		t.Fatal(err)
	}
	// Commands holds the same storeBackedAgentConsumers value copied into a
	// different AgentConsumers field; it implements AgentEligibilityConsumer
	// too, so this reaches the cache through a distinct struct copy.
	if _, err := agentConsumers.Commands.(AgentEligibilityConsumer).EligibleTargets(
		context.Background(), domain.OSLinux, domain.ArchAMD64,
	); err != nil {
		t.Fatal(err)
	}
	_, fullReads := recording.counts()
	if fullReads != 1 {
		t.Fatalf("full configuration reads across consumer copies = %d, want 1 (shared cache)", fullReads)
	}
}
