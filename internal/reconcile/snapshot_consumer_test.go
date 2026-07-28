package reconcile

import (
	"context"
	"errors"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

func TestSnapshotConsumerCommitsBeforeReturningNodeCapacity(t *testing.T) {
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)}})
	recorder := &recordingSnapshotStore{
		before: func() {
			if controller.FleetSnapshot().Nodes[0].Reconciled {
				t.Fatal("capacity returned before snapshot commit")
			}
		},
	}
	consumer, err := NewSnapshotConsumer(recorder, controller)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
	}
	if err := consumer.HandleAgentSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if recorder.calls != 1 || recorder.snapshot.NodeID != snapshot.NodeID ||
		!controller.FleetSnapshot().Nodes[0].Reconciled {
		t.Fatalf("consumer result = calls %d, snapshot %#v, fleet %#v",
			recorder.calls, recorder.snapshot, controller.FleetSnapshot())
	}
}

// TestSnapshotConsumerMapsOwnerStateIntoTheRecordedSnapshot pins the exact
// production-only regression the live fleet hit: this consumer replaces the
// store-backed one in every activated Controller, so an owner intent or
// exclusion set it drops is silently never adopted at reconnect even though
// every store- and consumer-level unit test stays green.
func TestSnapshotConsumerMapsOwnerStateIntoTheRecordedSnapshot(t *testing.T) {
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)}})
	recorder := &recordingSnapshotStore{}
	consumer, err := NewSnapshotConsumer(recorder, controller)
	if err != nil {
		t.Fatal(err)
	}
	populated := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		AvailabilityIntent: domain.AvailabilityStopped,
		ExcludedTargets:    transport.TargetIDSet("target-a", "target-b"),
		MaxControllerEpoch: 1,
	}
	if err := consumer.HandleAgentSnapshot(context.Background(), populated); err != nil {
		t.Fatal(err)
	}
	if recorder.snapshot.AvailabilityIntent != domain.AvailabilityStopped ||
		len(recorder.snapshot.ExcludedTargets) != 2 {
		t.Fatalf("owner state dropped before the snapshot transaction: %#v", recorder.snapshot)
	}

	// A confirmed-empty set must stay a non-nil replacement, and an absent one
	// must stay nil ("no change reported") — collapsing either direction makes
	// stale adopted rows immortal or wipes them spuriously.
	empty := populated
	empty.AvailabilityIntent = ""
	empty.ExcludedTargets = transport.TargetIDSet()
	if err := consumer.HandleAgentSnapshot(context.Background(), empty); err != nil {
		t.Fatal(err)
	}
	if recorder.snapshot.ExcludedTargets == nil || len(recorder.snapshot.ExcludedTargets) != 0 {
		t.Fatalf("confirmed-empty set collapsed: %#v", recorder.snapshot.ExcludedTargets)
	}
	absent := populated
	absent.AvailabilityIntent = ""
	absent.ExcludedTargets = nil
	if err := consumer.HandleAgentSnapshot(context.Background(), absent); err != nil {
		t.Fatal(err)
	}
	if recorder.snapshot.ExcludedTargets != nil {
		t.Fatalf("absent set fabricated a replacement: %#v", recorder.snapshot.ExcludedTargets)
	}
}

// The reported runner-isolation mode reaches production only through this
// consumer, which replaces the store-backed one in every activated Controller.
// A live deployment found it silently dropped here while every other test
// passed, so this asserts the mapping directly and keeps nil distinct from
// false: nil is "never reported", and reading it as false would claim the
// stronger isolated mode for a node that has no uid separation at all.
func TestSnapshotConsumerMapsSharedRunnerIdentityIntoTheRecordedSnapshot(t *testing.T) {
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)}})
	recorder := &recordingSnapshotStore{}
	consumer, err := NewSnapshotConsumer(recorder, controller)
	if err != nil {
		t.Fatal(err)
	}
	base := transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		AvailabilityIntent: domain.AvailabilityAccepting,
		MaxControllerEpoch: 1,
	}
	for _, reported := range []bool{true, false} {
		snapshot := base
		snapshot.SharedRunnerIdentity = &reported
		if err := consumer.HandleAgentSnapshot(context.Background(), snapshot); err != nil {
			t.Fatal(err)
		}
		if recorder.snapshot.SharedRunnerIdentity == nil ||
			*recorder.snapshot.SharedRunnerIdentity != reported {
			t.Fatalf(
				"reported isolation mode %t dropped before the snapshot transaction: %#v",
				reported, recorder.snapshot.SharedRunnerIdentity)
		}
		if recorder.snapshot.SharedRunnerIdentity == &reported {
			t.Fatal("recorded pointer aliases the caller's wire value")
		}
	}
	absent := base
	absent.SharedRunnerIdentity = nil
	if err := consumer.HandleAgentSnapshot(context.Background(), absent); err != nil {
		t.Fatal(err)
	}
	if recorder.snapshot.SharedRunnerIdentity != nil {
		t.Fatalf(
			"absent isolation mode fabricated a report: %#v",
			recorder.snapshot.SharedRunnerIdentity)
	}
}

func TestSnapshotConsumerStoreFailureLeavesLastKnownProjectionUntouched(t *testing.T) {
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
		Nodes:           []store.NodeAdministration{{NodeID: "node-a", State: domain.NodeActive}},
	}, Config{Nodes: []NodeDefinition{testNodeDefinition("node-a", 1)}})
	want := errors.New("disk full")
	consumer, err := NewSnapshotConsumer(
		&recordingSnapshotStore{err: want},
		controller,
	)
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.HandleAgentSnapshot(context.Background(), transport.AgentSnapshot{
		NodeID:             "node-a",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
	})
	if !errors.Is(err, want) {
		t.Fatalf("store failure = %v", err)
	}
	fleet := controller.FleetSnapshot()
	if fleet.Nodes[0].Node.ObservedState != domain.NodeOffline ||
		fleet.Nodes[0].Reconciled {
		t.Fatalf("store failure synthesized online state: %#v", fleet)
	}
}

func TestSnapshotConsumerAddsPostStartupEnrollmentOnlyAfterStoreCommit(t *testing.T) {
	controller := restoreForTest(t, store.ControllerSnapshot{
		ControllerEpoch: 2,
	}, Config{})
	recorder := &recordingSnapshotStore{
		restart: store.ControllerRestartSnapshot{
			Controller: store.ControllerSnapshot{
				ControllerEpoch: 2,
				Nodes: []store.NodeAdministration{{
					NodeID: "node-late",
					State:  domain.NodeActive,
				}},
			},
			NodeTopology: []store.RestartNodeTopology{{
				NodeID:              "node-late",
				DisplayName:         "Late node",
				CertificateSerial:   "certificate-late",
				CredentialEpoch:     1,
				AdministrativeState: domain.NodeActive,
				MaxRunners:          1,
				PlatformObserved:    true,
				OS:                  domain.OSLinux,
				Architecture:        domain.ArchAMD64,
			}},
		},
	}
	recorder.beforeRestart = func() {
		if recorder.calls != 1 {
			t.Fatal("node topology was read before the Agent snapshot commit")
		}
	}
	consumer, err := NewSnapshotConsumer(recorder, controller)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.HandleAgentSnapshot(context.Background(), transport.AgentSnapshot{
		NodeID:             "node-late",
		OS:                 domain.OSLinux,
		Arch:               domain.ArchAMD64,
		NativeRunnerReady:  true,
		MaxControllerEpoch: 1,
	}); err != nil {
		t.Fatal(err)
	}
	fleet := controller.FleetSnapshot()
	if recorder.restartCalls != 1 ||
		len(fleet.Nodes) != 1 ||
		!fleet.Nodes[0].Reconciled ||
		!fleet.Nodes[0].NativeReady {
		t.Fatalf("post-startup enrollment = recorder %#v fleet %#v", recorder, fleet)
	}
}

type recordingSnapshotStore struct {
	before        func()
	beforeRestart func()
	err           error
	calls         int
	snapshot      store.NodeAgentSnapshot
	restart       store.ControllerRestartSnapshot
	restartCalls  int
}

func (recorder *recordingSnapshotStore) RecordAgentSnapshot(
	_ context.Context,
	snapshot store.NodeAgentSnapshot,
) error {
	if recorder.before != nil {
		recorder.before()
	}
	recorder.calls++
	recorder.snapshot = snapshot
	return recorder.err
}

func (recorder *recordingSnapshotStore) RestartSnapshot(
	_ context.Context,
) (store.ControllerRestartSnapshot, error) {
	if recorder.beforeRestart != nil {
		recorder.beforeRestart()
	}
	recorder.restartCalls++
	return recorder.restart, recorder.err
}
