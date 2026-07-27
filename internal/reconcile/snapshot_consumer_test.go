package reconcile

import (
	"context"
	"errors"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
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
