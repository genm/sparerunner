package reconcile

import (
	"context"
	"errors"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

type AgentSnapshotRecorder interface {
	RecordAgentSnapshot(context.Context, store.NodeAgentSnapshot) error
}

type RestartSnapshotReader interface {
	RestartSnapshot(context.Context) (store.ControllerRestartSnapshot, error)
}

// SnapshotConsumer is the session-activation commit boundary. It implements
// the app AgentSnapshotConsumer shape without importing the app package:
// Controller projection changes only after SQLite accepts the same typed
// snapshot.
type SnapshotConsumer struct {
	recorder   AgentSnapshotRecorder
	controller *Controller
}

func NewSnapshotConsumer(
	recorder AgentSnapshotRecorder,
	controller *Controller,
) (*SnapshotConsumer, error) {
	if recorder == nil || controller == nil {
		return nil, invalid("snapshot_consumer_dependency_required", "snapshot_consumer", "recorder and Controller must be present")
	}
	return &SnapshotConsumer{recorder: recorder, controller: controller}, nil
}

func (consumer *SnapshotConsumer) HandleAgentSnapshot(
	ctx context.Context,
	snapshot transport.AgentSnapshot,
) error {
	if consumer == nil || consumer.recorder == nil || consumer.controller == nil {
		return invalid("snapshot_consumer_unavailable", "snapshot_consumer", "is not initialized")
	}
	if err := snapshot.Validate(); err != nil {
		return invalid("invalid_agent_snapshot", "agent_snapshot", "failed typed validation")
	}
	record := store.NodeAgentSnapshot{
		NodeID:            snapshot.NodeID,
		OS:                snapshot.OS,
		Architecture:      snapshot.Arch,
		RunnerVersion:     snapshot.RunnerVersion,
		NativeRunnerReady: snapshot.NativeRunnerReady,
		// Owner-editable observed state must reach the same snapshot
		// transaction, or reconnect-time adoption silently never happens: this
		// consumer replaces the store-backed one in every activated Controller,
		// so dropping the fields here would drop them in production while every
		// consumer-level unit test still passes. Nil-ness is preserved — nil is
		// "no change reported", not an empty set.
		AvailabilityIntent: snapshot.AvailabilityIntent,
		ExcludedTargets:    copiedExclusionSet(snapshot.ExcludedTargets),
		// The reported runner-isolation mode travels the same way and for the same
		// reason: dropping it here would leave every node's isolation column NULL
		// in production while the consumer-level tests still pass.
		SharedRunnerIdentity: copiedReportedFlag(snapshot.SharedRunnerIdentity),
		Journal: store.AgentSnapshot{
			MaxControllerEpoch: snapshot.MaxControllerEpoch,
			Commands:           append([]domain.Command(nil), snapshot.Commands...),
			Observations:       make([]store.ObservationSnapshot, len(snapshot.Observations)),
			CleanupTombstones:  make([]store.CleanupTombstoneSnapshot, len(snapshot.CleanupTombstones)),
		},
	}
	for index, observation := range snapshot.Observations {
		record.Journal.Observations[index] = store.ObservationSnapshot{
			ExecutionID:        observation.ExecutionID,
			State:              observation.State,
			ObservedAtUnixNano: observation.ObservedAtUnixNano,
		}
	}
	for index, tombstone := range snapshot.CleanupTombstones {
		record.Journal.CleanupTombstones[index] = store.CleanupTombstoneSnapshot{
			ExecutionID:        tombstone.ExecutionID,
			FailureCode:        tombstone.FailureCode,
			RecordedAtUnixNano: tombstone.RecordedAtUnixNano,
		}
	}
	if err := consumer.recorder.RecordAgentSnapshot(ctx, record); err != nil {
		return err
	}
	_, err := consumer.controller.ReconcileAgentSnapshot(snapshot)
	var classified *Error
	if !errors.As(err, &classified) || classified.Code != "node_not_found" {
		return err
	}
	// A node may enroll after this Controller process restored its startup
	// projection. Add it only from a fresh, post-commit store read; the Agent
	// payload is never administrative or credential authority.
	reader, ok := consumer.recorder.(RestartSnapshotReader)
	if !ok {
		return err
	}
	if err := EnsureStoreBackedRestartNode(
		ctx,
		reader,
		consumer.controller,
		snapshot.NodeID,
	); err != nil {
		return err
	}
	_, err = consumer.controller.ReconcileAgentSnapshot(snapshot)
	return err
}

// copiedExclusionSet flattens the wire pointer into the store's slice shape:
// nil stays nil ("no change reported"), and a present set — including an empty
// one — becomes a non-nil copy that replaces the adopted rows wholesale.
func copiedExclusionSet(set *[]domain.TargetID) []domain.TargetID {
	if set == nil {
		return nil
	}
	return append([]domain.TargetID{}, *set...)
}

// copiedReportedFlag preserves the nil-versus-present distinction of a reported
// boolean across the wire-to-store boundary: nil is "not reported" and must not
// collapse into false, which would claim the stronger isolated mode.
func copiedReportedFlag(reported *bool) *bool {
	if reported == nil {
		return nil
	}
	value := *reported
	return &value
}

// EnsureStoreBackedRestartNode projects a newly enrolled node from a fresh
// durable restart snapshot. Enrollment and first-snapshot reconciliation share
// this boundary so neither path treats Agent-provided identity as authority.
func EnsureStoreBackedRestartNode(
	ctx context.Context,
	reader RestartSnapshotReader,
	controller *Controller,
	nodeID domain.NodeID,
) error {
	if reader == nil || controller == nil || nodeID == "" {
		return invalid(
			"restart_node_dependency_required",
			"restart_node",
			"reader, Controller, and node ID must be present",
		)
	}
	restart, readErr := reader.RestartSnapshot(ctx)
	if readErr != nil {
		return readErr
	}
	if restart.Controller.ControllerEpoch != controller.Epoch() {
		return invalid("controller_epoch_mismatch", "restart_snapshot.controller_epoch", "changed while adding an enrolled node")
	}
	var topology *store.RestartNodeTopology
	for index := range restart.NodeTopology {
		if restart.NodeTopology[index].NodeID == nodeID {
			candidate := restart.NodeTopology[index]
			topology = &candidate
			break
		}
	}
	if topology == nil {
		return invalid("node_authority_not_found", "restart_snapshot.node_topology", "does not contain the authenticated Agent node")
	}
	return controller.EnsureRestartNode(*topology)
}
