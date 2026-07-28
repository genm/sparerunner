package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/reconcile"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

// driveNodeReconciliationAction advances at most one action derived from the
// latest authenticated Agent snapshot for one exact node. DB-only actions use
// the snapshot digest as their CAS authority. Network actions commit exact
// command authority inside AgentBroker before the wire write. The node is the
// one that owns the claim being reconciled, so a Target served by several nodes
// can never issue a command against the wrong machine.
func (coordinator *ControllerRunnerCoordinator) driveNodeReconciliationAction(
	ctx context.Context,
	nodeID domain.NodeID,
) (handled bool, blocked bool, err error) {
	if nodeID == "" {
		return false, true, errors.New("reconciliation action requires a node")
	}
	admission, err := coordinator.config.Reconciler.Admission(nodeID)
	if err != nil {
		return false, true, err
	}
	for _, action := range admission.Actions {
		if action.NodeID != nodeID {
			return false, true, errors.New("reconciliation action belongs to another node")
		}
		switch action.Kind {
		case reconcile.ActionReplayCommand:
			return coordinator.replayPrepareAction(ctx, action)
		case reconcile.ActionAdoptObservation:
			return coordinator.adoptObservationAction(ctx, action)
		case reconcile.ActionInspectAndDestroy:
			return coordinator.destroyOrFailAction(ctx, action)
		case reconcile.ActionFailDesired:
			return coordinator.failDesiredAction(ctx, action)
		case reconcile.ActionPersistQuarantine:
			// Snapshot/tombstone persistence already quarantined the Node in the
			// same transaction. Keep it as a capacity blocker, but do not let it
			// starve provider-only cleanup for the exact execution's durable
			// GitHub fence.
			fence, found, err := coordinator.store.NextGitHubReconciliationFence(
				ctx,
				store.ScaleSetID(coordinator.config.ScaleSetID),
			)
			if err != nil {
				return false, true, err
			}
			if found && fence.Claim.Execution.ID == action.ExecutionID {
				continue
			}
			return false, true, nil
		case reconcile.ActionIssuePrepare:
			// The ordinary claim driver owns the first Prepare dispatch.
			continue
		case reconcile.ActionObserveGitHubClaim,
			reconcile.ActionObserveGitHubRunner,
			reconcile.ActionConfirmAgentStartAccepted,
			reconcile.ActionAwaitAgentObservation:
			// The provider fence path below owns these actions.
			continue
		default:
			return false, true, fmt.Errorf(
				"unsupported reconciliation action %q", action.Kind)
		}
	}
	return false, false, nil
}

func (coordinator *ControllerRunnerCoordinator) replayPrepareAction(
	ctx context.Context,
	action reconcile.Action,
) (bool, bool, error) {
	issued, found, err := coordinator.store.IssuedAgentCommand(
		ctx, action.CommandID)
	if err != nil {
		return false, true, err
	}
	if !found || issued.NodeID != action.NodeID ||
		issued.Type != domain.CommandPrepare ||
		issued.Command.ID != action.CommandID ||
		issued.Command.ExecutionID != action.ExecutionID ||
		issued.Command.ControllerEpoch != action.ControllerEpoch ||
		issued.Command.ExpectedState != domain.ExecutionReserved {
		return false, true, errors.New(
			"Prepare replay action does not match exact durable command authority")
	}
	metadata := transport.CommandMetadata{
		CommandID:       issued.Command.ID,
		ControllerEpoch: issued.Command.ControllerEpoch,
		ExecutionID:     issued.Command.ExecutionID,
		ExpectedState:   issued.Command.ExpectedState,
		// A replay must re-encode the exact original payload, so it reuses the
		// coordinator's own Target rather than inventing one.
		Target: coordinator.commandTarget(),
	}
	update, err := coordinator.agents.ReplayPrepare(
		ctx,
		action.NodeID,
		metadata,
		coordinator.disableUpdate(),
		action.SnapshotDigest,
	)
	if err != nil {
		if errors.Is(err, ErrAgentReconciliationStale) {
			return false, true, nil
		}
		return true, true, err
	}
	if err := validateControllerRunnerUpdate(
		update, action.NodeID, metadata); err != nil {
		return true, true, err
	}
	return true, false, nil
}

func (coordinator *ControllerRunnerCoordinator) adoptObservationAction(
	ctx context.Context,
	action reconcile.Action,
) (bool, bool, error) {
	if action.ObservedState == "" || action.ObservedAtNano <= 0 ||
		action.ExpectedState == "" {
		return false, true, errors.New(
			"observation adoption action lacks exact snapshot evidence")
	}
	next, _, err := coordinator.store.AdoptAgentSnapshotObservation(
		ctx,
		action.NodeID,
		action.ExpectedState,
		store.ObservationSnapshot{
			ExecutionID:        action.ExecutionID,
			State:              action.ObservedState,
			ObservedAtUnixNano: action.ObservedAtNano,
		},
		action.SnapshotDigest,
		coordinator.config.ControllerEpoch,
	)
	if err != nil {
		if errors.Is(err, store.ErrAgentTerminalUpdatePending) {
			return false, true, nil
		}
		return false, true, err
	}
	if err := coordinator.config.Reconciler.ApplyDesiredExecution(next); err != nil {
		return true, true, err
	}
	return true, false, nil
}

func (coordinator *ControllerRunnerCoordinator) destroyOrFailAction(
	ctx context.Context,
	action reconcile.Action,
) (bool, bool, error) {
	if action.ObservedState == "" {
		return coordinator.failDesiredAction(ctx, action)
	}
	if !terminalDesiredState(action.ExpectedState) ||
		!activeAgentState(action.ObservedState) ||
		action.ObservedAtNano <= 0 {
		// Unknown/orphan runtime cleanup is intentionally not inferred from an
		// execution ID alone. The node stays suppressed for operator evidence.
		return false, true, nil
	}
	commandID := deterministicCommandID(
		"reconcile-cancel",
		action.ExecutionID,
		fmt.Sprintf(
			"%d\x00%s\x00%d\x00%s",
			coordinator.config.ControllerEpoch,
			action.ObservedState,
			action.ObservedAtNano,
			action.SnapshotDigest,
		),
	)
	metadata := transport.CommandMetadata{
		CommandID:       commandID,
		ControllerEpoch: coordinator.config.ControllerEpoch,
		ExecutionID:     action.ExecutionID,
		ExpectedState:   action.ObservedState,
	}
	update, err := coordinator.agents.SendReconciliationCancel(
		ctx, action.NodeID, metadata, action.SnapshotDigest)
	if err != nil {
		if errors.Is(err, ErrAgentReconciliationStale) {
			return false, true, nil
		}
		return true, true, err
	}
	if err := validateControllerRunnerUpdate(
		update, action.NodeID, metadata); err != nil {
		return true, true, err
	}
	switch update.State {
	case domain.ExecutionCleaning, domain.ExecutionReleased,
		domain.ExecutionFailed, domain.ExecutionCleanupFailed,
		domain.ExecutionQuarantined:
		return true, false, nil
	default:
		return true, true, errors.New(
			"reconciliation Cancel returned a non-teardown state")
	}
}

func (coordinator *ControllerRunnerCoordinator) failDesiredAction(
	ctx context.Context,
	action reconcile.Action,
) (bool, bool, error) {
	if action.ExpectedState == "" || action.SnapshotDigest == "" {
		return false, true, nil
	}
	next, err := coordinator.store.FailDesiredExecutionFromSnapshot(
		ctx,
		action.NodeID,
		action.ExecutionID,
		action.ExpectedState,
		action.SnapshotDigest,
		coordinator.config.ControllerEpoch,
	)
	if err != nil {
		return false, true, err
	}
	if err := coordinator.config.Reconciler.ApplyDesiredExecution(next); err != nil {
		return true, true, err
	}
	return true, false, nil
}

func terminalDesiredState(state domain.ExecutionState) bool {
	return state == domain.ExecutionReleased ||
		state == domain.ExecutionFailed
}

func activeAgentState(state domain.ExecutionState) bool {
	switch state {
	case domain.ExecutionPreparing, domain.ExecutionRunning,
		domain.ExecutionCleaning:
		return true
	default:
		return false
	}
}
