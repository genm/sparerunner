package app

import (
	"context"
	"sync"
	"time"

	"github.com/genm/tewake/internal/buildinfo"
	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/nodectl"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

// agentAvailability owns the node-local half of availability. The Controller
// keeps its administrative authority; this type only decides whether this
// computer offers its own capacity, so the two compose as a conjunction and a
// local decision can never re-admit a node the Controller refuses.
type agentAvailability struct {
	mu sync.Mutex

	store  *store.AgentStore
	nodeID domain.NodeID

	record store.AvailabilityRecord
	// connected and confirmedIntent are this Agent's own observation of its
	// controller session. Stopping subtracts capacity and therefore applies
	// locally at once; resuming adds capacity and stays pending until the
	// controller has acknowledged a heartbeat carrying it.
	connected       bool
	confirmedIntent domain.AvailabilityIntent
	nativeReady     bool
	// eligible is the last-known eligible-target list confirmed by a heartbeat
	// ack. It is converted once here (rather than at every Status() read) and
	// deliberately survives a disconnect: ControllerConnected already tells the
	// owner the session is down, so this stays the last-known list instead of
	// blanking to nothing.
	eligible []nodectl.EligibleTarget
}

func newAgentAvailability(
	ctx context.Context,
	agentStore *store.AgentStore,
	nodeID domain.NodeID,
) (*agentAvailability, error) {
	record, err := agentStore.ReadAvailability(ctx)
	if err != nil {
		return nil, err
	}
	return &agentAvailability{
		store:  agentStore,
		nodeID: nodeID,
		record: record,
	}, nil
}

func (availability *agentAvailability) Intent() domain.AvailabilityIntent {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	return availability.record.Intent
}

// Accepts is the local admission gate. It is combined with native runner
// readiness before anything is advertised to the Controller.
func (availability *agentAvailability) Accepts() bool {
	return availability.Intent().Accepts()
}

func (availability *agentAvailability) setConnected(connected bool) {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	availability.connected = connected
	if !connected {
		availability.confirmedIntent = ""
	}
}

// confirm records that the Controller acknowledged a heartbeat carrying this
// exact intent. Only then may a resume stop reporting as pending.
func (availability *agentAvailability) confirm(intent domain.AvailabilityIntent) {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	availability.confirmedIntent = intent
}

func (availability *agentAvailability) setNativeReady(ready bool) {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	availability.nativeReady = ready
}

// setEligibleTargets applies a heartbeat ack's eligible-target list. present
// must be false for a nil (absent) wire value and true otherwise, including
// for a confirmed-empty list: present=false keeps the last-known list rather
// than blanking it, matching the wire's own nil-vs-empty distinction.
func (availability *agentAvailability) setEligibleTargets(
	targets []transport.EligibleTarget,
	present bool,
) {
	if !present {
		return
	}
	converted := make([]nodectl.EligibleTarget, len(targets))
	for index, target := range targets {
		converted[index] = nodectl.EligibleTarget{
			TargetID:     target.TargetID,
			ScopeKind:    target.ScopeKind,
			Scope:        target.Scope,
			ScaleSetName: target.ScaleSetName,
			Excluded:     target.Excluded,
		}
	}
	availability.mu.Lock()
	availability.eligible = converted
	availability.mu.Unlock()
}

// SetIntent durably records the owner's decision before returning it. A failed
// write reports the failure rather than an optimistic new state.
func (availability *agentAvailability) SetIntent(
	ctx context.Context,
	intent domain.AvailabilityIntent,
	source nodectl.Source,
) (nodectl.Status, error) {
	record, err := availability.store.SetAvailability(ctx, intent, string(source))
	if err != nil {
		return nodectl.Status{}, err
	}
	availability.mu.Lock()
	availability.record = record
	if availability.confirmedIntent != intent {
		availability.confirmedIntent = ""
	}
	availability.mu.Unlock()
	return availability.Status(ctx)
}

func (availability *agentAvailability) Status(ctx context.Context) (nodectl.Status, error) {
	availability.mu.Lock()
	record := availability.record
	connected := availability.connected
	confirmed := availability.confirmedIntent
	nativeReady := availability.nativeReady
	eligible := append([]nodectl.EligibleTarget(nil), availability.eligible...)
	availability.mu.Unlock()

	status := nodectl.Status{
		ProtocolVersion:         nodectl.ProtocolVersion,
		NodeID:                  availability.nodeID,
		Intent:                  record.Intent,
		IntentExplicit:          record.Explicit,
		IntentChangedAtUnixNano: record.ChangedAtUnixNano,
		IntentChangedBy:         record.ChangedBy,
		ControllerConnected:     connected,
		PendingResume:           record.Intent.Accepts() && confirmed != record.Intent,
		NativeRunnerReady:       nativeReady,
		EligibleTargets:         eligible,
		ObservedAtUnixNano:      time.Now().UnixNano(),
		AgentVersion:            buildinfo.String(),
	}
	// Running work is read from the durable journal rather than remembered in
	// process, so a restarted Agent reports what this computer is actually
	// doing instead of an empty list.
	snapshot, err := availability.store.Snapshot(ctx)
	if err != nil {
		return nodectl.Status{}, err
	}
	for _, observation := range snapshot.Observations {
		switch observation.State {
		case domain.ExecutionReleased, domain.ExecutionFailed,
			domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
			continue
		}
		status.RunningExecutions = append(status.RunningExecutions, nodectl.RunningExecution{
			ExecutionID: observation.ExecutionID,
			State:       observation.State,
		})
	}
	return status, nil
}
