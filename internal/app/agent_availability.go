package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/genm/sparerunner/internal/buildinfo"
	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/nodectl"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
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
	//
	// eligibleReported distinguishes "no heartbeat ack has ever carried this
	// list" from "a heartbeat ack confirmed zero eligible targets": both leave
	// eligible at its zero value, and a plain nil-vs-empty check on the slice
	// cannot survive a copy (append onto a nil slice with nothing to append
	// stays nil), which previously collapsed a confirmed-empty list into the
	// same wire representation as never-reported.
	eligible         []nodectl.EligibleTarget
	eligibleReported bool
	// excluded mirrors the durable exclusion set. Every mutation goes through
	// this type, so the cache is invalidated exactly when the durable set
	// changes and a heartbeat never pays for a database read.
	excluded []domain.TargetID
	// sharedRunnerIdentity is a static process-level fact fixed at construction
	// from the serve flag: this node's native runner runs jobs under the Agent's
	// own uid, without uid isolation. It is deliberately not behind the mutex and
	// has no setter, so no runtime path can flip an operator-visible security
	// property. It is reported, never inferred, and never gates capacity.
	sharedRunnerIdentity bool
}

func newAgentAvailability(
	ctx context.Context,
	agentStore *store.AgentStore,
	nodeID domain.NodeID,
	sharedRunnerIdentity bool,
) (*agentAvailability, error) {
	record, err := agentStore.ReadAvailability(ctx)
	if err != nil {
		return nil, err
	}
	excluded, err := agentStore.ListExclusions(ctx)
	if err != nil {
		return nil, err
	}
	return &agentAvailability{
		store:                agentStore,
		nodeID:               nodeID,
		record:               record,
		excluded:             excluded,
		sharedRunnerIdentity: sharedRunnerIdentity,
	}, nil
}

// SharedRunnerIdentity reports whether this node's native runner executes jobs
// under the Agent's own uid. It is the single read path every observer — local
// status, snapshot, and heartbeat — shares, so no surface can render a different
// isolation claim than the one this process was started with.
func (availability *agentAvailability) SharedRunnerIdentity() bool {
	if availability == nil {
		return false
	}
	return availability.sharedRunnerIdentity
}

// ExcludedTargets is the sorted, deduplicated set this Agent reports on every
// snapshot and heartbeat. The store returns it ordered by target ID and its
// primary key makes duplicates impossible, so the wire order is stable.
func (availability *agentAvailability) ExcludedTargets() []domain.TargetID {
	if availability == nil {
		return nil
	}
	availability.mu.Lock()
	defer availability.mu.Unlock()
	// The returned set is always non-nil: the Agent knows its complete set, so
	// even "withdraws nothing" is an authoritative fact the wire must carry as
	// [] — otherwise removing the last exclusion would look like "no change
	// reported" and the controller would keep the stale adopted row forever.
	return append([]domain.TargetID{}, availability.excluded...)
}

// SetTargetExclusion durably records the owner's per-Target decision before any
// caller may observe it as effective. Excluding is subtractive and therefore
// locally effective the instant it is durable; including is additive and stays
// pending until the controller echoes its adoption.
func (availability *agentAvailability) SetTargetExclusion(
	ctx context.Context,
	targetID domain.TargetID,
	excluded bool,
	source nodectl.Source,
) (nodectl.Status, error) {
	if excluded {
		if err := availability.store.AddExclusion(ctx, targetID, string(source)); err != nil {
			if errors.Is(err, store.ErrTargetExclusionsFull) {
				// A full set is a rejected request, not a degraded agent: the
				// caller can act on it by including something first. Classify it
				// so a desktop surface branches on the class, not on text.
				return nodectl.Status{}, fmt.Errorf(
					"%w: this node already excludes the maximum number of targets",
					nodectl.ErrInvalidRequest,
				)
			}
			return nodectl.Status{}, err
		}
	} else if err := availability.store.RemoveExclusion(ctx, targetID); err != nil {
		return nodectl.Status{}, err
	}
	// Re-read rather than patch the cache: the durable set is the only
	// authority, and a failed re-read must not leave a divergent local view.
	refreshed, err := availability.store.ListExclusions(ctx)
	if err != nil {
		return nodectl.Status{}, err
	}
	availability.mu.Lock()
	availability.excluded = refreshed
	availability.mu.Unlock()
	return availability.Status(ctx)
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
	availability.eligibleReported = true
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
	eligibleReported := availability.eligibleReported
	var eligible []nodectl.EligibleTarget
	if eligibleReported {
		// Always allocate, even for zero elements, so a confirmed-empty list
		// stays a distinct non-nil value all the way to the wire instead of
		// collapsing into the same nil the never-reported case uses.
		eligible = make([]nodectl.EligibleTarget, len(availability.eligible))
		copy(eligible, availability.eligible)
	}
	locallyExcluded := make(map[domain.TargetID]struct{}, len(availability.excluded))
	for _, targetID := range availability.excluded {
		locallyExcluded[targetID] = struct{}{}
	}
	availability.mu.Unlock()

	// Join the durable local decision against the controller's last echoed
	// adoption. Pending is the honest disagreement between the two: an exclusion
	// the controller has not adopted yet, or an inclusion it has not released.
	var unknownExclusions []domain.TargetID
	known := make(map[domain.TargetID]struct{}, len(eligible))
	for index := range eligible {
		target := &eligible[index]
		known[target.TargetID] = struct{}{}
		_, target.LocallyExcluded = locallyExcluded[target.TargetID]
		target.Pending = target.LocallyExcluded != target.Excluded
	}
	// A locally excluded Target that is not in the last eligible list is a safe
	// no-op, not an error: the owner may have excluded it while offline, before
	// the first heartbeat round trip, or for a scope this platform never served.
	for _, targetID := range sortedTargetIDs(locallyExcluded) {
		if _, found := known[targetID]; !found {
			unknownExclusions = append(unknownExclusions, targetID)
		}
	}

	var eligibleTargets *[]nodectl.EligibleTarget
	if eligibleReported {
		eligibleTargets = &eligible
	}
	status := nodectl.Status{
		UnknownExclusions:       unknownExclusions,
		ProtocolVersion:         nodectl.ProtocolVersion,
		NodeID:                  availability.nodeID,
		Intent:                  record.Intent,
		IntentExplicit:          record.Explicit,
		IntentChangedAtUnixNano: record.ChangedAtUnixNano,
		IntentChangedBy:         record.ChangedBy,
		ControllerConnected:     connected,
		PendingResume:           record.Intent.Accepts() && confirmed != record.Intent,
		NativeRunnerReady:       nativeReady,
		SharedRunnerIdentity:    availability.sharedRunnerIdentity,
		EligibleTargets:         eligibleTargets,
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
	// Target attribution is recorded when the command that carried it was
	// admitted. An execution admitted before attribution existed simply has
	// none; it renders without a scope rather than with an invented one.
	attribution, err := availability.store.ExecutionTargets(ctx)
	if err != nil {
		return nodectl.Status{}, err
	}
	for _, observation := range snapshot.Observations {
		switch observation.State {
		case domain.ExecutionReleased, domain.ExecutionFailed,
			domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
			continue
		}
		running := nodectl.RunningExecution{
			ExecutionID: observation.ExecutionID,
			State:       observation.State,
		}
		if target, found := attribution[observation.ExecutionID]; found {
			running.TargetID = target.TargetID
			running.Scope = target.Scope
			running.ScopeKind = target.ScopeKind
		}
		status.RunningExecutions = append(status.RunningExecutions, running)
	}
	return status, nil
}

func sortedTargetIDs(set map[domain.TargetID]struct{}) []domain.TargetID {
	ids := make([]domain.TargetID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	return ids
}
