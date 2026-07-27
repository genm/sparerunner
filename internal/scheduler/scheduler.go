// Package scheduler assigns concrete node slots to GitHub Targets.
package scheduler

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/genm/tewake/internal/domain"
)

// Error is a stable, machine-readable scheduler input or ownership error.
type Error struct {
	Code    string
	Field   string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("scheduler failed: code=%s field=%s: %s", e.Code, e.Field, e.Message)
}

func invalid(code, field, message string) error {
	return &Error{Code: code, Field: field, Message: message}
}

// NodeSnapshot combines configured node identity with the latest Agent journal
// observation. Reconciled means that observation has passed Controller epoch
// reconciliation; admission additionally requires every ActiveExecution to
// have matching durable slot ownership on this node.
type NodeSnapshot struct {
	Node                 domain.Node
	Reconciled           bool
	NativeReady          bool
	AvailableMemoryBytes uint64
	ActiveExecutions     []domain.ExecutionID
	CachedRunnerPackages []string
}

// Validate rejects observations that could make scheduling bounds ambiguous.
func (snapshot NodeSnapshot) Validate() error {
	if err := snapshot.Node.Validate(); err != nil {
		return err
	}
	if len(snapshot.ActiveExecutions) > snapshot.Node.MaxRunners {
		return invalid("active_runners_exceed_node_maximum", "node.active_executions", "must not exceed node.max_runners")
	}
	executions := make(map[domain.ExecutionID]struct{}, len(snapshot.ActiveExecutions))
	for _, executionID := range snapshot.ActiveExecutions {
		if strings.TrimSpace(string(executionID)) == "" {
			return invalid("invalid_active_execution", "node.active_executions", "must not contain an empty execution id")
		}
		if _, duplicate := executions[executionID]; duplicate {
			return invalid("duplicate_active_execution", "node.active_executions", "must not contain a duplicate execution id")
		}
		executions[executionID] = struct{}{}
	}
	for _, packageID := range snapshot.CachedRunnerPackages {
		if strings.TrimSpace(packageID) == "" {
			return invalid("invalid_cached_runner_package", "node.cached_runner_packages", "must not contain an empty package identifier")
		}
	}
	return nil
}

// TargetSpec joins verified GitHub configuration to its placement profile and
// the exact runner package identifier used for cache preference.
type TargetSpec struct {
	Target        domain.GitHubTarget
	Profile       domain.RunnerProfile
	RunnerPackage string
}

// Validate ensures callers cannot route a target through a different profile.
func (spec TargetSpec) Validate() error {
	if err := spec.Target.Validate(); err != nil {
		return err
	}
	if err := spec.Profile.Validate(); err != nil {
		return err
	}
	if spec.Target.RunnerProfileID != spec.Profile.ID {
		return invalid("target_profile_mismatch", "target.runner_profile_id", "must match the supplied runner profile")
	}
	if strings.TrimSpace(spec.RunnerPackage) != spec.RunnerPackage {
		return invalid("invalid_runner_package", "target.runner_package", "must not contain leading or trailing whitespace")
	}
	return nil
}

// GrantID identifies one admission decision. Identifiers are never reused
// during a Scheduler lifetime, preventing a stale release from freeing a later
// owner of the same physical slot.
type GrantID uint64

// GrantRef is the complete non-secret ownership snapshot needed to bind or
// release a grant. Binding enriches the snapshot with an ExecutionID; release
// requires that current snapshot so a stale reservation cannot free a runner.
type GrantRef struct {
	ID          GrantID
	TargetID    domain.TargetID
	Slot        domain.SlotKey
	ExecutionID domain.ExecutionID
}

func (ref GrantRef) validate() error {
	if ref.ID == 0 {
		return invalid("invalid_grant_id", "grant.id", "must be greater than zero")
	}
	if strings.TrimSpace(string(ref.TargetID)) == "" {
		return invalid("invalid_grant_target", "grant.target_id", "must not be empty")
	}
	if strings.TrimSpace(string(ref.Slot.NodeID)) == "" {
		return invalid("invalid_grant_slot", "grant.slot.node_id", "must not be empty")
	}
	if ref.Slot.Index < 0 {
		return invalid("invalid_grant_slot", "grant.slot.index", "must not be negative")
	}
	return nil
}

// Grant is a concrete Target-owned slot. ExecutionID remains empty while the
// slot is only a temporary capacity grant or reservation.
type Grant struct {
	ID          GrantID
	TargetID    domain.TargetID
	Slot        domain.SlotKey
	ExecutionID domain.ExecutionID
}

// Ref returns the immutable ownership identity for subsequent operations.
func (grant Grant) Ref() GrantRef {
	return GrantRef{
		ID:          grant.ID,
		TargetID:    grant.TargetID,
		Slot:        grant.Slot,
		ExecutionID: grant.ExecutionID,
	}
}

// TargetCapacity is the maxCapacity backed by slots currently owned by one
// Target. It deliberately excludes ungranted free fleet capacity.
type TargetCapacity struct {
	TargetID   domain.TargetID
	Advertised int
}

// RestoredReservation is durable Controller ownership reconstructed before
// capacity is advertised after startup.
type RestoredReservation struct {
	TargetID    domain.TargetID
	Slot        domain.SlotKey
	ExecutionID domain.ExecutionID
}

type nodeState struct {
	snapshot NodeSnapshot
	cached   map[string]struct{}
}

// Scheduler serializes target round-robin and concrete slot ownership. Durable
// persistence and restart reconciliation are separate boundaries.
type Scheduler struct {
	mu sync.RWMutex

	ledger *domain.SlotLedger

	nodes       map[domain.NodeID]nodeState
	targets     map[domain.TargetID]TargetSpec
	targetOrder []domain.TargetID
	nextTarget  int

	nextGrantID GrantID
	grants      map[GrantID]Grant
	grantBySlot map[domain.SlotKey]GrantID
}

// New builds a scheduler with immutable slot topology and deterministic target
// order. A nil fleetMaximum uses the sum of node maxima.
func New(nodes []NodeSnapshot, targets []TargetSpec, fleetMaximum *int) (*Scheduler, error) {
	return NewWithReservations(nodes, targets, fleetMaximum, nil)
}

// NewWithReservations restores durable slot ownership before any placement.
// Missing ownership remains visible through ActiveExecutions and consumes
// conservative fleet capacity without becoming advertised capacity.
func NewWithReservations(
	nodes []NodeSnapshot,
	targets []TargetSpec,
	fleetMaximum *int,
	reservations []RestoredReservation,
) (*Scheduler, error) {
	nodeStates := make(map[domain.NodeID]nodeState, len(nodes))
	domainNodes := make([]domain.Node, 0, len(nodes))
	for _, snapshot := range nodes {
		state, err := newNodeState(snapshot)
		if err != nil {
			return nil, err
		}
		if _, duplicate := nodeStates[snapshot.Node.ID]; duplicate {
			return nil, invalid("duplicate_node_id", "nodes", "contains duplicate node id")
		}
		nodeStates[snapshot.Node.ID] = state
		domainNodes = append(domainNodes, snapshot.Node)
	}

	ledger, err := domain.NewSlotLedger(domainNodes, fleetMaximum)
	if err != nil {
		return nil, err
	}

	targetSpecs := make(map[domain.TargetID]TargetSpec, len(targets))
	targetOrder := make([]domain.TargetID, 0, len(targets))
	for _, spec := range targets {
		if err := spec.Validate(); err != nil {
			return nil, err
		}
		if _, duplicate := targetSpecs[spec.Target.ID]; duplicate {
			return nil, invalid("duplicate_target_id", "targets", "contains duplicate target id")
		}
		spec = cloneTargetSpec(spec)
		targetSpecs[spec.Target.ID] = spec
		targetOrder = append(targetOrder, spec.Target.ID)
	}
	sort.Slice(targetOrder, func(i, j int) bool {
		return targetOrder[i] < targetOrder[j]
	})

	scheduler := &Scheduler{
		ledger:      ledger,
		nodes:       nodeStates,
		targets:     targetSpecs,
		targetOrder: targetOrder,
		nextGrantID: 1,
		grants:      make(map[GrantID]Grant),
		grantBySlot: make(map[domain.SlotKey]GrantID),
	}
	if err := scheduler.restoreReservations(reservations); err != nil {
		return nil, err
	}
	return scheduler, nil
}

func newNodeState(snapshot NodeSnapshot) (nodeState, error) {
	if err := snapshot.Validate(); err != nil {
		return nodeState{}, err
	}
	snapshot.ActiveExecutions = append([]domain.ExecutionID(nil), snapshot.ActiveExecutions...)
	snapshot.CachedRunnerPackages = append([]string(nil), snapshot.CachedRunnerPackages...)
	cached := make(map[string]struct{}, len(snapshot.CachedRunnerPackages))
	for _, packageID := range snapshot.CachedRunnerPackages {
		cached[packageID] = struct{}{}
	}
	return nodeState{snapshot: snapshot, cached: cached}, nil
}

func (scheduler *Scheduler) restoreReservations(reservations []RestoredReservation) error {
	reservations = append([]RestoredReservation(nil), reservations...)
	sort.Slice(reservations, func(i, j int) bool {
		if reservations[i].Slot.NodeID != reservations[j].Slot.NodeID {
			return reservations[i].Slot.NodeID < reservations[j].Slot.NodeID
		}
		if reservations[i].Slot.Index != reservations[j].Slot.Index {
			return reservations[i].Slot.Index < reservations[j].Slot.Index
		}
		return reservations[i].TargetID < reservations[j].TargetID
	})

	configuredSlots := make(map[domain.SlotKey]struct{})
	for _, slot := range scheduler.ledger.Slots() {
		configuredSlots[slot.Key] = struct{}{}
	}
	seenSlots := make(map[domain.SlotKey]struct{}, len(reservations))
	seenExecutions := make(map[domain.ExecutionID]struct{}, len(reservations))
	for _, reservation := range reservations {
		if _, exists := scheduler.targets[reservation.TargetID]; !exists {
			return invalid("target_not_found", "reservation.target_id", "does not identify a configured target")
		}
		if _, exists := configuredSlots[reservation.Slot]; !exists {
			return invalid("slot_not_found", "reservation.slot", "does not identify a configured concrete slot")
		}
		if _, duplicate := seenSlots[reservation.Slot]; duplicate {
			return invalid("duplicate_restored_slot", "reservation.slot", "must occur at most once")
		}
		seenSlots[reservation.Slot] = struct{}{}
		if reservation.ExecutionID != "" {
			if strings.TrimSpace(string(reservation.ExecutionID)) == "" {
				return invalid("invalid_execution_id", "reservation.execution_id", "must not be whitespace")
			}
			if _, duplicate := seenExecutions[reservation.ExecutionID]; duplicate {
				return invalid("duplicate_restored_execution", "reservation.execution_id", "must occur at most once")
			}
			seenExecutions[reservation.ExecutionID] = struct{}{}
		}
	}
	if len(reservations) > scheduler.ledger.FleetMaximum() {
		return invalid("restored_capacity_exceeds_fleet_maximum", "reservations", "contains more claims than fleetMaximum")
	}

	for _, reservation := range reservations {
		owner := domain.SlotOwner{
			TargetID:    reservation.TargetID,
			ExecutionID: reservation.ExecutionID,
		}
		if err := scheduler.ledger.Claim(reservation.Slot, domain.SlotOwner{TargetID: reservation.TargetID}); err != nil {
			return fmt.Errorf("restore claimed slot: %w", err)
		}
		if reservation.ExecutionID != "" {
			if err := scheduler.ledger.BindExecution(reservation.Slot, reservation.TargetID, reservation.ExecutionID); err != nil {
				return fmt.Errorf("restore execution binding: %w", err)
			}
		}
		grant := Grant{
			ID:          scheduler.nextGrantID,
			TargetID:    owner.TargetID,
			Slot:        reservation.Slot,
			ExecutionID: owner.ExecutionID,
		}
		scheduler.nextGrantID++
		scheduler.grants[grant.ID] = grant
		scheduler.grantBySlot[grant.Slot] = grant.ID
	}
	return nil
}

func cloneTargetSpec(spec TargetSpec) TargetSpec {
	if spec.Profile.OS != nil {
		operatingSystem := *spec.Profile.OS
		spec.Profile.OS = &operatingSystem
	}
	if spec.Profile.Architecture != nil {
		architecture := *spec.Profile.Architecture
		spec.Profile.Architecture = &architecture
	}
	return spec
}

// UpdateNode replaces mutable administrative and observed state. Slot count,
// operating system, and architecture require rebuilding the durable topology.
func (scheduler *Scheduler) UpdateNode(snapshot NodeSnapshot) error {
	state, err := newNodeState(snapshot)
	if err != nil {
		return err
	}

	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	current, exists := scheduler.nodes[snapshot.Node.ID]
	if !exists {
		return invalid("node_not_found", "node.id", "does not identify a configured node")
	}
	if current.snapshot.Node.OS != snapshot.Node.OS ||
		current.snapshot.Node.Architecture != snapshot.Node.Architecture ||
		current.snapshot.Node.MaxRunners != snapshot.Node.MaxRunners {
		return invalid("node_topology_mismatch", "node", "cannot change operating system, architecture, or maxRunners in place")
	}
	scheduler.nodes[snapshot.Node.ID] = state
	return nil
}

// GrantNext assigns one best-ranked free slot to the next demanded Target in
// deterministic round-robin order. Empty demand and exhausted eligibility are
// successful no-capacity results.
func (scheduler *Scheduler) GrantNext(demand []domain.TargetID) (Grant, bool, error) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()

	demanded := make(map[domain.TargetID]struct{}, len(demand))
	for _, targetID := range demand {
		if _, exists := scheduler.targets[targetID]; !exists {
			return Grant{}, false, invalid("target_not_found", "demand.target_id", "does not identify a configured target")
		}
		if _, duplicate := demanded[targetID]; duplicate {
			return Grant{}, false, invalid("duplicate_demand_target", "demand.target_id", "must occur at most once")
		}
		demanded[targetID] = struct{}{}
	}
	if len(demanded) == 0 || len(scheduler.targetOrder) == 0 {
		return Grant{}, false, nil
	}
	if scheduler.occupiedCapacityLocked() >= scheduler.ledger.FleetMaximum() {
		return Grant{}, false, nil
	}
	if scheduler.nextGrantID == 0 {
		return Grant{}, false, invalid("grant_id_exhausted", "grant.id", "cannot allocate another unique grant id")
	}

	for offset := 0; offset < len(scheduler.targetOrder); offset++ {
		targetIndex := (scheduler.nextTarget + offset) % len(scheduler.targetOrder)
		targetID := scheduler.targetOrder[targetIndex]
		if _, requested := demanded[targetID]; !requested {
			continue
		}

		candidates := scheduler.candidatesLocked(scheduler.targets[targetID])
		if len(candidates) == 0 {
			continue
		}
		slot := candidates[0].slot
		if err := scheduler.ledger.Claim(slot, domain.SlotOwner{TargetID: targetID}); err != nil {
			return Grant{}, false, fmt.Errorf("claim ranked slot: %w", err)
		}

		grant := Grant{
			ID:       scheduler.nextGrantID,
			TargetID: targetID,
			Slot:     slot,
		}
		scheduler.nextGrantID++
		scheduler.grants[grant.ID] = grant
		scheduler.grantBySlot[grant.Slot] = grant.ID
		scheduler.nextTarget = (targetIndex + 1) % len(scheduler.targetOrder)
		return grant, true, nil
	}
	return Grant{}, false, nil
}

type candidate struct {
	slot            domain.SlotKey
	activeRunners   int
	availableMemory uint64
	packageCached   bool
}

func (scheduler *Scheduler) candidatesLocked(target TargetSpec) []candidate {
	claimedByNode := make(map[domain.NodeID]int)
	for slot := range scheduler.grantBySlot {
		claimedByNode[slot.NodeID]++
	}

	slots := scheduler.ledger.Slots()
	candidates := make([]candidate, 0, len(slots))
	for _, slot := range slots {
		if slot.Owner != nil {
			continue
		}
		node, exists := scheduler.nodes[slot.Key.NodeID]
		if !exists ||
			!eligible(node.snapshot, target.Profile) ||
			!scheduler.nodeOwnershipAlignedLocked(slot.Key.NodeID) {
			continue
		}
		activeRunners := len(node.snapshot.ActiveExecutions)
		// Reservations can precede the Agent's running observation. Taking the
		// larger count spreads those pending claims without double-counting a
		// runner once the matching execution appears in the snapshot.
		if claimedByNode[slot.Key.NodeID] > activeRunners {
			activeRunners = claimedByNode[slot.Key.NodeID]
		}
		_, packageCached := node.cached[target.RunnerPackage]
		if target.RunnerPackage == "" {
			packageCached = false
		}
		candidates = append(candidates, candidate{
			slot:            slot.Key,
			activeRunners:   activeRunners,
			availableMemory: node.snapshot.AvailableMemoryBytes,
			packageCached:   packageCached,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if left.activeRunners != right.activeRunners {
			return left.activeRunners < right.activeRunners
		}
		if left.availableMemory != right.availableMemory {
			return left.availableMemory > right.availableMemory
		}
		if left.packageCached != right.packageCached {
			return left.packageCached
		}
		if left.slot.NodeID != right.slot.NodeID {
			return left.slot.NodeID < right.slot.NodeID
		}
		return left.slot.Index < right.slot.Index
	})
	return candidates
}

func eligible(snapshot NodeSnapshot, profile domain.RunnerProfile) bool {
	if snapshot.Node.AdministrativeState != domain.NodeActive ||
		snapshot.Node.ObservedState != domain.NodeOnline ||
		!snapshot.Reconciled ||
		!snapshot.NativeReady {
		return false
	}
	if profile.OS != nil && snapshot.Node.OS != *profile.OS {
		return false
	}
	if profile.Architecture != nil && snapshot.Node.Architecture != *profile.Architecture {
		return false
	}
	return snapshot.AvailableMemoryBytes >= profile.MinAvailableMemoryBytes
}

// BindExecution attaches an execution without moving or re-claiming its slot.
// Repeating the exact binding is idempotent; a different execution fails closed.
func (scheduler *Scheduler) BindExecution(ref GrantRef, executionID domain.ExecutionID) (Grant, error) {
	if err := ref.validate(); err != nil {
		return Grant{}, err
	}
	if strings.TrimSpace(string(executionID)) == "" {
		return Grant{}, invalid("invalid_execution_id", "execution.id", "must not be empty")
	}

	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	grant, err := scheduler.lookupGrantIdentityLocked(ref)
	if err != nil {
		return Grant{}, err
	}
	if ref.ExecutionID != "" && ref.ExecutionID != grant.ExecutionID {
		return Grant{}, invalid("grant_reference_mismatch", "grant.execution_id", "does not match the active grant binding")
	}
	if grant.ExecutionID != "" {
		if grant.ExecutionID != executionID {
			return Grant{}, invalid("grant_execution_mismatch", "execution.id", "grant is already bound to another execution")
		}
		return grant, nil
	}
	for _, existing := range scheduler.grants {
		if existing.ID != grant.ID && existing.ExecutionID == executionID {
			return Grant{}, invalid("execution_already_bound", "execution.id", "is already bound to another active grant")
		}
	}
	if err := scheduler.ledger.BindExecution(grant.Slot, grant.TargetID, executionID); err != nil {
		return Grant{}, fmt.Errorf("bind execution to claimed slot: %w", err)
	}
	grant.ExecutionID = executionID
	scheduler.grants[grant.ID] = grant
	return grant, nil
}

// Release frees an active grant only when its complete identity still matches.
// A duplicate release is rejected because accepting it after slot reuse would
// make an old message capable of freeing a new owner's resource.
func (scheduler *Scheduler) Release(ref GrantRef) error {
	if err := ref.validate(); err != nil {
		return err
	}

	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	grant, err := scheduler.lookupGrantIdentityLocked(ref)
	if err != nil {
		return err
	}
	if grant.ExecutionID != ref.ExecutionID {
		return invalid("grant_reference_mismatch", "grant.execution_id", "does not match the active grant binding")
	}
	if scheduler.nodeReportsActiveExecutionLocked(grant.Slot.NodeID, grant.ExecutionID) {
		return invalid("execution_still_active", "grant.execution_id", "cannot release capacity while the node still reports the execution active")
	}
	owner := domain.SlotOwner{
		TargetID:    grant.TargetID,
		ExecutionID: grant.ExecutionID,
	}
	if err := scheduler.ledger.Release(grant.Slot, owner); err != nil {
		return fmt.Errorf("release claimed slot: %w", err)
	}
	delete(scheduler.grants, grant.ID)
	delete(scheduler.grantBySlot, grant.Slot)
	return nil
}

func (scheduler *Scheduler) lookupGrantIdentityLocked(ref GrantRef) (Grant, error) {
	grant, exists := scheduler.grants[ref.ID]
	if !exists {
		return Grant{}, invalid("grant_not_active", "grant.id", "does not identify an active grant")
	}
	if grant.ID != ref.ID || grant.TargetID != ref.TargetID || grant.Slot != ref.Slot {
		return Grant{}, invalid("grant_reference_mismatch", "grant", "does not match the active grant identity")
	}
	return grant, nil
}

// Capacity returns the target's currently backed advertised capacity.
func (scheduler *Scheduler) Capacity(targetID domain.TargetID) (int, error) {
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()
	if _, exists := scheduler.targets[targetID]; !exists {
		return 0, invalid("target_not_found", "target.id", "does not identify a configured target")
	}
	return scheduler.capacityLocked(targetID), nil
}

// Capacities returns every configured target in stable TargetID order,
// including explicit zero-capacity entries.
func (scheduler *Scheduler) Capacities() []TargetCapacity {
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()
	capacities := make([]TargetCapacity, 0, len(scheduler.targetOrder))
	for _, targetID := range scheduler.targetOrder {
		capacities = append(capacities, TargetCapacity{
			TargetID:   targetID,
			Advertised: scheduler.capacityLocked(targetID),
		})
	}
	return capacities
}

func (scheduler *Scheduler) capacityLocked(targetID domain.TargetID) int {
	capacity := 0
	for _, grant := range scheduler.grants {
		if grant.TargetID == targetID {
			capacity++
		}
	}
	return capacity
}

func (scheduler *Scheduler) nodeOwnershipAlignedLocked(nodeID domain.NodeID) bool {
	node, exists := scheduler.nodes[nodeID]
	if !exists {
		return false
	}
	for _, executionID := range node.snapshot.ActiveExecutions {
		if !scheduler.executionBackedOnNodeLocked(nodeID, executionID) {
			return false
		}
	}
	return true
}

func (scheduler *Scheduler) executionBackedOnNodeLocked(nodeID domain.NodeID, executionID domain.ExecutionID) bool {
	for _, grant := range scheduler.grants {
		if grant.Slot.NodeID == nodeID && grant.ExecutionID == executionID {
			return true
		}
	}
	return false
}

func (scheduler *Scheduler) nodeReportsActiveExecutionLocked(nodeID domain.NodeID, executionID domain.ExecutionID) bool {
	if executionID == "" {
		return false
	}
	node, exists := scheduler.nodes[nodeID]
	if !exists {
		return false
	}
	for _, activeExecutionID := range node.snapshot.ActiveExecutions {
		if activeExecutionID == executionID {
			return true
		}
	}
	return false
}

func (scheduler *Scheduler) unbackedActiveCountLocked() int {
	count := 0
	for nodeID, node := range scheduler.nodes {
		for _, executionID := range node.snapshot.ActiveExecutions {
			if !scheduler.executionBackedOnNodeLocked(nodeID, executionID) {
				count++
			}
		}
	}
	return count
}

func (scheduler *Scheduler) occupiedCapacityLocked() int {
	return scheduler.ledger.Claimed() + scheduler.unbackedActiveCountLocked()
}

// Grants returns a copy sorted by monotonically increasing GrantID.
func (scheduler *Scheduler) Grants() []Grant {
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()
	grants := make([]Grant, 0, len(scheduler.grants))
	for _, grant := range scheduler.grants {
		grants = append(grants, grant)
	}
	sort.Slice(grants, func(i, j int) bool {
		return grants[i].ID < grants[j].ID
	})
	return grants
}

// FleetMaximum returns the concrete global slot bound.
func (scheduler *Scheduler) FleetMaximum() int {
	return scheduler.ledger.FleetMaximum()
}

// ActiveGrantCount returns reserved, bound, and temporary grants.
func (scheduler *Scheduler) ActiveGrantCount() int {
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()
	return len(scheduler.grants)
}

// OccupiedCapacity includes unbacked observed runtimes conservatively. Those
// runtimes are never advertised, but they still reduce the fleet safety bound.
func (scheduler *Scheduler) OccupiedCapacity() int {
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()
	return scheduler.occupiedCapacityLocked()
}

// Validate checks that scheduler indexes and the domain ledger describe the
// same one-owner-per-slot state.
func (scheduler *Scheduler) Validate() error {
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()

	if err := scheduler.ledger.Validate(); err != nil {
		return fmt.Errorf("scheduler invariant violated: ledger: %w", err)
	}
	if len(scheduler.grants) != len(scheduler.grantBySlot) {
		return fmt.Errorf("scheduler invariant violated: grant indexes differ in size")
	}
	if scheduler.ledger.Claimed() != len(scheduler.grants) {
		return fmt.Errorf("scheduler invariant violated: ledger claimed count differs from grants")
	}
	if len(scheduler.grants) > scheduler.ledger.FleetMaximum() {
		return fmt.Errorf("scheduler invariant violated: grants exceed fleet maximum")
	}
	if scheduler.nextGrantID == 0 {
		return fmt.Errorf("scheduler invariant violated: grant id space is exhausted")
	}
	if len(scheduler.targetOrder) > 0 && (scheduler.nextTarget < 0 || scheduler.nextTarget >= len(scheduler.targetOrder)) {
		return fmt.Errorf("scheduler invariant violated: target cursor is out of range")
	}

	ledgerSlots := make(map[domain.SlotKey]*domain.SlotOwner)
	for _, slot := range scheduler.ledger.Slots() {
		if slot.Owner == nil {
			ledgerSlots[slot.Key] = nil
			continue
		}
		owner := *slot.Owner
		ledgerSlots[slot.Key] = &owner
	}

	for id, grant := range scheduler.grants {
		if grant.ID != id || grant.ID == 0 {
			return fmt.Errorf("scheduler invariant violated: grant id index mismatch")
		}
		if _, exists := scheduler.targets[grant.TargetID]; !exists {
			return fmt.Errorf("scheduler invariant violated: grant references unknown target")
		}
		indexedID, exists := scheduler.grantBySlot[grant.Slot]
		if !exists || indexedID != grant.ID {
			return fmt.Errorf("scheduler invariant violated: slot index does not reference grant")
		}
		owner, exists := ledgerSlots[grant.Slot]
		if !exists || owner == nil {
			return fmt.Errorf("scheduler invariant violated: grant slot is free or missing in ledger")
		}
		wantOwner := domain.SlotOwner{TargetID: grant.TargetID, ExecutionID: grant.ExecutionID}
		if *owner != wantOwner {
			return fmt.Errorf("scheduler invariant violated: ledger owner differs from grant")
		}
	}
	executions := make(map[domain.ExecutionID]GrantID)
	for _, grant := range scheduler.grants {
		if grant.ExecutionID == "" {
			continue
		}
		if existing, duplicate := executions[grant.ExecutionID]; duplicate {
			return fmt.Errorf("scheduler invariant violated: execution is bound to grants %d and %d", existing, grant.ID)
		}
		executions[grant.ExecutionID] = grant.ID
	}
	for slot, owner := range ledgerSlots {
		if owner == nil {
			continue
		}
		if _, exists := scheduler.grantBySlot[slot]; !exists {
			return fmt.Errorf("scheduler invariant violated: ledger owner lacks scheduler grant")
		}
	}
	return nil
}
