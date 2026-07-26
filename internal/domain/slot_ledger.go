package domain

import (
	"fmt"
	"sort"
	"sync"
)

// SlotKey identifies one physical runner slot. Slots are never inferred from a
// counter, so concurrent target capacity cannot refer to the same resource.
type SlotKey struct {
	NodeID NodeID
	Index  int
}

// SlotOwner is the reservation/execution currently backed by a concrete slot.
// ExecutionID may be empty while the controller has only reserved a target slot.
type SlotOwner struct {
	TargetID    TargetID
	ExecutionID ExecutionID
}

func (o SlotOwner) Validate() error {
	return required(string(o.TargetID), "slot.owner.target_id")
}

type Slot struct {
	Key   SlotKey
	Owner *SlotOwner
}

// SlotLedger provides the in-memory invariant boundary used by a future durable
// ledger. It is safe for concurrent schedulers, but persistence remains external.
type SlotLedger struct {
	mu           sync.RWMutex
	slots        map[SlotKey]Slot
	fleetMaximum int
	claimed      int
}

func NewSlotLedger(nodes []Node, fleetMaximum *int) (*SlotLedger, error) {
	ledger := &SlotLedger{slots: make(map[SlotKey]Slot)}
	nodeIDs := make(map[NodeID]struct{}, len(nodes))
	for _, node := range nodes {
		if err := node.Validate(); err != nil {
			return nil, err
		}
		if _, exists := nodeIDs[node.ID]; exists {
			return nil, invalid("duplicate_node_id", "nodes", "contains duplicate node id")
		}
		nodeIDs[node.ID] = struct{}{}
		for index := 0; index < node.MaxRunners; index++ {
			key := SlotKey{NodeID: node.ID, Index: index}
			ledger.slots[key] = Slot{Key: key}
		}
	}
	capacity := len(ledger.slots)
	if fleetMaximum == nil {
		ledger.fleetMaximum = capacity
		return ledger, nil
	}
	if *fleetMaximum < 1 {
		return nil, invalid("invalid_fleet_maximum", "fleet_maximum", "must be at least one")
	}
	if *fleetMaximum > capacity {
		return nil, invalid("fleet_maximum_exceeds_slots", "fleet_maximum", "must not exceed the concrete slot capacity")
	}
	ledger.fleetMaximum = *fleetMaximum
	return ledger, nil
}

func (l *SlotLedger) Claim(key SlotKey, owner SlotOwner) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	slot, exists := l.slots[key]
	if !exists {
		return invalid("slot_not_found", "slot", "does not identify a configured concrete slot")
	}
	if slot.Owner != nil {
		return invalid("slot_already_claimed", "slot", "is already owned")
	}
	if l.claimed >= l.fleetMaximum {
		return invalid("fleet_capacity_exhausted", "fleet_maximum", "has no remaining backed capacity")
	}
	ownerCopy := owner
	slot.Owner = &ownerCopy
	l.slots[key] = slot
	l.claimed++
	return nil
}

// BindExecution enriches an existing target reservation without freeing or moving
// the underlying slot, which prevents an execution start from double-claiming it.
func (l *SlotLedger) BindExecution(key SlotKey, targetID TargetID, executionID ExecutionID) error {
	if err := required(string(executionID), "slot.owner.execution_id"); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	slot, exists := l.slots[key]
	if !exists {
		return invalid("slot_not_found", "slot", "does not identify a configured concrete slot")
	}
	if slot.Owner == nil {
		return invalid("slot_not_claimed", "slot", "must be reserved before binding an execution")
	}
	if slot.Owner.TargetID != targetID {
		return invalid("slot_owner_mismatch", "slot.owner.target_id", "does not match the existing reservation")
	}
	if slot.Owner.ExecutionID != "" && slot.Owner.ExecutionID != executionID {
		return invalid("slot_execution_already_bound", "slot.owner.execution_id", "is already bound to another execution")
	}
	slot.Owner.ExecutionID = executionID
	l.slots[key] = slot
	return nil
}

func (l *SlotLedger) Release(key SlotKey, owner SlotOwner) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	slot, exists := l.slots[key]
	if !exists {
		return invalid("slot_not_found", "slot", "does not identify a configured concrete slot")
	}
	if slot.Owner == nil {
		return invalid("slot_not_claimed", "slot", "is already free")
	}
	if *slot.Owner != owner {
		return invalid("slot_owner_mismatch", "slot.owner", "does not match the current owner")
	}
	slot.Owner = nil
	l.slots[key] = slot
	l.claimed--
	return nil
}

func (l *SlotLedger) FleetMaximum() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.fleetMaximum
}

func (l *SlotLedger) Claimed() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.claimed
}

func (l *SlotLedger) Slots() []Slot {
	l.mu.RLock()
	defer l.mu.RUnlock()
	slots := make([]Slot, 0, len(l.slots))
	for _, slot := range l.slots {
		if slot.Owner != nil {
			owner := *slot.Owner
			slot.Owner = &owner
		}
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool {
		if slots[i].Key.NodeID == slots[j].Key.NodeID {
			return slots[i].Key.Index < slots[j].Key.Index
		}
		return slots[i].Key.NodeID < slots[j].Key.NodeID
	})
	return slots
}

// Validate makes every ledger invariant executable for property tests and callers
// recovering from a persisted representation.
func (l *SlotLedger) Validate() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.fleetMaximum < 0 {
		return fmt.Errorf("domain ledger invalid: negative fleet maximum")
	}
	if l.fleetMaximum > len(l.slots) {
		return fmt.Errorf("domain ledger invalid: fleet maximum exceeds slots")
	}
	claimed := 0
	for key, slot := range l.slots {
		if key != slot.Key {
			return fmt.Errorf("domain ledger invalid: slot key mismatch")
		}
		if slot.Owner != nil {
			if err := slot.Owner.Validate(); err != nil {
				return err
			}
			claimed++
		}
	}
	if claimed != l.claimed {
		return fmt.Errorf("domain ledger invalid: claimed count mismatch")
	}
	if claimed > l.fleetMaximum {
		return fmt.Errorf("domain ledger invalid: capacity overflow")
	}
	return nil
}
