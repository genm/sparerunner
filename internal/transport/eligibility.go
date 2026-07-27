package transport

import (
	"strings"

	"github.com/genm/tewake/internal/domain"
)

// MaxEligibleTargets bounds the informational list carried on a heartbeat
// acknowledgement. It exists only to reject a malformed or hostile payload
// before it reaches local Agent state; it is not a product quota.
const MaxEligibleTargets = 256

// EligibleTarget is the non-secret summary of a private GitHub Target whose
// Runner Profile currently matches a node's reported platform. It carries no
// installation ID, credential, or scheduling decision, only what a node owner
// needs to see which org/repo scopes could route work to this computer.
// Eligibility is visibility, not a capacity guarantee: it says nothing about
// whether a free slot currently exists.
type EligibleTarget struct {
	TargetID     domain.TargetID        `json:"targetId"`
	ScopeKind    domain.TargetScopeKind `json:"scopeKind"`
	Scope        string                 `json:"scope"`
	ScaleSetName string                 `json:"scaleSetName"`
	// Excluded reports whether the node owner has withdrawn this otherwise
	// eligible scope. The controller does not yet act on it (that adoption
	// lands in a later PR); this PR only carries the field so the wire shape
	// is stable before the exclusion set is consulted.
	Excluded bool `json:"excluded"`
}

func (target EligibleTarget) Validate() error {
	if strings.TrimSpace(string(target.TargetID)) == "" {
		return ErrInvalidCommand
	}
	switch target.ScopeKind {
	case domain.TargetRepository, domain.TargetOrganization:
	default:
		return ErrInvalidCommand
	}
	if strings.TrimSpace(target.Scope) == "" || strings.TrimSpace(target.ScaleSetName) == "" {
		return ErrInvalidCommand
	}
	return nil
}

// ValidateEligibleTargets fails closed on an oversized list, an invalid entry,
// or a duplicate TargetID. Eligible scopes are a set observed at one instant,
// not a replayable log, so a duplicate identity is corruption rather than a
// legitimate repeat.
func ValidateEligibleTargets(targets []EligibleTarget) error {
	if len(targets) > MaxEligibleTargets {
		return ErrInvalidCommand
	}
	seen := make(map[domain.TargetID]struct{}, len(targets))
	for _, target := range targets {
		if err := target.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[target.TargetID]; duplicate {
			return ErrInvalidCommand
		}
		seen[target.TargetID] = struct{}{}
	}
	return nil
}

// ValidateExcludedTargets applies the same fail-closed bound and duplicate
// rejection as ValidateEligibleTargets to the owner-editable exclusion set
// carried on AgentSnapshot and AgentHeartbeat. It shares MaxEligibleTargets
// as its bound because both lists are sized by the same configured Target
// population, not by an independent product quota.
func ValidateExcludedTargets(targetIDs []domain.TargetID) error {
	if len(targetIDs) > MaxEligibleTargets {
		return ErrInvalidCommand
	}
	seen := make(map[domain.TargetID]struct{}, len(targetIDs))
	for _, targetID := range targetIDs {
		if strings.TrimSpace(string(targetID)) == "" {
			return ErrInvalidCommand
		}
		if _, duplicate := seen[targetID]; duplicate {
			return ErrInvalidCommand
		}
		seen[targetID] = struct{}{}
	}
	return nil
}
