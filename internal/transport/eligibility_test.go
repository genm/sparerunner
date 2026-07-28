package transport

import (
	"fmt"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
)

func validEligibleTarget(id string) EligibleTarget {
	return EligibleTarget{
		TargetID:     domain.TargetID(id),
		ScopeKind:    domain.TargetRepository,
		Scope:        "owner/repo",
		ScaleSetName: "scale-set",
	}
}

func TestEligibleTargetValidateRejectsMissingFieldsAndBadScopeKind(t *testing.T) {
	tests := []struct {
		name   string
		target EligibleTarget
	}{
		{name: "empty target id", target: EligibleTarget{ScopeKind: domain.TargetRepository, Scope: "owner/repo", ScaleSetName: "scale-set"}},
		{name: "blank target id", target: EligibleTarget{TargetID: "  ", ScopeKind: domain.TargetRepository, Scope: "owner/repo", ScaleSetName: "scale-set"}},
		{name: "unknown scope kind", target: EligibleTarget{TargetID: "target-1", ScopeKind: "unknown", Scope: "owner/repo", ScaleSetName: "scale-set"}},
		{name: "empty scope", target: EligibleTarget{TargetID: "target-1", ScopeKind: domain.TargetRepository, ScaleSetName: "scale-set"}},
		{name: "empty scale set name", target: EligibleTarget{TargetID: "target-1", ScopeKind: domain.TargetRepository, Scope: "owner/repo"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.target.Validate(); err == nil {
				t.Fatalf("invalid target accepted: %+v", test.target)
			}
		})
	}
	if err := validEligibleTarget("target-1").Validate(); err != nil {
		t.Fatalf("valid target rejected: %v", err)
	}
}

func TestValidateEligibleTargetsRejectsDuplicateAndOversizedLists(t *testing.T) {
	if err := ValidateEligibleTargets(nil); err != nil {
		t.Fatalf("nil list rejected: %v", err)
	}
	if err := ValidateEligibleTargets([]EligibleTarget{
		validEligibleTarget("target-1"), validEligibleTarget("target-2"),
	}); err != nil {
		t.Fatalf("distinct targets rejected: %v", err)
	}
	if err := ValidateEligibleTargets([]EligibleTarget{
		validEligibleTarget("target-1"), validEligibleTarget("target-1"),
	}); err == nil {
		t.Fatal("duplicate target ID accepted")
	}
	oversized := make([]EligibleTarget, MaxEligibleTargets+1)
	for index := range oversized {
		oversized[index] = validEligibleTarget(fmt.Sprintf("target-%d", index))
	}
	if err := ValidateEligibleTargets(oversized); err == nil {
		t.Fatal("oversized list accepted")
	}
	// A realistic large-but-valid list at the exact bound must still pass, so
	// the bound rejects only what actually exceeds it.
	atBound := make([]EligibleTarget, MaxEligibleTargets)
	for index := range atBound {
		atBound[index] = validEligibleTarget(fmt.Sprintf("target-%d", index))
	}
	if err := ValidateEligibleTargets(atBound); err != nil {
		t.Fatalf("list at exact bound rejected: %v", err)
	}
}

func TestValidateExcludedTargetsRejectsBlankDuplicateAndOversizedLists(t *testing.T) {
	if err := ValidateExcludedTargets(nil); err != nil {
		t.Fatalf("nil list rejected: %v", err)
	}
	if err := ValidateExcludedTargets([]domain.TargetID{"target-1", "target-2"}); err != nil {
		t.Fatalf("distinct target IDs rejected: %v", err)
	}
	if err := ValidateExcludedTargets([]domain.TargetID{"target-1", "target-1"}); err == nil {
		t.Fatal("duplicate target ID accepted")
	}
	if err := ValidateExcludedTargets([]domain.TargetID{"  "}); err == nil {
		t.Fatal("blank target ID accepted")
	}
	oversized := make([]domain.TargetID, MaxEligibleTargets+1)
	for index := range oversized {
		oversized[index] = domain.TargetID(fmt.Sprintf("target-%d", index))
	}
	if err := ValidateExcludedTargets(oversized); err == nil {
		t.Fatal("oversized list accepted")
	}
	atBound := make([]domain.TargetID, MaxEligibleTargets)
	for index := range atBound {
		atBound[index] = domain.TargetID(fmt.Sprintf("target-%d", index))
	}
	if err := ValidateExcludedTargets(atBound); err != nil {
		t.Fatalf("list at exact bound rejected: %v", err)
	}
}
