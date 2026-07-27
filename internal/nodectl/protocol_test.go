package nodectl_test

import (
	"encoding/json"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/nodectl"
)

func TestStatusJSONIncludesEligibleTargetsWhenPresentAndOmitsWhenAbsent(t *testing.T) {
	absent := nodectl.Status{ProtocolVersion: nodectl.ProtocolVersion, NodeID: "node-1"}
	absentPayload, err := json.Marshal(absent)
	if err != nil {
		t.Fatal(err)
	}
	var absentRaw map[string]any
	if err := json.Unmarshal(absentPayload, &absentRaw); err != nil {
		t.Fatal(err)
	}
	if _, present := absentRaw["eligibleTargets"]; present {
		t.Fatalf("absent EligibleTargets was encoded: %s", absentPayload)
	}

	populated := nodectl.Status{
		ProtocolVersion: nodectl.ProtocolVersion,
		NodeID:          "node-1",
		EligibleTargets: []nodectl.EligibleTarget{{
			TargetID:     "target-1",
			ScopeKind:    domain.TargetRepository,
			Scope:        "owner/repo",
			ScaleSetName: "scale-set",
			Excluded:     true,
		}},
	}
	populatedPayload, err := json.Marshal(populated)
	if err != nil {
		t.Fatal(err)
	}
	var decoded nodectl.Status
	if err := json.Unmarshal(populatedPayload, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.EligibleTargets) != 1 {
		t.Fatalf("decoded EligibleTargets = %#v, want one entry", decoded.EligibleTargets)
	}
	got := decoded.EligibleTargets[0]
	if got.TargetID != "target-1" || got.ScopeKind != domain.TargetRepository ||
		got.Scope != "owner/repo" || got.ScaleSetName != "scale-set" || !got.Excluded {
		t.Fatalf("decoded EligibleTarget = %#v", got)
	}
}
