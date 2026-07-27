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

	targets := []nodectl.EligibleTarget{{
		TargetID:     "target-1",
		ScopeKind:    domain.TargetRepository,
		Scope:        "owner/repo",
		ScaleSetName: "scale-set",
		Excluded:     true,
	}}
	populated := nodectl.Status{
		ProtocolVersion: nodectl.ProtocolVersion,
		NodeID:          "node-1",
		EligibleTargets: &targets,
	}
	populatedPayload, err := json.Marshal(populated)
	if err != nil {
		t.Fatal(err)
	}
	var decoded nodectl.Status
	if err := json.Unmarshal(populatedPayload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.EligibleTargets == nil || len(*decoded.EligibleTargets) != 1 {
		t.Fatalf("decoded EligibleTargets = %#v, want one entry", decoded.EligibleTargets)
	}
	got := (*decoded.EligibleTargets)[0]
	if got.TargetID != "target-1" || got.ScopeKind != domain.TargetRepository ||
		got.Scope != "owner/repo" || got.ScaleSetName != "scale-set" || !got.Excluded {
		t.Fatalf("decoded EligibleTarget = %#v", got)
	}
}

// TestStatusJSONDistinguishesConfirmedEmptyFromNeverReported is the direct
// regression test for the bug a real end-to-end run caught: a plain
// `omitempty` slice cannot tell "a heartbeat confirmed zero eligible
// Targets" apart from "no heartbeat has reported yet", because
// encoding/json's `omitempty` on a slice checks length, not nilness. Both
// previously encoded as an absent field.
func TestStatusJSONDistinguishesConfirmedEmptyFromNeverReported(t *testing.T) {
	empty := []nodectl.EligibleTarget{}
	confirmedEmpty := nodectl.Status{
		ProtocolVersion: nodectl.ProtocolVersion,
		NodeID:          "node-1",
		EligibleTargets: &empty,
	}
	payload, err := json.Marshal(confirmedEmpty)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	value, present := raw["eligibleTargets"]
	if !present {
		t.Fatalf("confirmed-empty EligibleTargets was omitted: %s", payload)
	}
	if list, ok := value.([]any); !ok || len(list) != 0 {
		t.Fatalf("confirmed-empty EligibleTargets = %#v, want an empty array", value)
	}

	var decoded nodectl.Status
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.EligibleTargets == nil {
		t.Fatal("decoded confirmed-empty EligibleTargets is nil, want a non-nil pointer to an empty slice")
	}
	if len(decoded.Targets()) != 0 {
		t.Fatalf("decoded confirmed-empty EligibleTargets = %#v, want empty", decoded.Targets())
	}
}
