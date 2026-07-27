package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/nodectl"
)

func TestWriteAvailabilityTextRendersTargetsSectionWhenPresent(t *testing.T) {
	var out bytes.Buffer
	writeAvailabilityText(&out, nodectl.Status{
		NodeID:              "node-1",
		Intent:              domain.AvailabilityAccepting,
		ControllerConnected: true,
		NativeRunnerReady:   true,
		EligibleTargets: []nodectl.EligibleTarget{
			{TargetID: "target-1", ScopeKind: domain.TargetRepository, Scope: "owner/repo", ScaleSetName: "scale-set"},
			// Locally excluded and adopted by the controller: a settled exclusion
			// rather than one still syncing.
			{
				TargetID: "target-2", ScopeKind: domain.TargetOrganization, Scope: "owner",
				ScaleSetName: "scale-set-2", Excluded: true, LocallyExcluded: true,
			},
		},
	})
	rendered := out.String()
	if !strings.Contains(rendered, "Targets:    2 scope(s)") {
		t.Fatalf("rendered output missing targets header: %q", rendered)
	}
	if !strings.Contains(rendered, "owner/repo [repository]") {
		t.Fatalf("rendered output missing repository scope line: %q", rendered)
	}
	if !strings.Contains(rendered, "owner [organization] (excluded)") {
		t.Fatalf("rendered output missing excluded organization scope line: %q", rendered)
	}
}

func TestWriteAvailabilityTextRendersNoneReportedWhenTargetsAreAbsentOrEmpty(t *testing.T) {
	for _, targets := range [][]nodectl.EligibleTarget{nil, {}} {
		var out bytes.Buffer
		writeAvailabilityText(&out, nodectl.Status{
			NodeID:          "node-1",
			Intent:          domain.AvailabilityAccepting,
			EligibleTargets: targets,
		})
		if !strings.Contains(out.String(), "Targets:    none reported") {
			t.Fatalf("rendered output = %q, want the none-reported line", out.String())
		}
	}
}
