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
	targets := []nodectl.EligibleTarget{
		{TargetID: "target-1", ScopeKind: domain.TargetRepository, Scope: "owner/repo", ScaleSetName: "scale-set"},
		// Locally excluded and adopted by the controller: a settled exclusion
		// rather than one still syncing.
		{
			TargetID: "target-2", ScopeKind: domain.TargetOrganization, Scope: "owner",
			ScaleSetName: "scale-set-2", Excluded: true, LocallyExcluded: true,
		},
	}
	writeAvailabilityText(&out, nodectl.Status{
		NodeID:              "node-1",
		Intent:              domain.AvailabilityAccepting,
		ControllerConnected: true,
		NativeRunnerReady:   true,
		EligibleTargets:     &targets,
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

// TestWriteAvailabilityTextRendersSharedRunnerIdentityOnlyWhenReported pins the
// asymmetry: the weaker mode must always be named, and the isolated mode must
// print nothing rather than a line an operator could misread as a guarantee.
func TestWriteAvailabilityTextRendersSharedRunnerIdentityOnlyWhenReported(t *testing.T) {
	var shared bytes.Buffer
	writeAvailabilityText(&shared, nodectl.Status{
		NodeID:               "node-1",
		Intent:               domain.AvailabilityAccepting,
		ControllerConnected:  true,
		NativeRunnerReady:    true,
		SharedRunnerIdentity: true,
	})
	const line = "Isolation:  shared runner identity " +
		"(jobs run as the agent user; no UID isolation)"
	if !strings.Contains(shared.String(), line) {
		t.Fatalf("rendered output missing the isolation line: %q", shared.String())
	}

	var isolated bytes.Buffer
	writeAvailabilityText(&isolated, nodectl.Status{
		NodeID:               "node-1",
		Intent:               domain.AvailabilityAccepting,
		ControllerConnected:  true,
		NativeRunnerReady:    true,
		SharedRunnerIdentity: false,
	})
	if strings.Contains(isolated.String(), "Isolation:") {
		t.Fatalf("isolated node rendered an isolation line: %q", isolated.String())
	}
}

func TestWriteAvailabilityTextRendersNoneReportedWhenTargetsAreAbsentOrEmpty(t *testing.T) {
	empty := []nodectl.EligibleTarget{}
	for _, targets := range []*[]nodectl.EligibleTarget{nil, &empty} {
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
