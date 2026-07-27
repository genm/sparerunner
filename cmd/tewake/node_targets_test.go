package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/nodectl"
)

func TestWriteNodeTargetsTextRendersEveryExclusionState(t *testing.T) {
	var out bytes.Buffer
	writeNodeTargetsText(&out, nodectl.Status{
		NodeID:              "node-1",
		ControllerConnected: true,
		EligibleTargets: []nodectl.EligibleTarget{
			{TargetID: "t-served", ScopeKind: domain.TargetRepository, Scope: "owner/served", ScaleSetName: "s"},
			// Locally excluded and adopted: settled.
			{
				TargetID: "t-settled", ScopeKind: domain.TargetRepository, Scope: "owner/settled",
				ScaleSetName: "s", Excluded: true, LocallyExcluded: true,
			},
			// Locally excluded but not yet adopted. Excluding is subtractive, so
			// it is already in force locally and must not read as served.
			{
				TargetID: "t-syncing", ScopeKind: domain.TargetOrganization, Scope: "owner-syncing",
				ScaleSetName: "s", LocallyExcluded: true, Pending: true,
			},
			// Re-included locally, still adopted as excluded by the controller.
			// Including is additive, so it stays pending and unserved.
			{
				TargetID: "t-including", ScopeKind: domain.TargetRepository, Scope: "owner/including",
				ScaleSetName: "s", Excluded: true, Pending: true,
			},
		},
		UnknownExclusions: []domain.TargetID{"t-offline"},
	})
	rendered := out.String()
	for _, want := range []string{
		"Targets:    4 scope(s)",
		"owner/served [repository]\n",
		"owner/settled [repository] (excluded)",
		"owner-syncing [organization] (excluded — syncing)",
		"owner/including [repository] (include pending)",
		"t-offline (excluded, not currently eligible)",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered output missing %q: %q", want, rendered)
		}
	}
}

func TestWriteAvailabilityTextNamesTheScopeOfRunningWork(t *testing.T) {
	var out bytes.Buffer
	writeAvailabilityText(&out, nodectl.Status{
		NodeID:              "node-1",
		Intent:              domain.AvailabilityAccepting,
		ControllerConnected: true,
		NativeRunnerReady:   true,
		RunningExecutions: []nodectl.RunningExecution{
			{
				ExecutionID: "execution-1", State: domain.ExecutionRunning,
				TargetID: "t-1", Scope: "owner/repo", ScopeKind: domain.TargetRepository,
			},
			// An execution admitted before attribution existed renders without a
			// scope rather than with an invented one.
			{ExecutionID: "execution-2", State: domain.ExecutionRunning},
		},
	})
	rendered := out.String()
	if !strings.Contains(rendered, "execution-1 (running) owner/repo [repository]") {
		t.Fatalf("rendered output missing attributed execution: %q", rendered)
	}
	if !strings.Contains(rendered, "execution-2 (running)\n") {
		t.Fatalf("rendered output missing unattributed execution: %q", rendered)
	}
}

func TestNodeTargetsCommandRejectsAmbiguousInvocations(t *testing.T) {
	for name, args := range map[string][]string{
		"both flags":        {"node", "targets", "--exclude", "--include", "target-1"},
		"exclude no target": {"node", "targets", "--exclude"},
		"include no target": {"node", "targets", "--include"},
		"target no flag":    {"node", "targets", "target-1"},
		"too many targets":  {"node", "targets", "--exclude", "target-1", "target-2"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			// Refusing is the point: silently choosing one of two opposite
			// mutations would change fleet capacity by accident.
			if err := run(args, &stdout, &stderr); err == nil {
				t.Fatalf("ambiguous invocation accepted: %s", stdout.String())
			}
		})
	}
}

func TestNodeTargetsCommandEmitsTheFailureDocumentOnStdout(t *testing.T) {
	var stdout, stderr bytes.Buffer
	// No agent is listening, so this must exit non-zero with exactly one
	// machine-readable document on stdout for a launcher to parse.
	err := run([]string{
		"node", "targets", "--json", "--state-dir", t.TempDir(),
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("targets succeeded without a reachable agent")
	}
	var failure availabilityFailure
	if decodeErr := json.Unmarshal(stdout.Bytes(), &failure); decodeErr != nil {
		t.Fatalf("failure document = %q: %v", stdout.String(), decodeErr)
	}
	if failure.OK || failure.ErrorClass == "" ||
		failure.ProtocolVersion != nodectl.ProtocolVersion {
		t.Fatalf("failure document = %+v", failure)
	}
}
