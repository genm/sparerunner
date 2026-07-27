//go:build unix

package nodectl_test

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/nodectl"
)

func TestProtocolVersionIsTwoForThePerTargetContract(t *testing.T) {
	// The version is exact-matched in both directions with no pre-1.0 shim, so
	// a stale desktop client fails loudly instead of rendering a state it does
	// not understand.
	if nodectl.ProtocolVersion != 2 {
		t.Fatalf("protocol version = %d, want 2", nodectl.ProtocolVersion)
	}
}

func TestClientExcludesAndIncludesTargetsThroughTheEndpoint(t *testing.T) {
	controller := &fakeController{intent: domain.AvailabilityAccepting}
	directory := startServer(t, controller, selfAuthorizer(t))
	client := nodectl.Client{StateDirectory: directory, Source: nodectl.SourceTray}

	if _, err := client.Exclude("target-1"); err != nil {
		t.Fatalf("exclude: %v", err)
	}
	if excluded := controller.exclusions(); len(excluded) != 1 || excluded[0] != "target-1" {
		t.Fatalf("exclusions after exclude = %v", excluded)
	}
	status, err := client.Include("target-1")
	if err != nil {
		t.Fatalf("include: %v", err)
	}
	if len(controller.exclusions()) != 0 {
		t.Fatalf("exclusions after include = %v", controller.exclusions())
	}
	if status.ProtocolVersion != nodectl.ProtocolVersion {
		t.Fatalf("status protocol version = %d", status.ProtocolVersion)
	}

	// Listing is a read: it must not change anything.
	before := len(controller.exclusions())
	if _, err := client.Targets(); err != nil {
		t.Fatalf("targets: %v", err)
	}
	if len(controller.exclusions()) != before {
		t.Fatal("listing targets mutated the exclusion set")
	}
}

func TestRequestValidationPartitionsTheTargetIDField(t *testing.T) {
	long := strings.Repeat("x", domain.MaxTargetIDBytes+1)
	tests := map[string]struct {
		request nodectl.Request
		wantErr error
	}{
		"exclude with a target": {
			request: nodectl.Request{Operation: nodectl.OperationExclude, TargetID: "target-1"},
		},
		"include with a target": {
			request: nodectl.Request{Operation: nodectl.OperationInclude, TargetID: "target-1"},
		},
		"targets without a target": {
			request: nodectl.Request{Operation: nodectl.OperationTargets},
		},
		// An operation that takes no target must not be sent with one, so a
		// client cannot smuggle a field the server would silently ignore.
		"status with a target": {
			request: nodectl.Request{Operation: nodectl.OperationStatus, TargetID: "target-1"},
			wantErr: nodectl.ErrInvalidRequest,
		},
		"pause with a target": {
			request: nodectl.Request{Operation: nodectl.OperationPause, TargetID: "target-1"},
			wantErr: nodectl.ErrInvalidRequest,
		},
		"exclude without a target": {
			request: nodectl.Request{Operation: nodectl.OperationExclude},
			wantErr: nodectl.ErrInvalidRequest,
		},
		"exclude with a padded target": {
			request: nodectl.Request{Operation: nodectl.OperationExclude, TargetID: " target-1 "},
			wantErr: nodectl.ErrInvalidRequest,
		},
		"exclude with a control character": {
			request: nodectl.Request{Operation: nodectl.OperationExclude, TargetID: "target\x01one"},
			wantErr: nodectl.ErrInvalidRequest,
		},
		"exclude with an oversized target": {
			request: nodectl.Request{Operation: nodectl.OperationExclude, TargetID: domain.TargetID(long)},
			wantErr: nodectl.ErrInvalidRequest,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			request := test.request
			request.ProtocolVersion = nodectl.ProtocolVersion
			request.Source = nodectl.SourceCLI
			err := request.Validate()
			if test.wantErr == nil {
				if err != nil {
					t.Fatalf("validate = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("validate = %v, want %v", err, test.wantErr)
			}
		})
	}
	// A long-but-valid identifier is ordinary input and must still be accepted.
	atBoundary := nodectl.Request{
		ProtocolVersion: nodectl.ProtocolVersion,
		Operation:       nodectl.OperationExclude,
		Source:          nodectl.SourceCLI,
		TargetID:        domain.TargetID(strings.Repeat("x", domain.MaxTargetIDBytes)),
	}
	if err := atBoundary.Validate(); err != nil {
		t.Fatalf("identifier at the boundary rejected: %v", err)
	}
}

func TestServerRejectsMalformedTargetRequestsWithoutMutatingState(t *testing.T) {
	controller := &fakeController{intent: domain.AvailabilityAccepting}
	directory := startServer(t, controller, selfAuthorizer(t))
	path := filepath.Join(directory, nodectl.EndpointName)

	for name, request := range map[string]string{
		"exclude without a target": `{"protocolVersion":2,"operation":"exclude","source":"cli"}`,
		"exclude with a blank target": `{"protocolVersion":2,"operation":"exclude",` +
			`"source":"cli","targetId":"   "}`,
		"status carrying a target": `{"protocolVersion":2,"operation":"status",` +
			`"source":"cli","targetId":"target-1"}`,
	} {
		t.Run(name, func(t *testing.T) {
			response := exchange(t, path, request)
			if response.OK || response.ErrorClass != nodectl.ErrorClassInvalidRequest {
				t.Fatalf("unexpected response: %+v", response)
			}
		})
	}
	if len(controller.exclusions()) != 0 {
		t.Fatalf("malformed target requests mutated state: %v", controller.exclusions())
	}
}

func TestStatusDocumentCarriesThePerTargetViewAndUnknownExclusions(t *testing.T) {
	status := nodectl.Status{
		ProtocolVersion: nodectl.ProtocolVersion,
		NodeID:          "node-1",
		EligibleTargets: []nodectl.EligibleTarget{{
			TargetID:        "target-1",
			ScopeKind:       domain.TargetRepository,
			Scope:           "owner/repo",
			ScaleSetName:    "scale-set",
			LocallyExcluded: true,
			Pending:         true,
		}},
		UnknownExclusions: []domain.TargetID{"target-offline"},
		RunningExecutions: []nodectl.RunningExecution{{
			ExecutionID: "execution-1",
			State:       domain.ExecutionRunning,
			TargetID:    "target-1",
			Scope:       "owner/repo",
			ScopeKind:   domain.TargetRepository,
		}},
	}
	// Every field the per-Target view adds is non-secret observation: no token,
	// installation ID, JIT material, or workspace path has a home here.
	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(raw)
	for _, forbidden := range []string{
		"installationId", "jit", "token", "privateKey", "joinCode", "certificate",
	} {
		if strings.Contains(strings.ToLower(encoded), strings.ToLower(forbidden)) {
			t.Fatalf("status document leaked %q: %s", forbidden, encoded)
		}
	}
	for _, want := range []string{
		`"locallyExcluded":true`, `"pending":true`,
		`"unknownExclusions":["target-offline"]`, `"scopeKind":"repository"`,
	} {
		if !strings.Contains(encoded, want) {
			t.Fatalf("status document missing %s: %s", want, encoded)
		}
	}
}
