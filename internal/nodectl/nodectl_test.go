//go:build unix

package nodectl_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/nodectl"
)

type fakeController struct {
	mu      sync.Mutex
	intent  domain.AvailabilityIntent
	changes int
	err     error
}

func (controller *fakeController) Status(context.Context) (nodectl.Status, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.err != nil {
		return nodectl.Status{}, controller.err
	}
	return nodectl.Status{NodeID: "node-1", Intent: controller.intent}, nil
}

func (controller *fakeController) SetIntent(
	_ context.Context,
	intent domain.AvailabilityIntent,
	source nodectl.Source,
) (nodectl.Status, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.err != nil {
		return nodectl.Status{}, controller.err
	}
	controller.intent = intent
	controller.changes++
	return nodectl.Status{NodeID: "node-1", Intent: intent, IntentChangedBy: string(source)}, nil
}

func (controller *fakeController) snapshot() (domain.AvailabilityIntent, int) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.intent, controller.changes
}

type denyAll struct{}

func (denyAll) Authorize(nodectl.Peer) error { return nodectl.ErrUnauthorizedPeer }

func startServer(t *testing.T, controller nodectl.Controller, authorizer nodectl.Authorizer) string {
	t.Helper()
	// A short directory keeps the socket path inside the platform sun_path limit.
	directory := shortTempDir(t)
	listener, err := nodectl.Listen(directory)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server, err := nodectl.Serve(listener, nodectl.ServerOptions{
		Controller: controller,
		Authorizer: authorizer,
	})
	if err != nil {
		listener.Close()
		t.Fatalf("serve: %v", err)
	}
	t.Cleanup(func() { server.Close() })
	return directory
}

// shortTempDir stays outside the default per-test temporary directory, whose
// name alone can exceed the platform socket path limit on macOS.
func shortTempDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "twkctl")
	if err != nil {
		t.Fatalf("create temporary directory: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatalf("resolve temporary directory: %v", err)
	}
	return resolved
}

func selfAuthorizer(t *testing.T) nodectl.Authorizer {
	t.Helper()
	return nodectl.NewUIDAllowlist(currentUID())
}

func TestClientReadsStatusAndTogglesIntent(t *testing.T) {
	controller := &fakeController{intent: domain.AvailabilityAccepting}
	directory := startServer(t, controller, selfAuthorizer(t))
	client := nodectl.Client{StateDirectory: directory, Source: nodectl.SourceTray}

	status, err := client.Status()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Intent != domain.AvailabilityAccepting || status.ProtocolVersion != nodectl.ProtocolVersion {
		t.Fatalf("unexpected status: %+v", status)
	}

	stopped, err := client.Pause()
	if err != nil {
		t.Fatalf("pause: %v", err)
	}
	if stopped.Intent != domain.AvailabilityStopped {
		t.Fatalf("pause did not stop acceptance: %+v", stopped)
	}
	if stopped.IntentChangedBy != string(nodectl.SourceTray) {
		t.Fatalf("requesting surface was not recorded: %+v", stopped)
	}

	resumed, err := client.Resume()
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Intent != domain.AvailabilityAccepting {
		t.Fatalf("resume did not restore acceptance: %+v", resumed)
	}
	if intent, changes := controller.snapshot(); intent != domain.AvailabilityAccepting || changes != 2 {
		t.Fatalf("unexpected controller state: intent=%s changes=%d", intent, changes)
	}
}

func TestUnauthorizedPeerIsRefusedWithoutStateChange(t *testing.T) {
	controller := &fakeController{intent: domain.AvailabilityAccepting}
	directory := startServer(t, controller, denyAll{})
	client := nodectl.Client{StateDirectory: directory, Source: nodectl.SourceRaycast}

	_, err := client.Pause()
	if err == nil {
		t.Fatal("unauthorized peer was accepted")
	}
	var controlErr *nodectl.Error
	if !errors.As(err, &controlErr) || controlErr.Class != nodectl.ErrorClassUnauthorizedPeer {
		t.Fatalf("unexpected error: %v", err)
	}
	if intent, changes := controller.snapshot(); intent != domain.AvailabilityAccepting || changes != 0 {
		t.Fatalf("refused request mutated state: intent=%s changes=%d", intent, changes)
	}
}

func TestEmptyAllowlistAuthorizesNobody(t *testing.T) {
	if err := nodectl.NewUIDAllowlist().Authorize(nodectl.Peer{UID: 0}); !errors.Is(err, nodectl.ErrUnauthorizedPeer) {
		t.Fatalf("empty allowlist authorized a peer: %v", err)
	}
	if err := nodectl.NewUIDAllowlist(1000).Authorize(nodectl.Peer{UID: -1}); !errors.Is(err, nodectl.ErrUnauthorizedPeer) {
		t.Fatalf("unidentifiable peer authorized: %v", err)
	}
}

func TestServerRejectsMalformedAndUnsupportedRequests(t *testing.T) {
	controller := &fakeController{intent: domain.AvailabilityAccepting}
	directory := startServer(t, controller, selfAuthorizer(t))
	path := filepath.Join(directory, nodectl.EndpointName)

	tests := map[string]struct {
		request string
		class   string
	}{
		"protocol mismatch": {
			request: `{"protocolVersion":2,"operation":"pause","source":"cli"}`,
			class:   nodectl.ErrorClassProtocolMismatch,
		},
		"unsupported operation": {
			request: `{"protocolVersion":1,"operation":"revoke","source":"cli"}`,
			class:   nodectl.ErrorClassUnsupportedOperation,
		},
		"unknown field": {
			request: `{"protocolVersion":1,"operation":"pause","source":"cli","extra":true}`,
			class:   nodectl.ErrorClassInvalidRequest,
		},
		"unknown source": {
			request: `{"protocolVersion":1,"operation":"pause","source":"browser"}`,
			class:   nodectl.ErrorClassInvalidRequest,
		},
		"garbage": {
			request: `not json`,
			class:   nodectl.ErrorClassInvalidRequest,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			response := exchange(t, path, test.request)
			if response.OK || response.ErrorClass != test.class {
				t.Fatalf("unexpected response: %+v", response)
			}
		})
	}
	if intent, changes := controller.snapshot(); intent != domain.AvailabilityAccepting || changes != 0 {
		t.Fatalf("malformed requests mutated state: intent=%s changes=%d", intent, changes)
	}
}

func TestInternalFailureIsReportedByClassWithoutInternalDetail(t *testing.T) {
	controller := &fakeController{err: errors.New("sqlite: /private/var/agent.db is corrupt")}
	directory := startServer(t, controller, selfAuthorizer(t))
	client := nodectl.Client{StateDirectory: directory, Source: nodectl.SourceCLI}

	_, err := client.Status()
	if err == nil {
		t.Fatal("degraded agent reported success")
	}
	var controlErr *nodectl.Error
	if !errors.As(err, &controlErr) || controlErr.Class != nodectl.ErrorClassAgentDegraded {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(controlErr.Message, "agent.db") {
		t.Fatalf("internal detail crossed the boundary: %v", controlErr.Message)
	}
}

func TestMissingEndpointIsUnavailableRatherThanAssumed(t *testing.T) {
	client := nodectl.Client{StateDirectory: shortTempDir(t), Source: nodectl.SourceRaycast}
	status, err := client.Status()
	if err == nil {
		t.Fatalf("missing endpoint produced a status: %+v", status)
	}
	var controlErr *nodectl.Error
	if !errors.As(err, &controlErr) || controlErr.Class != nodectl.ErrorClassEndpointUnavailable {
		t.Fatalf("unexpected error: %v", err)
	}
}

func exchange(t *testing.T, path, request string) nodectl.Response {
	t.Helper()
	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer connection.Close()
	if _, err := connection.Write([]byte(request + "\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	var response nodectl.Response
	if err := json.NewDecoder(connection).Decode(&response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return response
}
