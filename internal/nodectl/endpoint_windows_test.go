//go:build windows

package nodectl_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/nodectl"
	"golang.org/x/sys/windows"
)

type windowsController struct {
	mu      sync.Mutex
	intent  domain.AvailabilityIntent
	changes int
}

func (controller *windowsController) Status(context.Context) (nodectl.Status, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return nodectl.Status{NodeID: "node-1", Intent: controller.intent}, nil
}

func (controller *windowsController) SetIntent(
	_ context.Context,
	intent domain.AvailabilityIntent,
	source nodectl.Source,
) (nodectl.Status, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	controller.intent = intent
	controller.changes++
	return nodectl.Status{NodeID: "node-1", Intent: intent, IntentChangedBy: string(source)}, nil
}

func (controller *windowsController) SetTargetExclusion(
	_ context.Context,
	_ domain.TargetID,
	_ bool,
	_ nodectl.Source,
) (nodectl.Status, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return nodectl.Status{NodeID: "node-1", Intent: controller.intent}, nil
}

func (controller *windowsController) changeCount() int {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.changes
}

func serveWindowsEndpoint(
	t *testing.T,
	stateDirectory string,
	owners []string,
	authorizer nodectl.Authorizer,
	controller nodectl.Controller,
) {
	t.Helper()
	listener, err := nodectl.Listen(stateDirectory, owners)
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
}

// The serving account always reaches its own endpoint, so the normal path proves
// the pipe DACL, the peer SID lookup, and the protocol together.
func TestWindowsLocalControlAuthorizedOwner(t *testing.T) {
	self, err := nodectl.ServiceAccountSID()
	if err != nil {
		t.Fatalf("service account sid: %v", err)
	}
	directory := t.TempDir()
	controller := &windowsController{intent: domain.AvailabilityAccepting}
	serveWindowsEndpoint(t, directory, []string{self}, nodectl.NewSIDAllowlist(self), controller)

	client := nodectl.Client{StateDirectory: directory, Source: nodectl.SourceTray}
	status, err := client.Status()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.NodeID != "node-1" || status.Intent != domain.AvailabilityAccepting {
		t.Fatalf("unexpected status: %+v", status)
	}
	if status.ProtocolVersion != nodectl.ProtocolVersion {
		t.Fatalf("protocol version = %d, want %d", status.ProtocolVersion, nodectl.ProtocolVersion)
	}
	paused, err := client.Pause()
	if err != nil {
		t.Fatalf("pause: %v", err)
	}
	if paused.Intent != domain.AvailabilityStopped {
		t.Fatalf("intent = %q, want stopped", paused.Intent)
	}
	if controller.changeCount() != 1 {
		t.Fatalf("intent changes = %d, want 1", controller.changeCount())
	}
}

// A peer that reaches the endpoint but is not an authorized owner must be
// refused with the machine-readable class and must change no state.
func TestWindowsLocalControlRefusesUnauthorizedPeer(t *testing.T) {
	self, err := nodectl.ServiceAccountSID()
	if err != nil {
		t.Fatalf("service account sid: %v", err)
	}
	directory := t.TempDir()
	controller := &windowsController{intent: domain.AvailabilityAccepting}
	// The DACL still admits this process, so the connection reaches the server
	// and the per-connection SID check is the thing under test. LocalService is a
	// well-known SID that this test process never runs as.
	serveWindowsEndpoint(
		t, directory, []string{self}, nodectl.NewSIDAllowlist("S-1-5-19"), controller,
	)

	client := nodectl.Client{StateDirectory: directory, Source: nodectl.SourceCLI}
	_, err = client.Pause()
	var controlErr *nodectl.Error
	if !errors.As(err, &controlErr) || controlErr.Class != nodectl.ErrorClassUnauthorizedPeer {
		t.Fatalf("unauthorized peer not refused: %v", err)
	}
	if controller.changeCount() != 0 {
		t.Fatalf("refused peer changed state: %d intent changes", controller.changeCount())
	}
}

// An owner value that cannot name a principal must fail startup rather than be
// dropped, which would produce a narrower endpoint than the operator asked for.
func TestWindowsLocalControlRejectsMalformedOwner(t *testing.T) {
	_, err := nodectl.Listen(t.TempDir(), []string{"not-a-sid"})
	if !errors.Is(err, nodectl.ErrEndpointUnavailable) {
		t.Fatalf("malformed owner did not fail closed: %v", err)
	}
}

// Creation must not join an existing pipe, so a squatted or duplicated endpoint
// is refused instead of silently sharing one control surface.
func TestWindowsLocalControlRejectsDuplicateEndpoint(t *testing.T) {
	self, err := nodectl.ServiceAccountSID()
	if err != nil {
		t.Fatalf("service account sid: %v", err)
	}
	directory := t.TempDir()
	serveWindowsEndpoint(
		t, directory, []string{self}, nodectl.NewSIDAllowlist(self), &windowsController{},
	)
	if _, err := nodectl.Listen(directory, []string{self}); err == nil {
		t.Fatal("a second listener claimed the same endpoint")
	}
}

// A client with no agent listening must report an unavailable endpoint, not an
// assumed state.
func TestWindowsLocalControlUnreachableEndpoint(t *testing.T) {
	client := nodectl.Client{StateDirectory: t.TempDir(), Source: nodectl.SourceTray}
	_, err := client.Status()
	var controlErr *nodectl.Error
	if !errors.As(err, &controlErr) ||
		controlErr.Class != nodectl.ErrorClassEndpointUnavailable {
		t.Fatalf("unreachable endpoint class = %v", err)
	}
}

func TestWindowsEndpointPathIsStableAndPerInstallation(t *testing.T) {
	first := t.TempDir()
	second := t.TempDir()
	firstName, err := nodectl.EndpointPath(first)
	if err != nil {
		t.Fatalf("endpoint path: %v", err)
	}
	repeated, err := nodectl.EndpointPath(first)
	if err != nil {
		t.Fatalf("endpoint path repeat: %v", err)
	}
	if firstName != repeated {
		t.Fatalf("endpoint path is not stable: %q then %q", firstName, repeated)
	}
	// The agent and its clients derive the name independently, so a differently
	// cased or unclean spelling of one installation must not split the endpoint.
	cased, err := nodectl.EndpointPath(strings.ToUpper(first) + `\.`)
	if err != nil {
		t.Fatalf("endpoint path cased: %v", err)
	}
	if cased != firstName {
		t.Fatalf("case or cleanliness split the endpoint: %q vs %q", cased, firstName)
	}
	secondName, err := nodectl.EndpointPath(second)
	if err != nil {
		t.Fatalf("endpoint path second: %v", err)
	}
	if secondName == firstName {
		t.Fatal("two installations share one endpoint name")
	}
	if !strings.HasPrefix(firstName, `\\.\pipe\`) {
		t.Fatalf("endpoint is not a named pipe: %q", firstName)
	}
	if _, err := nodectl.EndpointPath(""); !errors.Is(err, nodectl.ErrEndpointUnavailable) {
		t.Fatalf("empty state directory did not fail closed: %v", err)
	}
}

// The allowlist compares SID strings, so it must accept the canonical spelling
// the OS reports and refuse a peer it never named.
func TestWindowsSIDAllowlist(t *testing.T) {
	sid, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		t.Fatalf("well known sid: %v", err)
	}
	allowlist := nodectl.NewSIDAllowlist(strings.ToLower(sid.String()))
	if err := allowlist.Authorize(nodectl.Peer{UID: -1, PID: 1, SID: sid.String()}); err != nil {
		t.Fatalf("canonical sid refused: %v", err)
	}
	if err := allowlist.Authorize(nodectl.Peer{UID: -1, PID: 1, SID: "S-1-5-19"}); !errors.Is(
		err, nodectl.ErrUnauthorizedPeer,
	) {
		t.Fatalf("unnamed sid authorized: %v", err)
	}
	if err := allowlist.Authorize(nodectl.Peer{UID: -1, PID: 1}); !errors.Is(
		err, nodectl.ErrUnauthorizedPeer,
	) {
		t.Fatalf("unidentifiable peer authorized: %v", err)
	}
	if err := nodectl.NewSIDAllowlist().Authorize(
		nodectl.Peer{UID: -1, PID: 1, SID: sid.String()},
	); !errors.Is(err, nodectl.ErrUnauthorizedPeer) {
		t.Fatalf("empty allowlist authorized a peer: %v", err)
	}
}
