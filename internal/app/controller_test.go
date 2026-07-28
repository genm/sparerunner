package app

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	managementapi "github.com/genm/sparerunner/internal/api"
	"github.com/genm/sparerunner/internal/enroll"
	"github.com/genm/sparerunner/internal/store"
	"github.com/genm/sparerunner/internal/transport"
)

func TestAdminListenerRejectsNonLoopbackExposure(t *testing.T) {
	loopback, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer loopback.Close()
	if err := ValidateAdminListener(loopback); err != nil {
		t.Fatalf("loopback listener rejected: %v", err)
	}

	exposed, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer exposed.Close()
	if err := ValidateAdminListener(exposed); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("non-loopback listener error = %v", err)
	}
}

func TestAdminListenerOriginUsesTheActualLoopbackAuthority(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	origin, err := adminListenerOrigin(listener)
	if err != nil {
		t.Fatal(err)
	}
	if want := "http://" + listener.Addr().String(); origin != want {
		t.Fatalf("admin origin = %q, want %q", origin, want)
	}
}

func TestManagementAuditDegradationPublishesOneLiveInvalidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	controllerStore, err := store.OpenController(
		ctx,
		filepath.Join(directory, "controller.db"),
		store.Options{},
	)
	if err != nil {
		t.Fatal(err)
	}
	events := managementapi.NewEventBus()
	done := make(chan struct{})
	go func() {
		defer close(done)
		publishManagementInvalidations(
			ctx,
			&ControllerState{Store: controllerStore},
			events,
		)
	}()

	if err := controllerStore.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = controllerStore.AppendAuditEvent(ctx, store.AuditRecord{
		Actor:        store.AuditActorAnonymous,
		Action:       store.AuditActionAuthenticationFailed,
		Outcome:      store.AuditOutcomeRejected,
		ResourceKind: store.AuditResourceController,
		ErrorCode:    store.AuditErrorAuthenticationFailed,
		RequestID:    "req_00112233445566778899aabbccddeeff",
	})
	if err == nil {
		t.Fatal("closed audit store accepted an event")
	}
	deadline := time.Now().Add(2 * time.Second)
	for events.Generation() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if generation := events.Generation(); generation != 1 {
		t.Fatalf("audit degradation generation = %d, want 1", generation)
	}
	time.Sleep(10 * time.Millisecond)
	if generation := events.Generation(); generation != 1 {
		t.Fatalf("closed audit channel caused repeated invalidations: %d", generation)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("management invalidation fan-in did not stop")
	}
}

func TestEnrollmentRejectionsAppendBoundedSecretFreeAuditAfterClientCancellation(t *testing.T) {
	backend, _ := newManagementBackendForTest(t)
	handler := auditedEnrollmentHTTPHandler(backend.state)
	before, err := backend.state.Store.ReadAuditEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		body       string
		wantStatus int
	}{
		{body: `{"joinCode":`, wantStatus: http.StatusBadRequest},
		{
			body:       `{"joinCode":"twk_secret-canary","csr":"AA"}`,
			wantStatus: http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		requestContext, cancel := context.WithCancel(context.Background())
		cancel()
		request := httptest.NewRequest(
			http.MethodPost,
			"https://controller.example.test/enroll",
			strings.NewReader(test.body),
		).WithContext(requestContext)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.wantStatus {
			t.Fatalf(
				"enrollment rejection = %d %s, want %d",
				response.Code,
				response.Body.String(),
				test.wantStatus,
			)
		}
		if strings.Contains(response.Body.String(), "secret-canary") {
			t.Fatalf("enrollment rejection reflected join secret: %s", response.Body.String())
		}
	}
	after, err := backend.state.Store.ReadAuditEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	newEvents := after[len(before):]
	if len(newEvents) != 1 {
		t.Fatalf("enrollment rejection audit events = %#v", newEvents)
	}
	for _, event := range newEvents {
		if event.Record.Actor != store.AuditActorAnonymous ||
			event.Record.Action != store.AuditActionEnrollmentRejected ||
			event.Record.Outcome != store.AuditOutcomeRejected ||
			event.Record.ResourceKind != store.AuditResourceController ||
			event.Record.ResourceID != "" ||
			event.Record.ErrorCode != store.AuditErrorEnrollmentRejected {
			t.Fatalf("enrollment rejection audit = %#v", event)
		}
	}
}

func TestEnrollmentGuardBoundsValidShapedUnknownTokenWorkBeforeService(t *testing.T) {
	backend, _ := newManagementBackendForTest(t)
	handler := auditedEnrollmentHTTPHandler(backend.state)
	before, err := backend.state.Store.ReadAuditEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	code, err := enroll.NewJoinCode(
		backend.state.Identity.CAFingerprint(),
		nil,
		bytes.NewReader(bytes.Repeat([]byte{0x5a}, 48)),
	)
	if err != nil {
		t.Fatal(err)
	}
	encodedCode, err := code.Encode()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{
		"joinCode": encodedCode,
		"csr": base64.RawStdEncoding.EncodeToString(
			managementTestCSR(t),
		),
	})
	if err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < enrollmentRequestsPerSource+1; attempt++ {
		request := httptest.NewRequest(
			http.MethodPost,
			"https://controller.example.test/enroll",
			bytes.NewReader(payload),
		)
		request.RemoteAddr = "192.0.2.44:51234"
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		want := http.StatusUnauthorized
		if attempt == enrollmentRequestsPerSource {
			want = http.StatusTooManyRequests
			if response.Header().Get("Retry-After") != "60" {
				t.Fatalf("rate-limit Retry-After = %q", response.Header().Get("Retry-After"))
			}
		}
		if response.Code != want {
			t.Fatalf(
				"attempt %d status = %d %s, want %d",
				attempt,
				response.Code,
				response.Body.String(),
				want,
			)
		}
		if bytes.Contains(response.Body.Bytes(), []byte(encodedCode)) {
			t.Fatal("enrollment rejection reflected a join credential")
		}
	}

	after, err := backend.state.Store.ReadAuditEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	newEvents := after[len(before):]
	if len(newEvents) != 1 ||
		newEvents[0].Record.Action != store.AuditActionEnrollmentRejected ||
		newEvents[0].Record.ErrorCode != store.AuditErrorEnrollmentRejected {
		t.Fatalf("coalesced unknown-token audit = %#v", newEvents)
	}
}

func TestEnrollmentAvailabilityFailureIsNotMisreportedAsAuthentication(t *testing.T) {
	backend, _ := newManagementBackendForTest(t)
	registry := unavailableEnrollmentRegistry{
		Registry: enroll.NewMemoryRegistry(),
		err:      errors.New("projection provider secret-canary"),
	}
	var digestKey [32]byte
	if _, err := rand.Read(digestKey[:]); err != nil {
		t.Fatal(err)
	}
	service, err := enroll.NewService(
		registry,
		backend.state.Identity,
		digestKey,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	code, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	backend.state.Service = service
	handler := auditedEnrollmentHTTPHandler(backend.state)
	before, err := backend.state.Store.ReadAuditEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{
		"joinCode": code,
		"csr": base64.RawStdEncoding.EncodeToString(
			managementTestCSR(t),
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"https://controller.example.test/enroll",
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable ||
		strings.Contains(response.Body.String(), "secret-canary") {
		t.Fatalf("availability response = %d %q", response.Code, response.Body.String())
	}
	after, err := backend.state.Store.ReadAuditEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	newEvents := after[len(before):]
	if len(newEvents) != 1 ||
		newEvents[0].Record.Action != store.AuditActionEnrollmentUnavailable ||
		newEvents[0].Record.Outcome != store.AuditOutcomeFailed ||
		newEvents[0].Record.ErrorCode != store.AuditErrorEnrollmentUnavailable {
		t.Fatalf("availability audit = %#v", newEvents)
	}
}

func TestEnrollmentRequestGuardUsesPerSourceGlobalAndWindowBounds(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	guard := newEnrollmentRequestGuard(func() time.Time { return now })
	for source := 1; source <= 5; source++ {
		address := net.JoinHostPort(
			"192.0.2."+strconv.Itoa(source),
			"5000",
		)
		for request := 0; request < enrollmentRequestsPerSource; request++ {
			if !guard.admit(address) {
				t.Fatalf("source %d request %d rejected before its bound", source, request)
			}
		}
		if source == 1 && guard.admit(address) {
			t.Fatal("per-source limit accepted one extra request")
		}
	}
	if guard.admit("198.51.100.1:5000") {
		t.Fatal("global limit accepted one extra request")
	}
	if !guard.claimAudit(enrollmentAuditRejected) ||
		guard.claimAudit(enrollmentAuditRejected) {
		t.Fatal("rejection audit claim was not exactly once per window")
	}
	if !guard.claimAudit(enrollmentAuditUnavailable) {
		t.Fatal("independent availability audit claim was suppressed")
	}

	now = now.Add(enrollmentRequestWindow)
	if !guard.admit("192.0.2.1:5000") {
		t.Fatal("new window did not restore request capacity")
	}
	if !guard.claimAudit(enrollmentAuditRejected) {
		t.Fatal("new window did not restore aggregate audit marker")
	}
	if got := enrollmentSource("not-an-address"); got != "unknown" {
		t.Fatalf("malformed source = %q, want unknown", got)
	}
}

type unavailableEnrollmentRegistry struct {
	enroll.Registry
	err error
}

func (registry unavailableEnrollmentRegistry) ConsumeEnrollment(
	context.Context,
	enroll.TokenRecord,
	enroll.NodeRecord,
) error {
	return registry.err
}

func (registry unavailableEnrollmentRegistry) ReplayEnrollment(
	context.Context,
	enroll.TokenRecord,
	[32]byte,
) (enroll.NodeRecord, error) {
	return enroll.NodeRecord{}, registry.err
}

func TestControllerAuditsRevokedCredentialAndProtocolMismatchWithoutSecrets(t *testing.T) {
	t.Run("revoked credential before upgrade", func(t *testing.T) {
		state, result, privateKey := newControllerAgentAuditTestState(t)
		if _, err := state.Store.RevokeNode(context.Background(), result.NodeID); err != nil {
			t.Fatal(err)
		}
		server, clientTLS := newControllerAgentAuditTestServer(t, state, result, privateKey)
		connection, response, err := transport.DialNodeWSS(
			context.Background(),
			"wss"+strings.TrimPrefix(server.URL, "https"),
			clientTLS,
		)
		if connection != nil {
			connection.CloseNow()
		}
		if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("revoked connection = (%v, %#v), want HTTP 401", err, response)
		}
		event := waitForAgentSessionAudit(t, state.Store)
		assertAgentSessionAudit(
			t,
			event,
			result.NodeID,
			store.AuditErrorNodeCredentialRejected,
		)
	})

	t.Run("authenticated protocol version mismatch", func(t *testing.T) {
		state, result, privateKey := newControllerAgentAuditTestState(t)
		server, clientTLS := newControllerAgentAuditTestServer(t, state, result, privateKey)
		connection, _, err := transport.DialNodeWSS(
			context.Background(),
			"wss"+strings.TrimPrefix(server.URL, "https"),
			clientTLS,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.CloseNow()
		protocolSecret := "protocol-secret-canary"
		payload := []byte(`{"protocolVersion":2,"messageId":"` +
			protocolSecret +
			`","type":"hello","payload":{}}`)
		if err := connection.Write(
			context.Background(),
			websocket.MessageBinary,
			payload,
		); err != nil {
			t.Fatal(err)
		}
		event := waitForAgentSessionAudit(t, state.Store)
		assertAgentSessionAudit(
			t,
			event,
			result.NodeID,
			store.AuditErrorAgentProtocolRejected,
		)
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(encoded, []byte(protocolSecret)) {
			t.Fatalf("protocol audit leaked raw envelope: %s", encoded)
		}
	})
}

func newControllerAgentAuditTestState(
	t *testing.T,
) (*ControllerState, enroll.EnrollmentResult, ed25519.PrivateKey) {
	t.Helper()

	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	controllerStore, err := store.OpenController(
		ctx,
		filepath.Join(directory, "controller.db"),
		store.Options{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := controllerStore.Close(); err != nil {
			t.Error(err)
		}
	})
	identity, err := enroll.NewControllerIdentity(time.Now().Add(-time.Minute), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var digestKey [32]byte
	if _, err := rand.Read(digestKey[:]); err != nil {
		t.Fatal(err)
	}
	service, err := enroll.NewService(controllerStore, identity, digestKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	code, err := service.CreateJoinCode(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(
		rand.Reader,
		&x509.CertificateRequest{
			Subject: pkix.Name{CommonName: "tewake-agent"},
		},
		privateKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Enroll(ctx, code, csr)
	if err != nil {
		t.Fatal(err)
	}
	broker := NewAgentBroker(
		1,
		acceptingAgentConsumers(newRecordingUpdateConsumer()),
	)
	t.Cleanup(broker.Close)
	return &ControllerState{
		Identity:    identity,
		Store:       controllerStore,
		Service:     service,
		Sessions:    transport.NewActiveSessionRegistry(),
		AgentBroker: broker,
		Epoch:       1,
	}, result, privateKey
}

func newControllerAgentAuditTestServer(
	t *testing.T,
	state *ControllerState,
	result enroll.EnrollmentResult,
	privateKey ed25519.PrivateKey,
) (*httptest.Server, *tls.Config) {
	t.Helper()

	serverTLS, err := transport.ControllerServerTLSConfig(state.Identity)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(controllerAgentHandler(state))
	server.TLS = serverTLS
	server.StartTLS()
	t.Cleanup(server.Close)
	certificate, err := transport.NodeTLSCertificate(
		privateKey,
		result.CertificateDER,
		result.CACertificateDER,
	)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(result.CACertificateDER)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := transport.NodeClientTLSConfig(certificate, ca)
	if err != nil {
		t.Fatal(err)
	}
	return server, clientTLS
}

func waitForAgentSessionAudit(
	t *testing.T,
	controllerStore *store.ControllerStore,
) store.AuditEvent {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		events, err := controllerStore.ReadAuditEvents(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.Record.Action == store.AuditActionAgentSessionRejected {
				return event
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("agent session rejection audit was not persisted")
	return store.AuditEvent{}
}

func assertAgentSessionAudit(
	t *testing.T,
	event store.AuditEvent,
	nodeID string,
	errorCode store.AuditErrorCode,
) {
	t.Helper()

	if event.Record.Actor != store.AuditActorNode ||
		event.Record.Action != store.AuditActionAgentSessionRejected ||
		event.Record.Outcome != store.AuditOutcomeRejected ||
		event.Record.ResourceKind != store.AuditResourceNode ||
		event.Record.ResourceID != nodeID ||
		event.Record.ErrorCode != errorCode ||
		event.Record.RequestID == "" {
		t.Fatalf("agent session audit = %#v", event)
	}
}
