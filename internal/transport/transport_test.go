package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/genm/sparerunner/internal/enroll"
	"github.com/genm/sparerunner/internal/store"
	"github.com/hashicorp/mdns"
)

func transportService(t *testing.T) (enroll.Service, *enroll.MemoryRegistry) {
	t.Helper()
	now := time.Now().UTC().Add(-time.Minute)
	identity, err := enroll.NewControllerIdentity(now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var digestKey [32]byte
	if _, err := rand.Read(digestKey[:]); err != nil {
		t.Fatal(err)
	}
	registry := enroll.NewMemoryRegistry()
	service, err := enroll.NewService(registry, identity, digestKey, 4)
	if err != nil {
		t.Fatal(err)
	}
	service.Now = func() time.Time { return now }
	return service, registry
}

func transportCSR(t *testing.T) ([]byte, ed25519.PrivateKey) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := enroll.CreateNodeCSR(key)
	if err != nil {
		t.Fatal(err)
	}
	return csr, key
}

func TestDecodeEnvelopeRejectsVersionUnknownDuplicateTrailingAndOversize(t *testing.T) {
	valid := []byte(`{"protocolVersion":1,"messageId":"message-1","type":"hello","payload":{}}`)
	if _, err := DecodeEnvelope(valid); err != nil {
		t.Fatal(err)
	}
	tests := [][]byte{
		[]byte(`{"protocolVersion":2,"messageId":"message-1","type":"hello","payload":{}}`),
		[]byte(`{"protocolVersion":1,"messageId":"message-1","type":"future","payload":{}}`),
		[]byte(`{"protocolVersion":1,"messageId":"message-1","type":"hello","type":"ack","payload":{}}`),
		append(append([]byte(nil), valid...), []byte(` {}`)...),
		make([]byte, GitHubAdapterResponseLimit+1),
	}
	for _, payload := range tests {
		if _, err := DecodeEnvelope(payload); err == nil {
			t.Fatalf("invalid envelope accepted: %q", payload[:min(len(payload), 32)])
		}
	}
}

func TestEnrollmentClientDoesNotSendBodyToWrongFingerprint(t *testing.T) {
	service, _ := transportService(t)
	other, err := enroll.NewControllerIdentity(service.Now(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := other.TLSCertificate()
	if err != nil {
		t.Fatal(err)
	}
	var received atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.ReadAll(request.Body)
		received.Add(1)
		writer.WriteHeader(http.StatusCreated)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	defer server.Close()
	code, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := transportCSR(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := (EnrollmentClient{}).Enroll(ctx, server.URL, code, csr); err == nil {
		t.Fatal("fake controller accepted")
	}
	if got := received.Load(); got != 0 {
		t.Fatalf("fake controller received %d HTTP request(s); want no request body", got)
	}
}

func TestEnrollmentClientRequiresDeadlineBeforeNetworkAccess(t *testing.T) {
	service, _ := transportService(t)
	code, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := transportCSR(t)
	_, err = (EnrollmentClient{}).Enroll(context.Background(), "https://127.0.0.1:1", code, csr)
	if err == nil || !strings.Contains(err.Error(), "explicit deadline") {
		t.Fatalf("missing deadline error = %v", err)
	}
}

func TestEnrollmentClientDeadlineCancelsAStalledTrustedController(t *testing.T) {
	service, _ := transportService(t)
	certificate, err := service.Identity.TLSCertificate()
	if err != nil {
		t.Fatal(err)
	}
	handlerExited := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		defer close(handlerExited)
		if _, err := io.Copy(io.Discard, request.Body); err != nil {
			return
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusCreated)
		response.(http.Flusher).Flush()
		<-request.Context().Done()
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	t.Cleanup(server.CloseClientConnections)
	defer server.Close()
	code, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := transportCSR(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := (EnrollmentClient{}).Enroll(ctx, server.URL, code, csr); err == nil {
		t.Fatal("stalled controller request succeeded")
	}
	select {
	case <-handlerExited:
	case <-time.After(3 * time.Second):
		t.Fatal("client cancellation did not reach trusted controller")
	}
}

func TestEnrollmentHandlerAndPinnedClientCompleteInitialJoinWithoutClientCertificate(t *testing.T) {
	service, registry := transportService(t)
	serverConfig, err := ControllerServerTLSConfig(service.Identity)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(EnrollmentHandler(service))
	server.TLS = serverConfig
	server.StartTLS()
	defer server.Close()
	code, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := transportCSR(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := (EnrollmentClient{}).Enroll(ctx, server.URL, code, csr)
	if err != nil {
		t.Fatal(err)
	}
	if response.NodeID == "" {
		t.Fatal("missing node ID")
	}
	if err := registry.AuthorizeCredential(context.Background(), response.Credential, time.Now()); err != nil {
		t.Fatalf("enrolled credential rejected: %v", err)
	}
}

func TestAuthenticatedWSSAcceptsCurrentMTLSCredential(t *testing.T) {
	service, registry := transportService(t)
	code, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	csr, key := transportCSR(t)
	result, err := service.Enroll(context.Background(), code, csr)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := x509.ParseCertificate(result.CertificateDER)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, serial, epoch, err := enroll.NodeCredentialIdentity(issued)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AuthorizeCredential(context.Background(), enroll.Credential{NodeID: nodeID, Serial: serial, Epoch: epoch, NotBefore: issued.NotBefore, NotAfter: issued.NotAfter}, time.Now()); err != nil {
		t.Fatalf("preflight credential rejected: %v", err)
	}
	serverConfig, err := ControllerServerTLSConfig(service.Identity)
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan Envelope, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		err := UpgradeAuthenticated(writer, request, registry, func(ctx context.Context, session *AuthenticatedSession) error {
			envelope, readErr := session.Read(ctx)
			if readErr == nil {
				received <- envelope
			}
			return readErr
		})
		if err != nil {
			http.Error(writer, "rejected", http.StatusUnauthorized)
		}
	}))
	server.TLS = serverConfig
	server.StartTLS()
	defer server.Close()
	certificate, err := NodeTLSCertificate(key, result.CertificateDER, result.CACertificateDER)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(result.CACertificateDER)
	if err != nil {
		t.Fatal(err)
	}
	clientConfig, err := NodeClientTLSConfig(certificate, ca)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "wss" + strings.TrimPrefix(server.URL, "https")
	connection, _, err := DialNodeWSS(context.Background(), endpoint, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if err := WriteEnvelope(context.Background(), connection, Envelope{ProtocolVersion: ProtocolVersion, MessageID: "m-1", Type: MessageHello, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	select {
	case envelope := <-received:
		if envelope.Type != MessageHello {
			t.Fatalf("message type = %s", envelope.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("controller did not receive authenticated WebSocket frame")
	}
}

func TestAuthenticatedWSSRejectsMissingClientCertificate(t *testing.T) {
	service, registry := transportService(t)
	serverConfig, err := ControllerServerTLSConfig(service.Identity)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := UpgradeAuthenticated(writer, request, registry, func(context.Context, *AuthenticatedSession) error { return nil }); err != nil {
			http.Error(writer, "rejected", http.StatusUnauthorized)
		}
	}))
	server.TLS = serverConfig
	server.StartTLS()
	defer server.Close()
	pool := x509.NewCertPool()
	pool.AddCert(service.Identity.CA)
	endpoint := "wss" + strings.TrimPrefix(server.URL, "https")
	_, response, err := websocket.Dial(context.Background(), endpoint, &websocket.DialOptions{HTTPClient: &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: enroll.ControllerDNSName}}}, Subprotocols: []string{"tewake.v1"}})
	if err == nil {
		t.Fatal("missing client certificate accepted")
	}
	if response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing certificate response = %#v", response)
	}
}

func TestAuthenticatedWSSRejectsPeerLeafThatDiffersFromVerifiedChain(t *testing.T) {
	service, registry := transportService(t)
	firstCode, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	firstCSR, _ := transportCSR(t)
	first, err := service.Enroll(context.Background(), firstCode, firstCSR)
	if err != nil {
		t.Fatal(err)
	}
	secondCode, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	secondCSR, _ := transportCSR(t)
	second, err := service.Enroll(context.Background(), secondCode, secondCSR)
	if err != nil {
		t.Fatal(err)
	}
	peerLeaf, err := x509.ParseCertificate(first.CertificateDER)
	if err != nil {
		t.Fatal(err)
	}
	verifiedLeaf, err := x509.ParseCertificate(second.CertificateDER)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "https://tewake-controller/", nil)
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{peerLeaf, service.Identity.CA},
		VerifiedChains:   [][]*x509.Certificate{{verifiedLeaf, service.Identity.CA}},
	}
	err = UpgradeAuthenticated(httptest.NewRecorder(), request, registry, func(context.Context, *AuthenticatedSession) error {
		t.Fatal("mismatched peer entered session handler")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "does not match peer leaf") {
		t.Fatalf("mismatched verified leaf error = %v", err)
	}
}

func TestAuthenticatedWSSReturnsTypedSecretFreeRevokedCredentialRejection(t *testing.T) {
	service, registry := transportService(t)
	code, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := transportCSR(t)
	result, err := service.Enroll(context.Background(), code, csr)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(result.CertificateDER)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.RevokeNode(context.Background(), result.NodeID); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "https://tewake-controller/", nil)
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf, service.Identity.CA},
		VerifiedChains:   [][]*x509.Certificate{{leaf, service.Identity.CA}},
	}
	err = UpgradeAuthenticated(
		httptest.NewRecorder(),
		request,
		registry,
		func(context.Context, *AuthenticatedSession) error {
			t.Fatal("revoked credential entered session handler")
			return nil
		},
	)
	nodeID, kind, rejected := AgentSessionRejection(err)
	if !rejected ||
		nodeID != result.NodeID ||
		kind != AgentSessionCredentialRejected ||
		SessionWasUpgraded(err) {
		t.Fatalf(
			"typed credential rejection = (%q, %d, %t, upgraded=%t), err=%v",
			nodeID,
			kind,
			rejected,
			SessionWasUpgraded(err),
			err,
		)
	}
	if strings.Contains(err.Error(), result.NodeID) ||
		strings.Contains(err.Error(), leaf.SerialNumber.String()) {
		t.Fatalf("credential rejection error leaked identity material: %v", err)
	}
}

func TestAgentProtocolRejectionExposesOnlyAuthenticatedNodeAndClass(t *testing.T) {
	credential := enroll.Credential{
		NodeID: "00112233445566778899aabbccddeeff",
	}
	providerSecret := errors.New("protocol provider secret-canary")
	err := AgentProtocolRejection(credential, providerSecret)
	nodeID, kind, rejected := AgentSessionRejection(err)
	if !rejected ||
		nodeID != credential.NodeID ||
		kind != AgentSessionProtocolRejected ||
		!errors.Is(err, providerSecret) {
		t.Fatalf("typed protocol rejection = (%q, %d, %t), err=%v", nodeID, kind, rejected, err)
	}
	if strings.Contains(err.Error(), credential.NodeID) ||
		strings.Contains(err.Error(), "secret-canary") {
		t.Fatalf("protocol rejection error leaked detail: %v", err)
	}
}

func TestControllerStoreRevocationInterruptsBlockedReadWithoutDeliveringFrame(t *testing.T) {
	ctx := context.Background()
	privateDir := t.TempDir()
	if err := os.Chmod(privateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	registry, err := store.OpenController(ctx, filepath.Join(privateDir, "controller.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	now := time.Now().UTC().Add(-time.Minute)
	identity, err := enroll.NewControllerIdentity(now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var digestKey [32]byte
	if _, err := rand.Read(digestKey[:]); err != nil {
		t.Fatal(err)
	}
	service, err := enroll.NewService(registry, identity, digestKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	code, err := service.CreateJoinCode(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	csr, key := transportCSR(t)
	result, err := service.Enroll(ctx, code, csr)
	if err != nil {
		t.Fatal(err)
	}
	sessions := NewActiveSessionRegistry()
	registry.SetCredentialRevocationHook(sessions.Revoke)
	serverConfig, err := ControllerServerTLSConfig(identity)
	if err != nil {
		t.Fatal(err)
	}
	readStarted := make(chan struct{})
	readResult := make(chan error, 1)
	delivered := make(chan Envelope, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		readResult <- UpgradeAuthenticatedWithSessions(writer, request, registry, func(readContext context.Context, session *AuthenticatedSession) error {
			close(readStarted)
			envelope, readErr := session.Read(readContext)
			if readErr == nil {
				delivered <- envelope
			}
			return readErr
		}, sessions)
	}))
	server.TLS = serverConfig
	server.StartTLS()
	defer server.Close()
	certificate, err := NodeTLSCertificate(key, result.CertificateDER, result.CACertificateDER)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(result.CACertificateDER)
	if err != nil {
		t.Fatal(err)
	}
	clientConfig, err := NodeClientTLSConfig(certificate, ca)
	if err != nil {
		t.Fatal(err)
	}
	connection, _, err := DialNodeWSS(ctx, "wss"+strings.TrimPrefix(server.URL, "https"), clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	select {
	case <-readStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("controller did not begin authenticated read")
	}
	if _, err := registry.RevokeNode(ctx, result.NodeID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readResult:
		if err == nil {
			t.Fatal("revoked blocked read returned success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("revocation did not interrupt blocked read")
	}
	select {
	case envelope := <-delivered:
		t.Fatalf("revoked session delivered frame: %#v", envelope)
	default:
	}
}

func TestMDNSCandidateIsOnlyData(t *testing.T) {
	fake := fakeDiscoverer{candidates: []EndpointCandidate{{Address: "203.0.113.9:443"}}}
	candidates, err := fake.Discover(context.Background())
	if err != nil || len(candidates) != 1 || candidates[0].Address != "203.0.113.9:443" {
		t.Fatalf("fake discovery = %#v, %v", candidates, err)
	}
	// Candidate discovery does not produce a TLS config; callers must still bind
	// the connection to the join-code fingerprint through PinnedControllerTLSConfig.
	if PinnedControllerTLSConfig([32]byte{}).InsecureSkipVerify != true {
		t.Fatal("pinned verification configuration changed")
	}
}

func TestMDNSCandidatePreservesIPv6ZoneAndRejectsZonelessLinkLocal(t *testing.T) {
	entry := &mdns.ServiceEntry{Port: 443, AddrV4: net.IP{1, 2}, AddrV6IPAddr: &net.IPAddr{IP: net.ParseIP("fe80::1"), Zone: "en0"}}
	candidate, ok := candidateFor(entry)
	if !ok || candidate.Address != "[fe80::1%en0]:443" {
		t.Fatalf("zoned IPv6 candidate = %#v, %t", candidate, ok)
	}
	if _, ok := candidateFor(&mdns.ServiceEntry{Port: 443, AddrV6IPAddr: &net.IPAddr{IP: net.ParseIP("fe80::1")}}); ok {
		t.Fatal("zoneless link-local IPv6 accepted")
	}
}

type fakeDiscoverer struct{ candidates []EndpointCandidate }

func (fake fakeDiscoverer) Discover(context.Context) ([]EndpointCandidate, error) {
	return append([]EndpointCandidate(nil), fake.candidates...), nil
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
