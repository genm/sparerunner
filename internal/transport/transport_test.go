package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/genm/tewake/internal/enroll"
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
	return enroll.Service{Registry: registry, Identity: identity, DigestKey: digestKey, Epoch: 4, Now: func() time.Time { return now }}, registry
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
	if _, err := (EnrollmentClient{}).Enroll(context.Background(), server.URL, code, csr); err == nil {
		t.Fatal("fake controller accepted")
	}
	if got := received.Load(); got != 0 {
		t.Fatalf("fake controller received %d HTTP request(s); want no request body", got)
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
		err := UpgradeAuthenticated(writer, request, registry, func(ctx context.Context, connection *websocket.Conn, credential enroll.Credential) error {
			envelope, readErr := ReadEnvelope(ctx, connection)
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
