package transport

import (
	"bytes"
	"context"
	"crypto"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/genm/tewake/internal/enroll"
)

// CredentialAuthorizer is implemented by controller persistence. Its decision
// must check exact NodeID, serial, epoch, revocation, and validity; TLS only
// proves that a CA issued the presented certificate.
type CredentialAuthorizer interface {
	AuthorizeCredential(context.Context, enroll.Credential, time.Time) error
}

type EnrollmentFinalizer interface {
	FinalizeEnrollment(context.Context, enroll.Credential) error
}

func ControllerServerTLSConfig(identity enroll.ControllerIdentity) (*tls.Config, error) {
	certificate, err := identity.TLSCertificate()
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(identity.CA)
	// Enrollment starts on this listener and deliberately has no client
	// certificate. WSS authentication below still requires a verified, current
	// client certificate before accepting an upgraded session.
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientCAs: pool, ClientAuth: tls.VerifyClientCertIfGiven}, nil
}

// PinnedControllerTLSConfig does full verification in VerifyConnection because a
// joining node intentionally does not possess a pre-installed root. It accepts
// the presented root only after its SHA-256 fingerprint matches the join code,
// then verifies the server leaf and DNS name before HTTP can transmit a secret.
func PinnedControllerTLSConfig(expected [sha256.Size]byte) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		ServerName:         enroll.ControllerDNSName,
		InsecureSkipVerify: true, // VerifyConnection below replaces default roots with the pinned join-code anchor.
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) < 2 {
				return errors.New("controller did not provide its CA chain")
			}
			root := state.PeerCertificates[len(state.PeerCertificates)-1]
			if sha256.Sum256(root.Raw) != expected {
				return errors.New("controller fingerprint mismatch")
			}
			if !root.IsCA || root.CheckSignatureFrom(root) != nil {
				return errors.New("invalid controller CA")
			}
			intermediates := x509.NewCertPool()
			for _, certificate := range state.PeerCertificates[1 : len(state.PeerCertificates)-1] {
				intermediates.AddCert(certificate)
			}
			roots := x509.NewCertPool()
			roots.AddCert(root)
			_, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{DNSName: enroll.ControllerDNSName, Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, CurrentTime: time.Now()})
			return err
		},
	}
}

func NodeTLSCertificate(key crypto.PrivateKey, certificateDER, caDER []byte) (tls.Certificate, error) {
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		return tls.Certificate{}, err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return tls.Certificate{}, err
	}
	if err := validateNodeMaterial(key, certificate, ca, time.Now()); err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{certificateDER, caDER}, PrivateKey: key, Leaf: certificate}, nil
}

func NodeClientTLSConfig(certificate tls.Certificate, ca *x509.Certificate) (*tls.Config, error) {
	if ca == nil || certificate.Leaf == nil {
		return nil, errors.New("missing controller CA")
	}
	if err := validateNodeMaterial(certificate.PrivateKey, certificate.Leaf, ca, time.Now()); err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: pool, ServerName: enroll.ControllerDNSName}, nil
}

func validateNodeMaterial(key crypto.PrivateKey, certificate, ca *x509.Certificate, now time.Time) error {
	if certificate == nil || ca == nil || !ca.IsCA || ca.CheckSignatureFrom(ca) != nil || !ca.NotAfter.After(now) {
		return errors.New("invalid controller CA")
	}
	if _, _, _, err := enroll.NodeCredentialIdentity(certificate); err != nil {
		return err
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	if _, err := certificate.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, CurrentTime: now}); err != nil {
		return err
	}
	public, ok := key.(crypto.Signer)
	if !ok || !publicKeysEqual(public.Public(), certificate.PublicKey) {
		return errors.New("node private key does not match certificate")
	}
	return nil
}

type SessionHandler func(context.Context, *AuthenticatedSession) error

type upgradedSessionError struct{ err error }

func (err upgradedSessionError) Error() string { return err.err.Error() }
func (err upgradedSessionError) Unwrap() error { return err.err }

func SessionWasUpgraded(err error) bool {
	var upgraded upgradedSessionError
	return errors.As(err, &upgraded)
}

type AgentSessionRejectionKind uint8

const (
	AgentSessionCredentialRejected AgentSessionRejectionKind = iota + 1
	AgentSessionProtocolRejected
)

type agentSessionRejectionError struct {
	nodeID string
	kind   AgentSessionRejectionKind
	cause  error
}

func (err agentSessionRejectionError) Error() string {
	switch err.kind {
	case AgentSessionCredentialRejected:
		return "node credential rejected"
	case AgentSessionProtocolRejected:
		return "agent protocol rejected"
	default:
		return "agent session rejected"
	}
}

func (err agentSessionRejectionError) Unwrap() error { return err.cause }

// AgentSessionRejection extracts only the authenticated, persistence-safe
// identity and closed rejection class. Raw certificates, frames, and provider
// errors remain outside the audit boundary.
func AgentSessionRejection(err error) (string, AgentSessionRejectionKind, bool) {
	var rejection agentSessionRejectionError
	if !errors.As(err, &rejection) ||
		rejection.nodeID == "" ||
		(rejection.kind != AgentSessionCredentialRejected &&
			rejection.kind != AgentSessionProtocolRejected) {
		return "", 0, false
	}
	return rejection.nodeID, rejection.kind, true
}

// AgentProtocolRejection marks a protocol failure after the credential has
// already passed the Controller's current-credential authority.
func AgentProtocolRejection(credential enroll.Credential, cause error) error {
	return agentSessionRejectionError{
		nodeID: credential.NodeID,
		kind:   AgentSessionProtocolRejected,
		cause:  cause,
	}
}

type ActiveSessionRegistry struct {
	mu       sync.Mutex
	sessions map[string]map[*AuthenticatedSession]struct{}
}

func NewActiveSessionRegistry() *ActiveSessionRegistry {
	return &ActiveSessionRegistry{sessions: make(map[string]map[*AuthenticatedSession]struct{})}
}
func credentialKey(credential enroll.Credential) string {
	return credential.NodeID + ":" + credential.Serial + ":" + fmt.Sprint(credential.Epoch)
}
func (registry *ActiveSessionRegistry) Register(session *AuthenticatedSession) func() {
	if registry == nil {
		return func() {}
	}
	key := credentialKey(session.credential)
	registry.mu.Lock()
	if registry.sessions[key] == nil {
		registry.sessions[key] = map[*AuthenticatedSession]struct{}{}
	}
	registry.sessions[key][session] = struct{}{}
	registry.mu.Unlock()
	return func() {
		registry.mu.Lock()
		delete(registry.sessions[key], session)
		if len(registry.sessions[key]) == 0 {
			delete(registry.sessions, key)
		}
		registry.mu.Unlock()
	}
}
func (registry *ActiveSessionRegistry) Revoke(credential enroll.Credential) {
	if registry == nil {
		return
	}
	key := credentialKey(credential)
	registry.mu.Lock()
	sessions := registry.sessions[key]
	delete(registry.sessions, key)
	registry.mu.Unlock()
	for session := range sessions {
		// Revocation follows the durable store commit and must not block that
		// mutation on a peer that never completes a WebSocket close handshake.
		session.connection.CloseNow()
	}
}

func (registry *ActiveSessionRegistry) CloseAll() {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	all := make([]*AuthenticatedSession, 0)
	for key, sessions := range registry.sessions {
		for session := range sessions {
			all = append(all, session)
		}
		delete(registry.sessions, key)
	}
	registry.mu.Unlock()
	for _, session := range all {
		session.connection.CloseNow()
	}
}

func (registry *ActiveSessionRegistry) Count() int {
	if registry == nil {
		return 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	count := 0
	for _, sessions := range registry.sessions {
		count += len(sessions)
	}
	return count
}

// AuthenticatedSession rechecks current credential state before each inbound or
// outbound protocol operation. Revocation therefore terminates a live WSS
// session on its next heartbeat/command exchange instead of only on reconnect.
type AuthenticatedSession struct {
	connection *websocket.Conn
	authorizer CredentialAuthorizer
	credential enroll.Credential
}

func (session *AuthenticatedSession) Credential() enroll.Credential { return session.credential }

func (session *AuthenticatedSession) Read(ctx context.Context) (Envelope, error) {
	if err := session.authorizer.AuthorizeCredential(ctx, session.credential, time.Now()); err != nil {
		session.connection.CloseNow()
		return Envelope{}, agentSessionRejectionError{
			nodeID: session.credential.NodeID,
			kind:   AgentSessionCredentialRejected,
			cause:  err,
		}
	}
	envelope, err := ReadEnvelope(ctx, session.connection)
	if err != nil {
		return Envelope{}, err
	}
	if err := session.authorizer.AuthorizeCredential(ctx, session.credential, time.Now()); err != nil {
		session.connection.CloseNow()
		return Envelope{}, agentSessionRejectionError{
			nodeID: session.credential.NodeID,
			kind:   AgentSessionCredentialRejected,
			cause:  err,
		}
	}
	return envelope, nil
}

func (session *AuthenticatedSession) Write(ctx context.Context, envelope Envelope) error {
	if err := session.authorizer.AuthorizeCredential(ctx, session.credential, time.Now()); err != nil {
		session.connection.CloseNow()
		return agentSessionRejectionError{
			nodeID: session.credential.NodeID,
			kind:   AgentSessionCredentialRejected,
			cause:  err,
		}
	}
	return WriteEnvelope(ctx, session.connection, envelope)
}

// UpgradeAuthenticated performs certificate identity and current-credential
// authorization before the WebSocket is accepted, so rejected nodes cannot send
// protocol frames or be treated as capacity-bearing sessions.
func UpgradeAuthenticated(writer http.ResponseWriter, request *http.Request, authorizer CredentialAuthorizer, handler SessionHandler) error {
	return UpgradeAuthenticatedWithSessions(writer, request, authorizer, handler, nil)
}
func UpgradeAuthenticatedWithSessions(writer http.ResponseWriter, request *http.Request, authorizer CredentialAuthorizer, handler SessionHandler, sessions *ActiveSessionRegistry) error {
	if handler == nil || authorizer == nil {
		return errors.New("missing authenticated session dependency")
	}
	if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.VerifiedChains[0]) == 0 {
		return errors.New("missing node client certificate")
	}
	verifiedLeaf := request.TLS.VerifiedChains[0][0]
	if !bytes.Equal(verifiedLeaf.Raw, request.TLS.PeerCertificates[0].Raw) {
		return errors.New("verified client certificate does not match peer leaf")
	}
	nodeID, serial, epoch, err := enroll.NodeCredentialIdentity(verifiedLeaf)
	if err != nil {
		return err
	}
	credential := enroll.Credential{NodeID: nodeID, Serial: serial, Epoch: epoch, NotBefore: verifiedLeaf.NotBefore, NotAfter: verifiedLeaf.NotAfter}
	if err := authorizer.AuthorizeCredential(request.Context(), credential, time.Now()); err != nil {
		return agentSessionRejectionError{
			nodeID: credential.NodeID,
			kind:   AgentSessionCredentialRejected,
			cause:  err,
		}
	}
	connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled, Subprotocols: []string{"tewake.v1"}})
	if err != nil {
		return err
	}
	connection.SetReadLimit(MaxEnvelopeBytes)
	defer connection.CloseNow()
	if connection.Subprotocol() != "tewake.v1" {
		return upgradedSessionError{agentSessionRejectionError{
			nodeID: credential.NodeID,
			kind:   AgentSessionProtocolRejected,
			cause:  errors.New("missing tewake.v1 subprotocol"),
		}}
	}
	session := &AuthenticatedSession{connection: connection, authorizer: authorizer, credential: credential}
	deregister := sessions.Register(session)
	defer deregister()
	if err := authorizer.AuthorizeCredential(request.Context(), credential, time.Now()); err != nil {
		return upgradedSessionError{agentSessionRejectionError{
			nodeID: credential.NodeID,
			kind:   AgentSessionCredentialRejected,
			cause:  err,
		}}
	}
	if finalizer, ok := authorizer.(EnrollmentFinalizer); ok {
		if err := finalizer.FinalizeEnrollment(request.Context(), credential); err != nil {
			return upgradedSessionError{fmt.Errorf("node enrollment finalization failed: %w", err)}
		}
	}
	if err := handler(request.Context(), session); err != nil {
		return upgradedSessionError{err}
	}
	return nil
}

func DialNodeWSS(ctx context.Context, endpoint string, config *tls.Config) (*websocket.Conn, *http.Response, error) {
	if config == nil {
		return nil, nil, errors.New("missing node TLS config")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "wss" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, nil, errors.New("invalid secure WebSocket endpoint")
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: config}, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("WebSocket redirects are forbidden") }}
	connection, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: client, CompressionMode: websocket.CompressionDisabled, Subprotocols: []string{"tewake.v1"}})
	if err == nil {
		connection.SetReadLimit(MaxEnvelopeBytes)
		if connection.Subprotocol() != "tewake.v1" {
			connection.CloseNow()
			return nil, response, errors.New("controller did not negotiate tewake.v1")
		}
	}
	return connection, response, err
}
