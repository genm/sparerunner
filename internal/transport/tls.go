package transport

import (
	"context"
	"crypto"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
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

func ControllerServerTLSConfig(identity enroll.ControllerIdentity) (*tls.Config, error) {
	certificate, err := identity.TLSCertificate()
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(identity.CA)
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}, nil
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
	return tls.Certificate{Certificate: [][]byte{certificateDER, caDER}, PrivateKey: key, Leaf: certificate}, nil
}

func NodeClientTLSConfig(certificate tls.Certificate, ca *x509.Certificate) (*tls.Config, error) {
	if ca == nil {
		return nil, errors.New("missing controller CA")
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: pool, ServerName: enroll.ControllerDNSName}, nil
}

type SessionHandler func(context.Context, *websocket.Conn, enroll.Credential) error

// UpgradeAuthenticated performs certificate identity and current-credential
// authorization before the WebSocket is accepted, so rejected nodes cannot send
// protocol frames or be treated as capacity-bearing sessions.
func UpgradeAuthenticated(writer http.ResponseWriter, request *http.Request, authorizer CredentialAuthorizer, handler SessionHandler) error {
	if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 {
		return errors.New("missing node client certificate")
	}
	nodeID, serial, epoch, err := enroll.NodeCredentialIdentity(request.TLS.PeerCertificates[0])
	if err != nil {
		return err
	}
	credential := enroll.Credential{NodeID: nodeID, Serial: serial, Epoch: epoch, NotBefore: request.TLS.PeerCertificates[0].NotBefore, NotAfter: request.TLS.PeerCertificates[0].NotAfter}
	if authorizer == nil {
		return errors.New("missing credential authorizer")
	}
	if err := authorizer.AuthorizeCredential(request.Context(), credential, time.Now()); err != nil {
		return fmt.Errorf("node credential rejected: %w", err)
	}
	connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return err
	}
	connection.SetReadLimit(GitHubAdapterResponseLimit)
	defer connection.CloseNow()
	return handler(request.Context(), connection, credential)
}

func DialNodeWSS(ctx context.Context, endpoint string, config *tls.Config) (*websocket.Conn, *http.Response, error) {
	if config == nil {
		return nil, nil, errors.New("missing node TLS config")
	}
	connection, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: config}}, CompressionMode: websocket.CompressionDisabled})
	if err == nil {
		connection.SetReadLimit(GitHubAdapterResponseLimit)
	}
	return connection, response, err
}
