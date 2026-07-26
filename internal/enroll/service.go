package enroll

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"time"
)

// Service owns enrollment policy and certificate issuance. Its Registry is the
// sole persistence side-effect boundary.
type Service struct {
	Registry  Registry
	Identity  ControllerIdentity
	DigestKey [32]byte
	// Epoch is the durable controller epoch captured in one-time tokens. It is
	// distinct from a node credential epoch, which is per-node revocation state.
	Epoch  uint64
	Now    func() time.Time
	Random io.Reader
}

type EnrollmentResult struct {
	NodeID           string
	CertificateDER   []byte
	CACertificateDER []byte
	Credential       Credential
}

func (service Service) CreateJoinCode(ctx context.Context, hints []string) (string, error) {
	if err := service.valid(); err != nil {
		return "", err
	}
	canonical, err := CanonicalHints(hints)
	if err != nil {
		return "", err
	}
	code, err := NewJoinCode(service.Identity.CAFingerprint(), canonical, service.Random)
	if err != nil {
		return "", err
	}
	if err := service.Registry.CreateToken(ctx, TokenRecord{ID: code.TokenID, SecretDigest: SecretDigest(service.DigestKey, code.Secret), Epoch: service.Epoch}); err != nil {
		return "", err
	}
	return code.Encode()
}

func (service Service) Enroll(ctx context.Context, encodedCode string, csrDER []byte) (EnrollmentResult, error) {
	if err := service.valid(); err != nil {
		return EnrollmentResult{}, err
	}
	code, err := DecodeJoinCode(encodedCode)
	if err != nil {
		return EnrollmentResult{}, err
	}
	if code.CAFingerprint != service.Identity.CAFingerprint() {
		return EnrollmentResult{}, ErrControllerFingerprintMismatch
	}
	nodeID, err := NewNodeID(service.random())
	if err != nil {
		return EnrollmentResult{}, err
	}
	now := service.now()
	// Credential epoch is per node and starts at one. Controller process epoch is
	// deliberately only a join-token generation guard; a restart must not invalidate
	// an otherwise current node credential or prevent its renewal.
	certificate, certificateDER, err := service.Identity.IssueNodeCertificate(csrDER, nodeID, 1, now, service.random())
	if err != nil {
		return EnrollmentResult{}, err
	}
	credential := credentialFor(nodeID, certificate, 1)
	if err := service.Registry.ConsumeEnrollment(ctx, TokenRecord{ID: code.TokenID, SecretDigest: SecretDigest(service.DigestKey, code.Secret), Epoch: service.Epoch}, NodeRecord{NodeID: nodeID, Credential: credential}); err != nil {
		return EnrollmentResult{}, err
	}
	return EnrollmentResult{NodeID: nodeID, CertificateDER: certificateDER, CACertificateDER: service.Identity.CA.Raw, Credential: credential}, nil
}

func (service Service) Cancel(ctx context.Context, tokenID [16]byte) error {
	return service.Registry.CancelToken(ctx, tokenID)
}

// Renew verifies the still-current credential inside Registry's atomic renewal
// transaction. A replacement certificate is harmless unless that transaction wins.
func (service Service) Renew(ctx context.Context, current Credential, csrDER []byte) (EnrollmentResult, error) {
	if err := service.valid(); err != nil {
		return EnrollmentResult{}, err
	}
	now := service.now()
	certificate, certificateDER, err := service.Identity.IssueNodeCertificate(csrDER, current.NodeID, current.Epoch, now, service.random())
	if err != nil {
		return EnrollmentResult{}, err
	}
	replacement := credentialFor(current.NodeID, certificate, current.Epoch)
	if err := service.Registry.RenewCredential(ctx, current.NodeID, current, replacement, now); err != nil {
		return EnrollmentResult{}, err
	}
	return EnrollmentResult{NodeID: current.NodeID, CertificateDER: certificateDER, CACertificateDER: service.Identity.CA.Raw, Credential: replacement}, nil
}

func (service Service) valid() error {
	if service.Registry == nil || service.Identity.CA == nil || service.Epoch == 0 {
		return errors.New("incomplete enrollment service")
	}
	return nil
}
func (service Service) now() time.Time {
	if service.Now != nil {
		return service.Now()
	}
	return time.Now()
}
func (service Service) random() io.Reader {
	if service.Random != nil {
		return service.Random
	}
	return rand.Reader
}

func credentialFor(nodeID string, certificate *x509.Certificate, epoch uint64) Credential {
	return Credential{NodeID: nodeID, Serial: certificate.SerialNumber.Text(16), Epoch: epoch, NotBefore: certificate.NotBefore, NotAfter: certificate.NotAfter}
}

// RenewalDue derives a fresh CSPRNG jitter for a certificate lifetime. No fixed
// schedule is persisted, avoiding synchronized fleet renewal after restart.
func RenewalDue(certificate *x509.Certificate, now time.Time, reader io.Reader) (bool, error) {
	if certificate == nil || !certificate.NotAfter.After(certificate.NotBefore) {
		return false, errors.New("invalid certificate")
	}
	if !now.Before(certificate.NotAfter) {
		return true, nil
	}
	roll, err := rand.Int(readerOrDefault(reader), big.NewInt(21))
	if err != nil {
		return false, err
	}
	percent := 70 + roll.Int64()
	threshold := certificate.NotBefore.Add(time.Duration(percent) * certificate.NotAfter.Sub(certificate.NotBefore) / 100)
	return !now.Before(threshold), nil
}

func CreateNodeCSR(key ed25519.PrivateKey) ([]byte, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("invalid node private key")
	}
	return x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, crypto.Signer(key))
}
