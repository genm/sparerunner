package enroll

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"time"
)

// Service owns enrollment policy and certificate issuance. Its Registry is the
// sole persistence side-effect boundary.
type Service struct {
	Registry  Registry
	Identity  ControllerIdentity
	digestKey [32]byte
	// Epoch is the durable controller epoch captured in one-time tokens. It is
	// distinct from a node credential epoch, which is per-node revocation state.
	Epoch  uint64
	Now    func() time.Time
	Random io.Reader
}

func NewService(registry Registry, identity ControllerIdentity, digestKey [32]byte, epoch uint64) (Service, error) {
	service := Service{Registry: registry, Identity: identity, digestKey: digestKey, Epoch: epoch}
	if err := service.valid(); err != nil {
		return Service{}, err
	}
	return service, nil
}

func (service Service) String() string       { return "enrollment-service[redacted]" }
func (service Service) GoString() string     { return service.String() }
func (service Service) LogValue() slog.Value { return slog.StringValue(service.String()) }
func (service Service) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Epoch uint64 `json:"epoch"`
	}{Epoch: service.Epoch})
}

type EnrollmentResult struct {
	NodeID           string
	CertificateDER   []byte
	CACertificateDER []byte
	Credential       Credential
}

// JoinCodeDelivery keeps the durable token identity beside the one-time
// encoded credential. Callers must not decode a successfully persisted code
// merely to recover its non-secret token ID: a post-commit decode failure would
// make the committed token unreachable to the operator.
type JoinCodeDelivery struct {
	TokenID [16]byte
	encoded string
}

func (delivery JoinCodeDelivery) Encoded() string  { return delivery.encoded }
func (delivery JoinCodeDelivery) String() string   { return "join-code-delivery[redacted]" }
func (delivery JoinCodeDelivery) GoString() string { return delivery.String() }
func (delivery JoinCodeDelivery) LogValue() slog.Value {
	return slog.StringValue(delivery.String())
}

func (service Service) CreateJoinCode(ctx context.Context, hints []string) (string, error) {
	delivery, err := service.CreateJoinCodeDelivery(ctx, hints)
	if err != nil {
		return "", err
	}
	return delivery.Encoded(), nil
}

// CreateJoinCodeDelivery fully validates and encodes the one-time credential
// before persisting its digest. Once Registry.CreateToken commits, the returned
// delivery is already complete and cannot fail during response construction.
func (service Service) CreateJoinCodeDelivery(
	ctx context.Context,
	hints []string,
) (JoinCodeDelivery, error) {
	if err := service.valid(); err != nil {
		return JoinCodeDelivery{}, err
	}
	canonical, err := CanonicalHints(hints)
	if err != nil {
		return JoinCodeDelivery{}, err
	}
	code, err := NewJoinCode(service.Identity.CAFingerprint(), canonical, service.Random)
	if err != nil {
		return JoinCodeDelivery{}, err
	}
	encoded, err := code.Encode()
	if err != nil {
		return JoinCodeDelivery{}, err
	}
	if err := service.Registry.CreateToken(ctx, TokenRecord{ID: code.tokenID, SecretDigest: SecretDigest(service.digestKey, code.tokenID, code.secret), Epoch: service.Epoch}); err != nil {
		return JoinCodeDelivery{}, err
	}
	return JoinCodeDelivery{TokenID: code.TokenID(), encoded: encoded}, nil
}

func (service Service) Enroll(ctx context.Context, encodedCode string, csrDER []byte) (EnrollmentResult, error) {
	if err := service.valid(); err != nil {
		return EnrollmentResult{}, unavailableEnrollment(err)
	}
	code, err := DecodeJoinCode(encodedCode)
	if err != nil {
		return EnrollmentResult{}, malformedEnrollment(err)
	}
	if code.caFingerprint != service.Identity.CAFingerprint() {
		return EnrollmentResult{}, rejectedEnrollment(ErrControllerFingerprintMismatch)
	}
	nodeID, err := NewNodeID(service.random())
	if err != nil {
		return EnrollmentResult{}, unavailableEnrollment(err)
	}
	now := service.now()
	csrRequest, parseErr := x509.ParseCertificateRequest(csrDER)
	if parseErr != nil || csrRequest.CheckSignature() != nil {
		return EnrollmentResult{}, malformedEnrollment(errors.New("invalid certificate request"))
	}
	publicKeyDER, marshalErr := x509.MarshalPKIXPublicKey(csrRequest.PublicKey)
	if marshalErr != nil {
		return EnrollmentResult{}, malformedEnrollment(marshalErr)
	}
	csrDigest := sha256.Sum256(publicKeyDER)
	// Credential epoch is per node and starts at one. Controller process epoch is
	// deliberately only a join-token generation guard; a restart must not invalidate
	// an otherwise current node credential or prevent its renewal.
	certificate, certificateDER, err := service.Identity.IssueNodeCertificate(csrDER, nodeID, 1, now, service.random())
	if err != nil {
		return EnrollmentResult{}, unavailableEnrollment(err)
	}
	credential := credentialFor(nodeID, certificate, 1)
	token := TokenRecord{ID: code.tokenID, SecretDigest: SecretDigest(service.digestKey, code.tokenID, code.secret), Epoch: service.Epoch}
	node := NodeRecord{NodeID: nodeID, Credential: credential, PublicKeyDigest: csrDigest, CertificateDER: certificateDER, CACertificateDER: service.Identity.CA.Raw}
	if consumeErr := service.Registry.ConsumeEnrollment(ctx, token, node); consumeErr != nil {
		if replay, replayErr := service.Registry.ReplayEnrollment(ctx, token, csrDigest); replayErr == nil {
			return enrollmentResult(replay), nil
		} else if registryEnrollmentFailure(replayErr) == EnrollmentFailureUnavailable {
			// A commit may have succeeded before its in-process projection
			// failed. The replay authority therefore outranks an apparent
			// token-not-found result from a retried consume. Report a
			// recoverable outage so the same code and CSR can be retried.
			return EnrollmentResult{}, unavailableEnrollment(replayErr)
		}
		if registryEnrollmentFailure(consumeErr) == EnrollmentFailureRejected {
			return EnrollmentResult{}, rejectedEnrollment(consumeErr)
		}
		return EnrollmentResult{}, unavailableEnrollment(consumeErr)
	}
	return enrollmentResult(node), nil
}

func enrollmentResult(node NodeRecord) EnrollmentResult {
	return EnrollmentResult{NodeID: node.NodeID, CertificateDER: append([]byte(nil), node.CertificateDER...), CACertificateDER: append([]byte(nil), node.CACertificateDER...), Credential: node.Credential}
}

func (service Service) Cancel(ctx context.Context, tokenID [16]byte) error {
	if err := service.valid(); err != nil {
		return err
	}
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
	if service.Registry == nil || service.Epoch == 0 || zeroBytes(service.digestKey[:]) {
		return errors.New("incomplete enrollment service")
	}
	return service.Identity.Validate(service.now())
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

// RenewalTime deterministically derives one point in the documented 70-90%
// window from immutable certificate bytes. It is stable across polling and
// restart, while independently-issued certificates spread renewal work.
func RenewalTime(certificate *x509.Certificate) (time.Time, error) {
	if certificate == nil || len(certificate.Raw) == 0 || !certificate.NotAfter.After(certificate.NotBefore) {
		return time.Time{}, errors.New("invalid certificate")
	}
	spread := sha256.Sum256(certificate.Raw)
	percent := 70 + int(spread[0])%21
	return certificate.NotBefore.Add(time.Duration(percent) * certificate.NotAfter.Sub(certificate.NotBefore) / 100), nil
}

// RenewalDue is retained for callers that previously supplied a CSPRNG. The
// reader is intentionally unused: rerolling per poll would make renewal state
// nondeterministic and can indefinitely postpone a due credential.
func RenewalDue(certificate *x509.Certificate, now time.Time, reader io.Reader) (bool, error) {
	_ = reader
	threshold, err := RenewalTime(certificate)
	if err != nil {
		return false, err
	}
	if !now.Before(certificate.NotAfter) {
		return true, nil
	}
	return !now.Before(threshold), nil
}

func CreateNodeCSR(key ed25519.PrivateKey) ([]byte, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("invalid node private key")
	}
	return x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, crypto.Signer(key))
}
