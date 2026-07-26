package transport

import (
	"bytes"
	"context"
	"crypto"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/genm/tewake/internal/enroll"
)

// EnrollmentClient ignores caller HTTP transports. Enrollment is a trust-anchor
// bootstrap, so environment proxies, redirects, and permissive TLS settings must
// not influence where the one-time secret is delivered.
type EnrollmentClient struct{}

type enrollmentRequest struct {
	JoinCode string `json:"joinCode"`
	CSR      string `json:"csr"`
}

type EnrollmentResponse struct {
	NodeID           string
	CertificateDER   []byte
	CACertificateDER []byte
	Credential       enroll.Credential
}

func (EnrollmentClient) Enroll(ctx context.Context, endpoint, encodedCode string, csrDER []byte) (EnrollmentResponse, error) {
	code, err := enroll.DecodeJoinCode(encodedCode)
	if err != nil {
		return EnrollmentResponse{}, err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return EnrollmentResponse{}, errors.New("invalid enrollment endpoint")
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil || csr.CheckSignature() != nil {
		return EnrollmentResponse{}, errors.New("invalid certificate request")
	}
	body, err := json.Marshal(enrollmentRequest{JoinCode: encodedCode, CSR: base64.RawStdEncoding.EncodeToString(csrDER)})
	if err != nil || int64(len(body)) > EnrollmentBodyLimit {
		return EnrollmentResponse{}, errors.New("invalid enrollment request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.JoinPath("/enroll").String(), bytes.NewReader(body))
	if err != nil {
		return EnrollmentResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: PinnedControllerTLSConfig(code.CAFingerprint())}, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("enrollment redirects are forbidden") }}
	response, err := client.Do(request)
	if err != nil {
		return EnrollmentResponse{}, errors.New("controller identity verification or enrollment request failed")
	}
	defer response.Body.Close()
	defer io.Copy(io.Discard, io.LimitReader(response.Body, EnrollmentBodyLimit))
	if response.StatusCode != http.StatusCreated || response.Header.Get("Content-Type") != "application/json" {
		return EnrollmentResponse{}, errors.New("enrollment rejected")
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, EnrollmentBodyLimit+1))
	if err != nil || int64(len(responseBody)) > EnrollmentBodyLimit {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	values, err := strictObject(responseBody, []string{"nodeId", "certificate", "ca"})
	if err != nil {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	var nodeID, encodedCertificate, encodedCA string
	if json.Unmarshal(values["nodeId"], &nodeID) != nil || json.Unmarshal(values["certificate"], &encodedCertificate) != nil || json.Unmarshal(values["ca"], &encodedCA) != nil {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	certificateDER, err := decodeRawBase64(encodedCertificate)
	if err != nil {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	caDER, err := decodeRawBase64(encodedCA)
	if err != nil {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	credential, err := verifyEnrollmentResponse(code.CAFingerprint(), nodeID, csr.PublicKey, certificateDER, caDER, time.Now())
	if err != nil {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	return EnrollmentResponse{NodeID: nodeID, CertificateDER: certificateDER, CACertificateDER: caDER, Credential: credential}, nil
}

func verifyEnrollmentResponse(expected [sha256.Size]byte, nodeID string, publicKey crypto.PublicKey, certificateDER, caDER []byte, now time.Time) (enroll.Credential, error) {
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		return enroll.Credential{}, err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil || sha256.Sum256(ca.Raw) != expected || !ca.IsCA || ca.CheckSignatureFrom(ca) != nil {
		return enroll.Credential{}, errors.New("invalid controller CA")
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	if _, err := certificate.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, CurrentTime: now}); err != nil {
		return enroll.Credential{}, err
	}
	returnedNodeID, serial, epoch, err := enroll.NodeCredentialIdentity(certificate)
	if err != nil || returnedNodeID != nodeID || !publicKeysEqual(certificate.PublicKey, publicKey) {
		return enroll.Credential{}, errors.New("returned node identity does not match request")
	}
	return enroll.Credential{NodeID: nodeID, Serial: serial, Epoch: epoch, NotBefore: certificate.NotBefore, NotAfter: certificate.NotAfter}, nil
}

func publicKeysEqual(left, right crypto.PublicKey) bool {
	leftDER, leftErr := x509.MarshalPKIXPublicKey(left)
	rightDER, rightErr := x509.MarshalPKIXPublicKey(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftDER, rightDER)
}
