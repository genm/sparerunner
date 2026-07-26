package transport

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/genm/tewake/internal/enroll"
)

// EnrollmentClient is intentionally small: its caller chooses a candidate from
// mDNS or join-code hints, while this type proves that candidate's pinned CA and
// server chain before serializing the one-time secret into an HTTP request body.
type EnrollmentClient struct{ HTTPClient *http.Client }

type enrollmentRequest struct {
	JoinCode string `json:"joinCode"`
	CSR      string `json:"csr"`
}

type EnrollmentResponse struct {
	NodeID           string
	CertificateDER   []byte
	CACertificateDER []byte
}

func (client EnrollmentClient) Enroll(ctx context.Context, endpoint, encodedCode string, csrDER []byte) (EnrollmentResponse, error) {
	code, err := enroll.DecodeJoinCode(encodedCode)
	if err != nil {
		return EnrollmentResponse{}, err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return EnrollmentResponse{}, errors.New("invalid enrollment endpoint")
	}
	body, err := json.Marshal(enrollmentRequest{JoinCode: encodedCode, CSR: base64.RawStdEncoding.EncodeToString(csrDER)})
	if err != nil {
		return EnrollmentResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.JoinPath("/enroll").String(), bytes.NewReader(body))
	if err != nil {
		return EnrollmentResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	httpClient := client.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	copyClient := *httpClient
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if configured, ok := copyClient.Transport.(*http.Transport); ok && configured != nil {
		transport = configured.Clone()
	}
	transport.TLSClientConfig = PinnedControllerTLSConfig(code.CAFingerprint)
	copyClient.Transport = transport
	response, err := copyClient.Do(request)
	if err != nil {
		return EnrollmentResponse{}, errors.New("controller identity verification or enrollment request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return EnrollmentResponse{}, fmt.Errorf("enrollment rejected: status %d", response.StatusCode)
	}
	var payload struct {
		NodeID      string `json:"nodeId"`
		Certificate string `json:"certificate"`
		CA          string `json:"ca"`
	}
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	if payload.NodeID == "" {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	certificateDER, err := base64.RawStdEncoding.DecodeString(payload.Certificate)
	if err != nil {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	caDER, err := base64.RawStdEncoding.DecodeString(payload.CA)
	if err != nil {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	if _, err := x509.ParseCertificate(certificateDER); err != nil {
		return EnrollmentResponse{}, errors.New("invalid enrollment response")
	}
	return EnrollmentResponse{NodeID: payload.NodeID, CertificateDER: certificateDER, CACertificateDER: caDER}, nil
}
