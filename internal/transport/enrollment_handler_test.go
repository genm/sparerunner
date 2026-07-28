package transport

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/genm/sparerunner/internal/enroll"
)

func TestEnrollmentHandlerClassifiesMalformedCredentialsAndAuthorityFailures(t *testing.T) {
	service, registry := transportService(t)
	validCode, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := transportCSR(t)

	otherService, _ := transportService(t)
	wrongFingerprintCode, err := otherService.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cancelledCode, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mismatchedSecretCode := enrollmentCodeWithDifferentSecret(t, validCode)
	cancelled, err := enroll.DecodeJoinCode(cancelledCode)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.CancelToken(context.Background(), cancelled.TokenID()); err != nil {
		t.Fatal(err)
	}

	unavailable := service
	unavailable.Registry = failingEnrollmentRegistry{
		Registry:   service.Registry,
		consumeErr: errors.New("projection authority unavailable"),
		replayErr:  errors.New("projection authority unavailable"),
	}

	tests := []struct {
		name       string
		handler    http.Handler
		joinCode   string
		csr        []byte
		wantStatus int
		wantBody   string
	}{
		{
			name:       "malformed join code",
			handler:    EnrollmentHandler(service),
			joinCode:   "not-a-join-code",
			csr:        csr,
			wantStatus: http.StatusBadRequest,
			wantBody:   "invalid enrollment request\n",
		},
		{
			name:       "malformed certificate request",
			handler:    EnrollmentHandler(service),
			joinCode:   validCode,
			csr:        []byte("not-a-csr"),
			wantStatus: http.StatusBadRequest,
			wantBody:   "invalid enrollment request\n",
		},
		{
			name:       "controller fingerprint rejection",
			handler:    EnrollmentHandler(service),
			joinCode:   wrongFingerprintCode,
			csr:        csr,
			wantStatus: http.StatusUnauthorized,
			wantBody:   "enrollment rejected\n",
		},
		{
			name:       "join credential secret rejection",
			handler:    EnrollmentHandler(service),
			joinCode:   mismatchedSecretCode,
			csr:        csr,
			wantStatus: http.StatusUnauthorized,
			wantBody:   "enrollment rejected\n",
		},
		{
			name:       "consumed token rejection",
			handler:    EnrollmentHandler(service),
			joinCode:   cancelledCode,
			csr:        csr,
			wantStatus: http.StatusUnauthorized,
			wantBody:   "enrollment rejected\n",
		},
		{
			name:       "internal authority failure",
			handler:    EnrollmentHandler(unavailable),
			joinCode:   validCode,
			csr:        csr,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "enrollment unavailable\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := newEnrollmentHandlerRequest(t, test.joinCode, test.csr)
			response := httptest.NewRecorder()

			test.handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body = %q", response.Code, test.wantStatus, response.Body.String())
			}
			if response.Body.String() != test.wantBody {
				t.Fatalf("body = %q, want %q", response.Body.String(), test.wantBody)
			}
			if strings.Contains(response.Body.String(), "projection authority") {
				t.Fatalf("internal failure leaked through response: %q", response.Body.String())
			}
		})
	}
}

func TestEnrollmentHandlerSameCSRReplayRecoversAfterPostCommitAuthorityFailure(t *testing.T) {
	service, _ := transportService(t)
	code, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := transportCSR(t)
	recovering := &postCommitRecoveringEnrollmentRegistry{
		Registry:            service.Registry,
		projectionAvailable: false,
	}
	service.Registry = recovering
	handler := EnrollmentHandler(service)

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, newEnrollmentHandlerRequest(t, code, csr))
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-commit projection failure status = %d, want %d; body = %q", first.Code, http.StatusServiceUnavailable, first.Body.String())
	}

	recovering.setProjectionAvailable(true)
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, newEnrollmentHandlerRequest(t, code, csr))
	if second.Code != http.StatusCreated {
		t.Fatalf("same-CSR replay status = %d, want %d; body = %q", second.Code, http.StatusCreated, second.Body.String())
	}
	var firstRecovery EnrollmentResponse
	values, err := strictObject(second.Body.Bytes(), []string{"nodeId", "certificate", "ca"})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(values["nodeId"], &firstRecovery.NodeID); err != nil || firstRecovery.NodeID == "" {
		t.Fatalf("recovered node identity = %q, %v", firstRecovery.NodeID, err)
	}
	if recovering.consumeSuccesses() != 1 {
		t.Fatalf("durable enrollment commits = %d, want 1", recovering.consumeSuccesses())
	}
}

func enrollmentCodeWithDifferentSecret(t *testing.T, encoded string) string {
	t.Helper()
	body := strings.TrimPrefix(encoded, enroll.JoinCodePrefix)
	payload, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil || len(payload) == 0 {
		t.Fatalf("decode test join code: %v", err)
	}
	payload[len(payload)-1] ^= 1
	return enroll.JoinCodePrefix + base64.RawURLEncoding.EncodeToString(payload)
}

func newEnrollmentHandlerRequest(t *testing.T, joinCode string, csr []byte) *http.Request {
	t.Helper()
	body, err := json.Marshal(enrollmentRequest{
		JoinCode: joinCode,
		CSR:      base64.RawStdEncoding.EncodeToString(csr),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "https://controller.example.test/enroll", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return request
}

type failingEnrollmentRegistry struct {
	enroll.Registry
	consumeErr error
	replayErr  error
}

func (registry failingEnrollmentRegistry) ConsumeEnrollment(
	context.Context,
	enroll.TokenRecord,
	enroll.NodeRecord,
) error {
	return registry.consumeErr
}

func (registry failingEnrollmentRegistry) ReplayEnrollment(
	context.Context,
	enroll.TokenRecord,
	[32]byte,
) (enroll.NodeRecord, error) {
	return enroll.NodeRecord{}, registry.replayErr
}

type postCommitRecoveringEnrollmentRegistry struct {
	enroll.Registry

	mu                  sync.Mutex
	projectionAvailable bool
	committed           int
}

func (registry *postCommitRecoveringEnrollmentRegistry) ConsumeEnrollment(
	ctx context.Context,
	token enroll.TokenRecord,
	node enroll.NodeRecord,
) error {
	err := registry.Registry.ConsumeEnrollment(ctx, token, node)
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if err == nil {
		registry.committed++
	}
	if err == nil && !registry.projectionAvailable {
		return errors.New("post-commit projection unavailable")
	}
	return err
}

func (registry *postCommitRecoveringEnrollmentRegistry) ReplayEnrollment(
	ctx context.Context,
	token enroll.TokenRecord,
	digest [32]byte,
) (enroll.NodeRecord, error) {
	registry.mu.Lock()
	available := registry.projectionAvailable
	registry.mu.Unlock()
	if !available {
		return enroll.NodeRecord{}, errors.New("post-commit projection unavailable")
	}
	return registry.Registry.ReplayEnrollment(ctx, token, digest)
}

func (registry *postCommitRecoveringEnrollmentRegistry) setProjectionAvailable(available bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.projectionAvailable = available
}

func (registry *postCommitRecoveringEnrollmentRegistry) consumeSuccesses() int {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.committed
}
