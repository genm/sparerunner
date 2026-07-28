package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/auth"
	"github.com/genm/sparerunner/internal/enroll"
)

const (
	testCSRFToken     = "csrf-test-token"
	testSessionCookie = "session-test-cookie"
	testRawSecret     = "raw-server-secret-must-not-leak"
)

func TestUIAuthorizeUsesOwnerSessionAndDoesNotTreatCodeAsAuthority(t *testing.T) {
	code := canonicalManagementCLITestBrowserHandoffCode()
	server := newManagementCLITestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost ||
			request.URL.Path != "/api/v1/browser-handoff-authorizations" {
			t.Fatalf("operation request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get(auth.CSRFHeaderName) != testCSRFToken {
			t.Fatalf("operation CSRF = %q", request.Header.Get(auth.CSRFHeaderName))
		}
		if _, err := request.Cookie(auth.SessionCookieName); err != nil {
			t.Fatalf("operation session cookie: %v", err)
		}
		if request.Header.Get(auth.BootstrapHeaderName) != "" {
			t.Fatal("operation leaked owner bootstrap proof")
		}
		var body struct {
			Code string `json:"code"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Code != code {
			t.Fatalf("handoff code = %q", body.Code)
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := run(withManagementState(t, []string{
		"ui", "authorize", code,
		"--admin-url", server.URL + "/api/v1",
	}), &stdout, &stderr)
	if err != nil {
		t.Fatalf("ui authorize: %v", err)
	}
	if stdout.String() != "Browser authorized. Return to Tewake in the browser.\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestUIAuthorizeRejectsMalformedCodeBeforeReadingOwnerCredential(t *testing.T) {
	previous := loadManagementBootstrapProof
	loadManagementBootstrapProof = func(string, string) (string, error) {
		t.Fatal("malformed handoff reached owner credential boundary")
		return "", nil
	}
	t.Cleanup(func() {
		loadManagementBootstrapProof = previous
	})

	var stdout, stderr bytes.Buffer
	err := run([]string{
		"ui", "authorize", "twh1.not-a-valid-code",
	}, &stdout, &stderr)
	if err == nil || err.Error() != "browser handoff code is invalid" {
		t.Fatalf("error = %v", err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("output = (%q, %q)", stdout.String(), stderr.String())
	}
}

func TestNodeAddUsesManagementAPISessionCookieOriginAndCSRF(t *testing.T) {
	var operationCalls int
	hints := []string{"https://controller.example.test:7443"}
	joinCode, tokenID := newManagementCLITestJoinCode(t, hints)
	server := newManagementCLITestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		operationCalls++
		if request.Method != http.MethodPost || request.URL.Path != "/api/v1/join-codes" {
			t.Fatalf("operation request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Origin") != serverOrigin(request) {
			t.Fatalf("operation Origin = %q", request.Header.Get("Origin"))
		}
		if request.Header.Get("X-Tewake-CSRF") != testCSRFToken {
			t.Fatalf("operation CSRF = %q", request.Header.Get("X-Tewake-CSRF"))
		}
		if _, err := request.Cookie(auth.SessionCookieName); err != nil {
			t.Fatalf("operation session cookie: %v", err)
		}
		if request.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("operation content type = %q", request.Header.Get("Content-Type"))
		}
		var body struct {
			EndpointHints []string `json:"endpointHints"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(body.EndpointHints) != 1 ||
			body.EndpointHints[0] != "https://controller.example.test:7443" {
			t.Fatalf("endpoint hints = %#v", body.EndpointHints)
		}
		writeJSON(writer, http.StatusCreated, map[string]string{
			"code":    joinCode,
			"tokenId": tokenID,
		})
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := run(withManagementState(t, []string{
		"node", "add",
		"--admin-url", server.URL + "/api/v1",
		"--hint", "https://controller.example.test:7443",
	}), &stdout, &stderr)
	if err != nil {
		t.Fatalf("node add: %v", err)
	}
	if operationCalls != 1 {
		t.Fatalf("operation calls = %d, want 1", operationCalls)
	}
	if stdout.String() != "tewake join "+joinCode+"\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestNodeAddRejectsMalformedOrMismatchedJoinCodeDelivery(t *testing.T) {
	validCode, validTokenID := newManagementCLITestJoinCode(t, nil)
	tests := []struct {
		name    string
		code    string
		tokenID string
	}{
		{
			name:    "malformed code",
			code:    "join-code-delivered-once",
			tokenID: validTokenID,
		},
		{
			name:    "token id mismatch",
			code:    validCode,
			tokenID: strings.Repeat("f", 32),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newManagementCLITestServer(t, func(writer http.ResponseWriter, _ *http.Request) {
				writeJSON(writer, http.StatusCreated, map[string]string{
					"code":    test.code,
					"tokenId": test.tokenID,
				})
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			err := run(
				withManagementState(t, []string{
					"node", "add", "--admin-url", server.URL + "/api/v1",
				}),
				&stdout,
				&stderr,
			)
			if err == nil ||
				!strings.Contains(err.Error(), "join-code response is invalid") {
				t.Fatalf("error = %v, want invalid join-code response", err)
			}
			formatted := err.Error() + stdout.String() + stderr.String()
			if strings.Contains(formatted, validCode) {
				t.Fatalf("command leaked rejected join code: %q", formatted)
			}
		})
	}
}

func TestConfigExportWritesServerYAMLUnchanged(t *testing.T) {
	payload := []byte("schemaVersion: 1\nrevision: 7\nscheduler: {}\nnodes: []\nrunnerProfiles: []\ntargets: []\n")
	server := newManagementCLITestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet ||
			request.URL.Path != "/api/v1/configuration/export" {
			t.Fatalf("operation request = %s %s", request.Method, request.URL.Path)
		}
		if _, err := request.Cookie(auth.SessionCookieName); err != nil {
			t.Fatalf("operation session cookie: %v", err)
		}
		writer.Header().Set("Content-Type", "application/yaml")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(payload)
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	if err := run(withManagementState(t, []string{
		"config", "export", "--admin-url", server.URL + "/api/v1",
	}), &stdout, &stderr); err != nil {
		t.Fatalf("config export: %v", err)
	}
	if !bytes.Equal(stdout.Bytes(), payload) {
		t.Fatalf("stdout = %q, want unchanged YAML", stdout.Bytes())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestConfigApplyPreservesYAMLBodyAndUsesDocumentRevision(t *testing.T) {
	payload := []byte("schemaVersion: 1\nrevision: 7\nscheduler: {}\nnodes: []\nrunnerProfiles: []\ntargets: []\n")
	path := filepath.Join(t.TempDir(), "configuration.yaml")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	server := newManagementCLITestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		assertApplyRequest(t, request, payload, "application/yaml", `"cfg-7"`)
		writeJSON(writer, http.StatusOK, map[string]any{
			"schemaVersion":  1,
			"revision":       "8",
			"scheduler":      map[string]any{},
			"nodes":          []any{},
			"runnerProfiles": []any{},
			"targets":        []any{},
		})
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	if err := run(withManagementState(t, []string{
		"config", "apply", path, "--admin-url", server.URL + "/api/v1",
	}), &stdout, &stderr); err != nil {
		t.Fatalf("config apply: %v", err)
	}
	if stdout.String() != "Configuration applied.\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestConfigApplyReadsJSONFromStdinWithoutReencoding(t *testing.T) {
	payload := []byte("{\n  \"schemaVersion\": 1,\n  \"revision\": \"19\",\n  \"scheduler\": {},\n  \"nodes\": [],\n  \"runnerProfiles\": [],\n  \"targets\": []\n}\n")
	server := newManagementCLITestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		assertApplyRequest(t, request, payload, "application/json", `"cfg-19"`)
		writeJSON(writer, http.StatusOK, map[string]any{
			"schemaVersion":  1,
			"revision":       "20",
			"scheduler":      map[string]any{},
			"nodes":          []any{},
			"runnerProfiles": []any{},
			"targets":        []any{},
		})
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := runWithInput(
		withManagementState(t, []string{
			"config", "apply", "-", "--admin-url", server.URL + "/api/v1",
		}),
		bytes.NewReader(payload),
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("config apply stdin: %v", err)
	}
	if stdout.String() != "Configuration applied.\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestConfigApplyRejectsMalformedOrUnexpectedRevisionSuccessResponse(t *testing.T) {
	payload := []byte("schemaVersion: 1\nrevision: 7\nscheduler: {}\nnodes: []\nrunnerProfiles: []\ntargets: []\n")
	path := filepath.Join(t.TempDir(), "configuration.yaml")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		body string
	}{
		{
			name: "malformed JSON",
			body: `{"schemaVersion":1`,
		},
		{
			name: "unknown secret field",
			body: `{"schemaVersion":1,"revision":"8","scheduler":{},"nodes":[],"runnerProfiles":[],"targets":[],"serverSecret":"` + testRawSecret + `"}`,
		},
		{
			name: "unchanged revision",
			body: `{"schemaVersion":1,"revision":"7","scheduler":{},"nodes":[],"runnerProfiles":[],"targets":[]}`,
		},
		{
			name: "skipped revision",
			body: `{"schemaVersion":1,"revision":"9","scheduler":{},"nodes":[],"runnerProfiles":[],"targets":[]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newManagementCLITestServer(t, func(writer http.ResponseWriter, request *http.Request) {
				assertApplyRequest(t, request, payload, "application/yaml", `"cfg-7"`)
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(writer, test.body)
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			err := run(
				withManagementState(t, []string{
					"config", "apply", path, "--admin-url", server.URL + "/api/v1",
				}),
				&stdout,
				&stderr,
			)
			if err == nil ||
				!strings.Contains(err.Error(), "configuration apply response is invalid") {
				t.Fatalf("error = %v, want invalid apply response", err)
			}
			formatted := err.Error() + stdout.String() + stderr.String()
			if strings.Contains(formatted, testRawSecret) {
				t.Fatalf("command leaked rejected apply response: %q", formatted)
			}
		})
	}
}

func TestManagementClientDisablesEnvironmentProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	client, err := newManagementAPIClient(
		defaultAdminURL,
		testManagementBootstrapProof(t, "http://127.0.0.1:7442"),
	)
	if err != nil {
		t.Fatalf("new management API client: %v", err)
	}
	httpClient, ok := client.client.Client.(*http.Client)
	if !ok {
		t.Fatalf("management HTTP client type = %T", client.client.Client)
	}
	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("management transport type = %T", httpClient.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("management transport retained an environment proxy resolver")
	}
}

func TestManagementCommandsRejectNonCanonicalOrNonLoopbackAdminURL(t *testing.T) {
	tests := []string{
		"https://127.0.0.1:7442/api/v1",
		"http://192.0.2.1:7442/api/v1",
		"http://example.test:7442/api/v1",
		"http://localhost:7442/api/v1",
		"http://127.0.0.1:7442/",
		"http://127.0.0.1:7442/api/v1/",
		"http://127.0.0.1:7442/api/v1?query=yes",
		"http://user@127.0.0.1:7442/api/v1",
	}
	for _, adminURL := range tests {
		t.Run(adminURL, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run([]string{
				"node", "add", "--admin-url", adminURL,
			}, &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), "admin URL is invalid") {
				t.Fatalf("error = %v, want invalid admin URL", err)
			}
			if strings.Contains(err.Error(), adminURL) {
				t.Fatalf("error echoed rejected URL: %v", err)
			}
		})
	}
}

func TestSessionBootstrapRequiresExactlyOneNamedSessionCookie(t *testing.T) {
	tests := []struct {
		name    string
		cookies []*http.Cookie
	}{
		{name: "missing"},
		{
			name: "unexpected cookie",
			cookies: []*http.Cookie{{
				Name:  "unrelated",
				Value: "unexpected-cookie",
				Path:  "/",
			}},
		},
		{
			name: "expected and extra cookie",
			cookies: []*http.Cookie{
				{Name: auth.SessionCookieName, Value: testSessionCookie, Path: "/"},
				{Name: "unrelated", Value: "unexpected-cookie", Path: "/"},
			},
		},
		{
			name: "empty expected cookie",
			cookies: []*http.Cookie{{
				Name:  auth.SessionCookieName,
				Value: "",
				Path:  "/",
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodPost || request.URL.Path != "/api/v1/session" {
					t.Fatalf("request = %s %s", request.Method, request.URL.Path)
				}
				for _, cookie := range test.cookies {
					http.SetCookie(writer, cookie)
				}
				writeJSON(writer, http.StatusCreated, map[string]any{
					"authenticated": true,
					"csrfToken":     testCSRFToken,
				})
			}))
			defer server.Close()

			client, err := newManagementAPIClient(
				server.URL+"/api/v1",
				testManagementBootstrapProof(t, server.URL),
			)
			if err != nil {
				t.Fatalf("new management API client: %v", err)
			}
			err = client.bootstrap(t.Context())
			if err == nil || !strings.Contains(err.Error(), "session cookie is missing") {
				t.Fatalf("bootstrap error = %v, want exact session cookie failure", err)
			}
		})
	}
}

func TestManagementAPIErrorsExposeOnlyOperationAndStatus(t *testing.T) {
	validYAML := []byte("schemaVersion: 1\nrevision: 3\nscheduler: {}\nnodes: []\nrunnerProfiles: []\ntargets: []\n")
	path := filepath.Join(t.TempDir(), "configuration.yaml")
	if err := os.WriteFile(path, validYAML, 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		status int
		args   func(string) []string
	}{
		{
			name:   "join code unavailable",
			status: http.StatusServiceUnavailable,
			args: func(base string) []string {
				return []string{"node", "add", "--admin-url", base}
			},
		},
		{
			name:   "export unauthorized",
			status: http.StatusUnauthorized,
			args: func(base string) []string {
				return []string{"config", "export", "--admin-url", base}
			},
		},
		{
			name:   "apply stale",
			status: http.StatusConflict,
			args: func(base string) []string {
				return []string{"config", "apply", path, "--admin-url", base}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newManagementCLITestServer(t, func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/problem+json")
				writer.WriteHeader(test.status)
				_, _ = io.WriteString(writer, `{"code":"`+testRawSecret+`","detail":"`+testRawSecret+`"}`)
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			err := run(
				withManagementState(t, test.args(server.URL+"/api/v1")),
				&stdout,
				&stderr,
			)
			if err == nil || !strings.Contains(err.Error(), "HTTP "+strconv.Itoa(test.status)) {
				t.Fatalf("error = %v, want HTTP %d", err, test.status)
			}
			formatted := err.Error() + stdout.String() + stderr.String()
			if strings.Contains(formatted, testRawSecret) {
				t.Fatalf("command leaked raw response secret: %q", formatted)
			}
		})
	}
}

func TestSessionBootstrapFailureDoesNotExposeRawResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/problem+json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(writer, `{"detail":%q}`, testRawSecret)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := run(withManagementState(t, []string{
		"config", "export", "--admin-url", server.URL + "/api/v1",
	}), &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("error = %v, want HTTP 503", err)
	}
	if strings.Contains(err.Error(), testRawSecret) {
		t.Fatalf("bootstrap error leaked raw response: %v", err)
	}
}

func TestConfigApplyRejectsInvalidSecretBearingInputBeforeSessionBootstrap(t *testing.T) {
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requestCount++
	}))
	defer server.Close()

	inputSecret := "configuration-secret-must-not-leak"
	payload := []byte("schemaVersion: 1\nrevision: 3\nscheduler: {}\nnodes: []\nrunnerProfiles: []\ntargets: []\nclientSecret: " + inputSecret + "\n")
	var stdout, stderr bytes.Buffer
	err := runWithInput(
		[]string{"config", "apply", "-", "--admin-url", server.URL + "/api/v1"},
		bytes.NewReader(payload),
		&stdout,
		&stderr,
	)
	if err == nil {
		t.Fatal("config apply accepted an unknown secret field")
	}
	if requestCount != 0 {
		t.Fatalf("invalid input made %d management API requests", requestCount)
	}
	if strings.Contains(err.Error(), inputSecret) {
		t.Fatalf("decode error leaked configuration input: %v", err)
	}
}

func TestConfigExportRejectsSecretBearingSuccessResponseWithoutPrintingIt(t *testing.T) {
	server := newManagementCLITestServer(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/yaml")
		writer.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(
			writer,
			"schemaVersion: 1\nrevision: 3\nscheduler: {}\nnodes: []\nrunnerProfiles: []\ntargets: []\nclientSecret: "+testRawSecret+"\n",
		)
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := run(withManagementState(t, []string{
		"config", "export", "--admin-url", server.URL + "/api/v1",
	}), &stdout, &stderr)
	if err == nil {
		t.Fatal("config export accepted a secret-bearing success response")
	}
	formatted := err.Error() + stdout.String() + stderr.String()
	if strings.Contains(formatted, testRawSecret) {
		t.Fatalf("config export leaked a secret-bearing response: %q", formatted)
	}
}

func newManagementCLITestServer(
	t *testing.T,
	operation http.HandlerFunc,
) *httptest.Server {
	t.Helper()
	var server *httptest.Server
	var manager *auth.Manager
	var managerOnce sync.Once
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost && request.URL.Path == "/api/v1/session" {
			if request.Header.Get("Origin") != server.URL {
				t.Fatalf("bootstrap Origin = %q, want %q", request.Header.Get("Origin"), server.URL)
			}
			managerOnce.Do(func() {
				var err error
				manager, err = auth.NewManager(testManagementRoot(), server.URL, false)
				if err != nil {
					t.Fatalf("create test management auth manager: %v", err)
				}
			})
			if err := manager.ValidateBootstrap(request); err != nil {
				t.Fatalf("validate owner bootstrap proof: %v", err)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			if len(body) != 0 {
				t.Fatalf("bootstrap body = %q, want empty", body)
			}
			http.SetCookie(writer, &http.Cookie{
				Name:     auth.SessionCookieName,
				Value:    testSessionCookie,
				Path:     "/",
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
			})
			writeJSON(writer, http.StatusCreated, map[string]any{
				"authenticated": true,
				"csrfToken":     testCSRFToken,
			})
			return
		}
		if request.Method == http.MethodDelete && request.URL.Path == "/api/v1/session" {
			if request.Header.Get("Origin") != server.URL {
				t.Fatalf("logout Origin = %q, want %q", request.Header.Get("Origin"), server.URL)
			}
			if request.Header.Get(auth.CSRFHeaderName) != testCSRFToken {
				t.Fatalf("logout CSRF = %q", request.Header.Get(auth.CSRFHeaderName))
			}
			if _, err := request.Cookie(auth.SessionCookieName); err != nil {
				t.Fatalf("logout session cookie: %v", err)
			}
			if request.Header.Get(auth.BootstrapHeaderName) != "" {
				t.Fatal("logout leaked the owner bootstrap proof")
			}
			http.SetCookie(writer, &http.Cookie{
				Name:     auth.SessionCookieName,
				Path:     "/",
				MaxAge:   -1,
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
			})
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		if request.Header.Get("Origin") != server.URL {
			t.Fatalf("operation Origin = %q, want %q", request.Header.Get("Origin"), server.URL)
		}
		if request.Header.Get(auth.BootstrapHeaderName) != "" {
			t.Fatal("operation leaked the owner bootstrap proof")
		}
		operation(writer, request)
	}))
	return server
}

func withManagementState(t *testing.T, args []string) []string {
	t.Helper()

	directory := t.TempDir()
	previous := loadManagementBootstrapProof
	loadManagementBootstrapProof = func(_ string, origin string) (string, error) {
		return testManagementBootstrapProof(t, origin), nil
	}
	t.Cleanup(func() {
		loadManagementBootstrapProof = previous
	})
	result := append([]string(nil), args...)
	return append(result, "--state-dir", directory)
}

func testManagementBootstrapProof(t *testing.T, origin string) string {
	t.Helper()

	proof, err := auth.NewBootstrapProof(
		testManagementRoot(),
		origin,
		time.Now(),
		rand.Reader,
	)
	if err != nil {
		t.Fatalf("create management bootstrap proof: %v", err)
	}
	return proof
}

func testManagementRoot() [32]byte {
	var root [32]byte
	for index := range root {
		root[index] = byte(index + 1)
	}
	return root
}

func newManagementCLITestJoinCode(t *testing.T, hints []string) (string, string) {
	t.Helper()
	var fingerprint [sha256.Size]byte
	for index := range fingerprint {
		fingerprint[index] = byte(index + 1)
	}
	entropy := append(
		bytes.Repeat([]byte{0x11}, 16),
		bytes.Repeat([]byte{0x22}, 32)...,
	)
	code, err := enroll.NewJoinCode(fingerprint, hints, bytes.NewReader(entropy))
	if err != nil {
		t.Fatalf("create join code: %v", err)
	}
	encoded, err := code.Encode()
	if err != nil {
		t.Fatalf("encode join code: %v", err)
	}
	tokenID := code.TokenID()
	return encoded, hex.EncodeToString(tokenID[:])
}

func canonicalManagementCLITestBrowserHandoffCode() string {
	return "twh1.1." + strings.Repeat("A", 22) + "." +
		strings.Repeat("A", 43) + "." + strings.Repeat("A", 43)
}

func assertApplyRequest(
	t *testing.T,
	request *http.Request,
	wantBody []byte,
	wantContentType string,
	wantIfMatch string,
) {
	t.Helper()
	if request.Method != http.MethodPut ||
		request.URL.Path != "/api/v1/configuration" {
		t.Fatalf("operation request = %s %s", request.Method, request.URL.Path)
	}
	if request.Header.Get("Content-Type") != wantContentType {
		t.Fatalf("content type = %q, want %q", request.Header.Get("Content-Type"), wantContentType)
	}
	if request.Header.Get("If-Match") != wantIfMatch {
		t.Fatalf("If-Match = %q, want %q", request.Header.Get("If-Match"), wantIfMatch)
	}
	if request.Header.Get("X-Tewake-CSRF") != testCSRFToken {
		t.Fatalf("CSRF = %q", request.Header.Get("X-Tewake-CSRF"))
	}
	if _, err := request.Cookie(auth.SessionCookieName); err != nil {
		t.Fatalf("session cookie: %v", err)
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, wantBody) {
		t.Fatalf("request body was changed:\ngot:  %q\nwant: %q", body, wantBody)
	}
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func serverOrigin(request *http.Request) string {
	return "http://" + request.Host
}
