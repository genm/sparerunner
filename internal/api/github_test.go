package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/api/gen"
	"github.com/genm/sparerunner/internal/auth"
)

type githubAPIBackend struct {
	*apiTestBackend
	completed bool
}

func (backend *githubAPIBackend) StartGitHubAppManifest(context.Context, string, string) (gen.GitHubManifestStart, error) {
	return gen.GitHubManifestStart{ActionUrl: "https://github.com/settings/apps/new", Manifest: `{"name":"Tewake"}`, State: "twm1_test", ExpiresAt: testManifestExpiry()}, nil
}

func (backend *githubAPIBackend) CompleteGitHubAppManifest(context.Context, string, string) error {
	backend.completed = true
	return nil
}

func (backend *githubAPIBackend) ListGitHubInstallations(context.Context) (gen.GitHubInstallationList, error) {
	return gen.GitHubInstallationList{Installations: []gen.GitHubInstallation{{Id: "42", AccountLogin: "acme", AccountType: gen.GitHubInstallationAccountTypeOrganization, RepositorySelection: "all"}}}, nil
}

func (backend *githubAPIBackend) CreateGitHubTarget(context.Context, uint64, gen.CreateGitHubTargetRequest, string) (gen.GitHubTargetMutation, error) {
	return gen.GitHubTargetMutation{}, ErrBackendUnavailable
}

func testManifestExpiry() (result time.Time) { return time.Date(2026, 7, 27, 0, 10, 0, 0, time.UTC) }

func TestGitHubManagementRoutesKeepAuthAndCallbackStateBoundaries(t *testing.T) {
	manager, err := auth.NewManager(apiTestRoot(), apiTestOrigin, false)
	if err != nil {
		t.Fatal(err)
	}
	base := &apiTestBackend{auditHealthy: true, configuration: gen.Configuration{SchemaVersion: gen.ConfigurationSchemaVersionN1, Revision: "0", Scheduler: gen.SchedulerConfiguration{}, Nodes: []gen.NodeConfiguration{}, RunnerProfiles: []gen.RunnerProfile{}, Targets: []gen.TargetConfiguration{}}}
	backend := &githubAPIBackend{apiTestBackend: base}
	handler, err := NewHandler(Options{Auth: manager, Backend: backend, Events: NewEventBus(), UI: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(writer, "ui") }), Epoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	cookie, csrf := bootstrapAPISession(t, handler)
	manifestBody := []byte(`{"registrationAccount":"acme"}`)
	missingCSRF := httptest.NewRequest(http.MethodPost, apiTestOrigin+"/api/v1/github/app/manifest", bytes.NewReader(manifestBody))
	missingCSRF.Header.Set("Origin", apiTestOrigin)
	missingCSRF.Header.Set("Content-Type", "application/json")
	missingCSRF.AddCookie(cookie)
	missingResponse := httptest.NewRecorder()
	handler.ServeHTTP(missingResponse, missingCSRF)
	if missingResponse.Code != http.StatusForbidden {
		t.Fatalf("missing csrf status = %d", missingResponse.Code)
	}
	start := httptest.NewRequest(http.MethodPost, apiTestOrigin+"/api/v1/github/app/manifest", bytes.NewReader(manifestBody))
	start.Header.Set("Origin", apiTestOrigin)
	start.Header.Set("Content-Type", "application/json")
	start.Header.Set("X-SpareRunner-CSRF", csrf)
	start.AddCookie(cookie)
	startResponse := httptest.NewRecorder()
	handler.ServeHTTP(startResponse, start)
	if startResponse.Code != http.StatusOK || strings.Contains(startResponse.Body.String(), "private") {
		t.Fatalf("manifest response = %d %s", startResponse.Code, startResponse.Body.String())
	}
	installations := httptest.NewRequest(http.MethodGet, apiTestOrigin+"/api/v1/github/installations", nil)
	installations.AddCookie(cookie)
	installationResponse := httptest.NewRecorder()
	handler.ServeHTTP(installationResponse, installations)
	if installationResponse.Code != http.StatusOK || !strings.Contains(installationResponse.Body.String(), "acme") {
		t.Fatalf("installations response = %d %s", installationResponse.Code, installationResponse.Body.String())
	}
	callback := httptest.NewRequest(http.MethodGet, apiTestOrigin+"/api/v1/github/app/callback?code=temporary&state=twm1_test", nil)
	callbackResponse := httptest.NewRecorder()
	handler.ServeHTTP(callbackResponse, callback)
	if callbackResponse.Code != http.StatusFound || !backend.completed {
		t.Fatalf("callback response = %d completed=%v", callbackResponse.Code, backend.completed)
	}
	var decoded gen.GitHubInstallationList
	if err := json.Unmarshal(installationResponse.Body.Bytes(), &decoded); err != nil || len(decoded.Installations) != 1 {
		t.Fatalf("decoded installations = %#v, err=%v", decoded, err)
	}
}
