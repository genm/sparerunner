package github

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func testCredential(t *testing.T) AppCredential {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	credential, err := NewAppCredential(123, "Iv1.client", string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})))
	if err != nil {
		t.Fatal(err)
	}
	return credential
}

type authorityRoundTripper struct {
	deletedGroup       bool
	createEchoesPublic bool
	t                  *testing.T
	private            bool
	unsafeGroup        bool
	createdGroup       bool
	scaleSetCalls      int
	runnerGroupPages   map[string]string
}

func (transport *authorityRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.t.Helper()
	var status int = http.StatusOK
	var body string
	path := request.URL.Path
	switch {
	case path == "/app/installations" && request.Method == http.MethodGet:
		// The live endpoint returns a bare array; the wrapped {installations: []}
		// shape belongs to /user/installations and must not leak into this fake.
		body = `[{"id":42,"repository_selection":"all","account":{"login":"acme","type":"Organization"}},{"id":43,"repository_selection":"selected","account":{"login":"genm","type":"User"}}]`
	case strings.HasSuffix(path, "/access_tokens") && request.Method == http.MethodPost:
		body = `{"token":"ghs_test_token"}`
	case path == "/repos/acme/private" && request.Method == http.MethodGet:
		if transport.private {
			body = `{"private":true,"visibility":"private"}`
		} else {
			body = `{"private":false,"visibility":"public"}`
		}
	case strings.HasSuffix(path, "/actions/runner-groups") && request.Method == http.MethodGet:
		if transport.runnerGroupPages != nil {
			body = transport.runnerGroupPages[request.URL.Query().Get("page")]
			if body == "" {
				body = `{"runner_groups":[]}`
			}
		} else if transport.unsafeGroup {
			body = `{"runner_groups":[{"id":7,"name":"Default","visibility":"all","allows_public_repositories":true}]}`
		} else {
			body = `{"runner_groups":[{"id":7,"name":"Default","visibility":"private","allows_public_repositories":false}]}`
		}
	case strings.Contains(path, "/actions/runner-groups/") && request.Method == http.MethodDelete:
		transport.deletedGroup = true
		status = http.StatusNoContent
		body = ""
	case strings.HasSuffix(path, "/actions/runner-groups") && request.Method == http.MethodPost:
		transport.createdGroup = true
		if transport.createEchoesPublic {
			var unsafeRequest struct {
				Name string `json:"name"`
			}
			payload, _ := io.ReadAll(request.Body)
			_ = json.Unmarshal(payload, &unsafeRequest)
			body = `{"id":8,"name":"` + unsafeRequest.Name + `","visibility":"all","allows_public_repositories":true}`
			break
		}
		// Live GitHub echoes the requested name but coerces any requested
		// visibility to "all" for an organization runner group; the fake
		// mirrors the live echo so the verifier is tested against reality
		// rather than the request we sent.
		var groupRequest struct {
			Name string `json:"name"`
		}
		payload, _ := io.ReadAll(request.Body)
		_ = json.Unmarshal(payload, &groupRequest)
		body = `{"id":8,"name":"` + groupRequest.Name + `","visibility":"all","allows_public_repositories":false}`
	default:
		status = http.StatusNotFound
		body = `{}`
	}
	return &http.Response{StatusCode: status, Body: authorityNopCloser{Reader: bytes.NewReader([]byte(body))}, Header: make(http.Header), Request: request}, nil
}

func TestManifestStateIsSignedOneUseAndCredentialIsRedacted(t *testing.T) {
	credential := testCredential(t)
	store := &MemoryAppCredentialStore{}
	client := &http.Client{Transport: authorityRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]any{"id": credential.AppID, "client_id": credential.ClientID, "pem": credential.privateKey})
		return &http.Response{StatusCode: http.StatusCreated, Body: authorityNopCloser{Reader: bytes.NewReader(body)}, Header: make(http.Header), Request: request}, nil
	})}
	manager, err := NewManifestManager(store, nil, nil, client)
	if err != nil {
		t.Fatal(err)
	}
	start, err := manager.Start("http://127.0.0.1:7443/api/v1/github/app/callback", "")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(start.Manifest+start.State, credential.privateKey) {
		t.Fatal("manifest state exposed the private key")
	}
	if _, err := manager.Complete(context.Background(), "one-time-code", start.State); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Complete(context.Background(), "one-time-code", start.State); !errors.Is(err, ErrManifestStateConsumed) {
		t.Fatalf("replay error = %v", err)
	}
	tampered := start.State[:len("twm1_")] + "A" + start.State[len("twm1_")+1:]
	if _, err := manager.Complete(context.Background(), "one-time-code", tampered); !errors.Is(err, ErrManifestStateInvalid) {
		t.Fatalf("tampered state error = %v", err)
	}
	if got := credential.String(); strings.Contains(got, credential.privateKey) || !strings.Contains(got, "redacted") {
		t.Fatalf("credential redaction = %q", got)
	}
}

func TestAuthorityRejectsUnknownAndPublicTargetsBeforeScaleSetCreation(t *testing.T) {
	credential := testCredential(t)
	store := &MemoryAppCredentialStore{}
	if err := store.Save(credential); err != nil {
		t.Fatal(err)
	}
	transport := &authorityRoundTripper{t: t, private: false}
	scaleSetCalls := 0
	authority, err := NewAuthority(AuthorityOptions{CredentialStore: store, HTTPClient: &http.Client{Transport: transport}, CreateScaleSet: func(context.Context, AppClientConfig, ScaleSet) (ScaleSet, error) {
		scaleSetCalls++
		return ScaleSet{ID: 99, Name: "sparerunner", RunnerGroupID: 7, Labels: []string{"sparerunner"}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	request := TargetRequest{TargetID: "target-private", InstallationID: "42", ScopeKind: "repository", Scope: "acme/private", ScaleSetName: "sparerunner", RunnerProfileID: "profile-sparerunner", RunnerProfileLabel: "sparerunner"}
	if _, err := authority.VerifyAndProvisionTarget(context.Background(), request); !errors.Is(err, ErrGitHubTargetNotPrivate) {
		t.Fatalf("public target error = %v", err)
	}
	if scaleSetCalls != 0 {
		t.Fatal("public target reached scale-set creation")
	}
	request.InstallationID = "404"
	if _, err := authority.VerifyAndProvisionTarget(context.Background(), request); !errors.Is(err, ErrGitHubInstallation) {
		t.Fatalf("unknown installation error = %v", err)
	}
}

func TestAuthorityCreatesPrivateOrganizationGroupAndScaleSet(t *testing.T) {
	credential := testCredential(t)
	store := &MemoryAppCredentialStore{}
	_ = store.Save(credential)
	transport := &authorityRoundTripper{t: t, private: true}
	authority, err := NewAuthority(AuthorityOptions{CredentialStore: store, HTTPClient: &http.Client{Transport: transport}, CreateScaleSet: func(_ context.Context, _ AppClientConfig, requested ScaleSet) (ScaleSet, error) {
		return ScaleSet{ID: 99, Name: requested.Name, RunnerGroupID: requested.RunnerGroupID, Labels: requested.Labels}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := authority.VerifyAndProvisionTarget(context.Background(), TargetRequest{TargetID: "org", InstallationID: "42", ScopeKind: "organization", Scope: "acme", ScaleSetName: "sparerunner", RunnerProfileID: "profile-sparerunner", RunnerProfileLabel: "sparerunner"})
	if err != nil {
		t.Fatal(err)
	}
	if result.ScaleSetID != 99 || result.RunnerGroupID != 8 || !transport.createdGroup {
		t.Fatalf("verified target = %+v groupCreated=%v", result, transport.createdGroup)
	}
}

// TestAuthorityRemovesTheGroupWhenTheCreateEchoIsUnsafe pins the live-found
// leak: GitHub accepted the create (201) but echoed attributes the verifier
// refuses, and the old path returned unsafe while leaving the freshly created
// SpareRunner-named group behind in the organization on every rejected attempt.
func TestAuthorityRemovesTheGroupWhenTheCreateEchoIsUnsafe(t *testing.T) {
	credential := testCredential(t)
	store := &MemoryAppCredentialStore{}
	_ = store.Save(credential)
	transport := &authorityRoundTripper{t: t, private: true, createEchoesPublic: true}
	authority, err := NewAuthority(AuthorityOptions{CredentialStore: store, HTTPClient: &http.Client{Transport: transport}, CreateScaleSet: func(_ context.Context, _ AppClientConfig, requested ScaleSet) (ScaleSet, error) {
		t.Fatal("unsafe group reached scale-set creation")
		return ScaleSet{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = authority.VerifyAndProvisionTarget(context.Background(), TargetRequest{TargetID: "org", InstallationID: "42", ScopeKind: "organization", Scope: "acme", ScaleSetName: "sparerunner", RunnerProfileID: "profile-sparerunner", RunnerProfileLabel: "sparerunner"})
	if !errors.Is(err, ErrGitHubRunnerGroupUnsafe) {
		t.Fatalf("unsafe echo error = %v", err)
	}
	if !transport.createdGroup || !transport.deletedGroup {
		t.Fatalf("created=%v deleted=%v; the rejected group must be removed", transport.createdGroup, transport.deletedGroup)
	}
}

func TestAuthorityPaginatesRunnerGroupsBeforeProvisioning(t *testing.T) {
	credential := testCredential(t)
	store := &MemoryAppCredentialStore{}
	if err := store.Save(credential); err != nil {
		t.Fatal(err)
	}
	pageOne := make([]map[string]any, 100)
	for index := range pageOne {
		pageOne[index] = map[string]any{
			"id":                         index + 100,
			"name":                       fmt.Sprintf("group-%d", index),
			"visibility":                 "private",
			"allows_public_repositories": false,
		}
	}
	pageOneJSON, err := json.Marshal(map[string]any{"runner_groups": pageOne})
	if err != nil {
		t.Fatal(err)
	}
	transport := &authorityRoundTripper{
		t:       t,
		private: true,
		runnerGroupPages: map[string]string{
			"1": string(pageOneJSON),
			"2": `{"runner_groups":[{"id":777,"name":"sparerunner-target","visibility":"private","allows_public_repositories":false}]}`,
		},
	}
	authority, err := NewAuthority(AuthorityOptions{
		CredentialStore: store,
		HTTPClient:      &http.Client{Transport: transport},
		CreateScaleSet: func(context.Context, AppClientConfig, ScaleSet) (ScaleSet, error) {
			transport.scaleSetCalls++
			return ScaleSet{ID: 99, Name: "sparerunner", RunnerGroupID: 777, Labels: []string{"sparerunner"}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = authority.VerifyAndProvisionTarget(context.Background(), TargetRequest{
		TargetID:           "target",
		InstallationID:     "42",
		ScopeKind:          "organization",
		Scope:              "acme",
		ScaleSetName:       "sparerunner",
		RunnerProfileID:    "profile-sparerunner",
		RunnerProfileLabel: "sparerunner",
	})
	if !errors.Is(err, ErrGitHubTargetConflict) {
		t.Fatalf("paginated conflict error = %v", err)
	}
	if transport.scaleSetCalls != 0 || transport.createdGroup {
		t.Fatalf("provider was mutated after paginated conflict: scaleSets=%d createdGroup=%v", transport.scaleSetCalls, transport.createdGroup)
	}
}

type authorityRoundTripFunc func(*http.Request) (*http.Response, error)

func (function authorityRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type authorityNopCloser struct{ *bytes.Reader }

func (authorityNopCloser) Close() error { return nil }
