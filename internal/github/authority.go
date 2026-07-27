package github

// This file owns the small amount of GitHub REST API surface needed by the
// management UI. It deliberately stays below the app package: provider
// responses are validated here before they become Tewake authority.

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	ErrGitHubNotConnected          = errors.New("GitHub App is not connected")
	ErrGitHubInstallation          = errors.New("GitHub App installation is not available")
	ErrGitHubTargetNotPrivate      = errors.New("GitHub target is not private")
	ErrGitHubRunnerGroupUnsafe     = errors.New("GitHub runner group is unsafe")
	ErrGitHubTargetConflict        = errors.New("GitHub target conflicts with existing authority")
	ErrGitHubProviderFailure       = errors.New("GitHub provider request failed")
	ErrGitHubProvisioningAmbiguous = errors.New("GitHub target provisioning result is ambiguous")
	ErrGitHubTargetInvalid         = errors.New("GitHub target is invalid")
)

const (
	defaultGitHubAPIBase = "https://api.github.com"
	appJWTLifetime       = 10 * time.Minute
)

type Installation struct {
	ID                  int64
	AccountLogin        string
	AccountType         string
	RepositorySelection string
}

type TargetRequest struct {
	TargetID           string
	InstallationID     string
	ScopeKind          string
	Scope              string
	ScaleSetName       string
	RunnerProfileID    string
	RunnerProfileLabel string
}

type VerifiedTarget struct {
	TargetID        string
	InstallationID  string
	ScopeKind       string
	Scope           string
	ScaleSetName    string
	RunnerProfileID string
	ScaleSetID      ScaleSetID
	RunnerGroupID   int
}

type ScaleSetCreator func(context.Context, AppClientConfig, ScaleSet) (ScaleSet, error)

type AuthorityOptions struct {
	CredentialStore AppCredentialStore
	HTTPClient      *http.Client
	APIBaseURL      string
	Now             func() time.Time
	Random          io.Reader
	CreateScaleSet  ScaleSetCreator
}

type Authority struct {
	manifest       *ManifestManager
	credentials    AppCredentialStore
	client         *http.Client
	apiBaseURL     string
	now            func() time.Time
	createScaleSet ScaleSetCreator
}

func NewAuthority(options AuthorityOptions) (*Authority, error) {
	if options.CredentialStore == nil {
		return nil, ErrSecretStoreUnavailable
	}
	client := options.HTTPClient
	if client == nil {
		client = newHardenedRetryableClient().HTTPClient
	}
	base := options.APIBaseURL
	if base == "" {
		base = defaultGitHubAPIBase
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Hostname() != "api.github.com" {
		return nil, ErrUnsafeGitHubEndpoint
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	manifest, err := NewManifestManager(options.CredentialStore, now, options.Random, client)
	if err != nil {
		return nil, err
	}
	creator := options.CreateScaleSet
	if creator == nil {
		creator = func(ctx context.Context, config AppClientConfig, requested ScaleSet) (ScaleSet, error) {
			client, err := NewAppClient(config)
			if err != nil {
				return ScaleSet{}, ErrGitHubProviderFailure
			}
			return client.CreateScaleSet(ctx, requested)
		}
	}
	return &Authority{manifest: manifest, credentials: options.CredentialStore, client: client, apiBaseURL: strings.TrimRight(base, "/"), now: now, createScaleSet: creator}, nil
}

func (authority *Authority) Connected() (bool, error) {
	if authority == nil || authority.credentials == nil {
		return false, ErrGitHubNotConnected
	}
	_, present, err := authority.credentials.Load()
	if err != nil {
		return false, err
	}
	return present, nil
}

func (authority *Authority) StartManifest(callbackURL, account string) (ManifestStart, error) {
	if authority == nil || authority.manifest == nil {
		return ManifestStart{}, ErrGitHubNotConnected
	}
	return authority.manifest.Start(callbackURL, account)
}

func (authority *Authority) CompleteManifest(ctx context.Context, code, state string) error {
	if authority == nil || authority.manifest == nil {
		return ErrGitHubNotConnected
	}
	_, err := authority.manifest.Complete(ctx, code, state)
	return err
}

func (authority *Authority) ListInstallations(ctx context.Context) ([]Installation, error) {
	credential, err := authority.credential()
	if err != nil {
		return nil, err
	}
	token, err := authority.appJWT(credential)
	if err != nil {
		return nil, ErrGitHubProviderFailure
	}
	var result []Installation
	complete := false
	for page := 1; page <= 100; page++ {
		// GET /app/installations returns a bare JSON array, unlike
		// /user/installations whose response wraps the list in an object.
		// The live GitHub API is the authority here; decoding the wrapped
		// shape silently reported every configured App as unavailable.
		var response []struct {
			ID                  int64  `json:"id"`
			RepositorySelection string `json:"repository_selection"`
			Account             struct {
				Login string `json:"login"`
				Type  string `json:"type"`
			} `json:"account"`
		}
		if err := authority.doJSON(ctx, http.MethodGet, "/app/installations?per_page=100&page="+strconv.Itoa(page), token, nil, &response); err != nil {
			return nil, err
		}
		for _, item := range response {
			if item.ID <= 0 || !canonicalPathPart(item.Account.Login) || (item.Account.Type != "User" && item.Account.Type != "Organization") {
				return nil, ErrGitHubProviderFailure
			}
			result = append(result, Installation{ID: item.ID, AccountLogin: item.Account.Login, AccountType: item.Account.Type, RepositorySelection: item.RepositorySelection})
		}
		if len(response) < 100 {
			complete = true
			break
		}
	}
	if !complete {
		// A full final page is indistinguishable from a truncated provider
		// response. Never expose a partial installation list as authoritative.
		return nil, ErrGitHubProviderFailure
	}
	return result, nil
}

func (authority *Authority) VerifyAndProvisionTarget(ctx context.Context, request TargetRequest) (VerifiedTarget, error) {
	if err := validateTargetRequest(request); err != nil {
		return VerifiedTarget{}, err
	}
	credential, err := authority.credential()
	if err != nil {
		return VerifiedTarget{}, err
	}
	installations, err := authority.ListInstallations(ctx)
	if err != nil {
		return VerifiedTarget{}, err
	}
	installationID, _ := strconv.ParseInt(request.InstallationID, 10, 64)
	var installation Installation
	found := false
	for _, candidate := range installations {
		if candidate.ID == installationID {
			installation = candidate
			found = true
			break
		}
	}
	if !found {
		return VerifiedTarget{}, ErrGitHubInstallation
	}
	owner := request.Scope
	if request.ScopeKind == "repository" {
		owner = strings.Split(request.Scope, "/")[0]
	}
	if !strings.EqualFold(owner, installation.AccountLogin) {
		return VerifiedTarget{}, ErrGitHubInstallation
	}
	if request.ScopeKind == "organization" && installation.AccountType != "Organization" {
		return VerifiedTarget{}, ErrGitHubInstallation
	}
	installationToken, err := authority.issueInstallationToken(ctx, installationID, credential)
	if err != nil {
		return VerifiedTarget{}, err
	}
	if request.ScopeKind == "repository" {
		if err := authority.verifyRepositoryPrivacy(ctx, request, installationToken); err != nil {
			return VerifiedTarget{}, err
		}
	}
	groupID, createdGroup, err := authority.verifyRunnerGroup(ctx, request, installationToken)
	if err != nil {
		return VerifiedTarget{}, err
	}
	requested := ScaleSet{Name: request.ScaleSetName, RunnerGroupID: groupID, Labels: []string{request.RunnerProfileLabel}}
	created, err := authority.createScaleSet(ctx, AppClientConfig{
		GitHubConfigURL: "https://github.com/" + request.Scope,
		ClientID:        credential.ClientID,
		InstallationID:  installationID,
		PrivateKey:      credential.PrivateKey(),
		System:          "tewake",
		Version:         "dev",
		Subsystem:       "controller",
	}, requested)
	if err != nil {
		if createdGroup {
			if cleanupErr := authority.deleteRunnerGroup(ctx, request, installationToken, groupID); cleanupErr != nil {
				return VerifiedTarget{}, ErrGitHubProvisioningAmbiguous
			}
		}
		// A transport failure after the provider accepted the create request is
		// indistinguishable from a pre-request failure. Keep the intent
		// ambiguous until reconciliation proves which external object exists.
		return VerifiedTarget{}, ErrGitHubProvisioningAmbiguous
	}
	if created.ID <= 0 || created.Name != request.ScaleSetName || created.RunnerGroupID != groupID || len(created.Labels) != 1 || created.Labels[0] != request.RunnerProfileLabel {
		if createdGroup {
			if cleanupErr := authority.deleteRunnerGroup(ctx, request, installationToken, groupID); cleanupErr != nil {
				return VerifiedTarget{}, ErrGitHubProvisioningAmbiguous
			}
		}
		return VerifiedTarget{}, ErrGitHubProvisioningAmbiguous
	}
	return VerifiedTarget{TargetID: request.TargetID, InstallationID: request.InstallationID, ScopeKind: request.ScopeKind, Scope: request.Scope, ScaleSetName: request.ScaleSetName, RunnerProfileID: request.RunnerProfileID, ScaleSetID: created.ID, RunnerGroupID: groupID}, nil
}

func (authority *Authority) verifyRepositoryPrivacy(ctx context.Context, request TargetRequest, token string) error {
	parts := strings.Split(request.Scope, "/")
	var repository struct {
		Private    bool   `json:"private"`
		Visibility string `json:"visibility"`
	}
	if err := authority.doJSON(ctx, http.MethodGet, "/repos/"+url.PathEscape(parts[0])+"/"+url.PathEscape(parts[1]), token, nil, &repository); err != nil {
		return err
	}
	if !repository.Private || (repository.Visibility != "" && repository.Visibility != "private") {
		return ErrGitHubTargetNotPrivate
	}
	return nil
}

func (authority *Authority) credential() (AppCredential, error) {
	if authority == nil || authority.credentials == nil {
		return AppCredential{}, ErrGitHubNotConnected
	}
	credential, present, err := authority.credentials.Load()
	if err != nil {
		return AppCredential{}, err
	}
	if !present {
		return AppCredential{}, ErrGitHubNotConnected
	}
	if err := credential.Validate(); err != nil {
		return AppCredential{}, ErrAppCredentialInvalid
	}
	return credential, nil
}

func (authority *Authority) appJWT(credential AppCredential) (string, error) {
	block, _ := pemDecode([]byte(credential.privateKey))
	if block == nil || len(block) > maximumAppKeySize {
		return "", ErrAppCredentialInvalid
	}
	key, err := parseRSAKey(block)
	if err != nil {
		return "", err
	}
	now := authority.now().Unix()
	header := map[string]string{"alg": "RS256", "typ": "JWT"}
	claims := map[string]any{"iat": now - 60, "exp": now + int64(appJWTLifetime/time.Second), "iss": credential.AppID}
	encode := func(value any) (string, error) {
		body, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return base64.RawURLEncoding.EncodeToString(body), nil
	}
	encodedHeader, err := encode(header)
	if err != nil {
		return "", err
	}
	encodedClaims, err := encode(claims)
	if err != nil {
		return "", err
	}
	unsigned := encodedHeader + "." + encodedClaims
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (authority *Authority) issueInstallationToken(ctx context.Context, id int64, credential AppCredential) (string, error) {
	jwt, err := authority.appJWT(credential)
	if err != nil {
		return "", ErrGitHubProviderFailure
	}
	var response struct {
		Token string `json:"token"`
	}
	if err := authority.doJSON(ctx, http.MethodPost, "/app/installations/"+strconv.FormatInt(id, 10)+"/access_tokens", jwt, bytes.NewReader([]byte("{}")), &response); err != nil || response.Token == "" || len(response.Token) > 4096 {
		return "", ErrGitHubProviderFailure
	}
	return response.Token, nil
}

type runnerGroup struct {
	ID                       int    `json:"id"`
	Name                     string `json:"name"`
	Visibility               string `json:"visibility"`
	AllowsPublicRepositories bool   `json:"allows_public_repositories"`
}

func (authority *Authority) verifyRunnerGroup(ctx context.Context, request TargetRequest, token string) (int, bool, error) {
	path := "/orgs/" + url.PathEscape(request.Scope) + "/actions/runner-groups"
	if request.ScopeKind == "repository" {
		parts := strings.Split(request.Scope, "/")
		path = "/repos/" + url.PathEscape(parts[0]) + "/" + url.PathEscape(parts[1]) + "/actions/runner-groups"
	}
	runnerGroups, err := authority.listRunnerGroups(ctx, path, token)
	if err != nil {
		return 0, false, err
	}
	ownedName := "tewake-" + request.TargetID
	for _, group := range runnerGroups {
		// The owned name is claimed regardless of the group's attributes: a
		// same-named group with different settings is still someone else's
		// object, and adopting it would launder its access policy as ours.
		if group.Name == ownedName {
			return 0, false, ErrGitHubTargetConflict
		}
		if request.ScopeKind == "repository" &&
			group.ID > 0 && group.Visibility == "private" && !group.AllowsPublicRepositories {
			return group.ID, false, nil
		}
	}
	if request.ScopeKind == "repository" {
		return 0, false, ErrGitHubRunnerGroupUnsafe
	}
	// Live GitHub accepts only "all" and "selected" for an organization runner
	// group and silently coerces any other value, so requesting "private" used
	// to come back as "all" and fail verification on every real organization.
	// "all" matches the organization Target's own semantics — the whole private
	// scope routes here — and the safety property Tewake actually needs is that
	// public repositories can never reach the group.
	var created runnerGroup
	body := bytes.NewReader([]byte(`{"name":"` + ownedName + `","visibility":"all","allows_public_repositories":false,"restricted_to_workflows":false}`))
	if err := authority.doJSON(ctx, http.MethodPost, "/orgs/"+url.PathEscape(request.Scope)+"/actions/runner-groups", token, body, &created); err != nil {
		return 0, false, ErrGitHubRunnerGroupUnsafe
	}
	if created.ID <= 0 || created.Name != ownedName ||
		created.Visibility != "all" || created.AllowsPublicRepositories {
		// The provider accepted the create but echoed different attributes.
		// The group exists, so failing without removing it would leak a
		// Tewake-named object into the organization on every rejected attempt.
		if created.ID > 0 {
			if cleanupErr := authority.deleteRunnerGroup(ctx, request, token, created.ID); cleanupErr != nil {
				return 0, false, ErrGitHubProvisioningAmbiguous
			}
		}
		return 0, false, ErrGitHubRunnerGroupUnsafe
	}
	return created.ID, true, nil
}

func (authority *Authority) listRunnerGroups(ctx context.Context, path, token string) ([]runnerGroup, error) {
	groups := make([]runnerGroup, 0)
	complete := false
	for page := 1; page <= 100; page++ {
		var response struct {
			RunnerGroups []runnerGroup `json:"runner_groups"`
		}
		pagePath := path + "?per_page=100&page=" + strconv.Itoa(page)
		if err := authority.doJSON(ctx, http.MethodGet, pagePath, token, nil, &response); err != nil {
			return nil, err
		}
		groups = append(groups, response.RunnerGroups...)
		if len(response.RunnerGroups) < 100 {
			complete = true
			break
		}
	}
	if !complete {
		// A full final page is indistinguishable from a truncated provider
		// response. Never provision from a partial runner-group view.
		return nil, ErrGitHubProviderFailure
	}
	return groups, nil
}

func (authority *Authority) deleteRunnerGroup(ctx context.Context, request TargetRequest, token string, id int) error {
	if id <= 0 || request.ScopeKind != "organization" {
		return nil
	}
	return authority.doJSON(ctx, http.MethodDelete, "/orgs/"+url.PathEscape(request.Scope)+"/actions/runner-groups/"+strconv.Itoa(id), token, nil, nil)
}

func (authority *Authority) doJSON(ctx context.Context, method, path, bearer string, body io.Reader, result any) error {
	if authority == nil || authority.client == nil || ctx == nil || !strings.HasPrefix(path, "/") {
		return ErrGitHubProviderFailure
	}
	operationContext, cancel := WithFiniteOperationTimeout(ctx)
	defer cancel()
	request, err := http.NewRequestWithContext(operationContext, method, authority.apiBaseURL+path, body)
	if err != nil {
		return ErrGitHubProviderFailure
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("User-Agent", "tewake")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response, err := authority.client.Do(request)
	if err != nil || response == nil {
		return ErrGitHubProviderFailure
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(io.LimitReader(response.Body, maxPreviewResponseBody+1))
	if err != nil || len(contents) > maxPreviewResponseBody || response.StatusCode < 200 || response.StatusCode >= 300 {
		return ErrGitHubProviderFailure
	}
	if result == nil || len(contents) == 0 {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	if err := decoder.Decode(result); err != nil {
		return ErrGitHubProviderFailure
	}
	if err := requireJSONEOF(decoder); err != nil {
		return ErrGitHubProviderFailure
	}
	return nil
}

func validateTargetRequest(request TargetRequest) error {
	if !canonicalPathPart(request.TargetID) || !canonicalPathPart(request.InstallationID) || !canonicalPathPart(request.ScaleSetName) || !canonicalPathPart(request.RunnerProfileID) || !canonicalPathPart(request.RunnerProfileLabel) {
		return ErrGitHubTargetInvalid
	}
	if _, err := strconv.ParseInt(request.InstallationID, 10, 64); err != nil {
		return ErrGitHubTargetInvalid
	}
	switch request.ScopeKind {
	case "organization":
		if !canonicalPathPart(request.Scope) {
			return ErrGitHubTargetInvalid
		}
	case "repository":
		parts := strings.Split(request.Scope, "/")
		if len(parts) != 2 || !canonicalPathPart(parts[0]) || !canonicalPathPart(parts[1]) {
			return ErrGitHubTargetInvalid
		}
	default:
		return ErrGitHubTargetInvalid
	}
	if !strings.EqualFold(request.ScaleSetName, request.RunnerProfileLabel) {
		return ErrGitHubTargetInvalid
	}
	return nil
}

// Small wrappers keep PEM parsing private to this adapter while allowing the
// credential validation and JWT signer to share the exact RSA acceptance rule.
func pemDecode(raw []byte) ([]byte, string) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, ""
	}
	return block.Bytes, block.Type
}

func parseRSAKey(raw []byte) (*rsa.PrivateKey, error) {
	if key, err := x509.ParsePKCS1PrivateKey(raw); err == nil {
		return key, key.Validate()
	}
	parsed, err := x509.ParsePKCS8PrivateKey(raw)
	if err != nil {
		return nil, ErrAppCredentialInvalid
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, ErrAppCredentialInvalid
	}
	return key, key.Validate()
}
