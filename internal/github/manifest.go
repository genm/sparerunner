package github

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	manifestStateVersion = 1
	manifestStateTTL     = time.Hour
	manifestStatePrefix  = "twm1_"
	maximumManifestBody  = 1 << 20
	maximumManifestState = 4096
	maximumAppKeySize    = 128 << 10
	maximumPendingStates = 256
)

var (
	ErrManifestStateInvalid    = errors.New("GitHub App Manifest state is invalid")
	ErrManifestStateExpired    = errors.New("GitHub App Manifest state expired")
	ErrManifestStateConsumed   = errors.New("GitHub App Manifest state was already consumed")
	ErrManifestStateProcessing = errors.New("GitHub App Manifest state is already being completed")
	ErrManifestUnavailable     = errors.New("GitHub App Manifest provider is unavailable")
	ErrAppCredentialMissing    = errors.New("GitHub App credential is not configured")
	ErrAppCredentialInvalid    = errors.New("GitHub App credential is invalid")
	ErrSecretStoreUnavailable  = errors.New("GitHub App credential store is unavailable")
)

// AppCredential is the controller-only GitHub App identity. The private key is
// deliberately not exported and cannot be serialized by generic JSON/logging
// code. Callers receive it only as an opaque AppPrivateKey at the provider
// boundary.
type AppCredential struct {
	AppID      int64
	ClientID   string
	privateKey string
}

func NewAppCredential(appID int64, clientID, privateKey string) (AppCredential, error) {
	credential := AppCredential{AppID: appID, ClientID: clientID, privateKey: privateKey}
	if err := credential.Validate(); err != nil {
		return AppCredential{}, err
	}
	return credential, nil
}

func (credential AppCredential) Validate() error {
	if credential.AppID <= 0 || strings.TrimSpace(credential.ClientID) == "" || strings.TrimSpace(credential.privateKey) == "" || len(credential.privateKey) > maximumAppKeySize {
		return ErrAppCredentialInvalid
	}
	return validateRSAPrivateKey([]byte(credential.privateKey))
}

func (credential AppCredential) PrivateKey() AppPrivateKey {
	return NewAppPrivateKey(credential.privateKey)
}

func (credential AppCredential) String() string   { return "github.AppCredential(redacted)" }
func (credential AppCredential) GoString() string { return credential.String() }

// AppCredentialStore is the only persistence boundary for the App private key.
// Implementations must keep the raw key outside SQLite, logs, diagnostics, and
// API DTOs. NewPlatformAppCredentialStore selects the service-user file store on
// Linux and the native Keychain/DPAPI-backed store on macOS/Windows without
// changing the GitHub authority contract.
type AppCredentialStore interface {
	Load() (AppCredential, bool, error)
	Save(AppCredential) error
}

type fileAppCredential struct {
	Version    int    `json:"version"`
	AppID      int64  `json:"appId"`
	ClientID   string `json:"clientId"`
	PrivateKey string `json:"privateKey"`
}

// FileAppCredentialStore stores only the controller's private credential file.
// The containing state directory must already be private; the file is written
// atomically and is never accepted through a symlink or a relaxed mode.
type FileAppCredentialStore struct{ Path string }

func (store FileAppCredentialStore) Load() (AppCredential, bool, error) {
	if store.Path == "" {
		return AppCredential{}, false, ErrSecretStoreUnavailable
	}
	info, err := os.Lstat(store.Path)
	if errors.Is(err, os.ErrNotExist) {
		return AppCredential{}, false, nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return AppCredential{}, false, ErrSecretStoreUnavailable
	}
	contents, err := os.ReadFile(store.Path)
	if err != nil {
		return AppCredential{}, false, ErrSecretStoreUnavailable
	}
	credential, err := decodeAppCredential(contents)
	if err != nil {
		return AppCredential{}, false, err
	}
	return credential, true, nil
}

func (store FileAppCredentialStore) Save(credential AppCredential) error {
	if store.Path == "" {
		return ErrSecretStoreUnavailable
	}
	if err := credential.Validate(); err != nil {
		return err
	}
	parent := filepath.Dir(store.Path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return ErrSecretStoreUnavailable
	}
	if info, err := os.Lstat(parent); err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return ErrSecretStoreUnavailable
	}
	contents, err := encodeAppCredential(credential)
	if err != nil {
		return ErrSecretStoreUnavailable
	}
	temporary, err := os.CreateTemp(parent, ".sparerunner-github-app-*")
	if err != nil {
		return ErrSecretStoreUnavailable
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return ErrSecretStoreUnavailable
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return ErrSecretStoreUnavailable
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return ErrSecretStoreUnavailable
	}
	if err := temporary.Close(); err != nil {
		return ErrSecretStoreUnavailable
	}
	if err := os.Rename(temporaryPath, store.Path); err != nil {
		return ErrSecretStoreUnavailable
	}
	return nil
}

// ManifestStart is safe to return to a browser. The manifest and signed state
// contain no App key, installation token, or session credential.
type ManifestStart struct {
	ActionURL string
	Manifest  string
	State     string
	ExpiresAt time.Time
}

type manifestStatePayload struct {
	Version   int    `json:"v"`
	ExpiresAt int64  `json:"exp"`
	Nonce     string `json:"n"`
}

type manifestStateRecord struct {
	payload    manifestStatePayload
	processing bool
}

// ManifestManager owns a process-local signing key and one-use state ledger.
// Restart invalidates outstanding states; no callback state is written to
// SQLite, so a leaked state can never recover an App credential by itself.
type ManifestManager struct {
	mu        sync.Mutex
	key       ed25519.PrivateKey
	publicKey ed25519.PublicKey
	pending   map[[32]byte]manifestStateRecord
	store     AppCredentialStore
	now       func() time.Time
	rand      io.Reader
	client    *http.Client
}

func NewManifestManager(store AppCredentialStore, now func() time.Time, random io.Reader, client *http.Client) (*ManifestManager, error) {
	if store == nil {
		return nil, ErrSecretStoreUnavailable
	}
	if now == nil {
		now = time.Now
	}
	if random == nil {
		random = rand.Reader
	}
	if client == nil {
		client = newHardenedRetryableClient().HTTPClient
	}
	publicKey, key, err := ed25519.GenerateKey(random)
	if err != nil {
		return nil, ErrManifestUnavailable
	}
	return &ManifestManager{key: key, publicKey: publicKey, pending: make(map[[32]byte]manifestStateRecord), store: store, now: now, rand: random, client: client}, nil
}

func (manager *ManifestManager) Start(callbackURL, registrationAccount string) (ManifestStart, error) {
	if manager == nil || manager.store == nil {
		return ManifestStart{}, ErrManifestUnavailable
	}
	parsed, err := url.Parse(callbackURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ManifestStart{}, ErrManifestStateInvalid
	}
	if registrationAccount != "" && !canonicalPathPart(registrationAccount) {
		return ManifestStart{}, ErrManifestStateInvalid
	}
	nonce := make([]byte, 32)
	if _, err := io.ReadFull(manager.rand, nonce); err != nil {
		return ManifestStart{}, ErrManifestUnavailable
	}
	expiresAt := manager.now().Add(manifestStateTTL)
	payload := manifestStatePayload{Version: manifestStateVersion, ExpiresAt: expiresAt.Unix(), Nonce: base64.RawURLEncoding.EncodeToString(nonce)}
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return ManifestStart{}, ErrManifestUnavailable
	}
	payloadPart := base64.RawURLEncoding.EncodeToString(encodedPayload)
	signature := ed25519.Sign(manager.key, []byte(payloadPart))
	state := manifestStatePrefix + payloadPart + "." + base64.RawURLEncoding.EncodeToString(signature)
	digest := sha256.Sum256([]byte(state))
	manager.mu.Lock()
	for key, record := range manager.pending {
		if manager.now().Unix() > record.payload.ExpiresAt && !record.processing {
			delete(manager.pending, key)
		}
	}
	if len(manager.pending) >= maximumPendingStates {
		manager.mu.Unlock()
		return ManifestStart{}, ErrManifestUnavailable
	}
	manager.pending[digest] = manifestStateRecord{payload: payload}
	manager.mu.Unlock()
	manifest := map[string]any{
		"name":         "SpareRunner",
		"url":          "https://github.com/genm/sparerunner",
		"redirect_url": callbackURL,
		"public":       false,
		"description":  "Trusted private GitHub Actions runner fleet",
		"default_permissions": map[string]string{
			"actions":                          "write",
			"administration":                   "read",
			"metadata":                         "read",
			"organization_self_hosted_runners": "write",
		},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return ManifestStart{}, ErrManifestUnavailable
	}
	actionURL := "https://github.com/settings/apps/new"
	if registrationAccount != "" {
		actionURL = "https://github.com/organizations/" + url.PathEscape(registrationAccount) + "/settings/apps/new"
	}
	return ManifestStart{ActionURL: actionURL, Manifest: string(manifestBytes), State: state, ExpiresAt: expiresAt.UTC()}, nil
}

func (manager *ManifestManager) Complete(ctx context.Context, code, state string) (AppCredential, error) {
	if manager == nil || manager.client == nil || manager.store == nil || ctx == nil {
		return AppCredential{}, ErrManifestUnavailable
	}
	_, digest, err := manager.validateState(state)
	if err != nil {
		return AppCredential{}, err
	}
	if len(code) == 0 || len(code) > 512 || strings.ContainsAny(code, "\r\n/?#") {
		return AppCredential{}, ErrManifestStateInvalid
	}
	manager.mu.Lock()
	record, ok := manager.pending[digest]
	if !ok {
		manager.mu.Unlock()
		return AppCredential{}, ErrManifestStateConsumed
	}
	if record.processing {
		manager.mu.Unlock()
		return AppCredential{}, ErrManifestStateProcessing
	}
	if manager.now().Unix() > record.payload.ExpiresAt {
		delete(manager.pending, digest)
		manager.mu.Unlock()
		return AppCredential{}, ErrManifestStateExpired
	}
	record.processing = true
	manager.pending[digest] = record
	manager.mu.Unlock()

	credential, err := manager.exchange(ctx, code)
	if err == nil {
		err = manager.store.Save(credential)
	}
	manager.mu.Lock()
	if err == nil {
		delete(manager.pending, digest)
	} else if current, exists := manager.pending[digest]; exists {
		current.processing = false
		manager.pending[digest] = current
	}
	manager.mu.Unlock()
	if err != nil {
		return AppCredential{}, err
	}
	return credential, nil
}

func (manager *ManifestManager) validateState(state string) (manifestStatePayload, [32]byte, error) {
	var zero [32]byte
	if manager == nil || len(state) > maximumManifestState || !strings.HasPrefix(state, manifestStatePrefix) {
		return manifestStatePayload{}, zero, ErrManifestStateInvalid
	}
	parts := strings.Split(strings.TrimPrefix(state, manifestStatePrefix), ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return manifestStatePayload{}, zero, ErrManifestStateInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(manager.publicKey, []byte(parts[0]), signature) {
		return manifestStatePayload{}, zero, ErrManifestStateInvalid
	}
	encoded, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return manifestStatePayload{}, zero, ErrManifestStateInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var payload manifestStatePayload
	if err := decoder.Decode(&payload); err != nil || requireJSONEOF(decoder) != nil || payload.Version != manifestStateVersion || payload.ExpiresAt <= 0 || payload.Nonce == "" {
		return manifestStatePayload{}, zero, ErrManifestStateInvalid
	}
	digest := sha256.Sum256([]byte(state))
	manager.mu.Lock()
	record, exists := manager.pending[digest]
	manager.mu.Unlock()
	if !exists {
		return manifestStatePayload{}, zero, ErrManifestStateConsumed
	}
	if record.payload != payload {
		return manifestStatePayload{}, zero, ErrManifestStateInvalid
	}
	if manager.now().Unix() > payload.ExpiresAt {
		return manifestStatePayload{}, zero, ErrManifestStateExpired
	}
	return payload, digest, nil
}

func (manager *ManifestManager) exchange(ctx context.Context, code string) (AppCredential, error) {
	operationContext, cancel := WithFiniteOperationTimeout(ctx)
	defer cancel()
	endpoint := "https://api.github.com/app-manifests/" + url.PathEscape(code) + "/conversions"
	request, err := http.NewRequestWithContext(operationContext, http.MethodPost, endpoint, http.NoBody)
	if err != nil {
		return AppCredential{}, ErrManifestUnavailable
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "sparerunner")
	response, err := manager.client.Do(request)
	if err != nil || response == nil {
		return AppCredential{}, ErrManifestUnavailable
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumManifestBody+1))
	if err != nil || len(body) > maximumManifestBody || response.StatusCode < 200 || response.StatusCode >= 300 {
		return AppCredential{}, ErrManifestUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var result struct {
		ID       int64  `json:"id"`
		ClientID string `json:"client_id"`
		PEM      string `json:"pem"`
	}
	if err := decoder.Decode(&result); err != nil || requireJSONEOF(decoder) != nil {
		return AppCredential{}, ErrManifestUnavailable
	}
	return NewAppCredential(result.ID, result.ClientID, result.PEM)
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func canonicalPathPart(value string) bool {
	if value == "" || len(value) > 100 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

// MemoryAppCredentialStore is test-only convenience for boundary tests. It
// still copies key material and never exposes it through formatting.
type MemoryAppCredentialStore struct {
	mu         sync.Mutex
	credential AppCredential
	set        bool
}

func (store *MemoryAppCredentialStore) Load() (AppCredential, bool, error) {
	if store == nil {
		return AppCredential{}, false, ErrSecretStoreUnavailable
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.set {
		return AppCredential{}, false, nil
	}
	return store.credential, true, nil
}

func (store *MemoryAppCredentialStore) Save(credential AppCredential) error {
	if store == nil {
		return ErrSecretStoreUnavailable
	}
	if err := credential.Validate(); err != nil {
		return err
	}
	store.mu.Lock()
	store.credential = credential
	store.set = true
	store.mu.Unlock()
	return nil
}

var _ fmt.Stringer = AppCredential{}

func validateRSAPrivateKey(raw []byte) error {
	block, _ := pem.Decode(raw)
	if block == nil || !strings.Contains(strings.ToUpper(block.Type), "PRIVATE KEY") {
		return ErrAppCredentialInvalid
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		if err := key.Validate(); err == nil {
			return nil
		}
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return ErrAppCredentialInvalid
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return ErrAppCredentialInvalid
	}
	if err := key.Validate(); err != nil {
		return ErrAppCredentialInvalid
	}
	return nil
}
