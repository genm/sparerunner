package github

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"runtime"

	"github.com/genm/sparerunner/internal/enroll"
)

// NewPlatformAppCredentialStore keeps the GitHub App key behind the same
// controller-owned boundary on every supported host. Linux uses the
// service-user-only file store; macOS and Windows use the native private
// material adapters (Keychain and DPAPI respectively). The locator path is
// never the secret itself on hosts with an external credential store.
func NewPlatformAppCredentialStore(path string) AppCredentialStore {
	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		return PrivateMaterialAppCredentialStore{Path: path}
	}
	return FileAppCredentialStore{Path: path}
}

// PrivateMaterialAppCredentialStore serializes only the non-secret metadata
// alongside the private key bytes passed to enroll's platform credential
// boundary. On macOS the path is a Keychain locator; on Windows it is a DPAPI
// protected file. Save is intentionally no-clobber and idempotent for an exact
// replay so a manifest callback cannot replace an existing App identity.
type PrivateMaterialAppCredentialStore struct {
	Path string
}

func (store PrivateMaterialAppCredentialStore) Load() (AppCredential, bool, error) {
	if store.Path == "" {
		return AppCredential{}, false, ErrSecretStoreUnavailable
	}
	contents, err := enroll.LoadPrivateMaterial(store.Path)
	if errors.Is(err, os.ErrNotExist) {
		return AppCredential{}, false, nil
	}
	if err != nil {
		return AppCredential{}, false, ErrSecretStoreUnavailable
	}
	defer clear(contents)
	credential, err := decodeAppCredential(contents)
	if err != nil {
		return AppCredential{}, false, err
	}
	return credential, true, nil
}

func (store PrivateMaterialAppCredentialStore) Save(credential AppCredential) error {
	if store.Path == "" {
		return ErrSecretStoreUnavailable
	}
	if err := credential.Validate(); err != nil {
		return err
	}
	contents, err := encodeAppCredential(credential)
	if err != nil {
		return ErrSecretStoreUnavailable
	}
	defer clear(contents)
	if err := enroll.SavePrivateMaterial(store.Path, contents); err == nil {
		return nil
	}
	// A callback retry may observe the exact material already committed before
	// its response was lost. Accept that replay, but never overwrite a
	// different credential or treat an unavailable store as success.
	existing, loadErr := enroll.LoadPrivateMaterial(store.Path)
	if loadErr == nil {
		defer clear(existing)
		if bytes.Equal(existing, contents) {
			return nil
		}
	}
	return ErrSecretStoreUnavailable
}

func encodeAppCredential(credential AppCredential) ([]byte, error) {
	return json.Marshal(fileAppCredential{
		Version:    1,
		AppID:      credential.AppID,
		ClientID:   credential.ClientID,
		PrivateKey: credential.privateKey,
	})
}

func decodeAppCredential(contents []byte) (AppCredential, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var persisted fileAppCredential
	if err := decoder.Decode(&persisted); err != nil ||
		persisted.Version != 1 || persisted.AppID <= 0 ||
		persisted.ClientID == "" || persisted.PrivateKey == "" {
		return AppCredential{}, ErrAppCredentialInvalid
	}
	if err := requireJSONEOF(decoder); err != nil {
		return AppCredential{}, ErrAppCredentialInvalid
	}
	return NewAppCredential(persisted.AppID, persisted.ClientID, persisted.PrivateKey)
}
