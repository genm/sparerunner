package github

import (
	"errors"
	"os"
	"runtime"
	"testing"
)

func TestAppCredentialPayloadRoundTripRejectsUnknownAndTrailingData(t *testing.T) {
	want := testCredential(t)
	payload, err := encodeAppCredential(want)
	if err != nil {
		t.Fatalf("encodeAppCredential() error = %v", err)
	}
	got, err := decodeAppCredential(payload)
	if err != nil {
		t.Fatalf("decodeAppCredential() error = %v", err)
	}
	if got.AppID != want.AppID || got.ClientID != want.ClientID ||
		got.privateKey != want.privateKey {
		t.Fatalf("decoded credential metadata = %#v, want %#v", got, want)
	}

	for _, name := range []string{
		"{\"version\":1,\"appId\":1,\"clientId\":\"client\",\"privateKey\":\"key\",\"extra\":true}",
		string(payload) + "\n{}",
	} {
		if _, err := decodeAppCredential([]byte(name)); !errors.Is(err, ErrAppCredentialInvalid) {
			t.Fatalf("decodeAppCredential(%q) error = %v, want invalid", name, err)
		}
	}
}

func TestNewPlatformAppCredentialStoreSelectsNativeBoundary(t *testing.T) {
	store := NewPlatformAppCredentialStore(filepathForCredentialTest(t))
	switch runtime.GOOS {
	case "darwin", "windows":
		if _, ok := store.(PrivateMaterialAppCredentialStore); !ok {
			t.Fatalf("store type = %T, want native private-material store", store)
		}
	default:
		if _, ok := store.(FileAppCredentialStore); !ok {
			t.Fatalf("store type = %T, want service-user file store", store)
		}
	}
}

func TestPrivateMaterialAppCredentialStoreMissingIsNotConfigured(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("native credential locator preflight requires the host service boundary")
	}
	store := PrivateMaterialAppCredentialStore{Path: filepathForCredentialTest(t)}
	_, configured, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if configured {
		t.Fatal("Load() reported a missing credential as configured")
	}
}

func filepathForCredentialTest(t *testing.T) string {
	t.Helper()
	return t.TempDir() + string(os.PathSeparator) + "github-app-credential.json"
}
