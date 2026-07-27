//go:build darwin

package enroll

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lexfrei/keychain"
)

var errInjectedDarwinPersistence = errors.New("injected macOS persistence failure")

type fakeDarwinCredentialStore struct {
	items       map[string][]byte
	createErr   error
	getErr      error
	deleteErr   error
	createCalls int
	deleteCalls int
	missingErr  bool
}

func newFakeDarwinCredentialStore() *fakeDarwinCredentialStore {
	return &fakeDarwinCredentialStore{items: make(map[string][]byte)}
}

func (store *fakeDarwinCredentialStore) Create(service, account string, contents []byte) error {
	store.createCalls++
	if store.createErr != nil {
		return store.createErr
	}
	key := service + "\x00" + account
	if _, exists := store.items[key]; exists {
		return errDarwinCredentialExists
	}
	store.items[key] = append([]byte(nil), contents...)
	return nil
}

func (store *fakeDarwinCredentialStore) Get(service, account string) ([]byte, error) {
	if store.getErr != nil {
		return nil, store.getErr
	}
	contents, exists := store.items[service+"\x00"+account]
	if !exists {
		return nil, errDarwinCredentialUnavailable
	}
	return append([]byte(nil), contents...), nil
}

func (store *fakeDarwinCredentialStore) Delete(service, account string) error {
	store.deleteCalls++
	if store.deleteErr != nil {
		return store.deleteErr
	}
	key := service + "\x00" + account
	if _, exists := store.items[key]; !exists && store.missingErr {
		return keychain.ErrNotFound
	}
	delete(store.items, key)
	return nil
}

func testDarwinPersistenceOps(store darwinCredentialStore, seed byte) darwinPersistenceOps {
	return darwinPersistenceOps{
		credentials:   store,
		random:        bytes.NewReader(bytes.Repeat([]byte{seed}, darwinAccountBytes*8)),
		publish:       atomicPrivateFile,
		removeLocator: os.Remove,
		syncParent:    syncDirectory,
	}
}

func privateDarwinTestDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func TestDarwinPrivateMaterialStoresOnlyRandomLocatorOnDisk(t *testing.T) {
	store := newFakeDarwinCredentialStore()
	ops := testDarwinPersistenceOps(store, 0x41)
	path := filepath.Join(privateDarwinTestDirectory(t), "node-private-key.pem")
	secret := []byte("-----BEGIN PRIVATE KEY-----\nsuper-secret-canary\n")

	if err := persistDarwinPrivateMaterial(path, secret, ops); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, secret) ||
		bytes.Contains(raw, []byte("PRIVATE KEY")) ||
		bytes.Contains(raw, []byte("super-secret-canary")) {
		t.Fatalf("locator contains private material: %q", raw)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("locator mode = %o, want 600", info.Mode().Perm())
	}
	locator, err := readDarwinCredentialLocator(path)
	if err != nil {
		t.Fatal(err)
	}
	if locator.Account != strings.Repeat("41", darwinAccountBytes) {
		t.Fatalf("account = %q", locator.Account)
	}
	references, err := collectDarwinCredentialReferences(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(references) != 1 || len(references[0].paths) != 1 ||
		references[0].paths[0] != path {
		t.Fatalf("successful publication retained recovery references: %#v", references)
	}

	loaded, err := loadDarwinPrivateMaterial(path, ops)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(loaded, secret) {
		t.Fatal("private material did not round-trip exactly")
	}
	clear(loaded)
	if err := removeDarwinPrivateMaterial(path, ops); err != nil {
		t.Fatal(err)
	}
	if len(store.items) != 0 {
		t.Fatal("Keychain item remained after removal")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("locator remained after removal: %v", err)
	}
}

func TestDarwinPrivateMaterialLocatorSurvivesStatePublicationRename(t *testing.T) {
	store := newFakeDarwinCredentialStore()
	ops := testDarwinPersistenceOps(store, 0x42)
	root := privateDarwinTestDirectory(t)
	staging := filepath.Join(root, "staging")
	published := filepath.Join(root, "published")
	if err := os.Mkdir(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	stagingPath := filepath.Join(staging, "controller-identity.pem")
	secret := []byte("controller-key-material")
	if err := persistDarwinPrivateMaterial(stagingPath, secret, ops); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staging, published); err != nil {
		t.Fatal(err)
	}
	publishedPath := filepath.Join(published, "controller-identity.pem")
	loaded, err := loadDarwinPrivateMaterial(publishedPath, ops)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(loaded, secret) {
		t.Fatal("renamed locator loaded different material")
	}
	clear(loaded)
	if err := removeDarwinPrivateMaterial(publishedPath, ops); err != nil {
		t.Fatal(err)
	}
}

func TestDarwinPrivateMaterialNeverClobbersExistingLocator(t *testing.T) {
	store := newFakeDarwinCredentialStore()
	ops := testDarwinPersistenceOps(store, 0x43)
	path := filepath.Join(privateDarwinTestDirectory(t), "private-material")
	first := []byte("first-secret")
	if err := persistDarwinPrivateMaterial(path, first, ops); err != nil {
		t.Fatal(err)
	}
	if err := persistDarwinPrivateMaterial(path, []byte("replacement-secret"), ops); err == nil {
		t.Fatal("existing locator was clobbered")
	}
	if store.createCalls != 1 || len(store.items) != 1 {
		t.Fatalf("Keychain writes = %d, items = %d", store.createCalls, len(store.items))
	}
	loaded, err := loadDarwinPrivateMaterial(path, ops)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(loaded, first) {
		t.Fatal("existing private material changed")
	}
}

func TestDarwinPrivateMaterialPublishFailureDeletesOwnedKeychainItem(t *testing.T) {
	tests := []struct {
		name      string
		afterLink bool
	}{
		{
			name: "before locator link",
		},
		{
			name:      "after locator link",
			afterLink: true,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newFakeDarwinCredentialStore()
			ops := testDarwinPersistenceOps(store, byte(0x44+index))
			path := filepath.Join(privateDarwinTestDirectory(t), "private-material")
			ops.publish = func(candidate string, contents []byte) error {
				if candidate != path {
					return atomicPrivateFile(candidate, contents)
				}
				if test.afterLink {
					if err := atomicPrivateFile(candidate, contents); err != nil {
						return err
					}
				}
				return errInjectedDarwinPersistence
			}
			if err := persistDarwinPrivateMaterial(path, []byte("rollback-canary"), ops); err == nil {
				t.Fatal("injected publication failure succeeded")
			}
			if len(store.items) != 0 {
				t.Fatal("publication failure orphaned a Keychain item")
			}
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("publication failure left a locator: %v", err)
			}
			references, err := collectDarwinCredentialReferences(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(references) != 0 {
				t.Fatalf("publication rollback left recovery references: %#v", references)
			}
		})
	}
}

func TestDarwinPrivateMaterialCombinedPublishAndDeleteFailureRetainsRecovery(t *testing.T) {
	store := newFakeDarwinCredentialStore()
	store.deleteErr = errInjectedDarwinPersistence
	ops := testDarwinPersistenceOps(store, 0x46)
	path := filepath.Join(privateDarwinTestDirectory(t), "private-material")
	ops.publish = func(candidate string, contents []byte) error {
		if candidate == path {
			return errInjectedDarwinPersistence
		}
		return atomicPrivateFile(candidate, contents)
	}

	if err := persistDarwinPrivateMaterial(path, []byte("combined-fault-canary"), ops); err == nil {
		t.Fatal("combined locator publication and Keychain deletion failure succeeded")
	}
	if len(store.items) != 1 {
		t.Fatal("combined failure lost the still-live Keychain item")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed main locator unexpectedly exists: %v", err)
	}
	references, err := collectDarwinCredentialReferences(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(references) != 1 || len(references[0].paths) != 1 ||
		references[0].paths[0] == path {
		t.Fatalf("combined failure recovery references = %#v", references)
	}
	recoveryContents, err := os.ReadFile(references[0].paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(recoveryContents, []byte("combined-fault-canary")) {
		t.Fatal("recovery locator contains private material")
	}
	if _, err := loadDarwinPrivateMaterial(path, ops); !errors.Is(
		err,
		errDarwinCredentialRecovery,
	) {
		t.Fatalf("recovery-only state load error = %v", err)
	}

	store.deleteErr = nil
	ops.publish = atomicPrivateFile
	if err := removeDarwinPrivateMaterial(path, ops); err != nil {
		t.Fatal(err)
	}
	if len(store.items) != 0 {
		t.Fatal("recovery retry left the Keychain item")
	}
	references, err = collectDarwinCredentialReferences(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(references) != 0 {
		t.Fatalf("recovery retry left locators: %#v", references)
	}
}

func TestDarwinPrivateMaterialDeleteFailureRetainsRecoveryLocator(t *testing.T) {
	store := newFakeDarwinCredentialStore()
	ops := testDarwinPersistenceOps(store, 0x47)
	path := filepath.Join(privateDarwinTestDirectory(t), "private-material")
	if err := persistDarwinPrivateMaterial(path, []byte("deletion-canary"), ops); err != nil {
		t.Fatal(err)
	}
	store.deleteErr = errInjectedDarwinPersistence
	if err := removeDarwinPrivateMaterial(path, ops); err == nil {
		t.Fatal("Keychain deletion failure succeeded")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("recovery locator was removed: %v", err)
	}
	if len(store.items) != 1 {
		t.Fatal("failed deletion changed the Keychain item")
	}

	store.deleteErr = nil
	if err := removeDarwinPrivateMaterial(path, ops); err != nil {
		t.Fatal(err)
	}
	if err := removeDarwinPrivateMaterial(path, ops); err != nil {
		t.Fatalf("idempotent removal failed: %v", err)
	}
	if len(store.items) != 0 {
		t.Fatal("successful retry left a Keychain item")
	}
}

func TestDarwinRecoveryOnlyMissingItemRemovalIsIdempotent(t *testing.T) {
	store := newFakeDarwinCredentialStore()
	store.missingErr = true
	ops := testDarwinPersistenceOps(store, 0x48)
	path := filepath.Join(privateDarwinTestDirectory(t), "private-material")
	locator := darwinCredentialLocator{
		Version: darwinLocatorVersion,
		Backend: darwinLocatorBackend,
		Service: darwinKeychainService,
		Account: strings.Repeat("48", darwinAccountBytes),
	}
	encoded, err := json.Marshal(locator)
	if err != nil {
		t.Fatal(err)
	}
	recoveryPath := darwinRecoveryLocatorPath(path, locator.Account)
	if err := atomicPrivateFile(recoveryPath, encoded); err != nil {
		t.Fatal(err)
	}

	if err := removeDarwinPrivateMaterial(path, ops); err != nil {
		t.Fatalf("missing Keychain item blocked recovery removal: %v", err)
	}
	if err := removeDarwinPrivateMaterial(path, ops); err != nil {
		t.Fatalf("idempotent recovery removal: %v", err)
	}
	if _, err := os.Lstat(recoveryPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovery locator remained: %v", err)
	}
}

func TestDarwinMultipleLocatorRemovalRetriesAfterItemAlreadyDeleted(t *testing.T) {
	store := newFakeDarwinCredentialStore()
	store.missingErr = true
	ops := testDarwinPersistenceOps(store, 0x49)
	path := filepath.Join(privateDarwinTestDirectory(t), "private-material")
	if err := persistDarwinPrivateMaterial(path, []byte("partial-removal-canary"), ops); err != nil {
		t.Fatal(err)
	}
	locator, err := readDarwinCredentialLocator(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(locator)
	if err != nil {
		t.Fatal(err)
	}
	recoveryPath := darwinRecoveryLocatorPath(path, locator.Account)
	if err := atomicPrivateFile(recoveryPath, encoded); err != nil {
		t.Fatal(err)
	}
	failMainRemoval := true
	ops.removeLocator = func(candidate string) error {
		if candidate == path && failMainRemoval {
			failMainRemoval = false
			return errInjectedDarwinPersistence
		}
		return os.Remove(candidate)
	}
	if err := removeDarwinPrivateMaterial(path, ops); err == nil {
		t.Fatal("partial locator removal failure succeeded")
	}
	if len(store.items) != 0 {
		t.Fatal("first removal did not delete the Keychain item")
	}
	if _, err := os.Lstat(recoveryPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("successfully removed recovery locator remains: %v", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("failed main locator removal was not retryable: %v", err)
	}

	ops.removeLocator = os.Remove
	if err := removeDarwinPrivateMaterial(path, ops); err != nil {
		t.Fatalf("not-found Keychain retry failed: %v", err)
	}
	if store.deleteCalls != 2 {
		t.Fatalf("Keychain delete attempts = %d, want 2", store.deleteCalls)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retry left main locator: %v", err)
	}
}

func TestDarwinPrivateMaterialRejectsTamperedLocators(t *testing.T) {
	t.Run("shared mode", func(t *testing.T) {
		store := newFakeDarwinCredentialStore()
		ops := testDarwinPersistenceOps(store, 0x4a)
		path := filepath.Join(privateDarwinTestDirectory(t), "private-material")
		if err := persistDarwinPrivateMaterial(path, []byte("mode-canary"), ops); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o640); err != nil {
			t.Fatal(err)
		}
		if _, err := loadDarwinPrivateMaterial(path, ops); err == nil {
			t.Fatal("shared locator mode was accepted")
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		store := newFakeDarwinCredentialStore()
		ops := testDarwinPersistenceOps(store, 0x4b)
		path := filepath.Join(privateDarwinTestDirectory(t), "private-material")
		if err := persistDarwinPrivateMaterial(path, []byte("schema-canary"), ops); err != nil {
			t.Fatal(err)
		}
		locator, err := readDarwinCredentialLocator(path)
		if err != nil {
			t.Fatal(err)
		}
		tampered := []byte(`{"version":1,"backend":"macos-keychain","service":"` +
			locator.Service + `","account":"` + locator.Account + `","extra":true}`)
		if err := os.WriteFile(path, tampered, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadDarwinPrivateMaterial(path, ops); err == nil {
			t.Fatal("unknown locator field was accepted")
		}
	})

	t.Run("symlink", func(t *testing.T) {
		store := newFakeDarwinCredentialStore()
		ops := testDarwinPersistenceOps(store, 0x4c)
		directory := privateDarwinTestDirectory(t)
		target := filepath.Join(directory, "target")
		if err := persistDarwinPrivateMaterial(target, []byte("link-canary"), ops); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(directory, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, err := loadDarwinPrivateMaterial(link, ops); err == nil {
			t.Fatal("locator symlink was accepted")
		}
	})

	t.Run("hard link", func(t *testing.T) {
		store := newFakeDarwinCredentialStore()
		ops := testDarwinPersistenceOps(store, 0x4d)
		directory := privateDarwinTestDirectory(t)
		path := filepath.Join(directory, "private-material")
		if err := persistDarwinPrivateMaterial(path, []byte("hardlink-canary"), ops); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(path, filepath.Join(directory, "second-link")); err != nil {
			t.Fatal(err)
		}
		if _, err := loadDarwinPrivateMaterial(path, ops); err == nil {
			t.Fatal("multiply-linked locator was accepted")
		}
	})

	t.Run("user-owned ancestor symlink", func(t *testing.T) {
		store := newFakeDarwinCredentialStore()
		ops := testDarwinPersistenceOps(store, 0x4e)
		root := privateDarwinTestDirectory(t)
		realDirectory := filepath.Join(root, "real")
		if err := os.Mkdir(realDirectory, 0o700); err != nil {
			t.Fatal(err)
		}
		linkDirectory := filepath.Join(root, "linked")
		if err := os.Symlink(realDirectory, linkDirectory); err != nil {
			t.Fatal(err)
		}
		if err := persistDarwinPrivateMaterial(
			filepath.Join(linkDirectory, "private-material"),
			[]byte("ancestor-canary"),
			ops,
		); err == nil {
			t.Fatal("user-owned ancestor symlink was accepted")
		}
	})
}

func TestDarwinPrivateMaterialFailsClosedWhenKeychainIsUnavailable(t *testing.T) {
	store := newFakeDarwinCredentialStore()
	ops := testDarwinPersistenceOps(store, 0x4f)
	path := filepath.Join(privateDarwinTestDirectory(t), "private-material")
	secret := []byte("unavailable-canary")
	if err := persistDarwinPrivateMaterial(path, secret, ops); err != nil {
		t.Fatal(err)
	}
	locator, err := readDarwinCredentialLocator(path)
	if err != nil {
		t.Fatal(err)
	}
	store.getErr = errors.Join(errDarwinCredentialUnavailable, keychain.ErrLocked)
	_, err = loadDarwinPrivateMaterial(path, ops)
	if !errors.Is(err, errDarwinCredentialUnavailable) ||
		!errors.Is(err, keychain.ErrLocked) {
		t.Fatalf("unavailable Keychain error = %v", err)
	}
	if strings.Contains(err.Error(), string(secret)) || strings.Contains(err.Error(), locator.Account) {
		t.Fatalf("credential error disclosed private material or locator identity: %v", err)
	}
}

func TestDarwinPrivateMaterialRandomFailureCreatesNothing(t *testing.T) {
	store := newFakeDarwinCredentialStore()
	ops := testDarwinPersistenceOps(store, 0x50)
	ops.random = io.LimitReader(bytes.NewReader([]byte{1}), 1)
	path := filepath.Join(privateDarwinTestDirectory(t), "private-material")
	if err := persistDarwinPrivateMaterial(path, []byte("random-canary"), ops); err == nil {
		t.Fatal("short random source succeeded")
	}
	if store.createCalls != 0 || len(store.items) != 0 {
		t.Fatal("random failure created a Keychain item")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("random failure published a locator: %v", err)
	}
}
