//go:build darwin

package enroll

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/lexfrei/keychain"
	"golang.org/x/sys/unix"
)

const (
	darwinLocatorVersion  = 1
	darwinLocatorBackend  = "macos-keychain"
	darwinKeychainService = "com.genm.tewake.private-material.v1"
	darwinKeychainLabel   = "Tewake private material"
	darwinLocatorMaxBytes = 2048
	darwinAccountBytes    = 32
	darwinRecoveryPrefix  = ".tewake-keychain-recovery-v1-"
)

var (
	errDarwinCredentialExists      = errors.New("macOS Keychain item already exists")
	errDarwinCredentialUnavailable = errors.New("macOS Keychain private material is unavailable")
	errDarwinCredentialRecovery    = errors.New("macOS Keychain private material recovery is pending")
)

type darwinCredentialLocator struct {
	Version int    `json:"version"`
	Backend string `json:"backend"`
	Service string `json:"service"`
	Account string `json:"account"`
}

type darwinCredentialStore interface {
	Create(string, string, []byte) error
	Get(string, string) ([]byte, error)
	Delete(string, string) error
}

type nativeDarwinCredentialStore struct {
	keychain *keychain.Keychain
}

func newNativeDarwinCredentialStore() nativeDarwinCredentialStore {
	// TrustAll deliberately gives every process in the same Keychain user
	// context access; it does not bind access to Tewake's path or code-signing
	// identity. The packaged boundary is the root service user versus the
	// separate runner UID. A compromised root context is outside the native
	// trusted-workflow threat model.
	//
	// The trust-all decrypt ACL is not the whole prompt story: macOS also writes
	// a partition_id ACL naming the creating process, and for an ad-hoc signed
	// binary that is a per-build cdhash. A rebuilt binary therefore still gets a
	// login-password prompt until the partition list is emptied. Signed releases
	// carry a stable team identifier, so this only bites local development —
	// see `just trust-macos-keychain`.
	return nativeDarwinCredentialStore{
		keychain: keychain.New(
			keychain.WithAccessMode(keychain.TrustAll),
			keychain.WithLabel(darwinKeychainLabel),
		),
	}
}

func (store nativeDarwinCredentialStore) Create(service, account string, contents []byte) error {
	if store.keychain == nil {
		return errDarwinCredentialUnavailable
	}
	existing, err := store.keychain.Get(service, account)
	switch {
	case err == nil:
		clear(existing)
		return errDarwinCredentialExists
	case !errors.Is(err, keychain.ErrNotFound):
		return fmt.Errorf("%w: preflight failed: %w", errDarwinCredentialUnavailable, err)
	}
	if err := store.keychain.Set(service, account, contents); err != nil {
		return fmt.Errorf("%w: create failed: %w", errDarwinCredentialUnavailable, err)
	}
	return nil
}

func (store nativeDarwinCredentialStore) Get(service, account string) ([]byte, error) {
	if store.keychain == nil {
		return nil, errDarwinCredentialUnavailable
	}
	contents, err := store.keychain.Get(service, account)
	if err != nil {
		return nil, fmt.Errorf("%w: load failed: %w", errDarwinCredentialUnavailable, err)
	}
	return contents, nil
}

func (store nativeDarwinCredentialStore) Delete(service, account string) error {
	if store.keychain == nil {
		return errDarwinCredentialUnavailable
	}
	if err := store.keychain.Delete(service, account); err != nil {
		if errors.Is(err, keychain.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("%w: delete failed: %w", errDarwinCredentialUnavailable, err)
	}
	return nil
}

type darwinPersistenceOps struct {
	credentials   darwinCredentialStore
	random        io.Reader
	publish       func(string, []byte) error
	removeLocator func(string) error
	syncParent    func(string) error
}

func defaultDarwinPersistenceOps() darwinPersistenceOps {
	store := newNativeDarwinCredentialStore()
	return darwinPersistenceOps{
		credentials:   store,
		random:        rand.Reader,
		publish:       atomicPrivateFile,
		removeLocator: os.Remove,
		syncParent:    syncDirectory,
	}
}

func (ops darwinPersistenceOps) validate() error {
	if ops.credentials == nil || ops.random == nil || ops.publish == nil ||
		ops.removeLocator == nil || ops.syncParent == nil {
		return errors.New("macOS private material authority is incomplete")
	}
	return nil
}

func persistPrivateMaterial(path string, contents []byte) error {
	return persistDarwinPrivateMaterial(path, contents, defaultDarwinPersistenceOps())
}

func loadPrivateMaterial(path string) ([]byte, error) {
	return loadDarwinPrivateMaterial(path, defaultDarwinPersistenceOps())
}

func removePrivateMaterial(path string) error {
	return removeDarwinPrivateMaterial(path, defaultDarwinPersistenceOps())
}

func persistDarwinPrivateMaterial(path string, contents []byte, ops darwinPersistenceOps) error {
	if err := ops.validate(); err != nil {
		return err
	}
	if len(contents) == 0 {
		return errors.New("private material is empty")
	}
	if err := prepareDarwinLocatorDestination(path); err != nil {
		return err
	}
	account, err := randomDarwinAccount(ops.random)
	if err != nil {
		return err
	}
	locator := darwinCredentialLocator{
		Version: darwinLocatorVersion,
		Backend: darwinLocatorBackend,
		Service: darwinKeychainService,
		Account: account,
	}
	encoded, err := json.Marshal(locator)
	if err != nil {
		return err
	}
	recoveryPath := darwinRecoveryLocatorPath(path, locator.Account)
	// The recovery record is non-secret and is durable before the external
	// Keychain mutation. Therefore every crash or combined publish/delete
	// failure still leaves enough authority for RemovePrivateMaterial to retry.
	if err := ops.publish(recoveryPath, encoded); err != nil {
		cleanupErr := removeMatchingDarwinLocator(recoveryPath, locator, ops)
		return errors.Join(
			fmt.Errorf("publish macOS Keychain recovery locator: %w", err),
			cleanupErr,
		)
	}
	if err := ops.credentials.Create(locator.Service, locator.Account, contents); err != nil {
		cleanupErr := removeExpectedDarwinLocator(recoveryPath, locator, ops)
		return errors.Join(err, cleanupErr)
	}
	if err := ops.publish(path, encoded); err != nil {
		deleteErr := deleteDarwinCredential(
			ops.credentials,
			locator.Service,
			locator.Account,
		)
		if deleteErr != nil {
			// Keep the pre-published recovery locator. The base locator may also
			// exist when publication failed only at the parent-directory sync.
			return errors.Join(
				fmt.Errorf("publish macOS private material locator: %w", err),
				fmt.Errorf("roll back macOS Keychain item: %w", deleteErr),
			)
		}
		// atomicPrivateFile can report a directory-sync error after the locator
		// link was created. Remove only a locator that names our random item.
		cleanupErr := removeMatchingDarwinLocator(path, locator, ops)
		if cleanupErr != nil {
			return errors.Join(
				fmt.Errorf("publish macOS private material locator: %w", err),
				fmt.Errorf("roll back macOS private material locator: %w", cleanupErr),
			)
		}
		recoveryCleanupErr := removeExpectedDarwinLocator(recoveryPath, locator, ops)
		return errors.Join(
			fmt.Errorf("publish macOS private material locator: %w", err),
			recoveryCleanupErr,
		)
	}
	if err := removeExpectedDarwinLocator(recoveryPath, locator, ops); err != nil {
		// The main locator is already durable. Returning an error lets the
		// caller's state transaction remove both references and the item.
		return fmt.Errorf("remove macOS Keychain recovery locator: %w", err)
	}
	return nil
}

func loadDarwinPrivateMaterial(path string, ops darwinPersistenceOps) ([]byte, error) {
	if err := ops.validate(); err != nil {
		return nil, err
	}
	locator, err := readDarwinCredentialLocator(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			references, recoveryErr := collectDarwinCredentialReferences(path)
			if recoveryErr != nil {
				return nil, recoveryErr
			}
			if len(references) > 0 {
				// Never generate a replacement key while a prior Keychain
				// mutation still has durable recovery authority.
				return nil, errDarwinCredentialRecovery
			}
		}
		return nil, err
	}
	contents, err := ops.credentials.Get(locator.Service, locator.Account)
	if err != nil {
		return nil, err
	}
	if len(contents) == 0 {
		clear(contents)
		return nil, errDarwinCredentialUnavailable
	}
	copyOfContents := append([]byte(nil), contents...)
	clear(contents)
	return copyOfContents, nil
}

func removeDarwinPrivateMaterial(path string, ops darwinPersistenceOps) error {
	if err := ops.validate(); err != nil {
		return err
	}
	references, err := collectDarwinCredentialReferences(path)
	if err != nil {
		return err
	}
	if len(references) == 0 {
		return nil
	}
	var result error
	removed := false
	for _, reference := range references {
		if err := deleteDarwinCredential(
			ops.credentials,
			reference.locator.Service,
			reference.locator.Account,
		); err != nil {
			// Every path for this item remains durable so deletion can be
			// retried; other independently recoverable items may still clean.
			result = errors.Join(result, err)
			continue
		}
		for _, locatorPath := range reference.paths {
			if err := ops.removeLocator(locatorPath); err != nil {
				result = errors.Join(
					result,
					fmt.Errorf("remove macOS private material locator: %w", err),
				)
				continue
			}
			removed = true
		}
	}
	if removed {
		if err := ops.syncParent(filepath.Dir(path)); err != nil {
			result = errors.Join(
				result,
				fmt.Errorf("sync macOS private material locator parent: %w", err),
			)
		}
	}
	return result
}

func deleteDarwinCredential(
	store darwinCredentialStore,
	service string,
	account string,
) error {
	if store == nil {
		return errDarwinCredentialUnavailable
	}
	err := store.Delete(service, account)
	if errors.Is(err, keychain.ErrNotFound) {
		return nil
	}
	return err
}

func removeMatchingDarwinLocator(
	path string,
	expected darwinCredentialLocator,
	ops darwinPersistenceOps,
) error {
	actual, err := readDarwinCredentialLocator(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		// An existing path that is not our exact valid locator belongs to
		// another initializer and must never be removed.
		return nil
	}
	if actual != expected {
		return nil
	}
	if err := ops.removeLocator(path); err != nil {
		return err
	}
	return ops.syncParent(filepath.Dir(path))
}

func removeExpectedDarwinLocator(
	path string,
	expected darwinCredentialLocator,
	ops darwinPersistenceOps,
) error {
	actual, err := readDarwinCredentialLocator(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if actual != expected {
		return errors.New("macOS Keychain recovery locator changed")
	}
	if err := ops.removeLocator(path); err != nil {
		return err
	}
	return ops.syncParent(filepath.Dir(path))
}

type darwinCredentialReference struct {
	locator darwinCredentialLocator
	paths   []string
}

func collectDarwinCredentialReferences(path string) ([]darwinCredentialReference, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return nil, errors.New("private material locator path must be canonical and absolute")
	}
	parent := filepath.Dir(path)
	if err := requirePrivateDirectory(parent); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	byAccount := make(map[string]*darwinCredentialReference)
	add := func(locatorPath string, locator darwinCredentialLocator) {
		reference := byAccount[locator.Account]
		if reference == nil {
			reference = &darwinCredentialReference{locator: locator}
			byAccount[locator.Account] = reference
		}
		reference.paths = append(reference.paths, locatorPath)
	}
	locator, err := readDarwinCredentialLocator(path)
	if err == nil {
		add(path, locator)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil, err
	}
	prefix := darwinRecoveryLocatorPrefix(path)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		recoveryPath := filepath.Join(parent, entry.Name())
		recovery, err := readDarwinCredentialLocator(recoveryPath)
		if err != nil ||
			recoveryPath != darwinRecoveryLocatorPath(path, recovery.Account) {
			return nil, errors.New("invalid macOS Keychain recovery locator")
		}
		add(recoveryPath, recovery)
	}
	accounts := make([]string, 0, len(byAccount))
	for account := range byAccount {
		accounts = append(accounts, account)
	}
	sort.Strings(accounts)
	references := make([]darwinCredentialReference, 0, len(accounts))
	for _, account := range accounts {
		reference := byAccount[account]
		sort.Strings(reference.paths)
		references = append(references, *reference)
	}
	return references, nil
}

func darwinRecoveryLocatorPrefix(path string) string {
	digest := sha256.Sum256([]byte(filepath.Base(path)))
	return darwinRecoveryPrefix + hex.EncodeToString(digest[:]) + "-"
}

func darwinRecoveryLocatorPath(path, account string) string {
	return filepath.Join(
		filepath.Dir(path),
		darwinRecoveryLocatorPrefix(path)+account+".json",
	)
}

func randomDarwinAccount(reader io.Reader) (string, error) {
	var account [darwinAccountBytes]byte
	if _, err := io.ReadFull(reader, account[:]); err != nil {
		return "", fmt.Errorf("generate macOS Keychain item identity: %w", err)
	}
	return hex.EncodeToString(account[:]), nil
}

func prepareDarwinLocatorDestination(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return errors.New("private material locator path must be canonical and absolute")
	}
	parent := filepath.Dir(path)
	if err := requirePrivateDirectory(parent); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return errors.New("private material already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	references, err := collectDarwinCredentialReferences(path)
	if err != nil {
		return err
	}
	if len(references) > 0 {
		return errDarwinCredentialRecovery
	}
	return nil
}

func readDarwinCredentialLocator(path string) (darwinCredentialLocator, error) {
	if err := requirePrivateRegularFile(path); err != nil {
		return darwinCredentialLocator{}, err
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return darwinCredentialLocator{}, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return darwinCredentialLocator{}, errors.New("open macOS private material locator")
	}
	defer file.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return darwinCredentialLocator{}, err
	}
	if err := requireDarwinPrivateFileStat(stat); err != nil {
		return darwinCredentialLocator{}, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, darwinLocatorMaxBytes+1))
	if err != nil {
		return darwinCredentialLocator{}, err
	}
	if len(raw) == 0 || len(raw) > darwinLocatorMaxBytes {
		return darwinCredentialLocator{}, errors.New("invalid macOS private material locator size")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var locator darwinCredentialLocator
	if err := decoder.Decode(&locator); err != nil {
		return darwinCredentialLocator{}, errors.New("invalid macOS private material locator")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return darwinCredentialLocator{}, errors.New("invalid macOS private material locator")
	}
	if err := locator.validate(); err != nil {
		return darwinCredentialLocator{}, err
	}
	canonical, err := json.Marshal(locator)
	if err != nil || !bytes.Equal(raw, canonical) {
		return darwinCredentialLocator{}, errors.New("non-canonical macOS private material locator")
	}
	return locator, nil
}

func (locator darwinCredentialLocator) validate() error {
	if locator.Version != darwinLocatorVersion ||
		locator.Backend != darwinLocatorBackend ||
		locator.Service != darwinKeychainService ||
		len(locator.Account) != darwinAccountBytes*2 {
		return errors.New("invalid macOS private material locator")
	}
	decoded, err := hex.DecodeString(locator.Account)
	if err != nil || hex.EncodeToString(decoded) != locator.Account {
		return errors.New("invalid macOS private material locator")
	}
	return nil
}

func requirePrivateDirectory(path string) error {
	if err := requireDarwinPrivateAncestors(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.New("private material parent is unsafe")
	}
	return requireDarwinOwner(info)
}

func requirePrivateRegularFile(path string) error {
	if err := requireDarwinPrivateAncestors(filepath.Dir(path)); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
		return errors.New("private material locator is unsafe")
	}
	if err := requireDarwinOwner(info); err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return errors.New("private material locator has unsafe links")
	}
	return nil
}

func requireDarwinPrivateAncestors(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("private material ancestor path is unsafe")
	}
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.New("private material ancestor metadata is unavailable")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			// macOS exposes /var and /tmp as root-owned compatibility
			// symlinks. Accept only those root-controlled links and validate
			// the fully-resolved target chain with the stricter no-link walk.
			if stat.Uid != 0 {
				return errors.New("private material ancestor symlink is untrusted")
			}
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return err
			}
			if err := requireDarwinResolvedAncestors(resolved); err != nil {
				return err
			}
		} else if !info.IsDir() {
			return errors.New("private material ancestor is unsafe")
		}
		if stat.Uid != 0 && stat.Uid != uint32(os.Geteuid()) {
			return errors.New("private material ancestor has an untrusted owner")
		}
		if info.Mode().Perm()&0o022 != 0 &&
			!(stat.Uid == 0 && info.Mode()&os.ModeSticky != 0) {
			return errors.New("private material ancestor is writable by another user")
		}
		if current == "/" {
			return nil
		}
	}
}

func requireDarwinResolvedAncestors(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("resolved private material ancestor is unsafe")
	}
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("resolved private material ancestor is unsafe")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || (stat.Uid != 0 && stat.Uid != uint32(os.Geteuid())) {
			return errors.New("resolved private material ancestor has an untrusted owner")
		}
		if info.Mode().Perm()&0o022 != 0 &&
			!(stat.Uid == 0 && info.Mode()&os.ModeSticky != 0) {
			return errors.New("resolved private material ancestor is writable by another user")
		}
		if current == "/" {
			return nil
		}
	}
}

func requireDarwinOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("private material is not owned by service user")
	}
	return nil
}

func requireDarwinPrivateFileStat(stat unix.Stat_t) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFREG ||
		stat.Mode&0o777 != 0o600 ||
		stat.Uid != uint32(os.Geteuid()) ||
		stat.Nlink != 1 {
		return errors.New("private material locator changed while opening")
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
