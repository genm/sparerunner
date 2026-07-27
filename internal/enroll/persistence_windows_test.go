//go:build windows

package enroll

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/genm/tewake/internal/winacl"
	syswindows "golang.org/x/sys/windows"
)

func TestWindowsDPAPIRoundTripNoClobberAndIdempotentRemoval(t *testing.T) {
	directory := windowsPrivateDirectory(t)
	path := filepath.Join(directory, "node-private-key.locator")
	plaintext := []byte("windows-private-canary.example.test")
	wantPlaintext := append([]byte(nil), plaintext...)
	if err := SavePrivateMaterial(path, plaintext); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plaintext, wantPlaintext) {
		t.Fatal("successful persistence mutated the caller's plaintext")
	}
	ciphertext, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(ciphertext, []byte(windowsDPAPIMagic)) {
		t.Fatal("DPAPI envelope magic is missing")
	}
	if bytes.Contains(ciphertext, plaintext) {
		t.Fatal("plaintext canary was written to the locator")
	}
	loaded, err := LoadPrivateMaterial(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(loaded, plaintext) {
		t.Fatal("DPAPI round trip changed private material")
	}
	clear(loaded)
	replacement := []byte("replacement.example.test")
	wantReplacement := append([]byte(nil), replacement...)
	if err := SavePrivateMaterial(path, replacement); err == nil {
		t.Fatal("private material locator was clobbered")
	}
	if !bytes.Equal(replacement, wantReplacement) {
		t.Fatal("no-clobber persistence mutated the caller's plaintext")
	}
	reloaded, err := LoadPrivateMaterial(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reloaded, plaintext) {
		t.Fatal("no-clobber failure changed existing private material")
	}
	clear(reloaded)
	matches, err := filepath.Glob(filepath.Join(directory, ".tewake-private-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("private staging residue = %v", matches)
	}
	if err := RemovePrivateMaterial(path); err != nil {
		t.Fatal(err)
	}
	if err := RemovePrivateMaterial(path); err != nil {
		t.Fatalf("idempotent removal: %v", err)
	}
	if err := os.Remove(directory); err != nil {
		t.Fatal(err)
	}
	if err := RemovePrivateMaterial(path); err != nil {
		t.Fatalf("idempotent removal after parent removal: %v", err)
	}
}

func TestWindowsDPAPIRejectsTamperedCiphertext(t *testing.T) {
	directory := windowsPrivateDirectory(t)
	path := filepath.Join(directory, "tampered.locator")
	if err := SavePrivateMaterial(path, []byte("tamper-canary.example.test")); err != nil {
		t.Fatal(err)
	}
	ciphertext, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext[len(ciphertext)-1] ^= 0xff
	if err := os.WriteFile(path, ciphertext, 0o600); err != nil {
		t.Fatal(err)
	}
	clear(ciphertext)
	if _, err := LoadPrivateMaterial(path); err == nil {
		t.Fatal("tampered DPAPI ciphertext was accepted")
	}
}

func TestWindowsPrivateMaterialDoesNotClaimExistingUnsafeParent(t *testing.T) {
	directory := filepath.Clean(filepath.Join(t.TempDir(), "foreign-empty"))
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "must-not-exist.locator")
	if err := SavePrivateMaterial(
		path,
		[]byte("foreign-parent.example.test"),
	); err == nil {
		t.Fatal("existing inherited-ACL parent was claimed")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed save mutated foreign parent: %v", err)
	}
}

func TestWindowsDPAPILocatorSurvivesParentDirectoryRename(t *testing.T) {
	root := filepath.Clean(t.TempDir())
	staging := filepath.Join(root, "staging")
	if err := winacl.CreatePrivateDirectory(staging); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(staging, "movable.locator")
	plaintext := []byte("path-independent-dpapi.example.test")
	if err := SavePrivateMaterial(path, plaintext); err != nil {
		t.Fatal(err)
	}
	published := filepath.Join(root, "published")
	if err := os.Rename(staging, published); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadPrivateMaterial(filepath.Join(published, "movable.locator"))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(loaded)
	if !bytes.Equal(loaded, plaintext) {
		t.Fatal("parent rename changed DPAPI private material")
	}
}

func TestWindowsPrivateMaterialRejectsExpandedACL(t *testing.T) {
	directory := windowsPrivateDirectory(t)
	path := filepath.Join(directory, "expanded-acl.locator")
	if err := SavePrivateMaterial(path, []byte("acl-canary.example.test")); err != nil {
		t.Fatal(err)
	}
	if err := addEveryoneReadACL(path); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPrivateMaterial(path); err == nil {
		t.Fatal("private material with an expanded ACL was accepted")
	}
}

func TestWindowsLockedLocatorRemovalFailsAndPreservesLocator(t *testing.T) {
	directory := windowsPrivateDirectory(t)
	path := filepath.Join(directory, "locked.locator")
	if err := SavePrivateMaterial(path, []byte("locked-canary.example.test")); err != nil {
		t.Fatal(err)
	}
	pointer, err := syswindows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := syswindows.CreateFile(
		pointer,
		syswindows.GENERIC_READ,
		syswindows.FILE_SHARE_READ,
		nil,
		syswindows.OPEN_EXISTING,
		syswindows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := RemovePrivateMaterial(path); err == nil {
		syswindows.CloseHandle(handle)
		t.Fatal("locked private material reported removal success")
	}
	if _, err := os.Lstat(path); err != nil {
		syswindows.CloseHandle(handle)
		t.Fatalf("locked locator was not preserved: %v", err)
	}
	if err := syswindows.CloseHandle(handle); err != nil {
		t.Fatal(err)
	}
	if err := RemovePrivateMaterial(path); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsPrivateMaterialRejectsReparseLocator(t *testing.T) {
	directory := windowsPrivateDirectory(t)
	target := filepath.Join(directory, "target.locator")
	if err := SavePrivateMaterial(target, []byte("reparse-canary.example.test")); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link.locator")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("Windows symlink privilege unavailable: %v", err)
	}
	if _, err := LoadPrivateMaterial(link); err == nil {
		t.Fatal("reparse locator was accepted")
	}
}

func windowsPrivateDirectory(t *testing.T) string {
	t.Helper()
	_ = os.Setenv("TEWAKE_WINDOWS_DEBUG", "1")
	directory := filepath.Join(filepath.Clean(t.TempDir()), "private")
	if err := winacl.CreatePrivateDirectory(directory); err != nil {
		currentSID, _ := winacl.CurrentProcessSID()
		aclOutput, _ := exec.Command("icacls.exe", directory).CombinedOutput()
		t.Fatalf(
			"secure Windows private directory %q: %v (noReparse=%t currentSID=%q existingACL=%v icacls=%q)",
			directory,
			err,
			winacl.NoReparseComponents(directory),
			currentSID,
			winacl.ValidatePrivateDirectory(directory),
			aclOutput,
		)
	}
	return directory
}

func addEveryoneReadACL(path string) error {
	current, err := winacl.CurrentProcessSID()
	if err != nil {
		return err
	}
	sids := []string{current}
	for _, sid := range []string{"S-1-5-18", "S-1-5-32-544"} {
		found := false
		for _, existing := range sids {
			if strings.EqualFold(existing, sid) {
				found = true
				break
			}
		}
		if !found {
			sids = append(sids, sid)
		}
	}
	var acl strings.Builder
	fmt.Fprintf(&acl, "O:%sD:P", current)
	for _, sid := range sids {
		fmt.Fprintf(&acl, "(A;;FA;;;%s)", sid)
	}
	acl.WriteString("(A;;FR;;;WD)")
	descriptor, err := syswindows.SecurityDescriptorFromString(acl.String())
	if err != nil {
		return err
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return err
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return err
	}
	return syswindows.SetNamedSecurityInfo(
		path,
		syswindows.SE_FILE_OBJECT,
		syswindows.OWNER_SECURITY_INFORMATION|
			syswindows.DACL_SECURITY_INFORMATION|
			syswindows.PROTECTED_DACL_SECURITY_INFORMATION,
		owner,
		nil,
		dacl,
		nil,
	)
}
