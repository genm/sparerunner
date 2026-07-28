//go:build windows

package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/genm/sparerunner/internal/winacl"
)

func TestWindowsStateInitializationSecuresOnlyEmptyOwnedDirectory(t *testing.T) {
	directory := filepath.Clean(filepath.Join(t.TempDir(), "state"))
	if err := ensurePrivateStateDirectory(directory); err != nil {
		currentSID, _ := winacl.CurrentProcessSID()
		t.Fatalf(
			"initialize Windows state directory %q: %v (noReparse=%t currentSID=%q existingACL=%v)",
			directory,
			err,
			winacl.NoReparseComponents(directory),
			currentSID,
			winacl.ValidatePrivateDirectory(directory),
		)
	}
	if err := winacl.ValidatePrivateDirectory(directory); err != nil {
		t.Fatalf("initialized state ACL: %v", err)
	}
}

func TestWindowsStateInitializationDoesNotRepairNonEmptyUnsafeDirectory(t *testing.T) {
	directory := filepath.Clean(filepath.Join(t.TempDir(), "unsafe-state"))
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(directory, "attacker-controlled"),
		[]byte("unsafe.example.test"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := ensurePrivateStateDirectory(directory); err == nil {
		t.Fatal("non-empty inherited-ACL state directory was repaired")
	}
}

func TestWindowsStateInitializationDoesNotRepairExistingEmptyDirectory(t *testing.T) {
	directory := filepath.Clean(filepath.Join(t.TempDir(), "unsafe-empty-state"))
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ensurePrivateStateDirectory(directory); err == nil {
		t.Fatal("existing inherited-ACL state directory was repaired")
	}
}

func TestWindowsStatePublicationIsWriteThroughAndNoClobber(t *testing.T) {
	parent := filepath.Clean(t.TempDir())
	staging := filepath.Join(parent, "staging")
	destination := filepath.Join(parent, "published")
	if err := os.Mkdir(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(staging, "state"),
		[]byte("first.example.test"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := publishStateDirectory(staging, destination); err != nil {
		t.Fatal(err)
	}
	if contents, err := os.ReadFile(filepath.Join(destination, "state")); err != nil ||
		string(contents) != "first.example.test" {
		t.Fatalf("published state = %q, err=%v", contents, err)
	}
	second := filepath.Join(parent, "second")
	if err := os.Mkdir(second, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(second, "state"),
		[]byte("replacement.example.test"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := publishStateDirectory(second, destination); err == nil {
		t.Fatal("Windows state publication clobbered an existing destination")
	}
	if contents, err := os.ReadFile(filepath.Join(destination, "state")); err != nil ||
		string(contents) != "first.example.test" {
		t.Fatalf("destination changed after no-clobber failure: %q, err=%v", contents, err)
	}
}
