//go:build linux

package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	root, err := os.MkdirTemp(".", ".tewake-live-test-")
	if err != nil {
		panic(err)
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		panic(err)
	}
	previous := os.Getenv("TMPDIR")
	if err := os.Setenv("TMPDIR", absolute); err != nil {
		panic(err)
	}
	status := m.Run()
	_ = os.Setenv("TMPDIR", previous)
	_ = os.RemoveAll(absolute)
	os.Exit(status)
}

func TestTrustedPathRejectsWritableAndSymlinkParent(t *testing.T) {
	base := t.TempDir()
	writable := filepath.Join(base, "writable")
	if err := os.Mkdir(writable, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(writable, 0o777); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(writable, "leaf")
	if err := os.WriteFile(leaf, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateTrustedPathChain(leaf, false); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("writable parent error = %v, want errEvidenceInvalid", err)
	}

	target := filepath.Join(base, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "value"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateTrustedPathChain(filepath.Join(link, "value"), false); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("symlink parent error = %v, want errEvidenceInvalid", err)
	}
}

func TestLiveDriverPinsRepositoryProofToGitHubDotCom(t *testing.T) {
	driver, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range [][]byte{
		[]byte(`gh api --hostname github.com "repos/$repository"`),
		[]byte(`.full_name`),
		[]byte(`.html_url`),
	} {
		if !bytes.Contains(driver, required) {
			t.Fatalf("live driver omitted github.com proof binding %q", required)
		}
	}
	if bytes.Contains(driver, []byte(`gh repo view "$repository"`)) {
		t.Fatal("live driver uses GH_HOST-sensitive repository proof")
	}
}
