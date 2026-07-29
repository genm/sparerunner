//go:build linux

package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenTrustedInjectorRejectsWritableAndSymlinkAncestors(t *testing.T) {
	uid := uint32(os.Geteuid())
	root := secureLinuxTestRoot(t)
	source := filepath.Join(root, "injector")
	if err := os.WriteFile(source, []byte("#!/bin/sh\nexit 0\n"), 0o500); err != nil {
		t.Fatal(err)
	}
	if handle, _, err := openTrustedInjector(source, uid); err != nil {
		t.Fatalf("openTrustedInjector() error = %v", err)
	} else {
		_ = handle.Close()
	}
	if err := os.Chmod(root, 0o700|0o020); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openTrustedInjector(source, uid); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("writable parent error = %v, want errEvidenceInvalid", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	// The link sits beside the trusted root, which is outside what
	// secureLinuxTestRoot removes, so it is removed here instead. TestMain
	// creates one TMPDIR for the whole process, so without this the link
	// survives into a repeated run of this test and the run fails on an
	// existing name rather than on the boundary it is meant to prove.
	link := filepath.Join(filepath.Dir(root), "injector-link")
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(link) })
	if _, _, err := openTrustedInjector(link, uid); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("symlink error = %v, want errEvidenceInvalid", err)
	}
}

func TestVerifyInjectorFileRejectsCopyMutationAndRenameSwap(t *testing.T) {
	uid := uint32(os.Geteuid())
	root := secureLinuxTestRoot(t)
	path := filepath.Join(root, "injector")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o500); err != nil {
		t.Fatal(err)
	}
	handle, info, err := openTrustedInjector(path, uid)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := digestOpenFile(handle)
	_ = handle.Close()
	if err != nil {
		t.Fatal(err)
	}
	expected, err := injectorMetadata(path, info, digest, uid)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 1\n"), 0o500); err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifyInjectorFile(expected, uid); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("mutated copy error = %v, want errEvidenceInvalid", err)
	}

	originalInfo := info
	replacement := filepath.Join(root, "replacement")
	if err := os.WriteFile(replacement, []byte("#!/bin/sh\nexit 0\n"), 0o500); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if injectorSourceUnchanged(path, originalInfo) {
		t.Fatal("source rename swap was accepted")
	}
	if _, _, err := verifyInjectorFile(expected, uid); !errors.Is(err, errEvidenceInvalid) {
		t.Fatalf("rename swap error = %v, want errEvidenceInvalid", err)
	}
}

func secureLinuxTestRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join(os.Getenv("TMPDIR"), t.Name())
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}
