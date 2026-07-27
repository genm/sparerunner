package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunVersionPrintsBuildInformation(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if err := run([]string{"version"}, &stdout, &stderr); err != nil {
		t.Fatalf("run(version) returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "tewake ") {
		t.Fatalf("version output = %q, want build information", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("version stderr = %q, want empty", stderr.String())
	}
}

func TestJoinRejectsInvalidCodeBeforeCreatingState(t *testing.T) {
	var stdout, stderr bytes.Buffer
	stateDirectory := filepath.Join(t.TempDir(), "agent")
	err := run([]string{"join", "not-a-join-code", "--state-dir", stateDirectory}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "invalid join code") {
		t.Fatalf("invalid join error = %v", err)
	}
	if _, err := os.Lstat(stateDirectory); !os.IsNotExist(err) {
		t.Fatalf("invalid join created state: %v", err)
	}
}

func TestRunServeFailsClosedWhenControllerIsNotInitialized(t *testing.T) {
	var stdout, stderr bytes.Buffer

	err := run([]string{"serve", "--state-dir", t.TempDir(), "--mdns=false"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run(serve) returned nil error for uninitialized state")
	}
	if !strings.Contains(err.Error(), "not initialized") {
		t.Fatalf("run(serve) error = %q, want initialization failure", err)
	}
}

func TestEvidenceValidateRejectsMissingManifest(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"evidence", "validate", "--file", filepath.Join(t.TempDir(), "missing.json"),
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("evidence validate accepted a missing manifest")
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("evidence validate output = stdout %q stderr %q, want no secret-bearing output", stdout.String(), stderr.String())
	}
}
