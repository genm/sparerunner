package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunVersionPrintsBuildInformation(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if exitCode := run([]string{"--version"}, &stdout, &stderr); exitCode != 0 {
		t.Fatalf("run(--version) exit code = %d, want 0", exitCode)
	}
	if !strings.Contains(stdout.String(), "tewake ") {
		t.Fatalf("version output = %q, want build information", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("version stderr = %q, want empty", stderr.String())
	}
}

func TestServeRejectsUninitializedState(t *testing.T) {
	var stdout, stderr bytes.Buffer
	stateDirectory := filepath.Join(t.TempDir(), "agent")
	if exitCode := run([]string{"serve", "--state-dir", stateDirectory}, &stdout, &stderr); exitCode == 0 {
		t.Fatal("serve accepted uninitialized state")
	}
	if !strings.Contains(stderr.String(), "not initialized") {
		t.Fatalf("serve stderr = %q", stderr.String())
	}
}

func TestRunWithoutArgumentsRequiresACommand(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if exitCode := run(nil, &stdout, &stderr); exitCode == 0 {
		t.Fatal("run() exit code = 0, want nonzero without a command")
	}
	if !strings.Contains(stderr.String(), "command is required") {
		t.Fatalf("default stderr = %q, want explicit command requirement", stderr.String())
	}
}
