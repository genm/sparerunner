package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/genm/sparerunner/internal/app"
)

func TestRunVersionPrintsBuildInformation(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if err := run([]string{"version"}, &stdout, &stderr); err != nil {
		t.Fatalf("run(version) returned error: %v", err)
	}
	// `version` reports the product build, which every binary shares, so it stays
	// "sparerunner" even though the command itself is `sprun`.
	if !strings.Contains(stdout.String(), "sparerunner ") {
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

func TestPackagedMacOSJoinPrintsLaunchdInstructionWithoutSecondServe(t *testing.T) {
	var stdout, stderr bytes.Buffer
	const stateDirectory = "/Library/Application Support/SpareRunner/agent"
	command := newJoinCommandForPlatform(
		"darwin",
		func(_ context.Context, options app.JoinOptions) (string, error) {
			if options.StateDirectory != stateDirectory {
				t.Fatalf("join state directory = %q, want %q", options.StateDirectory, stateDirectory)
			}
			return "node-macos", nil
		},
	)
	command.SetOut(&stdout)
	command.SetErr(&stderr)
	command.SetArgs([]string{
		"spr_test-code",
		"--state-dir",
		stateDirectory,
	})
	if err := command.Execute(); err != nil {
		t.Fatalf("packaged macOS join returned error: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "Node node-macos joined successfully") ||
		!strings.Contains(
			output,
			"sudo /bin/launchctl kickstart -k system/com.genm.sparerunner.agent",
		) {
		t.Fatalf("packaged macOS join output = %q", output)
	}
	if strings.Contains(output, "sparerunner-agent serve") {
		t.Fatalf("packaged macOS join suggested a second Agent process: %q", output)
	}
	if stderr.Len() != 0 {
		t.Fatalf("packaged macOS join stderr = %q, want empty", stderr.String())
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
