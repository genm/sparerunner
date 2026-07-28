package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/genm/sparerunner/internal/runner"
)

type fakeNativeRunnerLifecycle struct {
	nativeRunnerLifecycle
	readyErr   error
	readyCalls int
}

func (fake *fakeNativeRunnerLifecycle) Ready(context.Context) error {
	fake.readyCalls++
	return fake.readyErr
}

func TestRunVersionPrintsBuildInformation(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if exitCode := run([]string{"--version"}, &stdout, &stderr); exitCode != 0 {
		t.Fatalf("run(--version) exit code = %d, want 0", exitCode)
	}
	if !strings.Contains(stdout.String(), "sparerunner ") {
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

func TestNativeRunnerReadinessIncludesDurableAgentCredential(t *testing.T) {
	lifecycle := &fakeNativeRunnerLifecycle{}
	probeCalls := 0
	bound, err := bindNativeRunnerCredential(lifecycle, func(context.Context) error {
		probeCalls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := bound.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lifecycle.readyCalls != 1 || probeCalls != 1 {
		t.Fatalf("ready calls=%d credential probes=%d", lifecycle.readyCalls, probeCalls)
	}

	lifecycle.readyErr = runner.ErrStrongOwnershipUnavailable
	if err := bound.Ready(context.Background()); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("platform readiness error=%v", err)
	}
	if probeCalls != 1 {
		t.Fatalf("credential probed after platform failure: %d", probeCalls)
	}

	lifecycle.readyErr = nil
	credentialFailure := errors.New("credential store locked")
	bound, err = bindNativeRunnerCredential(lifecycle, func(context.Context) error {
		return credentialFailure
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := bound.Ready(context.Background()); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("credential readiness error=%v", err)
	}
}

func TestNativeRunnerCredentialBindingRejectsMissingAuthority(t *testing.T) {
	if _, err := bindNativeRunnerCredential(nil, func(context.Context) error { return nil }); err == nil {
		t.Fatal("nil lifecycle was accepted")
	}
	lifecycle := &fakeNativeRunnerLifecycle{}
	if _, err := bindNativeRunnerCredential(lifecycle, nil); err == nil {
		t.Fatal("nil credential probe was accepted")
	}
}
