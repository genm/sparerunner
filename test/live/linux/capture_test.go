//go:build !windows

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCollectProcessEvidenceUsesOnlyAllowlistedRoles(t *testing.T) {
	procRoot := t.TempDir()
	writeFakeProcess(t, procRoot, 101, 1001, []string{"/usr/local/bin/tewake-agent", "serve", "--state-dir=/secret/path"})
	writeFakeProcess(t, procRoot, 202, 0, []string{"/usr/local/bin/tewake-agent", "supervisor", "--socket=/run/private.sock"})
	writeFakeProcess(t, procRoot, 303, 1001, []string{"/usr/bin/unrelated", "private-canary"})

	evidence, err := collectProcessEvidence(procRoot, "before")
	if err != nil {
		t.Fatalf("collectProcessEvidence() error = %v", err)
	}
	if evidence.Status != "passed" || len(evidence.Processes) != 2 {
		t.Fatalf("process evidence = %#v", evidence)
	}
	if evidence.Processes[0] != (observedProcess{PID: 101, UID: 1001, Role: "agent"}) ||
		evidence.Processes[1] != (observedProcess{PID: 202, UID: 0, Role: "supervisor"}) {
		t.Fatalf("processes = %#v", evidence.Processes)
	}
}

func TestCollectProcessEvidenceRejectsIdleRunnerListener(t *testing.T) {
	procRoot := t.TempDir()
	writeFakeProcess(t, procRoot, 101, 1001, []string{"/usr/local/bin/tewake-agent", "serve"})
	writeFakeProcess(t, procRoot, 202, 0, []string{"/usr/local/bin/tewake-agent", "supervisor"})
	writeFakeProcess(t, procRoot, 303, 1002, []string{"/var/lib/runner/Runner.Listener", "run"})
	if _, err := collectProcessEvidence(procRoot, "after"); !errors.Is(err, errNodeEvidenceInvalid) {
		t.Fatalf("collectProcessEvidence() error = %v, want errNodeEvidenceInvalid", err)
	}
}

func TestCollectProcessEvidenceRequiresExactlyOneRunningListener(t *testing.T) {
	procRoot := t.TempDir()
	writeFakeProcess(t, procRoot, 101, 1001, []string{"/usr/local/bin/tewake-agent", "serve"})
	writeFakeProcess(t, procRoot, 202, 0, []string{"/usr/local/bin/tewake-agent", "supervisor"})
	writeFakeProcess(t, procRoot, 303, 1002, []string{"/var/lib/runner/Runner.Listener", "run"})
	evidence, err := collectProcessEvidence(procRoot, "running-before-restart")
	if err != nil {
		t.Fatalf("collectProcessEvidence() error = %v", err)
	}
	if evidence.Status != "passed" || len(evidence.Processes) != 3 {
		t.Fatalf("process evidence = %#v", evidence)
	}

	if err := os.RemoveAll(filepath.Join(procRoot, "303")); err != nil {
		t.Fatal(err)
	}
	if _, err := collectProcessEvidence(
		procRoot,
		"running-after-restart",
	); !errors.Is(err, errNodeEvidenceInvalid) {
		t.Fatalf("missing listener error = %v, want errNodeEvidenceInvalid", err)
	}
}

func TestCollectFilesystemEvidenceRequiresEmptyExecutionRoot(t *testing.T) {
	runtimeRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(runtimeRoot, "executions"), 0o700); err != nil {
		t.Fatal(err)
	}
	evidence, err := collectFilesystemEvidence(runtimeRoot)
	if err != nil {
		t.Fatalf("collectFilesystemEvidence() error = %v", err)
	}
	if evidence.Status != "passed" || evidence.ExecutionEntries != 0 {
		t.Fatalf("filesystem evidence = %#v", evidence)
	}

	execution := filepath.Join(runtimeRoot, "executions", "twk-exec")
	if err := os.Mkdir(execution, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(execution, ".credentials"), []byte("jit-secret-canary"), 0o600); err != nil {
		t.Fatal(err)
	}
	evidence, err = collectFilesystemEvidence(runtimeRoot)
	if !errors.Is(err, errNodeEvidenceInvalid) ||
		evidence.ExecutionEntries != 1 ||
		evidence.CredentialFiles != 1 {
		t.Fatalf("unsafe filesystem evidence = %#v, error=%v", evidence, err)
	}
}

func TestBindProcessEvidenceRejectsFakeRunnerExecutableWithExpectedUIDAndCgroup(t *testing.T) {
	config := validTestConfig(t)
	if err := os.MkdirAll(config.EvidenceDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	evidence, err := openEvidenceStore(config.EvidenceDirectory)
	if err != nil {
		t.Fatal(err)
	}
	authority := validAuthorityEvidence(config, time.Now().Add(-time.Minute))
	if err := evidence.writeJSON(authorityFileName, authority); err != nil {
		t.Fatal(err)
	}
	executionID := "execution"
	if err := evidence.writeJSON(restartStartedName, restartStartedEvidence{
		Version: evidenceVersion, ScaleSetID: 41, RunnerRequestID: 91,
		ExecutionID: executionID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	probe := validAuthorityProbe()
	for _, unit := range []string{"tewake-agent.service", "tewake-supervisor.service"} {
		key := unit + "\x00ExecStart"
		probe.properties[key] = strings.Replace(
			probe.properties[key],
			"/var/lib/tewake-runtime",
			config.RuntimeRoot,
			1,
		)
	}
	digest := sha256.Sum256([]byte(executionID))
	runnerCgroup := filepath.Join(
		authority.Supervisor.ControlGroup,
		"tewake",
		"tewake-"+hex.EncodeToString(digest[:]),
	)
	probe.files["/proc/303/stat"] = fakeStat(303, 333)
	probe.files["/proc/303/cgroup"] = []byte("0::" + runnerCgroup + "\n")
	probe.executables[303] = [2]string{
		filepath.Join(
			config.RuntimeRoot,
			"executions",
			hex.EncodeToString(digest[:]),
			"bin",
			"Runner.Listener",
		),
		strings.Repeat("b", 64),
	}
	processes := processEvidence{Processes: []observedProcess{
		{PID: 101, UID: 1001, Role: "agent"},
		{PID: 202, UID: 0, Role: "supervisor"},
		{PID: 303, UID: 1002, Role: "runner_listener"},
	}}
	if err := bindProcessEvidence(
		&processes,
		"running-before-restart",
		config,
		probe,
	); err != nil {
		t.Fatalf("bindProcessEvidence() error = %v", err)
	}

	probe.executables[303] = [2]string{
		"/var/cache/tewake-agent/actions-runner/bin/Runner.Listener",
		strings.Repeat("c", 64),
	}
	if err := bindProcessEvidence(
		&processes,
		"running-before-restart",
		config,
		probe,
	); !errors.Is(err, errNodeEvidenceInvalid) {
		t.Fatalf("fake runner executable error = %v, want errNodeEvidenceInvalid", err)
	}
}

func writeFakeProcess(t *testing.T, procRoot string, pid, uid int, arguments []string) {
	t.Helper()
	directory := filepath.Join(procRoot, strconv.Itoa(pid))
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	var cmdline []byte
	for _, argument := range arguments {
		cmdline = append(cmdline, argument...)
		cmdline = append(cmdline, 0)
	}
	if err := os.WriteFile(filepath.Join(directory, "cmdline"), cmdline, 0o600); err != nil {
		t.Fatal(err)
	}
	status := []byte("Name:\ttest\nUid:\t" + strconv.Itoa(uid) + "\t" + strconv.Itoa(uid) + "\t" + strconv.Itoa(uid) + "\t" + strconv.Itoa(uid) + "\n")
	if err := os.WriteFile(filepath.Join(directory, "status"), status, 0o600); err != nil {
		t.Fatal(err)
	}
}
