package main

import (
	"bytes"
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

func TestRunWithoutArgumentsFailsClosedUntilImplemented(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if exitCode := run(nil, &stdout, &stderr); exitCode == 0 {
		t.Fatal("run() exit code = 0, want nonzero while runtime is unimplemented")
	}
	if !strings.Contains(stderr.String(), "not implemented") {
		t.Fatalf("default stderr = %q, want explicit unimplemented status", stderr.String())
	}
}
