package main

import (
	"bytes"
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

func TestRunServeFailsClosedUntilImplemented(t *testing.T) {
	var stdout, stderr bytes.Buffer

	err := run([]string{"serve"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run(serve) returned nil error, want explicit unimplemented error")
	}
	if !strings.Contains(err.Error(), "serve is not implemented") {
		t.Fatalf("run(serve) error = %q, want explicit serve implementation status", err)
	}
}
