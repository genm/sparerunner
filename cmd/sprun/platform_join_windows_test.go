//go:build windows

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestWindowsJoinReportsServiceCompletionWithoutWrongUserStateCommand(t *testing.T) {
	var output bytes.Buffer
	printPlatformJoinNextStep(&output, "windows", `C:\Users\example\AppData\wrong-state`)
	if !strings.Contains(output.String(), "service is enrolled and running") {
		t.Fatalf("join output = %q", output.String())
	}
	if strings.Contains(output.String(), "sparerunner-agent serve") ||
		strings.Contains(output.String(), "wrong-state") {
		t.Fatalf("join output exposed an incorrect manual state path: %q", output.String())
	}
}
