package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestStartRequestRedactsJITAcrossFormattingJSONAndStructuredLogs(t *testing.T) {
	const (
		jitCanary       = "jit-canary.example.test"
		directoryCanary = "workspace-canary.example.test"
		identityCanary  = "identity-canary.example.test"
	)
	request := StartRequest{
		Executable:   "/runtime/run",
		Directory:    "/runtime/" + directoryCanary,
		Arguments:    []string{"--ephemeral"},
		WorkspaceRef: WorkspaceRef{Backend: "test-v1", OwnerID: identityCanary},
		Containment:  ContainmentRef{Backend: "test", OwnerID: "owner"},
		jit:          jitArgument{value: jitCanary},
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	slog.New(slog.NewJSONHandler(&log, nil)).Info("request", "value", request)
	rendered := fmt.Sprintf("%v\n%+v\n%#v\n%s\n%s", request, request, request, encoded, log.String())
	for _, canary := range []string{jitCanary, directoryCanary, identityCanary} {
		if strings.Contains(rendered, canary) {
			t.Fatalf("StartRequest leaked runtime material %q: %s", canary, rendered)
		}
	}
}
