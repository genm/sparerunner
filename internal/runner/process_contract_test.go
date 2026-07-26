package runner

import (
	"bytes"
	"encoding/json"
	"errors"
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
		jit:          newJITArgument(jitCanary),
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

func TestJITDeliveryIsOneShotEvenWhenConsumerFails(t *testing.T) {
	request := StartRequest{jit: newJITArgument("one-job-jit.example.test")}
	calls := 0
	if err := request.DeliverJIT(func(value string) error {
		calls++
		if value != "one-job-jit.example.test" {
			t.Fatalf("delivered JIT = %q", value)
		}
		return errors.New("injected consumer failure")
	}); !errors.Is(err, ErrStartFailed) {
		t.Fatalf("first DeliverJIT error = %v", err)
	}
	if err := request.DeliverJIT(func(string) error {
		calls++
		return nil
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("second DeliverJIT error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("delivery calls = %d", calls)
	}
}
