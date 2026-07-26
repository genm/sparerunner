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
	const canary = "jit-canary.example.test"
	request := StartRequest{
		Executable:  "/runtime/run",
		Directory:   "/runtime",
		Arguments:   []string{"--ephemeral"},
		Containment: ContainmentRef{Backend: "test", OwnerID: "owner"},
		jit:         jitArgument{value: canary},
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	slog.New(slog.NewJSONHandler(&log, nil)).Info("request", "value", request)
	rendered := fmt.Sprintf("%v\n%+v\n%#v\n%s\n%s", request, request, request, encoded, log.String())
	if strings.Contains(rendered, canary) {
		t.Fatalf("StartRequest leaked JIT material: %s", rendered)
	}
}
