package transport

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/runner"
)

func targetTestMetadata(state domain.ExecutionState) CommandMetadata {
	return CommandMetadata{
		CommandID:       "command-1",
		ControllerEpoch: 1,
		ExecutionID:     "execution-1",
		ExpectedState:   state,
		Target: CommandTarget{
			TargetID:  "target-1",
			Scope:     "owner/repo",
			ScopeKind: domain.TargetRepository,
		},
	}
}

func TestPrepareAndStartCarryTargetIdentityThroughRoundTrip(t *testing.T) {
	prepareMetadata := targetTestMetadata(domain.ExecutionReserved)
	preparePayload, err := EncodePrepareCommandPayload(prepareMetadata, runner.OfficialRunnerVersion, false)
	if err != nil {
		t.Fatal(err)
	}
	prepare, err := DecodePrepareCommand(preparePayload)
	if err != nil {
		t.Fatal(err)
	}
	if prepare.Target() != prepareMetadata.Target {
		t.Fatalf("prepare target = %+v", prepare.Target())
	}

	startMetadata := targetTestMetadata(domain.ExecutionPreparing)
	startMetadata.Target.ScopeKind = domain.TargetOrganization
	startMetadata.Target.Scope = "owner"
	startPayload, err := EncodeStartCommandPayload(
		startMetadata, runner.OfficialRunnerVersion, false, "target-identity-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	start, err := DecodeStartCommand(startPayload)
	if err != nil {
		t.Fatal(err)
	}
	if start.Target() != startMetadata.Target {
		t.Fatalf("start target = %+v", start.Target())
	}
	// Target identity is display and enforcement data, never secret material.
	if strings.Contains(string(preparePayload), "jit") {
		t.Fatal("prepare payload carried JIT material")
	}
}

func TestCommandDecodeRejectsMissingOrMalformedTargetIdentity(t *testing.T) {
	valid := targetTestMetadata(domain.ExecutionPreparing)
	payload, err := EncodeStartCommandPayload(
		valid, runner.OfficialRunnerVersion, false, "reject-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatal(err)
	}
	// targetId is the enforcement key. Without it the agent cannot check its
	// own exclusion set, so the command must never be admitted.
	mutations := map[string]func(map[string]any){
		"missing targetId":  func(w map[string]any) { delete(w, "targetId") },
		"empty targetId":    func(w map[string]any) { w["targetId"] = "" },
		"blank targetId":    func(w map[string]any) { w["targetId"] = "   " },
		"missing scope":     func(w map[string]any) { delete(w, "scope") },
		"empty scope":       func(w map[string]any) { w["scope"] = "" },
		"missing scopeKind": func(w map[string]any) { delete(w, "scopeKind") },
		"unknown scopeKind": func(w map[string]any) { w["scopeKind"] = "enterprise" },
		"control character in targetId": func(w map[string]any) {
			w["targetId"] = "target\x01one"
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			mutated := make(map[string]any, len(wire))
			for key, value := range wire {
				mutated[key] = value
			}
			mutate(mutated)
			encoded, err := json.Marshal(mutated)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeStartCommand(encoded); !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("start decode error = %v", err)
			}
		})
	}
}

func TestPrepareEncodeRejectsMissingTargetIdentity(t *testing.T) {
	metadata := targetTestMetadata(domain.ExecutionReserved)
	metadata.Target.TargetID = ""
	if _, err := EncodePrepareCommandPayload(
		metadata, runner.OfficialRunnerVersion, false); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("prepare encode error = %v", err)
	}
}

// Target identity is part of the authenticated payload, so it necessarily
// changes the replay digest. This is an accepted pre-1.0 hard wire change with
// no compatibility shim: a command's identity now includes which Target it is
// for, and two otherwise identical commands for different Targets must not
// share a replay identity.
func TestTargetIdentityParticipatesInTheReplayDigest(t *testing.T) {
	first := targetTestMetadata(domain.ExecutionReserved)
	second := first
	second.Target.TargetID = "target-2"

	firstPayload, err := EncodePrepareCommandPayload(first, runner.OfficialRunnerVersion, false)
	if err != nil {
		t.Fatal(err)
	}
	secondPayload, err := EncodePrepareCommandPayload(second, runner.OfficialRunnerVersion, false)
	if err != nil {
		t.Fatal(err)
	}
	firstIdentity := first.replayIdentity(MessagePrepare, firstPayload)
	secondIdentity := second.replayIdentity(MessagePrepare, secondPayload)
	if firstIdentity.PayloadDigest == secondIdentity.PayloadDigest {
		t.Fatal("commands for different targets shared a replay digest")
	}

	// The pre-target wire shape is now simply invalid input rather than an
	// older-but-accepted encoding.
	legacy := `{"commandId":"command-1","controllerEpoch":1,"executionId":"execution-1",` +
		`"expectedState":"reserved","runnerVersion":"` + runner.OfficialRunnerVersion +
		`","disableUpdate":false}`
	if _, err := DecodePrepareCommand([]byte(legacy)); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("pre-target prepare wire decode error = %v", err)
	}
}

func TestCancelCommandCarriesNoTargetIdentity(t *testing.T) {
	metadata := targetTestMetadata(domain.ExecutionRunning)
	payload, err := EncodeCancelCommandPayload(metadata)
	if err != nil {
		t.Fatal(err)
	}
	// Teardown of work this node already owns is never target-gated, so the
	// exclusion set has no say over it and the field is absent from the wire.
	if strings.Contains(string(payload), "targetId") {
		t.Fatalf("cancel payload carried target identity: %s", payload)
	}
	command, err := DecodeCancelCommand(payload)
	if err != nil {
		t.Fatal(err)
	}
	if command.Metadata().Target != (CommandTarget{}) {
		t.Fatalf("cancel target = %+v", command.Metadata().Target)
	}
}
