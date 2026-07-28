package transport

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/runner"
)

func testCommandMetadata() CommandMetadata {
	return CommandMetadata{
		CommandID:       "command-1",
		ControllerEpoch: 1,
		ExecutionID:     "execution-1",
		ExpectedState:   domain.ExecutionPreparing,
		Target:          CommandTarget{TargetID: "target-1", Scope: "owner/repo", ScopeKind: domain.TargetRepository},
	}
}

func TestPrepareCommandRoundTrip(t *testing.T) {
	metadata := testCommandMetadata()
	metadata.ExpectedState = domain.ExecutionReserved
	payload, err := EncodePrepareCommandPayload(metadata, runner.OfficialRunnerVersion, true)
	if err != nil {
		t.Fatal(err)
	}
	command, err := DecodePrepareCommand(payload)
	if err != nil {
		t.Fatal(err)
	}
	if command.Metadata() != metadata || command.RunnerVersion() != runner.OfficialRunnerVersion || !command.DisableUpdate() {
		t.Fatalf("decoded prepare command = %#v", command)
	}
	if err := command.ReplayIdentity(payload).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestStartCommandRoundTripKeepsJITOneShotAndRedacted(t *testing.T) {
	payload, err := EncodeStartCommandPayload(testCommandMetadata(), runner.OfficialRunnerVersion, true, "jit-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	command, err := DecodeStartCommand(payload)
	if err != nil {
		t.Fatal(err)
	}
	if command.RunnerVersion() != runner.OfficialRunnerVersion || !command.DisableUpdate() {
		t.Fatalf("decoded command = %#v", command)
	}
	if got := fmt.Sprintf("%v %#v", command, command); strings.Contains(got, "jit-canary") {
		t.Fatalf("formatted command leaked JIT: %s", got)
	}
	if _, err := json.Marshal(command); !errors.Is(err, ErrCommandSerialization) {
		t.Fatalf("MarshalJSON error = %v", err)
	}
	var delivered string
	if err := command.Deliver(func(value string) error {
		delivered = value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if delivered != "jit-canary.example.test" {
		t.Fatalf("delivered value = %q", delivered)
	}
	if err := command.Deliver(func(string) error { return nil }); !errors.Is(err, ErrCommandSecret) {
		t.Fatalf("second delivery error = %v", err)
	}
}

func TestStartCommandDiscardErasesUndeliveredJIT(t *testing.T) {
	payload, err := EncodeStartCommandPayload(testCommandMetadata(), runner.OfficialRunnerVersion, false, "discard-canary.example.test")
	if err != nil {
		t.Fatal(err)
	}
	command, err := DecodeStartCommand(payload)
	if err != nil {
		t.Fatal(err)
	}
	digest := command.Digest()
	command.Discard()
	command.Discard()
	if command.Digest() != digest {
		t.Fatal("discard changed the non-secret JIT digest")
	}
	called := false
	if err := command.Deliver(func(string) error {
		called = true
		return nil
	}); !errors.Is(err, ErrCommandSecret) {
		t.Fatalf("delivery after discard error = %v", err)
	}
	if called {
		t.Fatal("delivery callback ran after discard")
	}
	if got := fmt.Sprintf("%v %#v", command, command); strings.Contains(got, "discard-canary") {
		t.Fatalf("discarded command formatting leaked JIT: %s", got)
	}
}

func TestStartCommandRejectsMismatchUnknownAndDuplicateFields(t *testing.T) {
	valid, err := EncodeStartCommandPayload(testCommandMetadata(), runner.OfficialRunnerVersion, false, "opaque")
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		[]byte(`{"commandId":"command-1","controllerEpoch":1,"executionId":"execution-1","expectedState":"preparing","runnerVersion":"old","disableUpdate":false,"jitConfig":"opaque"}`),
		[]byte(`{"commandId":"command-1","controllerEpoch":1,"executionId":"execution-1","expectedState":"preparing","runnerVersion":"` + runner.OfficialRunnerVersion + `","disableUpdate":false,"jitConfig":""}`),
		[]byte(`{"commandId":"command-1","commandId":"command-2","controllerEpoch":1,"executionId":"execution-1","expectedState":"preparing","runnerVersion":"` + runner.OfficialRunnerVersion + `","disableUpdate":false,"jitConfig":"opaque"}`),
		[]byte(strings.TrimSuffix(string(valid), "}") + `,"extra":true}`),
		append(append([]byte(nil), valid...), []byte(` {}`)...),
	}
	for _, payload := range cases {
		if _, err := DecodeStartCommand(payload); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("DecodeStartCommand(%s) error = %v", payload, err)
		}
	}
}

func TestCommandReplayIdentityBindsExactPayload(t *testing.T) {
	payload, err := EncodeStartCommandPayload(testCommandMetadata(), runner.OfficialRunnerVersion, false, "opaque")
	if err != nil {
		t.Fatal(err)
	}
	command, err := DecodeStartCommand(payload)
	if err != nil {
		t.Fatal(err)
	}
	identity := command.ReplayIdentity(payload)
	if err := identity.Validate(); err != nil {
		t.Fatal(err)
	}
	changed := append([]byte(" "), payload...)
	if identity.PayloadDigest == command.ReplayIdentity(changed).PayloadDigest {
		t.Fatal("payload digest did not bind exact authenticated bytes")
	}
}

func TestCommandReplayIdentityBindsMessageType(t *testing.T) {
	metadata := testCommandMetadata()
	metadata.ExpectedState = domain.ExecutionReserved
	preparePayload, err := EncodePrepareCommandPayload(metadata, runner.OfficialRunnerVersion, false)
	if err != nil {
		t.Fatal(err)
	}
	prepare, err := DecodePrepareCommand(preparePayload)
	if err != nil {
		t.Fatal(err)
	}
	startMetadata := metadata
	startMetadata.ExpectedState = domain.ExecutionPreparing
	startPayload, err := EncodeStartCommandPayload(startMetadata, runner.OfficialRunnerVersion, false, "opaque")
	if err != nil {
		t.Fatal(err)
	}
	start, err := DecodeStartCommand(startPayload)
	if err != nil {
		t.Fatal(err)
	}
	const sameAuthenticatedPayload = `{"same":"bytes"}`
	if prepare.ReplayIdentity([]byte(sameAuthenticatedPayload)).PayloadDigest ==
		start.ReplayIdentity([]byte(sameAuthenticatedPayload)).PayloadDigest {
		t.Fatal("replay digest did not bind the command message type")
	}
}

func TestCancelCommandRoundTrip(t *testing.T) {
	metadata := testCommandMetadata()
	for _, state := range []domain.ExecutionState{
		domain.ExecutionPreparing,
		domain.ExecutionRunning,
		domain.ExecutionCleaning,
	} {
		metadata.ExpectedState = state
		// Cancel deliberately carries no target identity: tearing down work this
		// node already owns is never gated on the owner's exclusion set.
		metadata.Target = CommandTarget{}
		payload, err := EncodeCancelCommandPayload(metadata)
		if err != nil {
			t.Fatal(err)
		}
		command, err := DecodeCancelCommand(payload)
		if err != nil {
			t.Fatal(err)
		}
		if command.Metadata() != metadata {
			t.Fatalf("%s metadata = %#v", state, command.Metadata())
		}
	}
	metadata.ExpectedState = domain.ExecutionReleased
	if _, err := EncodeCancelCommandPayload(metadata); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("terminal cancel expected state error = %v", err)
	}
}
