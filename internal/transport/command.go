package transport

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/runner"
)

var (
	ErrInvalidCommand       = errors.New("invalid agent command")
	ErrCommandSecret        = errors.New("agent command secret delivery failed")
	ErrCommandSerialization = errors.New("agent commands containing secrets cannot be serialized")
)

// CommandMetadata is the non-secret replay identity shared by prepare, start,
// and cancel. PayloadDigest is computed from the exact authenticated envelope
// kind and payload rather than accepted from the peer.
type CommandMetadata struct {
	CommandID       domain.CommandID
	ControllerEpoch domain.ControllerEpoch
	ExecutionID     domain.ExecutionID
	ExpectedState   domain.ExecutionState
}

func (metadata CommandMetadata) replayIdentity(kind MessageType, payload []byte) domain.Command {
	digest := PayloadDigest(kind, payload)
	return domain.Command{
		ID:              metadata.CommandID,
		ControllerEpoch: metadata.ControllerEpoch,
		ExecutionID:     metadata.ExecutionID,
		ExpectedState:   metadata.ExpectedState,
		PayloadDigest:   hex.EncodeToString(digest[:]),
	}
}

// PayloadDigest binds the authenticated envelope kind to the exact payload
// bytes. Controller issuance and Agent replay persistence must use this same
// helper so a cross-type command can never inherit another command's identity.
func PayloadDigest(kind MessageType, payload []byte) [sha256.Size]byte {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(kind))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(payload)
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}

// PrepareCommand downloads and materializes the pinned runner package without
// carrying JIT material. This durable phase must complete before the Controller
// requests a one-job JIT configuration.
type PrepareCommand struct {
	metadata      CommandMetadata
	runnerVersion string
	disableUpdate bool
}

func (command PrepareCommand) Metadata() CommandMetadata { return command.metadata }
func (command PrepareCommand) RunnerVersion() string     { return command.runnerVersion }
func (command PrepareCommand) DisableUpdate() bool       { return command.disableUpdate }
func (command PrepareCommand) ReplayIdentity(payload []byte) domain.Command {
	return command.metadata.replayIdentity(MessagePrepare, payload)
}

type prepareCommandWire struct {
	CommandID       domain.CommandID       `json:"commandId"`
	ControllerEpoch domain.ControllerEpoch `json:"controllerEpoch"`
	ExecutionID     domain.ExecutionID     `json:"executionId"`
	ExpectedState   domain.ExecutionState  `json:"expectedState"`
	RunnerVersion   string                 `json:"runnerVersion"`
	DisableUpdate   bool                   `json:"disableUpdate"`
}

func EncodePrepareCommandPayload(metadata CommandMetadata, runnerVersion string, disableUpdate bool) (json.RawMessage, error) {
	wire := prepareCommandWire{
		CommandID:       metadata.CommandID,
		ControllerEpoch: metadata.ControllerEpoch,
		ExecutionID:     metadata.ExecutionID,
		ExpectedState:   metadata.ExpectedState,
		RunnerVersion:   runnerVersion,
		DisableUpdate:   disableUpdate,
	}
	if err := validatePrepareWire(wire); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(wire)
	if err != nil || int64(len(payload)) > MaxEnvelopeBytes {
		return nil, ErrInvalidCommand
	}
	return payload, nil
}

func DecodePrepareCommand(payload []byte) (PrepareCommand, error) {
	var wire prepareCommandWire
	if err := decodeStrictCommand(payload, &wire); err != nil || validatePrepareWire(wire) != nil {
		return PrepareCommand{}, ErrInvalidCommand
	}
	return PrepareCommand{
		metadata: CommandMetadata{
			CommandID:       wire.CommandID,
			ControllerEpoch: wire.ControllerEpoch,
			ExecutionID:     wire.ExecutionID,
			ExpectedState:   wire.ExpectedState,
		},
		runnerVersion: wire.RunnerVersion,
		disableUpdate: wire.DisableUpdate,
	}, nil
}

func validatePrepareWire(wire prepareCommandWire) error {
	metadata := CommandMetadata{
		CommandID:       wire.CommandID,
		ControllerEpoch: wire.ControllerEpoch,
		ExecutionID:     wire.ExecutionID,
		ExpectedState:   wire.ExpectedState,
	}
	if wire.RunnerVersion != runner.OfficialRunnerVersion || metadata.ExpectedState != domain.ExecutionReserved {
		return ErrInvalidCommand
	}
	return metadata.replayIdentity(MessagePrepare, []byte(`{}`)).Validate()
}

// StartCommand is a decoded, authenticated command. The JIT body stays behind a
// one-shot delivery method and rejects formatting/JSON serialization.
type StartCommand struct {
	metadata      CommandMetadata
	runnerVersion string
	disableUpdate bool
	secret        *commandSecret
}

type commandSecret struct {
	mu       sync.Mutex
	value    string
	digest   string
	consumed bool
}

func (command StartCommand) Metadata() CommandMetadata { return command.metadata }
func (command StartCommand) RunnerVersion() string     { return command.runnerVersion }
func (command StartCommand) DisableUpdate() bool       { return command.disableUpdate }
func (command StartCommand) ReplayIdentity(payload []byte) domain.Command {
	return command.metadata.replayIdentity(MessageStart, payload)
}

func (command StartCommand) Digest() string {
	if command.secret == nil {
		return ""
	}
	command.secret.mu.Lock()
	defer command.secret.mu.Unlock()
	return command.secret.digest
}

// Discard irrevocably drops an undelivered JIT value. It is safe to call more
// than once and is used whenever a decoded command will not cross the runner
// exec boundary (for example, state rejection or a failed acknowledgement).
func (command StartCommand) Discard() {
	if command.secret == nil {
		return
	}
	command.secret.mu.Lock()
	command.secret.consumed = true
	command.secret.value = ""
	command.secret.mu.Unlock()
}

func (command StartCommand) Deliver(deliver func(string) error) error {
	if command.secret == nil || deliver == nil {
		return ErrCommandSecret
	}
	command.secret.mu.Lock()
	if command.secret.consumed || command.secret.value == "" {
		command.secret.mu.Unlock()
		return ErrCommandSecret
	}
	command.secret.consumed = true
	value := command.secret.value
	command.secret.value = ""
	command.secret.mu.Unlock()
	if err := deliver(value); err != nil {
		// Callback text can contain process output or the JIT value. Preserve only
		// the stable classification at this transport boundary.
		return ErrCommandSecret
	}
	return nil
}

func (StartCommand) String() string           { return "transport.StartCommand(redacted)" }
func (command StartCommand) GoString() string { return command.String() }
func (StartCommand) MarshalJSON() ([]byte, error) {
	return nil, ErrCommandSerialization
}

type startCommandWire struct {
	CommandID       domain.CommandID       `json:"commandId"`
	ControllerEpoch domain.ControllerEpoch `json:"controllerEpoch"`
	ExecutionID     domain.ExecutionID     `json:"executionId"`
	ExpectedState   domain.ExecutionState  `json:"expectedState"`
	RunnerVersion   string                 `json:"runnerVersion"`
	DisableUpdate   bool                   `json:"disableUpdate"`
	JITConfig       string                 `json:"jitConfig"`
}

// EncodeStartCommandPayload is the only serialization path for the secret-bearing
// wire object. Callers pass its result straight to an authenticated Envelope and
// must not retain it in desired-state persistence or logs.
func EncodeStartCommandPayload(metadata CommandMetadata, runnerVersion string, disableUpdate bool, jitConfig string) (json.RawMessage, error) {
	wire := startCommandWire{
		CommandID:       metadata.CommandID,
		ControllerEpoch: metadata.ControllerEpoch,
		ExecutionID:     metadata.ExecutionID,
		ExpectedState:   metadata.ExpectedState,
		RunnerVersion:   runnerVersion,
		DisableUpdate:   disableUpdate,
		JITConfig:       jitConfig,
	}
	if err := validateStartWire(wire); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(wire)
	if err != nil || int64(len(payload)) > MaxEnvelopeBytes {
		return nil, ErrInvalidCommand
	}
	return payload, nil
}

func DecodeStartCommand(payload []byte) (StartCommand, error) {
	var wire startCommandWire
	if err := decodeStrictCommand(payload, &wire); err != nil || validateStartWire(wire) != nil {
		return StartCommand{}, ErrInvalidCommand
	}
	digest := sha256.Sum256([]byte(wire.JITConfig))
	return StartCommand{
		metadata: CommandMetadata{
			CommandID:       wire.CommandID,
			ControllerEpoch: wire.ControllerEpoch,
			ExecutionID:     wire.ExecutionID,
			ExpectedState:   wire.ExpectedState,
		},
		runnerVersion: wire.RunnerVersion,
		disableUpdate: wire.DisableUpdate,
		secret:        &commandSecret{value: wire.JITConfig, digest: hex.EncodeToString(digest[:])},
	}, nil
}

func validateStartWire(wire startCommandWire) error {
	metadata := CommandMetadata{
		CommandID:       wire.CommandID,
		ControllerEpoch: wire.ControllerEpoch,
		ExecutionID:     wire.ExecutionID,
		ExpectedState:   wire.ExpectedState,
	}
	if wire.JITConfig == "" || wire.RunnerVersion != runner.OfficialRunnerVersion || metadata.ExpectedState != domain.ExecutionPreparing {
		return ErrInvalidCommand
	}
	return metadata.replayIdentity(MessageStart, []byte(`{}`)).Validate()
}

type CancelCommand struct {
	metadata CommandMetadata
}

func (command CancelCommand) Metadata() CommandMetadata { return command.metadata }
func (command CancelCommand) ReplayIdentity(payload []byte) domain.Command {
	return command.metadata.replayIdentity(MessageCancel, payload)
}

type cancelCommandWire struct {
	CommandID       domain.CommandID       `json:"commandId"`
	ControllerEpoch domain.ControllerEpoch `json:"controllerEpoch"`
	ExecutionID     domain.ExecutionID     `json:"executionId"`
	ExpectedState   domain.ExecutionState  `json:"expectedState"`
}

func EncodeCancelCommandPayload(metadata CommandMetadata) (json.RawMessage, error) {
	wire := cancelCommandWire{
		CommandID:       metadata.CommandID,
		ControllerEpoch: metadata.ControllerEpoch,
		ExecutionID:     metadata.ExecutionID,
		ExpectedState:   metadata.ExpectedState,
	}
	if err := validateCancelWire(wire); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return nil, ErrInvalidCommand
	}
	return payload, nil
}

func DecodeCancelCommand(payload []byte) (CancelCommand, error) {
	var wire cancelCommandWire
	if err := decodeStrictCommand(payload, &wire); err != nil || validateCancelWire(wire) != nil {
		return CancelCommand{}, ErrInvalidCommand
	}
	return CancelCommand{metadata: CommandMetadata{
		CommandID:       wire.CommandID,
		ControllerEpoch: wire.ControllerEpoch,
		ExecutionID:     wire.ExecutionID,
		ExpectedState:   wire.ExpectedState,
	}}, nil
}

func validateCancelWire(wire cancelCommandWire) error {
	metadata := CommandMetadata{
		CommandID:       wire.CommandID,
		ControllerEpoch: wire.ControllerEpoch,
		ExecutionID:     wire.ExecutionID,
		ExpectedState:   wire.ExpectedState,
	}
	if metadata.ExpectedState != domain.ExecutionPreparing &&
		metadata.ExpectedState != domain.ExecutionRunning &&
		metadata.ExpectedState != domain.ExecutionCleaning {
		return ErrInvalidCommand
	}
	return metadata.replayIdentity(MessageCancel, []byte(`{}`)).Validate()
}

func decodeStrictCommand(payload []byte, destination any) error {
	if len(payload) == 0 || int64(len(payload)) > MaxEnvelopeBytes || rejectDuplicateJSONKeys(payload) != nil {
		return ErrInvalidCommand
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return ErrInvalidCommand
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalidCommand
	}
	return nil
}

var _ json.Marshaler = StartCommand{}
