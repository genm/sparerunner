package github

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrSecretSerialization = errors.New("GitHub secret values must not be serialized")
var ErrJITDelivery = errors.New("GitHub JIT delivery failed")

// AppPrivateKey keeps GitHub App key material opaque at the adapter boundary.
// It is constructed from a credential-store read and is never suitable for
// persistence, diagnostics, or structured logging.
type AppPrivateKey struct {
	value string
}

// NewAppPrivateKey wraps an in-memory key returned by the OS credential store.
func NewAppPrivateKey(value string) AppPrivateKey {
	return AppPrivateKey{value: value}
}

func (k AppPrivateKey) String() string   { return "github.AppPrivateKey(redacted)" }
func (k AppPrivateKey) GoString() string { return k.String() }

func (k AppPrivateKey) MarshalJSON() ([]byte, error) {
	return nil, ErrSecretSerialization
}

func (k AppPrivateKey) MarshalText() ([]byte, error) {
	return nil, ErrSecretSerialization
}

// RunnerReference identifies the runner registration created with a JIT config.
// It contains no JIT material and may be stored as ordinary execution metadata.
type RunnerReference struct {
	ID         int
	Name       string
	ScaleSetID ScaleSetID
}

// JITConfig is an opaque, non-persisted adapter handoff. Its value is
// intentionally unavailable to formatting and serialization APIs. This type
// does not guarantee string zeroization; the official runner may decode
// --jitconfig and write credentials/settings/RSA material under its root.
type JITConfig struct {
	encoded string
	runner  RunnerReference
}

func newJITConfig(encoded string, runner RunnerReference) JITConfig {
	return JITConfig{encoded: encoded, runner: runner}
}

// Runner returns non-secret registration metadata for the configuration.
func (c JITConfig) Runner() RunnerReference {
	return c.runner
}

// Digest returns a stable identifier suitable for durable delivery-state
// tracking. It cannot be used to recover the encoded JIT configuration.
func (c JITConfig) Digest() string {
	digest := sha256.Sum256([]byte(c.encoded))
	return hex.EncodeToString(digest[:])
}

// Deliver passes the encoded JIT configuration to the runner boundary. Consumers
// must not retain, log, or persist it; task-006 owns deletion and verification of
// all official runner material created after this handoff.
func (c JITConfig) Deliver(deliver func(value string) error) error {
	if deliver == nil {
		return ErrJITDelivery
	}
	if err := deliver(c.encoded); err != nil {
		// The callback may include runner output or JIT material in its error.
		// Preserve failure classification without propagating that untrusted text.
		return ErrJITDelivery
	}
	return nil
}

func (c JITConfig) String() string   { return "github.JITConfig(redacted)" }
func (c JITConfig) GoString() string { return c.String() }

func (c JITConfig) MarshalJSON() ([]byte, error) {
	return nil, ErrSecretSerialization
}

func (c JITConfig) MarshalText() ([]byte, error) {
	return nil, ErrSecretSerialization
}

// Ensure encoding/json continues to select the deliberate serialization guard
// if this package is refactored.
var _ json.Marshaler = JITConfig{}
var _ fmt.Stringer = JITConfig{}
