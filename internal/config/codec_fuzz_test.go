package config

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

// FuzzDecodeJSON and FuzzDecodeYAML explore the operator-supplied configuration
// document that both the CLI and the management API accept. The two codecs are
// separate strict parsers over the same type, so they are fuzzed separately and
// then compared against each other.
func FuzzDecodeJSON(f *testing.F) {
	for _, seed := range configurationFuzzSeeds(f, EncodeJSON) {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, body []byte) {
		configuration, err := DecodeJSON(bytes.NewReader(body))
		if err != nil {
			requireDeclaredConfigurationError(t, err, ErrInvalidJSON)
			return
		}
		requireCodecFixpoint(t, configuration)
	})
}

func FuzzDecodeYAML(f *testing.F) {
	for _, seed := range configurationFuzzSeeds(f, EncodeYAML) {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, body []byte) {
		configuration, err := DecodeYAML(bytes.NewReader(body))
		if err != nil {
			requireDeclaredConfigurationError(t, err, ErrInvalidYAML)
			return
		}
		requireCodecFixpoint(t, configuration)
	})
}

// requireCodecFixpoint asserts what an accepted configuration has to satisfy.
//
// It must re-encode, because a document the controller accepted but cannot
// write back is state the operator can no longer read or edit.
//
// Encoding is a fixpoint: canonicalization sorts and normalizes, so a second
// round must return identical bytes. Without that, the same accepted document
// could keep producing a new revision body on every read-modify-write.
//
// Both codecs must agree. The CLI writes YAML and the management API writes
// JSON against one contract, so a value that survives one codec but changes
// meaning through the other is a divergence between the two mutation paths.
func requireCodecFixpoint(t *testing.T, configuration Configuration) {
	t.Helper()

	jsonBody, err := EncodeJSON(configuration)
	if err != nil {
		t.Fatalf("accepted configuration does not re-encode as JSON: %v", err)
	}
	fromJSON, err := DecodeJSON(bytes.NewReader(jsonBody))
	if err != nil {
		t.Fatalf("re-encoded JSON configuration is no longer accepted: %v", err)
	}
	reJSONBody, err := EncodeJSON(fromJSON)
	if err != nil {
		t.Fatalf("normalized configuration does not re-encode as JSON: %v", err)
	}
	if !bytes.Equal(jsonBody, reJSONBody) {
		t.Fatalf("JSON encoding is not a fixpoint: %s then %s", jsonBody, reJSONBody)
	}

	yamlBody, err := EncodeYAML(configuration)
	if err != nil {
		t.Fatalf("accepted configuration does not re-encode as YAML: %v", err)
	}
	fromYAML, err := DecodeYAML(bytes.NewReader(yamlBody))
	if err != nil {
		t.Fatalf("re-encoded YAML configuration is no longer accepted: %v", err)
	}
	reYAMLBody, err := EncodeYAML(fromYAML)
	if err != nil {
		t.Fatalf("normalized configuration does not re-encode as YAML: %v", err)
	}
	if !bytes.Equal(yamlBody, reYAMLBody) {
		t.Fatalf("YAML encoding is not a fixpoint: %s then %s", yamlBody, reYAMLBody)
	}

	if !reflect.DeepEqual(fromJSON, fromYAML) {
		t.Fatalf("codecs disagree about the same configuration: %+v then %+v", fromJSON, fromYAML)
	}
}

// requireDeclaredConfigurationError keeps rejection inside the classes the
// package declares. Decoding is a fail-closed boundary, so an error that is
// none of these means a path returned something the callers cannot classify.
func requireDeclaredConfigurationError(t *testing.T, err error, codecError error) {
	t.Helper()

	if errors.Is(err, codecError) ||
		errors.Is(err, ErrPayloadTooLarge) ||
		errors.Is(err, ErrInvalidConfiguration) {
		return
	}
	t.Fatalf("configuration rejected with an undeclared error class: %v", err)
}

func configurationFuzzSeeds(
	f *testing.F,
	encode func(Configuration) ([]byte, error),
) [][]byte {
	f.Helper()

	encoded, err := encode(configurationFixture())
	if err != nil {
		f.Fatalf("encode configuration seed: %v", err)
	}
	return [][]byte{
		nil,
		[]byte("{}"),
		[]byte("null"),
		encoded,
		append(bytes.Clone(encoded), '\n', '{'),
	}
}
