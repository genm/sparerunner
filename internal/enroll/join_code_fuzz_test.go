package enroll

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
)

// FuzzDecodeJoinCode explores the one untrusted string an operator pastes into a
// computer that is not enrolled yet. Two properties are asserted.
//
// Decoding must classify every input as either a join code or one of the two
// declared rejection errors. An unclassified error would mean a decoding path
// escaped the fail-closed contract.
//
// An accepted code must re-encode to exactly the bytes that were accepted.
// DecodeJoinCode documents one canonical representation, and that single
// representation is what rejects padded, standard-base64, reordered-hint,
// duplicate-hint, and trailing-byte variants before a network connection is
// attempted. A second accepted spelling of the same code would silently widen
// that surface, and only a differential property catches it.
//
// Failures never print the code. The fuzzing engine writes the failing input to
// this package's testdata corpus, which is where a join secret belongs during
// triage, rather than in a public CI log.
func FuzzDecodeJoinCode(f *testing.F) {
	for _, seed := range joinCodeFuzzSeeds(f) {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, encoded string) {
		code, err := DecodeJoinCode(encoded)
		if err != nil {
			if !errors.Is(err, ErrInvalidJoinCode) && !errors.Is(err, ErrJoinCodeVersion) {
				t.Fatalf("join code rejected with an undeclared error class: %v", err)
			}
			return
		}
		reEncoded, err := code.Encode()
		if err != nil {
			t.Fatalf("accepted join code does not re-encode: %v", err)
		}
		if reEncoded != encoded {
			t.Fatal("accepted join code has a second representation, so the canonical form is not enforced")
		}
	})
}

func joinCodeFuzzSeeds(f *testing.F) []string {
	f.Helper()

	seeds := []string{
		"",
		JoinCodePrefix,
		JoinCodePrefix + "not-base64!",
		JoinCodePrefix + "U1BS",
	}
	for _, hints := range [][]string{
		nil,
		{"192.0.2.10:8443"},
		{"controller.example.test:8443", "https://controller.example.test"},
	} {
		code, err := NewJoinCode(
			sha256.Sum256([]byte("fuzz-seed-authority")),
			hints,
			bytes.NewReader(bytes.Repeat([]byte{0x5a}, 48)),
		)
		if err != nil {
			f.Fatalf("build join code seed: %v", err)
		}
		encoded, err := code.Encode()
		if err != nil {
			f.Fatalf("encode join code seed: %v", err)
		}
		seeds = append(seeds, encoded, encoded+"A", encoded[:len(encoded)-1])
	}
	return seeds
}
