package github

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestJITConfigRedactsFormattingAndRejectsSerialization(t *testing.T) {
	const rawJIT = "jit-canary-not-for-logs-or-storage"
	jit := newJITConfig(rawJIT, RunnerReference{ID: 42, Name: "runner-42", ScaleSetID: 7})

	for _, formatted := range []string{fmt.Sprint(jit), fmt.Sprintf("%#v", jit)} {
		if strings.Contains(formatted, rawJIT) {
			t.Fatalf("formatted JIT configuration leaked raw value: %q", formatted)
		}
	}
	if _, err := json.Marshal(jit); err == nil {
		t.Fatal("json.Marshal(JITConfig) succeeded, want serialization rejection")
	}
	if got := jit.Digest(); strings.Contains(got, rawJIT) || got == "" {
		t.Fatalf("Digest() = %q, want non-empty non-secret digest", got)
	}

	var delivered string
	if err := jit.Deliver(func(value string) error {
		delivered = value
		return nil
	}); err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if delivered != rawJIT {
		t.Fatalf("Deliver() value = %q, want opaque JIT passed only to recipient", delivered)
	}
}

func TestAppPrivateKeyRedactsFormattingAndRejectsSerialization(t *testing.T) {
	const rawKey = "private-key-canary"
	key := NewAppPrivateKey(rawKey)

	for _, formatted := range []string{fmt.Sprint(key), fmt.Sprintf("%#v", key)} {
		if strings.Contains(formatted, rawKey) {
			t.Fatalf("formatted private key leaked raw value: %q", formatted)
		}
	}
	if _, err := json.Marshal(key); err == nil {
		t.Fatal("json.Marshal(AppPrivateKey) succeeded, want serialization rejection")
	}
}
