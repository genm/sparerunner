package transport

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/genm/tewake/internal/domain"
)

func testAgentSnapshot() AgentSnapshot {
	return AgentSnapshot{
		NodeID:             "node-1",
		OS:                 "linux",
		Arch:               "amd64",
		RunnerVersion:      "2.336.0",
		NativeRunnerReady:  true,
		MaxControllerEpoch: 2,
		Commands: []domain.Command{{
			ID:              "command-1",
			ControllerEpoch: 2,
			ExecutionID:     "execution-1",
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   domain.PayloadDigest([]byte("authenticated-command")),
		}},
		Observations: []AgentExecutionObservation{{
			ExecutionID:        "execution-1",
			State:              domain.ExecutionRunning,
			ObservedAtUnixNano: 1,
		}},
		CleanupTombstones: []AgentCleanupTombstone{{
			ExecutionID:        "execution-2",
			FailureCode:        domain.CleanupProcessResidue,
			RecordedAtUnixNano: 2,
		}},
	}
}

func TestAgentSnapshotStrictRoundTripContainsOnlyTypedEvidence(t *testing.T) {
	snapshot := testAgentSnapshot()
	payload, err := EncodeAgentSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeAgentSnapshot(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.NodeID != snapshot.NodeID || !decoded.NativeRunnerReady || len(decoded.Commands) != 1 ||
		decoded.RunnerVersion != "2.336.0" ||
		decoded.Commands[0] != snapshot.Commands[0] ||
		len(decoded.Observations) != 1 || len(decoded.CleanupTombstones) != 1 {
		t.Fatalf("decoded snapshot = %#v", decoded)
	}
	for _, forbidden := range []string{"jitConfig", "privateKey", "workspacePath", "runnerOutput"} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("snapshot contains forbidden field %q", forbidden)
		}
	}
}

func TestAgentSnapshotDigestSeparatesJournalAuthorityFromReadinessLease(t *testing.T) {
	snapshot := testAgentSnapshot()
	digest, err := AgentSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.NativeRunnerReady = false
	readinessDigest, err := AgentSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if readinessDigest != digest {
		t.Fatalf("readiness changed journal digest: %s != %s", readinessDigest, digest)
	}
	snapshot.RunnerVersion = "2.337.0"
	changedDigest, err := AgentSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == digest {
		t.Fatal("runner package identity did not change journal digest")
	}
}

func TestAgentSnapshotMissingRuntimeCapabilityIsExplicitProtocolError(t *testing.T) {
	snapshot := testAgentSnapshot()
	payload, err := EncodeAgentSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	delete(raw, "nativeRunnerReady")
	legacyPayload, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeAgentSnapshot(legacyPayload); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("snapshot without explicit runtime capability error = %v, want ErrInvalidCommand", err)
	}

	raw["nativeRunnerReady"] = true
	delete(raw, "runnerVersion")
	legacyPayload, err = json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeAgentSnapshot(legacyPayload); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("snapshot without explicit runner version error = %v, want ErrInvalidCommand", err)
	}
}

func TestAgentSnapshotRejectsUnknownFieldsAndRawCleanupError(t *testing.T) {
	valid, err := EncodeAgentSnapshot(testAgentSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	unknown := append([]byte(nil), valid[:len(valid)-1]...)
	unknown = append(unknown, []byte(`,"jitConfig":"canary.example.test"}`)...)
	if _, err := DecodeAgentSnapshot(unknown); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("unknown secret field error = %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(valid, &raw); err != nil {
		t.Fatal(err)
	}
	tombstones := raw["cleanupTombstones"].([]any)
	tombstones[0].(map[string]any)["failureCode"] = "runner-output-canary.example.test"
	invalid, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeAgentSnapshot(invalid); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("raw cleanup error = %v", err)
	}
}
