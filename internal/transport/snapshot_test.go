package transport

import (
	"encoding/json"
	"errors"
	"fmt"
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

func TestAgentSnapshotDigestExcludesExcludedTargets(t *testing.T) {
	snapshot := testAgentSnapshot()
	digest, err := AgentSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.ExcludedTargets = TargetIDSet("target-excluded")
	changedDigest, err := AgentSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest != digest {
		t.Fatal("excludedTargets changed the readiness-lease-keyed journal digest")
	}
}

// TestAgentSnapshotDigestExcludesSharedRunnerIdentity proves the isolation mode
// is owner-visible observed state and not journal authority: reporting it, and
// flipping it, must leave the digest an in-flight readiness lease is keyed to
// completely unchanged.
func TestAgentSnapshotDigestExcludesSharedRunnerIdentity(t *testing.T) {
	snapshot := testAgentSnapshot()
	digest, err := AgentSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, reported := range []bool{false, true} {
		snapshot.SharedRunnerIdentity = &reported
		changedDigest, err := AgentSnapshotDigest(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if changedDigest != digest {
			t.Fatalf(
				"sharedRunnerIdentity=%t changed the readiness-lease-keyed journal digest: %s != %s",
				reported, changedDigest, digest,
			)
		}
	}
}

func TestAgentSnapshotSharedRunnerIdentityRoundTrip(t *testing.T) {
	base := testAgentSnapshot()

	// Absent: the field is omitted entirely and decodes back to nil, so "not
	// reported" is never silently read as the isolated mode.
	absentPayload, err := EncodeAgentSnapshot(base)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(absentPayload), "sharedRunnerIdentity") {
		t.Fatalf("absent SharedRunnerIdentity was encoded: %s", absentPayload)
	}
	decodedAbsent, err := DecodeAgentSnapshot(absentPayload)
	if err != nil || decodedAbsent.SharedRunnerIdentity != nil {
		t.Fatalf(
			"decoded absent SharedRunnerIdentity = %#v, err = %v",
			decodedAbsent.SharedRunnerIdentity, err,
		)
	}

	// Both present values survive the round trip as distinct observations.
	for _, reported := range []bool{true, false} {
		present := base
		present.SharedRunnerIdentity = &reported
		payload, err := EncodeAgentSnapshot(present)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(payload), "sharedRunnerIdentity") {
			t.Fatalf("present SharedRunnerIdentity=%t was not encoded: %s", reported, payload)
		}
		decoded, err := DecodeAgentSnapshot(payload)
		if err != nil || decoded.SharedRunnerIdentity == nil ||
			*decoded.SharedRunnerIdentity != reported {
			t.Fatalf(
				"decoded SharedRunnerIdentity = %#v, err = %v, want %t",
				decoded.SharedRunnerIdentity, err, reported,
			)
		}
	}
}

func TestAgentSnapshotExcludedTargetsRoundTrip(t *testing.T) {
	base := testAgentSnapshot()

	// Absent: the field is entirely omitted, and decode reports nil, not an
	// empty-but-present slice.
	absentPayload, err := EncodeAgentSnapshot(base)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(absentPayload), "excludedTargets") {
		t.Fatalf("absent ExcludedTargets was encoded: %s", absentPayload)
	}
	decodedAbsent, err := DecodeAgentSnapshot(absentPayload)
	if err != nil || decodedAbsent.ExcludedTargets != nil {
		t.Fatalf("decoded absent ExcludedTargets = %#v, err = %v", decodedAbsent.ExcludedTargets, err)
	}

	// Present-but-empty is a distinct wire state from absent.
	var raw map[string]any
	if err := json.Unmarshal(absentPayload, &raw); err != nil {
		t.Fatal(err)
	}
	raw["excludedTargets"] = []string{}
	emptyPayload, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	decodedEmpty, err := DecodeAgentSnapshot(emptyPayload)
	if err != nil || decodedEmpty.ExcludedTargets == nil || len(*decodedEmpty.ExcludedTargets) != 0 {
		t.Fatalf("decoded empty ExcludedTargets = %#v, err = %v", decodedEmpty.ExcludedTargets, err)
	}

	// Populated round-trips intact.
	populated := base
	populated.ExcludedTargets = TargetIDSet("target-1", "target-2")
	populatedPayload, err := EncodeAgentSnapshot(populated)
	if err != nil {
		t.Fatal(err)
	}
	decodedPopulated, err := DecodeAgentSnapshot(populatedPayload)
	if err != nil || decodedPopulated.ExcludedTargets == nil ||
		len(*decodedPopulated.ExcludedTargets) != 2 ||
		(*decodedPopulated.ExcludedTargets)[0] != "target-1" ||
		(*decodedPopulated.ExcludedTargets)[1] != "target-2" {
		t.Fatalf("decoded populated ExcludedTargets = %#v, err = %v", decodedPopulated.ExcludedTargets, err)
	}

	// A duplicate TargetID is corruption, not a legitimate repeat.
	duplicated := base
	duplicated.ExcludedTargets = TargetIDSet("target-1", "target-1")
	if _, err := EncodeAgentSnapshot(duplicated); err == nil {
		t.Fatal("duplicate ExcludedTargets entry accepted at encode")
	}
	raw["excludedTargets"] = []string{"target-1", "target-1"}
	duplicatePayload, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeAgentSnapshot(duplicatePayload); err == nil {
		t.Fatal("duplicate ExcludedTargets entry accepted at decode")
	}

	// A list beyond MaxEligibleTargets is rejected as oversized.
	oversized := make([]domain.TargetID, MaxEligibleTargets+1)
	for index := range oversized {
		oversized[index] = domain.TargetID(fmt.Sprintf("target-%d", index))
	}
	oversizedSnapshot := base
	oversizedSnapshot.ExcludedTargets = &oversized
	if _, err := EncodeAgentSnapshot(oversizedSnapshot); err == nil {
		t.Fatal("oversized ExcludedTargets accepted at encode")
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
