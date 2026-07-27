package transport

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/genm/tewake/internal/domain"
)

func TestAgentHeartbeatRoundTrip(t *testing.T) {
	payload, err := EncodeAgentHeartbeat(AgentHeartbeat{
		NodeID:            "node-1",
		NativeRunnerReady: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeAgentHeartbeat(payload)
	if err != nil || decoded.NodeID != "node-1" || !decoded.NativeRunnerReady {
		t.Fatalf("heartbeat=%#v err=%v", decoded, err)
	}
}

func TestAgentHeartbeatExcludedTargetsRoundTrip(t *testing.T) {
	// Absent: an Agent that never populates the field omits it entirely, and
	// decode must report it back as nil rather than an empty-but-present slice.
	absentPayload, err := EncodeAgentHeartbeat(AgentHeartbeat{
		NodeID:            "node-1",
		NativeRunnerReady: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(absentPayload, []byte("excludedTargets")) {
		t.Fatalf("absent ExcludedTargets was encoded: %s", absentPayload)
	}
	decodedAbsent, err := DecodeAgentHeartbeat(absentPayload)
	if err != nil || decodedAbsent.ExcludedTargets != nil {
		t.Fatalf("decoded absent ExcludedTargets = %#v, err = %v", decodedAbsent.ExcludedTargets, err)
	}

	// Present-but-empty must survive as a non-nil, zero-length slice: it is a
	// distinct wire state from absent, not collapsed to the same nil value.
	emptyPayload, err := json.Marshal(struct {
		NodeID            domain.NodeID     `json:"nodeId"`
		NativeRunnerReady bool              `json:"nativeRunnerReady"`
		ExcludedTargets   []domain.TargetID `json:"excludedTargets"`
	}{NodeID: "node-1", NativeRunnerReady: true, ExcludedTargets: []domain.TargetID{}})
	if err != nil {
		t.Fatal(err)
	}
	decodedEmpty, err := DecodeAgentHeartbeat(emptyPayload)
	if err != nil || decodedEmpty.ExcludedTargets == nil || len(*decodedEmpty.ExcludedTargets) != 0 {
		t.Fatalf("decoded empty ExcludedTargets = %#v, err = %v", decodedEmpty.ExcludedTargets, err)
	}

	// Populated round-trips through Encode/Decode intact.
	populated := AgentHeartbeat{
		NodeID:            "node-1",
		NativeRunnerReady: true,
		ExcludedTargets:   TargetIDSet("target-1", "target-2"),
	}
	populatedPayload, err := EncodeAgentHeartbeat(populated)
	if err != nil {
		t.Fatal(err)
	}
	decodedPopulated, err := DecodeAgentHeartbeat(populatedPayload)
	if err != nil || decodedPopulated.ExcludedTargets == nil ||
		len(*decodedPopulated.ExcludedTargets) != 2 ||
		(*decodedPopulated.ExcludedTargets)[0] != "target-1" ||
		(*decodedPopulated.ExcludedTargets)[1] != "target-2" {
		t.Fatalf("decoded populated ExcludedTargets = %#v, err = %v", decodedPopulated.ExcludedTargets, err)
	}

	// A duplicate TargetID is corruption, not a legitimate repeat.
	duplicatePayload, err := json.Marshal(struct {
		NodeID            domain.NodeID     `json:"nodeId"`
		NativeRunnerReady bool              `json:"nativeRunnerReady"`
		ExcludedTargets   []domain.TargetID `json:"excludedTargets"`
	}{NodeID: "node-1", NativeRunnerReady: true, ExcludedTargets: []domain.TargetID{"target-1", "target-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeAgentHeartbeat(duplicatePayload); err == nil {
		t.Fatal("duplicate ExcludedTargets entry accepted")
	}

	// A list beyond MaxEligibleTargets is rejected as oversized.
	oversized := make([]domain.TargetID, MaxEligibleTargets+1)
	for index := range oversized {
		oversized[index] = domain.TargetID(string(rune('a' + index%26)))
	}
	oversizedHeartbeat := AgentHeartbeat{
		NodeID:            "node-1",
		NativeRunnerReady: true,
		ExcludedTargets:   &oversized,
	}
	if _, err := EncodeAgentHeartbeat(oversizedHeartbeat); err == nil {
		t.Fatal("oversized ExcludedTargets accepted at encode")
	}
}

func TestAgentHeartbeatRejectsMissingUnknownDuplicateAndTrailingFields(t *testing.T) {
	for _, payload := range [][]byte{
		nil,
		[]byte(`{"nativeRunnerReady":true}`),
		[]byte(`{"nodeId":"node-1"}`),
		[]byte(`{"nodeId":"node-1","nativeRunnerReady":true,"extra":1}`),
		[]byte(`{"nodeId":"node-1","nodeId":"node-2","nativeRunnerReady":true}`),
		[]byte(`{"nodeId":"node-1","nativeRunnerReady":true}{}`),
		bytes.Repeat([]byte("x"), int(MaxEnvelopeBytes)+1),
	} {
		if _, err := DecodeAgentHeartbeat(payload); err == nil {
			t.Fatalf("invalid heartbeat accepted: %q", payload)
		}
	}
}
