package transport

import (
	"bytes"
	"testing"
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
