package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/transport"
)

// TestEligibleTargetsEchoesDurablyAdoptedExclusions proves the echo is read
// from the controller's durable adoption table rather than an in-memory guess,
// which is what lets a desktop surface self-heal across reconnects instead of
// drifting from what actually gates capacity.
func TestEligibleTargetsEchoesDurablyAdoptedExclusions(t *testing.T) {
	recording := &recordingControllerAgentStore{
		adoptedExclusions: map[domain.NodeID][]domain.TargetID{
			"node-excluded": {"target-b"},
		},
	}
	recording.setConfiguration(eligibilityTestConfiguration(1))
	consumers := newStoreBackedAgentConsumers(recording)

	eligible, err := consumers.Eligibility.EligibleTargets(
		context.Background(), "node-excluded", domain.OSLinux, domain.ArchAMD64,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(eligible) != 2 {
		t.Fatalf("eligible targets = %#v, want two entries", eligible)
	}
	// An excluded Target stays visible; it is reported as excluded rather than
	// hidden, so the owner can include it again.
	if eligible[0].TargetID != "target-a" || eligible[0].Excluded {
		t.Fatalf("unexcluded target = %#v", eligible[0])
	}
	if eligible[1].TargetID != "target-b" || !eligible[1].Excluded {
		t.Fatalf("excluded target = %#v", eligible[1])
	}

	// A different node on the same configuration sees no exclusions: adoption is
	// per node, never fleet-wide.
	other, err := consumers.Eligibility.EligibleTargets(
		context.Background(), "node-other", domain.OSLinux, domain.ArchAMD64,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range other {
		if target.Excluded {
			t.Fatalf("another node's exclusion leaked: %#v", target)
		}
	}
}

// TestStoreBackedOwnerStateAdoptionPreservesEmptySetSemantics guards the
// nil-versus-empty distinction across the app/store boundary: an explicit empty
// set is an authoritative "no exclusions", not "nothing reported".
func TestStoreBackedOwnerStateAdoptionPreservesEmptySetSemantics(t *testing.T) {
	recording := &recordingControllerAgentStore{}
	consumers := newStoreBackedAgentConsumers(recording)
	if err := consumers.OwnerState.HandleAgentOwnerState(
		context.Background(),
		AgentOwnerStateRecord{
			NodeID:          "node-1",
			SnapshotDigest:  "digest",
			ExcludedTargets: []domain.TargetID{},
		},
	); err != nil {
		t.Fatal(err)
	}
	if len(recording.ownerStates) != 1 {
		t.Fatalf("owner state calls = %d, want 1", len(recording.ownerStates))
	}
	adopted := recording.ownerStates[0]
	if adopted.Exclusions == nil {
		t.Fatal("explicit empty set reached the store as absent")
	}
	if len(*adopted.Exclusions) != 0 {
		t.Fatalf("adopted exclusions = %#v, want empty", *adopted.Exclusions)
	}
}

// ownerStateRecorder captures adoption calls made by the broker and reports the
// order they were observed in relative to the heartbeat acknowledgement.
type ownerStateRecorder struct {
	records []AgentOwnerStateRecord
	err     error
}

func (recorder *ownerStateRecorder) HandleAgentOwnerState(
	_ context.Context,
	record AgentOwnerStateRecord,
) error {
	recorder.records = append(recorder.records, record)
	return recorder.err
}

// TestHeartbeatAdoptsOwnerStateBeforeAcknowledgement proves a heartbeat-carried
// change is durably adopted before the heartbeat is acknowledged, and that a
// steady-state repeat of the same set does not re-adopt.
func TestHeartbeatAdoptsOwnerStateBeforeAcknowledgement(t *testing.T) {
	recorder := &ownerStateRecorder{}
	consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
	consumers.OwnerState = recorder
	consumers.Eligibility = AgentEligibilityConsumerFunc(func(
		_ context.Context, _ domain.NodeID,
		_ domain.OperatingSystem, _ domain.Architecture,
	) ([]transport.EligibleTarget, error) {
		// The echo reflects adoption that has already been committed by the time
		// the ack is written.
		if len(recorder.records) == 0 {
			return nil, errors.New("acknowledged before adoption")
		}
		return []transport.EligibleTarget{{
			TargetID:     "target-1",
			ScopeKind:    domain.TargetRepository,
			Scope:        "owner/repo",
			ScaleSetName: "scale-set",
			Excluded:     true,
		}}, nil
	})
	broker := NewAgentBroker(1, consumers)
	session, serveResult := startReadyBrokerSession(t, broker, "node-1")

	session.send(brokerEnvelope(t, "heartbeat-1", transport.MessageHeartbeat, transport.AgentHeartbeat{
		NodeID:             "node-1",
		NativeRunnerReady:  true,
		AvailabilityIntent: domain.AvailabilityStopped,
		ExcludedTargets:    transport.TargetIDSet("target-1"),
	}))
	envelope := receiveBrokerWrite(t, session)
	if envelope.Type != transport.MessageAck {
		t.Fatalf("message type = %s, want ack", envelope.Type)
	}
	if len(recorder.records) != 1 {
		t.Fatalf("owner state adoptions = %d, want 1", len(recorder.records))
	}
	adopted := recorder.records[0]
	if adopted.NodeID != "node-1" ||
		adopted.AvailabilityIntent != domain.AvailabilityStopped ||
		len(adopted.ExcludedTargets) != 1 ||
		adopted.ExcludedTargets[0] != "target-1" {
		t.Fatalf("adopted owner state = %#v", adopted)
	}
	var acknowledgement struct {
		EligibleTargets []transport.EligibleTarget `json:"eligibleTargets"`
	}
	if err := json.Unmarshal(envelope.Payload, &acknowledgement); err != nil {
		t.Fatal(err)
	}
	if len(acknowledgement.EligibleTargets) != 1 ||
		!acknowledgement.EligibleTargets[0].Excluded {
		t.Fatalf("ack did not echo adopted exclusion: %s", envelope.Payload)
	}

	// Repeating the identical owner state is a steady-state heartbeat, not a
	// change, so it must not re-adopt.
	session.send(brokerEnvelope(t, "heartbeat-2", transport.MessageHeartbeat, transport.AgentHeartbeat{
		NodeID:             "node-1",
		NativeRunnerReady:  true,
		AvailabilityIntent: domain.AvailabilityStopped,
		ExcludedTargets:    transport.TargetIDSet("target-1"),
	}))
	if envelope := receiveBrokerWrite(t, session); envelope.Type != transport.MessageAck {
		t.Fatalf("message type = %s, want ack", envelope.Type)
	}
	if len(recorder.records) != 1 {
		t.Fatalf("steady-state heartbeat re-adopted: %#v", recorder.records)
	}

	session.disconnect()
	if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
		t.Fatalf("serve result = %v", err)
	}
}

// TestHeartbeatOwnerStateAdoptionFailureFailsTheHeartbeat proves a failed
// adoption is never acknowledged: the owner's change must not appear accepted
// while no durable record of it exists.
func TestHeartbeatOwnerStateAdoptionFailureFailsTheHeartbeat(t *testing.T) {
	for _, test := range []struct {
		name     string
		consumer AgentOwnerStateConsumer
		wantErr  error
	}{
		{
			name:     "commit failure",
			consumer: &ownerStateRecorder{err: errors.New("durable adoption failed")},
			wantErr:  ErrAgentSnapshotCommit,
		},
		{
			// Owner state is not best-effort display data: with no consumer to
			// adopt it, the session fails rather than silently dropping it.
			name:     "no consumer",
			consumer: nil,
			wantErr:  ErrAgentOwnerStateConsumerRequired,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
			consumers.OwnerState = test.consumer
			broker := NewAgentBroker(1, consumers)
			session, serveResult := startReadyBrokerSession(t, broker, "node-1")
			session.send(brokerEnvelope(t, "heartbeat-1", transport.MessageHeartbeat, transport.AgentHeartbeat{
				NodeID:            "node-1",
				NativeRunnerReady: true,
				ExcludedTargets:   transport.TargetIDSet("target-1"),
			}))
			if err := <-serveResult; !errors.Is(err, test.wantErr) {
				t.Fatalf("serve result = %v, want %v", err, test.wantErr)
			}
			session.disconnect()
		})
	}
}
