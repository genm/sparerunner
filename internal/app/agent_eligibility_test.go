package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/transport"
)

// TestAcknowledgeHeartbeatEncodesEligibleTargetsPresenceDistinctFromAbsence
// covers the controller-side half of the fix: a plain omitempty slice cannot
// tell "the lookup failed or there is no consumer" apart from "the lookup
// succeeded and confirmed zero eligible targets", so acknowledgeHeartbeat
// must encode the two states differently on the wire.
func TestAcknowledgeHeartbeatEncodesEligibleTargetsPresenceDistinctFromAbsence(t *testing.T) {
	targets := []transport.EligibleTarget{{
		TargetID:     "target-1",
		ScopeKind:    domain.TargetRepository,
		Scope:        "owner/repo",
		ScaleSetName: "scale-set",
	}}
	tests := []struct {
		name       string
		eligible   AgentEligibilityConsumer
		wantField  bool
		wantEmpty  bool
		wantLength int
	}{
		{
			name:      "no consumer",
			eligible:  nil,
			wantField: false,
		},
		{
			name: "lookup failed",
			eligible: AgentEligibilityConsumerFunc(func(
				context.Context, domain.NodeID, domain.OperatingSystem, domain.Architecture,
			) ([]transport.EligibleTarget, error) {
				return nil, errors.New("configuration unavailable")
			}),
			wantField: false,
		},
		{
			name: "lookup succeeded with zero targets",
			eligible: AgentEligibilityConsumerFunc(func(
				context.Context, domain.NodeID, domain.OperatingSystem, domain.Architecture,
			) ([]transport.EligibleTarget, error) {
				return nil, nil
			}),
			wantField: true,
			wantEmpty: true,
		},
		{
			name: "lookup succeeded with targets",
			eligible: AgentEligibilityConsumerFunc(func(
				context.Context, domain.NodeID, domain.OperatingSystem, domain.Architecture,
			) ([]transport.EligibleTarget, error) {
				return targets, nil
			}),
			wantField:  true,
			wantLength: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumers := acceptingAgentConsumers(newRecordingUpdateConsumer())
			consumers.Eligibility = test.eligible
			broker := NewAgentBroker(1, consumers)
			session, serveResult := startReadyBrokerSession(t, broker, "node-1")
			session.send(brokerEnvelope(t, "heartbeat-1", transport.MessageHeartbeat, transport.AgentHeartbeat{
				NodeID:            "node-1",
				NativeRunnerReady: true,
			}))
			envelope := receiveBrokerWrite(t, session)
			if envelope.Type != transport.MessageAck {
				t.Fatalf("message type = %s, want ack", envelope.Type)
			}
			hasField := strings.Contains(string(envelope.Payload), `"eligibleTargets"`)
			if hasField != test.wantField {
				t.Fatalf("payload = %s, wantField = %t", envelope.Payload, test.wantField)
			}
			if test.wantField {
				var decoded struct {
					EligibleTargets []transport.EligibleTarget `json:"eligibleTargets"`
				}
				if err := json.Unmarshal(envelope.Payload, &decoded); err != nil {
					t.Fatal(err)
				}
				if decoded.EligibleTargets == nil {
					t.Fatal("decoded eligibleTargets is nil despite the field being present")
				}
				if test.wantEmpty && len(decoded.EligibleTargets) != 0 {
					t.Fatalf("eligibleTargets = %#v, want empty", decoded.EligibleTargets)
				}
				if !test.wantEmpty && len(decoded.EligibleTargets) != test.wantLength {
					t.Fatalf("eligibleTargets length = %d, want %d", len(decoded.EligibleTargets), test.wantLength)
				}
			}
			session.disconnect()
			if err := <-serveResult; !errors.Is(err, ErrAgentDisconnected) {
				t.Fatalf("serve result = %v", err)
			}
		})
	}
}

// heartbeatAckServer runs a minimal fake controller over one websocket
// connection: it completes Hello/Snapshot with a plain ack, then acknowledges
// exactly one heartbeat with a caller-supplied raw eligibleTargets JSON
// fragment (or none at all) so the test can drive the agent through the
// absent/empty/populated/invalid wire states this PR must handle.
func heartbeatAckServer(t *testing.T, eligibleTargetsJSON string, hasField bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		ctx := request.Context()
		for _, expected := range []transport.MessageType{transport.MessageHello, transport.MessageSnapshot} {
			envelope, readErr := transport.ReadEnvelope(ctx, connection)
			if readErr != nil || envelope.Type != expected {
				return
			}
			if writeAgentAck(ctx, connection, envelope.MessageID) != nil {
				return
			}
		}
		envelope, readErr := transport.ReadEnvelope(ctx, connection)
		if readErr != nil || envelope.Type != transport.MessageHeartbeat {
			return
		}
		body := `"messageId":"` + envelope.MessageID + `"`
		if hasField {
			body += `,"eligibleTargets":` + eligibleTargetsJSON
		}
		payload := json.RawMessage("{" + body + "}")
		ackID, err := randomMessageID()
		if err != nil {
			return
		}
		_ = transport.WriteEnvelope(ctx, connection, transport.Envelope{
			ProtocolVersion: transport.ProtocolVersion,
			MessageID:       ackID,
			Type:            transport.MessageAck,
			Payload:         payload,
		})
		<-ctx.Done()
	}))
}

func dialAgentTestServer(t *testing.T, server *httptest.Server) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

// TestAgentSessionHeartbeatAckEligibleTargetsWireStates covers the agent-side
// half of the fix: decodeStrictJSON in runAgentSessionActor previously used
// an anonymous struct with only MessageID, so any ack carrying a non-empty
// eligibleTargets list made decodeStrictJSON fail with unknown-field
// rejection and killed the whole session.
func TestAgentSessionHeartbeatAckEligibleTargetsWireStates(t *testing.T) {
	tests := []struct {
		name           string
		hasField       bool
		json           string
		wantSessionErr bool
		wantEligible   []domain.TargetID
	}{
		{
			name:     "absent keeps previously known list",
			hasField: false,
		},
		{
			name:         "empty confirms zero eligible targets",
			hasField:     true,
			json:         `[]`,
			wantEligible: []domain.TargetID{},
		},
		{
			name:     "populated list is accepted",
			hasField: true,
			json: `[{"targetId":"target-1","scopeKind":"repository",` +
				`"scope":"owner/repo","scaleSetName":"scale-set","excluded":false}]`,
			wantEligible: []domain.TargetID{"target-1"},
		},
		{
			name:     "duplicate target ID fails the session closed",
			hasField: true,
			json: `[{"targetId":"target-1","scopeKind":"repository",` +
				`"scope":"owner/repo","scaleSetName":"scale-set","excluded":false},` +
				`{"targetId":"target-1","scopeKind":"repository",` +
				`"scope":"owner/repo","scaleSetName":"scale-set","excluded":false}]`,
			wantSessionErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agentStore := openAgentCommandStore(t)
			defer agentStore.Close()
			availability, err := newAgentAvailability(context.Background(), agentStore, "node-1", false)
			if err != nil {
				t.Fatal(err)
			}

			server := heartbeatAckServer(t, test.json, test.hasField)
			defer server.Close()
			connection := dialAgentTestServer(t, server)
			defer connection.CloseNow()

			sessionErr := runAgentSessionWithOptions(
				context.Background(),
				connection,
				&AgentState{NodeID: "node-1", Store: agentStore},
				nil,
				agentSessionOptions{
					heartbeatInterval: 20 * time.Millisecond,
					readinessTimeout:  5 * time.Millisecond,
					availability:      availability,
				},
			)
			if test.wantSessionErr {
				if sessionErr == nil || !strings.Contains(sessionErr.Error(), "controller acknowledgement mismatch") {
					t.Fatalf("session error = %v, want acknowledgement mismatch", sessionErr)
				}
				return
			}
			// A well-formed ack ends the session only via server-side context
			// cancellation (io error), never a protocol failure.
			if sessionErr == nil {
				t.Fatal("session ended without error")
			}
			if strings.Contains(sessionErr.Error(), "controller acknowledgement mismatch") {
				t.Fatalf("well-formed ack rejected: %v", sessionErr)
			}
			status, err := availability.Status(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if test.wantEligible == nil {
				if status.EligibleTargets != nil {
					t.Fatalf("absent ack changed eligible targets: %#v", status.Targets())
				}
				return
			}
			// EligibleTargets must itself be non-nil here: a confirmed-empty
			// heartbeat ack is a distinct wire value from never-reported, and a
			// nil pointer would collapse the two into the same absent JSON field.
			if status.EligibleTargets == nil {
				t.Fatalf("confirmed eligible targets were reported as absent, want present: wantEligible=%#v", test.wantEligible)
			}
			targets := status.Targets()
			if len(targets) != len(test.wantEligible) {
				t.Fatalf("eligible targets = %#v, want %#v", targets, test.wantEligible)
			}
			for index, wantID := range test.wantEligible {
				if string(targets[index].TargetID) != string(wantID) {
					t.Fatalf("eligible targets = %#v, want %#v", targets, test.wantEligible)
				}
			}
		})
	}
}
