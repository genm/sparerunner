package transport

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

// FuzzDecodeEnvelope explores the first bytes an enrolled peer can put on the
// agent WebSocket. Everything downstream of DecodeEnvelope trusts that the
// envelope was classified correctly, so three properties are asserted.
//
// Rejection stays inside the three declared error classes, because an
// unclassified error means a decoding path escaped the fail-closed contract.
//
// Anything the controller accepts must also be something the controller can
// send. An envelope that decodes but does not marshal would be a value the two
// directions of the protocol disagree about.
//
// Marshaling is a fixpoint: re-decoding and re-marshaling an accepted envelope
// returns identical bytes and an identical value. Duplicate-key rejection and
// the strict four-field shape only hold if normalization cannot drift, and a
// drift here would let two different wire forms produce the same accepted
// message.
func FuzzDecodeEnvelope(f *testing.F) {
	seeds := [][]byte{
		nil,
		[]byte("{"),
		[]byte(`{"protocolVersion":1,"messageId":"m-1","type":"hello","payload":{"nodeId":"node-1"}}`),
		[]byte(`{"protocolVersion":1,"messageId":"m-1","type":"heartbeat","payload":{"sequence":3}}`),
		[]byte(`{ "payload" : { "a" : [ 1 , 2 ] } , "type":"log","messageId":"m-1","protocolVersion":1}`),
		[]byte(`{"protocolVersion":2,"messageId":"m-1","type":"hello","payload":{}}`),
		[]byte(`{"protocolVersion":1,"messageId":"m-1","type":"nope","payload":{}}`),
		[]byte(`{"protocolVersion":1,"messageId":"m-1","messageId":"m-2","type":"ack","payload":{}}`),
		[]byte(`{"protocolVersion":1,"messageId":"m-1","type":"ack","payload":[]}`),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, payload []byte) {
		envelope, err := DecodeEnvelope(payload)
		if err != nil {
			if !errors.Is(err, ErrInvalidEnvelope) &&
				!errors.Is(err, ErrProtocolVersion) &&
				!errors.Is(err, ErrUnsupportedType) {
				t.Fatalf("envelope rejected with an undeclared error class: %v", err)
			}
			return
		}

		marshaled, err := MarshalEnvelope(envelope)
		if err != nil {
			t.Fatalf("accepted envelope cannot be sent back: %v", err)
		}
		normalized, err := DecodeEnvelope(marshaled)
		if err != nil {
			t.Fatalf("marshaled envelope is no longer accepted: %v", err)
		}
		remarshaled, err := MarshalEnvelope(normalized)
		if err != nil {
			t.Fatalf("normalized envelope cannot be sent back: %v", err)
		}
		if !bytes.Equal(marshaled, remarshaled) {
			t.Fatalf("marshaling is not a fixpoint: %q then %q", marshaled, remarshaled)
		}
		reNormalized, err := DecodeEnvelope(remarshaled)
		if err != nil {
			t.Fatalf("re-marshaled envelope is no longer accepted: %v", err)
		}
		if !reflect.DeepEqual(normalized, reNormalized) {
			t.Fatalf("decoding is not a fixpoint: %+v then %+v", normalized, reNormalized)
		}
	})
}
