// Package transport owns the authenticated agent WebSocket protocol.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/coder/websocket"
)

const ProtocolVersion = 1

// GitHubAdapterResponseLimit is the GitHub adapter's upstream 1 MiB response
// contract. The agent envelope shares this bounded transport reader so a peer
// cannot make controller memory exceed the only established upstream boundary.
const GitHubAdapterResponseLimit int64 = 1 << 20

// MaxEnvelopeBytes reserves one further upstream-sized payload for the JSON
// envelope metadata and structured payload representation. This is a transport
// memory boundary derived from the adapter cap, not a product message quota.
const MaxEnvelopeBytes int64 = 2 * GitHubAdapterResponseLimit

var (
	ErrProtocolVersion = errors.New("unsupported transport protocol version")
	ErrInvalidEnvelope = errors.New("invalid transport envelope")
	ErrUnsupportedType = errors.New("unsupported transport message type")
)

type MessageType string

const (
	MessageHello           MessageType = "hello"
	MessageSnapshot        MessageType = "snapshot"
	MessageHeartbeat       MessageType = "heartbeat"
	MessagePrepare         MessageType = "prepare"
	MessageStart           MessageType = "start"
	MessageCancel          MessageType = "cancel"
	MessageExecutionUpdate MessageType = "execution_update"
	MessageLog             MessageType = "log"
	MessageAck             MessageType = "ack"
)

type Envelope struct {
	ProtocolVersion int             `json:"protocolVersion"`
	MessageID       string          `json:"messageId"`
	Type            MessageType     `json:"type"`
	Payload         json.RawMessage `json:"payload"`
}

func (envelope Envelope) Validate() error {
	if envelope.ProtocolVersion != ProtocolVersion {
		return ErrProtocolVersion
	}
	if envelope.MessageID == "" || len(envelope.Payload) == 0 || int64(len(envelope.Payload)) > MaxEnvelopeBytes || !json.Valid(envelope.Payload) || rejectDuplicateJSONKeys(envelope.Payload) != nil {
		return ErrInvalidEnvelope
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(envelope.Payload, &object) != nil || object == nil {
		return ErrInvalidEnvelope
	}
	switch envelope.Type {
	case MessageHello, MessageSnapshot, MessageHeartbeat, MessagePrepare, MessageStart, MessageCancel, MessageExecutionUpdate, MessageLog, MessageAck:
		return nil
	default:
		return ErrUnsupportedType
	}
}

func MarshalEnvelope(envelope Envelope) ([]byte, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(envelope)
	if err != nil || int64(len(payload)) > MaxEnvelopeBytes {
		return nil, ErrInvalidEnvelope
	}
	return payload, nil
}

func DecodeEnvelope(payload []byte) (Envelope, error) {
	if int64(len(payload)) > MaxEnvelopeBytes {
		return Envelope{}, ErrInvalidEnvelope
	}
	if err := rejectDuplicateJSONKeys(payload); err != nil {
		return Envelope{}, ErrInvalidEnvelope
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return Envelope{}, ErrInvalidEnvelope
	}
	if len(fields) != 4 {
		return Envelope{}, ErrInvalidEnvelope
	}
	for key := range fields {
		switch key {
		case "protocolVersion", "messageId", "type", "payload":
		default:
			return Envelope{}, ErrInvalidEnvelope
		}
	}
	var envelope Envelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return Envelope{}, ErrInvalidEnvelope
	}
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func ReadEnvelope(ctx context.Context, connection *websocket.Conn) (Envelope, error) {
	messageType, reader, err := connection.Reader(ctx)
	if err != nil {
		return Envelope{}, err
	}
	if messageType != websocket.MessageBinary {
		return Envelope{}, ErrInvalidEnvelope
	}
	payload, err := io.ReadAll(io.LimitReader(reader, MaxEnvelopeBytes+1))
	if err != nil || int64(len(payload)) > MaxEnvelopeBytes {
		return Envelope{}, ErrInvalidEnvelope
	}
	return DecodeEnvelope(payload)
}

func WriteEnvelope(ctx context.Context, connection *websocket.Conn, envelope Envelope) error {
	payload, err := MarshalEnvelope(envelope)
	if err != nil {
		return err
	}
	return connection.Write(ctx, websocket.MessageBinary, payload)
}

// rejectDuplicateJSONKeys parses every object rather than relying on
// encoding/json's last-key-wins behavior. It also requires a single JSON value.
func rejectDuplicateJSONKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return keyErr
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object member is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		end, endErr := decoder.Token()
		if endErr != nil || end != json.Delim('}') {
			return errors.New("unterminated object")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		end, endErr := decoder.Token()
		if endErr != nil || end != json.Delim(']') {
			return errors.New("unterminated array")
		}
	default:
		return errors.New("unexpected delimiter")
	}
	return nil
}
