package nodectl

import (
	"encoding/json"
	"time"
)

// Client is the desktop-side half of the contract. Every surface (CLI, tray,
// launcher) goes through it, so the local protocol, peer refusal handling, and
// degraded-state semantics have exactly one implementation.
type Client struct {
	StateDirectory string
	Source         Source
	Timeout        time.Duration
}

func (client Client) Status() (Status, error) {
	return client.call(OperationStatus)
}

func (client Client) Pause() (Status, error) {
	return client.call(OperationPause)
}

func (client Client) Resume() (Status, error) {
	return client.call(OperationResume)
}

func (client Client) call(operation Operation) (Status, error) {
	source := client.Source
	if source == "" {
		source = SourceCLI
	}
	if err := source.Validate(); err != nil {
		return Status{}, &Error{Class: ErrorClassInvalidRequest, Message: err.Error()}
	}
	path, err := EndpointPath(client.StateDirectory)
	if err != nil {
		return Status{}, &Error{Class: errorClassFor(err), Message: err.Error()}
	}
	connection, err := dial(path)
	if err != nil {
		return Status{}, &Error{Class: errorClassFor(err), Message: err.Error()}
	}
	defer connection.Close()
	timeout := client.Timeout
	if timeout <= 0 {
		timeout = RequestTimeout
	}
	_ = connection.SetDeadline(time.Now().Add(timeout))

	payload, err := json.Marshal(Request{
		ProtocolVersion: ProtocolVersion,
		Operation:       operation,
		Source:          source,
	})
	if err != nil {
		return Status{}, &Error{Class: ErrorClassInvalidRequest, Message: err.Error()}
	}
	if _, err := connection.Write(append(payload, '\n')); err != nil {
		return Status{}, &Error{Class: ErrorClassEndpointUnavailable, Message: err.Error()}
	}
	frame, err := readFrame(connection)
	if err != nil {
		return Status{}, &Error{Class: ErrorClassEndpointUnavailable, Message: err.Error()}
	}
	var response Response
	if err := decodeStrict(frame, &response); err != nil {
		return Status{}, &Error{Class: ErrorClassInvalidRequest, Message: "agent response is unreadable"}
	}
	if response.ProtocolVersion != ProtocolVersion {
		return Status{}, &Error{
			Class:   ErrorClassProtocolMismatch,
			Message: "agent speaks a different node control protocol version",
		}
	}
	if !response.OK || response.Status == nil {
		class := response.ErrorClass
		if class == "" {
			class = ErrorClassAgentDegraded
		}
		message := response.Message
		if message == "" {
			message = ErrAgentDegraded.Error()
		}
		return Status{}, &Error{Class: class, Message: message}
	}
	return *response.Status, nil
}
