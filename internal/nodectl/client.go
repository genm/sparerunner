package nodectl

import (
	"encoding/json"
	"time"

	"github.com/genm/sparerunner/internal/domain"
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
	return client.call(OperationStatus, "")
}

func (client Client) Pause() (Status, error) {
	return client.call(OperationPause, "")
}

func (client Client) Resume() (Status, error) {
	return client.call(OperationResume, "")
}

// Targets reads the same status document as Status. It is a separate method so
// a caller's intent is explicit at the call site.
func (client Client) Targets() (Status, error) {
	return client.call(OperationTargets, "")
}

// Exclude withdraws one GitHub Target from this computer. It is subtractive, so
// it is effective the instant the agent records it durably.
func (client Client) Exclude(targetID domain.TargetID) (Status, error) {
	return client.call(OperationExclude, targetID)
}

// Include re-allows one GitHub Target. It is additive, so it stays pending in
// the returned document until the controller echoes its adoption.
func (client Client) Include(targetID domain.TargetID) (Status, error) {
	return client.call(OperationInclude, targetID)
}

func (client Client) call(operation Operation, targetID domain.TargetID) (Status, error) {
	source := client.Source
	if source == "" {
		source = SourceCLI
	}
	if err := source.Validate(); err != nil {
		return Status{}, &Error{Class: ErrorClassInvalidRequest, Message: err.Error()}
	}
	request := Request{
		ProtocolVersion: ProtocolVersion,
		Operation:       operation,
		Source:          source,
		TargetID:        targetID,
	}
	// Validating before dialing keeps a malformed identifier from ever leaving
	// this process and gives the caller the same class the server would.
	if err := request.Validate(); err != nil {
		return Status{}, &Error{Class: errorClassFor(err), Message: err.Error()}
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

	payload, err := json.Marshal(request)
	if err != nil {
		return Status{}, &Error{Class: ErrorClassInvalidRequest, Message: err.Error()}
	}
	writeErr := func() error {
		_, err := connection.Write(append(payload, '\n'))
		return err
	}()
	// The server rejects an unauthorized peer before ever reading the request,
	// so on a fast machine our write can hit the already-closed socket. The
	// rejection verdict is still buffered on the connection; a failed write
	// must therefore fall through to the read and surface that verdict instead
	// of masking it as an unavailable endpoint.
	frame, err := readFrame(connection)
	if err != nil {
		// Both phases fail closed the same way, but they have different causes,
		// so the message names the phase. A send failure means the agent never
		// took the request; a receive failure means it never answered one it may
		// well have acted on.
		if writeErr != nil {
			return Status{}, &Error{
				Class:   ErrorClassEndpointUnavailable,
				Message: "sending the request failed: " + writeErr.Error(),
			}
		}
		return Status{}, &Error{
			Class:   ErrorClassEndpointUnavailable,
			Message: "reading the response failed: " + err.Error(),
		}
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
