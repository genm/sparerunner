package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/genm/tewake/internal/domain"
)

type ExecutionErrorCode = domain.ExecutionErrorCode

const (
	ExecutionErrorNone            = domain.ExecutionErrorNone
	ExecutionErrorConflict        = domain.ExecutionErrorConflict
	ExecutionErrorReconciliation  = domain.ExecutionErrorReconciliation
	ExecutionErrorQuarantined     = domain.ExecutionErrorQuarantined
	ExecutionErrorCleanup         = domain.ExecutionErrorCleanup
	ExecutionErrorStart           = domain.ExecutionErrorStart
	ExecutionErrorPlatform        = domain.ExecutionErrorPlatform
	ExecutionErrorJournal         = domain.ExecutionErrorJournal
	ExecutionErrorCommandRejected = domain.ExecutionErrorCommandRejected
)

// ExecutionUpdate is safe to persist and log. It contains only classified state;
// raw runner, workspace, JIT, and process errors never cross this boundary.
type ExecutionUpdate struct {
	NodeID      domain.NodeID         `json:"nodeId"`
	CommandID   domain.CommandID      `json:"commandId"`
	ExecutionID domain.ExecutionID    `json:"executionId"`
	State       domain.ExecutionState `json:"state"`
	Replayed    bool                  `json:"replayed"`
	ErrorCode   ExecutionErrorCode    `json:"errorCode,omitempty"`
}

func (update ExecutionUpdate) Validate() error {
	if update.NodeID == "" || update.CommandID == "" || update.ExecutionID == "" {
		return ErrInvalidCommand
	}
	if err := domain.ValidateExecutionResult(update.State, update.ErrorCode, "execution_update"); err != nil {
		return ErrInvalidCommand
	}
	return nil
}

func EncodeExecutionUpdate(update ExecutionUpdate) (json.RawMessage, error) {
	if err := update.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(update)
	if err != nil {
		return nil, ErrInvalidCommand
	}
	return payload, nil
}

func DecodeExecutionUpdate(payload []byte) (ExecutionUpdate, error) {
	if len(payload) == 0 || int64(len(payload)) > MaxEnvelopeBytes || rejectDuplicateJSONKeys(payload) != nil {
		return ExecutionUpdate{}, ErrInvalidCommand
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var update ExecutionUpdate
	if err := decoder.Decode(&update); err != nil {
		return ExecutionUpdate{}, ErrInvalidCommand
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ExecutionUpdate{}, ErrInvalidCommand
	}
	if err := update.Validate(); err != nil {
		return ExecutionUpdate{}, err
	}
	return update, nil
}
