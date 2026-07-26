package domain

import (
	"sync"
)

type CommandType string

const (
	CommandPrepare CommandType = "prepare"
	CommandStart   CommandType = "start"
	CommandCancel  CommandType = "cancel"
)

func (commandType CommandType) Validate(field string) error {
	switch commandType {
	case CommandPrepare, CommandStart, CommandCancel:
		return nil
	default:
		return invalid("invalid_command_type", field, "must be prepare, start, or cancel")
	}
}

// Command is the complete replay identity sent from controller to agent. Payload
// content is deliberately absent: only its digest participates in replay checks.
type Command struct {
	ID              CommandID
	ControllerEpoch ControllerEpoch
	ExecutionID     ExecutionID
	ExpectedState   ExecutionState
	PayloadDigest   string
}

func (c Command) Validate() error {
	if err := required(string(c.ID), "command.id"); err != nil {
		return err
	}
	if err := c.ControllerEpoch.Validate(); err != nil {
		return err
	}
	if err := required(string(c.ExecutionID), "command.execution_id"); err != nil {
		return err
	}
	if err := c.ExpectedState.Validate("command.expected_state"); err != nil {
		return err
	}
	if err := required(c.PayloadDigest, "command.payload_digest"); err != nil {
		return err
	}
	if len(c.PayloadDigest) != 64 {
		return invalid("invalid_payload_digest", "command.payload_digest", "must be a SHA-256 hex digest")
	}
	for _, character := range c.PayloadDigest {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return invalid("invalid_payload_digest", "command.payload_digest", "must be a lowercase SHA-256 hex digest")
		}
	}
	return nil
}

// CommandReplay is the agent-journal in-memory contract. A store persists it, but
// every adapter receives the same fail-closed replay decision from this type.
type CommandReplay struct {
	mu       sync.Mutex
	commands map[CommandID]Command
}

func NewCommandReplay() *CommandReplay {
	return &CommandReplay{commands: make(map[CommandID]Command)}
}

// Record reports replayed=true only for the exact same command. A reused command ID
// with a different payload must never execute because it could start another job.
func (r *CommandReplay) Record(command Command) (replayed bool, err error) {
	if err := command.Validate(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, found := r.commands[command.ID]
	if !found {
		r.commands[command.ID] = command
		return false, nil
	}
	if existing.PayloadDigest != command.PayloadDigest {
		return false, invalid("command_payload_mismatch", "command.payload_digest", "does not match the command ID already recorded")
	}
	if existing != command {
		return false, invalid("command_replay_mismatch", "command", "does not match the command ID already recorded")
	}
	return true, nil
}
