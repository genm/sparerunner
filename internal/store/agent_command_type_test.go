package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
)

func TestTypedAgentCommandPersistsExactRecoveryAuthority(t *testing.T) {
	ctx := context.Background()
	agent, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	accepted := AcceptedAgentCommand{
		Type: domain.CommandStart,
		Command: domain.Command{
			ID:              "start-recovery-command",
			ControllerEpoch: 1,
			ExecutionID:     "execution-recovery-command",
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   strings.Repeat("a", 64),
		},
	}
	if replayed, err := agent.RecordTypedCommand(ctx, accepted); err != nil || replayed {
		t.Fatalf("first RecordTypedCommand replayed=%v err=%v", replayed, err)
	}
	if replayed, err := agent.LookupTypedCommand(ctx, accepted); err != nil || !replayed {
		t.Fatalf("LookupTypedCommand replayed=%v err=%v", replayed, err)
	}
	if replayed, err := agent.RecordTypedCommand(ctx, accepted); err != nil || !replayed {
		t.Fatalf("replayed RecordTypedCommand replayed=%v err=%v", replayed, err)
	}
	commands, err := agent.AcceptedAgentCommands(ctx)
	if err != nil || len(commands) != 1 || commands[0] != accepted {
		t.Fatalf("AcceptedAgentCommands = %#v, %v", commands, err)
	}

	mismatch := accepted
	mismatch.Type = domain.CommandCancel
	if _, err := agent.LookupTypedCommand(ctx, mismatch); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("type mismatch error = %v", err)
	}
}

func TestTypedAgentCommandNeverUpgradesUntypedReplayAuthority(t *testing.T) {
	ctx := context.Background()
	agent, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	command := domain.Command{
		ID:              "legacy-untyped-command",
		ControllerEpoch: 1,
		ExecutionID:     "legacy-untyped-execution",
		ExpectedState:   domain.ExecutionPreparing,
		PayloadDigest:   strings.Repeat("b", 64),
	}
	if replayed, err := agent.RecordCommand(ctx, command); err != nil || replayed {
		t.Fatalf("RecordCommand replayed=%v err=%v", replayed, err)
	}
	if _, err := agent.RecordTypedCommand(ctx, AcceptedAgentCommand{
		Type:    domain.CommandStart,
		Command: command,
	}); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("untyped upgrade error = %v", err)
	}
	commands, err := agent.AcceptedAgentCommands(ctx)
	if err != nil || len(commands) != 0 {
		t.Fatalf("AcceptedAgentCommands = %#v, %v", commands, err)
	}
}
