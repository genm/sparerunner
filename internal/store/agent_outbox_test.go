package store

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/runner"
)

func TestExecutionUpdateOutboxSurvivesRestartAndDeletesOnlyAfterAck(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "agent-outbox.db")
	agent, err := OpenAgent(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	update := ExecutionUpdateRecord{
		NodeID:      "node-1",
		CommandID:   "command-1",
		ExecutionID: "execution-1",
		State:       domain.ExecutionRunning,
	}
	messageID := strings.Repeat("a", 64)
	pending, created, err := agent.QueueExecutionUpdate(ctx, messageID, update)
	if err != nil || !created {
		t.Fatalf("initial queue = (%#v, %t, %v)", pending, created, err)
	}
	replayed, created, err := agent.QueueExecutionUpdate(ctx, messageID, update)
	if err != nil || created || replayed != pending {
		t.Fatalf("exact queue replay = (%#v, %t, %v)", replayed, created, err)
	}
	changed := update
	changed.State = domain.ExecutionCleaning
	if _, _, err := agent.QueueExecutionUpdate(ctx, messageID, changed); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("message collision error = %v", err)
	}
	before, err := agent.PendingExecutionUpdates(ctx)
	if err != nil || !reflect.DeepEqual(before, []PendingExecutionUpdate{pending}) {
		t.Fatalf("pending before restart = %#v, %v", before, err)
	}
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenAgent(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.PendingExecutionUpdates(ctx)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("pending after restart = %#v, %v", after, err)
	}
	if err := reopened.AcknowledgeExecutionUpdate(ctx, pending.MessageID); err != nil {
		t.Fatal(err)
	}
	empty, err := reopened.PendingExecutionUpdates(ctx)
	if err != nil || len(empty) != 0 {
		t.Fatalf("pending after acknowledgement = %#v, %v", empty, err)
	}
	if err := reopened.AcknowledgeExecutionUpdate(ctx, pending.MessageID); !errors.Is(err, ErrExecutionUpdateNotFound) {
		t.Fatalf("duplicate acknowledgement = %v", err)
	}
}

func TestExecutionUpdateOutboxRejectsUnclassifiedError(t *testing.T) {
	ctx := context.Background()
	agent, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent-outbox-invalid.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	update := ExecutionUpdateRecord{
		NodeID:      "node-1",
		CommandID:   "command-1",
		ExecutionID: "execution-1",
		State:       domain.ExecutionFailed,
		ErrorCode:   domain.ExecutionErrorCode("runner-output-jit-canary.example.test"),
	}
	if _, _, err := agent.QueueExecutionUpdate(ctx, strings.Repeat("b", 64), update); err == nil {
		t.Fatal("unclassified runner output entered the outbox")
	}
	valid := update
	valid.ErrorCode = domain.ExecutionErrorStart
	if _, _, err := agent.QueueExecutionUpdate(ctx, "jit-config-canary.example.test", valid); err == nil {
		t.Fatal("non-digest message ID entered the outbox")
	}
	pending, err := agent.PendingExecutionUpdates(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("invalid update persisted = %#v, %v", pending, err)
	}
}

func TestTerminalAcknowledgementPrunesOnlyCleanExecutionEvidence(t *testing.T) {
	ctx := context.Background()
	agent, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent-outbox-prune.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	executionID := domain.ExecutionID("execution-pruned-after-controller-ack")
	command := AcceptedAgentCommand{
		Type: domain.CommandPrepare,
		Command: domain.Command{
			ID:              "command-pruned-after-controller-ack",
			ControllerEpoch: 1,
			ExecutionID:     executionID,
			ExpectedState:   domain.ExecutionReserved,
			PayloadDigest:   strings.Repeat("c", 64),
		},
	}
	if replayed, err := agent.RecordTypedCommand(ctx, command); err != nil || replayed {
		t.Fatalf("record command = (%t, %v)", replayed, err)
	}
	record := testRunnerPreparingRecord(string(executionID))
	record.State = runner.StateFailed
	if _, created, err := agent.RunnerJournal().Create(ctx, strings.Repeat("d", 32), record); err != nil || !created {
		t.Fatalf("create terminal runner record = (%t, %v)", created, err)
	}
	messageID := strings.Repeat("e", 64)
	if _, _, err := agent.CommitExecutionLifecycle(ctx, ExecutionLifecycleCommit{
		Observation: Observation{ExecutionID: executionID, State: domain.ExecutionFailed},
		MessageID:   messageID,
		Update: ExecutionUpdateRecord{
			NodeID:      "node-1",
			CommandID:   command.Command.ID,
			ExecutionID: executionID,
			State:       domain.ExecutionFailed,
			ErrorCode:   domain.ExecutionErrorStart,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := agent.AcknowledgeExecutionUpdate(ctx, messageID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := agent.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Commands) != 0 || len(snapshot.Observations) != 0 ||
		len(snapshot.CleanupTombstones) != 0 {
		t.Fatalf("acknowledged clean evidence was retained: %#v", snapshot)
	}
	records, err := agent.RunnerJournalRecords(ctx)
	if err != nil || len(records) != 0 {
		t.Fatalf("acknowledged runner records = (%#v, %v)", records, err)
	}
}

func TestCleanupFailureAcknowledgementRetainsQuarantineEvidence(t *testing.T) {
	ctx := context.Background()
	agent, err := OpenAgent(ctx, filepath.Join(privateTestDir(t), "agent-outbox-quarantine.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	executionID := domain.ExecutionID("execution-quarantine-retained")
	command := AcceptedAgentCommand{
		Type: domain.CommandStart,
		Command: domain.Command{
			ID:              "command-quarantine-retained",
			ControllerEpoch: 1,
			ExecutionID:     executionID,
			ExpectedState:   domain.ExecutionPreparing,
			PayloadDigest:   strings.Repeat("f", 64),
		},
	}
	if replayed, err := agent.RecordTypedCommand(ctx, command); err != nil || replayed {
		t.Fatalf("record command = (%t, %v)", replayed, err)
	}
	record := testRunnerPreparingRecord(string(executionID))
	record.State = runner.StateCleanupFailed
	record.Tombstone = true
	if _, created, err := agent.RunnerJournal().Create(ctx, strings.Repeat("1", 32), record); err != nil || !created {
		t.Fatalf("create quarantined runner record = (%t, %v)", created, err)
	}
	messageID := strings.Repeat("2", 64)
	tombstone := CleanupTombstone{
		ExecutionID: executionID,
		FailureCode: CleanupVerificationFailed,
	}
	if _, _, err := agent.CommitExecutionLifecycle(ctx, ExecutionLifecycleCommit{
		Observation:      Observation{ExecutionID: executionID, State: domain.ExecutionCleanupFailed},
		CleanupTombstone: &tombstone,
		MessageID:        messageID,
		Update: ExecutionUpdateRecord{
			NodeID:      "node-1",
			CommandID:   command.Command.ID,
			ExecutionID: executionID,
			State:       domain.ExecutionCleanupFailed,
			ErrorCode:   domain.ExecutionErrorCleanup,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := agent.AcknowledgeExecutionUpdate(ctx, messageID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := agent.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Commands) != 1 || len(snapshot.Observations) != 1 ||
		len(snapshot.CleanupTombstones) != 1 {
		t.Fatalf("quarantine evidence was pruned: %#v", snapshot)
	}
	records, err := agent.RunnerJournalRecords(ctx)
	if err != nil || len(records) != 1 || records[0].State != runner.StateCleanupFailed {
		t.Fatalf("quarantine runner records = (%#v, %v)", records, err)
	}
}
