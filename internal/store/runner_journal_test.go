package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

func TestAgentRunnerJournalPersistsCASAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "agent.db")
	store, err := OpenAgent(ctx, path, Options{Now: func() time.Time { return time.Unix(300, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	journal := store.RunnerJournal()
	initial := testRunnerPreparingRecord("execution-journal-restart")
	created, won, err := journal.Create(ctx, strings.Repeat("a", 32), initial)
	if err != nil || !won || created.Revision != 1 {
		t.Fatalf("create = (%+v, %t, %v)", created, won, err)
	}
	duplicate, won, err := journal.Create(ctx, strings.Repeat("b", 32), initial)
	if err != nil || won || !reflect.DeepEqual(duplicate, created) {
		t.Fatalf("duplicate create = (%+v, %t, %v)", duplicate, won, err)
	}
	next := initial
	next.State = runner.StateFailed
	updated, won, err := journal.CompareAndSwap(ctx, initial.ExecutionID, created.Revision, strings.Repeat("c", 32), next)
	if err != nil || !won || updated.Revision != 2 || updated.MutationToken != strings.Repeat("c", 32) {
		t.Fatalf("compare and swap = (%+v, %t, %v)", updated, won, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenAgent(ctx, path, Options{Now: func() time.Time { return time.Unix(300, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, found, err := reopened.RunnerJournal().Load(ctx, initial.ExecutionID)
	if err != nil || !found || !reflect.DeepEqual(loaded, updated) {
		t.Fatalf("load after restart = (%+v, %t, %v)", loaded, found, err)
	}
	stale, won, err := reopened.RunnerJournal().CompareAndSwap(ctx, initial.ExecutionID, created.Revision, strings.Repeat("d", 32), initial)
	if err != nil || won || !reflect.DeepEqual(stale, updated) {
		t.Fatalf("stale compare and swap = (%+v, %t, %v)", stale, won, err)
	}
	records, err := reopened.RunnerJournalRecords(ctx)
	if err != nil || len(records) != 1 || !reflect.DeepEqual(records[0], updated) {
		t.Fatalf("startup records = (%+v, %v)", records, err)
	}
}

func TestAgentRunnerJournalConflictAndInvalidPersistenceFailClosed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "agent.db")
	store, err := OpenAgent(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	journal := store.RunnerJournal()
	initial := testRunnerPreparingRecord("execution-journal-conflict")
	created, won, err := journal.Create(ctx, strings.Repeat("1", 32), initial)
	if err != nil || !won {
		t.Fatalf("create = (%+v, %t, %v)", created, won, err)
	}

	invalid := initial
	invalid.State = runner.StatePrepared
	if _, _, err := journal.CompareAndSwap(ctx, initial.ExecutionID, created.Revision, strings.Repeat("2", 32), invalid); err == nil {
		t.Fatal("invalid lifecycle record accepted")
	}

	// This emulates disk corruption that passes the SQL type checks but violates
	// the lifecycle invariant. The adapter must not treat it as an absent record.
	if _, err := store.db.Exec(`UPDATE runner_journal_records SET workspace_backend = 'workspace', workspace_owner_id = '' WHERE execution_id = ?`, initial.ExecutionID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.Load(ctx, initial.ExecutionID); err == nil {
		t.Fatal("semantically invalid persisted record was accepted")
	}
	if _, _, err := journal.CompareAndSwap(ctx, initial.ExecutionID, created.Revision, strings.Repeat("3", 32), initial); err == nil {
		t.Fatal("CAS overwrote semantically invalid persisted record")
	}
}

func TestAgentRunnerJournalConcurrentCASHasOneWinner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "agent.db")
	first, err := OpenAgent(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenAgent(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	initial := testRunnerPreparingRecord("execution-journal-concurrent")
	created, won, err := first.RunnerJournal().Create(ctx, strings.Repeat("4", 32), initial)
	if err != nil || !won {
		t.Fatalf("create = (%+v, %t, %v)", created, won, err)
	}
	next := initial
	next.State = runner.StateFailed

	start := make(chan struct{})
	results := make(chan bool, 2)
	var group sync.WaitGroup
	for _, journal := range []runner.Journal{first.RunnerJournal(), second.RunnerJournal()} {
		group.Add(1)
		go func(journal runner.Journal) {
			defer group.Done()
			<-start
			_, swapped, swapErr := journal.CompareAndSwap(ctx, initial.ExecutionID, created.Revision, strings.Repeat("5", 32), next)
			if swapErr != nil {
				t.Errorf("compare and swap: %v", swapErr)
			}
			results <- swapped
		}(journal)
	}
	close(start)
	group.Wait()
	close(results)
	winners := 0
	for swapped := range results {
		if swapped {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("CAS winners = %d", winners)
	}
	final, found, err := first.RunnerJournal().Load(ctx, initial.ExecutionID)
	if err != nil || !found || final.Revision != 2 || final.State != runner.StateFailed {
		t.Fatalf("final record = (%+v, %t, %v)", final, found, err)
	}
}

func TestAgentRunnerJournalSchemaExcludesJITMaterial(t *testing.T) {
	store := openAgentForRunnerJournal(t, "agent.db")
	defer store.Close()
	columns := tableColumns(t, store.db, "runner_journal_records")
	for _, forbidden := range []string{"jit_body", "jit_config", "argv", "args", "environment", "env"} {
		for _, column := range columns {
			if column == forbidden {
				t.Fatalf("secret-bearing journal column %q exists", forbidden)
			}
		}
	}
	if !containsString(columns, "jit_digest") {
		t.Fatalf("journal columns = %v, missing jit_digest", columns)
	}
}

func TestAgentRunnerJournalSurvivesProcessKill(t *testing.T) {
	const environment = "TEWAKE_AGENT_RUNNER_JOURNAL_CRASH_PATH"
	if path := os.Getenv(environment); path != "" {
		store, err := OpenAgent(context.Background(), path, Options{})
		if err != nil {
			crashHelperExit("open agent store: %v", err)
		}
		if _, _, err := store.RunnerJournal().Create(context.Background(), strings.Repeat("e", 32), testRunnerPreparingRecord("execution-journal-crash")); err != nil {
			crashHelperExit("create journal record: %v", err)
		}
		fmt.Println("crash-ready")
		select {}
	}

	path := filepath.Join(privateTestDir(t), "agent.db")
	runAndKillStoreHelper(t, "TestAgentRunnerJournalSurvivesProcessKill", environment, path)
	reopened, err := OpenAgent(context.Background(), path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, found, err := reopened.RunnerJournal().Load(context.Background(), "execution-journal-crash")
	if err != nil || !found || loaded.Revision != 1 || loaded.MutationToken != strings.Repeat("e", 32) {
		t.Fatalf("load after process kill = (%+v, %t, %v)", loaded, found, err)
	}
}

func openAgentForRunnerJournal(t *testing.T, name string) *AgentStore {
	t.Helper()
	store, err := OpenAgent(context.Background(), filepath.Join(privateTestDir(t), name), Options{})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testRunnerPreparingRecord(executionID string) runner.Record {
	return runner.Record{
		ExecutionID: executionID,
		SpecDigest:  strings.Repeat("f", 64),
		State:       runner.StatePreparing,
		RootName:    testRunnerRootName(executionID),
	}
}

func testRunnerRootName(executionID string) string {
	sum := sha256.Sum256([]byte(executionID))
	return hex.EncodeToString(sum[:])
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
