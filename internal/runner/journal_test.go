package runner

import (
	"context"
	"strings"
	"sync"
	"testing"
)

func TestMemoryJournalCreateAndCompareAndSwapHaveSingleWinners(t *testing.T) {
	ctx := context.Background()
	journal := NewMemoryJournal()
	initial := Record{
		ExecutionID: "journal-cas",
		SpecDigest:  strings.Repeat("a", 64),
		State:       StatePreparing,
		RootName:    executionRootName("journal-cas"),
	}
	createToken := strings.Repeat("a", 32)
	casToken := strings.Repeat("b", 32)
	created, won, err := journal.Create(ctx, createToken, initial)
	if err != nil || !won || created.Revision != 1 {
		t.Fatalf("Create = %#v, won=%v, err=%v", created, won, err)
	}
	if existing, won, err := journal.Create(ctx, strings.Repeat("c", 32), initial); err != nil || won || existing != created {
		t.Fatalf("duplicate Create = %#v, won=%v, err=%v", existing, won, err)
	}

	next := initial
	next.State = StateFailed
	results := make(chan bool, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, swapped, swapErr := journal.CompareAndSwap(ctx, initial.ExecutionID, created.Revision, casToken, next)
			if swapErr != nil {
				t.Errorf("CompareAndSwap error = %v", swapErr)
			}
			results <- swapped
		}()
	}
	group.Wait()
	close(results)
	var winners int
	for swapped := range results {
		if swapped {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("CAS winners = %d", winners)
	}
	final, found, err := journal.Load(ctx, initial.ExecutionID)
	if err != nil || !found || final.Revision != 2 || final.State != StateFailed {
		t.Fatalf("final record = %#v, found=%v, err=%v", final, found, err)
	}
}

func TestMemoryJournalRejectsInvalidCASAuthority(t *testing.T) {
	ctx := context.Background()
	journal := NewMemoryJournal()
	record := Record{ExecutionID: "journal-authority"}
	created, won, err := journal.Create(ctx, strings.Repeat("a", 32), record)
	if err != nil || !won {
		t.Fatalf("Create = %#v, won=%v, err=%v", created, won, err)
	}
	for name, testCase := range map[string]struct {
		executionID string
		revision    uint64
		next        Record
	}{
		"zero revision": {record.ExecutionID, 0, record},
		"changed ID":    {record.ExecutionID, created.Revision, Record{ExecutionID: "other"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, swapped, err := journal.CompareAndSwap(ctx, testCase.executionID, testCase.revision, strings.Repeat("b", 32), testCase.next); err == nil || swapped {
				t.Fatalf("CompareAndSwap swapped=%v err=%v", swapped, err)
			}
		})
	}
	if _, swapped, err := journal.CompareAndSwap(ctx, record.ExecutionID, created.Revision+1, strings.Repeat("b", 32), record); err != nil || swapped {
		t.Fatalf("stale CompareAndSwap swapped=%v err=%v", swapped, err)
	}
}
