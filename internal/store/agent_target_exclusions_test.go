package store_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/store"
)

func TestAgentTargetExclusionsDefaultToEmpty(t *testing.T) {
	agent := openAvailabilityStore(t)
	excluded, err := agent.ListExclusions(context.Background())
	if err != nil {
		t.Fatalf("list exclusions: %v", err)
	}
	if len(excluded) != 0 {
		t.Fatalf("untouched node reported exclusions: %v", excluded)
	}
}

func TestAgentTargetExclusionRoundTripIsSortedAndIdempotent(t *testing.T) {
	agent := openAvailabilityStore(t)
	ctx := context.Background()
	for _, targetID := range []domain.TargetID{"target-c", "target-a", "target-b"} {
		if err := agent.AddExclusion(ctx, targetID, "cli"); err != nil {
			t.Fatalf("add %s: %v", targetID, err)
		}
	}
	// Re-adding an existing exclusion is the owner clicking twice. It refreshes
	// provenance rather than failing or duplicating the entry.
	if err := agent.AddExclusion(ctx, "target-a", "tray"); err != nil {
		t.Fatalf("re-add: %v", err)
	}
	excluded, err := agent.ListExclusions(ctx)
	if err != nil {
		t.Fatalf("list exclusions: %v", err)
	}
	want := []domain.TargetID{"target-a", "target-b", "target-c"}
	if len(excluded) != len(want) {
		t.Fatalf("exclusions = %v, want %v", excluded, want)
	}
	for index := range want {
		if excluded[index] != want[index] {
			t.Fatalf("exclusions = %v, want %v", excluded, want)
		}
	}

	if err := agent.RemoveExclusion(ctx, "target-b"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	// Removing an absent entry already expresses the owner's end state.
	if err := agent.RemoveExclusion(ctx, "target-b"); err != nil {
		t.Fatalf("idempotent remove: %v", err)
	}
	excluded, err = agent.ListExclusions(ctx)
	if err != nil {
		t.Fatalf("list after remove: %v", err)
	}
	if len(excluded) != 2 || excluded[0] != "target-a" || excluded[1] != "target-c" {
		t.Fatalf("exclusions after remove = %v", excluded)
	}
}

func TestAgentTargetExclusionsSurviveReopen(t *testing.T) {
	path := filepath.Join(privateDir(t), "agent.db")
	agent := openAvailabilityStoreAt(t, path)
	if err := agent.AddExclusion(context.Background(), "target-durable", "raycast"); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := agent.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// An exclusion is fail-closed local authority: it must outlive the process
	// that recorded it, exactly like the global availability intent.
	reopened := openAvailabilityStoreAt(t, path)
	excluded, err := reopened.ListExclusions(context.Background())
	if err != nil {
		t.Fatalf("list after reopen: %v", err)
	}
	if len(excluded) != 1 || excluded[0] != "target-durable" {
		t.Fatalf("exclusions after reopen = %v", excluded)
	}
}

func TestAgentTargetExclusionCapFailsClosedWithoutTruncating(t *testing.T) {
	agent := openAvailabilityStore(t)
	ctx := context.Background()
	for index := 0; index < store.MaxTargetExclusions; index++ {
		targetID := domain.TargetID(fmt.Sprintf("target-%04d", index))
		if err := agent.AddExclusion(ctx, targetID, "cli"); err != nil {
			t.Fatalf("add %s: %v", targetID, err)
		}
	}
	// A full set rejects explicitly. It never silently drops the new entry or
	// evicts an existing one the owner still relies on.
	if err := agent.AddExclusion(ctx, "target-overflow", "cli"); !errors.Is(err, store.ErrTargetExclusionsFull) {
		t.Fatalf("overflow error = %v", err)
	}
	// A full set still accepts a re-add of something already excluded, because
	// that consumes no additional room.
	if err := agent.AddExclusion(ctx, "target-0000", "tray"); err != nil {
		t.Fatalf("re-add at capacity: %v", err)
	}
	excluded, err := agent.ListExclusions(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(excluded) != store.MaxTargetExclusions {
		t.Fatalf("exclusion count = %d, want %d", len(excluded), store.MaxTargetExclusions)
	}
}

func TestAgentTargetExclusionRejectsUnstorableIdentifiers(t *testing.T) {
	agent := openAvailabilityStore(t)
	ctx := context.Background()
	long := ""
	for len(long) <= domain.MaxTargetIDBytes {
		long += "x"
	}
	for name, targetID := range map[string]domain.TargetID{
		"empty":             "",
		"leading space":     " target",
		"trailing space":    "target ",
		"control character": "tar\x00get",
		"too long":          domain.TargetID(long),
	} {
		t.Run(name, func(t *testing.T) {
			if err := agent.AddExclusion(ctx, targetID, "cli"); err == nil {
				t.Fatal("unstorable target identifier was accepted")
			}
		})
	}
	// A long-but-valid identifier is ordinary input and must still work.
	atBoundary := domain.TargetID(long[:domain.MaxTargetIDBytes])
	if err := agent.AddExclusion(ctx, atBoundary, "cli"); err != nil {
		t.Fatalf("identifier at the storage boundary rejected: %v", err)
	}
	excluded, err := agent.ListExclusions(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(excluded) != 1 || excluded[0] != atBoundary {
		t.Fatalf("exclusions = %v", excluded)
	}
}

func TestAgentTargetExclusionAcceptsUnknownTargetID(t *testing.T) {
	agent := openAvailabilityStore(t)
	// Excluding a Target this node has never been told about — offline, or
	// before the first eligible-target list arrives — is a safe no-op by design.
	if err := agent.AddExclusion(context.Background(), "never-heard-of-this", "cli"); err != nil {
		t.Fatalf("unknown target exclusion rejected: %v", err)
	}
}
