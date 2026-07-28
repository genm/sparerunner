package store_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/store"
)

func TestAgentAvailabilityDefaultsToAcceptingWithoutWriting(t *testing.T) {
	agent := openAvailabilityStore(t)
	record, err := agent.ReadAvailability(context.Background())
	if err != nil {
		t.Fatalf("read availability: %v", err)
	}
	if record.Intent != domain.AvailabilityAccepting || record.Explicit {
		t.Fatalf("unexpected default availability: %+v", record)
	}
}

func TestAgentAvailabilitySurvivesReopen(t *testing.T) {
	path := filepath.Join(privateDir(t), "agent.db")
	agent := openAvailabilityStoreAt(t, path)
	if _, err := agent.SetAvailability(
		context.Background(), domain.AvailabilityStopped, "tray",
	); err != nil {
		t.Fatalf("stop acceptance: %v", err)
	}
	if err := agent.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A stopped computer stays stopped across an agent service restart: the
	// owner's decision is durable, not process state.
	reopened := openAvailabilityStoreAt(t, path)
	record, err := reopened.ReadAvailability(context.Background())
	if err != nil {
		t.Fatalf("read availability: %v", err)
	}
	if record.Intent != domain.AvailabilityStopped || !record.Explicit {
		t.Fatalf("availability did not survive reopen: %+v", record)
	}
	if record.ChangedBy != "tray" || record.ChangedAtUnixNano <= 0 {
		t.Fatalf("availability provenance was lost: %+v", record)
	}
}

func TestAgentAvailabilityRejectsUnknownIntentAndMissingSurface(t *testing.T) {
	agent := openAvailabilityStore(t)
	if _, err := agent.SetAvailability(context.Background(), "paused-ish", "tray"); err == nil {
		t.Fatal("unknown intent accepted")
	}
	if _, err := agent.SetAvailability(
		context.Background(), domain.AvailabilityStopped, "",
	); err == nil {
		t.Fatal("availability change without a requesting surface accepted")
	}
	record, err := agent.ReadAvailability(context.Background())
	if err != nil {
		t.Fatalf("read availability: %v", err)
	}
	if record.Intent != domain.AvailabilityAccepting || record.Explicit {
		t.Fatalf("rejected mutation changed state: %+v", record)
	}
}

func openAvailabilityStore(t *testing.T) *store.AgentStore {
	t.Helper()
	return openAvailabilityStoreAt(t, filepath.Join(privateDir(t), "agent.db"))
}

// privateDir matches the store's production requirement that the database live
// in a directory without group or world access.
func privateDir(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create private directory: %v", err)
	}
	return directory
}

func openAvailabilityStoreAt(t *testing.T, path string) *store.AgentStore {
	t.Helper()
	agent, err := store.OpenAgent(context.Background(), path, store.Options{})
	if err != nil {
		t.Fatalf("open agent store: %v", err)
	}
	t.Cleanup(func() { _ = agent.Close() })
	return agent
}
