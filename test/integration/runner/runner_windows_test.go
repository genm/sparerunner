//go:build windows

package runner_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/genm/tewake/internal/runner"
)

type windowsCache struct{}

func (windowsCache) Ensure(context.Context, runner.Package) (runner.ArchiveRef, error) {
	return runner.ArchiveRef{}, nil
}

type windowsJIT struct{}

func (windowsJIT) Digest() string {
	sum := sha256.Sum256([]byte("test"))
	return hex.EncodeToString(sum[:])
}
func (windowsJIT) Deliver(func(string) error) error { return nil }

func TestWindowsRunnerAdmissionFailsClosedWithoutJobObject(t *testing.T) {
	pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runner.NewManager(runner.Options{RuntimeRoot: t.TempDir(), Cache: windowsCache{}, Journal: runner.NewMemoryJournal()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.EnsureRunning(context.Background(), runner.Start{Preparation: runner.Preparation{ExecutionID: "windows-test", Package: pkg}, JIT: windowsJIT{}})
	if !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("EnsureRunning error = %v", err)
	}
}
