//go:build linux && cgroupintegration

package linux

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

func testContainment(t *testing.T) runner.ContainmentRef {
	t.Helper()
	epoch, err := currentBootEpoch()
	if err != nil {
		t.Fatal(err)
	}
	return runner.ContainmentRef{
		Backend:    containmentBackend,
		OwnerID:    containmentOwner("linux-fence-test"),
		Scope:      filepath.Join("sparerunner", containmentOwner("linux-fence-test")),
		HostEpoch:  epoch,
		FenceToken: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}

func TestFileFencePersistsStopBeforeStartRevocation(t *testing.T) {
	runtime := &FileRuntime{fenceRoot: privilegedFenceRoot(t)}
	containment := testContainment(t)
	first, err := runtime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := runtime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	revoked, err := second.Revoked()
	if err != nil || !revoked {
		t.Fatalf("revoked=%v err=%v", revoked, err)
	}
	if !runtime.durableFenceRevoked(containment) {
		t.Fatal("revoked fence was not available for idempotent absence proof")
	}
}

func TestFileFenceWaitHonorsCallerContext(t *testing.T) {
	runtime := &FileRuntime{fenceRoot: privilegedFenceRoot(t)}
	containment := testContainment(t)
	first, err := runtime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := runtime.LockFence(ctx, containment); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("LockFence error = %v", err)
	}
}

func TestFileFenceSerializesDifferentTokensForSameContainment(t *testing.T) {
	runtime := &FileRuntime{fenceRoot: privilegedFenceRoot(t)}
	firstContainment := testContainment(t)
	first, err := runtime.LockFence(context.Background(), firstContainment)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	secondContainment := firstContainment
	secondContainment.FenceToken = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := runtime.LockFence(ctx, secondContainment); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("different-token LockFence error = %v", err)
	}
}

func TestFenceRejectsStaleHostEpochBeforeTouchingDurableState(t *testing.T) {
	runtime := &FileRuntime{fenceRoot: privilegedFenceRoot(t)}
	containment := testContainment(t)
	containment.HostEpoch = "stale-boot-id"
	if _, err := runtime.LockFence(context.Background(), containment); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("LockFence error = %v", err)
	}
	entries, err := os.ReadDir(runtime.fenceRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("stale containment created durable fence state: %#v", entries)
	}
}

func privilegedFenceRoot(t *testing.T) string {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("durable root-owned fence test requires the root Supervisor identity")
	}
	root, err := os.MkdirTemp("/var/lib", "sparerunner-fence-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if filepath.Dir(root) == "/var/lib" {
			_ = os.RemoveAll(root)
		}
	})
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestCgroupEventsRequireUnambiguousPopulation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cgroup.events")
	if err := os.WriteFile(path, []byte("populated 1\nfrozen 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	populated, err := cgroupPopulated(path)
	if err != nil || !populated {
		t.Fatalf("populated=%v err=%v", populated, err)
	}
	if err := os.WriteFile(path, []byte("frozen 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := cgroupPopulated(path); err == nil {
		t.Fatal("missing populated field was accepted")
	}
}
