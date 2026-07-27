//go:build darwin

package macos

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/genm/tewake/internal/runner"
)

type fakeProcessSource struct {
	mu        sync.Mutex
	processes []processObservation
	kills     []int
	killErr   error
}

func (source *fakeProcessSource) Processes(
	context.Context,
) ([]processObservation, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	return append([]processObservation(nil), source.processes...), nil
}

func (source *fakeProcessSource) Kill(pid int, _ syscall.Signal) error {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.kills = append(source.kills, pid)
	if source.killErr != nil {
		return source.killErr
	}
	kept := source.processes[:0]
	for _, process := range source.processes {
		if pid < 0 && process.PGID == -pid || pid > 0 && process.PID == pid {
			continue
		}
		kept = append(kept, process)
	}
	source.processes = kept
	return nil
}

func TestFileRuntimeKillsProcessGroupAndEscapedDedicatedUIDProcess(t *testing.T) {
	processes := &fakeProcessSource{processes: []processObservation{
		{PID: 4101, PGID: 4101, RUID: 7001, EUID: 7001},
		// This descendant escaped the original process group, but it cannot
		// escape the non-login slot UID without privilege.
		{PID: 4102, PGID: 4102, RUID: 7001, EUID: 7001},
		{PID: 5101, PGID: 5101, RUID: 7002, EUID: 7002},
	}}
	nativeRuntime := testFileRuntime(t, processes)
	containment := testContainment("a", nativeRuntime.hostEpoch)
	fence, err := nativeRuntime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.MarkLaunched(context.Background(), 4101); err != nil {
		t.Fatal(err)
	}
	if err := fence.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := nativeRuntime.KillAndWait(context.Background(), containment); err != nil {
		t.Fatal(err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	processes.mu.Lock()
	defer processes.mu.Unlock()
	if len(processes.processes) != 1 || processes.processes[0].PID != 5101 {
		t.Fatalf("remaining processes=%#v", processes.processes)
	}
	if len(processes.kills) < 2 || processes.kills[0] != -4101 {
		t.Fatalf("kill sequence=%v", processes.kills)
	}
}

func TestFileRuntimeCleanupFailureDoesNotFinalizeFence(t *testing.T) {
	processes := &fakeProcessSource{
		processes: []processObservation{
			{PID: 4201, PGID: 4201, RUID: 7001, EUID: 7001},
		},
		killErr: syscall.EPERM,
	}
	nativeRuntime := testFileRuntime(t, processes)
	containment := testContainment("b", nativeRuntime.hostEpoch)
	fence, err := nativeRuntime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.MarkLaunched(context.Background(), 4201); err != nil {
		t.Fatal(err)
	}
	if err := fence.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := nativeRuntime.KillAndWait(
		context.Background(),
		containment,
	); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("KillAndWait error=%v", err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	if err := nativeRuntime.FinalizeFence(
		context.Background(),
		containment,
	); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("FinalizeFence error=%v", err)
	}
	state, err := nativeRuntime.readFenceState(containment)
	if err != nil || state.State != fenceStateRevoked {
		t.Fatalf("state=%#v err=%v", state, err)
	}
}

func TestFileRuntimeFinalizedFenceGarbageCollectionIsIdempotent(t *testing.T) {
	processes := &fakeProcessSource{}
	nativeRuntime := testFileRuntime(t, processes)
	containment := testContainment("c", nativeRuntime.hostEpoch)
	fence, err := nativeRuntime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	if err := nativeRuntime.FinalizeFence(context.Background(), containment); err != nil {
		t.Fatal(err)
	}
	if err := nativeRuntime.GarbageCollectFence(context.Background(), containment); err != nil {
		t.Fatal(err)
	}
	if err := nativeRuntime.GarbageCollectFence(context.Background(), containment); err != nil {
		t.Fatalf("idempotent collection error=%v", err)
	}
	if _, err := os.Lstat(filepath.Join(
		nativeRuntime.fenceRoot,
		containment.OwnerID,
	)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fence remains: %v", err)
	}
}

func TestFileRuntimeRejectsFenceTokenMismatch(t *testing.T) {
	nativeRuntime := testFileRuntime(t, &fakeProcessSource{})
	containment := testContainment("d", nativeRuntime.hostEpoch)
	fence, err := nativeRuntime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	mismatch := containment
	mismatch.FenceToken = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := nativeRuntime.LockFence(
		context.Background(),
		mismatch,
	); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("LockFence mismatch error=%v", err)
	}
}

func TestFileRuntimeRejectsUnsafeFenceMetadataAndState(t *testing.T) {
	for _, test := range []struct {
		name string
		seed string
	}{
		{name: "metadata.json", seed: "e"},
		{name: "state.json", seed: "f"},
	} {
		t.Run(test.name, func(t *testing.T) {
			nativeRuntime := testFileRuntime(t, &fakeProcessSource{})
			containment := testContainment(test.seed, nativeRuntime.hostEpoch)
			fence, err := nativeRuntime.LockFence(context.Background(), containment)
			if err != nil {
				t.Fatal(err)
			}
			if err := fence.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(nativeRuntime.fenceRoot, containment.OwnerID, test.name)
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := nativeRuntime.LockFence(
				context.Background(),
				containment,
			); !errors.Is(err, runner.ErrCleanupFailed) {
				t.Fatalf("LockFence unsafe %s error=%v", test.name, err)
			}
			if _, err := nativeRuntime.readFenceState(
				containment,
			); !errors.Is(err, runner.ErrCleanupFailed) {
				t.Fatalf("readFenceState unsafe %s error=%v", test.name, err)
			}
		})
	}
}

func TestDarwinProcessSourceObservesCurrentIdentity(t *testing.T) {
	processes, err := (darwinProcesses{}).Processes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, process := range processes {
		if process.PID == os.Getpid() {
			found = process.RUID == os.Getuid() &&
				process.EUID == os.Geteuid() &&
				process.PGID > 0
			break
		}
	}
	if !found {
		t.Fatal("current process identity was not observable through kern.proc.all")
	}
}

func TestCurrentBootEpochIsStableAndOpaque(t *testing.T) {
	first, err := currentBootEpoch()
	if err != nil {
		t.Fatal(err)
	}
	second, err := currentBootEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if first != second || len(first) != 64 {
		t.Fatalf("boot epochs=%q %q", first, second)
	}
}

func TestFenceRootAdmissionRejectsWritableAncestorAndSymlink(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(base, "parent")
	root := filepath.Join(parent, "fences")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	uid, gid := os.Geteuid(), os.Getegid()
	if !safePrivateDirectoryChain(root, uid, gid) {
		t.Fatal("private fence path was rejected")
	}
	if err := os.Chmod(parent, 0o777); err != nil {
		t.Fatal(err)
	}
	if safePrivateDirectoryChain(root, uid, gid) {
		t.Fatal("writable fence ancestor was accepted")
	}
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "linked-fences")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if safePrivateDirectoryChain(link, uid, gid) {
		t.Fatal("symlink fence root was accepted")
	}
}

func testFileRuntime(t *testing.T, processes processSource) *FileRuntime {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return &FileRuntime{
		fenceRoot: root,
		identity:  RunnerIdentity{UID: 7001, GID: 7001},
		hostEpoch: "boot-test",
		processes: processes,
		ownerUID:  os.Geteuid(),
		ownerGID:  os.Getegid(),
	}
}

func testContainment(seed, hostEpoch string) runner.ContainmentRef {
	// Construct canonical lowercase hex without hiding the test's seed.
	owner := "tewake-" + seed + "000000000000000000000000000000000000000000000000000000000000000"
	return runner.ContainmentRef{
		Backend:    containmentBackend,
		OwnerID:    owner,
		Scope:      "tewake/" + owner,
		HostEpoch:  hostEpoch,
		FenceToken: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}
