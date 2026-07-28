//go:build linux

package linux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

const testCgroup2MountInfo = "31 24 0:27 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw\n"

func finalizedFenceTestRuntime(t *testing.T) (*FileRuntime, runner.ContainmentRef) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("root-owned finalized fence test requires root")
	}
	fenceRoot, err := os.MkdirTemp("/var/lib", "tewake-finalized-fence-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if filepath.Dir(fenceRoot) == "/var/lib" {
			_ = os.RemoveAll(fenceRoot)
		}
	})
	if err := os.Chmod(fenceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	cgroupRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(cgroupRoot, "tewake"), 0o755); err != nil {
		t.Fatal(err)
	}
	epoch, err := currentBootEpoch()
	if err != nil {
		t.Fatal(err)
	}
	containment := runner.ContainmentRef{
		Backend:    containmentBackend,
		OwnerID:    containmentOwner("finalized-fence-test"),
		Scope:      filepath.Join("tewake", containmentOwner("finalized-fence-test")),
		HostEpoch:  epoch,
		FenceToken: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	return &FileRuntime{cgroupRoot: cgroupRoot, fenceRoot: fenceRoot}, containment
}

func TestFileFenceFinalizeIsIdempotentAndGarbageCollectable(t *testing.T) {
	runtime, containment := finalizedFenceTestRuntime(t)
	fence, err := runtime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.MarkLaunched(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	fence, err = runtime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	launched, err := fence.Launched()
	if err != nil || !launched {
		t.Fatalf("reopened launched=%v err=%v", launched, err)
	}
	if err := fence.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.FinalizeFence(context.Background(), containment); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(runtime.fenceRoot, containment.OwnerID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized owner residue remains: %v", err)
	}
	restarted := &FileRuntime{cgroupRoot: runtime.cgroupRoot, fenceRoot: runtime.fenceRoot}
	replayed, err := restarted.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := replayed.Revoked()
	if err != nil || !revoked {
		t.Fatalf("finalized replay revoked=%v err=%v", revoked, err)
	}
	if err := replayed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := restarted.FinalizeFence(context.Background(), containment); err != nil {
		t.Fatal(err)
	}
	differentToken := containment
	differentToken.FenceToken = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := restarted.LockFence(context.Background(), differentToken); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("different-token finalized LockFence error=%v", err)
	}
	if err := restarted.GarbageCollectFence(context.Background(), containment); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(restarted.finalizedFencePath(containment)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized tombstone remains: %v", err)
	}
}

func TestFileFenceDescriptorsAreCloseOnExec(t *testing.T) {
	runtime, containment := finalizedFenceTestRuntime(t)
	fence, err := runtime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	defer fence.Close()
	opened, ok := fence.(*fileFence)
	if !ok {
		t.Fatalf("fence type=%T", fence)
	}
	for name, file := range map[string]*os.File{
		"owner lock": opened.lock,
		"state":      opened.state,
	} {
		flags, _, errno := syscall.Syscall(
			syscall.SYS_FCNTL,
			file.Fd(),
			uintptr(syscall.F_GETFD),
			0,
		)
		if errno != 0 {
			t.Fatalf("%s F_GETFD: %v", name, errno)
		}
		if flags&syscall.FD_CLOEXEC == 0 {
			t.Fatalf("%s descriptor can leak across exec", name)
		}
	}
}

func TestFileFenceDurableLaunchRejectsDifferentAuthority(t *testing.T) {
	runtime, containment := finalizedFenceTestRuntime(t)
	fence, err := runtime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.MarkLaunched(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	cgroupPath := filepath.Join(runtime.cgroupRoot, containment.Scope)
	if err := os.Mkdir(cgroupPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupPath, "cgroup.events"), []byte("populated 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupPath, "cgroup.kill"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupPath, "cgroup.procs"), []byte("1001\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if alive, err := runtime.Alive(context.Background(), containment, 1001); err != nil || !alive {
		t.Fatalf("exact durable authority Alive=%v err=%v", alive, err)
	}
	if alive, err := runtime.Alive(context.Background(), containment, 1002); err != nil || alive {
		t.Fatalf("completed-listener Alive=%v err=%v", alive, err)
	}

	differentToken := containment
	differentToken.FenceToken = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if alive, err := runtime.Alive(context.Background(), differentToken, 1001); err == nil || alive {
		t.Fatalf("different-token Alive=%v err=%v", alive, err)
	}
	if _, err := runtime.LockFence(context.Background(), differentToken); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("different-token LockFence error=%v", err)
	}

	statePath := filepath.Join(runtime.fenceRoot, containment.OwnerID, containment.FenceToken)
	mismatchedEpoch := containment
	mismatchedEpoch.HostEpoch = "different-boot-epoch"
	if err := os.WriteFile(
		statePath,
		[]byte(fenceStateContent(mismatchedEpoch, fenceStateLaunched)),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.LockFence(context.Background(), containment); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("mismatched durable authority LockFence error=%v", err)
	}
}

func TestFileFenceFinalizationRequiresRevokedAndEmptyContainment(t *testing.T) {
	runtime, containment := finalizedFenceTestRuntime(t)
	fence, err := runtime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ValidateFinalization(
		context.Background(),
		containment,
	); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("active fence ValidateFinalization error=%v", err)
	}

	fence, err = runtime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	cgroupPath := filepath.Join(runtime.cgroupRoot, containment.Scope)
	if err := os.Mkdir(cgroupPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupPath, "cgroup.events"), []byte("populated 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupPath, "cgroup.kill"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ValidateFinalization(
		context.Background(),
		containment,
	); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("populated cgroup ValidateFinalization error=%v", err)
	}
	if err := os.RemoveAll(cgroupPath); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ValidateFinalization(context.Background(), containment); err != nil {
		t.Fatalf("revoked empty ValidateFinalization error=%v", err)
	}
}

func TestFileFenceFinalizeRecoversPartialOwnerRemoval(t *testing.T) {
	for _, missing := range []string{"state", "lock"} {
		t.Run(missing, func(t *testing.T) {
			runtime, containment := finalizedFenceTestRuntime(t)
			fence, err := runtime.LockFence(context.Background(), containment)
			if err != nil {
				t.Fatal(err)
			}
			if err := fence.Revoke(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := fence.Close(); err != nil {
				t.Fatal(err)
			}
			if err := runtime.publishFinalizedFence(containment); err != nil {
				t.Fatal(err)
			}
			name := containment.FenceToken
			if missing == "lock" {
				name = "containment.lock"
			}
			if err := os.Remove(filepath.Join(runtime.fenceRoot, containment.OwnerID, name)); err != nil {
				t.Fatal(err)
			}
			if err := runtime.FinalizeFence(context.Background(), containment); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(filepath.Join(runtime.fenceRoot, containment.OwnerID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partial finalized owner remains: %v", err)
			}
		})
	}
}

func TestFileFenceFinalizeRecoversKnownPublicationCrashResidue(t *testing.T) {
	for _, boundary := range []string{"temp-only", "archive-and-temp"} {
		t.Run(boundary, func(t *testing.T) {
			runtime, containment := finalizedFenceTestRuntime(t)
			fence, err := runtime.LockFence(context.Background(), containment)
			if err != nil {
				t.Fatal(err)
			}
			if err := fence.Revoke(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := fence.Close(); err != nil {
				t.Fatal(err)
			}
			finalizedDirectory := filepath.Join(runtime.fenceRoot, finalizedFenceDirectory)
			if err := os.Mkdir(finalizedDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			temporaryName := ".finalizing-" + strings.Repeat("b", 32)
			temporaryPath := filepath.Join(finalizedDirectory, temporaryName)
			if err := os.WriteFile(
				temporaryPath,
				[]byte(finalizedFenceContent(containment)),
				0o600,
			); err != nil {
				t.Fatal(err)
			}
			if boundary == "archive-and-temp" {
				if err := os.Link(
					temporaryPath,
					runtime.finalizedFencePath(containment),
				); err != nil {
					t.Fatal(err)
				}
			}

			if err := runtime.FinalizeFence(context.Background(), containment); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(finalizedDirectory)
			if err != nil || len(entries) != 1 ||
				entries[0].Name() != runtime.finalizedFenceName(containment) {
				t.Fatalf("finalized entries=%#v err=%v", entries, err)
			}
			if err := runtime.GarbageCollectFence(context.Background(), containment); err != nil {
				t.Fatal(err)
			}
			entries, err = os.ReadDir(finalizedDirectory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("post-Released GC entries=%#v err=%v", entries, err)
			}
		})
	}
}

func TestFileFenceFinalizeRejectsUnknownTemporaryResidue(t *testing.T) {
	for _, test := range []struct {
		name    string
		temp    string
		payload string
	}{
		{
			name:    "unknown name",
			temp:    ".finalizing-unknown",
			payload: "valid",
		},
		{
			name:    "malformed payload",
			temp:    ".finalizing-" + strings.Repeat("c", 32),
			payload: "malformed finalized payload\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, containment := finalizedFenceTestRuntime(t)
			fence, err := runtime.LockFence(context.Background(), containment)
			if err != nil {
				t.Fatal(err)
			}
			if err := fence.Revoke(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := fence.Close(); err != nil {
				t.Fatal(err)
			}
			finalizedDirectory := filepath.Join(runtime.fenceRoot, finalizedFenceDirectory)
			if err := os.Mkdir(finalizedDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			payload := test.payload
			if payload == "valid" {
				payload = finalizedFenceContent(containment)
			}
			unknown := filepath.Join(finalizedDirectory, test.temp)
			if err := os.WriteFile(unknown, []byte(payload), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := runtime.FinalizeFence(
				context.Background(),
				containment,
			); !errors.Is(err, runner.ErrCleanupFailed) {
				t.Fatalf("FinalizeFence error=%v", err)
			}
			if _, err := os.Lstat(unknown); err != nil {
				t.Fatalf("unknown residue was removed: %v", err)
			}
		})
	}
}

func TestFileFenceFinalizeRejectsUnsafeResidueWithoutErasingAuthority(t *testing.T) {
	runtime, containment := finalizedFenceTestRuntime(t)
	fence, err := runtime.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	ownerDirectory := filepath.Join(runtime.fenceRoot, containment.OwnerID)
	if err := os.Symlink("/etc/passwd", filepath.Join(ownerDirectory, "unexpected")); err != nil {
		t.Fatal(err)
	}
	if err := runtime.FinalizeFence(context.Background(), containment); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("FinalizeFence error=%v", err)
	}
	for _, name := range []string{"containment.lock", containment.FenceToken, "unexpected"} {
		if _, err := os.Lstat(filepath.Join(ownerDirectory, name)); err != nil {
			t.Fatalf("failed finalize erased %s: %v", name, err)
		}
	}
}

func TestFileFenceFinalizedOwnerMismatchFailsClosed(t *testing.T) {
	runtime, containment := finalizedFenceTestRuntime(t)
	if err := os.Mkdir(filepath.Join(runtime.fenceRoot, finalizedFenceDirectory), 0o700); err != nil {
		t.Fatal(err)
	}
	path := runtime.finalizedFencePath(containment)
	if err := os.WriteFile(path, []byte("forged finalized authority\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.LockFence(context.Background(), containment); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("LockFence error=%v", err)
	}
	if err := runtime.GarbageCollectFence(context.Background(), containment); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("GarbageCollectFence error=%v", err)
	}
}

func TestDelegatedCgroupRootUsesOnlyCanonicalUnifiedMembership(t *testing.T) {
	root, err := delegatedCgroupRoot(
		[]byte("0::/system.slice/tewake-agent-supervisor.service\n"),
		[]byte(testCgroup2MountInfo),
	)
	if err != nil {
		t.Fatal(err)
	}
	expected := filepath.Join("/sys/fs/cgroup", "system.slice", "tewake-agent-supervisor.service")
	if root != expected {
		t.Fatalf("root=%q want=%q", root, expected)
	}
}

func TestDelegatedCgroupRootRejectsAmbiguousOrUnsafeProcState(t *testing.T) {
	tests := []struct {
		name       string
		membership string
		mounts     string
	}{
		{
			name:       "multiple memberships",
			membership: "0::/system.slice/tewake.service\n0::/other\n",
			mounts:     testCgroup2MountInfo,
		},
		{
			name:       "v1 membership",
			membership: "5:cpu:/system.slice/tewake.service\n",
			mounts:     testCgroup2MountInfo,
		},
		{
			name:       "hybrid membership",
			membership: "0::/system.slice/tewake.service\n5:cpu:/system.slice/tewake.service\n",
			mounts:     testCgroup2MountInfo,
		},
		{
			name:       "host root",
			membership: "0::/\n",
			mounts:     testCgroup2MountInfo,
		},
		{
			name:       "path traversal",
			membership: "0::/system.slice/../user.slice\n",
			mounts:     testCgroup2MountInfo,
		},
		{
			name:       "noncanonical path",
			membership: "0::/system.slice//tewake.service\n",
			mounts:     testCgroup2MountInfo,
		},
		{
			name:       "no cgroup2 mount",
			membership: "0::/system.slice/tewake.service\n",
			mounts:     "31 24 0:27 / /sys/fs/cgroup rw - tmpfs tmpfs rw\n",
		},
		{
			name:       "multiple cgroup2 mounts",
			membership: "0::/system.slice/tewake.service\n",
			mounts:     testCgroup2MountInfo + testCgroup2MountInfo,
		},
		{
			name:       "subtree cgroup mount",
			membership: "0::/system.slice/tewake.service\n",
			mounts:     "31 24 0:27 /system.slice /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
		},
		{
			name:       "unsupported mount escape",
			membership: "0::/system.slice/tewake.service\n",
			mounts:     "31 24 0:27 / /sys/fs/cgroup\\057evil rw - cgroup2 cgroup2 rw\n",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := delegatedCgroupRoot([]byte(testCase.membership), []byte(testCase.mounts))
			if !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestCgroupEventsRequireUnambiguousPopulationData(t *testing.T) {
	populated, err := cgroupPopulatedData([]byte("populated 1\nfrozen 0\n"))
	if err != nil || !populated {
		t.Fatalf("populated=%v err=%v", populated, err)
	}
	populated, err = cgroupPopulatedData([]byte("populated 0\nfrozen 0\n"))
	if err != nil || populated {
		t.Fatalf("populated=%v err=%v", populated, err)
	}
	if _, err := cgroupPopulatedData([]byte("frozen 0\n")); err == nil {
		t.Fatal("missing populated field was accepted")
	}
}

func TestSlotAdmissionTreatsOtherEmptyCgroupAsCleanupResidue(t *testing.T) {
	root := t.TempDir()
	tewakeRoot := filepath.Join(root, "tewake")
	if err := os.Mkdir(tewakeRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	candidateOwner := containmentOwner("candidate")
	candidate := testSlotCgroup(t, root, candidateOwner, false)
	runtime := &FileRuntime{cgroupRoot: root}

	busy, err := runtime.SlotBusy(context.Background(), candidate)
	if err != nil || busy {
		t.Fatalf("candidate-only admission busy=%v err=%v", busy, err)
	}

	testSlotCgroup(t, root, containmentOwner("cleanup-residue"), false)
	busy, err = runtime.SlotBusy(context.Background(), candidate)
	if err != nil || !busy {
		t.Fatalf("empty cleanup residue admission busy=%v err=%v", busy, err)
	}
}

func TestSlotAdmissionRejectsPopulatedCandidate(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "tewake"), 0o755); err != nil {
		t.Fatal(err)
	}
	candidate := testSlotCgroup(t, root, containmentOwner("candidate"), true)
	runtime := &FileRuntime{cgroupRoot: root}
	busy, err := runtime.SlotBusy(context.Background(), candidate)
	if err != nil || !busy {
		t.Fatalf("populated candidate admission busy=%v err=%v", busy, err)
	}
}

func testSlotCgroup(t *testing.T, root, owner string, populated bool) runner.ContainmentRef {
	t.Helper()
	epoch, err := currentBootEpoch()
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "tewake", owner)
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	populatedValue := 0
	if populated {
		populatedValue = 1
	}
	if err := os.WriteFile(
		filepath.Join(directory, "cgroup.events"),
		[]byte(fmt.Sprintf("populated %d\n", populatedValue)),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "cgroup.kill"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return runner.ContainmentRef{
		Backend:   containmentBackend,
		OwnerID:   owner,
		Scope:     filepath.Join("tewake", owner),
		HostEpoch: epoch,
	}
}

func TestLauncherReaperWaitsAndRemovesExitedChild(t *testing.T) {
	command := exec.Command("/bin/sh", "-c", "exit 0")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	pid := command.Process.Pid
	select {
	case err := <-reapCommand(command):
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("child was not reaped")
	}
	if command.ProcessState == nil || !command.ProcessState.Exited() {
		t.Fatal("reaper did not retain the exit observation")
	}
	if _, err := os.Stat(filepath.Join("/proc", strconv.Itoa(pid))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reaped child remains in proc: %v", err)
	}
}

func TestExecLauncherRejectsAgentWritableHelperBinary(t *testing.T) {
	helper := filepath.Join(t.TempDir(), "tewake-agent")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := NewExecLauncher(helper); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("NewExecLauncher error=%v", err)
	}
}

func TestFixedRunnerArgumentsRejectAdditionalCommandSurface(t *testing.T) {
	if !fixedRunnerArguments([]string{"--ephemeral"}) ||
		!fixedRunnerArguments([]string{"--ephemeral", "--disableupdate"}) {
		t.Fatal("official fixed runner arguments were rejected")
	}
	for _, arguments := range [][]string{
		nil,
		{"--jitconfig", "secret"},
		{"--ephemeral", "--name", "attacker"},
		{"--disableupdate", "--ephemeral"},
	} {
		if fixedRunnerArguments(arguments) {
			t.Fatalf("arbitrary arguments were accepted: %#v", arguments)
		}
	}
}

func TestRunnerEnvironmentScopesHomeAndTemporaryStateToPinnedExecution(t *testing.T) {
	environment, err := fixedRunnerEnvironment(os.Geteuid(), os.Getegid())
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(environment, "\n")
	for _, required := range []string{
		"HOME=/proc/self/fd/4/.tewake-home",
		"XDG_CACHE_HOME=/proc/self/fd/4/.tewake-home/.cache",
		"XDG_CONFIG_HOME=/proc/self/fd/4/.tewake-home/.config",
		"TMPDIR=/proc/self/fd/4/.tewake-home/.tmp",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("execution-scoped environment missing %q: %s", required, joined)
		}
	}
	account, err := user.Current()
	if err == nil && account.HomeDir != "" && strings.Contains(joined, "HOME="+account.HomeDir) {
		t.Fatalf("persistent account home leaked into execution environment: %s", joined)
	}
}

func TestPinnedLauncherHelperProcess(t *testing.T) {
	if os.Getenv("TEWAKE_PINNED_HELPER_PROCESS") != "1" {
		return
	}
	handled, err := RunExecLauncherHelper([]string{
		helperModeArgument,
		"--",
		"--ephemeral",
		"--disableupdate",
	})
	if !handled || err != nil {
		os.Exit(97)
	}
}

func TestPinnedLauncherIgnoresNameSwapAndSeparatesJobHome(t *testing.T) {
	first := newLauncherExecution(t, true)
	original := first.root + ".original"
	if err := os.Rename(first.root, original); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(first.root, ".tewake-home", ".tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(first.root, "run.sh"), []byte("#!/bin/sh\nexit 91\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	runPinnedLauncherHelper(t, first)
	assertLauncherResult(t, original, "canary")

	second := newLauncherExecution(t, false)
	runPinnedLauncherHelper(t, second)
	assertLauncherResult(t, second.root, "clean")
	if _, err := os.Lstat(filepath.Join(second.root, ".tewake-home", "canary")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first job HOME canary crossed into second execution: %v", err)
	}
}

type launcherExecution struct {
	root       string
	directory  *os.File
	executable *os.File
}

func newLauncherExecution(t *testing.T, canary bool) launcherExecution {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{
		".tewake-home",
		".tewake-home/.config",
		".tewake-home/.cache",
		".tewake-home/.tmp",
	} {
		if err := os.MkdirAll(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if canary {
		if err := os.WriteFile(filepath.Join(root, ".tewake-home", "canary"), []byte("first job"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Mirror the official v2.336.0 wrapper's BASH_SOURCE symlink resolution.
	// The production launcher executes /proc/self/fd/5, so this catches a
	// descriptor-pinned launch that cannot find sibling package files after the
	// Agent-writable workspace name is replaced.
	script := `#!/bin/bash
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
  DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
cp -f "$DIR/run-helper.sh.template" "$DIR/run-helper.sh"
"$DIR/run-helper.sh"
`
	helper := `#!/bin/bash
state=clean
if [ -e "$HOME/canary" ]; then state=canary; fi
printf '%s|%s|%s\n' "$state" "$HOME" "$TMPDIR" > "$HOME/result"
`
	if err := os.WriteFile(filepath.Join(root, "run.sh"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "run-helper.sh.template"), []byte(helper), 0o700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Open(filepath.Join(root, "run.sh"))
	if err != nil {
		_ = directory.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = executable.Close()
		_ = directory.Close()
	})
	return launcherExecution{root: root, directory: directory, executable: executable}
}

func runPinnedLauncherHelper(t *testing.T, execution launcherExecution) {
	t.Helper()
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer statusReader.Close()
	environment, err := fixedRunnerEnvironment(os.Geteuid(), os.Getegid())
	if err != nil {
		_ = statusWriter.Close()
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestPinnedLauncherHelperProcess$")
	command.Env = append(environment, "TEWAKE_PINNED_HELPER_PROCESS=1")
	command.Stdin = strings.NewReader("one-job-jit")
	command.ExtraFiles = []*os.File{statusWriter, execution.directory, execution.executable}
	if err := command.Start(); err != nil {
		_ = statusWriter.Close()
		t.Fatal(err)
	}
	_ = statusWriter.Close()
	status, readErr := io.ReadAll(statusReader)
	waitErr := command.Wait()
	if readErr != nil || waitErr != nil || len(status) != 0 {
		t.Fatalf("pinned helper status=%q readErr=%v waitErr=%v", status, readErr, waitErr)
	}
}

func assertLauncherResult(t *testing.T, root, state string) {
	t.Helper()
	result, err := os.ReadFile(filepath.Join(root, ".tewake-home", "result"))
	if err != nil {
		t.Fatal(err)
	}
	expected := state + "|/proc/self/fd/4/.tewake-home|/proc/self/fd/4/.tewake-home/.tmp\n"
	if string(result) != expected {
		t.Fatalf("launcher result=%q want=%q", result, expected)
	}
}
