//go:build linux

package linux

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

func sharedTestIdentity(t *testing.T) RunnerIdentity {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("the shared-identity runner is only for an unprivileged agent")
	}
	return RunnerIdentity{UID: os.Geteuid(), GID: os.Getegid()}
}

// safeUserDirectory creates a private directory whose whole ancestry satisfies
// safeOwnedDirectory. It cannot live under /tmp: that is world writable, which
// this mode correctly refuses.
func safeUserDirectory(t *testing.T, owner RunnerIdentity) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil || !filepath.IsAbs(home) {
		t.Skip("no usable home directory for a private runtime root")
	}
	directory, err := os.MkdirTemp(home, "tewake-shared-test-")
	if err != nil {
		t.Skip("cannot create a private directory under the home directory")
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if !safeOwnedDirectory(directory, 0o700, fileOwner{UID: owner.UID, GID: owner.GID}) {
		t.Skip("home directory ancestry is not private enough for this mode")
	}
	return directory
}

func TestNewRootlessWorkspaceRefusesRootIdentity(t *testing.T) {
	if _, err := NewRootlessWorkspace(0, 0); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("NewRootlessWorkspace error = %v", err)
	}
}

// safeDelegatedCgroupRoot is the delegation proof this mode's containment claim
// depends on. A leaf the Agent does not own, or an ancestry a third account can
// rewrite, is not a boundary.
func TestSafeDelegatedCgroupRootRefusesUnownedOrWritableAncestry(t *testing.T) {
	owner := sharedTestIdentity(t)
	base := safeUserDirectory(t, owner)
	delegated := filepath.Join(base, "delegated")
	if err := os.Mkdir(delegated, 0o755); err != nil {
		t.Fatal(err)
	}
	if !safeDelegatedCgroupRoot(delegated, owner.UID) {
		t.Fatal("an agent-owned leaf under a private ancestry must be accepted")
	}
	if safeDelegatedCgroupRoot(delegated, owner.UID+1) {
		t.Fatal("a leaf owned by another account must be refused")
	}
	if safeDelegatedCgroupRoot(delegated, 0) {
		t.Fatal("root must never take this path; it has the privileged supervisor")
	}
	if err := os.Chmod(base, 0o777); err != nil {
		t.Fatal(err)
	}
	if safeDelegatedCgroupRoot(delegated, owner.UID) {
		t.Fatal("a world-writable ancestor must be refused")
	}
}

func TestRootlessWorkspaceValidateRuntimeRootRefusesUnsafeRoots(t *testing.T) {
	owner := sharedTestIdentity(t)
	workspace, err := NewRootlessWorkspace(owner.UID, owner.GID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	base := safeUserDirectory(t, owner)
	runtimeRoot := filepath.Join(base, "runtime")
	if err := os.Mkdir(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ValidateRuntimeRoot(ctx, runtimeRoot); err == nil {
		t.Fatal("a runtime root without an executions child must be refused")
	}
	if err := os.Mkdir(filepath.Join(runtimeRoot, "executions"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ValidateRuntimeRoot(ctx, runtimeRoot); err != nil {
		t.Fatalf("ValidateRuntimeRoot error = %v", err)
	}
	if err := workspace.ValidateRuntimeRoot(ctx, "relative/path"); err == nil {
		t.Fatal("a relative runtime root must be refused")
	}
	if err := os.Chmod(filepath.Join(runtimeRoot, "executions"), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ValidateRuntimeRoot(ctx, runtimeRoot); err == nil {
		t.Fatal("a world-writable executions root must be refused")
	}
	if err := os.Chmod(filepath.Join(runtimeRoot, "executions"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(runtimeRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ValidateRuntimeRoot(ctx, runtimeRoot); err == nil {
		t.Fatal("a runtime root that is not 0700 must be refused")
	}
}

// Construction is the only place this mode can prove its containment. Every
// refusal here becomes zero advertised capacity, never a weaker boundary.
func TestNewRootlessRuntimeFailsClosedOnUnusableInputs(t *testing.T) {
	owner := sharedTestIdentity(t)
	workspace, err := NewRootlessWorkspace(owner.UID, owner.GID)
	if err != nil {
		t.Fatal(err)
	}
	base := safeUserDirectory(t, owner)
	launcher := SharedIdentityLauncher{HelperPath: "/bin/sh", Owner: owner}

	if _, err := NewRootlessRuntime(filepath.Join(base, "fences"), filepath.Join(base, "runtime"), nil, workspace); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("a missing launcher must be refused, got %v", err)
	}
	if _, err := NewRootlessRuntime(filepath.Join(base, "fences"), filepath.Join(base, "runtime"), launcher, nil); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("a missing workspace must be refused, got %v", err)
	}
	if _, err := NewRootlessRuntime("fences", filepath.Join(base, "runtime"), launcher, workspace); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("a relative fence root must be refused, got %v", err)
	}
	if _, err := NewRootlessRuntime(filepath.Join(base, "fences"), "runtime", launcher, workspace); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("a relative runtime root must be refused, got %v", err)
	}
	// A runtime root that does not exist yet cannot be validated, and this
	// constructor deliberately does not create or repair it.
	if _, err := NewRootlessRuntime(filepath.Join(base, "fences"), filepath.Join(base, "absent"), launcher, workspace); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("an absent runtime root must be refused, got %v", err)
	}
}

func TestSharedWorkspaceRefCarriesItsOwnBackendAndOwner(t *testing.T) {
	owner := sharedTestIdentity(t)
	base := safeUserDirectory(t, owner)
	info, err := os.Lstat(base)
	if err != nil {
		t.Fatal(err)
	}
	ref := sharedWorkspaceRef(info, owner.UID, owner.GID)
	if ref.Backend != SharedWorkspaceBackend || ref.OwnerID == "" {
		t.Fatalf("sharedWorkspaceRef = %#v", ref)
	}
	if !strings.HasPrefix(ref.OwnerID, "shared:") {
		t.Fatalf("owner id %q must be distinguishable from the privileged encoding", ref.OwnerID)
	}
	if ref == workspaceRef(info, owner.UID, owner.GID) {
		t.Fatal("the two modes must never produce an equal workspace ref")
	}
	if got := sharedWorkspaceRef(info, owner.UID+1, owner.GID); got != (runner.WorkspaceRef{}) {
		t.Fatalf("a foreign owner must produce no ref, got %#v", got)
	}
}

func TestNewSharedIdentityLauncherRefusesWritableOrForeignBinaries(t *testing.T) {
	owner := sharedTestIdentity(t)
	base := safeUserDirectory(t, owner)
	binary := filepath.Join(base, "agent")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSharedIdentityLauncher(binary, owner); err != nil {
		t.Fatalf("NewSharedIdentityLauncher error = %v", err)
	}
	if _, err := NewSharedIdentityLauncher("agent", owner); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("a relative helper path must be refused, got %v", err)
	}
	if _, err := NewSharedIdentityLauncher(binary, RunnerIdentity{}); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("an unset owner must be refused, got %v", err)
	}
	if err := os.Chmod(binary, 0o707); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSharedIdentityLauncher(binary, owner); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("a world-writable helper must be refused, got %v", err)
	}
	if err := os.Chmod(binary, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(base, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSharedIdentityLauncher(binary, owner); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("a world-writable helper directory must be refused, got %v", err)
	}
}

// The launcher can never switch credentials. A spec asking for a different one
// is a wiring error and must fail rather than launch under the wrong identity.
func TestSharedIdentityLauncherRefusesCredentialSwitch(t *testing.T) {
	owner := sharedTestIdentity(t)
	launcher := SharedIdentityLauncher{HelperPath: "/bin/sh", Owner: owner}
	directory := t.TempDir()
	handle, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	spec := LaunchSpec{
		Executable:       filepath.Join(directory, "run.sh"),
		Directory:        directory,
		Arguments:        []string{"--ephemeral"},
		UID:              owner.UID + 1,
		GID:              owner.GID,
		WorkspaceRef:     runner.WorkspaceRef{Backend: SharedWorkspaceBackend, OwnerID: "x"},
		DirectoryHandle:  handle,
		ExecutableHandle: handle,
	}
	if _, err := launcher.Launch(context.Background(), spec, strings.NewReader("jit"), handle); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("Launch error = %v", err)
	}
	// A privileged-mode workspace ref must not be launchable here either.
	spec.UID = owner.UID
	spec.WorkspaceRef = runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "x"}
	if _, err := launcher.Launch(context.Background(), spec, strings.NewReader("jit"), handle); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("Launch error = %v", err)
	}
}

// setsidTestLauncher starts a session leader that deliberately detaches from
// the launched process. Only cgroup membership can still own it.
type setsidTestLauncher struct{ setsid string }

func (launcher setsidTestLauncher) Launch(
	_ context.Context,
	_ LaunchSpec,
	material io.Reader,
	cgroup *os.File,
) (int, error) {
	_, _ = io.Copy(io.Discard, material)
	// --fork guarantees a real fork: setsid exits immediately and the sleep it
	// leaves behind is in a new session and is no longer a child of this
	// process. A PID-based supervisor loses it here; only cgroup membership
	// still owns it, which is exactly the escape this test must close.
	command := exec.Command(launcher.setsid, "--fork", "sleep", "300")
	command.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(cgroup.Fd())}
	if err := command.Start(); err != nil {
		return 0, err
	}
	pid := command.Process.Pid
	if err := command.Wait(); err != nil {
		return 0, err
	}
	return pid, nil
}

// sharedDelegatedRuntime builds a real cgroup-backed runtime under this user's
// systemd delegation, or skips. It is the only way to prove the containment
// property that justifies StrongDescendantOwnership.
func sharedDelegatedRuntime(t *testing.T, launcher PipeLauncher) (*FileRuntime, RunnerIdentity) {
	t.Helper()
	owner := sharedTestIdentity(t)
	cgroupRoot, err := SystemdDelegatedCgroupRoot()
	if err != nil {
		t.Skip("no unified systemd delegated cgroup for this process")
	}
	if !safeDelegatedCgroupRoot(cgroupRoot, owner.UID) {
		t.Skip("the delegated cgroup subtree is not owned by this user")
	}
	fenceRoot := filepath.Join(safeUserDirectory(t, owner), "fences")
	nativeRuntime, err := newFileRuntime(
		cgroupRoot, fenceRoot, launcher,
		fileOwner{UID: owner.UID, GID: owner.GID},
	)
	if err != nil {
		t.Skipf("shared-identity cgroup admission unavailable: %v", err)
	}
	if err := proveCgroupDelegation(cgroupRoot); err != nil {
		t.Skipf("cgroup delegation cannot be proven: %v", err)
	}
	return nativeRuntime, owner
}

func TestSharedIdentityContainmentKillsSetsidDescendant(t *testing.T) {
	setsid, err := exec.LookPath("setsid")
	if err != nil {
		t.Skip("setsid is unavailable")
	}
	nativeRuntime, _ := sharedDelegatedRuntime(t, setsidTestLauncher{setsid: setsid})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	owner := containmentOwner("shared-identity-setsid-test")
	cgroup, err := nativeRuntime.EnsureCgroup(ctx, owner)
	if err != nil {
		t.Fatalf("EnsureCgroup error = %v", err)
	}
	containment := runner.ContainmentRef{
		Backend:    containmentBackend,
		OwnerID:    owner,
		Scope:      cgroup.Scope,
		HostEpoch:  cgroup.HostEpoch,
		FenceToken: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	t.Cleanup(func() {
		cleanup, cancelCleanup := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancelCleanup()
		_ = nativeRuntime.KillAndWait(cleanup, containment)
	})
	launchPID, err := nativeRuntime.Launch(ctx, LaunchSpec{Containment: containment}, strings.NewReader("jit"))
	if err != nil {
		t.Fatalf("Launch error = %v", err)
	}

	descendants := waitForCgroupProcesses(t, nativeRuntime, containment)
	if len(descendants) == 0 {
		t.Fatal("the detached descendant never appeared in the cgroup")
	}
	// The launched process is already gone; what survives is a descendant a
	// PID-based supervisor could no longer reach.
	for _, pid := range descendants {
		if pid == launchPID {
			t.Fatal("the launched process must have exited, leaving only its descendant")
		}
	}
	if err := nativeRuntime.KillAndWait(ctx, containment); err != nil {
		t.Fatalf("KillAndWait error = %v", err)
	}
	// Cleanup is only verified when the descendant set is provably empty. rmdir
	// on a cgroup that still holds any task fails with EBUSY, so the directory
	// being gone is the kernel's own proof that nothing remained.
	if _, err := os.Lstat(filepath.Join(nativeRuntime.cgroupRoot, containment.Scope)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the containment directory must be gone, lstat error = %v", err)
	}
	for _, pid := range descendants {
		if !processStoppedRunning(pid) {
			t.Fatalf("pid %d survived cgroup.kill", pid)
		}
	}
}

// processStoppedRunning accepts an absent process or an unreaped zombie. The
// descendant is orphaned by design, so whether its exit status has been
// collected depends on the ambient init, not on the containment boundary.
func processStoppedRunning(pid int) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}
	fields := strings.Fields(string(data))
	// The comm field is parenthesized and may contain spaces; the state letter
	// is the first field after the closing parenthesis.
	closing := strings.LastIndex(string(data), ")")
	if closing < 0 || len(fields) < 3 {
		return true
	}
	remaining := strings.Fields(string(data)[closing+1:])
	return len(remaining) == 0 || remaining[0] == "Z" || remaining[0] == "X"
}

func waitForCgroupProcesses(t *testing.T, nativeRuntime *FileRuntime, containment runner.ContainmentRef) []int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cgroup, err := nativeRuntime.openCgroup(containment)
		if err != nil {
			t.Fatalf("openCgroup error = %v", err)
		}
		data, readErr := readCgroupControl(cgroup, "cgroup.procs")
		_ = cgroup.Close()
		if readErr != nil {
			t.Fatalf("read cgroup.procs error = %v", readErr)
		}
		var pids []int
		for _, field := range strings.Fields(string(data)) {
			pid, convErr := strconv.Atoi(field)
			if convErr != nil {
				t.Fatalf("unexpected cgroup.procs content %q", string(data))
			}
			pids = append(pids, pid)
		}
		if len(pids) > 0 {
			return pids
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}
