//go:build linux

package linux

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/genm/tewake/internal/runner"
)

// SharedIdentityLauncher spawns the runner directly into the per-execution
// cgroup without changing credentials. It is the one place where this mode
// differs from ExecLauncher: no syscall.Credential is applied, because the
// Agent and the job deliberately share a Unix identity here.
//
// CLONE_INTO_CGROUP is still mandatory. A launcher that could not place the
// first instruction inside the cgroup would leave a window in which a
// descendant escapes cgroup.kill, and the containment claim would be false.
type SharedIdentityLauncher struct {
	HelperPath string
	// Owner is the Agent's own credential. The launched child inherits it; it is
	// carried here only so LaunchSpec validation can prove the caller is not
	// asking for a credential switch this launcher cannot perform.
	Owner RunnerIdentity
}

// NewSharedIdentityLauncher pins the helper binary. The binary may be owned by
// root or by the Agent's own user (a user-local install), but it must never be
// writable by group or other, and no ancestor may be either, or another local
// account could substitute the code that receives the one-job JIT.
func NewSharedIdentityLauncher(helperPath string, owner RunnerIdentity) (SharedIdentityLauncher, error) {
	if owner.UID <= 0 || owner.GID <= 0 || !filepath.IsAbs(helperPath) {
		return SharedIdentityLauncher{}, runner.ErrStrongOwnershipUnavailable
	}
	resolved, err := filepath.EvalSymlinks(helperPath)
	if err != nil || !filepath.IsAbs(resolved) {
		return SharedIdentityLauncher{}, runner.ErrStrongOwnershipUnavailable
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.Mode().IsRegular() ||
		(!rootOwned(info) && !ownedBy(info, owner.UID, owner.GID)) ||
		info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 ||
		!safeExecutableAncestry(filepath.Dir(resolved), owner) {
		return SharedIdentityLauncher{}, runner.ErrStrongOwnershipUnavailable
	}
	return SharedIdentityLauncher{HelperPath: filepath.Clean(resolved), Owner: owner}, nil
}

// safeExecutableAncestry accepts a directory chain owned by root or by the
// Agent's own credential, with no group- or world-writable component.
func safeExecutableAncestry(value string, owner RunnerIdentity) bool {
	cleaned := filepath.Clean(value)
	for current := cleaned; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			info.Mode().Perm()&0o022 != 0 {
			return false
		}
		if !rootOwned(info) && !ownedBy(info, owner.UID, owner.GID) {
			return false
		}
		if current == "/" {
			return true
		}
	}
}

func (launcher SharedIdentityLauncher) Launch(ctx context.Context, spec LaunchSpec, material io.Reader, cgroup *os.File) (int, error) {
	if err := ctx.Err(); err != nil || cgroup == nil || launcher.HelperPath == "" ||
		launcher.Owner.UID <= 0 || launcher.Owner.GID <= 0 ||
		spec.UID != launcher.Owner.UID || spec.GID != launcher.Owner.GID ||
		!validSharedLaunchSpec(spec) {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		return 0, runner.ErrStartFailed
	}
	defer statusReader.Close()
	command := exec.Command(launcher.HelperPath, helperArguments(spec)...)
	command.Stdin = material
	environment, err := sharedRunnerEnvironment(spec.UID)
	if err != nil {
		_ = statusWriter.Close()
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	command.Env = environment
	command.ExtraFiles = []*os.File{statusWriter, spec.DirectoryHandle, spec.ExecutableHandle}
	// No Credential: this mode deliberately keeps the Agent's identity. The
	// cgroup file descriptor is still the ownership boundary and is mandatory.
	command.SysProcAttr = &syscall.SysProcAttr{
		UseCgroupFD: true,
		CgroupFD:    int(cgroup.Fd()),
	}
	if err := command.Start(); err != nil {
		_ = statusWriter.Close()
		return 0, err
	}
	_ = statusWriter.Close()
	pid := command.Process.Pid
	// The cgroup is the lifecycle authority, while Wait is still mandatory: this
	// process is the parent of every listener and must reap it after either a
	// natural exit or cgroup.kill.
	_ = reapCommand(command)
	status := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(io.LimitReader(statusReader, 256))
		status <- data
	}()
	select {
	case data := <-status:
		if len(data) != 0 {
			return 0, runner.ErrStartFailed
		}
		return pid, nil
	case <-ctx.Done():
		_ = statusReader.Close()
		if closable, ok := material.(io.Closer); ok {
			_ = closable.Close()
		}
		<-status
		return 0, ctx.Err()
	}
}

func validSharedLaunchSpec(spec LaunchSpec) bool {
	if !filepath.IsAbs(spec.Executable) || !filepath.IsAbs(spec.Directory) ||
		spec.DirectoryHandle == nil || spec.ExecutableHandle == nil ||
		spec.UID <= 0 || spec.GID <= 0 ||
		spec.WorkspaceRef.Backend != SharedWorkspaceBackend || spec.WorkspaceRef.OwnerID == "" ||
		filepath.Clean(spec.Executable) != filepath.Join(filepath.Clean(spec.Directory), "run.sh") {
		return false
	}
	return fixedRunnerArguments(spec.Arguments)
}

// sharedRunnerEnvironment is the fixed environment for a same-identity launch.
// Unlike fixedRunnerEnvironment it does not assert the passwd primary group,
// because no credential switch happens: the child simply inherits this
// process's egid, which may legitimately differ from the passwd entry.
func sharedRunnerEnvironment(uid int) ([]string, error) {
	account, err := lookupUnixAccount(uid)
	if err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	const executionHome = "/proc/self/fd/4/.tewake-home"
	return []string{
		"HOME=" + executionHome,
		"XDG_CACHE_HOME=" + executionHome + "/.cache",
		"XDG_CONFIG_HOME=" + executionHome + "/.config",
		"TMPDIR=" + executionHome + "/.tmp",
		"LANG=C.UTF-8",
		"LOGNAME=" + account,
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"USER=" + account,
	}, nil
}

// RootlessRuntime is the in-process composition the root HelperServer performs
// across a socket in privileged mode: cgroup and fence ownership plus the
// workspace admission and pinning that must happen inside the launch
// transaction. No socket exists here because there is no privilege boundary to
// cross — which is precisely the property this mode gives up.
type RootlessRuntime struct {
	fileRuntime *FileRuntime
	workspace   *RootlessWorkspace
	runtimeRoot string
	owner       RunnerIdentity

	// launchMu serializes the admission-check/pin/launch sequence so two
	// concurrent starts cannot both observe a free slot.
	launchMu sync.Mutex
}

// NewRootlessRuntime proves, at construction, every property this mode's
// containment claim depends on. It never degrades: a missing systemd user
// delegation, a v1 or hybrid hierarchy, an absent cgroup.kill, or an unsafe
// root all return ErrStrongOwnershipUnavailable, and the node then advertises
// zero capacity rather than running jobs it could not reliably kill.
func NewRootlessRuntime(
	fenceRoot, runtimeRoot string,
	launcher PipeLauncher,
	workspace *RootlessWorkspace,
) (*RootlessRuntime, error) {
	if launcher == nil || workspace == nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	owner := workspace.Identity()
	if owner.UID <= 0 || owner.GID <= 0 || os.Geteuid() != owner.UID {
		// This constructor exists only for the unprivileged Agent. Root must use
		// the privileged Supervisor, whose UID separation this mode lacks.
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if !filepath.IsAbs(fenceRoot) || filepath.Clean(fenceRoot) != fenceRoot ||
		!filepath.IsAbs(runtimeRoot) || filepath.Clean(runtimeRoot) != runtimeRoot {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	// SystemdDelegatedCgroupRoot resolves this process's own cgroup from
	// /proc/self/cgroup and the cgroup2 mount, rejecting v1, hybrid, multiple,
	// and non-canonical hierarchies. Under `systemctl --user` that is the user's
	// delegated subtree; no cgroup path is ever accepted from CLI input.
	cgroupRoot, err := SystemdDelegatedCgroupRoot()
	if err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if !safeDelegatedCgroupRoot(cgroupRoot, owner.UID) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	fileRuntime, err := newFileRuntime(
		cgroupRoot, fenceRoot, launcher,
		fileOwner{UID: owner.UID, GID: owner.GID},
	)
	if err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	// newFileRuntime already ran ValidateAdmission, which reads cgroup.controllers
	// and cgroup.events and proves cgroup.kill is writable. Delegation is proven
	// once more by actually creating and removing a child cgroup: a subtree we
	// cannot subdivide is not a containment boundary, whatever its mode bits say.
	if err := proveCgroupDelegation(cgroupRoot); err != nil {
		return nil, err
	}
	if err := workspace.ValidateRuntimeRoot(context.Background(), runtimeRoot); err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return &RootlessRuntime{
		fileRuntime: fileRuntime,
		workspace:   workspace,
		runtimeRoot: runtimeRoot,
		owner:       owner,
	}, nil
}

// proveCgroupDelegation creates and removes a probe child. Reading mode bits is
// not enough: without systemd delegation the user owns the leaf but cannot
// create children, and cgroup.kill on a cgroup that can hold no descendants
// would be an empty containment promise.
func proveCgroupDelegation(cgroupRoot string) error {
	probe := filepath.Join(cgroupRoot, "tewake", ".delegation-probe")
	if err := os.MkdirAll(probe, 0o755); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	fd, err := syscall.Open(probe, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		_ = os.Remove(probe)
		return runner.ErrStrongOwnershipUnavailable
	}
	directory := os.NewFile(uintptr(fd), probe)
	if directory == nil {
		_ = syscall.Close(fd)
		_ = os.Remove(probe)
		return runner.ErrStrongOwnershipUnavailable
	}
	_, readErr := readCgroupControl(directory, "cgroup.events")
	writable := cgroupControlWritable(directory, "cgroup.kill")
	closeErr := directory.Close()
	// Removing the probe is part of the proof, not cleanup courtesy: a leaf this
	// process cannot remove would become permanent slot residue and would make
	// every later admission check fail anyway.
	if err := os.Remove(probe); err != nil || readErr != nil || !writable || closeErr != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (nativeRuntime *RootlessRuntime) EnsureCgroup(ctx context.Context, owner string) (Cgroup, error) {
	return nativeRuntime.fileRuntime.EnsureCgroup(ctx, owner)
}

func (nativeRuntime *RootlessRuntime) LockFence(ctx context.Context, containment runner.ContainmentRef) (Fence, error) {
	return nativeRuntime.fileRuntime.LockFence(ctx, containment)
}

func (nativeRuntime *RootlessRuntime) KillAndWait(ctx context.Context, containment runner.ContainmentRef) error {
	return nativeRuntime.fileRuntime.KillAndWait(ctx, containment)
}

func (nativeRuntime *RootlessRuntime) WaitEmpty(ctx context.Context, containment runner.ContainmentRef) error {
	return nativeRuntime.fileRuntime.WaitEmpty(ctx, containment)
}

func (nativeRuntime *RootlessRuntime) Alive(ctx context.Context, containment runner.ContainmentRef, pid int) (bool, error) {
	return nativeRuntime.fileRuntime.Alive(ctx, containment, pid)
}

func (nativeRuntime *RootlessRuntime) ValidateAdmission(ctx context.Context) error {
	if err := nativeRuntime.workspace.ValidateRuntimeRoot(ctx, nativeRuntime.runtimeRoot); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nativeRuntime.fileRuntime.ValidateAdmission(ctx)
}

// Launch performs the admission, pinning, and spawn that the privileged
// HelperServer performs behind its socket. The workspace directory and run.sh
// are opened and identity-checked here and handed to the child as descriptors,
// so a rename between verification and exec cannot redirect the launch.
func (nativeRuntime *RootlessRuntime) Launch(ctx context.Context, spec LaunchSpec, material io.Reader) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	name := filepath.Base(filepath.Clean(spec.Directory))
	if !validWorkspaceName(name) ||
		filepath.Clean(spec.Directory) != filepath.Join(nativeRuntime.runtimeRoot, "executions", name) ||
		spec.UID != nativeRuntime.owner.UID || spec.GID != nativeRuntime.owner.GID {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	nativeRuntime.launchMu.Lock()
	defer nativeRuntime.launchMu.Unlock()

	slotBusy, err := nativeRuntime.fileRuntime.SlotBusy(ctx, spec.Containment)
	if err != nil || slotBusy {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	executions, err := nativeRuntime.openExecutionsRoot(ctx)
	if err != nil {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	workspaceBusy, err := nativeRuntime.workspace.SlotBusy(ctx, executions, name)
	if err != nil || workspaceBusy {
		_ = executions.Close()
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	pinned, pinErr := nativeRuntime.workspace.PinLaunch(ctx, executions, name, spec.WorkspaceRef)
	closeErr := executions.Close()
	if pinErr != nil || closeErr != nil {
		if pinned != nil {
			pinned.Close()
		}
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	defer pinned.Close()
	pinnedSpec := spec
	pinnedSpec.DirectoryHandle = pinned.directory
	pinnedSpec.ExecutableHandle = pinned.executable
	return nativeRuntime.fileRuntime.Launch(ctx, pinnedSpec, material)
}

// FinalizeCleanup joins verified process absence, workspace removal, and
// durable fence finalization in one transaction. It mirrors the privileged
// helper's finalize operation exactly, including the crash boundary where the
// workspace is already gone but the tombstone was never published.
func (nativeRuntime *RootlessRuntime) FinalizeCleanup(
	ctx context.Context,
	containment runner.ContainmentRef,
	root *os.Root,
	name string,
	expected runner.WorkspaceRef,
) error {
	if err := ctx.Err(); err != nil || root == nil || !validWorkspaceName(name) ||
		expected.Backend != SharedWorkspaceBackend || expected.OwnerID == "" {
		return runner.ErrCleanupFailed
	}
	executions, err := nativeRuntime.openExecutionsRoot(ctx)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	defer executions.Close()
	finalized, err := nativeRuntime.fileRuntime.FenceFinalized(ctx, containment)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	if finalized {
		absent, absenceErr := nativeRuntime.workspace.Absent(ctx, executions, name)
		if absenceErr != nil || !absent {
			return runner.ErrCleanupFailed
		}
	} else {
		ref, observeErr := nativeRuntime.workspace.Observe(ctx, executions, name)
		switch {
		case observeErr == nil && ref == expected:
			if removeErr := nativeRuntime.workspace.Remove(ctx, executions, name); removeErr != nil {
				return runner.ErrCleanupFailed
			}
		case observeErr == nil:
			// A present workspace with a different identity is not this record's
			// workspace. Removing it would delete an unrelated owner's credentials.
			return runner.ErrCleanupFailed
		}
		absent, absenceErr := nativeRuntime.workspace.Absent(ctx, executions, name)
		if absenceErr != nil || !absent {
			return runner.ErrCleanupFailed
		}
		if err := nativeRuntime.fileRuntime.ValidateFinalization(ctx, containment); err != nil {
			return runner.ErrCleanupFailed
		}
	}
	if err := nativeRuntime.fileRuntime.FinalizeFence(ctx, containment); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (nativeRuntime *RootlessRuntime) GarbageCollectFence(ctx context.Context, containment runner.ContainmentRef) error {
	return nativeRuntime.fileRuntime.GarbageCollectFence(ctx, containment)
}

func (nativeRuntime *RootlessRuntime) openExecutionsRoot(ctx context.Context) (*os.Root, error) {
	if err := nativeRuntime.workspace.ValidateRuntimeRoot(ctx, nativeRuntime.runtimeRoot); err != nil {
		return nil, err
	}
	return os.OpenRoot(filepath.Join(nativeRuntime.runtimeRoot, "executions"))
}

// RootlessWorkspace is the stat identity authority for the shared-identity
// mode. It records inode, device, and the Agent's own credential, and encodes
// them under SharedWorkspaceBackend so a privileged-mode workspace can never
// satisfy it, nor it a privileged one.
type RootlessWorkspace struct {
	UID int
	GID int
}

func NewRootlessWorkspace(uid, gid int) (*RootlessWorkspace, error) {
	if uid <= 0 || gid <= 0 {
		// uid 0 would mean the Agent is root, in which case the privileged
		// Supervisor with a dedicated runner account is the correct boundary.
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return &RootlessWorkspace{UID: uid, GID: gid}, nil
}

func (workspace *RootlessWorkspace) Identity() RunnerIdentity {
	return RunnerIdentity{UID: workspace.UID, GID: workspace.GID}
}

// RunnerIdentity and AgentIdentity are the same credential by construction.
// That equality is the single property this mode drops, and it is asserted
// here rather than left implicit.
func (workspace *RootlessWorkspace) RunnerIdentity() RunnerIdentity { return workspace.Identity() }
func (workspace *RootlessWorkspace) AgentIdentity() RunnerIdentity  { return workspace.Identity() }

// ValidateRuntimeRoot requires an absolute, canonical root owned by the Agent
// with mode 0700, no symlink component, and no group- or world-writable
// ancestor. Ancestors may be owned by root or by the Agent; anything else could
// let a third local account replace the tree between validation and use.
func (workspace *RootlessWorkspace) ValidateRuntimeRoot(ctx context.Context, root string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if workspace.UID <= 0 || workspace.GID <= 0 {
		return errors.New("shared runner identity is not configured")
	}
	if !filepath.IsAbs(root) {
		return errors.New("runtime root is not absolute")
	}
	cleaned := filepath.Clean(root)
	if cleaned != root || cleaned == "/" {
		return errors.New("runtime root must be canonical and scoped")
	}
	owner := fileOwner{UID: workspace.UID, GID: workspace.GID}
	if !safeOwnedDirectory(cleaned, 0o700, owner) {
		return errors.New("runtime root must be agent-owned 0700 with a safe ancestry")
	}
	info, err := os.Lstat(cleaned)
	if err != nil {
		return errors.New("runtime root cannot be observed")
	}
	runtimeRoot, err := os.OpenRoot(cleaned)
	if err != nil {
		return errors.New("runtime root cannot be pinned")
	}
	defer runtimeRoot.Close()
	opened, err := runtimeRoot.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		return errors.New("runtime root changed while opening")
	}
	executions, err := runtimeRoot.Lstat("executions")
	if err != nil || !executions.IsDir() || executions.Mode()&os.ModeSymlink != 0 ||
		!ownedBy(executions, workspace.UID, workspace.GID) ||
		executions.Mode().Perm() != 0o700 {
		return errors.New("executions root must be agent-owned 0700")
	}
	executionsRoot, err := runtimeRoot.OpenRoot("executions")
	if err != nil {
		return errors.New("executions root cannot be pinned")
	}
	defer executionsRoot.Close()
	openedExecutions, err := executionsRoot.Stat(".")
	if err != nil || !os.SameFile(executions, openedExecutions) {
		return errors.New("executions root changed while opening")
	}
	return nil
}

// Prepare adopts the tree the core already materialized from the verified
// package archive. In privileged mode the root Supervisor rebuilds that tree
// from its own copy because the Agent is a different, less trusted identity;
// here the Agent is the runner, so a second copy would protect nothing. The
// archive digest remains the package authority either way.
func (workspace *RootlessWorkspace) Prepare(ctx context.Context, root *os.Root, name string) (runner.WorkspaceRef, error) {
	if err := ctx.Err(); err != nil {
		return runner.WorkspaceRef{}, err
	}
	if root == nil || !validWorkspaceName(name) || workspace.validateExecutionsRoot(ctx, root) != nil {
		return runner.WorkspaceRef{}, errors.New("missing or unsafe workspace root")
	}
	if busy, err := workspace.singleWorkspace(root, name); err != nil || busy {
		return runner.WorkspaceRef{}, errors.New("runner slot has another workspace")
	}
	workspaceRoot, err := root.OpenRoot(name)
	if err != nil {
		return runner.WorkspaceRef{}, err
	}
	defer workspaceRoot.Close()
	for _, directory := range []string{
		".tewake-home",
		".tewake-home/.config",
		".tewake-home/.cache",
		".tewake-home/.tmp",
	} {
		if err := workspaceRoot.MkdirAll(directory, 0o700); err != nil {
			return runner.WorkspaceRef{}, err
		}
	}
	return workspace.Observe(ctx, root, name)
}

func (workspace *RootlessWorkspace) Observe(ctx context.Context, root *os.Root, name string) (runner.WorkspaceRef, error) {
	if err := ctx.Err(); err != nil {
		return runner.WorkspaceRef{}, err
	}
	if root == nil || !validWorkspaceName(name) || workspace.validateExecutionsRoot(ctx, root) != nil {
		return runner.WorkspaceRef{}, errors.New("missing workspace root")
	}
	info, err := root.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return runner.WorkspaceRef{}, errors.New("workspace identity is unsafe")
	}
	ref := sharedWorkspaceRef(info, workspace.UID, workspace.GID)
	if ref == (runner.WorkspaceRef{}) {
		return runner.WorkspaceRef{}, errors.New("workspace owner changed")
	}
	return ref, nil
}

func (workspace *RootlessWorkspace) Remove(ctx context.Context, root *os.Root, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if root == nil || !validWorkspaceName(name) || workspace.validateExecutionsRoot(ctx, root) != nil {
		return errors.New("missing workspace root")
	}
	info, err := root.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		!ownedBy(info, workspace.UID, workspace.GID) ||
		info.Mode().Perm() != 0o700 {
		return errors.New("workspace identity is unsafe")
	}
	opened, err := root.OpenRoot(name)
	if err != nil {
		return errors.New("workspace cannot be pinned")
	}
	openedInfo, statErr := opened.Stat(".")
	closeErr := opened.Close()
	if statErr != nil || closeErr != nil || !os.SameFile(info, openedInfo) {
		return errors.New("workspace changed before removal")
	}
	if err := root.RemoveAll(name); err != nil {
		return err
	}
	if _, err := root.Lstat(name); !errors.Is(err, fs.ErrNotExist) {
		return errors.New("workspace remains after removal")
	}
	return nil
}

// Absent is a read-only proof used after removal or a durable finalized fence.
// It never treats an unsafe replacement as absence.
func (workspace *RootlessWorkspace) Absent(ctx context.Context, root *os.Root, name string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if root == nil || !validWorkspaceName(name) || workspace.validateExecutionsRoot(ctx, root) != nil {
		return false, errors.New("missing workspace root")
	}
	if _, err := root.Lstat(name); errors.Is(err, fs.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return false, nil
}

func (workspace *RootlessWorkspace) SlotBusy(ctx context.Context, root *os.Root, candidate string) (bool, error) {
	if err := ctx.Err(); err != nil || root == nil || !validWorkspaceName(candidate) ||
		workspace.validateExecutionsRoot(ctx, root) != nil {
		return false, errors.New("executions root is unsafe")
	}
	return workspace.singleWorkspace(root, candidate)
}

// PinLaunch opens the workspace and run.sh before launch and verifies their
// inode/owner identity against the durable WorkspaceRef. The caller must keep
// both descriptors open until the launcher has duplicated them into the child.
func (workspace *RootlessWorkspace) PinLaunch(
	ctx context.Context,
	root *os.Root,
	name string,
	expected runner.WorkspaceRef,
) (*pinnedWorkspace, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if root == nil || !validWorkspaceName(name) || workspace.validateExecutionsRoot(ctx, root) != nil {
		return nil, errors.New("workspace root is unsafe")
	}
	directoryRoot, err := root.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	directory, err := directoryRoot.Open(".")
	if err != nil {
		_ = directoryRoot.Close()
		return nil, err
	}
	defer directoryRoot.Close()
	info, err := directory.Stat()
	pathInfo, pathErr := root.Lstat(name)
	if err != nil || pathErr != nil || !os.SameFile(info, pathInfo) ||
		sharedWorkspaceRef(info, workspace.UID, workspace.GID) != expected {
		_ = directory.Close()
		return nil, errors.New("workspace identity changed before launch")
	}
	executable, err := directoryRoot.Open("run.sh")
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	executableInfo, err := executable.Stat()
	if err != nil || !executableInfo.Mode().IsRegular() || !singleLink(executableInfo) ||
		!ownedBy(executableInfo, workspace.UID, workspace.GID) ||
		executableInfo.Mode().Perm()&0o100 == 0 {
		_ = executable.Close()
		_ = directory.Close()
		return nil, errors.New("runner executable is unsafe")
	}
	return &pinnedWorkspace{directory: directory, executable: executable}, nil
}

// singleWorkspace proves the executions root holds exactly the candidate and
// nothing else. Residue from another execution is treated as busy so a failed
// cleanup can never be turned into a fresh launch.
func (workspace *RootlessWorkspace) singleWorkspace(root *os.Root, candidate string) (bool, error) {
	directory, err := root.Open(".")
	if err != nil {
		return false, err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return false, err
	}
	found := false
	for _, entry := range entries {
		if !validWorkspaceName(entry.Name()) {
			return false, errors.New("executions root has an unsafe entry")
		}
		if entry.Name() != candidate {
			return true, nil
		}
		if found {
			return false, errors.New("duplicate workspace entry")
		}
		found = true
		info, err := root.Lstat(candidate)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			!ownedBy(info, workspace.UID, workspace.GID) || info.Mode().Perm() != 0o700 {
			return false, errors.New("candidate workspace identity is unsafe")
		}
	}
	if !found {
		return false, errors.New("candidate workspace is absent")
	}
	return false, nil
}

func (workspace *RootlessWorkspace) validateExecutionsRoot(ctx context.Context, root *os.Root) error {
	info, err := root.Stat(".")
	if err != nil || !info.IsDir() ||
		!ownedBy(info, workspace.UID, workspace.GID) ||
		info.Mode().Perm() != 0o700 {
		return errors.New("executions root is unsafe")
	}
	pathInfo, err := os.Lstat(root.Name())
	if err != nil || !os.SameFile(info, pathInfo) {
		return errors.New("executions root path changed")
	}
	if filepath.Base(filepath.Clean(root.Name())) != "executions" {
		return errors.New("unexpected executions root")
	}
	return workspace.ValidateRuntimeRoot(ctx, filepath.Dir(filepath.Clean(root.Name())))
}

// sharedWorkspaceRef is deliberately a different encoding from workspaceRef.
// The backend string is part of the identity, so a ref produced by one mode can
// never be accepted by the other even if the inode happened to match.
func sharedWorkspaceRef(info fs.FileInfo, uid, gid int) runner.WorkspaceRef {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != uid || int(stat.Gid) != gid {
		return runner.WorkspaceRef{}
	}
	return runner.WorkspaceRef{
		Backend: SharedWorkspaceBackend,
		OwnerID: "shared:dev:" + strconv.FormatUint(uint64(stat.Dev), 16) +
			":ino:" + strconv.FormatUint(uint64(stat.Ino), 16) +
			":uid:" + strconv.Itoa(int(stat.Uid)) +
			":gid:" + strconv.Itoa(int(stat.Gid)),
	}
}

func lookupUnixAccount(uid int) (string, error) {
	account, err := user.LookupId(strconv.Itoa(uid))
	if err != nil || account.Uid != strconv.Itoa(uid) || account.Username == "" ||
		strings.ContainsAny(account.Username, "=\x00\r\n") {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	return account.Username, nil
}

var _ Runtime = (*RootlessRuntime)(nil)
var _ RuntimeAdmission = (*RootlessRuntime)(nil)
var _ RuntimeCleanupFinalizer = (*RootlessRuntime)(nil)
var _ Workspace = (*RootlessWorkspace)(nil)
var _ WorkspaceSlotAdmission = (*RootlessWorkspace)(nil)
var _ WorkspaceAbsence = (*RootlessWorkspace)(nil)
var _ PipeLauncher = SharedIdentityLauncher{}
