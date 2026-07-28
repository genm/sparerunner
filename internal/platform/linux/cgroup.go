//go:build linux

package linux

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

// PipeLauncher performs an atomic spawn into the opened cgroup. Its JIT reader
// is an anonymous one-shot pipe owned by Adapter.Start; implementations must
// drain it synchronously before returning and must never place it in the root
// Supervisor/helper argv, environment, logs, unit properties, or files. The
// dedicated-slot child appends it only to the final official runner argv because
// that is the upstream runner's required JIT interface.
//
// cgroup is the already validated cgroup-v2 directory FD. A launcher that cannot
// use it for CLONE_INTO_CGROUP must return an error rather than fall back to a
// post-fork PID move.
type PipeLauncher interface {
	Launch(context.Context, LaunchSpec, io.Reader, *os.File) (int, error)
}

// ExecLauncher starts a trusted helper mode from the agent binary. The helper
// is the first instruction in the cgroup and under the slot credential; it
// drains the JIT pipe then syscall.Exec's the verified runner. A successful
// return means that exec completed, not merely that a helper process started.
type ExecLauncher struct {
	HelperPath string
}

func NewExecLauncher(helperPath string) (ExecLauncher, error) {
	if !filepath.IsAbs(helperPath) {
		return ExecLauncher{}, runner.ErrStrongOwnershipUnavailable
	}
	resolved, err := filepath.EvalSymlinks(helperPath)
	if err != nil || !filepath.IsAbs(resolved) {
		return ExecLauncher{}, runner.ErrStrongOwnershipUnavailable
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.Mode().IsRegular() || !rootOwned(info) ||
		info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 ||
		!safeRootedDirectory(filepath.Dir(resolved), 0) {
		return ExecLauncher{}, runner.ErrStrongOwnershipUnavailable
	}
	return ExecLauncher{HelperPath: filepath.Clean(resolved)}, nil
}

func (launcher ExecLauncher) Launch(ctx context.Context, spec LaunchSpec, material io.Reader, cgroup *os.File) (int, error) {
	if err := ctx.Err(); err != nil || cgroup == nil || launcher.HelperPath == "" || !validLaunchSpec(spec) {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		return 0, runner.ErrStartFailed
	}
	defer statusReader.Close()
	command := exec.Command(launcher.HelperPath, helperArguments(spec)...)
	command.Stdin = material
	environment, err := fixedRunnerEnvironment(spec.UID, spec.GID)
	if err != nil {
		_ = statusWriter.Close()
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	command.Env = environment
	command.ExtraFiles = []*os.File{statusWriter, spec.DirectoryHandle, spec.ExecutableHandle}
	command.SysProcAttr = &syscall.SysProcAttr{
		Credential:  &syscall.Credential{Uid: uint32(spec.UID), Gid: uint32(spec.GID)},
		UseCgroupFD: true,
		CgroupFD:    int(cgroup.Fd()),
	}
	if err := command.Start(); err != nil {
		_ = statusWriter.Close()
		return 0, err
	}
	_ = statusWriter.Close()
	pid := command.Process.Pid
	// The cgroup is the lifecycle authority, while Wait is still mandatory:
	// a long-lived Supervisor is the parent of every listener and must reap it
	// after either natural exit or cgroup.kill.  No output or secret-bearing
	// command value is retained by this reaper.
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

func reapCommand(command *exec.Cmd) <-chan error {
	reaped := make(chan error, 1)
	go func() {
		reaped <- command.Wait()
		close(reaped)
	}()
	return reaped
}

const helperModeArgument = "--sparerunner-linux-launcher-helper"

func helperArguments(spec LaunchSpec) []string {
	args := []string{helperModeArgument, "--"}
	return append(args, spec.Arguments...)
}

func validLaunchSpec(spec LaunchSpec) bool {
	if !filepath.IsAbs(spec.Executable) || !filepath.IsAbs(spec.Directory) ||
		spec.DirectoryHandle == nil || spec.ExecutableHandle == nil ||
		spec.UID < 0 || spec.GID < 0 ||
		spec.WorkspaceRef.Backend != WorkspaceBackend || spec.WorkspaceRef.OwnerID == "" ||
		filepath.Clean(spec.Executable) != filepath.Join(filepath.Clean(spec.Directory), "run.sh") {
		return false
	}
	return fixedRunnerArguments(spec.Arguments)
}

// RunExecLauncherHelper is the application entrypoint for the trusted helper
// mode. The agent main must call it before normal command parsing and exit when
// handled is true. The only value read from stdin is the one-job JIT material.
// The upstream runner requires --jitconfig, so it is intentionally present in
// the exec'd runner argv only; that argv is scoped to the dedicated slot UID and
// is never logged, persisted, or passed through the parent process arguments.
func RunExecLauncherHelper(args []string) (handled bool, err error) {
	if len(args) == 0 || args[0] != helperModeArgument {
		return false, nil
	}
	if len(args) < 3 || args[1] != "--" {
		reportHelperFailure()
		return true, runner.ErrStartFailed
	}
	if !fixedRunnerArguments(args[2:]) {
		reportHelperFailure()
		return true, runner.ErrStartFailed
	}
	jit, readErr := io.ReadAll(io.LimitReader(os.Stdin, maxJITMaterialBytes+1))
	if readErr != nil || len(jit) == 0 || len(jit) > maxJITMaterialBytes {
		clear(jit)
		reportHelperFailure()
		return true, runner.ErrStartFailed
	}
	defer clear(jit)
	// fd 4 is the pinned workspace directory and fd 5 is the pinned run.sh.
	// Both are supplied by the root Supervisor through ExtraFiles. Keeping the
	// directory descriptor inherited also makes HOME/TMPDIR immune to a later
	// rename in the Agent-writable executions parent.
	if err := syscall.Fchdir(4); err != nil {
		reportHelperFailure()
		return true, runner.ErrStartFailed
	}
	// The status descriptor must close only when exec succeeds. It is intentionally
	// not inherited by the runner, so the parent can distinguish exec from helper
	// startup without observing or retaining JIT material.
	syscall.CloseOnExec(3)
	runnerArgs := append([]string{"run.sh"}, args[2:]...)
	runnerArgs = append(runnerArgs, "--jitconfig", string(jit))
	if err := syscall.Exec("/proc/self/fd/5", runnerArgs, os.Environ()); err != nil {
		reportHelperFailure()
		return true, err
	}
	return true, nil
}

func reportHelperFailure() {
	status := os.NewFile(uintptr(3), "sparerunner-launch-status")
	if status == nil {
		return
	}
	_, _ = status.Write([]byte("exec failed\n"))
	_ = status.Close()
}

// FileRuntime provides durable fence records and cgroup-v2 cleanup.  It is
// usable only on Linux; NewFileRuntime rejects other hosts so cross-compilation
// remains safe while platform tests can use Runtime fakes.
type FileRuntime struct {
	cgroupRoot string
	fenceRoot  string
	launcher   PipeLauncher
	// owner is the credential every durable fence file and pinned directory must
	// carry. The privileged Supervisor uses rootFileOwner, which reproduces the
	// original root-only checks exactly. The opt-in shared-identity runtime uses
	// the Agent's own credential; it never widens what root mode accepts.
	owner fileOwner
}

// fileOwner is the expected uid/gid of the durable state one runtime owns.
type fileOwner struct{ UID, GID int }

var rootFileOwner = fileOwner{UID: 0, GID: 0}

func (owner fileOwner) owns(info os.FileInfo) bool {
	return info != nil && ownedBy(info, owner.UID, owner.GID)
}

func (owner fileOwner) isRoot() bool { return owner.UID == 0 && owner.GID == 0 }

const (
	finalizedFenceDirectory = ".finalized"
	finalizedFenceVersion   = "sparerunner-finalized-fence-v2"
	fenceStateVersion       = "sparerunner-containment-fence-v2"
	fenceStateActive        = "active"
	fenceStateLaunched      = "launched"
	fenceStateRevoked       = "revoked"
)

// NewSystemdFileRuntime resolves only the Supervisor process's own delegated
// cgroup-v2 subtree. No host-specific cgroup path is accepted from CLI input,
// and the host cgroup root is never a valid fallback.
func NewSystemdFileRuntime(fenceRoot string, launcher PipeLauncher) (*FileRuntime, error) {
	cgroupRoot, err := SystemdDelegatedCgroupRoot()
	if err != nil {
		return nil, err
	}
	return NewFileRuntime(cgroupRoot, fenceRoot, launcher)
}

// SystemdDelegatedCgroupRoot derives the current unit's cgroup from the kernel
// proc views and rejects v1, hybrid, multiple, root, and non-canonical paths.
func SystemdDelegatedCgroupRoot() (string, error) {
	cgroupData, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	mountData, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	return delegatedCgroupRoot(cgroupData, mountData)
}

func delegatedCgroupRoot(cgroupData, mountData []byte) (string, error) {
	cgroupPath, err := parseUnifiedCgroupPath(cgroupData)
	if err != nil {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	mountPoint, err := parseCgroup2MountPoint(mountData)
	if err != nil {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	relative := strings.TrimPrefix(cgroupPath, "/")
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != filepath.FromSlash(relative) {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	resolved := filepath.Join(mountPoint, filepath.FromSlash(relative))
	expectedPrefix := filepath.Clean(mountPoint) + string(filepath.Separator)
	if !strings.HasPrefix(filepath.Clean(resolved)+string(filepath.Separator), expectedPrefix) {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	return filepath.Clean(resolved), nil
}

func parseUnifiedCgroupPath(data []byte) (string, error) {
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	parts := strings.Split(lines[0], ":")
	if len(parts) != 3 || parts[0] != "0" || parts[1] != "" ||
		!strings.HasPrefix(parts[2], "/") || parts[2] == "/" ||
		strings.ContainsRune(parts[2], '\x00') ||
		path.Clean(parts[2]) != parts[2] {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	for _, component := range strings.Split(parts[2], "/") {
		if component == "." || component == ".." {
			return "", runner.ErrStrongOwnershipUnavailable
		}
	}
	return parts[2], nil
}

func parseCgroup2MountPoint(data []byte) (string, error) {
	var result string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		separator := -1
		for index, field := range fields {
			if field == "-" {
				separator = index
				break
			}
		}
		if separator < 6 || separator+3 > len(fields) || fields[separator+1] != "cgroup2" {
			continue
		}
		if fields[3] != "/" {
			return "", runner.ErrStrongOwnershipUnavailable
		}
		mountPoint, err := unescapeMountInfoPath(fields[4])
		if err != nil || !filepath.IsAbs(mountPoint) || filepath.Clean(mountPoint) != mountPoint {
			return "", runner.ErrStrongOwnershipUnavailable
		}
		if result != "" {
			return "", runner.ErrStrongOwnershipUnavailable
		}
		result = mountPoint
	}
	if result == "" {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	return result, nil
}

func unescapeMountInfoPath(value string) (string, error) {
	var output strings.Builder
	for index := 0; index < len(value); {
		if value[index] != '\\' {
			if value[index] == 0 {
				return "", runner.ErrStrongOwnershipUnavailable
			}
			output.WriteByte(value[index])
			index++
			continue
		}
		if index+4 > len(value) {
			return "", runner.ErrStrongOwnershipUnavailable
		}
		switch value[index : index+4] {
		case `\040`:
			output.WriteByte(' ')
		case `\011`:
			output.WriteByte('\t')
		case `\012`:
			output.WriteByte('\n')
		case `\134`:
			output.WriteByte('\\')
		default:
			return "", runner.ErrStrongOwnershipUnavailable
		}
		index += 4
	}
	return output.String(), nil
}

func NewFileRuntime(cgroupRoot, fenceRoot string, launcher PipeLauncher) (*FileRuntime, error) {
	return newFileRuntime(cgroupRoot, fenceRoot, launcher, rootFileOwner)
}

func newFileRuntime(cgroupRoot, fenceRoot string, launcher PipeLauncher, owner fileOwner) (*FileRuntime, error) {
	if runtime.GOOS != "linux" || cgroupRoot == "" || fenceRoot == "" || launcher == nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if !filepath.IsAbs(cgroupRoot) || !filepath.IsAbs(fenceRoot) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if filepath.Clean(cgroupRoot) != cgroupRoot || filepath.Clean(fenceRoot) != fenceRoot ||
		cgroupRoot == "/" || fenceRoot == "/" {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if err := os.MkdirAll(fenceRoot, 0o700); err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	nativeRuntime := &FileRuntime{
		cgroupRoot: filepath.Clean(cgroupRoot),
		fenceRoot:  filepath.Clean(fenceRoot),
		launcher:   launcher,
		owner:      owner,
	}
	if err := nativeRuntime.ValidateAdmission(context.Background()); err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return nativeRuntime, nil
}

// ValidateAdmission reopens and pins the delegated cgroup and fence roots on
// every readiness probe. It performs no mkdir, repair, JIT handling, or launch.
func (runtime *FileRuntime) ValidateAdmission(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if runtime == nil ||
		!runtime.safeCgroupRoot() ||
		!isCgroupV2(runtime.cgroupRoot) ||
		!safeOwnedDirectory(runtime.fenceRoot, 0o700, runtime.owner) {
		return runner.ErrStrongOwnershipUnavailable
	}
	cgroup, err := runtime.openPinnedCgroupRoot()
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	defer cgroup.Close()
	if _, err := readCgroupControl(cgroup, "cgroup.controllers"); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	events, err := readCgroupControl(cgroup, "cgroup.events")
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if _, err := cgroupPopulatedData(events); err != nil ||
		!cgroupControlWritable(cgroup, "cgroup.kill") {
		return runner.ErrStrongOwnershipUnavailable
	}
	fence, err := openPinnedDirectoryOwned(runtime.fenceRoot, runtime.owner.owns)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return fence.Close()
}

// safeCgroupRoot keeps the privileged walk unchanged and applies the delegation
// proof only in shared-identity mode.
func (runtime *FileRuntime) safeCgroupRoot() bool {
	if runtime.owner.isRoot() {
		return safeRootedDirectory(runtime.cgroupRoot, 0)
	}
	return safeDelegatedCgroupRoot(runtime.cgroupRoot, runtime.owner.UID)
}

func (runtime *FileRuntime) openPinnedCgroupRoot() (*os.File, error) {
	if runtime.owner.isRoot() {
		return openPinnedDirectory(runtime.cgroupRoot)
	}
	uid := runtime.owner.UID
	return openPinnedDirectoryOwned(runtime.cgroupRoot, func(info os.FileInfo) bool {
		stat, ok := info.Sys().(*syscall.Stat_t)
		return ok && uid != 0 && int(stat.Uid) == uid
	})
}

func openPinnedDirectory(name string) (*os.File, error) {
	return openPinnedDirectoryOwned(name, rootOwned)
}

func openPinnedDirectoryOwned(name string, owned func(os.FileInfo) bool) (*os.File, error) {
	info, err := os.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !owned(info) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	fd, err := syscall.Open(name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	directory := os.NewFile(uintptr(fd), name)
	if directory == nil {
		_ = syscall.Close(fd)
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	opened, err := directory.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = directory.Close()
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return directory, nil
}

func (runtime *FileRuntime) EnsureCgroup(ctx context.Context, owner string) (Cgroup, error) {
	if err := ctx.Err(); err != nil || !validOwner(owner) {
		return Cgroup{}, runner.ErrStrongOwnershipUnavailable
	}
	scope := filepath.Join("sparerunner", owner)
	path := filepath.Join(runtime.cgroupRoot, scope)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return Cgroup{}, runner.ErrStrongOwnershipUnavailable
	}
	epoch, err := currentBootEpoch()
	if err != nil {
		return Cgroup{}, runner.ErrStrongOwnershipUnavailable
	}
	containment := runner.ContainmentRef{
		Backend: containmentBackend, OwnerID: owner, Scope: scope, HostEpoch: epoch,
	}
	opened, err := runtime.openCgroup(containment)
	if err != nil {
		return Cgroup{}, runner.ErrStrongOwnershipUnavailable
	}
	defer opened.Close()
	populated, err := cgroupPopulatedFD(opened)
	if err != nil || populated {
		return Cgroup{}, runner.ErrStrongOwnershipUnavailable
	}
	return Cgroup{Scope: scope, HostEpoch: epoch}, nil
}

func (runtime *FileRuntime) LockFence(ctx context.Context, containment runner.ContainmentRef) (Fence, error) {
	if err := ctx.Err(); err != nil || !runtime.validScope(containment) || !canonicalToken(containment.FenceToken) {
		return nil, runner.ErrCleanupFailed
	}
	finalized, err := runtime.fenceFinalized(containment)
	if err != nil {
		return nil, runner.ErrCleanupFailed
	}
	if finalized {
		return finalizedFence{}, nil
	}
	directory := filepath.Join(runtime.fenceRoot, containment.OwnerID)
	if err := os.MkdirAll(directory, 0o700); err != nil || !safeOwnedDirectory(directory, 0o700, runtime.owner) {
		return nil, runner.ErrCleanupFailed
	}
	// The lock is owner-scoped rather than token-scoped. A corrupt replay with a
	// different token must still serialize against the one physical cgroup and
	// must not create a second authority beside the committed token.
	lockPath := filepath.Join(directory, "containment.lock")
	lock, lockCreated, err := openFenceFile(lockPath)
	if err != nil {
		return nil, runner.ErrCleanupFailed
	}
	if err := lockFile(ctx, int(lock.Fd())); err != nil {
		_ = lock.Close()
		return nil, err
	}
	closeLock := func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}
	if lockCreated {
		if err := initializeFenceFile(lock, "sparerunner-containment-lock-v1\n", directory); err != nil {
			closeLock()
			return nil, runner.ErrCleanupFailed
		}
	}
	if !validFenceFile(lock, lockPath, runtime.owner) || !fenceFileHasValue(lock, "sparerunner-containment-lock-v1\n") {
		closeLock()
		return nil, runner.ErrCleanupFailed
	}
	if !fenceOwnerCanOpenState(directory, containment.FenceToken) {
		closeLock()
		return nil, runner.ErrCleanupFailed
	}
	statePath := filepath.Join(directory, containment.FenceToken)
	state, stateCreated, err := openFenceFile(statePath)
	if err != nil {
		closeLock()
		return nil, runner.ErrCleanupFailed
	}
	if stateCreated {
		if err := initializeFenceFile(
			state,
			fenceStateContent(containment, fenceStateActive),
			directory,
		); err != nil {
			_ = state.Close()
			closeLock()
			return nil, runner.ErrCleanupFailed
		}
	}
	fence := &fileFence{
		lock:     lock,
		state:    state,
		active:   fenceStateContent(containment, fenceStateActive),
		launched: fenceStateContent(containment, fenceStateLaunched),
		revoked:  fenceStateContent(containment, fenceStateRevoked),
	}
	if !validFenceFile(state, statePath, runtime.owner) {
		_ = state.Close()
		closeLock()
		return nil, runner.ErrCleanupFailed
	}
	if _, err := fence.readState(); err != nil {
		_ = state.Close()
		closeLock()
		return nil, runner.ErrCleanupFailed
	}
	return fence, nil
}

// FinalizeFence replaces a revoked owner directory with a compact durable
// tombstone only after the cgroup is absent. The caller verifies workspace
// absence before entering this boundary. The tombstone prevents an ACK loss or
// crash before the Released journal commit from synthesizing a new active fence.
func (runtime *FileRuntime) FinalizeFence(ctx context.Context, containment runner.ContainmentRef) error {
	if err := ctx.Err(); err != nil || !validFinalizedContainment(containment) ||
		!canonicalToken(containment.FenceToken) {
		return runner.ErrCleanupFailed
	}
	if err := runtime.recoverFinalizedPublication(containment); err != nil {
		return runner.ErrCleanupFailed
	}
	finalized, err := runtime.fenceFinalized(containment)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	if finalized {
		if !runtime.cgroupAbsent(containment) {
			return runner.ErrCleanupFailed
		}
		return runtime.removeFinalizedOwner(ctx, containment)
	}
	if !runtime.validScope(containment) || !runtime.cgroupAbsent(containment) {
		return runner.ErrCleanupFailed
	}
	directory := filepath.Join(runtime.fenceRoot, containment.OwnerID)
	if !safeOwnedDirectory(directory, 0o700, runtime.owner) {
		return runner.ErrCleanupFailed
	}
	lockPath := filepath.Join(directory, "containment.lock")
	lock, err := openExistingFenceFile(lockPath)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	if err := lockFile(ctx, int(lock.Fd())); err != nil {
		_ = lock.Close()
		return err
	}
	statePath := filepath.Join(directory, containment.FenceToken)
	state, err := openExistingFenceFile(statePath)
	if err != nil {
		unlockFenceFile(lock)
		return runner.ErrCleanupFailed
	}
	if !validFenceFile(lock, lockPath, runtime.owner) ||
		!fenceFileHasValue(lock, "sparerunner-containment-lock-v1\n") ||
		!validFenceFile(state, statePath, runtime.owner) ||
		!fenceFileHasValue(state, fenceStateContent(containment, fenceStateRevoked)) ||
		!exactFenceOwnerEntries(directory, containment.FenceToken) {
		_ = state.Close()
		unlockFenceFile(lock)
		return runner.ErrCleanupFailed
	}
	if err := runtime.publishFinalizedFence(containment); err != nil {
		_ = state.Close()
		unlockFenceFile(lock)
		return runner.ErrCleanupFailed
	}
	if err := removePinnedFenceFile(state, statePath); err != nil {
		_ = state.Close()
		unlockFenceFile(lock)
		return runner.ErrCleanupFailed
	}
	if err := removePinnedFenceFile(lock, lockPath); err != nil {
		_ = state.Close()
		unlockFenceFile(lock)
		return runner.ErrCleanupFailed
	}
	stateErr := state.Close()
	unlockFenceFile(lock)
	if stateErr != nil {
		return runner.ErrCleanupFailed
	}
	if err := os.Remove(directory); err != nil || syncDirectory(runtime.fenceRoot) != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

// GarbageCollectFence removes only a fully validated finalized tombstone after
// the Released journal commit. Missing tombstones are idempotent success.
func (runtime *FileRuntime) GarbageCollectFence(ctx context.Context, containment runner.ContainmentRef) error {
	if err := ctx.Err(); err != nil || !validFinalizedContainment(containment) ||
		!canonicalToken(containment.FenceToken) || !runtime.cgroupAbsent(containment) {
		return runner.ErrCleanupFailed
	}
	if err := runtime.recoverFinalizedPublication(containment); err != nil {
		return runner.ErrCleanupFailed
	}
	finalized, err := runtime.fenceFinalized(containment)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	if !finalized {
		if _, ownerErr := os.Lstat(filepath.Join(runtime.fenceRoot, containment.OwnerID)); errors.Is(ownerErr, os.ErrNotExist) {
			return nil
		}
		return runner.ErrCleanupFailed
	}
	if _, err := os.Lstat(filepath.Join(runtime.fenceRoot, containment.OwnerID)); !errors.Is(err, os.ErrNotExist) {
		return runner.ErrCleanupFailed
	}
	root, err := os.OpenRoot(filepath.Join(runtime.fenceRoot, finalizedFenceDirectory))
	if err != nil {
		return runner.ErrCleanupFailed
	}
	defer root.Close()
	name := runtime.finalizedFenceName(containment)
	pathInfo, err := root.Lstat(name)
	if err != nil || !safeFinalizedFenceInfo(pathInfo, runtime.owner) {
		return runner.ErrCleanupFailed
	}
	opened, err := root.Open(name)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	openedInfo, statErr := opened.Stat()
	closeErr := opened.Close()
	if statErr != nil || closeErr != nil || !os.SameFile(pathInfo, openedInfo) {
		return runner.ErrCleanupFailed
	}
	if err := root.Remove(name); err != nil ||
		syncDirectory(filepath.Join(runtime.fenceRoot, finalizedFenceDirectory)) != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

// Shutdown is the root Supervisor stop boundary. Agent-only disconnects preserve
// launched runners; Supervisor shutdown revokes and empties every owned cgroup.
func (runtime *FileRuntime) Shutdown(ctx context.Context) error {
	if err := ctx.Err(); err != nil || runtime == nil ||
		!safeOwnedDirectory(runtime.fenceRoot, 0o700, runtime.owner) {
		return runner.ErrCleanupFailed
	}
	entries, err := os.ReadDir(runtime.fenceRoot)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	epoch, err := currentBootEpoch()
	if err != nil {
		return runner.ErrCleanupFailed
	}
	for _, entry := range entries {
		if entry.Name() == finalizedFenceDirectory {
			continue
		}
		if !entry.IsDir() || !validOwner(entry.Name()) {
			return runner.ErrCleanupFailed
		}
		token, err := runtime.singleFenceToken(entry.Name())
		if err != nil {
			return runner.ErrCleanupFailed
		}
		containment := runner.ContainmentRef{
			Backend:    containmentBackend,
			OwnerID:    entry.Name(),
			Scope:      filepath.Join("sparerunner", entry.Name()),
			HostEpoch:  epoch,
			FenceToken: token,
		}
		fence, err := runtime.LockFence(ctx, containment)
		if err != nil {
			return runner.ErrCleanupFailed
		}
		if err := fence.Revoke(ctx); err != nil {
			_ = fence.Close()
			return runner.ErrCleanupFailed
		}
		if err := runtime.KillAndWait(ctx, containment); err != nil {
			_ = fence.Close()
			return runner.ErrCleanupFailed
		}
		if err := fence.Close(); err != nil {
			return runner.ErrCleanupFailed
		}
	}
	return nil
}

func (runtime *FileRuntime) singleFenceToken(owner string) (string, error) {
	directory := filepath.Join(runtime.fenceRoot, owner)
	if !safeOwnedDirectory(directory, 0o700, runtime.owner) {
		return "", runner.ErrCleanupFailed
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 2 {
		return "", runner.ErrCleanupFailed
	}
	token := ""
	for _, entry := range entries {
		if entry.Name() == "containment.lock" {
			continue
		}
		if token != "" || !canonicalToken(entry.Name()) {
			return "", runner.ErrCleanupFailed
		}
		token = entry.Name()
	}
	if token == "" {
		return "", runner.ErrCleanupFailed
	}
	return token, nil
}

func validFinalizedContainment(containment runner.ContainmentRef) bool {
	return containment.Backend == containmentBackend &&
		validOwner(containment.OwnerID) &&
		containment.Scope == filepath.Join("sparerunner", containment.OwnerID) &&
		containment.HostEpoch != "" &&
		containment.InvocationID == "" &&
		canonicalToken(containment.FenceToken)
}

func (runtime *FileRuntime) ValidateFinalization(
	ctx context.Context,
	containment runner.ContainmentRef,
) error {
	if err := ctx.Err(); err != nil || !validFinalizedContainment(containment) ||
		!canonicalToken(containment.FenceToken) ||
		!runtime.cgroupAbsent(containment) {
		return runner.ErrCleanupFailed
	}
	if err := runtime.recoverFinalizedPublication(containment); err != nil {
		return runner.ErrCleanupFailed
	}
	if finalized, err := runtime.fenceFinalized(containment); err != nil {
		return runner.ErrCleanupFailed
	} else if finalized {
		return nil
	}
	if !runtime.validScope(containment) {
		return runner.ErrCleanupFailed
	}
	fence, err := runtime.LockFence(ctx, containment)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	revoked, revokedErr := fence.Revoked()
	closeErr := fence.Close()
	if revokedErr != nil || closeErr != nil || !revoked {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (runtime *FileRuntime) FenceFinalized(ctx context.Context, containment runner.ContainmentRef) (bool, error) {
	if err := ctx.Err(); err != nil || !validFinalizedContainment(containment) ||
		!canonicalToken(containment.FenceToken) {
		return false, runner.ErrCleanupFailed
	}
	if err := runtime.recoverFinalizedPublication(containment); err != nil {
		return false, runner.ErrCleanupFailed
	}
	return runtime.fenceFinalized(containment)
}

func (runtime *FileRuntime) removeFinalizedOwner(ctx context.Context, containment runner.ContainmentRef) error {
	directory := filepath.Join(runtime.fenceRoot, containment.OwnerID)
	if _, err := os.Lstat(directory); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err := ctx.Err(); err != nil || !safeOwnedDirectory(directory, 0o700, runtime.owner) {
		return runner.ErrCleanupFailed
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) > 2 {
		return runner.ErrCleanupFailed
	}
	seenLock, seenState := false, false
	for _, entry := range entries {
		switch entry.Name() {
		case "containment.lock":
			seenLock = true
		case containment.FenceToken:
			seenState = true
		default:
			return runner.ErrCleanupFailed
		}
	}
	lockPath := filepath.Join(directory, "containment.lock")
	var lock *os.File
	if seenLock {
		lock, err = openExistingFenceFile(lockPath)
		if err != nil {
			return runner.ErrCleanupFailed
		}
		if err := lockFile(ctx, int(lock.Fd())); err != nil {
			_ = lock.Close()
			return err
		}
		if !validFenceFile(lock, lockPath, runtime.owner) ||
			!fenceFileHasValue(lock, "sparerunner-containment-lock-v1\n") {
			unlockFenceFile(lock)
			return runner.ErrCleanupFailed
		}
	}
	statePath := filepath.Join(directory, containment.FenceToken)
	var state *os.File
	if seenState {
		state, err = openExistingFenceFile(statePath)
		if err != nil {
			unlockFenceFile(lock)
			return runner.ErrCleanupFailed
		}
		if !validFenceFile(state, statePath, runtime.owner) ||
			!fenceFileHasValue(state, fenceStateContent(containment, fenceStateRevoked)) {
			_ = state.Close()
			unlockFenceFile(lock)
			return runner.ErrCleanupFailed
		}
	}
	if state != nil && removePinnedFenceFile(state, statePath) != nil {
		_ = state.Close()
		unlockFenceFile(lock)
		return runner.ErrCleanupFailed
	}
	if lock != nil && removePinnedFenceFile(lock, lockPath) != nil {
		if state != nil {
			_ = state.Close()
		}
		unlockFenceFile(lock)
		return runner.ErrCleanupFailed
	}
	var stateErr error
	if state != nil {
		stateErr = state.Close()
	}
	unlockFenceFile(lock)
	remaining, readErr := os.ReadDir(directory)
	if stateErr != nil || readErr != nil || len(remaining) != 0 ||
		os.Remove(directory) != nil || syncDirectory(runtime.fenceRoot) != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

// recoverFinalizedPublication repairs only a known crash residue for this exact
// containment. A temp-only inode is discarded so publication can be retried; a
// temp hard-linked to the canonical tombstone is unlinked so the tombstone
// regains its required single-link identity. Unknown or malformed temp entries
// are never removed.
func (runtime *FileRuntime) recoverFinalizedPublication(containment runner.ContainmentRef) error {
	directory := filepath.Join(runtime.fenceRoot, finalizedFenceDirectory)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !safeOwnedDirectory(directory, 0o700, runtime.owner) {
		return runner.ErrCleanupFailed
	}
	matchingTemporary := ""
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".finalizing-") {
			continue
		}
		if !validFinalizingFenceName(entry.Name()) {
			return runner.ErrCleanupFailed
		}
		path := filepath.Join(directory, entry.Name())
		info, err := os.Lstat(path)
		if err != nil || !safeFinalizingFenceInfo(info, runtime.owner) {
			return runner.ErrCleanupFailed
		}
		file, err := openExistingFenceFile(path)
		if err != nil {
			return runner.ErrCleanupFailed
		}
		value, valid := readFinalizedFencePayload(file)
		closeErr := file.Close()
		if !valid || closeErr != nil {
			return runner.ErrCleanupFailed
		}
		if value != finalizedFenceContent(containment) {
			continue
		}
		if matchingTemporary != "" {
			return runner.ErrCleanupFailed
		}
		matchingTemporary = entry.Name()
	}
	if matchingTemporary == "" {
		return nil
	}

	temporaryPath := filepath.Join(directory, matchingTemporary)
	temporary, err := openExistingFenceFile(temporaryPath)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	defer temporary.Close()
	temporaryInfo, err := temporary.Stat()
	if err != nil || !safeFinalizingFenceInfo(temporaryInfo, runtime.owner) ||
		!fenceFileHasValue(temporary, finalizedFenceContent(containment)) {
		return runner.ErrCleanupFailed
	}

	finalizedPath := runtime.finalizedFencePath(containment)
	finalizedInfo, err := os.Lstat(finalizedPath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if !singleLink(temporaryInfo) ||
			removePinnedFenceFile(temporary, temporaryPath) != nil ||
			syncDirectory(directory) != nil {
			return runner.ErrCleanupFailed
		}
		return nil
	case err != nil:
		return runner.ErrCleanupFailed
	}
	finalized, err := openExistingFenceFile(finalizedPath)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	defer finalized.Close()
	openedFinalizedInfo, err := finalized.Stat()
	if err != nil || !sameTwoLinkFinalizedFiles(runtime.owner,
		temporaryInfo,
		finalizedInfo,
		openedFinalizedInfo,
	) || !fenceFileHasValue(finalized, finalizedFenceContent(containment)) {
		return runner.ErrCleanupFailed
	}
	if removePinnedFenceFile(temporary, temporaryPath) != nil ||
		syncDirectory(directory) != nil {
		return runner.ErrCleanupFailed
	}
	recoveredInfo, err := os.Lstat(finalizedPath)
	if err != nil || !safeFinalizedFenceInfo(recoveredInfo, runtime.owner) ||
		!os.SameFile(openedFinalizedInfo, recoveredInfo) {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (runtime *FileRuntime) publishFinalizedFence(containment runner.ContainmentRef) error {
	directory := filepath.Join(runtime.fenceRoot, finalizedFenceDirectory)
	if err := os.MkdirAll(directory, 0o700); err != nil ||
		!safeOwnedDirectory(directory, 0o700, runtime.owner) {
		return runner.ErrCleanupFailed
	}
	finalizedRoot, err := os.OpenRoot(directory)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	defer finalizedRoot.Close()
	name := runtime.finalizedFenceName(containment)
	if err := runtime.recoverFinalizedPublication(containment); err != nil {
		return runner.ErrCleanupFailed
	}
	if finalized, checkErr := runtime.fenceFinalized(containment); checkErr != nil {
		return runner.ErrCleanupFailed
	} else if finalized {
		return nil
	}
	temporaryName, err := fenceTemporaryName()
	if err != nil {
		return runner.ErrCleanupFailed
	}
	temporary, err := finalizedRoot.OpenFile(temporaryName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	defer func() {
		_ = temporary.Close()
		_ = finalizedRoot.Remove(temporaryName)
	}()
	content := finalizedFenceContent(containment)
	if written, err := temporary.WriteString(content); err != nil || written != len(content) ||
		temporary.Sync() != nil || temporary.Chmod(0o600) != nil {
		return runner.ErrCleanupFailed
	}
	info, err := temporary.Stat()
	if err != nil || !safeFinalizedFenceInfo(info, runtime.owner) {
		return runner.ErrCleanupFailed
	}
	if err := finalizedRoot.Link(temporaryName, name); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := finalizedRoot.Remove(temporaryName); err != nil || syncDirectory(directory) != nil {
		return runner.ErrCleanupFailed
	}
	pathInfo, err := finalizedRoot.Lstat(name)
	if err != nil || !safeFinalizedFenceInfo(pathInfo, runtime.owner) || !os.SameFile(info, pathInfo) {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (runtime *FileRuntime) fenceFinalized(containment runner.ContainmentRef) (bool, error) {
	directory := filepath.Join(runtime.fenceRoot, finalizedFenceDirectory)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !safeOwnedDirectory(directory, 0o700, runtime.owner) {
		return false, runner.ErrCleanupFailed
	}
	expectedName := runtime.finalizedFenceName(containment)
	found := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), containment.OwnerID+".") {
			continue
		}
		if entry.Name() != expectedName || found {
			return false, runner.ErrCleanupFailed
		}
		found = true
		path := filepath.Join(directory, entry.Name())
		info, statErr := os.Lstat(path)
		if statErr != nil || !safeFinalizedFenceInfo(info, runtime.owner) {
			return false, runner.ErrCleanupFailed
		}
		file, openErr := openExistingFenceFile(path)
		if openErr != nil {
			return false, runner.ErrCleanupFailed
		}
		valid := validFenceFile(file, path, runtime.owner) &&
			fenceFileHasValue(file, finalizedFenceContent(containment))
		closeErr := file.Close()
		if !valid || closeErr != nil {
			return false, runner.ErrCleanupFailed
		}
	}
	return found, nil
}

func (runtime *FileRuntime) cgroupAbsent(containment runner.ContainmentRef) bool {
	_, err := os.Lstat(filepath.Join(runtime.cgroupRoot, containment.Scope))
	return errors.Is(err, os.ErrNotExist)
}

func (runtime *FileRuntime) finalizedFenceName(containment runner.ContainmentRef) string {
	return containment.OwnerID + "." + containment.FenceToken
}

func (runtime *FileRuntime) finalizedFencePath(containment runner.ContainmentRef) string {
	return filepath.Join(runtime.fenceRoot, finalizedFenceDirectory, runtime.finalizedFenceName(containment))
}

func finalizedFenceContent(containment runner.ContainmentRef) string {
	return finalizedFenceVersion + "\n" +
		"owner=" + containment.OwnerID + "\n" +
		"token=" + containment.FenceToken + "\n" +
		"host_epoch=" + containment.HostEpoch + "\n" +
		"invocation=" + containment.InvocationID + "\n"
}

func fenceStateContent(containment runner.ContainmentRef, state string) string {
	return fenceStateVersion + "\n" +
		"owner=" + containment.OwnerID + "\n" +
		"token=" + containment.FenceToken + "\n" +
		"host_epoch=" + containment.HostEpoch + "\n" +
		"invocation=" + containment.InvocationID + "\n" +
		"state=" + state + "\n"
}

func safeFinalizedFenceInfo(info os.FileInfo, owner fileOwner) bool {
	return info != nil && info.Mode().IsRegular() && owner.owns(info) &&
		info.Mode().Perm() == 0o600 && singleLink(info)
}

func safeFinalizingFenceInfo(info os.FileInfo, owner fileOwner) bool {
	if info == nil || !info.Mode().IsRegular() || !owner.owns(info) ||
		info.Mode().Perm() != 0o600 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Nlink == 1 || stat.Nlink == 2)
}

func sameTwoLinkFinalizedFiles(owner fileOwner, infos ...os.FileInfo) bool {
	if len(infos) == 0 {
		return false
	}
	for _, info := range infos {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 2 || !info.Mode().IsRegular() ||
			!owner.owns(info) || info.Mode().Perm() != 0o600 ||
			!os.SameFile(infos[0], info) {
			return false
		}
	}
	return true
}

func validFinalizingFenceName(name string) bool {
	const prefix = ".finalizing-"
	return strings.HasPrefix(name, prefix) &&
		len(name) == len(prefix)+32 &&
		canonicalLowerHex(name[len(prefix):])
}

func readFinalizedFencePayload(file *os.File) (string, bool) {
	if file == nil {
		return "", false
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", false
	}
	data, err := io.ReadAll(io.LimitReader(file, 1025))
	if err != nil || len(data) > 1024 {
		return "", false
	}
	value := string(data)
	lines := strings.Split(value, "\n")
	if len(lines) != 6 || lines[0] != finalizedFenceVersion ||
		!strings.HasPrefix(lines[1], "owner=") ||
		!strings.HasPrefix(lines[2], "token=") ||
		!strings.HasPrefix(lines[3], "host_epoch=") ||
		lines[3] == "host_epoch=" ||
		lines[4] != "invocation=" || lines[5] != "" {
		return "", false
	}
	owner := strings.TrimPrefix(lines[1], "owner=")
	token := strings.TrimPrefix(lines[2], "token=")
	if !validOwner(owner) || !canonicalToken(token) {
		return "", false
	}
	return value, true
}

func exactFenceOwnerEntries(directory, token string) bool {
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 2 {
		return false
	}
	seenLock, seenToken := false, false
	for _, entry := range entries {
		switch entry.Name() {
		case "containment.lock":
			seenLock = true
		case token:
			seenToken = true
		default:
			return false
		}
	}
	return seenLock && seenToken
}

func fenceOwnerCanOpenState(directory, token string) bool {
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) < 1 || len(entries) > 2 {
		return false
	}
	seenLock := false
	for _, entry := range entries {
		switch entry.Name() {
		case "containment.lock":
			seenLock = true
		case token:
		default:
			return false
		}
	}
	return seenLock
}

func openExistingFenceFile(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, runner.ErrCleanupFailed
	}
	return file, nil
}

func removePinnedFenceFile(file *os.File, path string) error {
	info, err := file.Stat()
	pathInfo, pathErr := os.Lstat(path)
	if err != nil || pathErr != nil || !os.SameFile(info, pathInfo) {
		return runner.ErrCleanupFailed
	}
	return os.Remove(path)
}

func unlockFenceFile(lock *os.File) {
	if lock == nil {
		return
	}
	_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	_ = lock.Close()
}

func fenceTemporaryName() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return ".finalizing-" + hex.EncodeToString(entropy[:]), nil
}

func openFenceFile(path string) (*os.File, bool, error) {
	fd, err := syscall.Open(
		path,
		syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC,
		0o600,
	)
	created := err == nil
	if errors.Is(err, syscall.EEXIST) {
		fd, err = syscall.Open(path, syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
	}
	if err != nil {
		return nil, false, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, false, runner.ErrCleanupFailed
	}
	return file, created, nil
}

func initializeFenceFile(file *os.File, value, directory string) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	written, err := file.WriteString(value)
	if err != nil || written != len(value) || file.Sync() != nil {
		return runner.ErrCleanupFailed
	}
	return syncDirectory(directory)
}

func validFenceFile(file *os.File, path string, owner fileOwner) bool {
	info, err := file.Stat()
	pathInfo, pathErr := os.Lstat(path)
	return err == nil && pathErr == nil &&
		info.Mode().IsRegular() && owner.owns(info) &&
		os.SameFile(info, pathInfo) && info.Mode().Perm() == 0o600
}

func fenceFileHasValue(file *os.File, expected string) bool {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(len(expected)+1)))
	return err == nil && string(data) == expected
}

func (runtime *FileRuntime) Launch(ctx context.Context, spec LaunchSpec, material io.Reader) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	cgroup, err := runtime.openCgroup(spec.Containment)
	if err != nil {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	defer cgroup.Close()
	return runtime.launcher.Launch(ctx, spec, material, cgroup)
}

func (runtime *FileRuntime) KillAndWait(ctx context.Context, containment runner.ContainmentRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cgroup, err := runtime.openCgroup(containment)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && runtime.durableFenceRevoked(containment) {
			return nil
		}
		return runner.ErrCleanupFailed
	}
	if err := writeCgroupControl(cgroup, "cgroup.kill", []byte("1\n")); err != nil {
		_ = cgroup.Close()
		return runner.ErrCleanupFailed
	}
	for {
		populated, err := cgroupPopulatedFD(cgroup)
		if err != nil {
			_ = cgroup.Close()
			return runner.ErrCleanupFailed
		}
		if !populated {
			break
		}
		select {
		case <-ctx.Done():
			_ = cgroup.Close()
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err := cgroup.Close(); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := os.Remove(filepath.Join(runtime.cgroupRoot, containment.Scope)); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (runtime *FileRuntime) durableFenceRevoked(containment runner.ContainmentRef) bool {
	if !runtime.validScope(containment) || !canonicalToken(containment.FenceToken) {
		return false
	}
	if finalized, err := runtime.fenceFinalized(containment); err == nil && finalized {
		return true
	}
	path := filepath.Join(runtime.fenceRoot, containment.OwnerID, containment.FenceToken)
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return false
	}
	state := os.NewFile(uintptr(fd), path)
	if state == nil {
		_ = syscall.Close(fd)
		return false
	}
	defer state.Close()
	return validFenceFile(state, path, runtime.owner) &&
		fenceFileHasValue(state, fenceStateContent(containment, fenceStateRevoked))
}

func (runtime *FileRuntime) Alive(ctx context.Context, containment runner.ContainmentRef, pid int) (bool, error) {
	if err := ctx.Err(); err != nil || pid <= 0 {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	fence, err := runtime.LockFence(ctx, containment)
	if err != nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	launched, launchedErr := fence.Launched()
	revoked, revokedErr := fence.Revoked()
	closeErr := fence.Close()
	if launchedErr != nil || revokedErr != nil || closeErr != nil ||
		!launched || revoked {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	cgroup, err := runtime.openCgroup(containment)
	if err != nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	defer cgroup.Close()
	populated, err := cgroupPopulatedFD(cgroup)
	if err != nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	if !populated {
		// The exact launched authority is valid but naturally complete. Recovery
		// may adopt it only long enough for Wait/Destroy to converge cleanup.
		return false, nil
	}
	processes, err := readCgroupControl(cgroup, "cgroup.procs")
	if err != nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	contains, err := cgroupContainsPID(processes, pid)
	if err != nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	return contains, nil
}

func cgroupContainsPID(data []byte, expected int) (bool, error) {
	if expected <= 0 {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	found := false
	for _, field := range strings.Fields(string(data)) {
		pid, err := strconv.Atoi(field)
		if err != nil || pid <= 0 {
			return false, runner.ErrStrongOwnershipUnavailable
		}
		if pid == expected {
			if found {
				return false, runner.ErrStrongOwnershipUnavailable
			}
			found = true
		}
	}
	return found, nil
}

func (runtime *FileRuntime) SlotBusy(ctx context.Context, candidate runner.ContainmentRef) (bool, error) {
	if err := ctx.Err(); err != nil || !runtime.validScope(candidate) {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	entries, err := os.ReadDir(filepath.Join(runtime.cgroupRoot, "sparerunner"))
	if err != nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			// cgroupfs exposes controller files in every directory; user-created
			// regular files are not supported by the filesystem.
			continue
		}
		if !validOwner(entry.Name()) {
			return false, runner.ErrStrongOwnershipUnavailable
		}
		// An empty leaf from another execution is still cleanup residue. Treat it
		// as busy so a failed rmdir cannot be turned into a fresh launch after a
		// helper restart; reconciliation must quarantine and clear it explicitly.
		if entry.Name() != candidate.OwnerID {
			return true, nil
		}
		ref := runner.ContainmentRef{
			Backend:   containmentBackend,
			OwnerID:   entry.Name(),
			Scope:     filepath.Join("sparerunner", entry.Name()),
			HostEpoch: candidate.HostEpoch,
		}
		cgroup, openErr := runtime.openCgroup(ref)
		if openErr != nil {
			return false, runner.ErrStrongOwnershipUnavailable
		}
		populated, readErr := cgroupPopulatedFD(cgroup)
		closeErr := cgroup.Close()
		if readErr != nil || closeErr != nil {
			return false, runner.ErrStrongOwnershipUnavailable
		}
		if populated {
			return true, nil
		}
	}
	return false, nil
}

func (runtime *FileRuntime) WaitEmpty(ctx context.Context, containment runner.ContainmentRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cgroup, err := runtime.openCgroup(containment)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	defer cgroup.Close()
	for {
		populated, readErr := cgroupPopulatedFD(cgroup)
		if readErr != nil {
			return runner.ErrStrongOwnershipUnavailable
		}
		if !populated {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (runtime *FileRuntime) openCgroup(containment runner.ContainmentRef) (*os.File, error) {
	if !runtime.validScope(containment) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	fd, err := syscall.Open(filepath.Join(runtime.cgroupRoot, containment.Scope), syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	cgroup := os.NewFile(uintptr(fd), "cgroup:"+containment.Scope)
	if cgroup == nil {
		_ = syscall.Close(fd)
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	info, err := cgroup.Stat()
	if err != nil || !info.IsDir() {
		_ = cgroup.Close()
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if _, err := readCgroupControl(cgroup, "cgroup.events"); err != nil {
		_ = cgroup.Close()
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if !cgroupControlWritable(cgroup, "cgroup.kill") {
		_ = cgroup.Close()
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return cgroup, nil
}

func (runtime *FileRuntime) validScope(containment runner.ContainmentRef) bool {
	if containment.Backend != containmentBackend ||
		!validOwner(containment.OwnerID) ||
		containment.Scope != filepath.Join("sparerunner", containment.OwnerID) ||
		containment.InvocationID != "" {
		return false
	}
	epoch, err := currentBootEpoch()
	return err == nil && containment.HostEpoch == epoch
}

func currentBootEpoch() (string, error) {
	epoch, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(epoch)) == "" {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	return strings.TrimSpace(string(epoch)), nil
}

func safeRootedDirectory(value string, finalMode os.FileMode) bool {
	return safeOwnedDirectory(value, finalMode, rootFileOwner)
}

// safeOwnedDirectory proves that no identity other than root or the owning
// credential can rename, replace, or traverse into the durable root. With
// rootFileOwner it is exactly the original root-only walk: every component,
// including the leaf, must be root-owned.
func safeOwnedDirectory(value string, finalMode os.FileMode, owner fileOwner) bool {
	cleaned := filepath.Clean(value)
	for current := cleaned; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
			return false
		}
		if current == cleaned {
			if !owner.owns(info) {
				return false
			}
			if finalMode != 0 && info.Mode().Perm() != finalMode {
				return false
			}
		} else if !rootOwned(info) && !owner.owns(info) {
			return false
		}
		if current == "/" {
			return true
		}
	}
}

// safeDelegatedCgroupRoot validates the shared-identity cgroup root. cgroupfs
// ancestors are root-owned by construction and systemd's user delegation
// chowns only the leaf's uid, leaving its group implementation defined, so the
// leaf is proven by uid alone. Write capability is proven separately by
// ValidateAdmission through cgroup.kill and by EnsureCgroup's mkdir.
func safeDelegatedCgroupRoot(value string, uid int) bool {
	cleaned := filepath.Clean(value)
	if cleaned == "/" || !filepath.IsAbs(cleaned) {
		return false
	}
	for current := cleaned; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			info.Mode().Perm()&0o022 != 0 {
			return false
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return false
		}
		if current == cleaned {
			// A delegated subtree the Agent does not own cannot be a containment
			// boundary: it could not create children or write cgroup.kill.
			if int(stat.Uid) != uid || uid == 0 {
				return false
			}
		} else if stat.Uid != 0 && int(stat.Uid) != uid {
			return false
		}
		if current == "/" {
			return true
		}
	}
}

func isCgroupV2(path string) bool {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return false
	}
	const cgroup2SuperMagic = 0x63677270
	return stat.Type == cgroup2SuperMagic
}

type fileFence struct {
	lock     *os.File
	state    *os.File
	active   string
	launched string
	revoked  string
}

type finalizedFence struct{}

func (finalizedFence) Revoked() (bool, error)             { return true, nil }
func (finalizedFence) Launched() (bool, error)            { return false, nil }
func (finalizedFence) MarkLaunched(context.Context) error { return runner.ErrStartFenced }
func (finalizedFence) Revoke(context.Context) error       { return nil }
func (finalizedFence) Close() error                       { return nil }

func (fence *fileFence) Revoked() (bool, error) {
	if fence == nil || fence.lock == nil || fence.state == nil {
		return true, runner.ErrCleanupFailed
	}
	if _, err := fence.state.Seek(0, io.SeekStart); err != nil {
		return true, runner.ErrCleanupFailed
	}
	state, err := fence.readState()
	if err != nil {
		return true, err
	}
	return state == fence.revoked, nil
}

func (fence *fileFence) Launched() (bool, error) {
	state, err := fence.readState()
	if err != nil {
		return false, err
	}
	return state == fence.launched, nil
}

func (fence *fileFence) MarkLaunched(ctx context.Context) error {
	if err := ctx.Err(); err != nil || fence == nil || fence.lock == nil || fence.state == nil {
		return runner.ErrCleanupFailed
	}
	state, err := fence.readState()
	if err != nil || state != fence.active {
		return runner.ErrCleanupFailed
	}
	return fence.writeState(fence.launched)
}

func (fence *fileFence) Revoke(ctx context.Context) error {
	if err := ctx.Err(); err != nil || fence == nil || fence.lock == nil || fence.state == nil {
		return runner.ErrCleanupFailed
	}
	state, err := fence.readState()
	if err != nil {
		return runner.ErrCleanupFailed
	}
	if state == fence.revoked {
		return nil
	}
	if state != fence.active && state != fence.launched {
		return runner.ErrCleanupFailed
	}
	return fence.writeState(fence.revoked)
}

func (fence *fileFence) readState() (string, error) {
	if fence == nil || fence.lock == nil || fence.state == nil {
		return "", runner.ErrCleanupFailed
	}
	if _, err := fence.state.Seek(0, io.SeekStart); err != nil {
		return "", runner.ErrCleanupFailed
	}
	data, err := io.ReadAll(io.LimitReader(fence.state, int64(len(fence.launched)+1)))
	if err != nil {
		return "", runner.ErrCleanupFailed
	}
	state := string(data)
	switch state {
	case fence.active, fence.launched, fence.revoked:
		return state, nil
	default:
		return "", runner.ErrCleanupFailed
	}
}

func (fence *fileFence) writeState(state string) error {
	if _, err := fence.state.Seek(0, io.SeekStart); err != nil ||
		fence.state.Truncate(0) != nil {
		return runner.ErrCleanupFailed
	}
	if written, err := fence.state.WriteString(state); err != nil || written != len(state) {
		return runner.ErrCleanupFailed
	}
	if err := fence.state.Sync(); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (fence *fileFence) Close() error {
	if fence == nil {
		return nil
	}
	var stateErr error
	if fence.state != nil {
		stateErr = fence.state.Close()
		fence.state = nil
	}
	var lockErr error
	if fence.lock != nil {
		_ = syscall.Flock(int(fence.lock.Fd()), syscall.LOCK_UN)
		lockErr = fence.lock.Close()
		fence.lock = nil
	}
	if stateErr != nil {
		return stateErr
	}
	return lockErr
}

func lockFile(ctx context.Context, fd int) error {
	for {
		if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			return nil
		} else if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return runner.ErrCleanupFailed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func cgroupPopulated(eventsPath string) (bool, error) {
	data, err := os.ReadFile(eventsPath)
	if err != nil {
		return false, err
	}
	return cgroupPopulatedData(data)
}

func cgroupPopulatedFD(cgroup *os.File) (bool, error) {
	data, err := readCgroupControl(cgroup, "cgroup.events")
	if err != nil {
		return false, err
	}
	return cgroupPopulatedData(data)
}

func cgroupPopulatedData(data []byte) (bool, error) {
	for _, line := range strings.Split(string(data), "\n") {
		if line == "populated 0" {
			return false, nil
		}
		if line == "populated 1" {
			return true, nil
		}
	}
	return false, fmt.Errorf("cgroup.events has no populated field")
}

func readCgroupControl(cgroup *os.File, name string) ([]byte, error) {
	fd, err := syscall.Openat(int(cgroup.Fd()), name, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, errors.New("open cgroup control")
	}
	defer file.Close()
	return io.ReadAll(file)
}

func writeCgroupControl(cgroup *os.File, name string, value []byte) error {
	fd, err := syscall.Openat(int(cgroup.Fd()), name, syscall.O_WRONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = syscall.Close(fd)
		return errors.New("open cgroup control")
	}
	defer file.Close()
	_, err = file.Write(value)
	return err
}

func cgroupControlWritable(cgroup *os.File, name string) bool {
	fd, err := syscall.Openat(int(cgroup.Fd()), name, syscall.O_WRONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return false
	}
	return syscall.Close(fd) == nil
}

func validOwner(owner string) bool {
	if !strings.HasPrefix(owner, "sparerunner-") || len(owner) != len("sparerunner-")+64 {
		return false
	}
	for _, character := range owner[len("sparerunner-"):] {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func fixedRunnerArguments(arguments []string) bool {
	if len(arguments) == 1 {
		return arguments[0] == "--ephemeral"
	}
	return len(arguments) == 2 &&
		arguments[0] == "--ephemeral" &&
		arguments[1] == "--disableupdate"
}

func fixedRunnerEnvironment(uid, gid int) ([]string, error) {
	account, err := user.LookupId(strconv.Itoa(uid))
	if err != nil || account.Uid != strconv.Itoa(uid) || account.Gid != strconv.Itoa(gid) ||
		account.Username == "" || strings.ContainsAny(account.Username, "=\x00\r\n") ||
		!filepath.IsAbs(account.HomeDir) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	const executionHome = "/proc/self/fd/4/.sparerunner-home"
	return []string{
		"HOME=" + executionHome,
		"XDG_CACHE_HOME=" + executionHome + "/.cache",
		"XDG_CONFIG_HOME=" + executionHome + "/.config",
		"TMPDIR=" + executionHome + "/.tmp",
		"LANG=C.UTF-8",
		"LOGNAME=" + account.Username,
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"USER=" + account.Username,
	}, nil
}

var _ Runtime = (*FileRuntime)(nil)
var _ SlotAdmission = (*FileRuntime)(nil)
