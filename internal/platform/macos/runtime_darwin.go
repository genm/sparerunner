//go:build darwin

package macos

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/genm/tewake/internal/runner"
	"golang.org/x/sys/unix"
)

const (
	fenceVersion       = 1
	fenceStateActive   = "active"
	fenceStateLaunched = "launched"
	fenceStateRevoked  = "revoked"
	fenceStateFinal    = "finalized"

	maxJITMaterialBytes = 256 << 10
	helperModeArgument  = "--tewake-macos-launcher-helper"
)

type FileRuntime struct {
	fenceRoot string
	launcher  ExecLauncher
	identity  RunnerIdentity
	workspace *OSWorkspace
	hostEpoch string
	processes processSource
	ownerUID  int
	ownerGID  int
}

type processObservation struct {
	PID  int
	PGID int
	RUID int
	EUID int
}

type processSource interface {
	Processes(context.Context) ([]processObservation, error)
	Kill(int, syscall.Signal) error
}

type darwinProcesses struct{}

func (darwinProcesses) Processes(ctx context.Context) ([]processObservation, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	result := make([]processObservation, 0, len(processes))
	for _, process := range processes {
		if process.Proc.P_pid <= 0 {
			continue
		}
		result = append(result, processObservation{
			PID:  int(process.Proc.P_pid),
			PGID: int(process.Eproc.Pgid),
			RUID: int(process.Eproc.Pcred.P_ruid),
			EUID: int(process.Eproc.Ucred.Uid),
		})
	}
	return result, nil
}

func (darwinProcesses) Kill(pid int, signal syscall.Signal) error {
	return unix.Kill(pid, signal)
}

type ExecLauncher struct {
	HelperPath string
}

func NewExecLauncher(path string) (ExecLauncher, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ExecLauncher{}, runner.ErrStrongOwnershipUnavailable
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || !filepath.IsAbs(resolved) {
		return ExecLauncher{}, runner.ErrStrongOwnershipUnavailable
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.Mode().IsRegular() || !ownedBy(info, 0, 0) ||
		info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		return ExecLauncher{}, runner.ErrStrongOwnershipUnavailable
	}
	for current := filepath.Dir(resolved); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			!ownedBy(info, 0, 0) || info.Mode().Perm()&0o022 != 0 {
			return ExecLauncher{}, runner.ErrStrongOwnershipUnavailable
		}
		if current == "/" {
			break
		}
	}
	return ExecLauncher{HelperPath: resolved}, nil
}

func NewFileRuntime(
	fenceRoot string,
	launcher ExecLauncher,
	identity RunnerIdentity,
	workspace *OSWorkspace,
) (*FileRuntime, error) {
	if os.Geteuid() != 0 || workspace == nil || identity.UID <= 0 ||
		identity.GID <= 0 || identity != workspace.RunnerIdentity() ||
		launcher.HelperPath == "" || !filepath.IsAbs(fenceRoot) ||
		filepath.Clean(fenceRoot) != fenceRoot || fenceRoot == "/" {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if err := os.MkdirAll(fenceRoot, 0o700); err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if err := os.Chmod(fenceRoot, 0o700); err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	epoch, err := currentBootEpoch()
	if err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	nativeRuntime := &FileRuntime{
		fenceRoot: fenceRoot,
		launcher:  launcher,
		identity:  identity,
		workspace: workspace,
		hostEpoch: epoch,
		processes: darwinProcesses{},
		ownerUID:  0,
		ownerGID:  0,
	}
	if err := nativeRuntime.ValidateAdmission(context.Background()); err != nil {
		return nil, err
	}
	return nativeRuntime, nil
}

func (nativeRuntime *FileRuntime) ValidateAdmission(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil || nativeRuntime == nil ||
		nativeRuntime.processes == nil || nativeRuntime.workspace == nil ||
		nativeRuntime.identity != nativeRuntime.workspace.RunnerIdentity() ||
		nativeRuntime.hostEpoch == "" ||
		!safePrivateDirectoryChain(
			nativeRuntime.fenceRoot,
			nativeRuntime.ownerUID,
			nativeRuntime.ownerGID,
		) {
		return runner.ErrStrongOwnershipUnavailable
	}
	if _, err := nativeRuntime.processes.Processes(ctx); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if _, err := NewExecLauncher(nativeRuntime.launcher.HelperPath); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (nativeRuntime *FileRuntime) EnsureProcessGroup(
	ctx context.Context,
	owner string,
) (ProcessGroup, error) {
	if ctx == nil || ctx.Err() != nil || !validOwner(owner) ||
		nativeRuntime.ValidateAdmission(ctx) != nil {
		return ProcessGroup{}, runner.ErrStrongOwnershipUnavailable
	}
	processes, err := nativeRuntime.slotProcesses(ctx)
	if err != nil || len(processes) != 0 {
		return ProcessGroup{}, runner.ErrStrongOwnershipUnavailable
	}
	return ProcessGroup{
		Scope:     filepath.ToSlash(filepath.Join("tewake", owner)),
		HostEpoch: nativeRuntime.hostEpoch,
	}, nil
}

func (nativeRuntime *FileRuntime) LockFence(
	ctx context.Context,
	containment runner.ContainmentRef,
) (Fence, error) {
	return nativeRuntime.lockFence(ctx, containment, true)
}

type fenceMetadata struct {
	Version   int    `json:"version"`
	Token     string `json:"token"`
	HostEpoch string `json:"hostEpoch"`
}

type fenceState struct {
	Version int    `json:"version"`
	State   string `json:"state"`
	PID     int    `json:"pid,omitempty"`
}

type fileFence struct {
	runtime     *FileRuntime
	containment runner.ContainmentRef
	directory   string
	lock        *os.File
	closed      bool
}

func (nativeRuntime *FileRuntime) lockFence(
	ctx context.Context,
	containment runner.ContainmentRef,
	create bool,
) (*fileFence, error) {
	if ctx == nil || ctx.Err() != nil || !nativeRuntime.validContainment(containment) {
		return nil, fmt.Errorf("%w: invalid fence request", runner.ErrCleanupFailed)
	}
	directory := filepath.Join(nativeRuntime.fenceRoot, containment.OwnerID)
	if create {
		if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("%w: create fence directory", runner.ErrCleanupFailed)
		}
	}
	if !safePrivateDirectory(
		directory,
		nativeRuntime.ownerUID,
		nativeRuntime.ownerGID,
	) {
		return nil, fmt.Errorf("%w: unsafe fence directory", runner.ErrCleanupFailed)
	}
	lockPath := filepath.Join(directory, "lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("%w: open fence lock", runner.ErrCleanupFailed)
	}
	closeFailure := func() (*fileFence, error) {
		_ = lock.Close()
		return nil, fmt.Errorf("%w: unsafe fence lock", runner.ErrCleanupFailed)
	}
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() ||
		!ownedBy(info, nativeRuntime.ownerUID, nativeRuntime.ownerGID) ||
		info.Mode().Perm() != 0o600 || !singleLink(info) {
		return closeFailure()
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("%w: lock fence", runner.ErrCleanupFailed)
	}
	fence := &fileFence{
		runtime: nativeRuntime, containment: containment,
		directory: directory, lock: lock,
	}
	expected := fenceMetadata{
		Version: fenceVersion, Token: containment.FenceToken,
		HostEpoch: containment.HostEpoch,
	}
	metadataPath := filepath.Join(directory, "metadata.json")
	metadataBytes, err := readPrivateFile(
		metadataPath,
		nativeRuntime.ownerUID,
		nativeRuntime.ownerGID,
		4<<10,
	)
	if errors.Is(err, os.ErrNotExist) && create && containment.HostEpoch == nativeRuntime.hostEpoch {
		if err := writeNoClobberJSON(metadataPath, expected); err != nil {
			_ = fence.Close()
			return nil, fmt.Errorf("%w: create fence metadata: %v", runner.ErrCleanupFailed, err)
		}
	} else if err != nil {
		_ = fence.Close()
		return nil, fmt.Errorf("%w: read fence metadata", runner.ErrCleanupFailed)
	} else {
		var actual fenceMetadata
		if decodeStrictJSON(metadataBytes, &actual) != nil || actual != expected {
			_ = fence.Close()
			return nil, fmt.Errorf("%w: fence metadata mismatch", runner.ErrCleanupFailed)
		}
	}
	statePath := filepath.Join(directory, "state.json")
	if _, err := os.Lstat(statePath); errors.Is(err, os.ErrNotExist) &&
		create && containment.HostEpoch == nativeRuntime.hostEpoch {
		if err := writeNoClobberJSON(statePath, fenceState{
			Version: fenceVersion,
			State:   fenceStateActive,
		}); err != nil {
			_ = fence.Close()
			return nil, fmt.Errorf("%w: create fence state: %v", runner.ErrCleanupFailed, err)
		}
	} else if err != nil {
		_ = fence.Close()
		return nil, fmt.Errorf("%w: inspect fence state", runner.ErrCleanupFailed)
	}
	if _, err := fence.readState(); err != nil {
		_ = fence.Close()
		return nil, fmt.Errorf("%w: read fence state", runner.ErrCleanupFailed)
	}
	return fence, nil
}

func (fence *fileFence) readState() (fenceState, error) {
	if fence == nil || fence.closed {
		return fenceState{}, runner.ErrCleanupFailed
	}
	path := filepath.Join(fence.directory, "state.json")
	contents, err := readPrivateFile(
		path,
		fence.runtime.ownerUID,
		fence.runtime.ownerGID,
		4<<10,
	)
	if err != nil {
		return fenceState{}, fmt.Errorf("%w: read fence state: %v", runner.ErrCleanupFailed, err)
	}
	var state fenceState
	if err := decodeStrictJSON(contents, &state); err != nil {
		return fenceState{}, fmt.Errorf("%w: decode fence state: %v", runner.ErrCleanupFailed, err)
	}
	if state.Version != fenceVersion || !validFenceState(state) {
		return fenceState{}, fmt.Errorf("%w: invalid fence state %#v", runner.ErrCleanupFailed, state)
	}
	return state, nil
}

func validFenceState(state fenceState) bool {
	switch state.State {
	case fenceStateActive:
		return state.PID == 0
	case fenceStateLaunched, fenceStateRevoked, fenceStateFinal:
		return state.PID >= 0
	default:
		return false
	}
}

func (fence *fileFence) writeState(ctx context.Context, state fenceState) error {
	if ctx == nil || ctx.Err() != nil || fence == nil || fence.closed ||
		state.Version != fenceVersion || !validFenceState(state) {
		return runner.ErrCleanupFailed
	}
	if err := replaceJSON(filepath.Join(fence.directory, "state.json"), state); err != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (fence *fileFence) Revoked() (bool, error) {
	state, err := fence.readState()
	return state.State == fenceStateRevoked || state.State == fenceStateFinal, err
}

func (fence *fileFence) Launched() (bool, error) {
	state, err := fence.readState()
	return state.State == fenceStateLaunched, err
}

func (fence *fileFence) MarkLaunched(ctx context.Context, pid int) error {
	state, err := fence.readState()
	if err != nil || state.State != fenceStateActive || pid <= 0 {
		return runner.ErrStartFenced
	}
	if err := fence.writeState(ctx, fenceState{
		Version: fenceVersion,
		State:   fenceStateLaunched,
		PID:     pid,
	}); err != nil {
		return runner.ErrStartFenced
	}
	return nil
}

func (fence *fileFence) Revoke(ctx context.Context) error {
	state, err := fence.readState()
	if err != nil {
		return runner.ErrCleanupFailed
	}
	switch state.State {
	case fenceStateFinal, fenceStateRevoked:
		return nil
	case fenceStateActive, fenceStateLaunched:
		return fence.writeState(ctx, fenceState{
			Version: fenceVersion,
			State:   fenceStateRevoked,
			PID:     state.PID,
		})
	default:
		return runner.ErrCleanupFailed
	}
}

func (fence *fileFence) Close() error {
	if fence == nil || fence.closed {
		return nil
	}
	fence.closed = true
	unlockErr := unix.Flock(int(fence.lock.Fd()), unix.LOCK_UN)
	closeErr := fence.lock.Close()
	return errors.Join(unlockErr, closeErr)
}

func (nativeRuntime *FileRuntime) Launch(
	ctx context.Context,
	spec LaunchSpec,
	material io.Reader,
	admit func(context.Context, int) error,
) (int, error) {
	if ctx == nil || ctx.Err() != nil || material == nil || admit == nil ||
		!nativeRuntime.validContainment(spec.Containment) ||
		spec.Containment.HostEpoch != nativeRuntime.hostEpoch ||
		spec.Identity != nativeRuntime.identity ||
		!validLaunchSpec(spec) {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	if processes, err := nativeRuntime.slotProcesses(ctx); err != nil || len(processes) != 0 {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	pinned, err := nativeRuntime.workspace.PinLaunch(ctx, spec.Directory, spec.WorkspaceRef)
	if err != nil {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	defer pinned.Close()
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		return 0, runner.ErrStartFailed
	}
	defer statusReader.Close()
	jitReader, jitWriter, err := os.Pipe()
	if err != nil {
		_ = statusWriter.Close()
		return 0, runner.ErrStartFailed
	}
	command := exec.Command(nativeRuntime.launcher.HelperPath, helperArguments(spec.Arguments)...)
	command.Stdin = jitReader
	command.Env = fixedRunnerEnvironment(spec.Directory)
	command.ExtraFiles = []*os.File{
		statusWriter,
		pinned.Directory(),
		pinned.Executable(),
	}
	command.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    uint32(spec.Identity.UID),
			Gid:    uint32(spec.Identity.GID),
			Groups: []uint32{uint32(spec.Identity.GID)},
		},
		Setpgid: true,
		Pgid:    0,
	}
	if err := command.Start(); err != nil {
		_ = statusWriter.Close()
		_ = jitReader.Close()
		_ = jitWriter.Close()
		return 0, runner.ErrStartFailed
	}
	pid := command.Process.Pid
	_ = statusWriter.Close()
	_ = jitReader.Close()
	reaped := make(chan error, 1)
	go func() {
		reaped <- command.Wait()
		close(reaped)
	}()
	fail := func(cause error) (int, error) {
		_ = jitWriter.Close()
		_ = statusReader.Close()
		_ = nativeRuntime.processes.Kill(-pid, syscall.SIGKILL)
		select {
		case <-reaped:
		case <-time.After(time.Second):
		}
		return 0, cause
	}
	group, err := unix.Getpgid(pid)
	if err != nil || group != pid || !nativeRuntime.exactSlotProcess(ctx, pid, pid) {
		return fail(runner.ErrStrongOwnershipUnavailable)
	}
	if err := admit(ctx, pid); err != nil {
		return fail(runner.ErrStartFenced)
	}
	written, copyErr := io.Copy(
		jitWriter,
		io.LimitReader(material, maxJITMaterialBytes+1),
	)
	closeErr := jitWriter.Close()
	if copyErr != nil || closeErr != nil || written <= 0 || written > maxJITMaterialBytes {
		return fail(runner.ErrStartFailed)
	}
	status := make(chan []byte, 1)
	go func() {
		value, _ := io.ReadAll(io.LimitReader(statusReader, 256))
		status <- value
	}()
	select {
	case value := <-status:
		if len(value) != 0 {
			return fail(runner.ErrStartFailed)
		}
		return pid, nil
	case <-ctx.Done():
		return fail(runner.ErrStartFailed)
	}
}

func (nativeRuntime *FileRuntime) KillAndWait(
	ctx context.Context,
	containment runner.ContainmentRef,
) error {
	if ctx == nil || ctx.Err() != nil || !nativeRuntime.validContainment(containment) {
		return runner.ErrCleanupFailed
	}
	state, err := nativeRuntime.readFenceState(containment)
	if err != nil || (state.State != fenceStateRevoked && state.State != fenceStateFinal) {
		return runner.ErrCleanupFailed
	}
	if containment.HostEpoch != nativeRuntime.hostEpoch {
		processes, err := nativeRuntime.slotProcesses(ctx)
		if err != nil || len(processes) != 0 {
			return runner.ErrCleanupFailed
		}
		return nil
	}
	if state.PID > 0 {
		if err := nativeRuntime.processes.Kill(-state.PID, syscall.SIGKILL); err != nil &&
			!errors.Is(err, syscall.ESRCH) {
			return runner.ErrCleanupFailed
		}
	}
	for {
		processes, err := nativeRuntime.slotProcesses(ctx)
		if err != nil {
			return runner.ErrCleanupFailed
		}
		if len(processes) == 0 {
			return nil
		}
		for _, process := range processes {
			if process.PID <= 1 {
				return runner.ErrCleanupFailed
			}
			if !nativeRuntime.exactSlotProcess(ctx, process.PID, process.PGID) {
				continue
			}
			if err := nativeRuntime.processes.Kill(process.PID, syscall.SIGKILL); err != nil &&
				!errors.Is(err, syscall.ESRCH) {
				return runner.ErrCleanupFailed
			}
		}
		if err := waitPoll(ctx); err != nil {
			return runner.ErrCleanupFailed
		}
	}
}

func (nativeRuntime *FileRuntime) WaitEmpty(
	ctx context.Context,
	containment runner.ContainmentRef,
) error {
	if ctx == nil || ctx.Err() != nil || !nativeRuntime.validContainment(containment) {
		return runner.ErrStrongOwnershipUnavailable
	}
	for {
		processes, err := nativeRuntime.slotProcesses(ctx)
		if err != nil {
			return runner.ErrStrongOwnershipUnavailable
		}
		if len(processes) == 0 {
			return nil
		}
		if err := waitPoll(ctx); err != nil {
			return runner.ErrStrongOwnershipUnavailable
		}
	}
}

func (nativeRuntime *FileRuntime) Alive(
	ctx context.Context,
	containment runner.ContainmentRef,
	pid int,
) (bool, error) {
	if ctx == nil || ctx.Err() != nil || pid <= 0 ||
		!nativeRuntime.validContainment(containment) {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	state, err := nativeRuntime.readFenceState(containment)
	if err != nil || state.State != fenceStateLaunched || state.PID != pid {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	if containment.HostEpoch != nativeRuntime.hostEpoch {
		return false, nil
	}
	return nativeRuntime.exactSlotProcess(ctx, pid, pid), nil
}

func (nativeRuntime *FileRuntime) FinalizeFence(
	ctx context.Context,
	containment runner.ContainmentRef,
) error {
	fence, err := nativeRuntime.lockFence(ctx, containment, false)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	defer fence.Close()
	state, err := fence.readState()
	if err != nil {
		return runner.ErrCleanupFailed
	}
	if state.State == fenceStateFinal {
		return nil
	}
	if state.State != fenceStateRevoked {
		return runner.ErrCleanupFailed
	}
	processes, err := nativeRuntime.slotProcesses(ctx)
	if err != nil || len(processes) != 0 {
		return runner.ErrCleanupFailed
	}
	return fence.writeState(ctx, fenceState{
		Version: fenceVersion,
		State:   fenceStateFinal,
		PID:     state.PID,
	})
}

func (nativeRuntime *FileRuntime) GarbageCollectFence(
	ctx context.Context,
	containment runner.ContainmentRef,
) error {
	directory := filepath.Join(nativeRuntime.fenceRoot, containment.OwnerID)
	if _, err := os.Lstat(directory); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return runner.ErrCleanupFailed
	}
	fence, err := nativeRuntime.lockFence(ctx, containment, false)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return runner.ErrCleanupFailed
	}
	state, stateErr := fence.readState()
	if stateErr != nil || state.State != fenceStateFinal {
		_ = fence.Close()
		return runner.ErrCleanupFailed
	}
	// Keep the flock until the directory has been unlinked and its parent
	// synced. Releasing it first would let a replacement Agent adopt the old
	// fence between validation and deletion.
	for _, name := range []string{"state.json", "metadata.json", "lock"} {
		if err := os.Remove(filepath.Join(fence.directory, name)); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			_ = fence.Close()
			return runner.ErrCleanupFailed
		}
	}
	if err := os.Remove(fence.directory); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = fence.Close()
		return runner.ErrCleanupFailed
	}
	syncErr := syncDirectory(nativeRuntime.fenceRoot)
	closeErr := fence.Close()
	if syncErr != nil || closeErr != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (nativeRuntime *FileRuntime) readFenceState(
	containment runner.ContainmentRef,
) (fenceState, error) {
	if !nativeRuntime.validContainment(containment) {
		return fenceState{}, runner.ErrCleanupFailed
	}
	directory := filepath.Join(nativeRuntime.fenceRoot, containment.OwnerID)
	if !safePrivateDirectory(
		directory,
		nativeRuntime.ownerUID,
		nativeRuntime.ownerGID,
	) {
		return fenceState{}, runner.ErrCleanupFailed
	}
	metadataBytes, err := readPrivateFile(
		filepath.Join(directory, "metadata.json"),
		nativeRuntime.ownerUID,
		nativeRuntime.ownerGID,
		4<<10,
	)
	if err != nil {
		return fenceState{}, runner.ErrCleanupFailed
	}
	var metadata fenceMetadata
	expected := fenceMetadata{
		Version: fenceVersion, Token: containment.FenceToken,
		HostEpoch: containment.HostEpoch,
	}
	if decodeStrictJSON(metadataBytes, &metadata) != nil || metadata != expected {
		return fenceState{}, runner.ErrCleanupFailed
	}
	contents, err := readPrivateFile(
		filepath.Join(directory, "state.json"),
		nativeRuntime.ownerUID,
		nativeRuntime.ownerGID,
		4<<10,
	)
	if err != nil {
		return fenceState{}, runner.ErrCleanupFailed
	}
	var state fenceState
	if decodeStrictJSON(contents, &state) != nil || state.Version != fenceVersion ||
		!validFenceState(state) {
		return fenceState{}, runner.ErrCleanupFailed
	}
	return state, nil
}

func (nativeRuntime *FileRuntime) slotProcesses(
	ctx context.Context,
) ([]processObservation, error) {
	processes, err := nativeRuntime.processes.Processes(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]processObservation, 0)
	for _, process := range processes {
		if process.RUID == nativeRuntime.identity.UID ||
			process.EUID == nativeRuntime.identity.UID {
			result = append(result, process)
		}
	}
	return result, nil
}

func (nativeRuntime *FileRuntime) exactSlotProcess(
	ctx context.Context,
	pid int,
	pgid int,
) bool {
	processes, err := nativeRuntime.slotProcesses(ctx)
	if err != nil {
		return false
	}
	for _, process := range processes {
		if process.PID == pid && process.PGID == pgid {
			return true
		}
	}
	return false
}

func (nativeRuntime *FileRuntime) validContainment(
	containment runner.ContainmentRef,
) bool {
	return containment.Backend == containmentBackend &&
		validOwner(containment.OwnerID) &&
		containment.Scope == filepath.ToSlash(
			filepath.Join("tewake", containment.OwnerID),
		) &&
		containment.HostEpoch != "" &&
		containment.InvocationID == "" &&
		canonicalFenceToken(containment.FenceToken)
}

func validOwner(owner string) bool {
	if !strings.HasPrefix(owner, "tewake-") ||
		len(owner) != len("tewake-")+sha256.Size*2 {
		return false
	}
	for _, character := range owner[len("tewake-"):] {
		if !(character >= '0' && character <= '9') &&
			!(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func validLaunchSpec(spec LaunchSpec) bool {
	return filepath.IsAbs(spec.Executable) &&
		filepath.IsAbs(spec.Directory) &&
		filepath.Clean(spec.Executable) == filepath.Join(spec.Directory, "run.sh") &&
		spec.WorkspaceRef.Backend == WorkspaceBackend &&
		spec.WorkspaceRef.OwnerID != "" &&
		fixedRunnerArguments(spec.Arguments)
}

func fixedRunnerArguments(arguments []string) bool {
	return len(arguments) == 1 && arguments[0] == "--ephemeral" ||
		len(arguments) == 2 && arguments[0] == "--ephemeral" &&
			arguments[1] == "--disableupdate"
}

func helperArguments(arguments []string) []string {
	result := []string{helperModeArgument, "--"}
	return append(result, arguments...)
}

func fixedRunnerEnvironment(directory string) []string {
	home := filepath.Join(directory, ".tewake-home")
	return []string{
		"HOME=" + home,
		"XDG_CONFIG_HOME=" + filepath.Join(home, ".config"),
		"XDG_CACHE_HOME=" + filepath.Join(home, ".cache"),
		"TMPDIR=" + filepath.Join(home, ".tmp"),
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		"LANG=en_US.UTF-8",
	}
}

func RunExecLauncherHelper(args []string) (bool, error) {
	if len(args) == 0 || args[0] != helperModeArgument {
		return false, nil
	}
	if len(args) < 3 || args[1] != "--" || !fixedRunnerArguments(args[2:]) {
		reportHelperFailure()
		return true, runner.ErrStartFailed
	}
	jit, err := io.ReadAll(io.LimitReader(os.Stdin, maxJITMaterialBytes+1))
	if err != nil || len(jit) == 0 || len(jit) > maxJITMaterialBytes {
		clear(jit)
		reportHelperFailure()
		return true, runner.ErrStartFailed
	}
	defer clear(jit)
	if err := syscall.Fchdir(4); err != nil {
		reportHelperFailure()
		return true, runner.ErrStartFailed
	}
	syscall.CloseOnExec(3)
	runnerArguments := []string{"bash", "/dev/fd/5"}
	runnerArguments = append(runnerArguments, args[2:]...)
	runnerArguments = append(runnerArguments, "--jitconfig", string(jit))
	if err := syscall.Exec("/bin/bash", runnerArguments, os.Environ()); err != nil {
		reportHelperFailure()
		return true, err
	}
	return true, nil
}

func reportHelperFailure() {
	status := os.NewFile(uintptr(3), "tewake-launch-status")
	if status == nil {
		return
	}
	_, _ = status.Write([]byte("exec failed\n"))
	_ = status.Close()
}

func currentBootEpoch() (string, error) {
	boot, err := unix.SysctlTimeval("kern.boottime")
	if err != nil || boot == nil || boot.Sec <= 0 {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	value := strconv.FormatInt(boot.Sec, 10) + ":" +
		strconv.FormatInt(int64(boot.Usec), 10)
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:]), nil
}

func waitPoll(ctx context.Context) error {
	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func safePrivateDirectory(path string, uid, gid int) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return false
	}
	info, err := os.Lstat(path)
	return err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 &&
		ownedBy(info, uid, gid) && info.Mode().Perm() == 0o700
}

func safePrivateDirectoryChain(path string, uid, gid int) bool {
	if !safePrivateDirectory(path, uid, gid) {
		return false
	}
	for current := filepath.Dir(path); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return false
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || (int(stat.Uid) != 0 && int(stat.Uid) != uid) {
			return false
		}
		if info.Mode().Perm()&0o022 != 0 &&
			!(stat.Uid == 0 && info.Mode()&os.ModeSticky != 0) {
			return false
		}
		if current == "/" {
			return true
		}
	}
}

func readPrivateFile(path string, uid, gid int, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || limit <= 0 {
		return nil, runner.ErrCleanupFailed
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode().Perm() != 0o600 ||
		!ownedBy(before, uid, gid) || !singleLink(before) ||
		before.Size() < 1 || before.Size() > limit {
		return nil, runner.ErrCleanupFailed
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, runner.ErrCleanupFailed
	}
	contents, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || len(contents) == 0 || int64(len(contents)) > limit {
		return nil, runner.ErrCleanupFailed
	}
	return contents, nil
}

func writeNoClobberJSON(path string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	parent := filepath.Dir(path)
	temporary, err := os.CreateTemp(parent, ".tewake-json-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, path); err != nil {
		return err
	}
	if err := os.Remove(temporaryPath); err != nil {
		return err
	}
	return syncDirectory(parent)
}

func replaceJSON(path string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	parent := filepath.Dir(path)
	temporary, err := os.CreateTemp(parent, ".tewake-state-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(parent)
}

func decodeStrictJSON(contents []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON")
		}
		return err
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

var _ Runtime = (*FileRuntime)(nil)
var _ RuntimeAdmission = (*FileRuntime)(nil)
