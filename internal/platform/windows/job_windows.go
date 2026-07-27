//go:build windows

package windows

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/winacl"
	syswindows "golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

const (
	maxJITMaterialBytes = 256 << 10
	jobPollInterval     = 100 * time.Millisecond
	fenceVersionLine    = "tewake-windows-fence-v1"

	jobObjectQuery     = 0x0004
	jobObjectTerminate = 0x0008
	lockfileExclusive  = 0x00000002
	lockfileFailFast   = 0x00000001
)

var (
	kernel32             = syswindows.NewLazySystemDLL("kernel32.dll")
	procOpenJobObject    = kernel32.NewProc("OpenJobObjectW")
	procGetLastError     = kernel32.NewProc("GetLastError")
	errJobObjectNotFound = syscall.Errno(2)
)

type runnerTokenSource interface {
	Token(context.Context) (syswindows.Token, string, error)
}

// ServiceTokenSource duplicates the primary token of a dedicated inert Windows
// service. SCM owns that service logon, so Tewake never stores a runner-account
// password beside the Agent's DPAPI material.
type ServiceTokenSource struct {
	ServiceName string
}

func (source ServiceTokenSource) Token(ctx context.Context) (syswindows.Token, string, error) {
	if ctx == nil || ctx.Err() != nil || source.ServiceName == "" {
		return 0, "", runner.ErrStrongOwnershipUnavailable
	}
	manager, err := mgr.Connect()
	if err != nil {
		return 0, "", runner.ErrStrongOwnershipUnavailable
	}
	defer manager.Disconnect()
	service, err := manager.OpenService(source.ServiceName)
	if err != nil {
		return 0, "", runner.ErrStrongOwnershipUnavailable
	}
	defer service.Close()
	status, err := service.Query()
	if err != nil || status.State != svc.Running || status.ProcessId == 0 {
		return 0, "", runner.ErrStrongOwnershipUnavailable
	}
	if err := ctx.Err(); err != nil {
		return 0, "", runner.ErrStrongOwnershipUnavailable
	}
	process, err := syswindows.OpenProcess(
		syswindows.PROCESS_QUERY_LIMITED_INFORMATION,
		false,
		status.ProcessId,
	)
	if err != nil {
		return 0, "", runner.ErrStrongOwnershipUnavailable
	}
	defer syswindows.CloseHandle(process)
	var sourceToken syswindows.Token
	access := uint32(
		syswindows.TOKEN_ASSIGN_PRIMARY |
			syswindows.TOKEN_DUPLICATE |
			syswindows.TOKEN_QUERY,
	)
	if err := syswindows.OpenProcessToken(process, access, &sourceToken); err != nil {
		return 0, "", runner.ErrStrongOwnershipUnavailable
	}
	defer sourceToken.Close()
	var token syswindows.Token
	duplicateAccess := uint32(
		syswindows.TOKEN_ASSIGN_PRIMARY |
			syswindows.TOKEN_DUPLICATE |
			syswindows.TOKEN_QUERY |
			syswindows.TOKEN_ADJUST_DEFAULT |
			syswindows.TOKEN_ADJUST_SESSIONID,
	)
	if err := syswindows.DuplicateTokenEx(
		sourceToken,
		duplicateAccess,
		nil,
		syswindows.SecurityImpersonation,
		syswindows.TokenPrimary,
		&token,
	); err != nil {
		return 0, "", runner.ErrStrongOwnershipUnavailable
	}
	user, err := token.GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil || !user.User.Sid.IsValid() {
		token.Close()
		return 0, "", runner.ErrStrongOwnershipUnavailable
	}
	return token, user.User.Sid.String(), nil
}

type jobHandle struct {
	ref    runner.ContainmentRef
	handle syswindows.Handle
}

// JobRuntime is owned by the Agent Windows Service. Every child is created
// suspended under a distinct runner-service token, assigned to a kill-on-close
// Job Object, and only then resumed.
type JobRuntime struct {
	runtimeRoot string
	tokenSource runnerTokenSource
	runnerSID   string
	serviceSID  string
	hostEpoch   string

	mu     sync.Mutex
	closed bool
	jobs   map[string]jobHandle
}

func NewJobRuntime(
	ctx context.Context,
	runtimeRoot string,
	runnerIdentityService string,
) (*JobRuntime, string, error) {
	return newJobRuntime(
		ctx,
		runtimeRoot,
		ServiceTokenSource{ServiceName: runnerIdentityService},
	)
}

func newJobRuntime(
	ctx context.Context,
	runtimeRoot string,
	tokenSource runnerTokenSource,
) (*JobRuntime, string, error) {
	if ctx == nil || !filepath.IsAbs(runtimeRoot) ||
		filepath.Clean(runtimeRoot) != runtimeRoot ||
		tokenSource == nil {
		return nil, "", runner.ErrStrongOwnershipUnavailable
	}
	epoch, err := randomToken()
	if err != nil {
		return nil, "", runner.ErrStrongOwnershipUnavailable
	}
	serviceSID, err := currentProcessSID()
	if err != nil || !strings.EqualFold(serviceSID, "S-1-5-18") {
		return nil, "", runner.ErrStrongOwnershipUnavailable
	}
	runnerToken, runnerSID, err := tokenSource.Token(ctx)
	if err != nil {
		return nil, "", runner.ErrStrongOwnershipUnavailable
	}
	runnerToken.Close()
	if runnerSID == "" || strings.EqualFold(runnerSID, serviceSID) {
		// A same-identity runner could read and decrypt the Agent's DPAPI state.
		return nil, "", runner.ErrStrongOwnershipUnavailable
	}
	runtime := &JobRuntime{
		runtimeRoot: runtimeRoot,
		tokenSource: tokenSource,
		runnerSID:   runnerSID,
		serviceSID:  serviceSID,
		hostEpoch:   epoch,
		jobs:        make(map[string]jobHandle),
	}
	return runtime, runnerSID, nil
}

func (runtime *JobRuntime) HostEpoch() string {
	if runtime == nil {
		return ""
	}
	return runtime.hostEpoch
}

func (runtime *JobRuntime) ServiceSID() string {
	if runtime == nil {
		return ""
	}
	return runtime.serviceSID
}

func (runtime *JobRuntime) ValidateAdmission(ctx context.Context) error {
	if runtime == nil || ctx == nil || ctx.Err() != nil ||
		!canonicalToken(runtime.hostEpoch) ||
		runtime.runnerSID == "" ||
		runtime.serviceSID == "" ||
		!strings.EqualFold(runtime.serviceSID, "S-1-5-18") ||
		strings.EqualFold(runtime.runnerSID, runtime.serviceSID) {
		return runner.ErrStrongOwnershipUnavailable
	}
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	if closed {
		return runner.ErrStrongOwnershipUnavailable
	}
	token, sid, err := runtime.tokenSource.Token(ctx)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	token.Close()
	if !strings.EqualFold(sid, runtime.runnerSID) {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (runtime *JobRuntime) EnsureJob(
	ctx context.Context,
	ref runner.ContainmentRef,
) error {
	if runtime == nil || ctx == nil || ctx.Err() != nil ||
		ref.HostEpoch != runtime.hostEpoch ||
		!validPreparedJobRef(ref) {
		return runner.ErrStrongOwnershipUnavailable
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return runner.ErrStrongOwnershipUnavailable
	}
	if existing, found := runtime.jobs[ref.OwnerID]; found {
		if !samePreparedJobBoundary(existing.ref, ref) {
			return runner.ErrStrongOwnershipUnavailable
		}
		active, err := activeJobProcesses(existing.handle)
		if err != nil || active != 0 {
			return runner.ErrStrongOwnershipUnavailable
		}
		return nil
	}
	name, err := syswindows.UTF16PtrFromString(ref.Scope)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	descriptor, err := syswindows.SecurityDescriptorFromString(
		"D:P(A;;GA;;;SY)(A;;GA;;;BA)",
	)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	attributes := &syswindows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(syswindows.SecurityAttributes{})),
		SecurityDescriptor: descriptor,
	}
	handle, err := syswindows.CreateJobObject(attributes, name)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	lastError, _, _ := procGetLastError.Call()
	if syscall.Errno(lastError) == syswindows.ERROR_ALREADY_EXISTS {
		syswindows.CloseHandle(handle)
		return runner.ErrStrongOwnershipUnavailable
	}
	info := syswindows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags =
		syswindows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE |
			syswindows.JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION
	if _, err := syswindows.SetInformationJobObject(
		handle,
		syswindows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		syswindows.CloseHandle(handle)
		return runner.ErrStrongOwnershipUnavailable
	}
	var observed syswindows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	if err := syswindows.QueryInformationJobObject(
		handle,
		syswindows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&observed)),
		uint32(unsafe.Sizeof(observed)),
		nil,
	); err != nil ||
		observed.BasicLimitInformation.LimitFlags&syswindows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE == 0 ||
		observed.BasicLimitInformation.LimitFlags&syswindows.JOB_OBJECT_LIMIT_BREAKAWAY_OK != 0 ||
		observed.BasicLimitInformation.LimitFlags&syswindows.JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK != 0 {
		syswindows.CloseHandle(handle)
		return runner.ErrStrongOwnershipUnavailable
	}
	runtime.jobs[ref.OwnerID] = jobHandle{ref: ref, handle: handle}
	return nil
}

func (runtime *JobRuntime) LockFence(
	ctx context.Context,
	ref runner.ContainmentRef,
) (Fence, error) {
	if runtime == nil || ctx == nil || ctx.Err() != nil || !validJobRef(ref) {
		return nil, runner.ErrCleanupFailed
	}
	if ref.HostEpoch == runtime.hostEpoch {
		if err := runtime.bindJobFence(ref); err != nil {
			return nil, runner.ErrCleanupFailed
		}
	}
	return lockFileFence(ctx, runtime.runtimeRoot, ref)
}

func (runtime *JobRuntime) Launch(
	ctx context.Context,
	spec LaunchSpec,
	material io.Reader,
) (int, error) {
	if runtime == nil || ctx == nil || ctx.Err() != nil ||
		material == nil || !runtime.validLaunchSpec(spec) {
		return 0, runner.ErrStartFailed
	}
	jit, err := io.ReadAll(io.LimitReader(material, maxJITMaterialBytes+1))
	if err != nil || len(jit) == 0 || len(jit) > maxJITMaterialBytes {
		clear(jit)
		return 0, runner.ErrStartFailed
	}
	defer clear(jit)
	token, sid, err := runtime.tokenSource.Token(ctx)
	if err != nil || !strings.EqualFold(sid, runtime.runnerSID) {
		if token != 0 {
			token.Close()
		}
		return 0, runner.ErrStartFailed
	}
	defer token.Close()
	handle, err := runtime.duplicateJob(spec.Containment)
	if err != nil {
		return 0, runner.ErrStartFailed
	}
	defer syswindows.CloseHandle(handle)

	listener := filepath.Join(spec.Directory, "bin", "Runner.Listener.exe")
	if !noReparseTree(spec.Directory) || !safeWindowsExecutable(listener) {
		return 0, runner.ErrStartFailed
	}
	environment, err := disposableRunnerEnvironment(token, spec.Directory)
	if err != nil {
		return 0, runner.ErrStartFailed
	}
	defer clear(environment)
	// Recheck after creating the disposable profile so a raced-in reparse
	// point never reaches CreateProcessAsUser.
	if !noReparseTree(spec.Directory) {
		return 0, runner.ErrStartFailed
	}
	arguments := []string{listener, "run"}
	arguments = append(arguments, spec.Arguments...)
	arguments = append(arguments, "--jitconfig", string(jit))
	commandLine, err := syswindows.UTF16FromString(
		syswindows.ComposeCommandLine(arguments),
	)
	if err != nil {
		return 0, runner.ErrStartFailed
	}
	defer clear(commandLine)
	application, err := syswindows.UTF16PtrFromString(listener)
	if err != nil {
		return 0, runner.ErrStartFailed
	}
	directory, err := syswindows.UTF16PtrFromString(spec.Directory)
	if err != nil {
		return 0, runner.ErrStartFailed
	}
	startup := syswindows.StartupInfo{
		Cb:         uint32(unsafe.Sizeof(syswindows.StartupInfo{})),
		Flags:      syswindows.STARTF_USESHOWWINDOW,
		ShowWindow: syswindows.SW_HIDE,
	}
	var process syswindows.ProcessInformation
	flags := uint32(
		syswindows.CREATE_SUSPENDED |
			syswindows.CREATE_UNICODE_ENVIRONMENT |
			syswindows.CREATE_NO_WINDOW,
	)
	if err := syswindows.CreateProcessAsUser(
		token,
		application,
		&commandLine[0],
		nil,
		nil,
		false,
		flags,
		&environment[0],
		directory,
		&startup,
		&process,
	); err != nil {
		return 0, runner.ErrStartFailed
	}
	resumed := false
	defer func() {
		if !resumed {
			_ = syswindows.TerminateProcess(process.Process, 1)
		}
		syswindows.CloseHandle(process.Thread)
		syswindows.CloseHandle(process.Process)
	}()
	if ctx.Err() != nil ||
		syswindows.AssignProcessToJobObject(handle, process.Process) != nil {
		return 0, runner.ErrStartFailed
	}
	if _, err := syswindows.ResumeThread(process.Thread); err != nil {
		return 0, runner.ErrStartFailed
	}
	resumed = true
	return int(process.ProcessId), nil
}

func disposableRunnerEnvironment(
	token syswindows.Token,
	executionRoot string,
) ([]uint16, error) {
	home := filepath.Join(executionRoot, "_tewake-home")
	temporary := filepath.Join(executionRoot, "_tewake-tmp")
	appData := filepath.Join(home, "AppData", "Roaming")
	localAppData := filepath.Join(home, "AppData", "Local")
	for _, directory := range []string{home, temporary, appData, localAppData} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, runner.ErrStartFailed
		}
	}
	environment, err := token.Environ(false)
	if err != nil {
		return nil, runner.ErrStartFailed
	}
	values := make(map[string]string, len(environment)+10)
	names := make(map[string]string, len(environment)+10)
	for _, entry := range environment {
		index := strings.IndexByte(entry, '=')
		if index <= 0 || strings.IndexByte(entry, 0) >= 0 {
			continue
		}
		name := entry[:index]
		key := strings.ToUpper(name)
		names[key] = name
		values[key] = entry[index+1:]
	}
	overrides := map[string]string{
		"APPDATA":         appData,
		"HOME":            home,
		"LOCALAPPDATA":    localAppData,
		"RUNNER_TEMP":     temporary,
		"TEMP":            temporary,
		"TMP":             temporary,
		"USERPROFILE":     home,
		"XDG_CACHE_HOME":  filepath.Join(home, ".cache"),
		"XDG_CONFIG_HOME": filepath.Join(home, ".config"),
	}
	if volume := filepath.VolumeName(home); volume != "" {
		overrides["HOMEDRIVE"] = volume
		overrides["HOMEPATH"] = strings.TrimPrefix(home, volume)
	}
	for name, value := range overrides {
		names[name] = name
		values[name] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var block []uint16
	for _, key := range keys {
		entry, err := syswindows.UTF16FromString(names[key] + "=" + values[key])
		if err != nil {
			clear(block)
			return nil, runner.ErrStartFailed
		}
		block = append(block, entry...)
		clear(entry)
	}
	block = append(block, 0)
	if len(block) < 2 {
		clear(block)
		return nil, runner.ErrStartFailed
	}
	return block, nil
}

func (runtime *JobRuntime) TerminateAndWait(
	ctx context.Context,
	ref runner.ContainmentRef,
) error {
	if runtime == nil || ctx == nil || ctx.Err() != nil || !validJobRef(ref) {
		return runner.ErrCleanupFailed
	}
	handle, err := runtime.duplicateOrOpenJob(ref)
	if errors.Is(err, errJobObjectNotFound) {
		// A prior service process configured kill-on-close before publishing this
		// ref. Its missing named object therefore proves that boundary is gone.
		return nil
	}
	if err != nil {
		return runner.ErrCleanupFailed
	}
	defer syswindows.CloseHandle(handle)
	if err := syswindows.TerminateJobObject(handle, 1); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := waitJobEmpty(ctx, handle); err != nil {
		return runner.ErrCleanupFailed
	}
	runtime.dropJob(ref)
	return nil
}

func (runtime *JobRuntime) WaitEmpty(
	ctx context.Context,
	ref runner.ContainmentRef,
) error {
	if runtime == nil || ctx == nil || !validJobRef(ref) {
		return runner.ErrStrongOwnershipUnavailable
	}
	handle, err := runtime.duplicateOrOpenJob(ref)
	if errors.Is(err, errJobObjectNotFound) {
		return nil
	}
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	defer syswindows.CloseHandle(handle)
	if err := waitJobEmpty(ctx, handle); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (runtime *JobRuntime) Alive(
	ctx context.Context,
	ref runner.ContainmentRef,
	_ int,
) (bool, error) {
	if runtime == nil || ctx == nil || ctx.Err() != nil || !validJobRef(ref) {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	handle, err := runtime.duplicateOrOpenJob(ref)
	if errors.Is(err, errJobObjectNotFound) {
		return false, nil
	}
	if err != nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	defer syswindows.CloseHandle(handle)
	active, err := activeJobProcesses(handle)
	if err != nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	return active > 0, nil
}

func (runtime *JobRuntime) FinalizeCleanup(
	ctx context.Context,
	ref runner.ContainmentRef,
	root *os.Root,
	name string,
	expected runner.WorkspaceRef,
	workspace Workspace,
) error {
	if runtime == nil || ctx == nil || ctx.Err() != nil || root == nil ||
		workspace == nil || !validJobRef(ref) {
		return runner.ErrCleanupFailed
	}
	fence, err := runtime.LockFence(ctx, ref)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	defer fence.Close()
	revoked, err := fence.Revoked()
	if err != nil || !revoked {
		return runner.ErrCleanupFailed
	}
	if alive, err := runtime.Alive(ctx, ref, 0); err != nil || alive {
		return runner.ErrCleanupFailed
	}
	observed, err := workspace.Observe(ctx, root, name)
	if err != nil || observed != expected {
		return runner.ErrCleanupFailed
	}
	if err := workspace.Remove(ctx, root, name); err != nil {
		// ERROR_SHARING_VIOLATION from a locked file deliberately reaches the
		// core as CleanupFailed and quarantines the node.
		return runner.ErrCleanupFailed
	}
	if fileFence, ok := fence.(*diskFence); !ok ||
		fileFence.markFinalized(ctx) != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (runtime *JobRuntime) GarbageCollectFence(
	ctx context.Context,
	ref runner.ContainmentRef,
) error {
	if runtime == nil || ctx == nil || ctx.Err() != nil || !validJobRef(ref) {
		return runner.ErrCleanupFailed
	}
	return removeFinalizedFence(ctx, runtime.runtimeRoot, ref)
}

func (runtime *JobRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return nil
	}
	runtime.closed = true
	handles := make([]syswindows.Handle, 0, len(runtime.jobs))
	for _, job := range runtime.jobs {
		handles = append(handles, job.handle)
	}
	clear(runtime.jobs)
	runtime.mu.Unlock()
	var failed bool
	for _, handle := range handles {
		// KILL_ON_JOB_CLOSE is the final service-crash/recovery owner.
		if err := syswindows.CloseHandle(handle); err != nil {
			failed = true
		}
	}
	if failed {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (runtime *JobRuntime) validCurrentRef(ref runner.ContainmentRef) bool {
	return validJobRef(ref) && ref.HostEpoch == runtime.hostEpoch
}

func (runtime *JobRuntime) validLaunchSpec(spec LaunchSpec) bool {
	if !runtime.validCurrentRef(spec.Containment) ||
		spec.WorkspaceRef.Backend != WorkspaceBackend ||
		spec.WorkspaceRef.OwnerID == "" ||
		!filepath.IsAbs(spec.Directory) ||
		filepath.Clean(spec.Directory) != spec.Directory ||
		filepath.Clean(spec.Executable) != filepath.Join(spec.Directory, "run.cmd") ||
		!fixedRunnerArguments(spec.Arguments) {
		return false
	}
	executions := filepath.Join(runtime.runtimeRoot, "executions")
	relative, err := filepath.Rel(executions, spec.Directory)
	return err == nil && validWorkspaceName(relative)
}

func fixedRunnerArguments(arguments []string) bool {
	return len(arguments) == 1 && arguments[0] == "--ephemeral" ||
		len(arguments) == 2 &&
			arguments[0] == "--ephemeral" &&
			arguments[1] == "--disableupdate"
}

func validJobRef(ref runner.ContainmentRef) bool {
	return validPreparedJobBoundary(ref) &&
		canonicalToken(ref.FenceToken)
}

func validPreparedJobRef(ref runner.ContainmentRef) bool {
	return validPreparedJobBoundary(ref) && ref.FenceToken == ""
}

func validPreparedJobBoundary(ref runner.ContainmentRef) bool {
	return ref.Backend == containmentBackend &&
		strings.HasPrefix(ref.OwnerID, "tewake-") &&
		len(ref.OwnerID) == len("tewake-")+sha256HexLength &&
		ref.Scope == jobObjectName(ref.HostEpoch, ref.OwnerID) &&
		canonicalToken(ref.HostEpoch) &&
		ref.InvocationID == ""
}

func samePreparedJobBoundary(left, right runner.ContainmentRef) bool {
	left.FenceToken = ""
	right.FenceToken = ""
	return left == right &&
		validPreparedJobRef(left) &&
		validPreparedJobRef(right)
}

const sha256HexLength = 64

func (runtime *JobRuntime) duplicateJob(
	ref runner.ContainmentRef,
) (syswindows.Handle, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	job, found := runtime.jobs[ref.OwnerID]
	if !found || job.ref != ref {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	var duplicate syswindows.Handle
	process, _ := syswindows.GetCurrentProcess()
	if err := syswindows.DuplicateHandle(
		process,
		job.handle,
		process,
		&duplicate,
		0,
		false,
		syswindows.DUPLICATE_SAME_ACCESS,
	); err != nil {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	return duplicate, nil
}

func (runtime *JobRuntime) bindJobFence(ref runner.ContainmentRef) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed || !runtime.validCurrentRef(ref) {
		return runner.ErrStrongOwnershipUnavailable
	}
	job, found := runtime.jobs[ref.OwnerID]
	if !found {
		return runner.ErrStrongOwnershipUnavailable
	}
	if job.ref == ref {
		return nil
	}
	if !validPreparedJobRef(job.ref) ||
		job.ref.Backend != ref.Backend ||
		job.ref.OwnerID != ref.OwnerID ||
		job.ref.Scope != ref.Scope ||
		job.ref.HostEpoch != ref.HostEpoch ||
		job.ref.InvocationID != ref.InvocationID {
		return runner.ErrStrongOwnershipUnavailable
	}
	job.ref = ref
	runtime.jobs[ref.OwnerID] = job
	return nil
}

func (runtime *JobRuntime) duplicateOrOpenJob(
	ref runner.ContainmentRef,
) (syswindows.Handle, error) {
	if handle, err := runtime.duplicateJob(ref); err == nil {
		return handle, nil
	}
	name, err := syswindows.UTF16PtrFromString(ref.Scope)
	if err != nil {
		return 0, runner.ErrCleanupFailed
	}
	handle, _, callErr := procOpenJobObject.Call(
		uintptr(jobObjectQuery|jobObjectTerminate),
		0,
		uintptr(unsafe.Pointer(name)),
	)
	if handle == 0 {
		if errors.Is(callErr, syswindows.ERROR_FILE_NOT_FOUND) {
			return 0, errJobObjectNotFound
		}
		return 0, runner.ErrCleanupFailed
	}
	return syswindows.Handle(handle), nil
}

func (runtime *JobRuntime) dropJob(ref runner.ContainmentRef) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	job, found := runtime.jobs[ref.OwnerID]
	if !found || job.ref != ref {
		return
	}
	delete(runtime.jobs, ref.OwnerID)
	_ = syswindows.CloseHandle(job.handle)
}

type jobBasicAccounting struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

func activeJobProcesses(handle syswindows.Handle) (uint32, error) {
	var accounting jobBasicAccounting
	if err := syswindows.QueryInformationJobObject(
		handle,
		syswindows.JobObjectBasicAccountingInformation,
		uintptr(unsafe.Pointer(&accounting)),
		uint32(unsafe.Sizeof(accounting)),
		nil,
	); err != nil {
		return 0, err
	}
	return accounting.ActiveProcesses, nil
}

func waitJobEmpty(ctx context.Context, handle syswindows.Handle) error {
	timer := time.NewTicker(jobPollInterval)
	defer timer.Stop()
	for {
		active, err := activeJobProcesses(handle)
		if err != nil {
			return err
		}
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func safeWindowsExecutable(path string) bool {
	pointer, err := syswindows.UTF16PtrFromString(path)
	if err != nil {
		return false
	}
	attributes, err := syswindows.GetFileAttributes(pointer)
	if err != nil ||
		attributes&syswindows.FILE_ATTRIBUTE_DIRECTORY != 0 ||
		attributes&syswindows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return false
	}
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0
}

func currentProcessSID() (string, error) {
	user, err := syswindows.GetCurrentProcessToken().GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil || !user.User.Sid.IsValid() {
		return "", runner.ErrStrongOwnershipUnavailable
	}
	return user.User.Sid.String(), nil
}

func randomToken() (string, error) {
	var value [16]byte
	if _, err := io.ReadFull(rand.Reader, value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

type fenceState string

const (
	fenceActive    fenceState = "active"
	fenceLaunched  fenceState = "launched"
	fenceRevoked   fenceState = "revoked"
	fenceFinalized fenceState = "finalized"
)

type diskFence struct {
	file  *os.File
	path  string
	ref   runner.ContainmentRef
	state fenceState
}

func lockFileFence(
	ctx context.Context,
	runtimeRoot string,
	ref runner.ContainmentRef,
) (*diskFence, error) {
	fenceRoot := filepath.Join(runtimeRoot, ".tewake-fences")
	if err := ensurePrivateFenceDirectory(fenceRoot); err != nil {
		return nil, runner.ErrCleanupFailed
	}
	directory := filepath.Join(fenceRoot, ref.OwnerID)
	if err := ensurePrivateFenceDirectory(directory); err != nil {
		return nil, runner.ErrCleanupFailed
	}
	path := filepath.Join(directory, ref.FenceToken+".fence")
	file, err := openPrivateFenceFile(path)
	if err != nil {
		return nil, runner.ErrCleanupFailed
	}
	locked := false
	defer func() {
		if !locked {
			file.Close()
		}
	}()
	overlapped := new(syswindows.Overlapped)
	for {
		err = syswindows.LockFileEx(
			syswindows.Handle(file.Fd()),
			lockfileExclusive|lockfileFailFast,
			0,
			1,
			0,
			overlapped,
		)
		if err == nil {
			break
		}
		if !errors.Is(err, syswindows.ERROR_LOCK_VIOLATION) {
			return nil, runner.ErrCleanupFailed
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, runner.ErrCleanupFailed
		case <-timer.C:
		}
	}
	locked = true
	fence := &diskFence{file: file, path: path, ref: ref}
	state, err := readFenceState(file, ref)
	if errors.Is(err, io.EOF) {
		fence.state = fenceActive
		if err := fence.writeState(fenceActive); err != nil {
			_ = fence.Close()
			return nil, runner.ErrCleanupFailed
		}
	} else if err != nil {
		_ = fence.Close()
		return nil, runner.ErrCleanupFailed
	} else {
		fence.state = state
	}
	return fence, nil
}

func ensurePrivateFenceDirectory(path string) error {
	if err := os.Mkdir(path, 0o700); err == nil {
		if err := winacl.SecureEmptyPrivateDirectory(path); err != nil {
			_ = os.Remove(path)
			return err
		}
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	return winacl.ValidatePrivateDirectory(path)
}

func openPrivateFenceFile(path string) (*os.File, error) {
	pointer, err := syswindows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := syswindows.CreateFile(
		pointer,
		syswindows.GENERIC_READ|syswindows.GENERIC_WRITE,
		syswindows.FILE_SHARE_READ|syswindows.FILE_SHARE_WRITE|syswindows.FILE_SHARE_DELETE,
		nil,
		syswindows.CREATE_NEW,
		syswindows.FILE_ATTRIBUTE_NORMAL|syswindows.FILE_FLAG_WRITE_THROUGH,
		0,
	)
	if err == nil {
		file := os.NewFile(uintptr(handle), path)
		if file == nil {
			syswindows.CloseHandle(handle)
			return nil, runner.ErrCleanupFailed
		}
		if err := winacl.SecurePrivateFile(path); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return nil, err
		}
		return file, nil
	}
	if !errors.Is(err, syswindows.ERROR_FILE_EXISTS) &&
		!errors.Is(err, syswindows.ERROR_ALREADY_EXISTS) {
		return nil, err
	}
	if err := winacl.ValidatePrivateFile(path); err != nil {
		return nil, err
	}
	file, openErr := os.OpenFile(path, os.O_RDWR, 0)
	if openErr != nil {
		return nil, openErr
	}
	if err := winacl.ValidatePrivateFile(path); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (fence *diskFence) Revoked() (bool, error) {
	if fence == nil || fence.file == nil {
		return false, runner.ErrCleanupFailed
	}
	return fence.state == fenceRevoked || fence.state == fenceFinalized, nil
}

func (fence *diskFence) MarkLaunched(ctx context.Context) error {
	if fence == nil || fence.file == nil || ctx == nil || ctx.Err() != nil ||
		fence.state != fenceActive {
		return runner.ErrStartFenced
	}
	return fence.writeState(fenceLaunched)
}

func (fence *diskFence) Revoke(ctx context.Context) error {
	if fence == nil || fence.file == nil || ctx == nil || ctx.Err() != nil {
		return runner.ErrCleanupFailed
	}
	if fence.state == fenceFinalized || fence.state == fenceRevoked {
		return nil
	}
	return fence.writeState(fenceRevoked)
}

func (fence *diskFence) markFinalized(ctx context.Context) error {
	if fence == nil || fence.file == nil || ctx == nil || ctx.Err() != nil ||
		fence.state != fenceRevoked {
		return runner.ErrCleanupFailed
	}
	return fence.writeState(fenceFinalized)
}

func (fence *diskFence) writeState(state fenceState) error {
	content := fenceContent(fence.ref, state)
	if err := fence.file.Truncate(0); err != nil {
		return runner.ErrCleanupFailed
	}
	if _, err := fence.file.Seek(0, io.SeekStart); err != nil {
		return runner.ErrCleanupFailed
	}
	if _, err := fence.file.Write(content); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := fence.file.Sync(); err != nil {
		return runner.ErrCleanupFailed
	}
	fence.state = state
	return nil
}

func (fence *diskFence) Close() error {
	if fence == nil || fence.file == nil {
		return nil
	}
	file := fence.file
	fence.file = nil
	var overlapped syswindows.Overlapped
	unlockErr := syswindows.UnlockFileEx(
		syswindows.Handle(file.Fd()),
		0,
		1,
		0,
		&overlapped,
	)
	closeErr := file.Close()
	if unlockErr != nil || closeErr != nil {
		return runner.ErrCleanupFailed
	}
	return nil
}

func fenceContent(ref runner.ContainmentRef, state fenceState) []byte {
	return []byte(fmt.Sprintf(
		"%s\nbackend=%s\nowner=%s\nscope=%s\nepoch=%s\ntoken=%s\nstate=%s\n",
		fenceVersionLine,
		ref.Backend,
		ref.OwnerID,
		ref.Scope,
		ref.HostEpoch,
		ref.FenceToken,
		state,
	))
}

func readFenceState(
	file *os.File,
	ref runner.ContainmentRef,
) (fenceState, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	content, err := io.ReadAll(io.LimitReader(file, 2049))
	if err != nil {
		return "", err
	}
	if len(content) == 0 {
		return "", io.EOF
	}
	if len(content) > 2048 {
		return "", runner.ErrCleanupFailed
	}
	for _, state := range []fenceState{
		fenceActive,
		fenceLaunched,
		fenceRevoked,
		fenceFinalized,
	} {
		if bytes.Equal(content, fenceContent(ref, state)) {
			return state, nil
		}
	}
	return "", runner.ErrCleanupFailed
}

func removeFinalizedFence(
	ctx context.Context,
	runtimeRoot string,
	ref runner.ContainmentRef,
) error {
	fence, err := lockFileFence(ctx, runtimeRoot, ref)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	if fence.state != fenceFinalized {
		_ = fence.Close()
		return runner.ErrCleanupFailed
	}
	path := fence.path
	directory := filepath.Dir(path)
	if err := fence.Close(); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return runner.ErrCleanupFailed
	}
	if err := os.Remove(directory); err != nil &&
		!errors.Is(err, os.ErrNotExist) &&
		!errors.Is(err, syswindows.ERROR_DIR_NOT_EMPTY) {
		return runner.ErrCleanupFailed
	}
	return nil
}

var _ Runtime = (*JobRuntime)(nil)
