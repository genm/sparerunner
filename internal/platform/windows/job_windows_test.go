//go:build windows

package windows

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/genm/tewake/internal/runner"
	syswindows "golang.org/x/sys/windows"
)

type currentTokenSource struct{}

func (currentTokenSource) Token(context.Context) (syswindows.Token, string, error) {
	var token syswindows.Token
	if err := syswindows.OpenProcessToken(
		syswindows.CurrentProcess(),
		syswindows.TOKEN_ASSIGN_PRIMARY|
			syswindows.TOKEN_DUPLICATE|
			syswindows.TOKEN_QUERY,
		&token,
	); err != nil {
		return 0, "", err
	}
	user, err := token.GetTokenUser()
	if err != nil {
		token.Close()
		return 0, "", err
	}
	return token, user.User.Sid.String(), nil
}

func TestJobRuntimeRejectsRunnerTokenFromAgentIdentity(t *testing.T) {
	if _, _, err := newJobRuntime(
		context.Background(),
		t.TempDir(),
		currentTokenSource{},
	); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("newJobRuntime error = %v", err)
	}
}

func TestJobRuntimeBindsExactlyOneCoreFenceAfterContainmentPreparation(t *testing.T) {
	epoch := "0123456789abcdef0123456789abcdef"
	owner := "tewake-" + strings.Repeat("a", sha256HexLength)
	runtime := &JobRuntime{
		runtimeRoot: t.TempDir(),
		hostEpoch:   epoch,
		jobs:        make(map[string]jobHandle),
	}
	ref := runner.ContainmentRef{
		Backend:   containmentBackend,
		OwnerID:   owner,
		Scope:     jobObjectName(epoch, owner),
		HostEpoch: epoch,
	}
	if err := runtime.EnsureJob(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	ref.FenceToken = "abcdef0123456789abcdef0123456789"
	fence, err := runtime.LockFence(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	handle, err := runtime.duplicateJob(ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := syswindows.CloseHandle(handle); err != nil {
		t.Fatal(err)
	}
	unfenced := ref
	unfenced.FenceToken = ""
	if err := runtime.EnsureJob(context.Background(), unfenced); err != nil {
		t.Fatalf("idempotent containment preparation: %v", err)
	}
	conflict := ref
	conflict.FenceToken = "0123456789abcdef0123456789abcdef"
	if _, err := runtime.LockFence(context.Background(), conflict); err == nil {
		t.Fatal("second core fence was accepted for one Job Object")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestJobObjectTerminationOwnsDescendantTree(t *testing.T) {
	serviceSID, err := currentProcessSID()
	if err != nil {
		t.Fatal(err)
	}
	runtime := &JobRuntime{
		runtimeRoot: t.TempDir(),
		tokenSource: currentTokenSource{},
		runnerSID:   serviceSID,
		serviceSID:  "S-1-5-18",
		hostEpoch:   "0123456789abcdef0123456789abcdef",
		jobs:        make(map[string]jobHandle),
	}
	ref := runner.ContainmentRef{
		Backend:      containmentBackend,
		OwnerID:      "tewake-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Scope:        jobObjectName(runtime.hostEpoch, "tewake-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		HostEpoch:    runtime.hostEpoch,
		FenceToken:   "abcdef0123456789abcdef0123456789",
		InvocationID: "",
	}
	if err := runtime.EnsureJob(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	job, err := runtime.duplicateJob(ref)
	if err != nil {
		t.Fatal(err)
	}
	defer syswindows.CloseHandle(job)

	pidFile := filepath.Join(t.TempDir(), "descendant.pid")
	commandLine, err := syswindows.UTF16FromString(syswindows.ComposeCommandLine([]string{
		os.Args[0],
		"-test.run=TestWindowsJobHelperProcess",
		"--",
		"parent",
		pidFile,
	}))
	if err != nil {
		t.Fatal(err)
	}
	application, err := syswindows.UTF16PtrFromString(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	startup := syswindows.StartupInfo{Cb: uint32(unsafe.Sizeof(syswindows.StartupInfo{}))}
	var process syswindows.ProcessInformation
	if err := syswindows.CreateProcess(
		application,
		&commandLine[0],
		nil,
		nil,
		false,
		syswindows.CREATE_SUSPENDED|syswindows.CREATE_NO_WINDOW,
		nil,
		nil,
		&startup,
		&process,
	); err != nil {
		t.Fatal(err)
	}
	resumed := false
	defer func() {
		if !resumed {
			_ = syswindows.TerminateProcess(process.Process, 1)
		}
		syswindows.CloseHandle(process.Thread)
		syswindows.CloseHandle(process.Process)
	}()
	if err := syswindows.AssignProcessToJobObject(job, process.Process); err != nil {
		t.Fatal(err)
	}
	if _, err := syswindows.ResumeThread(process.Thread); err != nil {
		t.Fatal(err)
	}
	resumed = true

	descendantPID := waitForPIDFile(t, pidFile)
	descendant, err := syswindows.OpenProcess(
		syswindows.SYNCHRONIZE|syswindows.PROCESS_QUERY_LIMITED_INFORMATION,
		false,
		uint32(descendantPID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer syswindows.CloseHandle(descendant)
	if err := runtime.TerminateAndWait(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	result, err := syswindows.WaitForSingleObject(descendant, 5_000)
	if err != nil || result != syswindows.WAIT_OBJECT_0 {
		t.Fatalf("descendant wait result=%#x err=%v", result, err)
	}
}

func TestWindowsJobHelperProcess(t *testing.T) {
	separator := -1
	for index, argument := range os.Args {
		if argument == "--" {
			separator = index
			break
		}
	}
	if separator < 0 || len(os.Args) != separator+3 {
		return
	}
	mode, pidFile := os.Args[separator+1], os.Args[separator+2]
	if mode == "parent" {
		child := exec.Command(
			os.Args[0],
			"-test.run=TestWindowsJobHelperProcess",
			"--",
			"child",
			pidFile,
		)
		if err := child.Start(); err != nil {
			os.Exit(71)
		}
		if err := os.WriteFile(
			pidFile,
			[]byte(strconv.Itoa(child.Process.Pid)),
			0o600,
		); err != nil {
			os.Exit(72)
		}
	}
	select {}
}

func TestLockedFileProducesWindowsSharingViolation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "locked.txt")
	pointer, err := syswindows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := syswindows.CreateFile(
		pointer,
		syswindows.GENERIC_READ|syswindows.GENERIC_WRITE,
		0,
		nil,
		syswindows.CREATE_NEW,
		syswindows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer syswindows.CloseHandle(handle)
	if err := os.RemoveAll(root); err == nil {
		t.Fatal("locked workspace was reported removed")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("locked file disappeared: %v", err)
	}
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(string(contents))
			if parseErr != nil || pid <= 0 {
				t.Fatalf("invalid descendant pid %q", contents)
			}
			return pid
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("descendant pid was not published")
	return 0
}
