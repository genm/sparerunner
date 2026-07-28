//go:build linux

package linux

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

type helperTestWorkspace struct {
	agent      RunnerIdentity
	identity   RunnerIdentity
	ref        runner.WorkspaceRef
	admission  *helperAdmissionState
	observeErr error
	removeErr  error
	absentErr  error
	absent     *bool
}

func (helperTestWorkspace) OfficialAuthorityConfigured() bool { return true }

func (workspace helperTestWorkspace) ValidateOfficialAuthority(context.Context, string) error {
	if workspace.admission == nil {
		return nil
	}
	workspace.admission.mu.Lock()
	defer workspace.admission.mu.Unlock()
	workspace.admission.calls++
	return workspace.admission.err
}

func (workspace helperTestWorkspace) ValidateRuntimeRoot(context.Context, string) error {
	return nil
}
func (workspace helperTestWorkspace) Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return workspace.ref, nil
}
func (workspace helperTestWorkspace) PrepareOfficial(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return workspace.ref, nil
}
func (helperTestWorkspace) PinLaunch(
	context.Context,
	*os.Root,
	string,
	runner.WorkspaceRef,
) (*pinnedWorkspace, error) {
	directory, err := os.Open("/")
	if err != nil {
		return nil, err
	}
	executable, err := os.Open("/dev/null")
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	return &pinnedWorkspace{directory: directory, executable: executable}, nil
}
func (workspace helperTestWorkspace) Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return workspace.ref, workspace.observeErr
}
func (workspace helperTestWorkspace) Remove(context.Context, *os.Root, string) error {
	return workspace.removeErr
}
func (workspace helperTestWorkspace) Absent(context.Context, *os.Root, string) (bool, error) {
	if workspace.absent != nil {
		return *workspace.absent, workspace.absentErr
	}
	return true, workspace.absentErr
}
func (workspace helperTestWorkspace) AgentIdentity() RunnerIdentity  { return workspace.agent }
func (workspace helperTestWorkspace) RunnerIdentity() RunnerIdentity { return workspace.identity }
func (helperTestWorkspace) SlotBusy(context.Context, *os.Root, string) (bool, error) {
	return false, nil
}

type helperRecordingRuntime struct {
	*testRuntime
	mu              sync.Mutex
	lastSpec        LaunchSpec
	admissionCalls  int
	admissionErr    error
	finalized       map[string]bool
	finalizationErr error
	finalizeErr     error
	garbageErr      error
	shutdownCalls   int
}

type helperAdmissionState struct {
	mu    sync.Mutex
	calls int
	err   error
}

type helperRuntimeWithoutAdmission struct {
	*testRuntime
}

func (runtime *helperRuntimeWithoutAdmission) SlotBusy(context.Context, runner.ContainmentRef) (bool, error) {
	return false, nil
}

func (runtime *helperRecordingRuntime) ValidateAdmission(context.Context) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.admissionCalls++
	return runtime.admissionErr
}

func (runtime *helperRecordingRuntime) FenceFinalized(_ context.Context, containment runner.ContainmentRef) (bool, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.finalized[containmentSessionKey(containment)], nil
}

func (runtime *helperRecordingRuntime) ValidateFinalization(
	_ context.Context,
	_ runner.ContainmentRef,
) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.finalizationErr
}

func (runtime *helperRecordingRuntime) FinalizeFence(_ context.Context, containment runner.ContainmentRef) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.finalizeErr != nil {
		return runtime.finalizeErr
	}
	if runtime.finalized == nil {
		runtime.finalized = make(map[string]bool)
	}
	runtime.finalized[containmentSessionKey(containment)] = true
	return nil
}

func (runtime *helperRecordingRuntime) GarbageCollectFence(_ context.Context, containment runner.ContainmentRef) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.garbageErr != nil {
		return runtime.garbageErr
	}
	delete(runtime.finalized, containmentSessionKey(containment))
	return nil
}

func (runtime *helperRecordingRuntime) Shutdown(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime.mu.Lock()
	runtime.shutdownCalls++
	runtime.mu.Unlock()
	runtime.testRuntime.mu.Lock()
	defer runtime.testRuntime.mu.Unlock()
	for _, fence := range runtime.fences {
		fence.mu.Lock()
		fence.revoked = true
		fence.mu.Unlock()
	}
	if runtime.launches > runtime.kills {
		runtime.kills = runtime.launches
	}
	return nil
}

func TestHelperPolicyRejectsBroadOrNoncanonicalPathsAndRootIdentities(t *testing.T) {
	base := HelperPolicy{
		SocketPath:  "/run/sparerunner/supervisor.sock",
		RuntimeRoot: "/var/lib/sparerunner-runtime",
		CacheRoot:   "/var/cache/sparerunner-agent",
		AgentUID:    1001,
		AgentGID:    1001,
		RunnerUID:   1002,
		RunnerGID:   1002,
	}
	mutations := []func(*HelperPolicy){
		func(policy *HelperPolicy) { policy.SocketPath = "/run/sparerunner/../supervisor.sock" },
		func(policy *HelperPolicy) { policy.RuntimeRoot = "/" },
		func(policy *HelperPolicy) { policy.AgentUID = 0 },
		func(policy *HelperPolicy) {
			policy.RunnerUID = policy.AgentUID
			policy.RunnerGID = policy.AgentGID
		},
	}
	if err := base.validate(); err != nil {
		t.Fatalf("base policy error=%v", err)
	}
	for index, mutate := range mutations {
		policy := base
		mutate(&policy)
		if err := policy.validate(); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
			t.Fatalf("mutation %d error=%v", index, err)
		}
	}
}

func (runtime *helperRecordingRuntime) Launch(ctx context.Context, spec LaunchSpec, material io.Reader) (int, error) {
	runtime.mu.Lock()
	runtime.lastSpec = spec
	runtime.mu.Unlock()
	return runtime.testRuntime.Launch(ctx, spec, material)
}

func (runtime *helperRecordingRuntime) Alive(context.Context, runner.ContainmentRef, int) (bool, error) {
	runtime.testRuntime.mu.Lock()
	defer runtime.testRuntime.mu.Unlock()
	return runtime.launches > 0, nil
}

func (runtime *helperRecordingRuntime) SlotBusy(context.Context, runner.ContainmentRef) (bool, error) {
	runtime.testRuntime.mu.Lock()
	defer runtime.testRuntime.mu.Unlock()
	return runtime.launches > runtime.kills, nil
}

func newHelperHarness(t *testing.T) (*HelperServer, *HelperClient, *helperRecordingRuntime, HelperPolicy) {
	t.Helper()
	if os.Geteuid() == 0 || os.Getegid() == 0 {
		t.Skip("the production Agent peer is intentionally non-root")
	}
	runtimeRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(runtimeRoot, "executions"), 0o700); err != nil {
		t.Fatal(err)
	}
	policy := HelperPolicy{
		SocketPath:  filepath.Join(t.TempDir(), "supervisor.sock"),
		RuntimeRoot: runtimeRoot,
		CacheRoot:   t.TempDir(),
		AgentUID:    os.Geteuid(),
		AgentGID:    os.Getegid(),
		RunnerUID:   os.Geteuid() + 10000,
		RunnerGID:   os.Getegid() + 10000,
	}
	recording := &helperRecordingRuntime{
		testRuntime: newTestRuntime(),
		finalized:   make(map[string]bool),
	}
	workspace := helperTestWorkspace{
		agent:     RunnerIdentity{UID: policy.AgentUID, GID: policy.AgentGID},
		identity:  RunnerIdentity{UID: policy.RunnerUID, GID: policy.RunnerGID},
		ref:       runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"},
		admission: &helperAdmissionState{},
	}
	server, err := NewHelperServer(policy, recording, workspace)
	if err != nil {
		t.Fatal(err)
	}
	client := &HelperClient{
		policy:   policy,
		sessions: make(map[string]*helperClientSession),
	}
	client.dial = func(context.Context) (*net.UnixConn, error) {
		serverConnection, clientConnection := unixSocketPair(t)
		go server.serveConnection(context.Background(), serverConnection)
		return clientConnection, nil
	}
	return server, client, recording, policy
}

func TestHelperServerRejectsWorkspaceWithoutOfficialPackageAuthority(t *testing.T) {
	policy := HelperPolicy{
		SocketPath:  "/run/sparerunner/supervisor.sock",
		RuntimeRoot: "/var/lib/sparerunner-runtime",
		CacheRoot:   "/var/cache/sparerunner-agent",
		AgentUID:    1001,
		AgentGID:    1001,
		RunnerUID:   1002,
		RunnerGID:   1002,
	}
	workspace := NewOSWorkspace(
		policy.AgentUID, policy.AgentGID, policy.RunnerUID, policy.RunnerGID,
	)
	if _, err := NewHelperServer(policy, &helperRecordingRuntime{testRuntime: newTestRuntime()}, workspace); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("NewHelperServer error=%v", err)
	}
}

func TestHelperServerRejectsRuntimeWithoutAdmissionProbe(t *testing.T) {
	policy := HelperPolicy{
		SocketPath:  "/run/sparerunner/supervisor.sock",
		RuntimeRoot: "/var/lib/sparerunner-runtime",
		CacheRoot:   "/var/cache/sparerunner-agent",
		AgentUID:    1001,
		AgentGID:    1001,
		RunnerUID:   1002,
		RunnerGID:   1002,
	}
	workspace := helperTestWorkspace{
		agent:     RunnerIdentity{UID: policy.AgentUID, GID: policy.AgentGID},
		identity:  RunnerIdentity{UID: policy.RunnerUID, GID: policy.RunnerGID},
		admission: &helperAdmissionState{},
	}
	runtime := &helperRuntimeWithoutAdmission{testRuntime: newTestRuntime()}
	if _, err := NewHelperServer(policy, runtime, workspace); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("NewHelperServer error=%v", err)
	}
}

func TestAdapterValidateRuntimeRootProbesLiveHelperSocket(t *testing.T) {
	server, client, runtime, policy := newHelperHarness(t)
	adapter, err := New(Config{
		Identity: StaticIdentity{
			UID: policy.RunnerUID,
			GID: policy.RunnerGID,
		},
	}, client, client)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.ValidateRuntimeRoot(context.Background(), policy.RuntimeRoot); err != nil {
		t.Fatalf("live helper readiness probe failed: %v", err)
	}
	runtime.mu.Lock()
	runtimeCalls := runtime.admissionCalls
	runtime.mu.Unlock()
	workspaceAdmission := server.packageAuthority.(helperTestWorkspace).admission
	workspaceAdmission.mu.Lock()
	workspaceCalls := workspaceAdmission.calls
	workspaceAdmission.mu.Unlock()
	if runtimeCalls != 1 || workspaceCalls != 1 {
		t.Fatalf("admission calls runtime=%d workspace=%d", runtimeCalls, workspaceCalls)
	}

	runtime.mu.Lock()
	runtime.admissionErr = errors.New("cgroup or fence degraded")
	runtime.mu.Unlock()
	if err := adapter.ValidateRuntimeRoot(context.Background(), policy.RuntimeRoot); err == nil {
		t.Fatal("readiness probe accepted degraded cgroup or fence authority")
	}
	runtime.mu.Lock()
	runtime.admissionErr = nil
	runtime.mu.Unlock()

	workspaceAdmission.mu.Lock()
	workspaceAdmission.err = errors.New("official archive authority degraded")
	workspaceAdmission.mu.Unlock()
	if err := adapter.ValidateRuntimeRoot(context.Background(), policy.RuntimeRoot); err == nil {
		t.Fatal("readiness probe accepted degraded official archive authority")
	}
	workspaceAdmission.mu.Lock()
	workspaceAdmission.err = nil
	workspaceAdmission.mu.Unlock()

	client.dial = func(context.Context) (*net.UnixConn, error) {
		return nil, errors.New("helper socket unavailable")
	}
	if err := adapter.ValidateRuntimeRoot(context.Background(), policy.RuntimeRoot); err == nil {
		t.Fatal("readiness probe accepted an unavailable helper socket")
	}
}

func TestAdmissionProbeRejectsJITOrCallerSelectedPayload(t *testing.T) {
	jitLength := 1
	request := newHelperRequest(helperOpValidateAdmission)
	request.JITLength = &jitLength
	if err := validateHelperRequest(request); err == nil {
		t.Fatal("admission probe accepted JIT-bearing payload")
	}
	request = newHelperRequest(helperOpValidateAdmission)
	request.RootName = strings.Repeat("a", 64)
	if err := validateHelperRequest(request); err == nil {
		t.Fatal("admission probe accepted a caller-selected path")
	}
}

func unixSocketPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	descriptors, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	connections := make([]*net.UnixConn, 0, 2)
	for index, descriptor := range descriptors {
		file := os.NewFile(uintptr(descriptor), fmt.Sprintf("helper-socket-%d", index))
		connection, convertErr := net.FileConn(file)
		_ = file.Close()
		if convertErr != nil {
			t.Fatal(convertErr)
		}
		unixConnection, ok := connection.(*net.UnixConn)
		if !ok {
			t.Fatalf("socketpair connection type = %T", connection)
		}
		connections = append(connections, unixConnection)
	}
	return connections[0], connections[1]
}

func helperTestContainment(rootName string) runner.ContainmentRef {
	return runner.ContainmentRef{
		Backend:    containmentBackend,
		OwnerID:    "sparerunner-" + rootName,
		Scope:      filepath.Join("sparerunner", "sparerunner-"+rootName),
		HostEpoch:  "boot-test",
		FenceToken: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}

func TestHelperLaunchDerivesPrivilegeInputsAndKeepsJITOutOfFrame(t *testing.T) {
	_, client, runtime, policy := newHelperHarness(t)
	rootName := strings.Repeat("a", 64)
	containment := helperTestContainment(rootName)
	fence, err := client.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	defer fence.Close()
	directory := filepath.Join(policy.RuntimeRoot, "executions", rootName)
	jit := "jit-secret-do-not-format.example.test"
	pid, err := client.Launch(context.Background(), LaunchSpec{
		Executable:   filepath.Join(directory, "run.sh"),
		Directory:    directory,
		Arguments:    []string{"--ephemeral", "--disableupdate"},
		UID:          policy.RunnerUID,
		GID:          policy.RunnerGID,
		WorkspaceRef: runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"},
		Containment:  containment,
	}, strings.NewReader(jit))
	if err != nil || pid <= 0 {
		t.Fatalf("Launch pid=%d err=%v", pid, err)
	}
	runtime.mu.Lock()
	spec := runtime.lastSpec
	runtime.mu.Unlock()
	runtime.testRuntime.mu.Lock()
	delivered := runtime.jit
	runtime.testRuntime.mu.Unlock()
	if spec.Executable != filepath.Join(directory, "run.sh") ||
		spec.UID != policy.RunnerUID || spec.GID != policy.RunnerGID ||
		!fixedRunnerArguments(spec.Arguments) || delivered != jit {
		t.Fatalf("fixed launch mismatch: spec=%#v delivered=%q", spec, delivered)
	}
	request := newHelperRequest(helperOpLaunch)
	request.RootName = rootName
	request.DisableUpdate = boolPointer(true)
	request.WorkspaceRef = &spec.WorkspaceRef
	request.JITLength = intPointer(len(jit))
	wire, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wire, []byte(jit)) || strings.Contains(fmt.Sprintf("%#v", request), jit) {
		t.Fatal("JIT material entered the structured protocol frame")
	}
}

func TestHelperClientRejectsArbitraryLaunchInputsBeforeJITRead(t *testing.T) {
	_, client, runtime, policy := newHelperHarness(t)
	rootName := strings.Repeat("b", 64)
	containment := helperTestContainment(rootName)
	fence, err := client.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	defer fence.Close()
	read := false
	material := readerFunc(func([]byte) (int, error) {
		read = true
		return 0, io.EOF
	})
	_, err = client.Launch(context.Background(), LaunchSpec{
		Executable:   "/tmp/attacker",
		Directory:    filepath.Join(policy.RuntimeRoot, "executions", rootName),
		Arguments:    []string{"--ephemeral", "--arbitrary"},
		UID:          0,
		GID:          0,
		WorkspaceRef: runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"},
		Containment:  containment,
	}, material)
	if !errors.Is(err, runner.ErrStrongOwnershipUnavailable) || read {
		t.Fatalf("Launch error=%v materialRead=%v", err, read)
	}
	runtime.testRuntime.mu.Lock()
	defer runtime.testRuntime.mu.Unlock()
	if runtime.launches != 0 || runtime.jit != "" {
		t.Fatal("rejected launch reached the privileged runtime")
	}
}

func TestHelperServerNeverLaunchesThroughPersistedRevokedFence(t *testing.T) {
	_, client, runtime, policy := newHelperHarness(t)
	runtime.testRuntime.mu.Lock()
	runtime.revokeNew = true
	runtime.testRuntime.mu.Unlock()
	rootName := strings.Repeat("9", 64)
	containment := helperTestContainment(rootName)
	fence, err := client.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	defer fence.Close()
	directory := filepath.Join(policy.RuntimeRoot, "executions", rootName)
	_, err = client.Launch(context.Background(), LaunchSpec{
		Executable:   filepath.Join(directory, "run.sh"),
		Directory:    directory,
		Arguments:    []string{"--ephemeral"},
		UID:          policy.RunnerUID,
		GID:          policy.RunnerGID,
		WorkspaceRef: runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"},
		Containment:  containment,
	}, strings.NewReader("must-not-reach-root-helper.example.test"))
	if !errors.Is(err, runner.ErrStartFailed) {
		t.Fatalf("Launch error=%v", err)
	}
	runtime.testRuntime.mu.Lock()
	defer runtime.testRuntime.mu.Unlock()
	if runtime.launches != 0 || runtime.jit != "" {
		t.Fatal("revoked fence launched a runner or retained JIT")
	}
}

func TestHelperServerRejectsConcurrentExecutionOnSingleSlotIdentity(t *testing.T) {
	_, client, runtime, policy := newHelperHarness(t)
	firstRoot := strings.Repeat("6", 64)
	firstContainment := helperTestContainment(firstRoot)
	firstFence, err := client.LockFence(context.Background(), firstContainment)
	if err != nil {
		t.Fatal(err)
	}
	firstDirectory := filepath.Join(policy.RuntimeRoot, "executions", firstRoot)
	if _, err := client.Launch(context.Background(), LaunchSpec{
		Executable:   filepath.Join(firstDirectory, "run.sh"),
		Directory:    firstDirectory,
		Arguments:    []string{"--ephemeral"},
		UID:          policy.RunnerUID,
		GID:          policy.RunnerGID,
		WorkspaceRef: runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"},
		Containment:  firstContainment,
	}, strings.NewReader("first-jit.example.test")); err != nil {
		t.Fatal(err)
	}
	if err := firstFence.Close(); err != nil {
		t.Fatal(err)
	}

	secondRoot := strings.Repeat("7", 64)
	secondContainment := helperTestContainment(secondRoot)
	secondFence, err := client.LockFence(context.Background(), secondContainment)
	if err != nil {
		t.Fatal(err)
	}
	defer secondFence.Close()
	secondDirectory := filepath.Join(policy.RuntimeRoot, "executions", secondRoot)
	_, err = client.Launch(context.Background(), LaunchSpec{
		Executable:   filepath.Join(secondDirectory, "run.sh"),
		Directory:    secondDirectory,
		Arguments:    []string{"--ephemeral"},
		UID:          policy.RunnerUID,
		GID:          policy.RunnerGID,
		WorkspaceRef: runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"},
		Containment:  secondContainment,
	}, strings.NewReader("second-jit-must-not-launch.example.test"))
	if !errors.Is(err, runner.ErrStartFailed) {
		t.Fatalf("second Launch error=%v", err)
	}
	runtime.testRuntime.mu.Lock()
	defer runtime.testRuntime.mu.Unlock()
	if runtime.launches != 1 || runtime.jit != "first-jit.example.test" {
		t.Fatalf("launches=%d jit=%q", runtime.launches, runtime.jit)
	}
}

type readerFunc func([]byte) (int, error)

func (function readerFunc) Read(buffer []byte) (int, error) { return function(buffer) }

func TestHelperProtocolRejectsDuplicateUnknownOversizedAndMismatchedFrames(t *testing.T) {
	validID := strings.Repeat("c", 32)
	cases := []struct {
		name string
		data []byte
	}{
		{
			name: "duplicate field",
			data: []byte(`{"version":1,"requestId":"` + validID + `","operation":"validate_root","operation":"alive"}`),
		},
		{
			name: "unknown field",
			data: []byte(`{"version":1,"requestId":"` + validID + `","operation":"validate_root","argv":["sh"]}`),
		},
		{
			name: "protocol version",
			data: []byte(`{"version":2,"requestId":"` + validID + `","operation":"validate_root"}`),
		},
		{
			name: "unknown operation",
			data: []byte(`{"version":1,"requestId":"` + validID + `","operation":"exec"}`),
		},
		{
			name: "malformed",
			data: []byte(`{"version":1`),
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var request helperRequest
			if err := decodeHelperTestFrame(testCase.data, &request); err == nil {
				if validateHelperRequest(request) == nil {
					t.Fatal("invalid protocol frame was accepted")
				}
			}
		})
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], maxHelperFrameBytes+1)
	if err := readHelperFrame(bytes.NewReader(header[:]), &helperRequest{}); err == nil {
		t.Fatal("oversized protocol frame was accepted")
	}
}

func decodeHelperTestFrame(data []byte, destination any) error {
	var framed bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	framed.Write(header[:])
	framed.Write(data)
	return readHelperFrame(&framed, destination)
}

func TestHelperFenceSessionRejectsDuplicateRequestAndCleansContainment(t *testing.T) {
	server, _, runtime, _ := newHelperHarness(t)
	serverConnection, clientConnection := unixSocketPair(t)
	go server.serveConnection(context.Background(), serverConnection)
	rootName := strings.Repeat("d", 64)
	containment := helperTestContainment(rootName)
	lock := newHelperRequest(helperOpLock)
	lock.Containment = &containment
	if _, err := roundTripHelper(context.Background(), clientConnection, lock, nil); err != nil {
		t.Fatal(err)
	}
	duplicate := helperRequest{
		Version: helperProtocolVersion, RequestID: lock.RequestID, Operation: helperOpRevoked,
	}
	response, err := roundTripHelper(context.Background(), clientConnection, duplicate, nil)
	if err == nil || response.Code != helperCodeProtocol {
		t.Fatalf("duplicate response=%#v err=%v", response, err)
	}
	_ = clientConnection.Close()
	waitForHelperCleanup(t, runtime)
}

func TestHelperFenceDisconnectRevokesBeforeKill(t *testing.T) {
	_, client, runtime, _ := newHelperHarness(t)
	rootName := strings.Repeat("e", 64)
	containment := helperTestContainment(rootName)
	fence, err := client.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	session := fence.(*helperClientSession)
	_ = session.connection.Close()
	waitForHelperCleanup(t, runtime)
}

func directFenceSession(
	t *testing.T,
	ctx context.Context,
	server *HelperServer,
	containment runner.ContainmentRef,
) (*net.UnixConn, <-chan struct{}) {
	t.Helper()
	serverConnection, clientConnection := unixSocketPair(t)
	first := newHelperRequest(helperOpLock)
	first.Containment = &containment
	served := make(chan struct{})
	go func() {
		server.serveFenceSession(ctx, serverConnection, first)
		close(served)
	}()
	var initial helperResponse
	if err := readHelperFrame(clientConnection, &initial); err != nil || !initial.OK ||
		initial.Flag == nil {
		t.Fatalf("initial fence response=%#v err=%v", initial, err)
	}
	return clientConnection, served
}

func directFenceServer(t *testing.T) (*HelperServer, *helperRecordingRuntime, HelperPolicy) {
	t.Helper()
	runtimeRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(runtimeRoot, "executions"), 0o700); err != nil {
		t.Fatal(err)
	}
	policy := HelperPolicy{
		SocketPath:  filepath.Join(t.TempDir(), "supervisor.sock"),
		RuntimeRoot: runtimeRoot,
		CacheRoot:   t.TempDir(),
		AgentUID:    1001,
		AgentGID:    1001,
		RunnerUID:   1002,
		RunnerGID:   1002,
	}
	runtime := &helperRecordingRuntime{
		testRuntime: newTestRuntime(),
		finalized:   make(map[string]bool),
	}
	workspace := helperTestWorkspace{
		agent:     RunnerIdentity{UID: policy.AgentUID, GID: policy.AgentGID},
		identity:  RunnerIdentity{UID: policy.RunnerUID, GID: policy.RunnerGID},
		ref:       runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"},
		admission: &helperAdmissionState{},
	}
	server, err := NewHelperServer(policy, runtime, workspace)
	if err != nil {
		t.Fatal(err)
	}
	return server, runtime, policy
}

func directLaunchRequest(rootName string, containment runner.ContainmentRef) helperRequest {
	disableUpdate := false
	jitLength := len("durable-launch.example.test")
	ref := runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"}
	request := newHelperRequest(helperOpLaunch)
	request.RootName = rootName
	request.DisableUpdate = &disableUpdate
	request.WorkspaceRef = &ref
	request.JITLength = &jitLength
	return request
}

func TestLaunchAckLossPreservesDurableLaunchAndRestartStopsSameContainment(t *testing.T) {
	server, runtime, _ := directFenceServer(t)
	rootName := strings.Repeat("a", 64)
	containment := helperTestContainment(rootName)
	first, firstServed := directFenceSession(t, context.Background(), server, containment)
	launch := directLaunchRequest(rootName, containment)
	if err := writeHelperFrame(first, launch); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(first, []byte("durable-launch.example.test")); err != nil {
		t.Fatal(err)
	}
	// Drop the Agent connection without reading the PID response. The durable
	// launched transition is the authority for recovery, not delivery of its ACK.
	_ = first.Close()
	select {
	case <-firstServed:
	case <-time.After(time.Second):
		t.Fatal("disconnected launched session did not release its lock")
	}
	runtime.testRuntime.mu.Lock()
	killsAfterDisconnect := runtime.kills
	runtime.testRuntime.mu.Unlock()
	if killsAfterDisconnect != 0 {
		t.Fatal("Agent-only disconnect killed the launched runner")
	}
	pid := 1001
	pinnedContainment := containment
	alive := server.handleOneShot(context.Background(), helperRequest{
		Version:     helperProtocolVersion,
		RequestID:   strings.Repeat("c", 32),
		Operation:   helperOpAlive,
		Containment: &pinnedContainment,
		PID:         &pid,
	})
	if !alive.OK || alive.Flag == nil || !*alive.Flag {
		t.Fatalf("restarted Agent could not adopt PID %d: %#v", pid, alive)
	}

	restarted, restartedServed := directFenceSession(t, context.Background(), server, containment)
	secondLaunch := directLaunchRequest(rootName, containment)
	if response, err := roundTripHelper(
		context.Background(),
		restarted,
		secondLaunch,
		[]byte("durable-launch.example.test"),
	); err == nil || response.Code != helperCodeProtocol {
		t.Fatalf("replayed launch response=%#v err=%v", response, err)
	}
	runtime.testRuntime.mu.Lock()
	launches := runtime.launches
	runtime.testRuntime.mu.Unlock()
	if launches != 1 {
		t.Fatalf("restarted Agent created %d listeners", launches)
	}
	_ = restarted.Close()
	select {
	case <-restartedServed:
	case <-time.After(time.Second):
		t.Fatal("replayed session did not finish")
	}

	stop, stopServed := directFenceSession(t, context.Background(), server, containment)
	for _, operation := range []string{helperOpRevoke, helperOpKill, helperOpClose} {
		if response, err := roundTripHelper(
			context.Background(),
			stop,
			newHelperRequest(operation),
			nil,
		); err != nil || !response.OK {
			t.Fatalf("%s response=%#v err=%v", operation, response, err)
		}
	}
	_ = stop.Close()
	select {
	case <-stopServed:
	case <-time.After(time.Second):
		t.Fatal("restart cleanup session did not finish")
	}
	runtime.testRuntime.mu.Lock()
	defer runtime.testRuntime.mu.Unlock()
	if runtime.kills != 1 {
		t.Fatalf("restart cleanup kills=%d", runtime.kills)
	}
}

func TestSupervisorShutdownRevokesAndKillsDurableLaunch(t *testing.T) {
	server, runtime, _ := directFenceServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	rootName := strings.Repeat("b", 64)
	containment := helperTestContainment(rootName)
	connection, served := directFenceSession(t, ctx, server, containment)
	if _, err := roundTripHelper(
		context.Background(),
		connection,
		directLaunchRequest(rootName, containment),
		[]byte("durable-launch.example.test"),
	); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = connection.Close()
	select {
	case <-served:
	case <-time.After(time.Second):
		t.Fatal("Supervisor shutdown did not finish launched-session cleanup")
	}
	runtime.testRuntime.mu.Lock()
	defer runtime.testRuntime.mu.Unlock()
	if runtime.kills != 1 {
		t.Fatalf("Supervisor shutdown kills=%d", runtime.kills)
	}
}

func TestHelperFinalizeCleanupIsFailClosedAndGarbageCollectable(t *testing.T) {
	server, runtime, _ := directFenceServer(t)
	rootName := strings.Repeat("d", 64)
	containment := helperTestContainment(rootName)
	ref := runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"}
	request := newHelperRequest(helperOpFinalizeCleanup)
	request.Containment = &containment
	request.RootName = rootName
	request.WorkspaceRef = &ref
	response := server.handleOneShot(context.Background(), request)
	if !response.OK {
		t.Fatalf("FinalizeCleanup response=%#v", response)
	}
	runtime.mu.Lock()
	finalized := runtime.finalized[containmentSessionKey(containment)]
	runtime.mu.Unlock()
	if !finalized {
		t.Fatal("successful finalize did not publish durable authority")
	}
	gc := newHelperRequest(helperOpGarbageCollect)
	gc.Containment = &containment
	if response := server.handleOneShot(context.Background(), gc); !response.OK {
		t.Fatalf("GarbageCollect response=%#v", response)
	}

	runtime.mu.Lock()
	runtime.finalizeErr = errors.New("injected finalize failure")
	runtime.mu.Unlock()
	containment.FenceToken = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	request = newHelperRequest(helperOpFinalizeCleanup)
	request.Containment = &containment
	request.RootName = rootName
	request.WorkspaceRef = &ref
	response = server.handleOneShot(context.Background(), request)
	if response.OK || response.Code != helperCodeCleanup {
		t.Fatalf("failed FinalizeCleanup response=%#v", response)
	}
	runtime.mu.Lock()
	finalized = runtime.finalized[containmentSessionKey(containment)]
	runtime.mu.Unlock()
	if finalized {
		t.Fatal("failed finalize published success authority")
	}

	mismatched := newHelperRequest(helperOpFinalizeCleanup)
	mismatched.Containment = &containment
	mismatched.RootName = strings.Repeat("e", 64)
	mismatched.WorkspaceRef = &ref
	if response := server.handleOneShot(context.Background(), mismatched); response.OK ||
		response.Code != helperCodeProtocol {
		t.Fatalf("mismatched workspace/containment response=%#v", response)
	}
}

func TestHelperFinalizeCleanupResumesAfterWorkspaceRemovalBeforeTombstone(t *testing.T) {
	server, runtime, _ := directFenceServer(t)
	workspace := server.workspace.(helperTestWorkspace)
	workspace.observeErr = os.ErrNotExist
	absent := true
	workspace.absent = &absent
	server.workspace = workspace
	server.workspaceAbsence = workspace

	rootName := strings.Repeat("f", 64)
	containment := helperTestContainment(rootName)
	ref := runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"}
	request := newHelperRequest(helperOpFinalizeCleanup)
	request.Containment = &containment
	request.RootName = rootName
	request.WorkspaceRef = &ref
	if response := server.handleOneShot(context.Background(), request); !response.OK {
		t.Fatalf("resumed FinalizeCleanup response=%#v", response)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if !runtime.finalized[containmentSessionKey(containment)] {
		t.Fatal("resumed removal did not publish finalized authority")
	}
}

func TestHelperFinalizeCleanupRejectsForgedAbsenceWithoutSafeFence(t *testing.T) {
	server, runtime, _ := directFenceServer(t)
	workspace := server.workspace.(helperTestWorkspace)
	workspace.observeErr = os.ErrNotExist
	absent := true
	workspace.absent = &absent
	server.workspace = workspace
	server.workspaceAbsence = workspace
	runtime.mu.Lock()
	runtime.finalizationErr = errors.New("fence is active or cgroup populated")
	runtime.mu.Unlock()

	rootName := strings.Repeat("1", 64)
	containment := helperTestContainment(rootName)
	ref := runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"}
	request := newHelperRequest(helperOpFinalizeCleanup)
	request.Containment = &containment
	request.RootName = rootName
	request.WorkspaceRef = &ref
	response := server.handleOneShot(context.Background(), request)
	if response.OK || response.Code != helperCodeCleanup {
		t.Fatalf("unsafe absent FinalizeCleanup response=%#v", response)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.finalized[containmentSessionKey(containment)] {
		t.Fatal("unsafe absent workspace was finalized")
	}
}

func TestHelperFenceCloseBoundsStalledSupervisorIO(t *testing.T) {
	serverConnection, clientConnection := unixSocketPair(t)
	defer serverConnection.Close()
	client := &HelperClient{
		sessions:         make(map[string]*helperClientSession),
		operationTimeout: 20 * time.Millisecond,
	}
	containment := helperTestContainment(strings.Repeat("0", 64))
	session := &helperClientSession{
		client: client, connection: clientConnection, containment: containment,
	}
	client.sessions[containmentSessionKey(containment)] = session

	started := time.Now()
	if err := session.Close(); !errors.Is(err, runner.ErrCleanupFailed) {
		t.Fatalf("stalled close error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("stalled helper close took %s", elapsed)
	}
	if _, found := client.sessions[containmentSessionKey(containment)]; found {
		t.Fatal("failed close retained a reusable helper session")
	}
}

type helperBlockedCleanupRuntime struct {
	*helperRecordingRuntime
	entered  chan struct{}
	canceled chan struct{}
	once     sync.Once
}

func (runtime *helperBlockedCleanupRuntime) KillAndWait(ctx context.Context, _ runner.ContainmentRef) error {
	runtime.once.Do(func() { close(runtime.entered) })
	<-ctx.Done()
	close(runtime.canceled)
	return ctx.Err()
}

func TestHelperFenceDisconnectBoundsCleanupBeforeServiceOwnership(t *testing.T) {
	server, _, recording, _ := newHelperHarness(t)
	blocked := &helperBlockedCleanupRuntime{
		helperRecordingRuntime: recording,
		entered:                make(chan struct{}),
		canceled:               make(chan struct{}),
	}
	server.runtime = blocked
	server.cleanupTimeout = 20 * time.Millisecond
	serverConnection, clientConnection := unixSocketPair(t)
	served := make(chan struct{})
	go func() {
		server.serveConnection(context.Background(), serverConnection)
		close(served)
	}()
	rootName := strings.Repeat("1", 64)
	containment := helperTestContainment(rootName)
	lock := newHelperRequest(helperOpLock)
	lock.Containment = &containment
	if _, err := roundTripHelper(context.Background(), clientConnection, lock, nil); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_ = clientConnection.Close()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("disconnect cleanup did not reach cgroup ownership")
	}
	select {
	case <-blocked.canceled:
	case <-time.After(time.Second):
		t.Fatal("disconnect cleanup ignored its service shutdown deadline")
	}
	select {
	case <-served:
	case <-time.After(time.Second):
		t.Fatal("helper connection remained blocked after cleanup deadline")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("bounded helper cleanup took %s", elapsed)
	}
	recording.testRuntime.mu.Lock()
	defer recording.testRuntime.mu.Unlock()
	for _, fence := range recording.fences {
		fence.mu.Lock()
		revoked := fence.revoked
		fence.mu.Unlock()
		if !revoked {
			t.Fatal("helper cleanup deadline released an unrevoked fence")
		}
	}
}

func TestHelperLaunchDisconnectCancelsSpawnAndOwnsCleanup(t *testing.T) {
	server, client, runtime, policy := newHelperHarness(t)
	blocked := &helperBlockedRuntime{
		helperRecordingRuntime: runtime,
		entered:                make(chan struct{}),
	}
	server.runtime = blocked
	rootName := strings.Repeat("8", 64)
	containment := helperTestContainment(rootName)
	fence, err := client.LockFence(context.Background(), containment)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(policy.RuntimeRoot, "executions", rootName)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = client.Launch(ctx, LaunchSpec{
		Executable:   filepath.Join(directory, "run.sh"),
		Directory:    directory,
		Arguments:    []string{"--ephemeral"},
		UID:          policy.RunnerUID,
		GID:          policy.RunnerGID,
		WorkspaceRef: runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"},
		Containment:  containment,
	}, strings.NewReader("jit-disconnect.example.test"))
	if !errors.Is(err, runner.ErrStartFailed) {
		t.Fatalf("Launch error=%v", err)
	}
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("privileged launch was not reached")
	}
	if err := fence.Close(); err != nil {
		t.Fatal(err)
	}
	waitForHelperCleanup(t, runtime)
}

type helperBlockedRuntime struct {
	*helperRecordingRuntime
	entered chan struct{}
	once    sync.Once
}

func (runtime *helperBlockedRuntime) Launch(ctx context.Context, _ LaunchSpec, _ io.Reader) (int, error) {
	runtime.once.Do(func() { close(runtime.entered) })
	<-ctx.Done()
	return 0, ctx.Err()
}

func waitForHelperCleanup(t *testing.T, runtime *helperRecordingRuntime) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		runtime.testRuntime.mu.Lock()
		kills := runtime.kills
		runtime.testRuntime.mu.Unlock()
		if kills > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("ambiguous helper session did not revoke and kill its cgroup")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHelperRejectsUnexpectedPeerUIDBeforeReadingCommand(t *testing.T) {
	_, _, runtime, policy := newHelperHarness(t)
	workspace := helperTestWorkspace{
		agent:    RunnerIdentity{UID: policy.AgentUID, GID: policy.AgentGID},
		identity: RunnerIdentity{UID: policy.RunnerUID, GID: policy.RunnerGID},
		ref:      runner.WorkspaceRef{Backend: WorkspaceBackend, OwnerID: "workspace-test"},
	}
	policy.AgentUID++
	server := &HelperServer{
		policy: policy, runtime: runtime, workspace: workspace,
		connections: make(map[*net.UnixConn]struct{}),
	}
	serverConnection, clientConnection := unixSocketPair(t)
	go server.serveConnection(context.Background(), serverConnection)
	request := newHelperRequest(helperOpValidateAdmission)
	writeErr := writeHelperFrame(clientConnection, request)
	if writeErr == nil {
		_ = clientConnection.SetReadDeadline(time.Now().Add(time.Second))
		var response helperResponse
		if err := readHelperFrame(clientConnection, &response); err == nil {
			t.Fatal("unexpected peer received a privileged response")
		}
	}
	runtime.testRuntime.mu.Lock()
	defer runtime.testRuntime.mu.Unlock()
	if runtime.launches != 0 || runtime.kills != 0 {
		t.Fatal("unexpected peer reached privileged runtime")
	}
}

func TestHelperWorkspaceClientRejectsUnpinnedRoot(t *testing.T) {
	_, client, _, _ := newHelperHarness(t)
	wrong, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wrong.Close()
	if _, err := client.Prepare(context.Background(), wrong, strings.Repeat("f", 64)); !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("Prepare error=%v", err)
	}
}

func TestHelperSocketIsRootOwnedAndNotWorldAccessible(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-owned socket admission requires a root Supervisor test environment")
	}
	socketDirectory, err := os.MkdirTemp("/run", "sparerunner-helper-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(socketDirectory)
	const agentGID = 21001
	if err := os.Chown(socketDirectory, 0, agentGID); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	policy := HelperPolicy{
		SocketPath:  filepath.Join(socketDirectory, "supervisor.sock"),
		RuntimeRoot: "/var/lib/sparerunner-runtime",
		CacheRoot:   "/var/cache/sparerunner-agent",
		AgentUID:    21001,
		AgentGID:    agentGID,
		RunnerUID:   22001,
		RunnerGID:   22001,
	}
	listener, err := ListenHelperSocket(policy)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if !helperSocketSafe(policy) {
		t.Fatal("new helper socket failed its own admission policy")
	}
	if err := os.Chmod(policy.SocketPath, 0o666); err != nil {
		t.Fatal(err)
	}
	if helperSocketSafe(policy) {
		t.Fatal("world-accessible helper socket was accepted")
	}
}
