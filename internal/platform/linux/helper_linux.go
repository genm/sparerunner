//go:build linux

package linux

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

const (
	helperProtocolVersion = 2
	maxHelperFrameBytes   = 64 << 10

	helperOpEnsureCgroup      = "ensure_cgroup"
	helperOpLock              = "lock"
	helperOpRevoked           = "revoked"
	helperOpLaunched          = "launched"
	helperOpLaunch            = "launch"
	helperOpRevoke            = "revoke"
	helperOpKill              = "kill"
	helperOpClose             = "close"
	helperOpAlive             = "alive"
	helperOpWaitEmpty         = "wait_empty"
	helperOpValidateAdmission = "validate_admission"
	helperOpFinalizeCleanup   = "finalize_cleanup"
	helperOpGarbageCollect    = "garbage_collect"
	helperOpWorkspacePrepare  = "workspace_prepare"
	helperOpWorkspaceObserve  = "workspace_observe"
	helperOpWorkspaceRemove   = "workspace_remove"

	helperCodeProtocol    = "protocol_error"
	helperCodeDenied      = "peer_denied"
	helperCodeFenced      = "start_fenced"
	helperCodeOwnership   = "ownership_unavailable"
	helperCodeStart       = "start_failed"
	helperCodeCleanup     = "cleanup_failed"
	helperCodeUnavailable = "unavailable"
)

// HelperPolicy is the fixed privilege boundary shared by the non-root Agent
// client and the root Supervisor server. None of these values can be supplied
// by a command received over the socket.
type HelperPolicy struct {
	SocketPath  string
	RuntimeRoot string
	CacheRoot   string
	AgentUID    int
	AgentGID    int
	RunnerUID   int
	RunnerGID   int
}

func (policy HelperPolicy) validate() error {
	if !filepath.IsAbs(policy.SocketPath) || !filepath.IsAbs(policy.RuntimeRoot) ||
		!filepath.IsAbs(policy.CacheRoot) ||
		filepath.Clean(policy.SocketPath) != policy.SocketPath ||
		filepath.Clean(policy.RuntimeRoot) != policy.RuntimeRoot ||
		filepath.Clean(policy.CacheRoot) != policy.CacheRoot ||
		policy.SocketPath == "/" || policy.RuntimeRoot == "/" || policy.CacheRoot == "/" ||
		policy.AgentUID <= 0 || policy.AgentGID <= 0 ||
		policy.RunnerUID <= 0 || policy.RunnerGID <= 0 ||
		policy.AgentUID == policy.RunnerUID || policy.AgentGID == policy.RunnerGID {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

type pinnedWorkspace struct {
	directory  *os.File
	executable *os.File
}

func (pinned *pinnedWorkspace) Close() {
	if pinned == nil {
		return
	}
	_ = pinned.executable.Close()
	_ = pinned.directory.Close()
}

// workspacePackageAuthority is implemented only by the root-side workspace.
// It prevents HelperServer from accidentally falling back to Agent-created
// package content or path-based launch.
type workspacePackageAuthority interface {
	OfficialAuthorityConfigured() bool
	ValidateOfficialAuthority(context.Context, string) error
	PrepareOfficial(context.Context, *os.Root, string) (runner.WorkspaceRef, error)
	PinLaunch(context.Context, *os.Root, string, runner.WorkspaceRef) (*pinnedWorkspace, error)
}

type runtimeFenceFinalizer interface {
	FenceFinalized(context.Context, runner.ContainmentRef) (bool, error)
	ValidateFinalization(context.Context, runner.ContainmentRef) error
	FinalizeFence(context.Context, runner.ContainmentRef) error
	GarbageCollectFence(context.Context, runner.ContainmentRef) error
}

type helperRequest struct {
	Version       int                    `json:"version"`
	RequestID     string                 `json:"requestId"`
	Operation     string                 `json:"operation"`
	Owner         string                 `json:"owner,omitempty"`
	Containment   *runner.ContainmentRef `json:"containment,omitempty"`
	RootName      string                 `json:"rootName,omitempty"`
	DisableUpdate *bool                  `json:"disableUpdate,omitempty"`
	WorkspaceRef  *runner.WorkspaceRef   `json:"workspaceRef,omitempty"`
	PID           *int                   `json:"pid,omitempty"`
	JITLength     *int                   `json:"jitLength,omitempty"`
}

type helperResponse struct {
	Version      int                  `json:"version"`
	RequestID    string               `json:"requestId"`
	OK           bool                 `json:"ok"`
	Code         string               `json:"code,omitempty"`
	Cgroup       *Cgroup              `json:"cgroup,omitempty"`
	PID          *int                 `json:"pid,omitempty"`
	Flag         *bool                `json:"flag,omitempty"`
	WorkspaceRef *runner.WorkspaceRef `json:"workspaceRef,omitempty"`
}

// HelperServer exposes only the fixed Linux runner operations. It contains no
// network client, GitHub credential, controller token, or general-purpose exec
// primitive.
type HelperServer struct {
	policy             HelperPolicy
	runtime            Runtime
	admission          SlotAdmission
	runtimeAdmission   RuntimeAdmission
	fenceFinalizer     runtimeFenceFinalizer
	runtimeShutdown    RuntimeShutdown
	workspace          Workspace
	workspaceAdmission WorkspaceSlotAdmission
	workspaceAbsence   WorkspaceAbsence
	packageAuthority   workspacePackageAuthority
	cleanupTimeout     time.Duration
	launchMu           sync.Mutex

	connectionMu sync.Mutex
	connections  map[*net.UnixConn]struct{}
	connectionWG sync.WaitGroup
}

func NewHelperServer(policy HelperPolicy, runtime Runtime, workspace Workspace) (*HelperServer, error) {
	admission, supportsSlotAdmission := runtime.(SlotAdmission)
	runtimeAdmission, supportsRuntimeAdmission := runtime.(RuntimeAdmission)
	fenceFinalizer, supportsFenceFinalizer := runtime.(runtimeFenceFinalizer)
	runtimeShutdown, supportsRuntimeShutdown := runtime.(RuntimeShutdown)
	workspaceAdmission, supportsWorkspaceAdmission := workspace.(WorkspaceSlotAdmission)
	workspaceAbsence, supportsWorkspaceAbsence := workspace.(WorkspaceAbsence)
	packageAuthority, supportsPackageAuthority := workspace.(workspacePackageAuthority)
	if policy.validate() != nil || runtime == nil || workspace == nil ||
		!supportsSlotAdmission || !supportsWorkspaceAdmission || !supportsPackageAuthority ||
		!supportsRuntimeAdmission || !supportsFenceFinalizer || !supportsRuntimeShutdown ||
		!supportsWorkspaceAbsence ||
		!packageAuthority.OfficialAuthorityConfigured() ||
		workspace.AgentIdentity() != (RunnerIdentity{UID: policy.AgentUID, GID: policy.AgentGID}) ||
		workspace.RunnerIdentity() != (RunnerIdentity{UID: policy.RunnerUID, GID: policy.RunnerGID}) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return &HelperServer{
		policy: policy, runtime: runtime, admission: admission,
		runtimeAdmission: runtimeAdmission,
		fenceFinalizer:   fenceFinalizer,
		runtimeShutdown:  runtimeShutdown,
		workspace:        workspace, workspaceAdmission: workspaceAdmission,
		workspaceAbsence: workspaceAbsence,
		packageAuthority: packageAuthority,
		cleanupTimeout:   DefaultAmbiguousCleanupTimeout,
		connections:      make(map[*net.UnixConn]struct{}),
	}, nil
}

// ListenHelperSocket creates the root-owned, Agent-group-writable local socket.
// It deliberately does not unlink a pre-existing path: a second Supervisor must
// fail instead of replacing the live privilege boundary.
func ListenHelperSocket(policy HelperPolicy) (*net.UnixListener, error) {
	if policy.validate() != nil || os.Geteuid() != 0 ||
		!safeHelperSocketDirectory(filepath.Dir(policy.SocketPath), policy.AgentGID) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: policy.SocketPath, Net: "unix"})
	if err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	ok := false
	defer func() {
		if !ok {
			_ = listener.Close()
		}
	}()
	if err := os.Chown(policy.SocketPath, 0, policy.AgentGID); err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if err := os.Chmod(policy.SocketPath, 0o660); err != nil || !helperSocketSafe(policy) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	ok = true
	return listener, nil
}

func (server *HelperServer) Serve(ctx context.Context, listener *net.UnixListener) (serveErr error) {
	if server == nil || listener == nil || os.Geteuid() != 0 ||
		!helperSocketSafe(server.policy) {
		return runner.ErrStrongOwnershipUnavailable
	}
	stop := context.AfterFunc(ctx, func() {
		_ = listener.Close()
		server.closeConnections()
	})
	defer func() {
		stop()
		server.closeConnections()
		server.connectionWG.Wait()
		cleanupCtx, cancelCleanup := context.WithTimeout(
			context.WithoutCancel(ctx),
			server.cleanupTimeout,
		)
		defer cancelCleanup()
		if err := server.runtimeShutdown.Shutdown(cleanupCtx); err != nil && serveErr == nil {
			serveErr = runner.ErrCleanupFailed
		}
	}()
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return runner.ErrStrongOwnershipUnavailable
		}
		server.connectionMu.Lock()
		if ctx.Err() != nil {
			server.connectionMu.Unlock()
			_ = connection.Close()
			return nil
		}
		server.connections[connection] = struct{}{}
		server.connectionWG.Add(1)
		server.connectionMu.Unlock()
		go func() {
			defer server.connectionWG.Done()
			defer func() {
				server.connectionMu.Lock()
				delete(server.connections, connection)
				server.connectionMu.Unlock()
			}()
			server.serveConnection(ctx, connection)
		}()
	}
}

func (server *HelperServer) closeConnections() {
	server.connectionMu.Lock()
	defer server.connectionMu.Unlock()
	for connection := range server.connections {
		_ = connection.Close()
	}
}

func (server *HelperServer) serveConnection(serviceCtx context.Context, connection *net.UnixConn) {
	defer connection.Close()
	credential, err := unixPeerCredential(connection)
	if err != nil || int(credential.Uid) != server.policy.AgentUID ||
		int(credential.Gid) != server.policy.AgentGID {
		return
	}
	request, err := readHelperRequest(connection)
	if err != nil {
		return
	}
	if request.Operation == helperOpLock {
		server.serveFenceSession(serviceCtx, connection, request)
		return
	}
	operationCtx, cancel := context.WithCancel(serviceCtx)
	defer cancel()
	go func() {
		var unexpected [1]byte
		_, _ = connection.Read(unexpected[:])
		cancel()
	}()
	response := server.handleOneShot(operationCtx, request)
	_ = writeHelperFrame(connection, response)
}

func (server *HelperServer) handleOneShot(ctx context.Context, request helperRequest) helperResponse {
	response := baseHelperResponse(request.RequestID)
	if err := validateHelperRequest(request); err != nil {
		return failedHelperResponse(response, helperCodeProtocol)
	}
	switch request.Operation {
	case helperOpEnsureCgroup:
		cgroup, err := server.runtime.EnsureCgroup(ctx, request.Owner)
		if err != nil {
			return failedHelperResponse(response, helperCodeOwnership)
		}
		response.Cgroup = &cgroup
	case helperOpAlive:
		alive, err := server.runtime.Alive(ctx, *request.Containment, *request.PID)
		if err != nil {
			return failedHelperResponse(response, helperCodeOwnership)
		}
		response.Flag = boolPointer(alive)
	case helperOpWaitEmpty:
		if err := server.runtime.WaitEmpty(ctx, *request.Containment); err != nil {
			return failedHelperResponse(response, helperCodeUnavailable)
		}
	case helperOpValidateAdmission:
		if err := server.workspace.ValidateRuntimeRoot(ctx, server.policy.RuntimeRoot); err != nil {
			return failedHelperResponse(response, helperCodeOwnership)
		}
		if err := server.runtimeAdmission.ValidateAdmission(ctx); err != nil {
			return failedHelperResponse(response, helperCodeOwnership)
		}
		if err := server.packageAuthority.ValidateOfficialAuthority(ctx, server.policy.RuntimeRoot); err != nil {
			return failedHelperResponse(response, helperCodeOwnership)
		}
	case helperOpFinalizeCleanup:
		executions, err := server.openExecutionsRoot(ctx)
		if err != nil {
			return failedHelperResponse(response, helperCodeCleanup)
		}
		defer executions.Close()
		finalized, err := server.fenceFinalizer.FenceFinalized(ctx, *request.Containment)
		if err != nil {
			return failedHelperResponse(response, helperCodeCleanup)
		}
		if finalized {
			absent, absenceErr := server.workspaceAbsence.Absent(ctx, executions, request.RootName)
			if absenceErr != nil || !absent {
				return failedHelperResponse(response, helperCodeCleanup)
			}
		} else {
			ref, observeErr := server.workspace.Observe(ctx, executions, request.RootName)
			switch {
			case observeErr == nil && ref == *request.WorkspaceRef:
				if removeErr := server.workspace.Remove(ctx, executions, request.RootName); removeErr != nil {
					return failedHelperResponse(response, helperCodeCleanup)
				}
			case observeErr == nil:
				return failedHelperResponse(response, helperCodeCleanup)
			}
			absent, absenceErr := server.workspaceAbsence.Absent(ctx, executions, request.RootName)
			if absenceErr != nil || !absent {
				return failedHelperResponse(response, helperCodeCleanup)
			}
			// Absence may be the exact crash boundary after Remove completed but
			// before the tombstone was published. Only a revoked, empty, exact
			// fence can turn that absence into a resumable finalization.
			if err := server.fenceFinalizer.ValidateFinalization(
				ctx,
				*request.Containment,
			); err != nil {
				return failedHelperResponse(response, helperCodeCleanup)
			}
		}
		if err := server.fenceFinalizer.FinalizeFence(ctx, *request.Containment); err != nil {
			return failedHelperResponse(response, helperCodeCleanup)
		}
	case helperOpGarbageCollect:
		if err := server.fenceFinalizer.GarbageCollectFence(ctx, *request.Containment); err != nil {
			return failedHelperResponse(response, helperCodeCleanup)
		}
	case helperOpWorkspacePrepare, helperOpWorkspaceObserve, helperOpWorkspaceRemove:
		executions, err := server.openExecutionsRoot(ctx)
		if err != nil {
			return failedHelperResponse(response, helperCodeOwnership)
		}
		defer executions.Close()
		switch request.Operation {
		case helperOpWorkspacePrepare:
			ref, prepareErr := server.packageAuthority.PrepareOfficial(ctx, executions, request.RootName)
			if prepareErr != nil {
				return failedHelperResponse(response, helperCodeOwnership)
			}
			response.WorkspaceRef = &ref
		case helperOpWorkspaceObserve:
			ref, observeErr := server.workspace.Observe(ctx, executions, request.RootName)
			if observeErr != nil {
				return failedHelperResponse(response, helperCodeOwnership)
			}
			response.WorkspaceRef = &ref
		case helperOpWorkspaceRemove:
			if removeErr := server.workspace.Remove(ctx, executions, request.RootName); removeErr != nil {
				return failedHelperResponse(response, helperCodeCleanup)
			}
		}
	default:
		return failedHelperResponse(response, helperCodeProtocol)
	}
	return response
}

func (server *HelperServer) serveFenceSession(ctx context.Context, connection *net.UnixConn, first helperRequest) {
	if validateHelperRequest(first) != nil {
		_ = writeHelperFrame(connection, failedHelperResponse(baseHelperResponse(first.RequestID), helperCodeProtocol))
		return
	}
	containment := *first.Containment
	fence, err := server.runtime.LockFence(ctx, containment)
	if err != nil {
		_ = writeHelperFrame(connection, failedHelperResponse(baseHelperResponse(first.RequestID), helperCodeFenced))
		return
	}
	defer fence.Close()

	normalClose := false
	launched := false
	revoked := false
	killed := false
	defer func() {
		if normalClose {
			return
		}
		if launched && !revoked && !killed && ctx.Err() == nil {
			// An Agent-only restart must not stop a committed runner. The
			// durable launched state rejects a second launch while allowing the
			// restarted Agent to adopt Alive or open the same token for Stop.
			return
		}
		// A lost local session is ambiguous. Persist Stop-before-Start and own
		// the complete descendant teardown before releasing the file fence.
		cleanupCtx, cancelCleanup := context.WithTimeout(
			context.WithoutCancel(ctx),
			server.cleanupTimeout,
		)
		defer cancelCleanup()
		_ = fence.Revoke(cleanupCtx)
		_ = server.runtime.KillAndWait(cleanupCtx, containment)
	}()
	var stateErr error
	revoked, stateErr = fence.Revoked()
	if stateErr != nil {
		_ = writeHelperFrame(connection, failedHelperResponse(baseHelperResponse(first.RequestID), helperCodeCleanup))
		return
	}
	launched, stateErr = fence.Launched()
	if stateErr != nil {
		_ = writeHelperFrame(connection, failedHelperResponse(baseHelperResponse(first.RequestID), helperCodeCleanup))
		return
	}

	initial := baseHelperResponse(first.RequestID)
	initial.Flag = boolPointer(revoked)
	if err := writeHelperFrame(connection, initial); err != nil {
		return
	}
	seen := map[string]struct{}{first.RequestID: {}}
	for {
		request, readErr := readHelperRequest(connection)
		if readErr != nil {
			return
		}
		response := baseHelperResponse(request.RequestID)
		if validateHelperRequest(request) != nil || request.Containment != nil ||
			!canonicalRequestID(request.RequestID) {
			_ = writeHelperFrame(connection, failedHelperResponse(response, helperCodeProtocol))
			return
		}
		if _, duplicate := seen[request.RequestID]; duplicate {
			_ = writeHelperFrame(connection, failedHelperResponse(response, helperCodeProtocol))
			return
		}
		seen[request.RequestID] = struct{}{}

		switch request.Operation {
		case helperOpRevoked:
			if !emptySessionRequest(request) {
				response = failedHelperResponse(response, helperCodeProtocol)
				break
			}
			flag, stateErr := fence.Revoked()
			if stateErr != nil {
				response = failedHelperResponse(response, helperCodeCleanup)
				break
			}
			revoked = flag
			response.Flag = boolPointer(flag)

		case helperOpLaunched:
			if !emptySessionRequest(request) {
				response = failedHelperResponse(response, helperCodeProtocol)
				break
			}
			flag, stateErr := fence.Launched()
			if stateErr != nil {
				response = failedHelperResponse(response, helperCodeCleanup)
				break
			}
			launched = flag
			response.Flag = boolPointer(flag)

		case helperOpLaunch:
			if launched || revoked || killed || !validLaunchRequest(request, containment) {
				response = failedHelperResponse(response, helperCodeProtocol)
				break
			}
			server.launchMu.Lock()
			slotBusy, slotErr := server.admission.SlotBusy(ctx, containment)
			if slotErr != nil || slotBusy {
				server.launchMu.Unlock()
				response = failedHelperResponse(response, helperCodeOwnership)
				break
			}
			executions, openErr := server.openExecutionsRoot(ctx)
			if openErr != nil {
				server.launchMu.Unlock()
				response = failedHelperResponse(response, helperCodeOwnership)
				break
			}
			workspaceBusy, workspaceBusyErr := server.workspaceAdmission.SlotBusy(ctx, executions, request.RootName)
			if workspaceBusyErr != nil || workspaceBusy {
				_ = executions.Close()
				server.launchMu.Unlock()
				response = failedHelperResponse(response, helperCodeOwnership)
				break
			}
			pinned, pinErr := server.packageAuthority.PinLaunch(
				ctx, executions, request.RootName, *request.WorkspaceRef,
			)
			closeErr := executions.Close()
			if pinErr != nil || closeErr != nil {
				if pinned != nil {
					pinned.Close()
				}
				server.launchMu.Unlock()
				response = failedHelperResponse(response, helperCodeOwnership)
				break
			}
			material := make([]byte, *request.JITLength)
			if _, readErr := io.ReadFull(connection, material); readErr != nil {
				pinned.Close()
				server.launchMu.Unlock()
				clear(material)
				return
			}
			arguments := []string{"--ephemeral"}
			if *request.DisableUpdate {
				arguments = append(arguments, "--disableupdate")
			}
			directory := filepath.Join(server.policy.RuntimeRoot, "executions", request.RootName)
			launchCtx, stopDisconnectMonitor := contextUntilDisconnect(ctx, connection)
			pid, launchErr := server.runtime.Launch(launchCtx, LaunchSpec{
				Executable:       filepath.Join(directory, "run.sh"),
				Directory:        directory,
				Arguments:        arguments,
				UID:              server.policy.RunnerUID,
				GID:              server.policy.RunnerGID,
				WorkspaceRef:     *request.WorkspaceRef,
				Containment:      containment,
				DirectoryHandle:  pinned.directory,
				ExecutableHandle: pinned.executable,
			}, bytes.NewReader(material))
			stopDisconnectMonitor()
			pinned.Close()
			server.launchMu.Unlock()
			clear(material)
			if launchErr != nil || pid <= 0 {
				response = failedHelperResponse(response, helperCodeStart)
				break
			}
			if err := fence.MarkLaunched(ctx); err != nil {
				cleanupCtx, cancelCleanup := context.WithTimeout(
					context.WithoutCancel(ctx),
					server.cleanupTimeout,
				)
				_ = fence.Revoke(cleanupCtx)
				_ = server.runtime.KillAndWait(cleanupCtx, containment)
				cancelCleanup()
				response = failedHelperResponse(response, helperCodeCleanup)
				break
			}
			launched = true
			response.PID = intPointer(pid)

		case helperOpRevoke:
			if !emptySessionRequest(request) || killed {
				response = failedHelperResponse(response, helperCodeProtocol)
				break
			}
			if err := fence.Revoke(ctx); err != nil {
				response = failedHelperResponse(response, helperCodeCleanup)
				break
			}
			revoked = true

		case helperOpKill:
			if !emptySessionRequest(request) || !revoked {
				response = failedHelperResponse(response, helperCodeProtocol)
				break
			}
			if err := server.runtime.KillAndWait(ctx, containment); err != nil {
				response = failedHelperResponse(response, helperCodeCleanup)
				break
			}
			killed = true

		case helperOpClose:
			if !emptySessionRequest(request) {
				response = failedHelperResponse(response, helperCodeProtocol)
			}
			if writeHelperFrame(connection, response) == nil && response.OK {
				normalClose = launched || revoked || killed
			}
			return

		default:
			response = failedHelperResponse(response, helperCodeProtocol)
		}
		if err := writeHelperFrame(connection, response); err != nil || !response.OK {
			return
		}
	}
}

func (server *HelperServer) openExecutionsRoot(ctx context.Context) (*os.Root, error) {
	if err := server.workspace.ValidateRuntimeRoot(ctx, server.policy.RuntimeRoot); err != nil {
		return nil, err
	}
	return os.OpenRoot(filepath.Join(server.policy.RuntimeRoot, "executions"))
}

// HelperClient implements Runtime and Workspace for the unprivileged Agent.
// Every privileged path, argument, credential, and cgroup location remains a
// server-side policy value.
type HelperClient struct {
	policy HelperPolicy
	dial   func(context.Context) (*net.UnixConn, error)

	mu               sync.Mutex
	sessions         map[string]*helperClientSession
	operationTimeout time.Duration
}

func NewHelperClient(policy HelperPolicy) (*HelperClient, error) {
	if policy.validate() != nil || os.Geteuid() != policy.AgentUID || os.Getegid() != policy.AgentGID {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	client := &HelperClient{
		policy:           policy,
		sessions:         make(map[string]*helperClientSession),
		operationTimeout: DefaultAmbiguousCleanupTimeout,
	}
	client.dial = client.dialProduction
	return client, nil
}

func (client *HelperClient) RunnerIdentity() RunnerIdentity {
	return RunnerIdentity{UID: client.policy.RunnerUID, GID: client.policy.RunnerGID}
}

func (client *HelperClient) AgentIdentity() RunnerIdentity {
	return RunnerIdentity{UID: client.policy.AgentUID, GID: client.policy.AgentGID}
}

func (client *HelperClient) EnsureCgroup(ctx context.Context, owner string) (Cgroup, error) {
	response, err := client.oneShot(ctx, helperRequest{Operation: helperOpEnsureCgroup, Owner: owner})
	if err != nil || response.Cgroup == nil {
		return Cgroup{}, runner.ErrStrongOwnershipUnavailable
	}
	return *response.Cgroup, nil
}

func (client *HelperClient) LockFence(ctx context.Context, containment runner.ContainmentRef) (Fence, error) {
	connection, err := client.dial(ctx)
	if err != nil {
		return nil, runner.ErrCleanupFailed
	}
	request := newHelperRequest(helperOpLock)
	request.Containment = &containment
	response, err := roundTripHelper(ctx, connection, request, nil)
	if err != nil || !response.OK || response.Flag == nil {
		_ = connection.Close()
		return nil, runner.ErrCleanupFailed
	}
	session := &helperClientSession{client: client, connection: connection, containment: containment}
	key := containmentSessionKey(containment)
	client.mu.Lock()
	if _, exists := client.sessions[key]; exists {
		client.mu.Unlock()
		_ = connection.Close()
		return nil, runner.ErrCleanupFailed
	}
	client.sessions[key] = session
	client.mu.Unlock()
	return session, nil
}

func (client *HelperClient) Launch(ctx context.Context, spec LaunchSpec, material io.Reader) (int, error) {
	rootName, disableUpdate, valid := client.fixedLaunch(spec)
	if !valid {
		return 0, runner.ErrStrongOwnershipUnavailable
	}
	session := client.session(spec.Containment)
	if session == nil {
		return 0, runner.ErrStartFenced
	}
	jit, err := io.ReadAll(io.LimitReader(material, maxJITMaterialBytes+1))
	if err != nil || len(jit) == 0 || len(jit) > maxJITMaterialBytes {
		clear(jit)
		return 0, runner.ErrStartFailed
	}
	defer clear(jit)
	request := newHelperRequest(helperOpLaunch)
	request.RootName = rootName
	request.DisableUpdate = boolPointer(disableUpdate)
	request.WorkspaceRef = &spec.WorkspaceRef
	request.JITLength = intPointer(len(jit))
	response, err := session.roundTrip(ctx, request, jit)
	if err != nil || !response.OK || response.PID == nil || *response.PID <= 0 {
		return 0, runner.ErrStartFailed
	}
	return *response.PID, nil
}

func (client *HelperClient) KillAndWait(ctx context.Context, containment runner.ContainmentRef) error {
	session := client.session(containment)
	if session == nil {
		return runner.ErrCleanupFailed
	}
	response, err := session.roundTrip(ctx, newHelperRequest(helperOpKill), nil)
	if err != nil || !response.OK {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (client *HelperClient) WaitEmpty(ctx context.Context, containment runner.ContainmentRef) error {
	pinned := containment
	response, err := client.oneShot(ctx, helperRequest{Operation: helperOpWaitEmpty, Containment: &pinned})
	if err != nil || !response.OK {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (client *HelperClient) Alive(ctx context.Context, containment runner.ContainmentRef, pid int) (bool, error) {
	pinned, pinnedPID := containment, pid
	response, err := client.oneShot(ctx, helperRequest{
		Operation: helperOpAlive, Containment: &pinned, PID: &pinnedPID,
	})
	if err != nil || response.Flag == nil {
		return false, runner.ErrStrongOwnershipUnavailable
	}
	return *response.Flag, nil
}

func (client *HelperClient) ValidateRuntimeRoot(ctx context.Context, root string) error {
	if filepath.Clean(root) != filepath.Clean(client.policy.RuntimeRoot) {
		return runner.ErrStrongOwnershipUnavailable
	}
	response, err := client.oneShot(ctx, helperRequest{Operation: helperOpValidateAdmission})
	if err != nil || !response.OK {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (client *HelperClient) FinalizeCleanup(
	ctx context.Context,
	containment runner.ContainmentRef,
	root *os.Root,
	name string,
	expected runner.WorkspaceRef,
) error {
	if !client.validWorkspaceCall(root, name) ||
		expected.Backend != WorkspaceBackend || expected.OwnerID == "" {
		return runner.ErrCleanupFailed
	}
	pinnedContainment, pinnedRef := containment, expected
	response, err := client.oneShot(ctx, helperRequest{
		Operation:    helperOpFinalizeCleanup,
		Containment:  &pinnedContainment,
		RootName:     name,
		WorkspaceRef: &pinnedRef,
	})
	if err != nil || !response.OK {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (client *HelperClient) GarbageCollectFence(ctx context.Context, containment runner.ContainmentRef) error {
	pinned := containment
	response, err := client.oneShot(ctx, helperRequest{
		Operation: helperOpGarbageCollect, Containment: &pinned,
	})
	if err != nil || !response.OK {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (client *HelperClient) Prepare(ctx context.Context, root *os.Root, name string) (runner.WorkspaceRef, error) {
	if !client.validWorkspaceCall(root, name) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	response, err := client.oneShot(ctx, helperRequest{Operation: helperOpWorkspacePrepare, RootName: name})
	if err != nil || response.WorkspaceRef == nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return *response.WorkspaceRef, nil
}

func (client *HelperClient) Observe(ctx context.Context, root *os.Root, name string) (runner.WorkspaceRef, error) {
	if !client.validWorkspaceCall(root, name) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	response, err := client.oneShot(ctx, helperRequest{Operation: helperOpWorkspaceObserve, RootName: name})
	if err != nil || response.WorkspaceRef == nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return *response.WorkspaceRef, nil
}

func (client *HelperClient) Remove(ctx context.Context, root *os.Root, name string) error {
	if !client.validWorkspaceCall(root, name) {
		return runner.ErrCleanupFailed
	}
	response, err := client.oneShot(ctx, helperRequest{Operation: helperOpWorkspaceRemove, RootName: name})
	if err != nil || !response.OK {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (client *HelperClient) fixedLaunch(spec LaunchSpec) (string, bool, bool) {
	directory := filepath.Clean(spec.Directory)
	executions := filepath.Join(filepath.Clean(client.policy.RuntimeRoot), "executions")
	if filepath.Dir(directory) != executions ||
		filepath.Clean(spec.Executable) != filepath.Join(directory, "run.sh") ||
		spec.UID != client.policy.RunnerUID || spec.GID != client.policy.RunnerGID ||
		spec.WorkspaceRef.Backend != WorkspaceBackend || spec.WorkspaceRef.OwnerID == "" ||
		!validWorkspaceName(filepath.Base(directory)) ||
		spec.Containment.OwnerID != "tewake-"+filepath.Base(directory) ||
		!fixedRunnerArguments(spec.Arguments) {
		return "", false, false
	}
	return filepath.Base(directory), len(spec.Arguments) == 2, true
}

func (client *HelperClient) validWorkspaceCall(root *os.Root, name string) bool {
	return root != nil && validWorkspaceName(name) &&
		filepath.Clean(root.Name()) == filepath.Join(filepath.Clean(client.policy.RuntimeRoot), "executions")
}

func (client *HelperClient) oneShot(ctx context.Context, partial helperRequest) (helperResponse, error) {
	connection, err := client.dial(ctx)
	if err != nil {
		return helperResponse{}, err
	}
	defer connection.Close()
	request := newHelperRequest(partial.Operation)
	request.Owner = partial.Owner
	request.Containment = partial.Containment
	request.RootName = partial.RootName
	request.DisableUpdate = partial.DisableUpdate
	request.WorkspaceRef = partial.WorkspaceRef
	request.PID = partial.PID
	request.JITLength = partial.JITLength
	return roundTripHelper(ctx, connection, request, nil)
}

func (client *HelperClient) session(containment runner.ContainmentRef) *helperClientSession {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.sessions[containmentSessionKey(containment)]
}

func (client *HelperClient) removeSession(containment runner.ContainmentRef, expected *helperClientSession) {
	client.mu.Lock()
	defer client.mu.Unlock()
	key := containmentSessionKey(containment)
	if client.sessions[key] == expected {
		delete(client.sessions, key)
	}
}

func (client *HelperClient) dialProduction(ctx context.Context) (*net.UnixConn, error) {
	if !helperSocketSafe(client.policy) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "unix", client.policy.SocketPath)
	if err != nil {
		return nil, err
	}
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		_ = connection.Close()
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	credential, err := unixPeerCredential(unixConnection)
	if err != nil || credential.Uid != 0 {
		_ = unixConnection.Close()
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return unixConnection, nil
}

type helperClientSession struct {
	client      *HelperClient
	connection  *net.UnixConn
	containment runner.ContainmentRef
	mu          sync.Mutex
	closed      bool
}

func (session *helperClientSession) Revoked() (bool, error) {
	ctx, cancel := session.backgroundOperationContext()
	defer cancel()
	response, err := session.roundTrip(ctx, newHelperRequest(helperOpRevoked), nil)
	if err != nil || response.Flag == nil {
		return true, runner.ErrCleanupFailed
	}
	return *response.Flag, nil
}

func (session *helperClientSession) Launched() (bool, error) {
	ctx, cancel := session.backgroundOperationContext()
	defer cancel()
	response, err := session.roundTrip(ctx, newHelperRequest(helperOpLaunched), nil)
	if err != nil || response.Flag == nil {
		return false, runner.ErrCleanupFailed
	}
	return *response.Flag, nil
}

func (session *helperClientSession) MarkLaunched(context.Context) error {
	launched, err := session.Launched()
	if err != nil || !launched {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (session *helperClientSession) Revoke(ctx context.Context) error {
	response, err := session.roundTrip(ctx, newHelperRequest(helperOpRevoke), nil)
	if err != nil || !response.OK {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (session *helperClientSession) Close() error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return nil
	}
	request := newHelperRequest(helperOpClose)
	ctx, cancel := session.backgroundOperationContext()
	defer cancel()
	response, err := roundTripHelper(ctx, session.connection, request, nil)
	session.closed = true
	session.client.removeSession(session.containment, session)
	closeErr := session.connection.Close()
	if err != nil || !response.OK {
		return runner.ErrCleanupFailed
	}
	return closeErr
}

func (session *helperClientSession) backgroundOperationContext() (context.Context, context.CancelFunc) {
	timeout := DefaultAmbiguousCleanupTimeout
	if session != nil && session.client != nil && session.client.operationTimeout > 0 {
		timeout = session.client.operationTimeout
	}
	// Fence.Revoked and Fence.Close do not accept a caller context. Bound their
	// local protocol I/O so a stalled privileged helper cannot pin Agent
	// shutdown or command admission indefinitely.
	return context.WithTimeout(context.Background(), timeout)
}

func (session *helperClientSession) roundTrip(ctx context.Context, request helperRequest, material []byte) (helperResponse, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return helperResponse{}, runner.ErrCleanupFailed
	}
	response, err := roundTripHelper(ctx, session.connection, request, material)
	if err != nil || !response.OK {
		session.closed = true
		session.client.removeSession(session.containment, session)
		_ = session.connection.Close()
	}
	return response, err
}

func roundTripHelper(ctx context.Context, connection *net.UnixConn, request helperRequest, material []byte) (helperResponse, error) {
	stop := context.AfterFunc(ctx, func() {
		_ = connection.SetDeadline(time.Now())
	})
	defer func() {
		stop()
		_ = connection.SetDeadline(time.Time{})
	}()
	if deadline, ok := ctx.Deadline(); ok {
		_ = connection.SetDeadline(deadline)
	}
	if err := writeHelperFrame(connection, request); err != nil {
		return helperResponse{}, err
	}
	if len(material) > 0 {
		if err := writeAll(connection, material); err != nil {
			return helperResponse{}, err
		}
	}
	var response helperResponse
	if err := readHelperFrame(connection, &response); err != nil {
		return helperResponse{}, err
	}
	if response.Version != helperProtocolVersion || response.RequestID != request.RequestID ||
		!canonicalRequestID(response.RequestID) || !validHelperResponse(request.Operation, response) {
		return helperResponse{}, runner.ErrStrongOwnershipUnavailable
	}
	if !response.OK {
		return response, helperResponseError(response.Code)
	}
	return response, nil
}

func validHelperResponse(operation string, response helperResponse) bool {
	if !response.OK {
		return fixedHelperErrorCode(response.Code) &&
			response.Cgroup == nil && response.PID == nil &&
			response.Flag == nil && response.WorkspaceRef == nil
	}
	if response.Code != "" {
		return false
	}
	switch operation {
	case helperOpEnsureCgroup:
		return response.Cgroup != nil && response.PID == nil &&
			response.Flag == nil && response.WorkspaceRef == nil
	case helperOpLock, helperOpRevoked, helperOpLaunched, helperOpAlive:
		return response.Cgroup == nil && response.PID == nil &&
			response.Flag != nil && response.WorkspaceRef == nil
	case helperOpLaunch:
		return response.Cgroup == nil && response.PID != nil &&
			response.Flag == nil && response.WorkspaceRef == nil
	case helperOpWorkspacePrepare, helperOpWorkspaceObserve:
		return response.Cgroup == nil && response.PID == nil &&
			response.Flag == nil && response.WorkspaceRef != nil
	case helperOpRevoke, helperOpKill, helperOpClose, helperOpWaitEmpty,
		helperOpValidateAdmission, helperOpFinalizeCleanup,
		helperOpGarbageCollect, helperOpWorkspaceRemove:
		return response.Cgroup == nil && response.PID == nil &&
			response.Flag == nil && response.WorkspaceRef == nil
	default:
		return false
	}
}

func fixedHelperErrorCode(code string) bool {
	switch code {
	case helperCodeProtocol, helperCodeDenied, helperCodeFenced,
		helperCodeOwnership, helperCodeStart, helperCodeCleanup,
		helperCodeUnavailable:
		return true
	default:
		return false
	}
}

func readHelperRequest(reader io.Reader) (helperRequest, error) {
	var request helperRequest
	if err := readHelperFrame(reader, &request); err != nil {
		return helperRequest{}, err
	}
	return request, nil
}

func writeHelperFrame(writer io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil || len(data) == 0 || len(data) > maxHelperFrameBytes {
		return runner.ErrStrongOwnershipUnavailable
	}
	defer clear(data)
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if err := writeAll(writer, header[:]); err != nil {
		return err
	}
	return writeAll(writer, data)
}

func readHelperFrame(reader io.Reader, destination any) error {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	length := int(binary.BigEndian.Uint32(header[:]))
	if length <= 0 || length > maxHelperFrameBytes {
		return runner.ErrStrongOwnershipUnavailable
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		clear(data)
		return err
	}
	defer clear(data)
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := consumeJSONValue(decoder, 0); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 16 {
		return runner.ErrStrongOwnershipUnavailable
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			key, ok := keyToken.(string)
			if keyErr != nil || !ok {
				return runner.ErrStrongOwnershipUnavailable
			}
			if _, duplicate := seen[key]; duplicate {
				return runner.ErrStrongOwnershipUnavailable
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closing, closeErr := decoder.Token()
		if closeErr != nil || closing != json.Delim('}') {
			return runner.ErrStrongOwnershipUnavailable
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closing, closeErr := decoder.Token()
		if closeErr != nil || closing != json.Delim(']') {
			return runner.ErrStrongOwnershipUnavailable
		}
	default:
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func validateHelperRequest(request helperRequest) error {
	if request.Version != helperProtocolVersion ||
		!canonicalRequestID(request.RequestID) ||
		request.Operation == "" {
		return runner.ErrStrongOwnershipUnavailable
	}
	switch request.Operation {
	case helperOpEnsureCgroup:
		if !validOwner(request.Owner) || request.hasPayloadExcept("owner") {
			return runner.ErrStrongOwnershipUnavailable
		}
	case helperOpLock:
		if request.Containment == nil || !validWireContainment(*request.Containment) ||
			request.hasPayloadExcept("containment") {
			return runner.ErrStrongOwnershipUnavailable
		}
	case helperOpAlive:
		if request.Containment == nil || !validWireContainment(*request.Containment) ||
			request.PID == nil || *request.PID <= 0 ||
			request.hasPayloadExcept("containment", "pid") {
			return runner.ErrStrongOwnershipUnavailable
		}
	case helperOpWaitEmpty:
		if request.Containment == nil || !validWireContainment(*request.Containment) ||
			request.hasPayloadExcept("containment") {
			return runner.ErrStrongOwnershipUnavailable
		}
	case helperOpValidateAdmission, helperOpRevoked, helperOpLaunched,
		helperOpRevoke, helperOpKill, helperOpClose:
		if request.hasPayloadExcept() {
			return runner.ErrStrongOwnershipUnavailable
		}
	case helperOpFinalizeCleanup:
		if request.Containment == nil || !validWireContainment(*request.Containment) ||
			!canonicalRootName(request.RootName) ||
			request.Containment.OwnerID != "tewake-"+request.RootName ||
			request.WorkspaceRef == nil ||
			request.WorkspaceRef.Backend != WorkspaceBackend ||
			request.WorkspaceRef.OwnerID == "" ||
			request.hasPayloadExcept("containment", "rootName", "workspaceRef") {
			return runner.ErrStrongOwnershipUnavailable
		}
	case helperOpGarbageCollect:
		if request.Containment == nil || !validWireContainment(*request.Containment) ||
			request.hasPayloadExcept("containment") {
			return runner.ErrStrongOwnershipUnavailable
		}
	case helperOpWorkspacePrepare, helperOpWorkspaceObserve, helperOpWorkspaceRemove:
		if !canonicalRootName(request.RootName) || request.hasPayloadExcept("rootName") {
			return runner.ErrStrongOwnershipUnavailable
		}
	case helperOpLaunch:
		// Session-specific containment checks happen while holding the fence.
		if request.RootName == "" || request.DisableUpdate == nil ||
			request.WorkspaceRef == nil || request.JITLength == nil ||
			request.hasPayloadExcept("rootName", "disableUpdate", "workspaceRef", "jitLength") {
			return runner.ErrStrongOwnershipUnavailable
		}
	default:
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (request helperRequest) hasPayloadExcept(allowed ...string) bool {
	set := make(map[string]bool, len(allowed))
	for _, field := range allowed {
		set[field] = true
	}
	return (!set["owner"] && request.Owner != "") ||
		(!set["containment"] && request.Containment != nil) ||
		(!set["rootName"] && request.RootName != "") ||
		(!set["disableUpdate"] && request.DisableUpdate != nil) ||
		(!set["workspaceRef"] && request.WorkspaceRef != nil) ||
		(!set["pid"] && request.PID != nil) ||
		(!set["jitLength"] && request.JITLength != nil)
}

func validLaunchRequest(request helperRequest, containment runner.ContainmentRef) bool {
	return validateHelperRequest(request) == nil &&
		validWireContainment(containment) &&
		canonicalRootName(request.RootName) &&
		containment.OwnerID == "tewake-"+request.RootName &&
		request.WorkspaceRef.Backend == WorkspaceBackend &&
		request.WorkspaceRef.OwnerID != "" &&
		*request.JITLength > 0 &&
		*request.JITLength <= maxJITMaterialBytes
}

func validWireContainment(containment runner.ContainmentRef) bool {
	return containment.Backend == containmentBackend &&
		validOwner(containment.OwnerID) &&
		containment.Scope == filepath.Join("tewake", containment.OwnerID) &&
		containment.HostEpoch != "" &&
		containment.InvocationID == "" &&
		canonicalToken(containment.FenceToken)
}

func emptySessionRequest(request helperRequest) bool {
	return !request.hasPayloadExcept()
}

func newHelperRequest(operation string) helperRequest {
	sequence := helperRequestSequence.Add(1)
	return helperRequest{
		Version:   helperProtocolVersion,
		RequestID: fmt.Sprintf("%032x", sequence),
		Operation: operation,
	}
}

func baseHelperResponse(requestID string) helperResponse {
	return helperResponse{Version: helperProtocolVersion, RequestID: requestID, OK: true}
}

func failedHelperResponse(response helperResponse, code string) helperResponse {
	response.OK = false
	response.Code = code
	response.Cgroup = nil
	response.PID = nil
	response.Flag = nil
	response.WorkspaceRef = nil
	return response
}

func helperResponseError(code string) error {
	switch code {
	case helperCodeFenced:
		return runner.ErrStartFenced
	case helperCodeStart:
		return runner.ErrStartFailed
	case helperCodeCleanup:
		return runner.ErrCleanupFailed
	case helperCodeOwnership, helperCodeDenied, helperCodeProtocol, helperCodeUnavailable:
		return runner.ErrStrongOwnershipUnavailable
	default:
		return runner.ErrStrongOwnershipUnavailable
	}
}

func containmentSessionKey(containment runner.ContainmentRef) string {
	return containment.Backend + "\x00" + containment.OwnerID + "\x00" +
		containment.Scope + "\x00" + containment.HostEpoch + "\x00" +
		containment.InvocationID + "\x00" + containment.FenceToken
}

func canonicalRequestID(value string) bool {
	return len(value) == 32 && canonicalLowerHex(value)
}

func canonicalRootName(value string) bool {
	return validWorkspaceName(value)
}

func canonicalLowerHex(value string) bool {
	for _, character := range value {
		if !(character >= '0' && character <= '9') &&
			!(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func helperSocketSafe(policy HelperPolicy) bool {
	info, err := os.Lstat(policy.SocketPath)
	if err != nil || info.Mode()&os.ModeSocket == 0 ||
		info.Mode().Perm() != 0o660 ||
		!ownedBy(info, 0, policy.AgentGID) ||
		!safeHelperSocketDirectory(filepath.Dir(policy.SocketPath), policy.AgentGID) {
		return false
	}
	return true
}

func safeHelperSocketDirectory(value string, agentGID int) bool {
	cleaned := filepath.Clean(value)
	for current := cleaned; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			info.Mode().Perm()&0o022 != 0 {
			return false
		}
		if current == cleaned {
			if !ownedBy(info, 0, agentGID) || info.Mode().Perm() != 0o750 {
				return false
			}
		} else if !rootOwned(info) {
			return false
		}
		if current == "/" {
			return true
		}
	}
}

func unixPeerCredential(connection *net.UnixConn) (*syscall.Ucred, error) {
	if connection == nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, err
	}
	var credential *syscall.Ucred
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		credential, socketErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return nil, err
	}
	if socketErr != nil || credential == nil || credential.Pid <= 0 {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return credential, nil
}

func contextUntilDisconnect(parent context.Context, connection *net.UnixConn) (context.Context, func()) {
	operationCtx, cancel := context.WithCancel(parent)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		var unexpected [1]byte
		_, _ = connection.Read(unexpected[:])
		cancel()
	}()
	return operationCtx, func() {
		_ = connection.SetReadDeadline(time.Now())
		<-finished
		_ = connection.SetReadDeadline(time.Time{})
		cancel()
	}
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func boolPointer(value bool) *bool { return &value }
func intPointer(value int) *int    { return &value }

var _ Runtime = (*HelperClient)(nil)
var _ Workspace = (*HelperClient)(nil)
var _ Fence = (*helperClientSession)(nil)

var helperRequestSequence atomic.Uint64
