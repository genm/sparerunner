//go:build !linux

package linux

import (
	"context"
	"io"
	"net"
	"os"

	"github.com/genm/sparerunner/internal/runner"
)

type HelperPolicy struct {
	SocketPath  string
	RuntimeRoot string
	CacheRoot   string
	AgentUID    int
	AgentGID    int
	RunnerUID   int
	RunnerGID   int
}

type HelperServer struct{}
type HelperClient struct{}

func NewHelperServer(HelperPolicy, Runtime, Workspace) (*HelperServer, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}

func ListenHelperSocket(HelperPolicy) (*net.UnixListener, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}

func (*HelperServer) Serve(context.Context, *net.UnixListener) error {
	return runner.ErrStrongOwnershipUnavailable
}

func NewHelperClient(HelperPolicy) (*HelperClient, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}

func (*HelperClient) RunnerIdentity() RunnerIdentity { return RunnerIdentity{} }
func (*HelperClient) AgentIdentity() RunnerIdentity  { return RunnerIdentity{} }
func (*HelperClient) EnsureCgroup(context.Context, string) (Cgroup, error) {
	return Cgroup{}, runner.ErrStrongOwnershipUnavailable
}
func (*HelperClient) LockFence(context.Context, runner.ContainmentRef) (Fence, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}
func (*HelperClient) Launch(context.Context, LaunchSpec, io.Reader) (int, error) {
	return 0, runner.ErrStrongOwnershipUnavailable
}
func (*HelperClient) KillAndWait(context.Context, runner.ContainmentRef) error {
	return runner.ErrStrongOwnershipUnavailable
}
func (*HelperClient) WaitEmpty(context.Context, runner.ContainmentRef) error {
	return runner.ErrStrongOwnershipUnavailable
}
func (*HelperClient) Alive(context.Context, runner.ContainmentRef, int) (bool, error) {
	return false, runner.ErrStrongOwnershipUnavailable
}
func (*HelperClient) ValidateRuntimeRoot(context.Context, string) error {
	return runner.ErrStrongOwnershipUnavailable
}
func (*HelperClient) Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
}
func (*HelperClient) Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
}
func (*HelperClient) Remove(context.Context, *os.Root, string) error {
	return runner.ErrStrongOwnershipUnavailable
}

var _ Runtime = (*HelperClient)(nil)
var _ Workspace = (*HelperClient)(nil)
