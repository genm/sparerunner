//go:build !linux

package linux

import (
	"context"
	"io"
	"os"

	"github.com/genm/sparerunner/internal/runner"
)

// The shared-identity mode depends on cgroup-v2 delegation and Linux stat
// identity. The stubs keep cross-platform wiring compilable while refusing any
// claim of containment on macOS or Windows.

type SharedIdentityLauncher struct {
	HelperPath string
	Owner      RunnerIdentity
}

func NewSharedIdentityLauncher(string, RunnerIdentity) (SharedIdentityLauncher, error) {
	return SharedIdentityLauncher{}, runner.ErrStrongOwnershipUnavailable
}

func (SharedIdentityLauncher) Launch(context.Context, LaunchSpec, io.Reader, *os.File) (int, error) {
	return 0, runner.ErrStrongOwnershipUnavailable
}

type RootlessRuntime struct{}

func NewRootlessRuntime(string, string, PipeLauncher, *RootlessWorkspace) (*RootlessRuntime, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}

type RootlessWorkspace struct{}

func NewRootlessWorkspace(int, int) (*RootlessWorkspace, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}

func (*RootlessWorkspace) Identity() RunnerIdentity       { return RunnerIdentity{} }
func (*RootlessWorkspace) RunnerIdentity() RunnerIdentity { return RunnerIdentity{} }
func (*RootlessWorkspace) AgentIdentity() RunnerIdentity  { return RunnerIdentity{} }

func (*RootlessWorkspace) ValidateRuntimeRoot(context.Context, string) error {
	return runner.ErrStrongOwnershipUnavailable
}

func (*RootlessWorkspace) Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
}

func (*RootlessWorkspace) Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
}

func (*RootlessWorkspace) Remove(context.Context, *os.Root, string) error {
	return runner.ErrStrongOwnershipUnavailable
}

var _ Workspace = (*RootlessWorkspace)(nil)
var _ PipeLauncher = SharedIdentityLauncher{}
