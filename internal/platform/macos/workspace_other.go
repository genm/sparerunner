//go:build !darwin

package macos

import (
	"context"
	"os"

	"github.com/genm/tewake/internal/runner"
)

type OSWorkspace struct{}

func NewOSWorkspace(int, int, int, int) *OSWorkspace { return &OSWorkspace{} }

func (*OSWorkspace) AgentIdentity() RunnerIdentity  { return RunnerIdentity{} }
func (*OSWorkspace) RunnerIdentity() RunnerIdentity { return RunnerIdentity{} }
func (*OSWorkspace) ValidateRuntimeRoot(context.Context, string) error {
	return runner.ErrStrongOwnershipUnavailable
}
func (*OSWorkspace) Prepare(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
}
func (*OSWorkspace) Observe(context.Context, *os.Root, string) (runner.WorkspaceRef, error) {
	return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
}
func (*OSWorkspace) Remove(context.Context, *os.Root, string) error {
	return runner.ErrCleanupFailed
}
func (*OSWorkspace) Absent(context.Context, *os.Root, string) (bool, error) {
	return false, runner.ErrCleanupFailed
}

type PinnedWorkspace struct{}

func (*OSWorkspace) PinLaunch(
	context.Context,
	string,
	runner.WorkspaceRef,
) (*PinnedWorkspace, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}
func (*PinnedWorkspace) Directory() *os.File  { return nil }
func (*PinnedWorkspace) Executable() *os.File { return nil }
func (*PinnedWorkspace) Close() error          { return nil }

