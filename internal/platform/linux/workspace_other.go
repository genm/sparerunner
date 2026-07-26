//go:build !linux

package linux

import (
	"context"
	"os"

	"github.com/genm/tewake/internal/runner"
)

// OSWorkspace is intentionally unavailable outside Linux.  Keeping the stub
// explicit prevents a macOS or Windows binary from claiming statx semantics.
type OSWorkspace struct{}

func NewOSWorkspace(int, int, int, int) *OSWorkspace { return &OSWorkspace{} }
func NewVerifiedOSWorkspace(int, int, int, int, string, runner.Package) (*OSWorkspace, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}

func (*OSWorkspace) RunnerIdentity() RunnerIdentity { return RunnerIdentity{} }
func (*OSWorkspace) AgentIdentity() RunnerIdentity  { return RunnerIdentity{} }

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
	return runner.ErrStrongOwnershipUnavailable
}

var _ Workspace = (*OSWorkspace)(nil)
