//go:build !darwin

package macos

import (
	"context"
	"io"

	"github.com/genm/tewake/internal/runner"
)

type ExecLauncher struct{}

func NewExecLauncher(string) (ExecLauncher, error) {
	return ExecLauncher{}, runner.ErrStrongOwnershipUnavailable
}

type FileRuntime struct{}

func NewFileRuntime(
	string,
	ExecLauncher,
	RunnerIdentity,
	*OSWorkspace,
) (*FileRuntime, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}
func (*FileRuntime) ValidateAdmission(context.Context) error {
	return runner.ErrStrongOwnershipUnavailable
}
func (*FileRuntime) EnsureProcessGroup(context.Context, string) (ProcessGroup, error) {
	return ProcessGroup{}, runner.ErrStrongOwnershipUnavailable
}
func (*FileRuntime) LockFence(context.Context, runner.ContainmentRef) (Fence, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}
func (*FileRuntime) Launch(
	context.Context,
	LaunchSpec,
	io.Reader,
	func(context.Context, int) error,
) (int, error) {
	return 0, runner.ErrStrongOwnershipUnavailable
}
func (*FileRuntime) KillAndWait(context.Context, runner.ContainmentRef) error {
	return runner.ErrCleanupFailed
}
func (*FileRuntime) WaitEmpty(context.Context, runner.ContainmentRef) error {
	return runner.ErrStrongOwnershipUnavailable
}
func (*FileRuntime) Alive(context.Context, runner.ContainmentRef, int) (bool, error) {
	return false, runner.ErrStrongOwnershipUnavailable
}
func (*FileRuntime) FinalizeFence(context.Context, runner.ContainmentRef) error {
	return runner.ErrCleanupFailed
}
func (*FileRuntime) GarbageCollectFence(context.Context, runner.ContainmentRef) error {
	return runner.ErrCleanupFailed
}

func RunExecLauncherHelper([]string) (bool, error) { return false, nil }
