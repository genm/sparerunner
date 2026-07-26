//go:build !linux

package linux

import (
	"context"
	"io"
	"os"

	"github.com/genm/tewake/internal/runner"
)

// PipeLauncher is declared on every platform so callers can compile their
// platform wiring, while NewFileRuntime below refuses non-Linux admission.
type PipeLauncher interface {
	Launch(context.Context, LaunchSpec, io.Reader, *os.File) (int, error)
}

// ExecLauncher is an explicit non-Linux stub; it cannot claim cgroup-v2
// containment on macOS or Windows.
type ExecLauncher struct{ HelperPath string }

func NewExecLauncher(string) (ExecLauncher, error) {
	return ExecLauncher{}, runner.ErrStrongOwnershipUnavailable
}

func (ExecLauncher) Launch(context.Context, LaunchSpec, io.Reader, *os.File) (int, error) {
	return 0, runner.ErrStrongOwnershipUnavailable
}

const helperModeArgument = "--tewake-linux-launcher-helper"

func RunExecLauncherHelper(args []string) (bool, error) {
	if len(args) == 0 || args[0] != helperModeArgument {
		return false, nil
	}
	return true, runner.ErrStrongOwnershipUnavailable
}

// FileRuntime is an intentional non-Linux stub.  macOS and Windows must use
// their own ownership adapters rather than accidentally inheriting cgroup rules.
type FileRuntime struct{}

func NewFileRuntime(string, string, PipeLauncher) (*FileRuntime, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}

func NewSystemdFileRuntime(string, PipeLauncher) (*FileRuntime, error) {
	return nil, runner.ErrStrongOwnershipUnavailable
}

func SystemdDelegatedCgroupRoot() (string, error) {
	return "", runner.ErrStrongOwnershipUnavailable
}
