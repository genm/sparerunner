//go:build !linux

package linux

import (
	"errors"
	"testing"

	"github.com/genm/sparerunner/internal/runner"
)

func TestNonLinuxHelperProbeLeavesNormalCLIArgumentsUntouched(t *testing.T) {
	handled, err := RunExecLauncherHelper([]string{"serve"})
	if err != nil || handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	handled, err = RunExecLauncherHelper([]string{helperModeArgument})
	if !handled || !errors.Is(err, runner.ErrStrongOwnershipUnavailable) {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
}
