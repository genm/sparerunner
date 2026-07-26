//go:build linux

package runner

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestBareProcessGroupLosesARealSetsidDescendantAfterLeaderExit(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid utility is unavailable")
	}
	directory := t.TempDir()
	script := filepath.Join(directory, "escape.sh")
	contents := "#!/bin/sh\nsetsid /bin/sh -c 'echo $$ > escaped.pid; exec sleep 60' &\nwhile [ ! -s escaped.pid ]; do sleep 0.01; done\nexit 0\n"
	if err := os.WriteFile(script, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(context.Background(), "/bin/sh", script)
	command.Dir = directory
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	leaderPID := command.Process.Pid
	childPID := waitForPIDFile(t, filepath.Join(directory, "escaped.pid"))
	t.Cleanup(func() {
		_ = syscall.Kill(childPID, syscall.SIGKILL)
	})
	if err := command.Wait(); err != nil {
		t.Fatalf("leader exit: %v", err)
	}
	// This is the strongest cleanup a bare process-group supervisor can perform
	// after its leader exits. The descendant has created a new session/PGID.
	if err := syscall.Kill(-leaderPID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("process-group cleanup: %v", err)
	}
	if err := syscall.Kill(childPID, 0); err != nil {
		t.Fatalf("setsid descendant did not escape the process group: %v", err)
	}
	if NewSupervisor().StrongDescendantOwnership() {
		t.Fatal("bare process group reported strong ownership after a demonstrated escape")
	}
}

func waitForPIDFile(t *testing.T, filename string) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(filename)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(raw)))
			if parseErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("helper did not write %s", filename)
	return 0
}
