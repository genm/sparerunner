//go:build linux

package main

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func diagnoseNodeHost() []doctorFinding {
	return diagnoseLinuxHost(hostProbe{
		Root:          "/",
		KernelRelease: runningKernelRelease(),
		UID:           os.Geteuid(),
		LingerEnabled: readLingerState(),
	})
}

func runningKernelRelease() string {
	var name syscall.Utsname
	if err := syscall.Uname(&name); err != nil {
		return ""
	}
	release := make([]byte, 0, len(name.Release))
	for _, value := range name.Release {
		if value == 0 {
			break
		}
		release = append(release, byte(value))
	}
	return string(release)
}

// readLingerState asks logind read-only. A missing loginctl or an error is nil
// (unreadable), never a claim in either direction.
func readLingerState() *bool {
	output, err := exec.Command(
		"loginctl", "show-user", "--property=Linger", "--value",
		strconv.Itoa(os.Geteuid()),
	).Output()
	if err != nil {
		return nil
	}
	enabled := strings.TrimSpace(string(output)) == "yes"
	return &enabled
}
