package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Linux is the only platform whose native runner has host prerequisites an
// operator can satisfy before enrollment (kernel, cgroup v2, delegation,
// linger, the root Supervisor). Doctor reports them read-only so "which mode
// can this machine run" is answered before a join, not discovered as a
// crash-looping service afterwards. The deep proof — actually creating and
// removing a child cgroup — deliberately stays at runtime construction; doctor
// never mutates the host.

const (
	doctorCheckPrivilegedRunnerHost = "linux_privileged_runner_host"
	doctorCheckSharedRunnerHost     = "linux_shared_runner_host"
)

// hostProbe carries every host observation the diagnosis needs, so the logic
// is a pure function of its inputs and testable on any development platform.
type hostProbe struct {
	// Root prefixes every absolute host path; tests point it at a fixture tree.
	Root          string
	KernelRelease string
	UID           int
	// LingerEnabled reflects loginctl's answer; nil means it could not be read.
	LingerEnabled *bool
}

const (
	hostMinimumKernelMajor = 5
	hostMinimumKernelMinor = 14
)

func diagnoseLinuxHost(probe hostProbe) []doctorFinding {
	return []doctorFinding{
		diagnosePrivilegedRunnerHost(probe),
		diagnoseSharedRunnerHost(probe),
	}
}

// diagnosePrivilegedRunnerHost reports whether the root Supervisor boundary is
// present. Its absence is the normal state of an uninstalled machine, so it is
// unavailable with the installation pointer, never a failure.
func diagnosePrivilegedRunnerHost(probe hostProbe) doctorFinding {
	finding := doctorFinding{Check: doctorCheckPrivilegedRunnerHost}
	socket := filepath.Join(probe.Root, "run/sparerunner-supervisor/supervisor.sock")
	info, err := os.Lstat(socket)
	switch {
	case err == nil && info.Mode()&os.ModeSocket != 0:
		finding.Status = doctorStatusPass
		finding.Detail = "root Supervisor socket is present; the privileged native mode is installed"
	case err == nil:
		finding.Status = doctorStatusFail
		finding.Detail = "the root Supervisor socket path exists but is not a socket"
	default:
		finding.Status = doctorStatusUnavailable
		finding.Detail = "root Supervisor is not installed; packaging/linux/install-service.sh provides the privileged mode"
	}
	return finding
}

// diagnoseSharedRunnerHost reports whether this host can construct the
// sudo-free shared-identity mode, naming the first missing prerequisite in
// dependency order so the operator fixes the load-bearing one first.
func diagnoseSharedRunnerHost(probe hostProbe) doctorFinding {
	finding := doctorFinding{Check: doctorCheckSharedRunnerHost}
	if probe.UID == 0 {
		finding.Status = doctorStatusUnavailable
		finding.Detail = "the shared-identity mode requires an unprivileged user; run doctor as the owning user"
		return finding
	}
	major, minor, ok := parseKernelRelease(probe.KernelRelease)
	if !ok {
		finding.Status = doctorStatusFail
		finding.Detail = "cannot parse the running kernel release: " + probe.KernelRelease
		return finding
	}
	if major < hostMinimumKernelMajor ||
		(major == hostMinimumKernelMajor && minor < hostMinimumKernelMinor) {
		finding.Status = doctorStatusUnavailable
		finding.Detail = fmt.Sprintf(
			"kernel %s lacks delegated cgroup.kill; Linux %d.%d or newer is required",
			probe.KernelRelease, hostMinimumKernelMajor, hostMinimumKernelMinor,
		)
		return finding
	}
	if !regularFileExists(filepath.Join(probe.Root, "sys/fs/cgroup/cgroup.controllers")) {
		finding.Status = doctorStatusUnavailable
		finding.Detail = "the unified cgroup v2 hierarchy is not mounted at /sys/fs/cgroup"
		return finding
	}
	delegated := filepath.Join(
		probe.Root,
		fmt.Sprintf("sys/fs/cgroup/user.slice/user-%d.slice/user@%d.service/cgroup.controllers", probe.UID, probe.UID),
	)
	if !regularFileExists(delegated) {
		finding.Status = doctorStatusUnavailable
		finding.Detail = fmt.Sprintf(
			"no delegated systemd user subtree for uid %d; start a systemd --user session first",
			probe.UID,
		)
		return finding
	}
	finding.Status = doctorStatusPass
	detail := []string{
		fmt.Sprintf("shared-identity prerequisites are met (kernel %s, delegated cgroup v2)", probe.KernelRelease),
	}
	switch {
	case probe.LingerEnabled == nil:
		detail = append(detail, "linger state is unreadable; a full-time machine needs loginctl enable-linger")
	case *probe.LingerEnabled:
		detail = append(detail, "linger is enabled, so the user service survives logout")
	default:
		detail = append(detail, "linger is disabled; the user service stops at logout until loginctl enable-linger")
	}
	finding.Detail = strings.Join(detail, "; ")
	return finding
}

func parseKernelRelease(release string) (major, minor int, ok bool) {
	if _, err := fmt.Sscanf(release, "%d.%d", &major, &minor); err != nil ||
		major < 0 || minor < 0 {
		return 0, 0, false
	}
	return major, minor, true
}

func regularFileExists(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular()
}
