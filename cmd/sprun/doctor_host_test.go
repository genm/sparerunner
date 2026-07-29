package main

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func healthyHostFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, directory := range []string{
		"sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service",
		"run/sparerunner-supervisor",
	} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []string{
		"sys/fs/cgroup/cgroup.controllers",
		"sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.controllers",
	} {
		if err := os.WriteFile(filepath.Join(root, file), []byte("memory pids\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func fixtureProbe(root string) hostProbe {
	linger := false
	return hostProbe{
		Root:          root,
		KernelRelease: "6.8.0-52-generic",
		UID:           1000,
		LingerEnabled: &linger,
	}
}

func findingByCheck(t *testing.T, findings []doctorFinding, check string) doctorFinding {
	t.Helper()
	for _, finding := range findings {
		if finding.Check == check {
			return finding
		}
	}
	t.Fatalf("no %q finding: %#v", check, findings)
	return doctorFinding{}
}

func TestDiagnoseLinuxHostReportsBothModes(t *testing.T) {
	root := healthyHostFixture(t)
	socket := filepath.Join(root, "run/sparerunner-supervisor/supervisor.sock")
	listener, err := net.Listen("unix", shortSocketPath(t, socket))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	findings := diagnoseLinuxHost(fixtureProbe(root))
	if len(findings) != 2 {
		t.Fatalf("findings = %#v", findings)
	}
	shared := findingByCheck(t, findings, doctorCheckSharedRunnerHost)
	if shared.Status != doctorStatusPass ||
		!strings.Contains(shared.Detail, "prerequisites are met") ||
		!strings.Contains(shared.Detail, "loginctl enable-linger") {
		t.Fatalf("shared finding = %#v", shared)
	}
}

// shortSocketPath links the fixture path into a short temporary directory when
// the fixture root exceeds the AF_UNIX limit, keeping the fixture layout while
// letting the listener bind.
func shortSocketPath(t *testing.T, socket string) string {
	t.Helper()
	if len(socket) < 100 {
		return socket
	}
	short, err := os.MkdirTemp("/tmp", "sprun-doctor")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(short); err != nil {
			t.Errorf("cannot remove %s: %v", short, err)
		}
	})
	bound := filepath.Join(short, "supervisor.sock")
	t.Cleanup(func() { _ = os.Remove(socket) })
	if err := os.Symlink(bound, socket); err != nil {
		t.Fatal(err)
	}
	return bound
}

func TestDiagnoseLinuxHostPrivilegedModeStates(t *testing.T) {
	t.Run("absent supervisor is unavailable with the installer pointer", func(t *testing.T) {
		findings := diagnoseLinuxHost(fixtureProbe(healthyHostFixture(t)))
		privileged := findingByCheck(t, findings, doctorCheckPrivilegedRunnerHost)
		if privileged.Status != doctorStatusUnavailable ||
			!strings.Contains(privileged.Detail, "install-service.sh") {
			t.Fatalf("privileged finding = %#v", privileged)
		}
	})

	t.Run("a non-socket file at the socket path fails", func(t *testing.T) {
		root := healthyHostFixture(t)
		if err := os.WriteFile(
			filepath.Join(root, "run/sparerunner-supervisor/supervisor.sock"),
			[]byte("not a socket"),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
		findings := diagnoseLinuxHost(fixtureProbe(root))
		privileged := findingByCheck(t, findings, doctorCheckPrivilegedRunnerHost)
		if privileged.Status != doctorStatusFail {
			t.Fatalf("privileged finding = %#v", privileged)
		}
	})
}

func TestDiagnoseLinuxHostSharedModeNamesTheMissingPrerequisite(t *testing.T) {
	for _, missing := range []struct {
		name    string
		detail  string
		status  string
		prepare func(*testing.T, hostProbe) hostProbe
	}{
		{
			name:   "root user",
			detail: "requires an unprivileged user",
			status: doctorStatusUnavailable,
			prepare: func(_ *testing.T, probe hostProbe) hostProbe {
				probe.UID = 0
				return probe
			},
		},
		{
			name:   "unparsable kernel",
			detail: "cannot parse the running kernel release",
			status: doctorStatusFail,
			prepare: func(_ *testing.T, probe hostProbe) hostProbe {
				probe.KernelRelease = "unknown"
				return probe
			},
		},
		{
			name:   "old kernel",
			detail: "Linux 5.14 or newer is required",
			status: doctorStatusUnavailable,
			prepare: func(_ *testing.T, probe hostProbe) hostProbe {
				probe.KernelRelease = "5.10.0-30-generic"
				return probe
			},
		},
		{
			name:   "no cgroup v2",
			detail: "unified cgroup v2 hierarchy is not mounted",
			status: doctorStatusUnavailable,
			prepare: func(t *testing.T, probe hostProbe) hostProbe {
				if err := os.Remove(filepath.Join(probe.Root, "sys/fs/cgroup/cgroup.controllers")); err != nil {
					t.Fatal(err)
				}
				return probe
			},
		},
		{
			name:   "no delegated user subtree",
			detail: "no delegated systemd user subtree",
			status: doctorStatusUnavailable,
			prepare: func(t *testing.T, probe hostProbe) hostProbe {
				if err := os.Remove(filepath.Join(
					probe.Root,
					"sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.controllers",
				)); err != nil {
					t.Fatal(err)
				}
				return probe
			},
		},
	} {
		t.Run(missing.name, func(t *testing.T) {
			probe := missing.prepare(t, fixtureProbe(healthyHostFixture(t)))
			shared := findingByCheck(t, diagnoseLinuxHost(probe), doctorCheckSharedRunnerHost)
			if shared.Status != missing.status || !strings.Contains(shared.Detail, missing.detail) {
				t.Fatalf("shared finding = %#v, want %s containing %q", shared, missing.status, missing.detail)
			}
		})
	}
}

func TestDiagnoseLinuxHostReportsLingerStates(t *testing.T) {
	root := healthyHostFixture(t)

	probe := fixtureProbe(root)
	enabled := true
	probe.LingerEnabled = &enabled
	shared := findingByCheck(t, diagnoseLinuxHost(probe), doctorCheckSharedRunnerHost)
	if !strings.Contains(shared.Detail, "linger is enabled") {
		t.Fatalf("shared finding = %#v", shared)
	}

	probe.LingerEnabled = nil
	shared = findingByCheck(t, diagnoseLinuxHost(probe), doctorCheckSharedRunnerHost)
	if !strings.Contains(shared.Detail, "linger state is unreadable") {
		t.Fatalf("shared finding = %#v", shared)
	}
}
