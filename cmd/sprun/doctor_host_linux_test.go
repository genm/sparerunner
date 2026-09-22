package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Missing prerequisites are ordinary on a new host. Broken paths are not:
// neither their findings nor the aggregate report may claim a healthy host.
func TestDiagnoseLinuxHostRejectsMalformedPrerequisitePaths(t *testing.T) {
	for _, target := range []struct {
		name, path, check string
	}{
		{"supervisor", "run/sparerunner-supervisor/supervisor.sock", doctorCheckPrivilegedRunnerHost},
		{"cgroup hierarchy", "sys/fs/cgroup/cgroup.controllers", doctorCheckSharedRunnerHost},
		{"user delegation", "sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.controllers", doctorCheckSharedRunnerHost},
	} {
		for _, kind := range []string{"directory", "parent is a file", "symlink", "permission denied"} {
			t.Run(target.name+"/"+kind, func(t *testing.T) {
				if kind == "permission denied" && os.Geteuid() == 0 {
					t.Skip("root bypasses directory permissions")
				}
				root := healthyHostFixture(t)
				path := filepath.Join(root, target.path)
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "directory":
					if err := os.Mkdir(path, 0o755); err != nil {
						t.Fatal(err)
					}
				case "parent is a file":
					parent := filepath.Dir(path)
					if err := os.RemoveAll(parent); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(parent, []byte("private fixture content"), 0o600); err != nil {
						t.Fatal(err)
					}
				case "symlink":
					if err := os.Symlink(filepath.Join(root, "missing"), path); err != nil {
						t.Fatal(err)
					}
				case "permission denied":
					parent := filepath.Dir(path)
					if err := os.Chmod(parent, 0); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := os.Chmod(parent, 0o755); err != nil {
							t.Error(err)
						}
					})
				}
				findings := diagnoseLinuxHost(fixtureProbe(root))
				finding := findingByCheck(t, findings, target.check)
				if finding.Status != doctorStatusFail || doctorFindingsHealthy(findings) {
					t.Fatalf("broken prerequisite must fail: %#v", findings)
				}
				if strings.Contains(finding.Detail, root) || strings.Contains(finding.Detail, "private fixture content") {
					t.Fatalf("finding leaks filesystem details: %#v", finding)
				}
			})
		}
	}
}
