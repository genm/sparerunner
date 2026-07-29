package macospackaging

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func (harness installerHarness) runUninstall(t *testing.T, wantSuccess bool) string {
	t.Helper()
	script, err := filepath.Abs("uninstall-service.sh")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/bash", script, harness.plist)
	command.Env = installerEnvironment(harness)
	output, err := command.CombinedOutput()
	if wantSuccess && err != nil {
		t.Fatalf("uninstaller failed: %v\n%s", err, output)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("uninstaller unexpectedly succeeded:\n%s", output)
	}
	return string(output)
}

func (harness installerHarness) resetMutations(t *testing.T) {
	t.Helper()
	if err := os.Remove(harness.mutations); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func (harness installerHarness) plistTarget() string {
	return filepath.Join(
		harness.root,
		"Library/LaunchDaemons/com.genm.sparerunner.agent.plist",
	)
}

func TestUninstallServiceRemovesOnlyThePropertyList(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the macOS shell installer harness requires /bin/bash")
	}
	if output, err := exec.Command("/usr/bin/id", "-u").Output(); err == nil &&
		strings.TrimSpace(string(output)) == "0" {
		t.Skip("production root intentionally rejects installer test indirection")
	}

	t.Run("a never-installed host is a no-op", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)

		output := harness.runUninstall(t, true)
		if !strings.Contains(output, "no SpareRunner package file") {
			t.Fatalf("never-installed uninstall output = %q", output)
		}
	})

	t.Run("removes the loaded plist and retains state", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.resetMutations(t)

		output := harness.runUninstall(t, true)
		if !strings.Contains(output, "removed the SpareRunner LaunchDaemon property list") ||
			!strings.Contains(output, "retained node state") ||
			!strings.Contains(output, "dscl . -delete /Users/sparerunner-runner-0") {
			t.Fatalf("uninstall output = %q", output)
		}
		if _, err := os.Lstat(harness.plistTarget()); !os.IsNotExist(err) {
			t.Fatalf("uninstall retained the plist: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(harness.helper, "launchd-loaded")); !os.IsNotExist(err) {
			t.Fatalf("uninstall left the service loaded: %v", err)
		}
		for _, path := range []string{
			filepath.Join(harness.root, "Library/Application Support/SpareRunner"),
			filepath.Join(harness.root, "Library/Caches/com.genm.sparerunner"),
			filepath.Join(harness.helper, "dscl/Groups/sparerunner-runner-0"),
			filepath.Join(harness.helper, "dscl/Users/sparerunner-runner-0"),
		} {
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("uninstall removed retained state at %s: %v", path, err)
			}
		}
	})

	t.Run("removes an installed but unloaded plist", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.resetMutations(t)
		if err := os.Remove(filepath.Join(harness.helper, "launchd-loaded")); err != nil {
			t.Fatal(err)
		}

		harness.runUninstall(t, true)
		if _, err := os.Lstat(harness.plistTarget()); !os.IsNotExist(err) {
			t.Fatalf("uninstall retained the unloaded plist: %v", err)
		}
	})

	t.Run("a modified plist is refused before any mutation", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.resetMutations(t)
		if err := os.WriteFile(
			harness.plistTarget(),
			[]byte("foreign plist"),
			0o600,
		); err != nil {
			t.Fatal(err)
		}

		output := harness.runUninstall(t, false)
		if !strings.Contains(output, "differs from this package and is not removed") {
			t.Fatalf("modified-plist rejection output = %q", output)
		}
		harness.requireNoMutations(t)
		if _, err := os.Lstat(filepath.Join(harness.helper, "launchd-loaded")); err != nil {
			t.Fatalf("uninstall stopped the service before refusing: %v", err)
		}
	})
}
