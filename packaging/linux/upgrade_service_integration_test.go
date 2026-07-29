package linuxpackaging

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// previousReleaseSuffix simulates a unit shipped by an older release: the same
// package file with one extra trailing comment, so provenance is decided by
// byte identity and never by anything weaker.
const previousReleaseSuffix = "# shipped by the previous release\n"

// makePreviousPackage copies the repository package into a fresh directory and
// appends previousReleaseSuffix to the two service units, standing in for the
// extracted archive of an older release. The declarative definitions stay
// identical because most releases do not change them; unit replacement is the
// provenance path under test either way.
func makePreviousPackage(t *testing.T, source string) string {
	t.Helper()
	working := packageTemporaryDirectory(t)
	previous, err := filepath.EvalSymlinks(working)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"systemd/" + installerAgentUnit,
		"systemd/" + installerSupervisorUnit,
		"systemd/user/" + installerAgentUnit,
		"sysusers.d/sparerunner.conf",
		"tmpfiles.d/sparerunner.conf",
	} {
		contents, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(name, ".service") {
			contents = append(contents, []byte(previousReleaseSuffix)...)
		}
		target := filepath.Join(previous, name)
		makeDirectory(t, filepath.Dir(target), 0o755)
		if err := os.WriteFile(target, contents, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return previous
}

func (harness installerHarness) runPackagingScript(
	t *testing.T,
	wantSuccess bool,
	script string,
	args ...string,
) string {
	t.Helper()
	path := filepath.Join(filepath.Dir(harness.script), script)
	command := exec.Command("/bin/bash", append([]string{path}, args...)...)
	command.Env = installerEnvironment(harness)
	output, err := command.CombinedOutput()
	if wantSuccess && err != nil {
		t.Fatalf("%s failed: %v\n%s", script, err, output)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("%s unexpectedly succeeded:\n%s", script, output)
	}
	return string(output)
}

func (harness installerHarness) runUpgrade(t *testing.T, wantSuccess bool, args ...string) string {
	t.Helper()
	return harness.runPackagingScript(t, wantSuccess, "upgrade-service.sh", args...)
}

func (harness installerHarness) runUpgradeRejecting(t *testing.T, reason string, args ...string) {
	t.Helper()
	output := harness.runUpgrade(t, false, args...)
	if !strings.Contains(output, reason) {
		t.Fatalf("upgrade rejection lacks %q:\n%s", reason, output)
	}
}

func (harness installerHarness) enrollAgentState(t *testing.T) {
	t.Helper()
	writeFile(
		t,
		filepath.Join(harness.root, "var/lib/sparerunner-agent/node-state.json"),
		"{}\n",
	)
}

func (harness installerHarness) requireServicesActive(t *testing.T) {
	t.Helper()
	for _, unit := range []string{installerSupervisorUnit, installerAgentUnit} {
		if _, err := os.Stat(filepath.Join(harness.helper, "active-"+unit)); err != nil {
			t.Fatalf("%s is not active after the upgrade path: %v", unit, err)
		}
	}
}

func (harness installerHarness) requireNoUpgradeStaging(t *testing.T) {
	t.Helper()
	for _, target := range []string{
		filepath.Join(harness.root, "usr/lib/systemd/system", installerAgentUnit),
		filepath.Join(harness.root, "usr/lib/systemd/system", installerSupervisorUnit),
		filepath.Join(harness.root, "usr/lib/sysusers.d/sparerunner.conf"),
		filepath.Join(harness.root, "usr/lib/tmpfiles.d/sparerunner.conf"),
	} {
		for _, suffix := range []string{".sparerunner-upgrade-prev", ".sparerunner-install-tmp"} {
			if _, err := os.Lstat(target + suffix); !os.IsNotExist(err) {
				t.Fatalf("upgrade left staging state at %s%s: %v", target, suffix, err)
			}
		}
	}
}

func TestUpgradeServiceReplacesOnlyProvenPackageFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the Linux shell installer harness requires /bin/bash")
	}
	if os.Geteuid() == 0 {
		t.Skip("production root intentionally rejects installer test indirection")
	}

	t.Run("binary-only upgrade restarts without republishing", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.resetMutations(t)

		output := harness.runUpgrade(t, true, harness.source)
		if !strings.Contains(output, "already matches this package") {
			t.Fatalf("binary-only upgrade output = %q", output)
		}
		mutations := harness.mutationLines(t)
		requireMutation(t, mutations, "systemctl stop ")
		requireMutation(t, mutations, "systemd-sysusers ")
		requireMutation(t, mutations, "systemd-tmpfiles --create ")
		requireMutation(t, mutations, "systemctl daemon-reload")
		requireMutation(t, mutations, "systemctl start ")
		for _, mutation := range mutations {
			if strings.HasPrefix(mutation, "install ") || strings.HasPrefix(mutation, "ln ") {
				t.Fatalf("binary-only upgrade republished a package file: %q", mutations)
			}
		}
		harness.requireServicesActive(t)
		harness.requireNoUpgradeStaging(t)
	})

	t.Run("previous-release units are replaced with proven provenance", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		previous := makePreviousPackage(t, harness.source)
		harness.runPackagingScript(t, true, "install-service.sh", previous)
		harness.resetMutations(t)

		output := harness.runUpgrade(t, true, harness.source, "--previous", previous)
		if !strings.Contains(output, "replaced "+installerAgentUnit) {
			t.Fatalf("upgrade output = %q", output)
		}
		requireSameContents(
			t,
			filepath.Join(harness.root, "usr/lib/systemd/system", installerAgentUnit),
			filepath.Join(harness.source, "systemd", installerAgentUnit),
		)
		requireSameContents(
			t,
			filepath.Join(harness.root, "usr/lib/systemd/system", installerSupervisorUnit),
			filepath.Join(harness.source, "systemd", installerSupervisorUnit),
		)
		harness.requireServicesActive(t)
		harness.requireNoUpgradeStaging(t)
	})

	t.Run("previous-release unit without --previous is refused", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		previous := makePreviousPackage(t, harness.source)
		harness.runPackagingScript(t, true, "install-service.sh", previous)
		harness.resetMutations(t)

		harness.runUpgradeRejecting(
			t,
			"pass --previous <extracted-previous-package> to prove its provenance",
			harness.source,
		)
		harness.requireNoMutations(t)
	})

	t.Run("operator-modified unit is refused before any mutation", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		previous := makePreviousPackage(t, harness.source)
		harness.run(t, true)
		target := filepath.Join(harness.root, "usr/lib/systemd/system", installerAgentUnit)
		contents, err := os.ReadFile(target)
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, target, string(contents)+"# operator edit\n")
		harness.resetMutations(t)

		harness.runUpgradeRejecting(
			t,
			"matches neither this package nor the previous package",
			harness.source,
			"--previous", previous,
		)
		harness.requireNoMutations(t)
	})

	t.Run("a host that was never installed is refused", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)

		harness.runUpgradeRejecting(
			t,
			"no owned SpareRunner installation to upgrade",
			harness.source,
		)
		harness.requireNoMutations(t)
	})

	t.Run("leftover staging state is refused before any mutation", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		target := filepath.Join(harness.root, "usr/lib/systemd/system", installerAgentUnit)
		writeFile(t, target+".sparerunner-upgrade-prev", "stale staging\n")
		harness.resetMutations(t)

		harness.runUpgradeRejecting(
			t,
			"refusing to replace upgrade staging state",
			harness.source,
		)
		harness.requireNoMutations(t)
	})

	t.Run("staging failure restores the previous release and restarts it", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		previous := makePreviousPackage(t, harness.source)
		harness.runPackagingScript(t, true, "install-service.sh", previous)
		harness.enrollAgentState(t)
		harness.resetMutations(t)
		harness.injectMutationFailureAfter(t, "install ")

		output := harness.runUpgrade(t, false, harness.source, "--previous", previous)
		if !strings.Contains(output, "the previous installation was restored and restarted") {
			t.Fatalf("rollback output = %q", output)
		}
		requireFileContents(
			t,
			filepath.Join(harness.root, "usr/lib/systemd/system", installerAgentUnit),
			readRepositoryPackagingFile(t, "systemd/"+installerAgentUnit)+previousReleaseSuffix,
		)
		harness.requireServicesActive(t)
		harness.requireNoUpgradeStaging(t)
	})

	t.Run("post-publication failure removes the new files and restores", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		previous := makePreviousPackage(t, harness.source)
		harness.runPackagingScript(t, true, "install-service.sh", previous)
		harness.enrollAgentState(t)
		harness.resetMutations(t)
		harness.injectMutationFailureAfter(t, "systemctl start ")

		harness.runUpgrade(t, false, harness.source, "--previous", previous)
		requireFileContents(
			t,
			filepath.Join(harness.root, "usr/lib/systemd/system", installerAgentUnit),
			readRepositoryPackagingFile(t, "systemd/"+installerAgentUnit)+previousReleaseSuffix,
		)
		requireFileContents(
			t,
			filepath.Join(harness.root, "usr/lib/systemd/system", installerSupervisorUnit),
			readRepositoryPackagingFile(t, "systemd/"+installerSupervisorUnit)+previousReleaseSuffix,
		)
		harness.requireServicesActive(t)
		harness.requireNoUpgradeStaging(t)
	})

	t.Run("enrolled node with a dead agent fails and rolls back", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		previous := makePreviousPackage(t, harness.source)
		harness.runPackagingScript(t, true, "install-service.sh", previous)
		harness.enrollAgentState(t)
		harness.blockUnitStart(t, installerAgentUnit)
		harness.resetMutations(t)

		output := harness.runUpgrade(t, false, harness.source, "--previous", previous)
		if !strings.Contains(output, "did not start with existing node state") {
			t.Fatalf("dead-agent upgrade output = %q", output)
		}
		requireFileContents(
			t,
			filepath.Join(harness.root, "usr/lib/systemd/system", installerAgentUnit),
			readRepositoryPackagingFile(t, "systemd/"+installerAgentUnit)+previousReleaseSuffix,
		)
		harness.requireNoUpgradeStaging(t)
	})

	t.Run("unenrolled node reports the pending enrollment step", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.blockUnitStart(t, installerAgentUnit)
		harness.resetMutations(t)

		output := harness.runUpgrade(t, true, harness.source)
		if !strings.Contains(output, "stays not-initialized until this node is enrolled") {
			t.Fatalf("unenrolled upgrade output = %q", output)
		}
	})
}

func (harness userInstallerHarness) runUserPackagingScript(
	t *testing.T,
	wantSuccess bool,
	script string,
	args ...string,
) string {
	t.Helper()
	path := filepath.Join(filepath.Dir(harness.script), script)
	command := exec.Command("/bin/bash", append([]string{path}, args...)...)
	command.Env = harness.environment()
	output, err := command.CombinedOutput()
	if wantSuccess && err != nil {
		t.Fatalf("%s failed: %v\n%s", script, err, output)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("%s unexpectedly succeeded:\n%s", script, output)
	}
	return string(output)
}

func (harness userInstallerHarness) runUserUpgrade(
	t *testing.T,
	wantSuccess bool,
	args ...string,
) string {
	t.Helper()
	return harness.runUserPackagingScript(t, wantSuccess, "upgrade-user-service.sh", args...)
}

func (harness userInstallerHarness) runUserUpgradeRejecting(
	t *testing.T,
	reason string,
	args ...string,
) {
	t.Helper()
	output := harness.runUserUpgrade(t, false, args...)
	if !strings.Contains(output, reason) {
		t.Fatalf("user upgrade rejection lacks %q:\n%s", reason, output)
	}
}

func TestUpgradeUserServiceReplacesOnlyProvenPackageFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the Linux shell installer harness requires /bin/bash")
	}
	if os.Geteuid() == 0 {
		t.Skip("the user upgrade intentionally refuses root")
	}

	t.Run("binary-only upgrade restarts without republishing", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		harness.run(t, true)
		removePath(t, harness.mutations)

		output := harness.runUserUpgrade(t, true, harness.source)
		if !strings.Contains(output, "already matches this package") {
			t.Fatalf("binary-only user upgrade output = %q", output)
		}
		mutations := harness.mutationLines(t)
		requireMutation(t, mutations, "systemctl --user stop ")
		requireMutation(t, mutations, "systemctl --user daemon-reload")
		requireMutation(t, mutations, "systemctl --user start ")
		for _, mutation := range mutations {
			if strings.HasPrefix(mutation, "install ") || strings.HasPrefix(mutation, "ln ") {
				t.Fatalf("binary-only user upgrade republished the unit: %q", mutations)
			}
		}
	})

	t.Run("previous-release unit is replaced with proven provenance", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		previous := makePreviousPackage(t, harness.source)
		harness.runUserPackagingScript(t, true, "install-user-service.sh", previous)
		removePath(t, harness.mutations)

		output := harness.runUserUpgrade(t, true, harness.source, "--previous", previous)
		if !strings.Contains(output, "replaced "+installerAgentUnit) {
			t.Fatalf("user upgrade output = %q", output)
		}
		requireSameContents(
			t,
			harness.unitTarget(),
			filepath.Join(harness.source, "systemd/user", installerAgentUnit),
		)
		for _, suffix := range []string{".sparerunner-upgrade-prev", ".sparerunner-install-tmp"} {
			if _, err := os.Lstat(harness.unitTarget() + suffix); !os.IsNotExist(err) {
				t.Fatalf("user upgrade left staging state at %s%s: %v", harness.unitTarget(), suffix, err)
			}
		}
	})

	t.Run("modified unit is refused before any mutation", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		previous := makePreviousPackage(t, harness.source)
		harness.run(t, true)
		contents, err := os.ReadFile(harness.unitTarget())
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, harness.unitTarget(), string(contents)+"# operator edit\n")
		removePath(t, harness.mutations)

		harness.runUserUpgradeRejecting(
			t,
			"matches neither this package nor the previous package",
			harness.source,
			"--previous", previous,
		)
		harness.requireNoMutations(t)
	})

	t.Run("a user without an installed unit is refused", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)

		harness.runUserUpgradeRejecting(
			t,
			"no installed "+installerAgentUnit+" to upgrade",
			harness.source,
		)
		harness.requireNoMutations(t)
	})

	t.Run("post-publication failure restores the previous unit", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		previous := makePreviousPackage(t, harness.source)
		harness.runUserPackagingScript(t, true, "install-user-service.sh", previous)
		removePath(t, harness.mutations)
		writeFile(t, filepath.Join(harness.helper, "fail-after-mutation-prefix"), "systemctl --user start \n")

		output := harness.runUserUpgrade(t, false, harness.source, "--previous", previous)
		if !strings.Contains(output, "the previous installation was restored and restarted") {
			t.Fatalf("user rollback output = %q", output)
		}
		requireFileContents(
			t,
			harness.unitTarget(),
			readRepositoryPackagingFile(t, "systemd/user/"+installerAgentUnit)+previousReleaseSuffix,
		)
		for _, suffix := range []string{".sparerunner-upgrade-prev", ".sparerunner-install-tmp"} {
			if _, err := os.Lstat(harness.unitTarget() + suffix); !os.IsNotExist(err) {
				t.Fatalf("user rollback left staging state at %s%s: %v", harness.unitTarget(), suffix, err)
			}
		}
	})
}
