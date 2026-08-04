package linuxpackaging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const (
	installerAgentUnit      = "sparerunner-agent.service"
	installerSupervisorUnit = "sparerunner-supervisor.service"
	installerMarkerName     = ".sparerunner-install-ownership-v1"
	installerDefaultKernel  = "6.8.0-52-generic"
)

type installerHarness struct {
	root      string
	tools     string
	helper    string
	mutations string
	script    string
	source    string
	kernel    string
}

func TestInstallServiceAcceptsOnlyCleanOwnedState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the Linux shell installer harness requires /bin/bash")
	}
	if os.Geteuid() == 0 {
		t.Skip("production root intentionally rejects installer test indirection")
	}

	t.Run("clean host installs the package and starts the supervisor", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)

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
		requireSameContents(
			t,
			filepath.Join(harness.root, "usr/lib/sysusers.d/sparerunner.conf"),
			filepath.Join(harness.source, "sysusers.d/sparerunner.conf"),
		)
		requireSameContents(
			t,
			filepath.Join(harness.root, "usr/lib/tmpfiles.d/sparerunner.conf"),
			filepath.Join(harness.source, "tmpfiles.d/sparerunner.conf"),
		)
		requireFileContents(
			t,
			filepath.Join(harness.root, "var/lib/sparerunner-supervisor", installerMarkerName),
			"version=1\nrole=supervisor-state\npath=/var/lib/sparerunner-supervisor\n",
		)

		mutations := harness.mutationLines(t)
		requireMutation(t, mutations, "systemd-sysusers ")
		requireMutation(t, mutations, "systemd-tmpfiles --create ")
		requireMutation(t, mutations, "systemctl daemon-reload")
		requireMutation(t, mutations, "systemctl enable --now ")
		joined := strings.Join(mutations, "\n")
		if strings.Contains(joined, "chown") || strings.Contains(joined, "chmod") {
			t.Fatal("installer repaired existing ownership or mode")
		}
		// The privileged Supervisor owns the delegated cgroup boundary, so the
		// Agent group must reach the socket without owning its directory.
		agentGID := harness.identity(t, "group", "sparerunner-agent")
		requireStatOverride(
			t,
			harness.helper,
			filepath.Join(harness.root, "run/sparerunner-supervisor"),
			"0:"+agentGID+":0750",
		)
	})

	t.Run("owned retry publishes no file again", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.prepareRetry(t)

		harness.run(t, true)
		mutations := harness.mutationLines(t)
		for _, mutation := range mutations {
			if strings.HasPrefix(mutation, "install ") || strings.HasPrefix(mutation, "ln ") {
				t.Fatalf("owned retry republished a package file: %q", mutations)
			}
		}
		requireMutation(t, mutations, "systemctl enable --now ")
	})

	t.Run("first install reports the pending enrollment step", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.blockUnitStart(t, installerAgentUnit)

		output := harness.run(t, true)
		if !strings.Contains(output, "stays not-initialized until this node is enrolled") {
			t.Fatalf("first install output = %q", output)
		}
	})

	t.Run("an enrolled node with a dead agent fails the install", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		writeFile(
			t,
			filepath.Join(harness.root, "var/lib/sparerunner-agent/node.json"),
			"{\"nodeId\":\"durable\"}\n",
		)
		harness.prepareRetry(t)
		harness.blockUnitStart(t, installerAgentUnit)

		harness.runRejecting(t, "did not start with existing node state")
	})

	t.Run("running services are never reinstalled over", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.resetMutations(t)

		harness.runRejecting(t, "already running; use the documented upgrade flow")
		harness.requireNoMutations(t)
	})

	for _, unsupported := range []struct {
		name    string
		reason  string
		prepare func(*testing.T, installerHarness) installerHarness
	}{
		{
			name:   "kernel older than delegated cgroup.kill",
			reason: "requires Linux 5.14 or newer",
			prepare: func(_ *testing.T, harness installerHarness) installerHarness {
				harness.kernel = "5.10.0-30-generic"
				return harness
			},
		},
		{
			name:   "unparsable kernel release",
			reason: "cannot parse the running kernel release",
			prepare: func(_ *testing.T, harness installerHarness) installerHarness {
				harness.kernel = "unknown"
				return harness
			},
		},
		{
			name:   "cgroup v1 hierarchy",
			reason: "unified cgroup v2 hierarchy is not mounted",
			prepare: func(t *testing.T, harness installerHarness) installerHarness {
				removePath(t, filepath.Join(harness.root, "sys/fs/cgroup/cgroup.controllers"))
				return harness
			},
		},
		{
			name:   "missing agent binary",
			reason: "unsafe or missing regular file",
			prepare: func(t *testing.T, harness installerHarness) installerHarness {
				removePath(t, filepath.Join(harness.root, "usr/bin/sparerunner-agent"))
				return harness
			},
		},
		{
			name:   "group-writable agent binary",
			reason: "does not match its ownership contract",
			prepare: func(t *testing.T, harness installerHarness) installerHarness {
				setStatOverride(
					t,
					harness.helper,
					filepath.Join(harness.root, "usr/bin/sparerunner-agent"),
					"0:0:0775",
				)
				return harness
			},
		},
		{
			name:   "symlinked unit directory",
			reason: "unsafe or missing installer ancestor",
			prepare: func(t *testing.T, harness installerHarness) installerHarness {
				units := filepath.Join(harness.root, "usr/lib/systemd/system")
				removePath(t, units)
				foreign := filepath.Join(harness.root, "foreign-units")
				makeDirectory(t, foreign, 0o755)
				if err := os.Symlink(foreign, units); err != nil {
					t.Fatal(err)
				}
				return harness
			},
		},
		{
			name:   "world-writable unit ancestor",
			reason: "not root-owned and write-safe",
			prepare: func(t *testing.T, harness installerHarness) installerHarness {
				setStatOverride(
					t,
					harness.helper,
					filepath.Join(harness.root, "usr/lib/systemd"),
					"0:0:0777",
				)
				return harness
			},
		},
		{
			name:   "foreign unit file",
			reason: "differs from this package",
			prepare: func(t *testing.T, harness installerHarness) installerHarness {
				writeFile(
					t,
					filepath.Join(harness.root, "usr/lib/systemd/system", installerAgentUnit),
					"[Service]\nExecStart=/bin/false\n",
				)
				return harness
			},
		},
		{
			name:   "foreign tmpfiles definition",
			reason: "differs from this package",
			prepare: func(t *testing.T, harness installerHarness) installerHarness {
				writeFile(
					t,
					filepath.Join(harness.root, "usr/lib/tmpfiles.d/sparerunner.conf"),
					"d /var/lib/elsewhere 0777 root root -\n",
				)
				return harness
			},
		},
		{
			name:   "unmarked supervisor state root holding foreign content",
			reason: "contains foreign content",
			prepare: func(t *testing.T, harness installerHarness) installerHarness {
				state := filepath.Join(harness.root, "var/lib/sparerunner-supervisor")
				makeDirectory(t, state, 0o700)
				writeFile(t, filepath.Join(state, "foreign.db"), "not SpareRunner state\n")
				return harness
			},
		},
		{
			name:   "declared directory owned by another identity",
			reason: "does not match its ownership contract",
			prepare: func(t *testing.T, harness installerHarness) installerHarness {
				agentState := filepath.Join(harness.root, "var/lib/sparerunner-agent")
				makeDirectory(t, agentState, 0o700)
				writeIdentity(t, harness.helper, "passwd", "sparerunner-agent", "901")
				writeIdentity(t, harness.helper, "group", "sparerunner-agent", "901")
				setStatOverride(t, harness.helper, agentState, "1000:1000:0700")
				return harness
			},
		},
		{
			name:   "declared directory without its service identity",
			reason: "without its service identity",
			prepare: func(t *testing.T, harness installerHarness) installerHarness {
				makeDirectory(
					t,
					filepath.Join(harness.root, "var/cache/sparerunner-agent"),
					0o700,
				)
				return harness
			},
		},
	} {
		t.Run("preflight rejects "+unsupported.name, func(t *testing.T) {
			t.Parallel()
			harness := unsupported.prepare(t, newInstallerHarness(t))

			harness.runRejecting(t, unsupported.reason)
			harness.requireNoMutations(t)
		})
	}

	for _, failure := range []struct {
		name   string
		prefix func(installerHarness) string
	}{
		{
			name: "unit staging",
			prefix: func(installerHarness) string {
				return "install "
			},
		},
		{
			name: "unit publication",
			prefix: func(harness installerHarness) string {
				target := filepath.Join(
					harness.root,
					"usr/lib/systemd/system",
					installerAgentUnit,
				)
				return "ln " + target + ".sparerunner-install-tmp " + target
			},
		},
		{
			name: "account declaration",
			prefix: func(installerHarness) string {
				return "systemd-sysusers "
			},
		},
		{
			name: "directory declaration",
			prefix: func(installerHarness) string {
				return "systemd-tmpfiles --create "
			},
		},
		{
			name: "daemon reload",
			prefix: func(installerHarness) string {
				return "systemctl daemon-reload"
			},
		},
		{
			name: "service activation",
			prefix: func(installerHarness) string {
				return "systemctl enable --now "
			},
		},
	} {
		t.Run("verified rollback converges after "+failure.name+" failure", func(t *testing.T) {
			t.Parallel()
			harness := newInstallerHarness(t)
			harness.injectMutationFailureAfter(t, failure.prefix(harness))

			harness.run(t, false)
			harness.requirePublishedTargetsAbsent(t)
			harness.prepareRetry(t)

			harness.run(t, true)
			requireFileContents(
				t,
				filepath.Join(harness.root, "var/lib/sparerunner-supervisor", installerMarkerName),
				"version=1\nrole=supervisor-state\npath=/var/lib/sparerunner-supervisor\n",
			)
		})
	}
}

func newInstallerHarness(t *testing.T) installerHarness {
	t.Helper()
	working := shortTemporaryDirectory(t)
	root := filepath.Join(working, "root")
	tools := filepath.Join(working, "tools")
	helper := filepath.Join(working, "helper")
	for _, directory := range []string{
		root,
		tools,
		helper,
		filepath.Join(root, "run"),
		filepath.Join(root, "sys/fs/cgroup"),
		filepath.Join(root, "usr/lib/systemd/system"),
		filepath.Join(root, "usr/lib/sysusers.d"),
		filepath.Join(root, "usr/lib/tmpfiles.d"),
		filepath.Join(root, "usr/bin"),
		filepath.Join(root, "var/cache"),
		filepath.Join(root, "var/lib"),
	} {
		makeDirectory(t, directory, 0o755)
	}
	writeFile(t, filepath.Join(root, "sys/fs/cgroup/cgroup.controllers"), "cpu memory pids\n")
	writeFile(t, filepath.Join(root, ".sparerunner-installer-test-root"), "test-only\n")
	binary := filepath.Join(root, "usr/bin/sparerunner-agent")
	writeFile(t, binary, "#!/bin/sh\nexit 0\n")
	if err := os.Chmod(binary, 0o755); err != nil {
		t.Fatal(err)
	}
	wrapper := []byte(`#!/bin/sh
export SPARERUNNER_INSTALL_HELPER_PROCESS=1
export SPARERUNNER_INSTALL_HELPER_TOOL="${0##*/}"
# Each command is a short-lived child of the race-instrumented test binary;
# omit the detector's default one-second exit delay for these helpers.
export GORACE="atexit_sleep_ms=0"
exec "$SPARERUNNER_INSTALL_TEST_BINARY" -test.run=TestLinuxInstallerHelperProcess -- "$@"
`)
	for _, tool := range []string{
		"cmp",
		"getent",
		"id",
		"install",
		"ln",
		"rm",
		"sleep",
		"stat",
		"systemctl",
		"systemd-sysusers",
		"systemd-tmpfiles",
		"uname",
	} {
		path := filepath.Join(tools, tool)
		if err := os.WriteFile(path, wrapper, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	script, err := filepath.Abs("install-service.sh")
	if err != nil {
		t.Fatal(err)
	}
	source, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	return installerHarness{
		root:      root,
		tools:     tools,
		helper:    helper,
		mutations: filepath.Join(helper, "mutations.log"),
		script:    script,
		source:    source,
		kernel:    installerDefaultKernel,
	}
}

func (harness installerHarness) run(t *testing.T, wantSuccess bool) string {
	t.Helper()
	command := exec.Command("/bin/bash", harness.script, harness.source)
	command.Env = installerEnvironment(harness)
	output, err := command.CombinedOutput()
	if wantSuccess && err != nil {
		t.Fatalf("installer failed: %v\n%s", err, output)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("installer unexpectedly succeeded:\n%s", output)
	}
	return string(output)
}

// runRejecting requires the installer to refuse for the exact reason under test.
// Without it a rejection assertion would also pass for an unrelated preflight
// failure, which is how a fail-closed guard silently becomes untested.
func (harness installerHarness) runRejecting(t *testing.T, reason string) {
	t.Helper()
	output := harness.run(t, false)
	if !strings.Contains(output, reason) {
		t.Fatalf("installer rejection lacks %q:\n%s", reason, output)
	}
}

// prepareRetry clears the observation log and the started-service state so a
// second run is evaluated as a fresh operator attempt against whatever durable
// state the previous run legitimately left behind.
func (harness installerHarness) prepareRetry(t *testing.T) {
	t.Helper()
	harness.resetMutations(t)
	entries, err := os.ReadDir(harness.helper)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "active-") {
			removePath(t, filepath.Join(harness.helper, entry.Name()))
		}
	}
	removePath(t, filepath.Join(harness.root, "run/sparerunner-supervisor/supervisor.sock"))
}

// blockUnitStart makes the fake service manager enable a unit without leaving it
// running, which is what systemd reports while a not-initialized Agent restarts.
func (harness installerHarness) blockUnitStart(t *testing.T, unit string) {
	t.Helper()
	writeFile(t, filepath.Join(harness.helper, "units-that-fail-to-start"), unit+"\n")
}

func (harness installerHarness) resetMutations(t *testing.T) {
	t.Helper()
	removePath(t, harness.mutations)
}

func (harness installerHarness) injectMutationFailureAfter(t *testing.T, prefix string) {
	t.Helper()
	if strings.TrimSpace(prefix) == "" {
		t.Fatal("mutation failure prefix must not be empty")
	}
	writeFile(t, filepath.Join(harness.helper, "fail-after-mutation-prefix"), prefix+"\n")
}

func (harness installerHarness) requirePublishedTargetsAbsent(t *testing.T) {
	t.Helper()
	for _, path := range []string{
		filepath.Join(harness.root, "usr/lib/systemd/system", installerAgentUnit),
		filepath.Join(harness.root, "usr/lib/systemd/system", installerSupervisorUnit),
		filepath.Join(harness.root, "usr/lib/sysusers.d/sparerunner.conf"),
		filepath.Join(harness.root, "usr/lib/tmpfiles.d/sparerunner.conf"),
		filepath.Join(harness.root, "var/lib/sparerunner-supervisor", installerMarkerName),
		filepath.Join(harness.helper, "active-"+installerAgentUnit),
		filepath.Join(harness.helper, "active-"+installerSupervisorUnit),
	} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("failed install retained %s: %v", path, err)
		}
	}
}

func (harness installerHarness) mutationLines(t *testing.T) []string {
	t.Helper()
	contents, err := os.ReadFile(harness.mutations)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	raw := strings.TrimSpace(string(contents))
	if raw == "" {
		return nil
	}
	return strings.Split(raw, "\n")
}

func (harness installerHarness) requireNoMutations(t *testing.T) {
	t.Helper()
	if mutations := harness.mutationLines(t); len(mutations) != 0 {
		t.Fatalf("failed preflight mutated host state: %q", mutations)
	}
}

func (harness installerHarness) identity(t *testing.T, database, name string) string {
	t.Helper()
	records := readIdentityDatabase(t, harness.helper, database)
	value, exists := records[name]
	if !exists {
		t.Fatalf("%s database has no %q record", database, name)
	}
	return value
}

func installerEnvironment(harness installerHarness) []string {
	prefixes := []string{
		"SPARERUNNER_LINUX_INSTALL_TEST_",
		"SPARERUNNER_INSTALL_HELPER_",
		"SPARERUNNER_INSTALL_TEST_BINARY=",
	}
	environment := make([]string, 0, len(os.Environ())+6)
	for _, entry := range os.Environ() {
		skip := false
		for _, prefix := range prefixes {
			if strings.HasPrefix(entry, prefix) {
				skip = true
				break
			}
		}
		if !skip {
			environment = append(environment, entry)
		}
	}
	return append(
		environment,
		"SPARERUNNER_LINUX_INSTALL_TESTING=1",
		"SPARERUNNER_LINUX_INSTALL_TEST_ROOT="+harness.root,
		"SPARERUNNER_LINUX_INSTALL_TEST_TOOLS="+harness.tools,
		"SPARERUNNER_INSTALL_TEST_BINARY="+os.Args[0],
		"SPARERUNNER_INSTALL_HELPER_STATE="+harness.helper,
		"SPARERUNNER_INSTALL_HELPER_ROOT="+harness.root,
		"SPARERUNNER_INSTALL_HELPER_KERNEL="+harness.kernel,
	)
}

func requireFileContents(t *testing.T, path, expected string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != expected {
		t.Fatalf("%s = %q, want %q", path, contents, expected)
	}
}

func requireSameContents(t *testing.T, path, source string) {
	t.Helper()
	expected, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	requireFileContents(t, path, string(expected))
}

func requireMutation(t *testing.T, mutations []string, prefix string) {
	t.Helper()
	for _, mutation := range mutations {
		if strings.HasPrefix(mutation, prefix) {
			return
		}
	}
	t.Fatalf("mutation log lacks %q: %q", prefix, mutations)
}

func requireStatOverride(t *testing.T, helperRoot, path, expected string) {
	t.Helper()
	overrides := readStatOverrides(t, helperRoot)
	if overrides[path] != expected {
		t.Fatalf("%s contract = %q, want %q", path, overrides[path], expected)
	}
}

// shortTemporaryDirectory keeps the harness root short enough for a real
// AF_UNIX path. The default temporary directory on macOS already consumes most
// of the 104-byte sockaddr_un limit, which would make the Supervisor socket
// impossible to create in the fake service surface.
func shortTemporaryDirectory(t *testing.T) string {
	t.Helper()
	working, err := os.MkdirTemp("/tmp", "sparerunner-install")
	if err != nil {
		return t.TempDir()
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(working); err != nil {
			t.Errorf("cannot remove %s: %v", working, err)
		}
	})
	return working
}

func makeDirectory(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(path, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	// WriteFile applies the process umask. Preserve the fixture's declared mode
	// so hardened environments exercise content checks instead of mode checks.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
}

func removePath(t *testing.T, path string) {
	t.Helper()
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
}

func readStatOverrides(t *testing.T, helperRoot string) map[string]string {
	t.Helper()
	overrides, err := loadJSONMap(filepath.Join(helperRoot, "stat-overrides.json"))
	if err != nil {
		t.Fatal(err)
	}
	return overrides
}

func setStatOverride(t *testing.T, helperRoot, path, contract string) {
	t.Helper()
	overrides := readStatOverrides(t, helperRoot)
	overrides[path] = contract
	if err := storeJSONMap(filepath.Join(helperRoot, "stat-overrides.json"), overrides); err != nil {
		t.Fatal(err)
	}
}

func readIdentityDatabase(t *testing.T, helperRoot, database string) map[string]string {
	t.Helper()
	records, err := loadJSONMap(filepath.Join(helperRoot, database+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func writeIdentity(t *testing.T, helperRoot, database, name, id string) {
	t.Helper()
	records := readIdentityDatabase(t, helperRoot, database)
	records[name] = id
	if err := storeJSONMap(filepath.Join(helperRoot, database+".json"), records); err != nil {
		t.Fatal(err)
	}
}

func loadJSONMap(path string) (map[string]string, error) {
	values := make(map[string]string)
	encoded, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return values, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(encoded, &values); err != nil {
		return nil, err
	}
	return values, nil
}

func storeJSONMap(path string, values map[string]string) error {
	encoded, err := json.Marshal(values)
	if err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0o600)
}

// TestLinuxInstallerHelperProcess implements the fixed Linux command surface
// used by install-service.sh. It runs only in subprocesses spawned above.
func TestLinuxInstallerHelperProcess(t *testing.T) {
	if os.Getenv("SPARERUNNER_INSTALL_HELPER_PROCESS") != "1" {
		return
	}
	tool := os.Getenv("SPARERUNNER_INSTALL_HELPER_TOOL")
	state := os.Getenv("SPARERUNNER_INSTALL_HELPER_STATE")
	args := helperArguments(os.Args)
	os.Exit(runInstallerHelper(tool, state, args, os.Stdout, os.Stderr))
}

func helperArguments(arguments []string) []string {
	for index, argument := range arguments {
		if argument == "--" {
			return arguments[index+1:]
		}
	}
	return nil
}

func runInstallerHelper(
	tool string,
	state string,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
) int {
	switch tool {
	case "id":
		uid := os.Getenv("SPARERUNNER_INSTALL_HELPER_UID")
		if uid == "" {
			uid = "0"
		}
		fmt.Fprintln(stdout, uid)
		return 0
	case "sleep":
		return 0
	case "uname":
		if len(args) != 1 || args[0] != "-r" {
			return 2
		}
		fmt.Fprintln(stdout, os.Getenv("SPARERUNNER_INSTALL_HELPER_KERNEL"))
		return 0
	case "stat":
		return fakeStat(state, args, stdout, stderr)
	case "getent":
		return fakeGetent(state, args, stdout, stderr)
	case "systemctl":
		return fakeSystemctl(state, args, stderr)
	case "loginctl":
		return fakeLoginctl(state, args, stdout, stderr)
	case "systemd-sysusers":
		return fakeSysusers(state, args, stderr)
	case "systemd-tmpfiles":
		return fakeTmpfiles(state, args, stderr)
	case "install":
		return fakeInstall(state, args, stderr)
	case "ln":
		return fakeLink(state, args, stderr)
	case "mkdir":
		return fakeMkdir(state, args, stderr)
	case "rm":
		return fakeRemove(state, args, stderr)
	case "cmp":
		return fakeCompare(args, stderr)
	default:
		fmt.Fprintf(stderr, "unsupported fake installer tool %q\n", tool)
		return 2
	}
}

func fakeStat(state string, args []string, stdout, stderr io.Writer) int {
	if len(args) != 3 || args[0] != "-c" {
		fmt.Fprintf(stderr, "unexpected stat arguments: %q\n", args)
		return 2
	}
	info, err := os.Lstat(args[2])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	switch args[1] {
	case "%u:%g:%04a":
		overrides, err := loadJSONMap(filepath.Join(state, "stat-overrides.json"))
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if override, exists := overrides[args[2]]; exists {
			fmt.Fprintln(stdout, override)
			return 0
		}
		permissions := uint32(info.Mode().Perm())
		if info.Mode()&os.ModeSticky != 0 {
			permissions |= 0o1000
		}
		if info.Mode()&os.ModeSetgid != 0 {
			permissions |= 0o2000
		}
		if info.Mode()&os.ModeSetuid != 0 {
			permissions |= 0o4000
		}
		// The unprivileged harness cannot own files as root, so the fake reports
		// the contract a correctly packaged host would have unless a test
		// deliberately overrides one exact path.
		fmt.Fprintf(stdout, "0:0:%04o\n", permissions)
	case "%h":
		fmt.Fprintln(stdout, "1")
	default:
		fmt.Fprintf(stderr, "unexpected stat format: %q\n", args[1])
		return 2
	}
	return 0
}

func fakeGetent(state string, args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 {
		fmt.Fprintf(stderr, "unexpected getent arguments: %q\n", args)
		return 2
	}
	var database string
	switch args[0] {
	case "passwd", "group":
		database = args[0]
	default:
		return 2
	}
	records, err := loadJSONMap(filepath.Join(state, database+".json"))
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	id, exists := records[args[1]]
	if !exists {
		return 2
	}
	if database == "passwd" {
		fmt.Fprintf(stdout, "%s:x:%s:%s::/nonexistent:/usr/sbin/nologin\n", args[1], id, id)
		return 0
	}
	fmt.Fprintf(stdout, "%s:x:%s:\n", args[1], id)
	return 0
}

func fakeSystemctl(state string, args []string, stderr io.Writer) int {
	recorded := "systemctl " + strings.Join(args, " ")
	if len(args) > 0 && args[0] == "--user" {
		args = args[1:]
	}
	if len(args) == 1 && args[0] == "show-environment" {
		return 0
	}
	if len(args) == 3 && args[0] == "is-active" && args[1] == "--quiet" {
		if _, err := os.Stat(filepath.Join(state, "active-"+args[2])); err == nil {
			return 0
		}
		return 3
	}
	if len(args) == 1 && args[0] == "daemon-reload" {
		return recordMutation(state, recorded, nil)
	}
	if len(args) >= 3 && args[0] == "enable" && args[1] == "--now" {
		units := args[2:]
		return recordMutation(
			state,
			recorded,
			func() error { return activateUnits(state, units) },
		)
	}
	if len(args) >= 3 && args[0] == "disable" && args[1] == "--now" {
		units := args[2:]
		return recordMutation(
			state,
			recorded,
			func() error { return deactivateUnits(state, units) },
		)
	}
	if len(args) >= 2 && args[0] == "start" {
		units := args[1:]
		return recordMutation(
			state,
			recorded,
			func() error { return activateUnits(state, units) },
		)
	}
	if len(args) >= 2 && args[0] == "stop" {
		units := args[1:]
		return recordMutation(
			state,
			recorded,
			func() error { return deactivateUnits(state, units) },
		)
	}
	fmt.Fprintf(stderr, "unexpected systemctl arguments: %q\n", args)
	return 2
}

func activateUnits(state string, units []string) error {
	blocked, err := os.ReadFile(filepath.Join(state, "units-that-fail-to-start"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, unit := range units {
		if strings.Contains(string(blocked), unit) {
			continue
		}
		if err := os.WriteFile(
			filepath.Join(state, "active-"+unit),
			[]byte("active\n"),
			0o600,
		); err != nil {
			return err
		}
	}
	return publishSupervisorSocket(state)
}

func deactivateUnits(state string, units []string) error {
	for _, unit := range units {
		if err := os.Remove(filepath.Join(state, "active-"+unit)); err != nil &&
			!os.IsNotExist(err) {
			return err
		}
	}
	socket := filepath.Join(
		os.Getenv("SPARERUNNER_INSTALL_HELPER_ROOT"),
		"run/sparerunner-supervisor/supervisor.sock",
	)
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// publishSupervisorSocket creates the real peer-authenticated socket file the
// started Supervisor would create, so the installer's post-start verification
// exercises an actual socket rather than a placeholder regular file.
func publishSupervisorSocket(state string) error {
	root := os.Getenv("SPARERUNNER_INSTALL_HELPER_ROOT")
	path := filepath.Join(root, "run/sparerunner-supervisor/supervisor.sock")
	if _, err := os.Lstat(filepath.Dir(path)); err != nil {
		// The user-service harness has no Supervisor socket directory; only the
		// root-service harness models the privileged socket contract.
		return nil
	}
	if _, err := os.Lstat(path); err == nil {
		return nil
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		return listener.Close()
	}
	unixListener.SetUnlinkOnClose(false)
	if err := unixListener.Close(); err != nil {
		return err
	}
	records, err := loadJSONMap(filepath.Join(state, "group.json"))
	if err != nil {
		return err
	}
	overrides, err := loadJSONMap(filepath.Join(state, "stat-overrides.json"))
	if err != nil {
		return err
	}
	overrides[path] = "0:" + records["sparerunner-agent"] + ":0660"
	return storeJSONMap(filepath.Join(state, "stat-overrides.json"), overrides)
}

func fakeSysusers(state string, args []string, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintf(stderr, "unexpected systemd-sysusers arguments: %q\n", args)
		return 2
	}
	return recordMutation(
		state,
		"systemd-sysusers "+args[0],
		func() error { return declareIdentities(state, args[0]) },
	)
}

func declareIdentities(state, definition string) error {
	contents, err := os.ReadFile(definition)
	if err != nil {
		return err
	}
	passwd, err := loadJSONMap(filepath.Join(state, "passwd.json"))
	if err != nil {
		return err
	}
	group, err := loadJSONMap(filepath.Join(state, "group.json"))
	if err != nil {
		return err
	}
	next := 900 + len(passwd)
	for _, line := range strings.Split(string(contents), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "u" {
			continue
		}
		if _, exists := passwd[fields[1]]; exists {
			continue
		}
		identifier := strconv.Itoa(next)
		next++
		passwd[fields[1]] = identifier
		group[fields[1]] = identifier
	}
	if err := storeJSONMap(filepath.Join(state, "passwd.json"), passwd); err != nil {
		return err
	}
	return storeJSONMap(filepath.Join(state, "group.json"), group)
}

func fakeTmpfiles(state string, args []string, stderr io.Writer) int {
	if len(args) != 2 || args[0] != "--create" {
		fmt.Fprintf(stderr, "unexpected systemd-tmpfiles arguments: %q\n", args)
		return 2
	}
	return recordMutation(
		state,
		"systemd-tmpfiles --create "+args[1],
		func() error { return declareDirectories(state, args[1]) },
	)
}

func declareDirectories(state, definition string) error {
	contents, err := os.ReadFile(definition)
	if err != nil {
		return err
	}
	root := os.Getenv("SPARERUNNER_INSTALL_HELPER_ROOT")
	passwd, err := loadJSONMap(filepath.Join(state, "passwd.json"))
	if err != nil {
		return err
	}
	group, err := loadJSONMap(filepath.Join(state, "group.json"))
	if err != nil {
		return err
	}
	overrides, err := loadJSONMap(filepath.Join(state, "stat-overrides.json"))
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(contents), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 6 || fields[0] != "d" {
			continue
		}
		path := filepath.Join(root, fields[1])
		mode, err := strconv.ParseUint(fields[2], 8, 32)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(path, os.FileMode(mode)); err != nil {
			return err
		}
		if err := os.Chmod(path, os.FileMode(mode)); err != nil {
			return err
		}
		uid := passwd[fields[3]]
		if fields[3] == "root" {
			uid = "0"
		}
		gid := group[fields[4]]
		if fields[4] == "root" {
			gid = "0"
		}
		overrides[path] = fmt.Sprintf("%s:%s:%s", uid, gid, fields[2])
	}
	return storeJSONMap(filepath.Join(state, "stat-overrides.json"), overrides)
}

func fakeInstall(state string, args []string, stderr io.Writer) int {
	var modeField int
	switch {
	case len(args) == 8 && args[0] == "-o" && args[2] == "-g" && args[4] == "-m":
		modeField = 5
	case len(args) == 4 && args[0] == "-m":
		modeField = 1
	default:
		fmt.Fprintf(stderr, "unexpected install arguments: %q\n", args)
		return 2
	}
	mode, err := strconv.ParseUint(args[modeField], 8, 32)
	if err != nil {
		return 2
	}
	args = args[modeField+1:]
	args = append([]string{"-src-dst"}, args...)
	return recordMutation(state, "install "+strings.Join(args[1:], " "), func() error {
		source, err := os.Open(args[1])
		if err != nil {
			return err
		}
		defer source.Close()
		target, err := os.OpenFile(
			args[2],
			os.O_CREATE|os.O_EXCL|os.O_WRONLY,
			os.FileMode(mode),
		)
		if err != nil {
			return err
		}
		if _, err := io.Copy(target, source); err != nil {
			target.Close()
			return err
		}
		if err := target.Close(); err != nil {
			return err
		}
		return os.Chmod(args[2], os.FileMode(mode))
	})
}

func fakeLink(state string, args []string, stderr io.Writer) int {
	if len(args) != 2 {
		fmt.Fprintf(stderr, "unexpected ln arguments: %q\n", args)
		return 2
	}
	return recordMutation(state, "ln "+strings.Join(args, " "), func() error {
		return os.Link(args[0], args[1])
	})
}

func fakeRemove(state string, args []string, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintf(stderr, "unexpected rm arguments: %q\n", args)
		return 2
	}
	return recordMutation(state, "rm "+args[0], func() error {
		return os.Remove(args[0])
	})
}

func fakeCompare(args []string, stderr io.Writer) int {
	if len(args) != 3 || args[0] != "-s" {
		fmt.Fprintf(stderr, "unexpected cmp arguments: %q\n", args)
		return 2
	}
	left, err := os.ReadFile(args[1])
	if err != nil {
		return 1
	}
	right, err := os.ReadFile(args[2])
	if err != nil {
		return 1
	}
	if !bytes.Equal(left, right) {
		return 1
	}
	return 0
}

func recordMutation(state, mutation string, apply func() error) int {
	if failInjectedMutation(state, mutation) {
		return 1
	}
	if err := appendMutation(state, mutation); err != nil {
		return 1
	}
	if apply != nil {
		if err := apply(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	if failAfterInjectedMutation(state, mutation) {
		return 1
	}
	return 0
}

func appendMutation(state, mutation string) error {
	if err := os.MkdirAll(state, 0o700); err != nil {
		return err
	}
	log, err := os.OpenFile(
		filepath.Join(state, "mutations.log"),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0o600,
	)
	if err != nil {
		return err
	}
	defer log.Close()
	_, err = fmt.Fprintln(log, mutation)
	return err
}

func failInjectedMutation(state, mutation string) bool {
	path := filepath.Join(state, "fail-mutation-prefix")
	encoded, err := os.ReadFile(path)
	if err != nil || !strings.HasPrefix(mutation, strings.TrimSpace(string(encoded))) {
		return false
	}
	_ = os.Remove(path)
	return true
}

func failAfterInjectedMutation(state, mutation string) bool {
	path := filepath.Join(state, "fail-after-mutation-prefix")
	encoded, err := os.ReadFile(path)
	if err != nil || !strings.HasPrefix(mutation, strings.TrimSpace(string(encoded))) {
		return false
	}
	_ = os.Remove(path)
	return true
}

func TestUninstallServiceRemovesOnlyPackageFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the Linux shell installer harness requires /bin/bash")
	}
	if os.Geteuid() == 0 {
		t.Skip("production root intentionally rejects installer test indirection")
	}

	t.Run("installed package is removed and node state is retained", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.resetMutations(t)

		harness.runUninstall(t, true)
		harness.requirePublishedTargetsAbsentExceptMarker(t)
		requireFileContents(
			t,
			filepath.Join(harness.root, "var/lib/sparerunner-supervisor", installerMarkerName),
			"version=1\nrole=supervisor-state\npath=/var/lib/sparerunner-supervisor\n",
		)
		requireDirectoryPresent(t, filepath.Join(harness.root, "var/lib/sparerunner-agent"))
		requireDirectoryPresent(t, filepath.Join(harness.root, "var/cache/sparerunner-agent"))
		mutations := harness.mutationLines(t)
		requireMutation(t, mutations, "systemctl disable --now ")
		requireMutation(t, mutations, "systemctl daemon-reload")
	})

	t.Run("locally modified unit is never discarded", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.resetMutations(t)
		unit := filepath.Join(harness.root, "usr/lib/systemd/system", installerSupervisorUnit)
		writeFile(t, unit, "[Service]\nExecStart=/bin/false\n")

		output := harness.runUninstall(t, false)
		if !strings.Contains(output, "differs from this package and is not removed") {
			t.Fatalf("uninstall rejection lacks the modified-unit reason:\n%s", output)
		}
		harness.requireNoMutations(t)
		requireFileContents(t, unit, "[Service]\nExecStart=/bin/false\n")
		requireDirectoryPresent(
			t,
			filepath.Join(harness.root, "usr/lib/systemd/system"),
		)
	})

	t.Run("clean host is an idempotent no-op", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)

		output := harness.runUninstall(t, true)
		if !strings.Contains(output, "no SpareRunner package file from this package is installed") {
			t.Fatalf("clean-host uninstall output = %q", output)
		}
		harness.requireNoMutations(t)
	})

	t.Run("reinstall after uninstall succeeds", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.runUninstall(t, true)
		harness.prepareRetry(t)

		harness.run(t, true)
		mutations := harness.mutationLines(t)
		requireMutation(t, mutations, "systemctl enable --now ")
	})
}

func (harness installerHarness) runUninstall(t *testing.T, wantSuccess bool) string {
	t.Helper()
	script := filepath.Join(filepath.Dir(harness.script), "uninstall-service.sh")
	command := exec.Command("/bin/bash", script, harness.source)
	command.Env = installerEnvironment(harness)
	output, err := command.CombinedOutput()
	if wantSuccess && err != nil {
		t.Fatalf("uninstall failed: %v\n%s", err, output)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("uninstall unexpectedly succeeded:\n%s", output)
	}
	return string(output)
}

func (harness installerHarness) requirePublishedTargetsAbsentExceptMarker(t *testing.T) {
	t.Helper()
	for _, path := range []string{
		filepath.Join(harness.root, "usr/lib/systemd/system", installerAgentUnit),
		filepath.Join(harness.root, "usr/lib/systemd/system", installerSupervisorUnit),
		filepath.Join(harness.root, "usr/lib/sysusers.d/sparerunner.conf"),
		filepath.Join(harness.root, "usr/lib/tmpfiles.d/sparerunner.conf"),
		filepath.Join(harness.helper, "active-"+installerAgentUnit),
		filepath.Join(harness.helper, "active-"+installerSupervisorUnit),
	} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("uninstall retained %s: %v", path, err)
		}
	}
}

func requireDirectoryPresent(t *testing.T, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("expected directory %s: %v", path, err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", path)
	}
}

// TestUserAgentUnitStaysUnprivilegedAndDelegated pins the sudo-free service to
// its contract: the shared-identity mode with delegation, and none of the
// privileged Supervisor's boundary flags, which the user manager could not
// honor anyway.
func TestUserAgentUnitStaysUnprivilegedAndDelegated(t *testing.T) {
	unit := readRepositoryPackagingFile(t, "systemd/user/sparerunner-agent.service")
	for _, required := range []string{
		"--allow-shared-runner-identity",
		"--require-native-runner",
		"Delegate=yes",
		"NoNewPrivileges=yes",
		"KillMode=control-group",
		"WantedBy=default.target",
		"ExecStart=%h/.local/bin/sparerunner-agent serve",
	} {
		if !strings.Contains(unit, required) {
			t.Fatalf("user unit lacks %q", required)
		}
	}
	for _, forbidden := range []string{
		// User= and Group= are system-unit directives; a user unit carrying them
		// would be claiming an identity boundary it does not have.
		"User=",
		"Group=",
		"--supervisor-socket",
		"--runner-user",
		// The join hint recognizes the packaged service by the default state
		// directory, so an explicit one here would break that contract.
		"--state-dir",
		"/var/lib/",
		"Environment=",
		"jitconfig",
	} {
		if strings.Contains(unit, forbidden) {
			t.Fatalf("user unit contains forbidden directive %q", forbidden)
		}
	}
}

func readRepositoryPackagingFile(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

// fakeLoginctl serves the read-only linger query the user installer makes. The
// linger state lives in a helper file so tests can flip it per scenario.
func fakeLoginctl(state string, args []string, stdout, stderr io.Writer) int {
	if len(args) != 4 || args[0] != "show-user" ||
		args[1] != "--property=Linger" || args[2] != "--value" {
		fmt.Fprintf(stderr, "unexpected loginctl arguments: %q\n", args)
		return 2
	}
	value, err := os.ReadFile(filepath.Join(state, "linger"))
	if err != nil {
		fmt.Fprintln(stdout, "no")
		return 0
	}
	fmt.Fprintln(stdout, strings.TrimSpace(string(value)))
	return 0
}

type userInstallerHarness struct {
	root      string
	home      string
	tools     string
	helper    string
	mutations string
	script    string
	source    string
	kernel    string
	uid       string
}

func newUserInstallerHarness(t *testing.T) userInstallerHarness {
	t.Helper()
	// The fake HOME must sit under a chain with no group/world-writable
	// component, because the installer verifies the binary's real ancestor
	// modes. The system temporary directory is sticky world-writable, so the
	// harness anchors in the package directory instead.
	working := packageTemporaryDirectory(t)
	root := filepath.Join(working, "root")
	home := filepath.Join(root, "home/tester")
	tools := filepath.Join(working, "tools")
	helper := filepath.Join(working, "helper")
	for _, directory := range []string{
		root,
		tools,
		helper,
		filepath.Join(home, ".local/bin"),
		filepath.Join(root, "sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service"),
		filepath.Join(root, "var/lib/systemd/linger"),
	} {
		makeDirectory(t, directory, 0o755)
	}
	writeFile(t, filepath.Join(root, "sys/fs/cgroup/cgroup.controllers"), "cpu memory pids\n")
	writeFile(
		t,
		filepath.Join(root, "sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.controllers"),
		"memory pids\n",
	)
	writeFile(t, filepath.Join(root, ".sparerunner-installer-test-root"), "test-only\n")
	binary := filepath.Join(home, ".local/bin/sparerunner-agent")
	writeFile(t, binary, "#!/bin/sh\nexit 0\n")
	if err := os.Chmod(binary, 0o755); err != nil {
		t.Fatal(err)
	}
	wrapper := []byte(`#!/bin/sh
export SPARERUNNER_INSTALL_HELPER_PROCESS=1
export SPARERUNNER_INSTALL_HELPER_TOOL="${0##*/}"
export GORACE="atexit_sleep_ms=0"
exec "$SPARERUNNER_INSTALL_TEST_BINARY" -test.run=TestLinuxInstallerHelperProcess -- "$@"
`)
	for _, tool := range []string{
		"cmp", "id", "install", "ln", "loginctl", "mkdir", "rm", "stat", "systemctl", "uname",
	} {
		path := filepath.Join(tools, tool)
		if err := os.WriteFile(path, wrapper, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	script, err := filepath.Abs("install-user-service.sh")
	if err != nil {
		t.Fatal(err)
	}
	source, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	return userInstallerHarness{
		root:      root,
		home:      home,
		tools:     tools,
		helper:    helper,
		mutations: filepath.Join(helper, "mutations.log"),
		script:    script,
		source:    source,
		kernel:    installerDefaultKernel,
		uid:       "1000",
	}
}

func (harness userInstallerHarness) environment() []string {
	prefixes := []string{
		"SPARERUNNER_LINUX_INSTALL_TEST_",
		"SPARERUNNER_INSTALL_HELPER_",
		"SPARERUNNER_INSTALL_TEST_BINARY=",
		"HOME=",
		"XDG_CONFIG_HOME=",
		"USER=",
	}
	environment := make([]string, 0, len(os.Environ())+8)
	for _, entry := range os.Environ() {
		skip := false
		for _, prefix := range prefixes {
			if strings.HasPrefix(entry, prefix) {
				skip = true
				break
			}
		}
		if !skip {
			environment = append(environment, entry)
		}
	}
	return append(
		environment,
		"SPARERUNNER_LINUX_INSTALL_TESTING=1",
		"SPARERUNNER_LINUX_INSTALL_TEST_ROOT="+harness.root,
		"SPARERUNNER_LINUX_INSTALL_TEST_TOOLS="+harness.tools,
		"SPARERUNNER_INSTALL_TEST_BINARY="+os.Args[0],
		"SPARERUNNER_INSTALL_HELPER_STATE="+harness.helper,
		"SPARERUNNER_INSTALL_HELPER_ROOT="+harness.root,
		"SPARERUNNER_INSTALL_HELPER_KERNEL="+harness.kernel,
		"SPARERUNNER_INSTALL_HELPER_UID="+harness.uid,
		"HOME="+harness.home,
		"USER=tester",
	)
}

func (harness userInstallerHarness) runScript(t *testing.T, script string, wantSuccess bool) string {
	t.Helper()
	command := exec.Command("/bin/bash", script, harness.source)
	command.Env = harness.environment()
	output, err := command.CombinedOutput()
	if wantSuccess && err != nil {
		t.Fatalf("user installer failed: %v\n%s", err, output)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("user installer unexpectedly succeeded:\n%s", output)
	}
	return string(output)
}

func (harness userInstallerHarness) run(t *testing.T, wantSuccess bool) string {
	t.Helper()
	return harness.runScript(t, harness.script, wantSuccess)
}

func (harness userInstallerHarness) runUninstall(t *testing.T, wantSuccess bool) string {
	t.Helper()
	script := filepath.Join(filepath.Dir(harness.script), "uninstall-user-service.sh")
	return harness.runScript(t, script, wantSuccess)
}

func (harness userInstallerHarness) runRejecting(t *testing.T, reason string) {
	t.Helper()
	output := harness.run(t, false)
	if !strings.Contains(output, reason) {
		t.Fatalf("user installer rejection lacks %q:\n%s", reason, output)
	}
}

func (harness userInstallerHarness) mutationLines(t *testing.T) []string {
	t.Helper()
	contents, err := os.ReadFile(harness.mutations)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	raw := strings.TrimSpace(string(contents))
	if raw == "" {
		return nil
	}
	return strings.Split(raw, "\n")
}

func (harness userInstallerHarness) requireNoMutations(t *testing.T) {
	t.Helper()
	if mutations := harness.mutationLines(t); len(mutations) != 0 {
		t.Fatalf("failed preflight mutated host state: %q", mutations)
	}
}

func (harness userInstallerHarness) prepareRetry(t *testing.T) {
	t.Helper()
	removePath(t, harness.mutations)
	entries, err := os.ReadDir(harness.helper)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "active-") {
			removePath(t, filepath.Join(harness.helper, entry.Name()))
		}
	}
}

func (harness userInstallerHarness) unitTarget() string {
	return filepath.Join(harness.home, ".config/systemd/user", installerAgentUnit)
}

func TestInstallUserServiceInstallsWithoutRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the Linux shell installer harness requires /bin/bash")
	}
	if os.Geteuid() == 0 {
		t.Skip("the user installer intentionally refuses root")
	}

	t.Run("clean install publishes the unit and reports pending enrollment", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)

		output := harness.run(t, true)
		requireSameContents(
			t,
			harness.unitTarget(),
			filepath.Join(harness.source, "systemd/user", installerAgentUnit),
		)
		mutations := harness.mutationLines(t)
		requireMutation(t, mutations, "systemctl --user daemon-reload")
		requireMutation(t, mutations, "systemctl --user enable --now ")
		for _, expected := range []string{
			"stays not-initialized until this node is enrolled",
			"sprun join",
			"systemctl --user restart",
			"loginctl enable-linger",
		} {
			if !strings.Contains(output, expected) {
				t.Fatalf("install output lacks %q:\n%s", expected, output)
			}
		}
	})

	t.Run("enabled linger omits the linger note", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		writeFile(t, filepath.Join(harness.helper, "linger"), "yes\n")

		output := harness.run(t, true)
		if strings.Contains(output, "loginctl enable-linger") {
			t.Fatalf("install output suggests linger although it is enabled:\n%s", output)
		}
	})

	t.Run("owned retry publishes no file again", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		harness.run(t, true)
		harness.prepareRetry(t)

		harness.run(t, true)
		for _, mutation := range harness.mutationLines(t) {
			if strings.HasPrefix(mutation, "install ") || strings.HasPrefix(mutation, "ln ") {
				t.Fatalf("owned retry republished the unit: %q", harness.mutationLines(t))
			}
		}
	})

	t.Run("running service is never reinstalled over", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		harness.run(t, true)
		removePath(t, harness.mutations)

		harness.runRejecting(t, "already running for this user")
		harness.requireNoMutations(t)
	})

	t.Run("enrolled node with a dead agent fails the install", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		harness.run(t, true)
		makeDirectory(t, filepath.Join(harness.home, ".config/sparerunner/agent"), 0o700)
		writeFile(
			t,
			filepath.Join(harness.home, ".config/sparerunner/agent/node.json"),
			"{\"nodeId\":\"durable\"}\n",
		)
		harness.prepareRetry(t)
		writeFile(
			t,
			filepath.Join(harness.helper, "units-that-fail-to-start"),
			installerAgentUnit+"\n",
		)

		harness.runRejecting(t, "did not start with existing node state")
	})

	for _, unsupported := range []struct {
		name    string
		reason  string
		prepare func(*testing.T, userInstallerHarness) userInstallerHarness
	}{
		{
			name:   "root invocation",
			reason: "must not run as root",
			prepare: func(_ *testing.T, harness userInstallerHarness) userInstallerHarness {
				harness.uid = "0"
				return harness
			},
		},
		{
			name:   "kernel older than delegated cgroup.kill",
			reason: "requires Linux 5.14 or newer",
			prepare: func(_ *testing.T, harness userInstallerHarness) userInstallerHarness {
				harness.kernel = "5.10.0-30-generic"
				return harness
			},
		},
		{
			name:   "cgroup v1 hierarchy",
			reason: "unified cgroup v2 hierarchy is not mounted",
			prepare: func(t *testing.T, harness userInstallerHarness) userInstallerHarness {
				removePath(t, filepath.Join(harness.root, "sys/fs/cgroup/cgroup.controllers"))
				return harness
			},
		},
		{
			name:   "missing delegated user subtree",
			reason: "no delegated systemd user subtree",
			prepare: func(t *testing.T, harness userInstallerHarness) userInstallerHarness {
				removePath(t, filepath.Join(
					harness.root,
					"sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.controllers",
				))
				return harness
			},
		},
		{
			name:   "missing agent binary",
			reason: "install the agent binary at",
			prepare: func(t *testing.T, harness userInstallerHarness) userInstallerHarness {
				removePath(t, filepath.Join(harness.home, ".local/bin/sparerunner-agent"))
				return harness
			},
		},
		{
			name:   "group-writable agent binary",
			reason: "not owner-held and write-safe",
			prepare: func(t *testing.T, harness userInstallerHarness) userInstallerHarness {
				setStatOverride(
					t,
					harness.helper,
					filepath.Join(harness.home, ".local/bin/sparerunner-agent"),
					"1000:1000:0775",
				)
				return harness
			},
		},
		{
			name:   "foreign-owned binary ancestor",
			reason: "not owner-held and write-safe",
			prepare: func(t *testing.T, harness userInstallerHarness) userInstallerHarness {
				setStatOverride(
					t,
					harness.helper,
					filepath.Join(harness.home, ".local"),
					"1001:1001:0755",
				)
				return harness
			},
		},
		{
			name:   "foreign unit file",
			reason: "differs from this package",
			prepare: func(t *testing.T, harness userInstallerHarness) userInstallerHarness {
				makeDirectory(t, filepath.Dir(harness.unitTarget()), 0o755)
				writeFile(t, harness.unitTarget(), "[Service]\nExecStart=/bin/false\n")
				return harness
			},
		},
	} {
		t.Run("preflight rejects "+unsupported.name, func(t *testing.T) {
			t.Parallel()
			harness := unsupported.prepare(t, newUserInstallerHarness(t))

			harness.runRejecting(t, unsupported.reason)
			harness.requireNoMutations(t)
		})
	}

	t.Run("verified rollback converges after activation failure", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		writeFile(
			t,
			filepath.Join(harness.helper, "fail-after-mutation-prefix"),
			"systemctl --user enable --now \n",
		)

		harness.run(t, false)
		if _, err := os.Lstat(harness.unitTarget()); !os.IsNotExist(err) {
			t.Fatalf("failed install retained the unit: %v", err)
		}
		harness.prepareRetry(t)

		harness.run(t, true)
		requireSameContents(
			t,
			harness.unitTarget(),
			filepath.Join(harness.source, "systemd/user", installerAgentUnit),
		)
	})
}

func TestUninstallUserServiceRemovesOnlyTheUnit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the Linux shell installer harness requires /bin/bash")
	}
	if os.Geteuid() == 0 {
		t.Skip("the user installer intentionally refuses root")
	}

	t.Run("installed unit is removed and node state is retained", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		harness.run(t, true)
		state := filepath.Join(harness.home, ".config/sparerunner/agent")
		makeDirectory(t, state, 0o700)
		writeFile(t, filepath.Join(state, "node.json"), "{\"nodeId\":\"durable\"}\n")
		removePath(t, harness.mutations)

		output := harness.runUninstall(t, true)
		if _, err := os.Lstat(harness.unitTarget()); !os.IsNotExist(err) {
			t.Fatalf("uninstall retained the unit: %v", err)
		}
		requireDirectoryPresent(t, state)
		if !strings.Contains(output, "retained node state") {
			t.Fatalf("uninstall output lacks the retention notice:\n%s", output)
		}
		mutations := harness.mutationLines(t)
		requireMutation(t, mutations, "systemctl --user disable --now ")
		requireMutation(t, mutations, "systemctl --user daemon-reload")
	})

	t.Run("locally modified unit is never discarded", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)
		harness.run(t, true)
		writeFile(t, harness.unitTarget(), "[Service]\nExecStart=/bin/false\n")
		removePath(t, harness.mutations)

		output := harness.runUninstall(t, false)
		if !strings.Contains(output, "differs from this package and is not removed") {
			t.Fatalf("uninstall rejection lacks the modified-unit reason:\n%s", output)
		}
		harness.requireNoMutations(t)
	})

	t.Run("clean home is an idempotent no-op", func(t *testing.T) {
		t.Parallel()
		harness := newUserInstallerHarness(t)

		output := harness.runUninstall(t, true)
		if !strings.Contains(output, "no SpareRunner user service from this package is installed") {
			t.Fatalf("clean-home uninstall output = %q", output)
		}
		harness.requireNoMutations(t)
	})
}

func packageTemporaryDirectory(t *testing.T) string {
	t.Helper()
	created, err := os.MkdirTemp(".", "user-harness-")
	if err != nil {
		t.Fatal(err)
	}
	working, err := filepath.Abs(created)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(working); err != nil {
			t.Errorf("cannot remove %s: %v", working, err)
		}
	})
	return working
}

func fakeMkdir(state string, args []string, stderr io.Writer) int {
	if len(args) != 3 || args[0] != "-m" {
		fmt.Fprintf(stderr, "unexpected mkdir arguments: %q\n", args)
		return 2
	}
	mode, err := strconv.ParseUint(args[1], 8, 32)
	if err != nil {
		return 2
	}
	return recordMutation(state, "mkdir "+strings.Join(args, " "), func() error {
		if err := os.Mkdir(args[2], os.FileMode(mode)); err != nil {
			return err
		}
		return os.Chmod(args[2], os.FileMode(mode))
	})
}
