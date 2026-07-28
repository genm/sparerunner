package macospackaging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const installerTestID = "11111111-2222-4333-8444-555555555555"

type installerHarness struct {
	root      string
	tools     string
	helper    string
	mutations string
	script    string
	plist     string
}

func TestInstallServiceAcceptsOnlyCleanOwnedState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the macOS shell installer harness requires /bin/bash")
	}
	if output, err := exec.Command("/usr/bin/id", "-u").Output(); err == nil &&
		strings.TrimSpace(string(output)) == "0" {
		t.Skip("production root intentionally rejects installer test indirection")
	}

	t.Run("clean initial layout", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)

		stateMarker := filepath.Join(
			harness.root,
			"Library/Application Support/SpareRunner/.sparerunner-install-ownership-v1",
		)
		cacheMarker := filepath.Join(
			harness.root,
			"Library/Caches/com.genm.sparerunner/.sparerunner-install-ownership-v1",
		)
		requireFileContents(t, stateMarker, markerFixture(
			installerTestID,
			"agent-state",
			"/Library/Application Support/SpareRunner",
		))
		requireFileContents(t, cacheMarker, markerFixture(
			installerTestID,
			"agent-cache",
			"/Library/Caches/com.genm.sparerunner",
		))
		requireFileContents(t, filepath.Join(
			harness.helper,
			"dscl/Groups/sparerunner-runner-0/RealName",
		), "SpareRunner native runner slot 0 ["+installerTestID+"]")
		requireFileContents(t, filepath.Join(
			harness.helper,
			"dscl/Users/sparerunner-runner-0/UserShell",
		), "/usr/bin/false")

		mutations := harness.mutationLines(t)
		requireMutation(t, mutations, "install ")
		requireMutation(t, mutations, "launchctl bootstrap ")
		if strings.Contains(strings.Join(mutations, "\n"), "chown") ||
			strings.Contains(strings.Join(mutations, "\n"), "chmod") {
			t.Fatal("installer repaired existing ownership or mode")
		}
	})

	t.Run("owned clean retry mutates only launchd", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.prepareRetry(t)

		harness.run(t, true)
		mutations := harness.mutationLines(t)
		if len(mutations) != 1 ||
			!strings.HasPrefix(mutations[0], "launchctl bootstrap ") {
			t.Fatalf("owned retry mutations = %q", mutations)
		}
	})

	t.Run("Directory Services read failure is not absence", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		writeDSCLRecord(t, harness.helper, "Groups", "sparerunner-runner-0", map[string]string{
			"PrimaryGroupID": "500",
			"RealName":       "Foreign local group",
			"Password":       "*",
		})
		if err := os.WriteFile(
			filepath.Join(harness.helper, "dscl-read-fail"),
			[]byte("/Groups/sparerunner-runner-0\n"),
			0o600,
		); err != nil {
			t.Fatal(err)
		}

		harness.run(t, false)
		harness.requireNoMutations(t)
		requireFileContents(
			t,
			filepath.Join(harness.helper, "dscl/Groups/sparerunner-runner-0/RealName"),
			"Foreign local group",
		)
	})

	for _, failure := range []struct {
		name   string
		prefix func(installerHarness) string
	}{
		{
			name: "mkdir",
			prefix: func(h installerHarness) string {
				return "mkdir -m 0700 " + filepath.Join(
					h.root,
					"Library/Application Support/SpareRunner/agent",
				)
			},
		},
		{
			name: "marker publication",
			prefix: func(h installerHarness) string {
				marker := filepath.Join(
					h.root,
					"Library/Application Support/SpareRunner/.sparerunner-install-ownership-v1",
				)
				return "ln " + marker + ".tmp-" + installerTestID + " " + marker
			},
		},
		{
			name: "Directory Services attribute",
			prefix: func(installerHarness) string {
				return "dscl-create /Groups/sparerunner-runner-0 PrimaryGroupID"
			},
		},
		{
			name: "plist copy",
			prefix: func(installerHarness) string {
				return "install "
			},
		},
		{
			name: "plist publication",
			prefix: func(h installerHarness) string {
				target := filepath.Join(
					h.root,
					"Library/LaunchDaemons/com.genm.sparerunner.agent.plist",
				)
				return "ln " + target + ".tmp-" + installerTestID + " " + target
			},
		},
		{
			name: "launchctl bootstrap",
			prefix: func(installerHarness) string {
				return "launchctl bootstrap system "
			},
		},
	} {
		failure := failure
		t.Run("verified rollback converges after "+failure.name+" failure", func(t *testing.T) {
			t.Parallel()
			harness := newInstallerHarness(t)
			harness.injectMutationFailureAfter(t, failure.prefix(harness))

			harness.run(t, false)
			harness.requireInitialTargetsAbsent(t)
			harness.prepareRetry(t)

			harness.run(t, true)
			requireFileContents(
				t,
				filepath.Join(
					harness.root,
					"Library/Application Support/SpareRunner/.sparerunner-install-ownership-v1",
				),
				markerFixture(
					installerTestID,
					"agent-state",
					"/Library/Application Support/SpareRunner",
				),
			)
		})
	}

	t.Run("foreign unmarked roots", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		makeDirectory(t, filepath.Join(
			harness.root,
			"Library/Application Support/SpareRunner",
		), 0o711)
		makeDirectory(t, filepath.Join(
			harness.root,
			"Library/Caches/com.genm.sparerunner",
		), 0o700)
		if err := os.WriteFile(filepath.Join(
			harness.root,
			"Library/Application Support/SpareRunner/foreign.txt",
		), []byte("not SpareRunner state"), 0o600); err != nil {
			t.Fatal(err)
		}

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("partial roots", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		makeDirectory(t, filepath.Join(
			harness.root,
			"Library/Application Support/SpareRunner",
		), 0o711)

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("non-directory root", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		if err := os.WriteFile(filepath.Join(
			harness.root,
			"Library/Application Support/SpareRunner",
		), []byte("foreign leaf"), 0o600); err != nil {
			t.Fatal(err)
		}

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("symlinked ancestor", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		caches := filepath.Join(harness.root, "Library/Caches")
		if err := os.Remove(caches); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(harness.root, "foreign-caches")
		makeDirectory(t, target, 0o755)
		if err := os.Symlink(target, caches); err != nil {
			t.Fatal(err)
		}

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("foreign ancestor owner", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		setStatOverride(t, harness.helper, filepath.Join(
			harness.root,
			"usr/local",
		), "501:20:40755")

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("tampered owned marker", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.prepareRetry(t)
		marker := filepath.Join(
			harness.root,
			"Library/Application Support/SpareRunner/.sparerunner-install-ownership-v1",
		)
		if err := os.WriteFile(marker, []byte(markerFixture(
			installerTestID,
			"controller-state",
			"/Library/Application Support/SpareRunner",
		)), 0o600); err != nil {
			t.Fatal(err)
		}

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("tampered owned mode", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.prepareRetry(t)
		if err := os.Chmod(filepath.Join(
			harness.root,
			"Library/Application Support/SpareRunner/agent",
		), 0o755); err != nil {
			t.Fatal(err)
		}

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("tampered owned owner", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.prepareRetry(t)
		setStatOverride(t, harness.helper, filepath.Join(
			harness.root,
			"Library/Application Support/SpareRunner/agent",
		), "501:20:40700")

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("foreign account and group", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		writeDSCLRecord(t, harness.helper, "Groups", "sparerunner-runner-0", map[string]string{
			"PrimaryGroupID": "500",
			"RealName":       "Existing local group",
			"Password":       "*",
		})
		writeDSCLRecord(t, harness.helper, "Users", "sparerunner-runner-0", map[string]string{
			"UniqueID":                "501",
			"PrimaryGroupID":          "500",
			"RealName":                "Existing local user",
			"NFSHomeDirectory":        "/var/empty",
			"UserShell":               "/usr/bin/false",
			"IsHidden":                "1",
			"AuthenticationAuthority": ";DisabledUser;",
			"Password":                "*",
		})

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("foreign launchd property list", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		target := filepath.Join(
			harness.root,
			"Library/LaunchDaemons/com.genm.sparerunner.agent.plist",
		)
		if err := os.WriteFile(target, []byte("foreign plist"), 0o600); err != nil {
			t.Fatal(err)
		}

		harness.run(t, false)
		harness.requireNoMutations(t)
		requireFileContents(t, target, "foreign plist")
	})

	t.Run("Directory Services enumeration failure", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		if err := os.WriteFile(
			filepath.Join(harness.helper, "dscl-list-fail"),
			[]byte("injected failure"),
			0o600,
		); err != nil {
			t.Fatal(err)
		}

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("duplicate owned account ID", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.prepareRetry(t)
		writeDSCLRecord(t, harness.helper, "Users", "foreign-user", map[string]string{
			"UniqueID": "501",
		})

		harness.run(t, false)
		harness.requireNoMutations(t)
	})

	t.Run("tampered owned group contract", func(t *testing.T) {
		t.Parallel()
		harness := newInstallerHarness(t)
		harness.run(t, true)
		harness.prepareRetry(t)
		realName := filepath.Join(
			harness.helper,
			"dscl/Groups/sparerunner-runner-0/RealName",
		)
		if err := os.WriteFile(realName, []byte("Foreign group"), 0o600); err != nil {
			t.Fatal(err)
		}

		harness.run(t, false)
		harness.requireNoMutations(t)
	})
}

func newInstallerHarness(t *testing.T) installerHarness {
	t.Helper()
	working := t.TempDir()
	root := filepath.Join(working, "root")
	tools := filepath.Join(working, "tools")
	helper := filepath.Join(working, "helper")
	for _, directory := range []string{
		root,
		tools,
		helper,
		filepath.Join(root, "Library"),
		filepath.Join(root, "Library/Application Support"),
		filepath.Join(root, "Library/Caches"),
		filepath.Join(root, "Library/LaunchDaemons"),
		filepath.Join(root, "usr"),
		filepath.Join(root, "usr/local"),
		filepath.Join(root, "usr/local/libexec"),
	} {
		makeDirectory(t, directory, 0o755)
	}
	// /Library/Caches is root-owned sticky 1777 on supported macOS releases.
	if err := os.Chmod(
		filepath.Join(root, "Library/Caches"),
		0o777|os.ModeSticky,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(root, ".sparerunner-installer-test-root"),
		[]byte("test-only\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "usr/local/libexec/sparerunner-agent")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(binary, 0o755); err != nil {
		t.Fatal(err)
	}
	// macOS exposes negative nobody IDs and can expose reserved positive IDs;
	// neither is a candidate for the dedicated service identity.
	writeDSCLRecord(t, helper, "Users", "_nobody", map[string]string{
		"UniqueID": "-2",
	})
	writeDSCLRecord(t, helper, "Groups", "nobody", map[string]string{
		"PrimaryGroupID": "-2",
	})
	writeDSCLRecord(t, helper, "Groups", "reserved", map[string]string{
		"PrimaryGroupID": "2147483647",
	})
	wrapper := []byte(`#!/bin/sh
export SPARERUNNER_INSTALL_HELPER_PROCESS=1
export SPARERUNNER_INSTALL_HELPER_TOOL="${0##*/}"
# Each command is a short-lived child of the race-instrumented test binary;
# omit the detector's default one-second exit delay for these helpers.
export GORACE="atexit_sleep_ms=0"
exec "$SPARERUNNER_INSTALL_TEST_BINARY" -test.run=TestMacOSInstallerHelperProcess -- "$@"
`)
	for _, tool := range []string{
		"cmp",
		"dscl",
		"id",
		"install",
		"launchctl",
		"ln",
		"mkdir",
		"plutil",
		"rm",
		"rmdir",
		"stat",
		"uuidgen",
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
	plist, err := filepath.Abs("launchd/com.genm.sparerunner.agent.plist")
	if err != nil {
		t.Fatal(err)
	}
	return installerHarness{
		root:      root,
		tools:     tools,
		helper:    helper,
		mutations: filepath.Join(helper, "mutations.log"),
		script:    script,
		plist:     plist,
	}
}

func (harness installerHarness) run(t *testing.T, wantSuccess bool) {
	t.Helper()
	command := exec.Command("/bin/bash", harness.script, harness.plist)
	command.Env = installerEnvironment(harness)
	output, err := command.CombinedOutput()
	if wantSuccess && err != nil {
		t.Fatalf("installer failed: %v\n%s", err, output)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("installer unexpectedly succeeded:\n%s", output)
	}
}

func (harness installerHarness) prepareRetry(t *testing.T) {
	t.Helper()
	for _, path := range []string{
		harness.mutations,
		filepath.Join(harness.helper, "launchd-loaded"),
	} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
}

func (harness installerHarness) injectMutationFailureAfter(t *testing.T, prefix string) {
	t.Helper()
	if strings.TrimSpace(prefix) == "" {
		t.Fatal("mutation failure prefix must not be empty")
	}
	if err := os.WriteFile(
		filepath.Join(harness.helper, "fail-after-mutation-prefix"),
		[]byte(prefix+"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
}

func (harness installerHarness) requireInitialTargetsAbsent(t *testing.T) {
	t.Helper()
	for _, path := range []string{
		filepath.Join(harness.root, "Library/Application Support/SpareRunner"),
		filepath.Join(harness.root, "Library/Caches/com.genm.sparerunner"),
		filepath.Join(
			harness.root,
			"Library/LaunchDaemons/com.genm.sparerunner.agent.plist",
		),
		filepath.Join(harness.helper, "dscl/Groups/sparerunner-runner-0"),
		filepath.Join(harness.helper, "dscl/Users/sparerunner-runner-0"),
		filepath.Join(harness.helper, "launchd-loaded"),
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

func installerEnvironment(harness installerHarness) []string {
	prefixes := []string{
		"SPARERUNNER_MACOS_INSTALL_TEST_",
		"SPARERUNNER_INSTALL_HELPER_",
		"SPARERUNNER_INSTALL_TEST_BINARY=",
	}
	environment := make([]string, 0, len(os.Environ())+7)
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
		"SPARERUNNER_MACOS_INSTALL_TESTING=1",
		"SPARERUNNER_MACOS_INSTALL_TEST_ROOT="+harness.root,
		"SPARERUNNER_MACOS_INSTALL_TEST_TOOLS="+harness.tools,
		"SPARERUNNER_INSTALL_TEST_BINARY="+os.Args[0],
		"SPARERUNNER_INSTALL_HELPER_STATE="+harness.helper,
		"SPARERUNNER_INSTALL_HELPER_UUID="+installerTestID,
	)
}

func markerFixture(installID, role, path string) string {
	return fmt.Sprintf(
		"version=1\ninstall_id=%s\nrole=%s\npath=%s\n",
		installID,
		role,
		path,
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

func requireMutation(t *testing.T, mutations []string, prefix string) {
	t.Helper()
	for _, mutation := range mutations {
		if strings.HasPrefix(mutation, prefix) {
			return
		}
	}
	t.Fatalf("mutation log lacks %q: %q", prefix, mutations)
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

func writeDSCLRecord(
	t *testing.T,
	helperRoot string,
	kind string,
	name string,
	attributes map[string]string,
) {
	t.Helper()
	record := filepath.Join(helperRoot, "dscl", kind, name)
	makeDirectory(t, record, 0o700)
	for attribute, value := range attributes {
		if err := os.WriteFile(
			filepath.Join(record, attribute),
			[]byte(value),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
	}
}

func setStatOverride(t *testing.T, helperRoot, path, contract string) {
	t.Helper()
	overridePath := filepath.Join(helperRoot, "stat-overrides.json")
	overrides := make(map[string]string)
	if encoded, err := os.ReadFile(overridePath); err == nil {
		if err := json.Unmarshal(encoded, &overrides); err != nil {
			t.Fatal(err)
		}
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	overrides[path] = contract
	encoded, err := json.Marshal(overrides)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(overridePath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestMacOSInstallerHelperProcess implements the fixed macOS command surface
// used by install-service.sh. It runs only in subprocesses spawned above.
func TestMacOSInstallerHelperProcess(t *testing.T) {
	if os.Getenv("SPARERUNNER_INSTALL_HELPER_PROCESS") != "1" {
		return
	}
	tool := os.Getenv("SPARERUNNER_INSTALL_HELPER_TOOL")
	state := os.Getenv("SPARERUNNER_INSTALL_HELPER_STATE")
	args := helperArguments(os.Args)
	exitCode := runInstallerHelper(tool, state, args, os.Stdout, os.Stderr)
	os.Exit(exitCode)
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
		fmt.Fprintln(stdout, "0")
		return 0
	case "uuidgen":
		fmt.Fprintln(stdout, os.Getenv("SPARERUNNER_INSTALL_HELPER_UUID"))
		return 0
	case "stat":
		return fakeStat(state, args, stdout, stderr)
	case "plutil":
		return 0
	case "launchctl":
		return fakeLaunchctl(state, args)
	case "dscl":
		return fakeDSCL(state, args, stdout, stderr)
	case "mkdir":
		return fakeMkdir(state, args, stderr)
	case "ln":
		return fakeLink(state, args, stderr)
	case "rm":
		return fakeRemove(state, args, stderr)
	case "rmdir":
		return fakeRmdir(state, args, stderr)
	case "install":
		return fakeInstall(state, args, stderr)
	case "cmp":
		return fakeCompare(args, stderr)
	default:
		fmt.Fprintf(stderr, "unsupported fake installer tool %q\n", tool)
		return 2
	}
}

func fakeStat(state string, args []string, stdout, stderr io.Writer) int {
	if len(args) != 3 || args[0] != "-f" {
		fmt.Fprintf(stderr, "unexpected stat arguments: %q\n", args)
		return 2
	}
	info, err := os.Lstat(args[2])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	switch args[1] {
	case "%u:%g:%p":
		overrides := make(map[string]string)
		if encoded, err := os.ReadFile(filepath.Join(state, "stat-overrides.json")); err == nil {
			if err := json.Unmarshal(encoded, &overrides); err != nil {
				fmt.Fprintln(stderr, err)
				return 1
			}
		} else if !os.IsNotExist(err) {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if override, exists := overrides[args[2]]; exists {
			fmt.Fprintln(stdout, override)
			return 0
		}
		typeBits := uint32(0)
		switch {
		case info.Mode().IsRegular():
			typeBits = 0o100000
		case info.IsDir():
			typeBits = 0o040000
		case info.Mode()&os.ModeSymlink != 0:
			typeBits = 0o120000
		default:
			fmt.Fprintln(stderr, "unsupported fake file type")
			return 1
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
		fmt.Fprintf(stdout, "0:0:%o\n", typeBits|permissions)
	case "%l":
		fmt.Fprintln(stdout, "1")
	default:
		fmt.Fprintf(stderr, "unexpected stat format: %q\n", args[1])
		return 2
	}
	return 0
}

func fakeLaunchctl(state string, args []string) int {
	if len(args) == 2 && args[0] == "print" {
		if _, err := os.Stat(filepath.Join(state, "launchd-loaded")); err == nil {
			return 0
		}
		return 1
	}
	if len(args) == 3 && args[0] == "bootstrap" && args[1] == "system" {
		mutation := "launchctl " + strings.Join(args, " ")
		if failInjectedMutation(state, mutation) {
			return 1
		}
		if err := appendMutation(state, mutation); err != nil {
			return 1
		}
		if err := os.WriteFile(
			filepath.Join(state, "launchd-loaded"),
			[]byte(args[2]),
			0o600,
		); err != nil {
			return 1
		}
		if failAfterInjectedMutation(state, mutation) {
			return 1
		}
		return 0
	}
	if len(args) == 2 && args[0] == "bootout" {
		mutation := "launchctl " + strings.Join(args, " ")
		if failInjectedMutation(state, mutation) {
			return 1
		}
		if err := appendMutation(state, mutation); err != nil {
			return 1
		}
		if err := os.Remove(filepath.Join(state, "launchd-loaded")); err != nil &&
			!os.IsNotExist(err) {
			return 1
		}
		return 0
	}
	return 2
}

func fakeDSCL(
	state string,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
) int {
	if len(args) < 3 || args[0] != "." {
		fmt.Fprintf(stderr, "unexpected dscl arguments: %q\n", args)
		return 2
	}
	dsclRoot := filepath.Join(state, "dscl")
	switch args[1] {
	case "-list":
		if len(args) != 4 {
			return 2
		}
		if _, err := os.Stat(filepath.Join(state, "dscl-list-fail")); err == nil {
			return 1
		}
		kind := strings.TrimPrefix(args[2], "/")
		entries, err := os.ReadDir(filepath.Join(dsclRoot, kind))
		if os.IsNotExist(err) {
			return 0
		}
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		sort.Slice(entries, func(left, right int) bool {
			return entries[left].Name() < entries[right].Name()
		})
		for _, entry := range entries {
			value, err := os.ReadFile(filepath.Join(
				dsclRoot,
				kind,
				entry.Name(),
				args[3],
			))
			if err == nil {
				fmt.Fprintf(stdout, "%s %s\n", entry.Name(), value)
			}
		}
		return 0
	case "-read":
		if len(args) != 3 && len(args) != 4 {
			return 2
		}
		if failedRecord, err := os.ReadFile(filepath.Join(state, "dscl-read-fail")); err == nil &&
			strings.TrimSpace(string(failedRecord)) == args[2] {
			fmt.Fprintln(stderr, "injected Directory Services read failure")
			return 74
		}
		record := fakeDSCLRecordPath(dsclRoot, args[2])
		if info, err := os.Stat(record); err != nil || !info.IsDir() {
			fmt.Fprintln(stderr, "<dscl_cmd> DS Error: -14136 (eDSRecordNotFound)")
			return 56
		}
		if len(args) == 3 {
			return 0
		}
		value, err := os.ReadFile(filepath.Join(record, args[3]))
		if err != nil {
			return 1
		}
		fmt.Fprintf(stdout, "%s: %s\n", args[3], value)
		return 0
	case "-create":
		if len(args) != 3 && len(args) != 5 {
			return 2
		}
		mutation := "dscl-create " + strings.Join(args[2:], " ")
		if failInjectedMutation(state, mutation) {
			return 1
		}
		if err := appendMutation(state, mutation); err != nil {
			return 1
		}
		record := fakeDSCLRecordPath(dsclRoot, args[2])
		if err := os.MkdirAll(record, 0o700); err != nil {
			return 1
		}
		if len(args) == 5 {
			if err := os.WriteFile(
				filepath.Join(record, args[3]),
				[]byte(args[4]),
				0o600,
			); err != nil {
				return 1
			}
		}
		if failAfterInjectedMutation(state, mutation) {
			return 1
		}
		return 0
	case "-delete":
		if len(args) != 3 {
			return 2
		}
		mutation := "dscl-delete " + args[2]
		if failInjectedMutation(state, mutation) {
			return 1
		}
		if err := appendMutation(state, mutation); err != nil {
			return 1
		}
		if err := os.RemoveAll(fakeDSCLRecordPath(dsclRoot, args[2])); err != nil {
			return 1
		}
		return 0
	default:
		return 2
	}
}

func fakeDSCLRecordPath(root, record string) string {
	parts := strings.Split(strings.TrimPrefix(record, "/"), "/")
	return filepath.Join(append([]string{root}, parts...)...)
}

func fakeMkdir(state string, args []string, stderr io.Writer) int {
	if len(args) != 3 || args[0] != "-m" {
		return 2
	}
	mode, err := strconv.ParseUint(args[1], 8, 32)
	if err != nil {
		return 2
	}
	mutation := "mkdir " + strings.Join(args, " ")
	if failInjectedMutation(state, mutation) {
		return 1
	}
	if err := appendMutation(state, mutation); err != nil {
		return 1
	}
	if err := os.Mkdir(args[2], os.FileMode(mode)); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := os.Chmod(args[2], os.FileMode(mode)); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if failAfterInjectedMutation(state, mutation) {
		return 1
	}
	return 0
}

func fakeLink(state string, args []string, stderr io.Writer) int {
	if len(args) != 2 {
		return 2
	}
	mutation := "ln " + strings.Join(args, " ")
	if failInjectedMutation(state, mutation) {
		return 1
	}
	if err := appendMutation(state, mutation); err != nil {
		return 1
	}
	if err := os.Link(args[0], args[1]); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if failAfterInjectedMutation(state, mutation) {
		return 1
	}
	return 0
}

func fakeRemove(state string, args []string, stderr io.Writer) int {
	if len(args) != 1 {
		return 2
	}
	mutation := "rm " + args[0]
	if failInjectedMutation(state, mutation) {
		return 1
	}
	if err := appendMutation(state, mutation); err != nil {
		return 1
	}
	if err := os.Remove(args[0]); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func fakeRmdir(state string, args []string, stderr io.Writer) int {
	if len(args) != 1 {
		return 2
	}
	mutation := "rmdir " + args[0]
	if failInjectedMutation(state, mutation) {
		return 1
	}
	if err := appendMutation(state, mutation); err != nil {
		return 1
	}
	if err := os.Remove(args[0]); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func fakeInstall(state string, args []string, stderr io.Writer) int {
	if len(args) != 8 ||
		args[0] != "-o" ||
		args[2] != "-g" ||
		args[4] != "-m" {
		fmt.Fprintf(stderr, "unexpected install arguments: %q\n", args)
		return 2
	}
	mode, err := strconv.ParseUint(args[5], 8, 32)
	if err != nil {
		return 2
	}
	mutation := "install " + strings.Join(args, " ")
	if failInjectedMutation(state, mutation) {
		return 1
	}
	if err := appendMutation(state, mutation); err != nil {
		return 1
	}
	source, err := os.Open(args[6])
	if err != nil {
		return 1
	}
	defer source.Close()
	target, err := os.OpenFile(args[7], os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(mode))
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	_, copyErr := io.Copy(target, source)
	closeErr := target.Close()
	if copyErr != nil || closeErr != nil {
		return 1
	}
	if failAfterInjectedMutation(state, mutation) {
		return 1
	}
	return 0
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
