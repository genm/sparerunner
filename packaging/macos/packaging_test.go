package macospackaging

import (
	"encoding/xml"
	"errors"
	"os"
	"strings"
	"testing"
)

type plistDocument struct {
	XMLName xml.Name `xml:"plist"`
}

func TestLaunchDaemonRestartsFailingAgentWithoutSecretEnvironment(t *testing.T) {
	plist := readPackagingFile(t, "launchd/com.genm.sparerunner.agent.plist")
	var document plistDocument
	if err := xml.Unmarshal([]byte(plist), &document); err != nil {
		t.Fatalf("invalid plist XML: %v", err)
	}
	for _, required := range []string{
		"<string>com.genm.sparerunner.agent</string>",
		"<string>/usr/local/libexec/sparerunner-agent</string>",
		"<string>root</string>",
		"<string>wheel</string>",
		"<key>RunAtLoad</key>",
		"<key>KeepAlive</key>",
		"<key>SuccessfulExit</key>",
		"<string>--require-native-runner</string>",
		"<key>AbandonProcessGroup</key>",
		"<false/>",
	} {
		if !strings.Contains(plist, required) {
			t.Fatalf("launchd plist lacks %q", required)
		}
	}
	for _, forbidden := range []string{
		"<key>EnvironmentVariables</key>",
		"jitconfig",
		"node-private-key",
		"join-token",
		"sparerunner-runner-0</string>",
	} {
		if strings.Contains(plist, forbidden) {
			t.Fatalf("launchd plist contains forbidden value %q", forbidden)
		}
	}
}

func TestInstallScriptCreatesHiddenNonLoginRunnerAndPrivateRoots(t *testing.T) {
	script := readPackagingFile(t, "install-service.sh")
	for _, required := range []string{
		`runner_account="sparerunner-runner-0"`,
		`UserShell "/usr/bin/false"`,
		`IsHidden 1`,
		`AuthenticationAuthority ";DisabledUser;"`,
		`configured_home`,
		`configured_auth`,
		`configured_password`,
		`marker_name=".sparerunner-install-ownership-v1"`,
		`"$EUID" -eq 0`,
		`stat -f '%u:%g:%p'`,
		`validate_owned_layout`,
		`refusing foreign or partial SpareRunner service roots`,
		`run_tool install -o root -g wheel -m 0600`,
		`run_tool launchctl bootstrap system`,
	} {
		if !strings.Contains(script, required) {
			t.Fatalf("installer lacks %q", required)
		}
	}
	for _, forbidden := range []string{
		"sudo ",
		".env",
		"-w ",
		"-X ",
		"Password \"-\"",
		"/usr/sbin/chown",
		"/bin/chmod",
		"%Mp%Lp",
	} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("installer contains forbidden value %q", forbidden)
		}
	}
	loadedCheck := strings.Index(script, `launchctl print "system/${label}"`)
	installPlist := strings.Index(script, `run_tool install -o root -g wheel -m 0600`)
	if loadedCheck < 0 || installPlist < 0 || loadedCheck > installPlist {
		t.Fatal("installer checks the live label only after mutating its plist")
	}
}

func TestInstallationPrecedesRootContextJoinIntoServiceState(t *testing.T) {
	readme := readPackagingFile(t, "README.md")
	install := `sudo ./packaging/macos/install-service.sh`
	installCLI := `./sparerunner /usr/local/bin/sparerunner`
	join := `sudo /usr/local/bin/sparerunner join spr_... \`
	state := `--state-dir "/Library/Application Support/SpareRunner/agent"`
	activation := `sudo /bin/launchctl kickstart -k system/com.genm.sparerunner.agent`
	installAt := strings.Index(readme, install)
	installCLIAt := strings.Index(readme, installCLI)
	joinAt := strings.Index(readme, join)
	stateAt := strings.Index(readme, state)
	activationAt := strings.Index(readme, activation)
	if installAt < 0 || installCLIAt < 0 || joinAt < 0 ||
		stateAt < 0 || activationAt < 0 {
		t.Fatalf(
			"README must install the CLI then document root join and launchd activation: cli=%d install=%d join=%d state=%d activation=%d",
			installCLIAt,
			installAt,
			joinAt,
			stateAt,
			activationAt,
		)
	}
	if installCLIAt > installAt || installAt > joinAt ||
		joinAt > stateAt || stateAt > activationAt {
		t.Fatal("README changed the CLI install -> service install -> root join -> launchd activation sequence")
	}
}

func TestPackagingDoesNotClaimCompletedLiveAcceptance(t *testing.T) {
	readme := readPackagingFile(t, "README.md")
	for _, pending := range []string{
		"reboot/sleep cycle",
		"Keychain ACL",
		"private GitHub job",
		"remain live acceptance gates",
	} {
		if !strings.Contains(readme, pending) {
			t.Fatalf("README does not retain pending gate %q", pending)
		}
	}
	if strings.Contains(readme, "Status: complete") {
		t.Fatal(errors.New("packaging claimed unverified completion"))
	}
	if strings.Contains(readme, "binds native Keychain access to code-signing identity") {
		t.Fatal("README claims TrustAll is constrained by code-signing identity")
	}
	for _, boundary := range []string{
		"any process running in the same root service",
		"Code signing does not narrow this",
		"runner UID",
		"do not prove native Keychain access",
	} {
		if !strings.Contains(readme, boundary) {
			t.Fatalf("README does not document TrustAll boundary %q", boundary)
		}
	}
}

func readPackagingFile(t *testing.T, name string) string {
	t.Helper()
	contents, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}
