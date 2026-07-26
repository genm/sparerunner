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
	plist := readPackagingFile(t, "launchd/com.genm.tewake.agent.plist")
	var document plistDocument
	if err := xml.Unmarshal([]byte(plist), &document); err != nil {
		t.Fatalf("invalid plist XML: %v", err)
	}
	for _, required := range []string{
		"<string>com.genm.tewake.agent</string>",
		"<string>/usr/local/libexec/tewake-agent</string>",
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
		"tewake-runner-0</string>",
	} {
		if strings.Contains(plist, forbidden) {
			t.Fatalf("launchd plist contains forbidden value %q", forbidden)
		}
	}
}

func TestInstallScriptCreatesHiddenNonLoginRunnerAndPrivateRoots(t *testing.T) {
	script := readPackagingFile(t, "install-service.sh")
	for _, required := range []string{
		`runner_account="tewake-runner-0"`,
		`UserShell "/usr/bin/false"`,
		`IsHidden 1`,
		`AuthenticationAuthority ";DisabledUser;"`,
		`configured_home`,
		`configured_auth`,
		`configured_password`,
		`refusing unsafe service directory`,
		`/bin/chmod 0700`,
		`/bin/chmod 0711`,
		`/usr/bin/install -o root -g wheel -m 0600`,
		`launchctl bootstrap system`,
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
	} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("installer contains forbidden value %q", forbidden)
		}
	}
	loadedCheck := strings.Index(script, `launchctl print "system/${label}"`)
	installPlist := strings.Index(script, `/usr/bin/install -o root -g wheel -m 0600`)
	if loadedCheck < 0 || installPlist < 0 || loadedCheck > installPlist {
		t.Fatal("installer checks the live label only after mutating its plist")
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
}

func readPackagingFile(t *testing.T, name string) string {
	t.Helper()
	contents, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}
