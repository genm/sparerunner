package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func writeTestAppKey(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "app.pem")
	encoded := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// privateControllerDir prepares the state directory the documented way, by
// running sparerunner init. A hand-made directory is not equivalent: on Windows the
// credential store requires the restrictive ACL initialization applies, and the
// connect command requires a controller that actually exists.
//
// macOS has no state-publication credential adapter yet (spr-008), so init
// cannot run there. The skip is conditioned on that platform rather than on any
// init failure, so a real failure on a supported platform still fails the test,
// and these tests start running on macOS as soon as the adapter lands.
func privateControllerDir(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("sparerunner init needs the macOS credential adapter (spr-008)")
	}
	directory := filepath.Join(t.TempDir(), "controller")
	var stdout, stderr bytes.Buffer
	if err := run([]string{"init", "--state-dir", directory}, &stdout, &stderr); err != nil {
		t.Fatalf("init = %v (stderr %q)", err, stderr.String())
	}
	return directory
}

// TestGitHubConnectStoresTheAppWithoutABrowserOrManagementSession proves the
// credential entry point the documented setup path relies on: no Web UI, no
// admin session, and no key material on the command line or in the output.
func TestGitHubConnectStoresTheAppWithoutABrowserOrManagementSession(t *testing.T) {
	directory := privateControllerDir(t)
	keyPath := writeTestAppKey(t)
	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := run([]string{
		"github", "connect",
		"--state-dir", directory,
		"--app-id", "4409279",
		"--client-id", "Iv1.testclientid",
		"--private-key-file", keyPath,
	}, &stdout, &stderr); err != nil {
		t.Fatalf("connect = %v (stderr %q)", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "4409279") {
		t.Fatalf("connect output omitted the App identity: %q", stdout.String())
	}
	if strings.Contains(stdout.String(), "PRIVATE KEY") ||
		strings.Contains(stdout.String(), string(keyBytes)) ||
		strings.Contains(stderr.String(), "PRIVATE KEY") {
		t.Fatal("connect echoed private key material")
	}
	if _, err := os.Stat(filepath.Join(directory, githubAppCredentialFile)); err != nil {
		t.Fatalf("credential was not stored: %v", err)
	}

	// Re-running with the same App is idempotent, so a retried setup step does
	// not become an error the operator has to reason about.
	var replayOut, replayErr bytes.Buffer
	if err := run([]string{
		"github", "connect",
		"--state-dir", directory,
		"--app-id", "4409279",
		"--client-id", "Iv1.testclientid",
		"--private-key-file", keyPath,
	}, &replayOut, &replayErr); err != nil {
		t.Fatalf("identical replay = %v (stderr %q)", err, replayErr.String())
	}
}

// TestGitHubConnectRefusesToReplaceADifferentApp keeps the no-clobber store
// contract visible at the CLI: silently rebinding a controller to another App
// would strand every Target already provisioned through the first one.
func TestGitHubConnectRefusesToReplaceADifferentApp(t *testing.T) {
	directory := privateControllerDir(t)
	first := writeTestAppKey(t)
	second := writeTestAppKey(t)

	var stdout, stderr bytes.Buffer
	if err := run([]string{
		"github", "connect", "--state-dir", directory,
		"--app-id", "111", "--client-id", "Iv1.first", "--private-key-file", first,
	}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	err := run([]string{
		"github", "connect", "--state-dir", directory,
		"--app-id", "222", "--client-id", "Iv1.second", "--private-key-file", second,
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("connecting a different App replaced the existing one")
	}
	if !strings.Contains(err.Error(), "111") || !strings.Contains(err.Error(), "222") {
		t.Fatalf("error does not name both Apps: %v", err)
	}
}

func TestGitHubConnectRejectsIncompleteOrUnusableInput(t *testing.T) {
	keyPath := writeTestAppKey(t)
	notAKey := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(notAKey, []byte("this is not a PEM key"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := map[string][]string{
		"missing app id":      {"--client-id", "Iv1.x", "--private-key-file", keyPath},
		"missing client id":   {"--app-id", "1", "--private-key-file", keyPath},
		"missing key file":    {"--app-id", "1", "--client-id", "Iv1.x"},
		"absent key file":     {"--app-id", "1", "--client-id", "Iv1.x", "--private-key-file", filepath.Join(t.TempDir(), "gone.pem")},
		"key file is not pem": {"--app-id", "1", "--client-id", "Iv1.x", "--private-key-file", notAKey},
	}
	for name, flags := range tests {
		t.Run(name, func(t *testing.T) {
			directory := privateControllerDir(t)
			var stdout, stderr bytes.Buffer
			arguments := append([]string{"github", "connect", "--state-dir", directory}, flags...)
			if err := run(arguments, &stdout, &stderr); err == nil {
				t.Fatal("invalid connect input was accepted")
			}
			if _, err := os.Stat(filepath.Join(directory, githubAppCredentialFile)); !os.IsNotExist(err) {
				t.Fatalf("rejected input still stored a credential: %v", err)
			}
		})
	}
}

// TestGitHubCommandsRefuseAnUninitializedStateDirectory keeps the missing setup
// step visible: the platform credential store's own failure names neither the
// directory nor sparerunner init.
func TestGitHubCommandsRefuseAnUninitializedStateDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "not-a-controller")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	keyPath := writeTestAppKey(t)
	commands := map[string][]string{
		"connect": {
			"github", "connect", "--state-dir", directory,
			"--app-id", "1", "--client-id", "Iv1.x", "--private-key-file", keyPath,
		},
		"installations": {"github", "installations", "--state-dir", directory},
	}
	for name, arguments := range commands {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(arguments, &stdout, &stderr)
			if err == nil {
				t.Fatal("uninitialized state directory was accepted")
			}
			if !strings.Contains(err.Error(), "sparerunner init") {
				t.Fatalf("error does not name the missing step: %v", err)
			}
		})
	}
}

// TestGitHubInstallationsRequiresAConnectedApp fails closed rather than
// reporting an empty fleet-wide installation list for a controller that has no
// App at all.
func TestGitHubInstallationsRequiresAConnectedApp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"github", "installations", "--state-dir", privateControllerDir(t),
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("installations succeeded without a connected App")
	}
	if !strings.Contains(err.Error(), "sparerunner github connect") {
		t.Fatalf("error does not name the next step: %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unconnected controller printed a list: %q", stdout.String())
	}
}
