//go:build !windows

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/runner"
)

func TestLoadLiveConfigAcceptsStrictNonSecretVersionOne(t *testing.T) {
	config := validTestConfig(t)
	path := filepath.Join(t.TempDir(), "live.json")
	writeJSONTestFile(t, path, config, 0o600)

	loaded, err := loadLiveConfig(path)
	if err != nil {
		t.Fatalf("loadLiveConfig() error = %v", err)
	}
	if loaded != config {
		t.Fatalf("loadLiveConfig() = %#v, want %#v", loaded, config)
	}
}

func TestValidateConfigCommandRejectsWritableAuthorityBeforeShellUsesPaths(t *testing.T) {
	config := validTestConfig(t)
	trustedDirectory := t.TempDir()
	trustedPath := filepath.Join(trustedDirectory, "live.json")
	writeJSONTestFile(t, trustedPath, config, 0o600)
	if err := runCLI(
		context.Background(),
		[]string{"validate-config", "--config", trustedPath},
		nil,
	); err != nil {
		t.Fatalf("validate-config trusted error = %v", err)
	}

	writableDirectory := t.TempDir()
	writablePath := filepath.Join(writableDirectory, "live.json")
	writeJSONTestFile(t, writablePath, config, 0o600)
	if err := os.Chmod(writableDirectory, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := runCLI(
		context.Background(),
		[]string{"validate-config", "--config", writablePath},
		nil,
	); !errors.Is(err, errConfigInvalid) {
		t.Fatalf("validate-config writable authority error = %v, want errConfigInvalid", err)
	}
}

func TestLoadLiveConfigRejectsUnknownDuplicateAndTrailingData(t *testing.T) {
	config := validTestConfig(t)
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		name    string
		payload []byte
	}{
		{
			name: "unknown secret field",
			payload: []byte(`{
				"version": 1,
				"privateKey": "jit-secret-canary"
			}`),
		},
		{
			name:    "duplicate version",
			payload: append([]byte(`{"version":1,"version":1,"ignored":`), append(encoded, '}')...),
		},
		{
			name:    "trailing document",
			payload: append(encoded, []byte(` {}`)...),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "live.json")
			if err := os.WriteFile(path, testCase.payload, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadLiveConfig(path); !errors.Is(err, errConfigInvalid) {
				t.Fatalf("loadLiveConfig() error = %v, want errConfigInvalid", err)
			}
		})
	}
}

func TestLiveConfigValidationFailsClosedAtOwnedBoundaries(t *testing.T) {
	base := validTestConfig(t)
	testCases := []struct {
		name   string
		mutate func(*liveConfig)
	}{
		{name: "version mismatch", mutate: func(config *liveConfig) { config.Version = 2 }},
		{name: "relative key path", mutate: func(config *liveConfig) { config.GitHub.PrivateKeyFile = "key.pem" }},
		{name: "unsafe runtime root", mutate: func(config *liveConfig) { config.RuntimeRoot = "/" }},
		{name: "unsafe restart window", mutate: func(config *liveConfig) {
			config.AgentRestartMinimumRunningSeconds = 0
		}},
		{name: "key inside state", mutate: func(config *liveConfig) {
			config.GitHub.PrivateKeyFile = filepath.Join(config.ControllerStateDirectory, "key.pem")
		}},
		{name: "proof outside evidence", mutate: func(config *liveConfig) {
			config.GitHub.PrivateRepositoryProofFile = filepath.Join(t.TempDir(), "proof.json")
		}},
		{name: "proof filename drift", mutate: func(config *liveConfig) {
			config.GitHub.PrivateRepositoryProofFile = filepath.Join(
				config.EvidenceDirectory,
				"unexpected-proof.json",
			)
		}},
		{name: "organization target", mutate: func(config *liveConfig) {
			config.GitHub.ConfigURL = "https://github.com/example-org"
		}},
		{name: "non github target", mutate: func(config *liveConfig) {
			config.GitHub.ConfigURL = "https://github.example.test/example-org/private-sandbox"
		}},
		{name: "public preview label drift", mutate: func(config *liveConfig) {
			config.GitHub.ScaleSetName = "tewake"
		}},
		{name: "non canonical node", mutate: func(config *liveConfig) {
			config.NodeID = "ABCDEF0123456789ABCDEF0123456789"
		}},
		{name: "hostname listener", mutate: func(config *liveConfig) {
			config.AgentListenAddress = "controller.example.test:7443"
		}},
		{name: "relative unit fragment", mutate: func(config *liveConfig) {
			config.Provenance.ExpectedAgentUnitFragmentPath = "tewake-agent.service"
		}},
		{name: "uppercase commit", mutate: func(config *liveConfig) {
			config.Provenance.ExpectedCommitSHA = strings.Repeat("A", 40)
		}},
		{name: "short installed digest", mutate: func(config *liveConfig) {
			config.Provenance.ExpectedInstalledAgentSHA256 = strings.Repeat("a", 63)
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			config := base
			testCase.mutate(&config)
			if err := config.validate(); !errors.Is(err, errConfigInvalid) {
				t.Fatalf("validate() error = %v, want errConfigInvalid", err)
			}
		})
	}
}

func TestPrivateRepositoryProofRequiresFreshPrivateExactRepository(t *testing.T) {
	config := validTestConfig(t)
	if err := os.MkdirAll(config.EvidenceDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	proof := privateRepositoryProof{
		Version:    privateProofVersion,
		Repository: "example-org/private-sandbox",
		Visibility: "PRIVATE",
	}
	writeJSONTestFile(t, config.GitHub.PrivateRepositoryProofFile, proof, 0o600)
	if _, err := loadPrivateRepositoryProof(config, time.Now()); err != nil {
		t.Fatalf("loadPrivateRepositoryProof() error = %v", err)
	}

	proof.Visibility = "PUBLIC"
	writeJSONTestFile(t, config.GitHub.PrivateRepositoryProofFile, proof, 0o600)
	if _, err := loadPrivateRepositoryProof(config, time.Now()); !errors.Is(err, errPrivateProofInvalid) {
		t.Fatalf("public proof error = %v, want errPrivateProofInvalid", err)
	}

	proof.Visibility = "PRIVATE"
	writeJSONTestFile(t, config.GitHub.PrivateRepositoryProofFile, proof, 0o600)
	stale := time.Now().Add(-privateProofMaxAge - time.Minute)
	if err := os.Chtimes(config.GitHub.PrivateRepositoryProofFile, stale, stale); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPrivateRepositoryProof(config, time.Now()); !errors.Is(err, errPrivateProofInvalid) {
		t.Fatalf("stale proof error = %v, want errPrivateProofInvalid", err)
	}
}

func TestLoadGitHubPrivateKeyRequiresAbsoluteLinuxPrivateFile(t *testing.T) {
	if _, err := loadGitHubPrivateKey("relative.pem"); !errors.Is(err, errCredentialUnavailable) {
		t.Fatalf("relative key error = %v, want errCredentialUnavailable", err)
	}
	if runtime.GOOS != "linux" {
		t.Skip("Linux credential-file policy is deliberately unavailable on this host")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "app.pem")
	if err := os.WriteFile(path, []byte("dummy-key-material"), 0o600); err != nil {
		t.Fatal(err)
	}
	contents, err := loadGitHubPrivateKey(path)
	if err != nil {
		t.Fatalf("loadGitHubPrivateKey() error = %v", err)
	}
	clear(contents)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadGitHubPrivateKey(path); !errors.Is(err, errCredentialUnavailable) {
		t.Fatalf("0644 key error = %v, want errCredentialUnavailable", err)
	}
}

func TestValidateLiveScaleSetAndStableTargetID(t *testing.T) {
	config := validTestConfig(t)
	scaleSet := github.ScaleSet{
		ID:            41,
		Name:          liveScaleSetName,
		RunnerGroupID: config.GitHub.RunnerGroupID,
		Labels:        []string{liveScaleSetName},
		DisableUpdate: config.GitHub.DisableUpdate,
	}
	if err := validateLiveScaleSet(config, &scaleSet); err != nil {
		t.Fatalf("validateLiveScaleSet() error = %v", err)
	}
	first := stableTargetID(config, scaleSet)
	config.NodeID = "11111111111111111111111111111111"
	if second := stableTargetID(config, scaleSet); second != first {
		t.Fatalf("target ID changed with node: %q != %q", second, first)
	}
	scaleSet.ID++
	if changed := stableTargetID(config, scaleSet); changed == first {
		t.Fatal("target ID did not change with scale-set identity")
	}
	scaleSet.Labels = []string{liveScaleSetName, "unexpected"}
	if err := validateLiveScaleSet(config, &scaleSet); !errors.Is(err, errScaleSetPreflight) {
		t.Fatalf("label drift error = %v, want errScaleSetPreflight", err)
	}
}

func validTestConfig(t *testing.T) liveConfig {
	t.Helper()
	root := t.TempDir()
	state := filepath.Join(root, "state")
	evidence := filepath.Join(root, "evidence")
	credentials := filepath.Join(root, "credentials")
	runtimeRoot := filepath.Join(root, "runtime")
	officialPackage, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	return liveConfig{
		Version:                           liveConfigVersion,
		ControllerStateDirectory:          state,
		AgentListenAddress:                "127.0.0.1:7443",
		EvidenceDirectory:                 evidence,
		RuntimeRoot:                       runtimeRoot,
		NodeReadyTimeoutSeconds:           30,
		RunTimeoutSeconds:                 300,
		AgentRestartMinimumRunningSeconds: 30,
		Provenance: provenanceConfig{
			ExpectedCommitSHA:                  strings.Repeat("1", 40),
			ExpectedInstalledAgentSHA256:       strings.Repeat("2", 64),
			ExpectedAgentUnitFragmentPath:      "/etc/systemd/system/tewake-agent.service",
			ExpectedAgentUnitSHA256:            strings.Repeat("3", 64),
			ExpectedSupervisorUnitFragmentPath: "/etc/systemd/system/tewake-supervisor.service",
			ExpectedSupervisorUnitSHA256:       strings.Repeat("4", 64),
			ExpectedRunnerPackageSHA256:        officialPackage.Checksum,
		},
		GitHub: githubConfig{
			ConfigURL:                  "https://github.com/example-org/private-sandbox",
			ClientID:                   "Iv1-test-client",
			InstallationID:             101,
			PrivateKeyFile:             filepath.Join(credentials, "app.pem"),
			PrivateRepositoryProofFile: filepath.Join(evidence, privateProofFileName),
			RunnerGroupID:              1,
			ScaleSetName:               liveScaleSetName,
			DisableUpdate:              true,
		},
		NodeID: "0123456789abcdef0123456789abcdef",
	}
}

func writeJSONTestFile(t *testing.T, path string, value any, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}
