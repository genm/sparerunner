//go:build darwin

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/genm/sparerunner/internal/runner"
)

func TestMacOSLiveConfigAcceptsPinnedDarwinPackage(t *testing.T) {
	config := validMacOSConfig(t, "arm64")
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	config = validMacOSConfig(t, "amd64")
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMacOSLiveConfigFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*macOSLiveConfig)
	}{
		{name: "wrong version", mutate: func(config *macOSLiveConfig) {
			config.Version++
		}},
		{name: "evidence overlaps state", mutate: func(config *macOSLiveConfig) {
			config.EvidenceDirectory = agentStateRoot + "/evidence"
		}},
		{name: "unsupported architecture", mutate: func(config *macOSLiveConfig) {
			config.ExpectedArchitecture = "386"
		}},
		{name: "relative harness", mutate: func(config *macOSLiveConfig) {
			config.HarnessPath = "sparerunner-macos-live"
		}},
		{name: "package substitution", mutate: func(config *macOSLiveConfig) {
			config.ExpectedRunnerPackageSHA256 = strings.Repeat("a", 64)
		}},
		{name: "unbounded run", mutate: func(config *macOSLiveConfig) {
			config.MaximumRunSeconds = int(maxMacOSRunDuration.Seconds()) + 1
		}},
		{name: "unsafe execution ID", mutate: func(config *macOSLiveConfig) {
			config.ExecutionID = "../execution"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := validMacOSConfig(t, "arm64")
			test.mutate(&config)
			if err := config.validate(); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}
}

func TestLoadMacOSLiveConfigRejectsUnknownAndTrailingJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	for _, payload := range []string{
		`{"unknown":true}`,
		`{}` + "\n{}",
	} {
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadMacOSLiveConfig(path); err == nil {
			t.Fatalf("payload was accepted: %s", payload)
		}
	}
}

func validMacOSConfig(t *testing.T, architecture string) macOSLiveConfig {
	t.Helper()
	pkg, err := runner.OfficialPackage(runner.Platform{
		OS: "darwin", Arch: architecture,
	})
	if err != nil {
		t.Fatal(err)
	}
	return macOSLiveConfig{
		Version:                      macOSLiveConfigVersion,
		EvidenceDirectory:            filepath.Join(t.TempDir(), "evidence"),
		HarnessPath:                  "/usr/local/libexec/sparerunner-macos-live",
		ExecutionID:                  "macos-live-execution",
		ExpectedArchitecture:         architecture,
		ExpectedCommitSHA:            strings.Repeat("a", 40),
		ExpectedHarnessSHA256:        strings.Repeat("d", 64),
		ExpectedInstalledAgentSHA256: strings.Repeat("b", 64),
		ExpectedLaunchDaemonSHA256:   strings.Repeat("c", 64),
		ExpectedRunnerPackageSHA256:  pkg.Checksum,
		MaximumRunSeconds:            7200,
	}
}
