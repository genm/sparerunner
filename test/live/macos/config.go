package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

const (
	macOSLiveConfigVersion = 1
	maxMacOSConfigBytes    = 32 << 10
	maxMacOSRunDuration    = 24 * time.Hour

	installedAgentPath = "/usr/local/libexec/tewake-agent"
	installedPlistPath = "/Library/LaunchDaemons/com.genm.tewake.agent.plist"
	agentStateRoot     = "/Library/Application Support/Tewake/agent"
	agentDatabasePath  = agentStateRoot + "/agent.db"
	nodeKeyLocatorPath = agentStateRoot + "/node-private-key.pem"
	runtimeRootPath    = "/Library/Application Support/Tewake/runtime"
	executionsRootPath = runtimeRootPath + "/executions"
	fenceRootPath      = "/Library/Application Support/Tewake/fences"
	cacheRootPath      = "/Library/Caches/com.genm.tewake/runner"
	launchDaemonLabel  = "com.genm.tewake.agent"
	runnerAccountName  = "tewake-runner-0"
)

var (
	errMacOSConfigInvalid   = errors.New("macOS live acceptance config is invalid")
	errMacOSEvidenceInvalid = errors.New("macOS live acceptance evidence is invalid")
)

type macOSLiveConfig struct {
	Version                      int    `json:"version"`
	EvidenceDirectory            string `json:"evidenceDirectory"`
	HarnessPath                  string `json:"harnessPath"`
	ExecutionID                  string `json:"executionId"`
	ExpectedArchitecture         string `json:"expectedArchitecture"`
	ExpectedCommitSHA            string `json:"expectedCommitSha"`
	ExpectedHarnessSHA256        string `json:"expectedHarnessSha256"`
	ExpectedInstalledAgentSHA256 string `json:"expectedInstalledAgentSha256"`
	ExpectedLaunchDaemonSHA256   string `json:"expectedLaunchDaemonSha256"`
	ExpectedRunnerPackageSHA256  string `json:"expectedRunnerPackageSha256"`
	MaximumRunSeconds            int    `json:"maximumRunSeconds"`
}

func loadMacOSLiveConfig(path string) (macOSLiveConfig, error) {
	if !canonicalAbsolutePath(path) {
		return macOSLiveConfig{}, fmt.Errorf("%w: config path", errMacOSConfigInvalid)
	}
	if err := validateMacOSLiveFileAuthority(path, 0o600); err != nil {
		return macOSLiveConfig{}, fmt.Errorf("%w: config authority", errMacOSConfigInvalid)
	}
	contents, err := readStrictRegularFile(path, maxMacOSConfigBytes)
	if err != nil {
		return macOSLiveConfig{}, fmt.Errorf("%w: config file", errMacOSConfigInvalid)
	}
	var config macOSLiveConfig
	if err := decodeStrictJSON(contents, &config); err != nil {
		return macOSLiveConfig{}, fmt.Errorf("%w: config document", errMacOSConfigInvalid)
	}
	if err := config.validate(); err != nil {
		return macOSLiveConfig{}, err
	}
	return config, nil
}

func (config macOSLiveConfig) validate() error {
	if config.Version != macOSLiveConfigVersion ||
		!canonicalAbsolutePath(config.EvidenceDirectory) ||
		!canonicalAbsolutePath(config.HarnessPath) ||
		config.EvidenceDirectory == "/" ||
		pathsOverlap(config.EvidenceDirectory, agentStateRoot) ||
		pathsOverlap(config.EvidenceDirectory, runtimeRootPath) ||
		pathsOverlap(config.EvidenceDirectory, fenceRootPath) ||
		pathsOverlap(config.EvidenceDirectory, cacheRootPath) {
		return fmt.Errorf("%w: version or evidence path", errMacOSConfigInvalid)
	}
	if !safeOpaqueID(config.ExecutionID, 256) ||
		(config.ExpectedArchitecture != "arm64" &&
			config.ExpectedArchitecture != "amd64") ||
		!lowerHex(config.ExpectedCommitSHA, 40) ||
		!lowerHex(config.ExpectedHarnessSHA256, 64) ||
		!lowerHex(config.ExpectedInstalledAgentSHA256, 64) ||
		!lowerHex(config.ExpectedLaunchDaemonSHA256, 64) ||
		!lowerHex(config.ExpectedRunnerPackageSHA256, 64) ||
		config.MaximumRunSeconds < 1 ||
		time.Duration(config.MaximumRunSeconds)*time.Second > maxMacOSRunDuration {
		return fmt.Errorf("%w: identity, provenance, or time bound", errMacOSConfigInvalid)
	}
	pkg, err := runner.OfficialPackage(runner.Platform{
		OS: "darwin", Arch: config.ExpectedArchitecture,
	})
	if err != nil || pkg.Checksum != config.ExpectedRunnerPackageSHA256 {
		return fmt.Errorf("%w: official runner package", errMacOSConfigInvalid)
	}
	return nil
}

func readStrictRegularFile(path string, limit int64) ([]byte, error) {
	if !canonicalAbsolutePath(path) || limit <= 0 {
		return nil, errMacOSConfigInvalid
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0o022 != 0 ||
		before.Size() <= 0 || before.Size() > limit {
		return nil, errMacOSConfigInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errMacOSConfigInvalid
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, errMacOSConfigInvalid
	}
	contents, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || len(contents) == 0 || int64(len(contents)) > limit {
		return nil, errMacOSConfigInvalid
	}
	return contents, nil
}

func decodeStrictJSON(contents []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON")
		}
		return err
	}
	return nil
}

func canonicalAbsolutePath(value string) bool {
	return filepath.IsAbs(value) && filepath.Clean(value) == value && value != "/"
}

func pathsOverlap(first, second string) bool {
	return pathContains(first, second) || pathContains(second, first)
}

func pathContains(parent, child string) bool {
	relative, err := filepath.Rel(filepath.Clean(parent), filepath.Clean(child))
	return err == nil && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func lowerHex(value string, lengths ...int) bool {
	validLength := false
	for _, length := range lengths {
		validLength = validLength || len(value) == length
	}
	if !validLength {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') &&
			!(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func safeOpaqueID(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e ||
			character == '/' || character == '\\' {
			return false
		}
	}
	return true
}
