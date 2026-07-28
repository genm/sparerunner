package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/genm/sparerunner/internal/enroll"
	"github.com/genm/sparerunner/internal/runner"
)

const (
	liveConfigVersion       = 1
	privateProofVersion     = 1
	privateProofFileName    = "private-repository-proof.json"
	liveScaleSetName        = "sparerunner-linux"
	maxLiveConfigBytes      = 64 << 10
	maxPrivateKeyBytes      = 64 << 10
	privateProofMaxAge      = 5 * time.Minute
	maxNodeReadyTimeout     = 10 * time.Minute
	maxAcceptanceRunTimeout = 2 * time.Hour
)

var (
	errConfigInvalid         = errors.New("live acceptance config is invalid")
	errCredentialUnavailable = errors.New("live acceptance credential is unavailable")
	errPrivateProofInvalid   = errors.New("private repository proof is invalid")
)

type liveConfig struct {
	Version                           int              `json:"version"`
	ControllerStateDirectory          string           `json:"controllerStateDirectory"`
	AgentListenAddress                string           `json:"agentListenAddress"`
	EvidenceDirectory                 string           `json:"evidenceDirectory"`
	RuntimeRoot                       string           `json:"runtimeRoot"`
	NodeReadyTimeoutSeconds           int              `json:"nodeReadyTimeoutSeconds"`
	RunTimeoutSeconds                 int              `json:"runTimeoutSeconds"`
	AgentRestartMinimumRunningSeconds int              `json:"agentRestartMinimumRunningSeconds"`
	Provenance                        provenanceConfig `json:"provenance"`
	GitHub                            githubConfig     `json:"github"`
	NodeID                            string           `json:"nodeId"`
}

type provenanceConfig struct {
	ExpectedCommitSHA                  string `json:"expectedCommitSha"`
	ExpectedInstalledAgentSHA256       string `json:"expectedInstalledAgentSha256"`
	ExpectedAgentUnitFragmentPath      string `json:"expectedAgentUnitFragmentPath"`
	ExpectedAgentUnitSHA256            string `json:"expectedAgentUnitSha256"`
	ExpectedSupervisorUnitFragmentPath string `json:"expectedSupervisorUnitFragmentPath"`
	ExpectedSupervisorUnitSHA256       string `json:"expectedSupervisorUnitSha256"`
	ExpectedRunnerPackageSHA256        string `json:"expectedRunnerPackageSha256"`
}

type githubConfig struct {
	ConfigURL                  string `json:"configUrl"`
	ClientID                   string `json:"clientId"`
	InstallationID             int64  `json:"installationId"`
	PrivateKeyFile             string `json:"privateKeyFile"`
	PrivateRepositoryProofFile string `json:"privateRepositoryProofFile"`
	RunnerGroupID              int    `json:"runnerGroupId"`
	ScaleSetName               string `json:"scaleSetName"`
	DisableUpdate              bool   `json:"disableUpdate"`
}

type privateRepositoryProof struct {
	Version    int    `json:"version"`
	Repository string `json:"repository"`
	Visibility string `json:"visibility"`
}

type acceptanceMode string

const (
	modeNormal          acceptanceMode = "normal"
	modeCommitBeforeAck acceptanceMode = "commit-before-ack"
	modeCleanupFailure  acceptanceMode = "cleanup-failure"
	modeAgentRestart    acceptanceMode = "agent-restart"
)

func parseAcceptanceMode(value string) (acceptanceMode, error) {
	mode := acceptanceMode(value)
	switch mode {
	case modeNormal, modeCommitBeforeAck, modeCleanupFailure, modeAgentRestart:
		return mode, nil
	default:
		return "", fmt.Errorf("%w: mode", errConfigInvalid)
	}
}

func loadLiveConfig(path string) (liveConfig, error) {
	if !canonicalAbsolutePath(path) {
		return liveConfig{}, fmt.Errorf("%w: config path", errConfigInvalid)
	}
	var config liveConfig
	if err := decodeStrictRegularJSONFile(path, maxLiveConfigBytes, &config, false); err != nil {
		return liveConfig{}, fmt.Errorf("%w: config document", errConfigInvalid)
	}
	if err := config.validate(); err != nil {
		return liveConfig{}, err
	}
	return config, nil
}

func (config liveConfig) validate() error {
	if config.Version != liveConfigVersion {
		return fmt.Errorf("%w: version", errConfigInvalid)
	}
	if !canonicalAbsolutePath(config.ControllerStateDirectory) ||
		!canonicalAbsolutePath(config.EvidenceDirectory) ||
		!canonicalAbsolutePath(config.RuntimeRoot) ||
		!canonicalAbsolutePath(config.Provenance.ExpectedAgentUnitFragmentPath) ||
		!canonicalAbsolutePath(config.Provenance.ExpectedSupervisorUnitFragmentPath) ||
		!canonicalAbsolutePath(config.GitHub.PrivateKeyFile) ||
		!canonicalAbsolutePath(config.GitHub.PrivateRepositoryProofFile) {
		return fmt.Errorf("%w: paths must be canonical and absolute", errConfigInvalid)
	}
	if config.RuntimeRoot == "/" {
		return fmt.Errorf("%w: runtime root", errConfigInvalid)
	}
	if pathsOverlap(config.ControllerStateDirectory, config.EvidenceDirectory) {
		return fmt.Errorf("%w: state and evidence directories overlap", errConfigInvalid)
	}
	if pathContains(config.ControllerStateDirectory, config.GitHub.PrivateKeyFile) ||
		pathContains(config.EvidenceDirectory, config.GitHub.PrivateKeyFile) {
		return fmt.Errorf("%w: private key must be outside state and evidence directories", errConfigInvalid)
	}
	if filepath.Dir(config.GitHub.PrivateRepositoryProofFile) != config.EvidenceDirectory {
		return fmt.Errorf("%w: repository proof must be a direct evidence child", errConfigInvalid)
	}
	if filepath.Base(config.GitHub.PrivateRepositoryProofFile) != privateProofFileName {
		return fmt.Errorf("%w: repository proof filename", errConfigInvalid)
	}
	if err := validateListenAddress(config.AgentListenAddress); err != nil {
		return fmt.Errorf("%w: agent listen address", errConfigInvalid)
	}
	if config.NodeReadyTimeoutSeconds < 1 ||
		time.Duration(config.NodeReadyTimeoutSeconds)*time.Second > maxNodeReadyTimeout {
		return fmt.Errorf("%w: node ready timeout", errConfigInvalid)
	}
	if config.RunTimeoutSeconds < 1 ||
		time.Duration(config.RunTimeoutSeconds)*time.Second > maxAcceptanceRunTimeout {
		return fmt.Errorf("%w: run timeout", errConfigInvalid)
	}
	if config.AgentRestartMinimumRunningSeconds < 10 ||
		config.AgentRestartMinimumRunningSeconds > 600 {
		return fmt.Errorf("%w: agent restart minimum running time", errConfigInvalid)
	}
	if !lowerHexDigest(config.Provenance.ExpectedCommitSHA, 40, 64) ||
		!lowerHexDigest(config.Provenance.ExpectedInstalledAgentSHA256, 64) ||
		!lowerHexDigest(config.Provenance.ExpectedAgentUnitSHA256, 64) ||
		!lowerHexDigest(config.Provenance.ExpectedSupervisorUnitSHA256, 64) ||
		!lowerHexDigest(config.Provenance.ExpectedRunnerPackageSHA256, 64) {
		return fmt.Errorf("%w: provenance expectations", errConfigInvalid)
	}
	officialPackage, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil ||
		config.Provenance.ExpectedRunnerPackageSHA256 != officialPackage.Checksum {
		return fmt.Errorf("%w: official runner provenance", errConfigInvalid)
	}
	if _, err := repositoryFromConfigURL(config.GitHub.ConfigURL); err != nil {
		return err
	}
	if !safeIdentifier(config.GitHub.ClientID, 128) || config.GitHub.InstallationID <= 0 ||
		config.GitHub.RunnerGroupID <= 0 || config.GitHub.ScaleSetName != liveScaleSetName {
		return fmt.Errorf("%w: GitHub target identity", errConfigInvalid)
	}
	if !canonicalNodeID(config.NodeID) {
		return fmt.Errorf("%w: node ID", errConfigInvalid)
	}
	return nil
}

func lowerHexDigest(value string, lengths ...int) bool {
	validLength := false
	for _, length := range lengths {
		if len(value) == length {
			validLength = true
		}
	}
	if !validLength {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func (config liveConfig) nodeReadyTimeout() time.Duration {
	return time.Duration(config.NodeReadyTimeoutSeconds) * time.Second
}

func (config liveConfig) runTimeout() time.Duration {
	return time.Duration(config.RunTimeoutSeconds) * time.Second
}

func repositoryFromConfigURL(raw string) (string, error) {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != "https" ||
		!strings.EqualFold(parsed.Hostname(), "github.com") ||
		parsed.Host != parsed.Hostname() || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" ||
		parsed.Opaque != "" || strings.Contains(raw, "#") {
		return "", fmt.Errorf("%w: GitHub repository URL", errConfigInvalid)
	}
	escaped := strings.Trim(parsed.EscapedPath(), "/")
	parts := strings.Split(escaped, "/")
	if len(parts) != 2 || !safeGitHubPathPart(parts[0]) || !safeGitHubPathPart(parts[1]) {
		return "", fmt.Errorf("%w: a repository-level GitHub URL is required", errConfigInvalid)
	}
	return strings.ToLower(parts[0] + "/" + parts[1]), nil
}

func validateListenAddress(address string) error {
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil || rawPort == "" {
		return errors.New("invalid listen address")
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("invalid listen port")
	}
	if host != "" && net.ParseIP(host) == nil {
		return errors.New("listen host must be a numeric IP")
	}
	return nil
}

func loadPrivateRepositoryProof(config liveConfig, now time.Time) (privateRepositoryProof, error) {
	path := config.GitHub.PrivateRepositoryProofFile
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o600 {
		return privateRepositoryProof{}, errPrivateProofInvalid
	}
	age := now.Sub(info.ModTime())
	if age < -time.Minute || age > privateProofMaxAge {
		return privateRepositoryProof{}, errPrivateProofInvalid
	}
	var proof privateRepositoryProof
	if err := decodeStrictRegularJSONFile(path, maxLiveConfigBytes, &proof, true); err != nil {
		return privateRepositoryProof{}, errPrivateProofInvalid
	}
	repository, err := repositoryFromConfigURL(config.GitHub.ConfigURL)
	if err != nil || proof.Version != privateProofVersion ||
		strings.ToLower(proof.Repository) != repository ||
		proof.Visibility != "PRIVATE" {
		return privateRepositoryProof{}, errPrivateProofInvalid
	}
	return proof, nil
}

func loadGitHubPrivateKey(path string) ([]byte, error) {
	if !canonicalAbsolutePath(path) {
		return nil, errCredentialUnavailable
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Size() < 1 || info.Size() > maxPrivateKeyBytes {
		return nil, errCredentialUnavailable
	}
	contents, err := enroll.LoadPrivateMaterial(path)
	if err != nil {
		return nil, errCredentialUnavailable
	}
	if len(contents) == 0 || len(contents) > maxPrivateKeyBytes {
		clear(contents)
		return nil, errCredentialUnavailable
	}
	return contents, nil
}

func decodeStrictRegularJSONFile(path string, limit int64, destination any, requirePrivate bool) error {
	file, info, err := openTrustedRegular(path, requirePrivate)
	if err != nil || info.Size() < 1 || info.Size() > limit {
		return errors.New("JSON file is not a bounded regular file")
	}
	payload, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	closeErr := file.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if int64(len(payload)) > limit {
		return errors.New("JSON file exceeds size limit")
	}
	if err := rejectDuplicateJSONKeys(payload); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON document has trailing data")
	}
	return nil
}

func rejectDuplicateJSONKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON object key is not a string")
				}
				if _, duplicate := seen[key]; duplicate {
					return errors.New("JSON object contains a duplicate key")
				}
				seen[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("JSON object is malformed")
			}
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("JSON array is malformed")
			}
		default:
			return errors.New("JSON delimiter is malformed")
		}
		return nil
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("JSON document has trailing data")
	}
	return nil
}

func canonicalAbsolutePath(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path
}

func pathsOverlap(left, right string) bool {
	return pathContains(left, right) || pathContains(right, left)
}

func pathContains(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func canonicalNodeID(value string) bool {
	if len(value) != 32 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	if err != nil {
		return false
	}
	_, err = enroll.CanonicalNodeURI(value)
	return err == nil
}

func safeIdentifier(value string, limit int) bool {
	if value == "" || len(value) > limit || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func safeGitHubPathPart(value string) bool {
	if value == "" || value == "." || value == ".." {
		return false
	}
	for _, character := range value {
		if !((character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '-' || character == '_' || character == '.') {
			return false
		}
	}
	return true
}
