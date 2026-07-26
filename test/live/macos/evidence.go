package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/genm/tewake/internal/runner"
)

const macOSEvidenceVersion = 1

type capturePhase string

const (
	phaseBefore             capturePhase = "before"
	phaseRunning            capturePhase = "running"
	phaseRunningBeforeSleep capturePhase = "running-before-sleep"
	phaseRunningAfterWake   capturePhase = "running-after-wake"
	phaseAfter              capturePhase = "after"
	phasePreReboot          capturePhase = "pre-reboot"
	phasePostReboot         capturePhase = "post-reboot"
)

type acceptanceScenario string

const (
	scenarioNormal acceptanceScenario = "normal"
	scenarioSleep  acceptanceScenario = "sleep"
	scenarioReboot acceptanceScenario = "reboot"
)

type processEvidence struct {
	PID  int `json:"pid"`
	PGID int `json:"processGroupId"`
	UID  int `json:"uid"`
}

type executionEvidence struct {
	Found        bool         `json:"found"`
	State        runner.State `json:"state,omitempty"`
	PID          int          `json:"pid,omitempty"`
	HostEpoch    string       `json:"hostEpoch,omitempty"`
	Revision     uint64       `json:"revision,omitempty"`
	HasTombstone bool         `json:"hasTombstone,omitempty"`
}

type provenanceEvidence struct {
	CommitSHA           string `json:"commitSha"`
	HarnessSHA256       string `json:"harnessSha256"`
	AgentSHA256         string `json:"agentSha256"`
	LaunchDaemonSHA256  string `json:"launchDaemonSha256"`
	RunnerPackageSHA256 string `json:"runnerPackageSha256"`
}

type privateMaterialEvidence struct {
	ServiceCanLoad        bool `json:"serviceCanLoad"`
	RunnerAccountDenied   bool `json:"runnerAccountDenied"`
	LocatorContainsSecret bool `json:"locatorContainsSecret"`
}

type nodeEvidence struct {
	Version              int                     `json:"version"`
	Phase                capturePhase            `json:"phase"`
	CapturedAt           string                  `json:"capturedAt"`
	BootEpoch            string                  `json:"bootEpoch"`
	Architecture         string                  `json:"architecture"`
	LaunchDaemonState    string                  `json:"launchDaemonState"`
	Agent                processEvidence         `json:"agent"`
	AgentInstances       int                     `json:"agentInstances"`
	RunnerUID            int                     `json:"runnerUid"`
	RunnerGID            int                     `json:"runnerGid"`
	RunnerProcesses      []processEvidence       `json:"runnerProcesses"`
	ControllerEpoch      uint64                  `json:"controllerEpoch"`
	Execution            executionEvidence       `json:"execution"`
	ExecutionDirectories int                     `json:"executionDirectories"`
	FenceDirectories     int                     `json:"fenceDirectories"`
	PrivateMaterial      privateMaterialEvidence `json:"privateMaterial"`
	Provenance           provenanceEvidence      `json:"provenance"`
}

type evidenceStore struct {
	directory string
}

func openEvidenceStore(directory string) (*evidenceStore, error) {
	if !canonicalAbsolutePath(directory) {
		return nil, errMacOSEvidenceInvalid
	}
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(directory, 0o700); err != nil {
			return nil, errMacOSEvidenceInvalid
		}
		info, err = os.Lstat(directory)
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o700 ||
		validateMacOSLiveFileAuthority(directory, 0o700) != nil {
		return nil, errMacOSEvidenceInvalid
	}
	return &evidenceStore{directory: directory}, nil
}

func parseCapturePhase(value string) (capturePhase, error) {
	phase := capturePhase(value)
	switch phase {
	case phaseBefore, phaseRunning, phaseRunningBeforeSleep, phaseRunningAfterWake,
		phaseAfter, phasePreReboot, phasePostReboot:
		return phase, nil
	default:
		return "", errMacOSConfigInvalid
	}
}

func parseAcceptanceScenario(value string) (acceptanceScenario, error) {
	scenario := acceptanceScenario(value)
	switch scenario {
	case scenarioNormal, scenarioSleep, scenarioReboot:
		return scenario, nil
	default:
		return "", errMacOSConfigInvalid
	}
}

func evidenceFileName(phase capturePhase) (string, error) {
	if _, err := parseCapturePhase(string(phase)); err != nil {
		return "", err
	}
	return string(phase) + ".json", nil
}

func (store *evidenceStore) writeNode(evidence nodeEvidence) error {
	if store == nil || validateNodeEvidenceShape(evidence) != nil {
		return errMacOSEvidenceInvalid
	}
	name, err := evidenceFileName(evidence.Phase)
	if err != nil {
		return errMacOSEvidenceInvalid
	}
	return store.writeJSON(name, evidence)
}

func (store *evidenceStore) loadNode(phase capturePhase) (nodeEvidence, error) {
	if store == nil {
		return nodeEvidence{}, errMacOSEvidenceInvalid
	}
	name, err := evidenceFileName(phase)
	if err != nil {
		return nodeEvidence{}, errMacOSEvidenceInvalid
	}
	path := filepath.Join(store.directory, name)
	if validateMacOSLiveFileAuthority(path, 0o600) != nil {
		return nodeEvidence{}, errMacOSEvidenceInvalid
	}
	contents, err := readStrictRegularFile(path, 64<<10)
	if err != nil {
		return nodeEvidence{}, errMacOSEvidenceInvalid
	}
	var evidence nodeEvidence
	if decodeStrictJSON(contents, &evidence) != nil ||
		validateNodeEvidenceShape(evidence) != nil {
		return nodeEvidence{}, errMacOSEvidenceInvalid
	}
	return evidence, nil
}

func (store *evidenceStore) writeJSON(name string, value any) error {
	if store == nil || filepath.Base(name) != name ||
		!strings.HasSuffix(name, ".json") {
		return errMacOSEvidenceInvalid
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil || evidenceContainsSecretField(encoded) {
		return errMacOSEvidenceInvalid
	}
	encoded = append(encoded, '\n')
	temporary, err := os.CreateTemp(store.directory, ".tewake-macos-evidence-")
	if err != nil {
		return errMacOSEvidenceInvalid
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return errMacOSEvidenceInvalid
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return errMacOSEvidenceInvalid
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return errMacOSEvidenceInvalid
	}
	if err := temporary.Close(); err != nil {
		return errMacOSEvidenceInvalid
	}
	target := filepath.Join(store.directory, name)
	if err := os.Link(temporaryPath, target); err != nil {
		return errMacOSEvidenceInvalid
	}
	if err := os.Remove(temporaryPath); err != nil {
		return errMacOSEvidenceInvalid
	}
	directory, err := os.Open(store.directory)
	if err != nil {
		return errMacOSEvidenceInvalid
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return errMacOSEvidenceInvalid
	}
	return nil
}

func validateNodeEvidenceShape(evidence nodeEvidence) error {
	if evidence.Version != macOSEvidenceVersion {
		return errMacOSEvidenceInvalid
	}
	if _, err := parseCapturePhase(string(evidence.Phase)); err != nil {
		return errMacOSEvidenceInvalid
	}
	captured, err := time.Parse(time.RFC3339Nano, evidence.CapturedAt)
	if err != nil || captured.Location() != time.UTC ||
		!lowerHex(evidence.BootEpoch, 64) ||
		(evidence.Architecture != "arm64" && evidence.Architecture != "amd64") ||
		evidence.Agent.PID <= 1 || evidence.Agent.UID != 0 ||
		evidence.Agent.PGID <= 0 || evidence.AgentInstances < 1 ||
		evidence.RunnerUID <= 0 || evidence.RunnerGID <= 0 ||
		evidence.ExecutionDirectories < 0 || evidence.FenceDirectories < 0 ||
		!lowerHex(evidence.Provenance.CommitSHA, 40) ||
		!lowerHex(evidence.Provenance.HarnessSHA256, 64) ||
		!lowerHex(evidence.Provenance.AgentSHA256, 64) ||
		!lowerHex(evidence.Provenance.LaunchDaemonSHA256, 64) ||
		!lowerHex(evidence.Provenance.RunnerPackageSHA256, 64) {
		return errMacOSEvidenceInvalid
	}
	if evidence.Execution.Found {
		if evidence.Execution.Revision == 0 ||
			evidence.Execution.State == "" ||
			!lowerHex(evidence.Execution.HostEpoch, 64) {
			return errMacOSEvidenceInvalid
		}
	} else if evidence.Execution != (executionEvidence{}) {
		return errMacOSEvidenceInvalid
	}
	seen := make(map[int]struct{}, len(evidence.RunnerProcesses))
	for index, process := range evidence.RunnerProcesses {
		if process.PID <= 1 || process.PGID <= 0 ||
			process.UID != evidence.RunnerUID {
			return errMacOSEvidenceInvalid
		}
		if _, found := seen[process.PID]; found {
			return errMacOSEvidenceInvalid
		}
		seen[process.PID] = struct{}{}
		if index > 0 && evidence.RunnerProcesses[index-1].PID >= process.PID {
			return errMacOSEvidenceInvalid
		}
	}
	return nil
}

func evidenceContainsSecretField(encoded []byte) bool {
	lower := bytes.ToLower(encoded)
	for _, forbidden := range [][]byte{
		[]byte(`"jit`),
		[]byte(`"privatekey`),
		[]byte(`"join`),
		[]byte(`"authorization`),
		[]byte(`"fencetoken`),
		[]byte(`"keychainitem`),
	} {
		if bytes.Contains(lower, forbidden) {
			return true
		}
	}
	return false
}

func sortProcessEvidence(processes []processEvidence) {
	sort.Slice(processes, func(first, second int) bool {
		return processes[first].PID < processes[second].PID
	})
}
