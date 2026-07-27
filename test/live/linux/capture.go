package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

var errNodeEvidenceInvalid = errors.New("Linux node evidence is invalid")

type processEvidence struct {
	Version     int               `json:"version"`
	Phase       string            `json:"phase"`
	Status      string            `json:"status"`
	Processes   []observedProcess `json:"processes"`
	GeneratedAt string            `json:"generatedAt"`
}

type observedProcess struct {
	PID              int    `json:"pid"`
	UID              int    `json:"uid"`
	Role             string `json:"role"`
	SystemdUnit      string `json:"systemdUnit,omitempty"`
	ControlGroup     string `json:"controlGroup"`
	StartTimeTicks   uint64 `json:"startTimeTicks"`
	BootID           string `json:"bootId"`
	Executable       string `json:"executable"`
	ExecutableSHA256 string `json:"executableSha256"`
}

type filesystemEvidence struct {
	Version                 int    `json:"version"`
	Status                  string `json:"status"`
	RuntimeRootPresent      bool   `json:"runtimeRootPresent"`
	ExecutionEntries        int    `json:"executionEntries"`
	Symlinks                int    `json:"symlinks"`
	WorkDirectories         int    `json:"workDirectories"`
	RunnerRegistrations     int    `json:"runnerRegistrations"`
	CredentialFiles         int    `json:"credentialFiles"`
	CredentialRSAParamFiles int    `json:"credentialRSAParamFiles"`
	JITCanaryFiles          int    `json:"jitCanaryFiles"`
	GeneratedAt             string `json:"generatedAt"`
}

func captureNodeEvidence(phase string, config liveConfig, probe authorityProbe) error {
	if runtime.GOOS != "linux" ||
		(phase != "before" &&
			phase != "after" &&
			phase != "running-before-restart" &&
			phase != "running-after-restart") ||
		probe == nil {
		return errNodeEvidenceInvalid
	}
	evidence, err := openEvidenceStore(config.EvidenceDirectory)
	if err != nil {
		return err
	}
	processes, err := collectProcessEvidence("/proc", phase)
	if err != nil {
		return err
	}
	if err := bindProcessEvidence(&processes, phase, config, probe); err != nil {
		return err
	}
	name := processBeforeName
	switch phase {
	case "after":
		name = processAfterName
	case "running-before-restart":
		name = processRunningBeforeRestartName
	case "running-after-restart":
		name = processRunningAfterRestartName
	}
	if err := evidence.writeJSON(name, processes); err != nil {
		return err
	}
	if phase == "after" {
		filesystem, err := collectFilesystemEvidence(config.RuntimeRoot)
		if err != nil {
			return err
		}
		if err := evidence.writeJSON(filesystemName, filesystem); err != nil {
			return err
		}
	}
	return nil
}

func bindProcessEvidence(
	processes *processEvidence,
	phase string,
	config liveConfig,
	probe authorityProbe,
) error {
	authority, err := loadEvidenceFile[authorityEvidence](
		config.EvidenceDirectory,
		authorityFileName,
	)
	if err != nil || authority.Version != evidenceVersion ||
		authority.Status != "passed" || authority.RuntimeRoot != config.RuntimeRoot {
		return errNodeEvidenceInvalid
	}
	bootRaw, err := probe.readFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(bootRaw)) != authority.BootID {
		return errNodeEvidenceInvalid
	}
	agent, err := readServiceAuthority(
		probe,
		"tewake-agent.service",
		"serve",
		config.RuntimeRoot,
		config.Provenance.ExpectedAgentUnitFragmentPath,
		authority.Agent.UID,
	)
	if err != nil {
		return errNodeEvidenceInvalid
	}
	supervisor, err := readServiceAuthority(
		probe,
		"tewake-supervisor.service",
		"supervisor",
		config.RuntimeRoot,
		config.Provenance.ExpectedSupervisorUnitFragmentPath,
		0,
	)
	if err != nil ||
		supervisor.MainPID != authority.Supervisor.MainPID ||
		supervisor.ProcessStartTicks != authority.Supervisor.ProcessStartTicks {
		return errNodeEvidenceInvalid
	}
	var expectedRunnerCgroup, expectedRunnerExecutionID string
	if phase == "running-before-restart" || phase == "running-after-restart" {
		marker, loadErr := loadEvidenceFile[restartStartedEvidence](
			config.EvidenceDirectory,
			restartStartedName,
		)
		if loadErr != nil || marker.ExecutionID == "" {
			return errNodeEvidenceInvalid
		}
		expectedRunnerExecutionID = marker.ExecutionID
		digest := sha256.Sum256([]byte(marker.ExecutionID))
		expectedRunnerCgroup = filepath.Join(
			supervisor.ControlGroup,
			"tewake",
			"tewake-"+hex.EncodeToString(digest[:]),
		)
	}
	for index := range processes.Processes {
		process := &processes.Processes[index]
		var expected serviceAuthority
		switch process.Role {
		case "agent":
			expected = agent
			process.SystemdUnit = agent.Unit
		case "supervisor":
			expected = supervisor
			process.SystemdUnit = supervisor.Unit
		case "runner_listener":
			if process.UID != authority.RunnerUID {
				return errNodeEvidenceInvalid
			}
			process.ControlGroup, err = processCgroup(probe, process.PID)
			if err != nil || process.ControlGroup != expectedRunnerCgroup {
				return errNodeEvidenceInvalid
			}
			process.StartTimeTicks, err = processStartTicks(probe, process.PID)
			if err != nil {
				return errNodeEvidenceInvalid
			}
			process.Executable, process.ExecutableSHA256, err =
				probe.executableIdentity(process.PID)
			if err != nil ||
				process.Executable != expectedRunnerListenerPath(
					config.RuntimeRoot,
					expectedRunnerExecutionID,
				) ||
				len(process.ExecutableSHA256) != sha256.Size*2 {
				return errNodeEvidenceInvalid
			}
			process.BootID = authority.BootID
			continue
		default:
			return errNodeEvidenceInvalid
		}
		if process.PID != expected.MainPID || process.UID != expected.UID {
			return errNodeEvidenceInvalid
		}
		process.ControlGroup = expected.ControlGroup
		process.StartTimeTicks = expected.ProcessStartTicks
		process.BootID = authority.BootID
		process.Executable = expected.Executable
		process.ExecutableSHA256 = expected.ExecutableSHA256
	}
	return nil
}

func expectedRunnerListenerPath(runtimeRoot, executionID string) string {
	digest := sha256.Sum256([]byte(executionID))
	return filepath.Join(
		runtimeRoot,
		"executions",
		hex.EncodeToString(digest[:]),
		"bin",
		"Runner.Listener",
	)
}

func collectProcessEvidence(procRoot, phase string) (processEvidence, error) {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return processEvidence{}, errNodeEvidenceInvalid
	}
	result := processEvidence{
		Version:     evidenceVersion,
		Phase:       phase,
		Status:      "failed",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || !entry.IsDir() {
			continue
		}
		process, relevant, err := inspectRelevantProcess(procRoot, pid)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
			continue
		}
		if err != nil {
			return processEvidence{}, errNodeEvidenceInvalid
		}
		if relevant {
			result.Processes = append(result.Processes, process)
		}
	}
	sort.Slice(result.Processes, func(left, right int) bool {
		return result.Processes[left].PID < result.Processes[right].PID
	})
	var serveCount, supervisorCount, listenerCount int
	for _, process := range result.Processes {
		switch process.Role {
		case "agent":
			serveCount++
			if process.UID == 0 {
				return processEvidence{}, errNodeEvidenceInvalid
			}
		case "supervisor":
			supervisorCount++
			if process.UID != 0 {
				return processEvidence{}, errNodeEvidenceInvalid
			}
		case "runner_listener":
			listenerCount++
		default:
			return processEvidence{}, errNodeEvidenceInvalid
		}
	}
	expectedListeners := 0
	if phase == "running-before-restart" || phase == "running-after-restart" {
		expectedListeners = 1
	}
	if serveCount != 1 || supervisorCount != 1 || listenerCount != expectedListeners {
		return processEvidence{}, errNodeEvidenceInvalid
	}
	result.Status = "passed"
	return result, nil
}

func inspectRelevantProcess(procRoot string, pid int) (observedProcess, bool, error) {
	processRoot := filepath.Join(procRoot, strconv.Itoa(pid))
	cmdline, err := os.ReadFile(filepath.Join(processRoot, "cmdline"))
	if err != nil {
		return observedProcess{}, false, err
	}
	arguments := splitNullTerminated(cmdline)
	if len(arguments) == 0 {
		return observedProcess{}, false, nil
	}
	role := ""
	switch filepath.Base(arguments[0]) {
	case "tewake-agent":
		if len(arguments) < 2 {
			return observedProcess{}, false, nil
		}
		switch arguments[1] {
		case "serve":
			role = "agent"
		case "supervisor":
			role = "supervisor"
		}
	case "Runner.Listener":
		role = "runner_listener"
	}
	if role == "" {
		return observedProcess{}, false, nil
	}
	status, err := os.ReadFile(filepath.Join(processRoot, "status"))
	if err != nil {
		return observedProcess{}, false, err
	}
	uid, err := parseProcessUID(status)
	if err != nil {
		return observedProcess{}, false, err
	}
	return observedProcess{PID: pid, UID: uid, Role: role}, true, nil
}

func splitNullTerminated(payload []byte) []string {
	raw := bytes.Split(payload, []byte{0})
	result := make([]string, 0, len(raw))
	for _, value := range raw {
		if len(value) != 0 {
			result = append(result, string(value))
		}
	}
	return result
}

func parseProcessUID(status []byte) (int, error) {
	for _, line := range strings.Split(string(status), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "Uid:" {
			value, err := strconv.Atoi(fields[1])
			if err != nil || value < 0 {
				return 0, errNodeEvidenceInvalid
			}
			return value, nil
		}
	}
	return 0, errNodeEvidenceInvalid
}

func collectFilesystemEvidence(runtimeRoot string) (filesystemEvidence, error) {
	result := filesystemEvidence{
		Version:     evidenceVersion,
		Status:      "failed",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	rootInfo, err := os.Lstat(runtimeRoot)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return filesystemEvidence{}, errNodeEvidenceInvalid
	}
	result.RuntimeRootPresent = true
	executions := filepath.Join(runtimeRoot, "executions")
	executionEntries, err := os.ReadDir(executions)
	if err != nil {
		return filesystemEvidence{}, errNodeEvidenceInvalid
	}
	result.ExecutionEntries = len(executionEntries)
	err = filepath.WalkDir(runtimeRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			result.Symlinks++
			if entry.IsDir() {
				return filepath.SkipDir
			}
		}
		switch entry.Name() {
		case "_work":
			result.WorkDirectories++
		case ".runner":
			result.RunnerRegistrations++
		case ".credentials":
			result.CredentialFiles++
		case ".credentials_rsaparams":
			result.CredentialRSAParamFiles++
		case ".tewake-jit-canary":
			result.JITCanaryFiles++
		}
		return nil
	})
	if err != nil {
		return filesystemEvidence{}, errNodeEvidenceInvalid
	}
	if result.ExecutionEntries != 0 || result.Symlinks != 0 ||
		result.WorkDirectories != 0 || result.RunnerRegistrations != 0 ||
		result.CredentialFiles != 0 || result.CredentialRSAParamFiles != 0 ||
		result.JITCanaryFiles != 0 {
		return result, errNodeEvidenceInvalid
	}
	result.Status = "passed"
	return result, nil
}
