package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type serviceAuthority struct {
	Unit                string   `json:"unit"`
	FragmentPath        string   `json:"fragmentPath"`
	DropInPaths         []string `json:"dropInPaths"`
	EffectiveUnitSHA256 string   `json:"effectiveUnitSha256"`
	ExecStartArgv       []string `json:"execStartArgv"`
	Executable          string   `json:"executable"`
	Subcommand          string   `json:"subcommand"`
	RuntimeRoot         string   `json:"runtimeRoot"`
	MainPID             int      `json:"mainPid"`
	UID                 int      `json:"uid"`
	ControlGroup        string   `json:"controlGroup"`
	ProcessStartTicks   uint64   `json:"processStartTicks"`
	ExecutableSHA256    string   `json:"executableSha256"`
}

type authorityEvidence struct {
	Version     int              `json:"version"`
	Status      string           `json:"status"`
	BootID      string           `json:"bootId"`
	RuntimeRoot string           `json:"runtimeRoot"`
	RunnerUID   int              `json:"runnerUid"`
	Agent       serviceAuthority `json:"agent"`
	Supervisor  serviceAuthority `json:"supervisor"`
	GeneratedAt string           `json:"generatedAt"`
}

type authorityProbe interface {
	systemdProperty(string, string) (string, error)
	systemdUnitContent(string) ([]byte, error)
	lookupUID(string) (int, error)
	readFile(string) ([]byte, error)
	executableIdentity(int) (string, string, error)
	regularFileDigest(string) (string, error)
	goBuildVCS(string) (string, bool, error)
	officialRunnerAuthority(string) (string, string, int64, error)
	trustedRootFile(string) error
	gitState(string) (string, bool, error)
}

type liveAuthorityProbe struct{}

func (liveAuthorityProbe) systemdProperty(unit, property string) (string, error) {
	command := exec.Command("systemctl", "show", "--no-pager", "--property="+property, "--value", unit)
	output, err := command.Output()
	if err != nil {
		return "", errNodeEvidenceInvalid
	}
	return strings.TrimSpace(string(output)), nil
}

func (liveAuthorityProbe) systemdUnitContent(unit string) ([]byte, error) {
	command := exec.Command("systemctl", "cat", "--no-pager", unit)
	output, err := command.Output()
	if err != nil || len(output) == 0 {
		return nil, errNodeEvidenceInvalid
	}
	return output, nil
}

func (liveAuthorityProbe) gitState(repoRoot string) (string, bool, error) {
	commitCommand := exec.Command("git", "-C", repoRoot, "rev-parse", "--verify", "HEAD")
	commitOutput, err := commitCommand.Output()
	if err != nil {
		return "", false, errNodeEvidenceInvalid
	}
	statusCommand := exec.Command(
		"git", "-C", repoRoot, "status", "--porcelain=v1", "--untracked-files=all",
	)
	statusOutput, err := statusCommand.Output()
	if err != nil {
		return "", false, errNodeEvidenceInvalid
	}
	return strings.TrimSpace(string(commitOutput)), len(statusOutput) == 0, nil
}

func (liveAuthorityProbe) lookupUID(name string) (int, error) {
	account, err := user.Lookup(name)
	if err != nil {
		return 0, errNodeEvidenceInvalid
	}
	uid, err := strconv.Atoi(account.Uid)
	if err != nil || uid < 0 {
		return 0, errNodeEvidenceInvalid
	}
	return uid, nil
}

func (liveAuthorityProbe) readFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (liveAuthorityProbe) executableIdentity(pid int) (string, string, error) {
	procPath := filepath.Join("/proc", strconv.Itoa(pid), "exe")
	resolved, err := os.Readlink(procPath)
	if err != nil || !canonicalAbsolutePath(resolved) {
		return "", "", errNodeEvidenceInvalid
	}
	file, err := os.Open(procPath)
	if err != nil {
		return "", "", errNodeEvidenceInvalid
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return "", "", errNodeEvidenceInvalid
	}
	return resolved, hex.EncodeToString(digest.Sum(nil)), nil
}

func (liveAuthorityProbe) regularFileDigest(path string) (string, error) {
	return hashRegularFile(path)
}

func captureAuthorityEvidence(config liveConfig, repoRoot string, probe authorityProbe) error {
	if probe == nil {
		return errNodeEvidenceInvalid
	}
	if !canonicalAbsolutePath(repoRoot) {
		return errNodeEvidenceInvalid
	}
	if err := trustedDirectory(config.RuntimeRoot, 0o711); err != nil {
		return errNodeEvidenceInvalid
	}
	bootRaw, err := probe.readFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return errNodeEvidenceInvalid
	}
	bootID := strings.TrimSpace(string(bootRaw))
	if !safeIdentifier(bootID, 64) {
		return errNodeEvidenceInvalid
	}
	agentUID, err := probe.lookupUID("sparerunner-agent")
	if err != nil || agentUID <= 0 {
		return errNodeEvidenceInvalid
	}
	runnerUID, err := probe.lookupUID("sparerunner-runner-0")
	if err != nil || runnerUID <= 0 || runnerUID == agentUID {
		return errNodeEvidenceInvalid
	}
	agent, err := readServiceAuthority(
		probe,
		"sparerunner-agent.service",
		"serve",
		config.RuntimeRoot,
		config.Provenance.ExpectedAgentUnitFragmentPath,
		agentUID,
	)
	if err != nil {
		return err
	}
	supervisor, err := readServiceAuthority(
		probe,
		"sparerunner-supervisor.service",
		"supervisor",
		config.RuntimeRoot,
		config.Provenance.ExpectedSupervisorUnitFragmentPath,
		0,
	)
	if err != nil {
		return err
	}
	evidence, err := openEvidenceStore(config.EvidenceDirectory)
	if err != nil {
		return err
	}
	authority := authorityEvidence{
		Version:     evidenceVersion,
		Status:      "passed",
		BootID:      bootID,
		RuntimeRoot: config.RuntimeRoot,
		RunnerUID:   runnerUID,
		Agent:       agent,
		Supervisor:  supervisor,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := evidence.writeJSON(authorityFileName, authority); err != nil {
		return err
	}
	if err := captureProvenanceEvidence(config, repoRoot, authority, probe, evidence); err != nil {
		_ = os.Remove(filepath.Join(config.EvidenceDirectory, authorityFileName))
		return err
	}
	return nil
}

func readServiceAuthority(
	probe authorityProbe,
	unit string,
	subcommand string,
	runtimeRoot string,
	expectedFragmentPath string,
	expectedUID int,
) (serviceAuthority, error) {
	execStart, err := probe.systemdProperty(unit, "ExecStart")
	if err != nil {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	expectedArgv := expectedServiceArgv(subcommand, runtimeRoot)
	actualArgv, err := parseEffectiveExecStart(execStart)
	if err != nil || !stringSlicesEqual(actualArgv, expectedArgv) {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	executable := actualArgv[0]
	fragmentPath, err := probe.systemdProperty(unit, "FragmentPath")
	if err != nil || fragmentPath != expectedFragmentPath ||
		probe.trustedRootFile(fragmentPath) != nil {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	rawDropIns, err := probe.systemdProperty(unit, "DropInPaths")
	if err != nil {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	dropIns := strings.Fields(rawDropIns)
	for _, path := range dropIns {
		if !canonicalAbsolutePath(path) || probe.trustedRootFile(path) != nil {
			return serviceAuthority{}, errNodeEvidenceInvalid
		}
	}
	unitContent, err := probe.systemdUnitContent(unit)
	if err != nil {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	unitDigest := sha256.Sum256(unitContent)
	rawPID, err := probe.systemdProperty(unit, "MainPID")
	if err != nil {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	pid, err := strconv.Atoi(rawPID)
	if err != nil || pid <= 0 {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	controlGroup, err := probe.systemdProperty(unit, "ControlGroup")
	if err != nil || !strings.HasPrefix(controlGroup, "/") ||
		filepath.Clean(controlGroup) != controlGroup {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	uid, err := processUIDFromProbe(probe, pid)
	if err != nil || uid != expectedUID {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	startTicks, err := processStartTicks(probe, pid)
	if err != nil {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	cgroup, err := processCgroup(probe, pid)
	if err != nil || cgroup != controlGroup {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	resolvedExecutable, executableDigest, err := probe.executableIdentity(pid)
	if err != nil || resolvedExecutable != executable ||
		len(executableDigest) != sha256.Size*2 {
		return serviceAuthority{}, errNodeEvidenceInvalid
	}
	return serviceAuthority{
		Unit: unit, FragmentPath: fragmentPath, DropInPaths: dropIns,
		EffectiveUnitSHA256: hex.EncodeToString(unitDigest[:]), ExecStartArgv: actualArgv,
		Executable: executable, Subcommand: subcommand,
		RuntimeRoot: runtimeRoot, MainPID: pid, UID: uid,
		ControlGroup: controlGroup, ProcessStartTicks: startTicks,
		ExecutableSHA256: executableDigest,
	}, nil
}

func parseEffectiveExecStart(value string) ([]string, error) {
	segments := strings.Split(value, ";")
	if len(segments) < 3 {
		return nil, errNodeEvidenceInvalid
	}
	knownMetadata := map[string]struct{}{
		"ignore_errors": {}, "start_time": {}, "stop_time": {},
		"pid": {}, "code": {}, "status": {},
	}
	values := make(map[string]string, len(segments))
	for _, rawSegment := range segments {
		segment := strings.Trim(strings.TrimSpace(rawSegment), "{} ")
		if segment == "" {
			continue
		}
		separator := strings.IndexByte(segment, '=')
		if separator <= 0 {
			return nil, errNodeEvidenceInvalid
		}
		key := strings.TrimSpace(segment[:separator])
		if _, duplicate := values[key]; duplicate {
			return nil, errNodeEvidenceInvalid
		}
		if key != "path" && key != "argv[]" {
			if _, allowed := knownMetadata[key]; !allowed {
				return nil, errNodeEvidenceInvalid
			}
		}
		values[key] = strings.TrimSpace(segment[separator+1:])
	}
	if len(values) < 2 {
		return nil, errNodeEvidenceInvalid
	}
	declaredExecutable := strings.Trim(values["path"], "\"'")
	argv := strings.Fields(values["argv[]"])
	for index := range argv {
		argv[index] = strings.Trim(argv[index], "\"'")
	}
	if declaredExecutable != "/usr/bin/sparerunner-agent" ||
		len(argv) < 2 || argv[0] != declaredExecutable ||
		strings.Count(value, "/usr/bin/sparerunner-agent") != 2 {
		return nil, errNodeEvidenceInvalid
	}
	return argv, nil
}

func expectedServiceArgv(subcommand, runtimeRoot string) []string {
	switch subcommand {
	case "serve":
		return []string{
			"/usr/bin/sparerunner-agent", "serve",
			"--state-dir=/var/lib/sparerunner-agent",
			"--cache-root=/var/cache/sparerunner-agent",
			"--runtime-root=" + runtimeRoot,
			"--supervisor-socket=/run/sparerunner-supervisor/supervisor.sock",
			"--require-native-runner",
		}
	case "supervisor":
		return []string{
			"/usr/bin/sparerunner-agent", "supervisor",
			"--socket=/run/sparerunner-supervisor/supervisor.sock",
			"--runtime-root=" + runtimeRoot,
			"--cache-root=/var/cache/sparerunner-agent",
			"--fence-root=/var/lib/sparerunner-supervisor/fences",
			"--runner-user=sparerunner-runner-0",
			"--agent-user=sparerunner-agent",
		}
	default:
		return nil
	}
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func processUIDFromProbe(probe authorityProbe, pid int) (int, error) {
	status, err := probe.readFile(filepath.Join("/proc", strconv.Itoa(pid), "status"))
	if err != nil {
		return 0, err
	}
	return parseProcessUID(status)
}

func processStartTicks(probe authorityProbe, pid int) (uint64, error) {
	payload, err := probe.readFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, errNodeEvidenceInvalid
	}
	closeParen := bytes.LastIndexByte(payload, ')')
	if closeParen < 0 {
		return 0, errNodeEvidenceInvalid
	}
	fields := strings.Fields(string(payload[closeParen+1:]))
	if len(fields) <= 19 {
		return 0, errNodeEvidenceInvalid
	}
	value, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || value == 0 {
		return 0, errNodeEvidenceInvalid
	}
	return value, nil
}

func processCgroup(probe authorityProbe, pid int) (string, error) {
	payload, err := probe.readFile(filepath.Join("/proc", strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return "", errNodeEvidenceInvalid
	}
	lines := strings.Split(strings.TrimSpace(string(payload)), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "0::/") {
		return "", errNodeEvidenceInvalid
	}
	value := strings.TrimPrefix(lines[0], "0::")
	if filepath.Clean(value) != value {
		return "", errNodeEvidenceInvalid
	}
	return value, nil
}
