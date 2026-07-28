//go:build darwin

package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"debug/buildinfo"
	"encoding/hex"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/genm/sparerunner/internal/enroll"
	"github.com/genm/sparerunner/internal/runner"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const (
	privateMaterialProbeArgument = "--sparerunner-live-private-material-probe"
	runnerCouldReadExitCode      = 77
)

func captureMacOSNode(
	ctx context.Context,
	config macOSLiveConfig,
	phase capturePhase,
) (nodeEvidence, error) {
	if ctx == nil || ctx.Err() != nil || os.Geteuid() != 0 {
		return nodeEvidence{}, errMacOSEvidenceInvalid
	}
	if err := config.validate(); err != nil {
		return nodeEvidence{}, err
	}
	if _, err := parseCapturePhase(string(phase)); err != nil {
		return nodeEvidence{}, errMacOSEvidenceInvalid
	}
	identity, err := lookupRunnerIdentity(runnerAccountName)
	if err != nil {
		return nodeEvidence{}, err
	}
	provenance, err := captureProvenance(config)
	if err != nil {
		return nodeEvidence{}, err
	}
	bootEpoch, err := macOSBootEpoch()
	if err != nil {
		return nodeEvidence{}, err
	}
	agent, instances, state, err := captureLaunchDaemon(ctx)
	if err != nil {
		return nodeEvidence{}, err
	}
	processes, err := runnerIdentityProcesses(ctx, identity.uid)
	if err != nil {
		return nodeEvidence{}, err
	}
	execution, controllerEpoch, err := captureJournal(ctx, config.ExecutionID)
	if err != nil {
		return nodeEvidence{}, err
	}
	executionDirectories, err := countOwnedRuntimeDirectories(
		config.ExecutionID,
		identity,
	)
	if err != nil {
		return nodeEvidence{}, err
	}
	fenceDirectories, err := countOwnedFenceDirectories(config.ExecutionID)
	if err != nil {
		return nodeEvidence{}, err
	}
	privateMaterial, err := capturePrivateMaterial(
		ctx,
		config.HarnessPath,
		identity,
	)
	if err != nil {
		return nodeEvidence{}, err
	}
	result := nodeEvidence{
		Version:              macOSEvidenceVersion,
		Phase:                phase,
		CapturedAt:           time.Now().UTC().Format(time.RFC3339Nano),
		BootEpoch:            bootEpoch,
		Architecture:         runtime.GOARCH,
		LaunchDaemonState:    state,
		Agent:                agent,
		AgentInstances:       instances,
		RunnerUID:            identity.uid,
		RunnerGID:            identity.gid,
		RunnerProcesses:      processes,
		ControllerEpoch:      controllerEpoch,
		Execution:            execution,
		ExecutionDirectories: executionDirectories,
		FenceDirectories:     fenceDirectories,
		PrivateMaterial:      privateMaterial,
		Provenance:           provenance,
	}
	if validateNodeEvidenceShape(result) != nil {
		return nodeEvidence{}, errMacOSEvidenceInvalid
	}
	return result, nil
}

type numericIdentity struct {
	uid int
	gid int
}

func lookupRunnerIdentity(name string) (numericIdentity, error) {
	account, err := user.Lookup(name)
	if err != nil {
		return numericIdentity{}, errMacOSEvidenceInvalid
	}
	uid, uidErr := strconv.Atoi(account.Uid)
	gid, gidErr := strconv.Atoi(account.Gid)
	if uidErr != nil || gidErr != nil || uid <= 0 || gid <= 0 {
		return numericIdentity{}, errMacOSEvidenceInvalid
	}
	return numericIdentity{uid: uid, gid: gid}, nil
}

func captureProvenance(config macOSLiveConfig) (provenanceEvidence, error) {
	resolvedHarness, err := filepath.EvalSymlinks(config.HarnessPath)
	if err != nil {
		return provenanceEvidence{}, errMacOSEvidenceInvalid
	}
	executable, err := os.Executable()
	if err != nil {
		return provenanceEvidence{}, errMacOSEvidenceInvalid
	}
	resolvedExecutable, err := filepath.EvalSymlinks(executable)
	if err != nil || resolvedExecutable != resolvedHarness {
		return provenanceEvidence{}, errMacOSEvidenceInvalid
	}
	harnessDigest, err := hashTrustedFile(
		resolvedHarness,
		config.ExpectedHarnessSHA256,
		0o755,
		0,
	)
	if err != nil || validateBuildCommit(resolvedHarness, config.ExpectedCommitSHA) != nil {
		return provenanceEvidence{}, errMacOSEvidenceInvalid
	}
	agentDigest, err := hashTrustedFile(
		installedAgentPath,
		config.ExpectedInstalledAgentSHA256,
		0o755,
		0,
	)
	if err != nil || validateBuildCommit(installedAgentPath, config.ExpectedCommitSHA) != nil {
		return provenanceEvidence{}, errMacOSEvidenceInvalid
	}
	plistDigest, err := hashTrustedFile(
		installedPlistPath,
		config.ExpectedLaunchDaemonSHA256,
		0o600,
		0,
	)
	if err != nil || validateInstalledPlist() != nil {
		return provenanceEvidence{}, errMacOSEvidenceInvalid
	}
	pkg, err := runner.OfficialPackage(runner.Platform{
		OS: "darwin", Arch: config.ExpectedArchitecture,
	})
	if err != nil {
		return provenanceEvidence{}, errMacOSEvidenceInvalid
	}
	key, err := pkg.CacheKey()
	if err != nil {
		return provenanceEvidence{}, errMacOSEvidenceInvalid
	}
	archivePath := filepath.Join(cacheRootPath, "packages", key, "archive")
	packageDigest, err := hashTrustedFile(
		archivePath,
		config.ExpectedRunnerPackageSHA256,
		0o444,
		pkg.Size,
	)
	if err != nil {
		return provenanceEvidence{}, errMacOSEvidenceInvalid
	}
	return provenanceEvidence{
		CommitSHA:           config.ExpectedCommitSHA,
		HarnessSHA256:       harnessDigest,
		AgentSHA256:         agentDigest,
		LaunchDaemonSHA256:  plistDigest,
		RunnerPackageSHA256: packageDigest,
	}, nil
}

func hashTrustedFile(
	path string,
	expectedDigest string,
	expectedMode os.FileMode,
	expectedSize int64,
) (string, error) {
	if !canonicalAbsolutePath(path) {
		return "", errMacOSEvidenceInvalid
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() ||
		before.Mode().Perm() != expectedMode ||
		!ownedByRoot(before) || !singleLink(before) ||
		(expectedSize > 0 && before.Size() != expectedSize) {
		return "", errMacOSEvidenceInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return "", errMacOSEvidenceInvalid
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return "", errMacOSEvidenceInvalid
	}
	hash := sha256.New()
	copied, err := io.Copy(hash, file)
	if err != nil || copied != before.Size() {
		return "", errMacOSEvidenceInvalid
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || after.Size() != before.Size() {
		return "", errMacOSEvidenceInvalid
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if digest != expectedDigest {
		return "", errMacOSEvidenceInvalid
	}
	return digest, nil
}

func validateBuildCommit(path, commit string) error {
	info, err := buildinfo.ReadFile(path)
	if err != nil || info.Path == "" {
		return errMacOSEvidenceInvalid
	}
	settings := make(map[string]string, len(info.Settings))
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	if settings["vcs.revision"] != commit || settings["vcs.modified"] != "false" {
		return errMacOSEvidenceInvalid
	}
	return nil
}

func validateInstalledPlist() error {
	for key, expected := range map[string]string{
		"Label":              launchDaemonLabel,
		"ProgramArguments.0": installedAgentPath,
		"UserName":           "root",
		"GroupName":          "wheel",
	} {
		command := exec.Command(
			"/usr/bin/plutil",
			"-extract", key, "raw", "-o", "-", installedPlistPath,
		)
		output, err := command.Output()
		if err != nil || strings.TrimSpace(string(output)) != expected {
			return errMacOSEvidenceInvalid
		}
	}
	return nil
}

func captureLaunchDaemon(
	ctx context.Context,
) (processEvidence, int, string, error) {
	output, err := exec.CommandContext(
		ctx,
		"/bin/launchctl",
		"print",
		"system/"+launchDaemonLabel,
	).Output()
	if err != nil {
		return processEvidence{}, 0, "", errMacOSEvidenceInvalid
	}
	pid := 0
	state := ""
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "pid = "):
			value, parseErr := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "pid = ")))
			if parseErr != nil || pid != 0 {
				return processEvidence{}, 0, "", errMacOSEvidenceInvalid
			}
			pid = value
		case strings.HasPrefix(line, "state = "):
			if state != "" {
				return processEvidence{}, 0, "", errMacOSEvidenceInvalid
			}
			state = strings.TrimSpace(strings.TrimPrefix(line, "state = "))
		}
	}
	if pid <= 1 || state != "running" {
		return processEvidence{}, 0, "", errMacOSEvidenceInvalid
	}
	processes, err := exec.CommandContext(
		ctx,
		"/bin/ps",
		"-axo",
		"pid=,uid=,pgid=,comm=",
	).Output()
	if err != nil {
		return processEvidence{}, 0, "", errMacOSEvidenceInvalid
	}
	instances := 0
	var agent processEvidence
	for _, line := range strings.Split(string(processes), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		command := strings.Join(fields[3:], " ")
		if command != installedAgentPath {
			continue
		}
		observedPID, pidErr := strconv.Atoi(fields[0])
		uid, uidErr := strconv.Atoi(fields[1])
		pgid, pgidErr := strconv.Atoi(fields[2])
		if pidErr != nil || uidErr != nil || pgidErr != nil {
			return processEvidence{}, 0, "", errMacOSEvidenceInvalid
		}
		instances++
		if observedPID == pid {
			agent = processEvidence{PID: observedPID, UID: uid, PGID: pgid}
		}
	}
	if agent.PID != pid || agent.UID != 0 || agent.PGID <= 0 || instances < 1 {
		return processEvidence{}, 0, "", errMacOSEvidenceInvalid
	}
	return agent, instances, state, nil
}

func runnerIdentityProcesses(
	ctx context.Context,
	runnerUID int,
) ([]processEvidence, error) {
	if ctx == nil || ctx.Err() != nil || runnerUID <= 0 {
		return nil, errMacOSEvidenceInvalid
	}
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, errMacOSEvidenceInvalid
	}
	result := make([]processEvidence, 0)
	for _, process := range processes {
		pid := int(process.Proc.P_pid)
		ruid := int(process.Eproc.Pcred.P_ruid)
		euid := int(process.Eproc.Ucred.Uid)
		if pid <= 1 || (ruid != runnerUID && euid != runnerUID) {
			continue
		}
		pgid := int(process.Eproc.Pgid)
		if pgid <= 0 {
			return nil, errMacOSEvidenceInvalid
		}
		result = append(result, processEvidence{
			PID: pid, PGID: pgid, UID: runnerUID,
		})
	}
	sortProcessEvidence(result)
	return result, nil
}

func captureJournal(
	ctx context.Context,
	executionID string,
) (executionEvidence, uint64, error) {
	uri := &url.URL{Scheme: "file", Path: agentDatabasePath}
	query := uri.Query()
	query.Set("mode", "ro")
	uri.RawQuery = query.Encode()
	database, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return executionEvidence{}, 0, errMacOSEvidenceInvalid
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	if _, err := database.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
		return executionEvidence{}, 0, errMacOSEvidenceInvalid
	}
	var check string
	if err := database.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&check); err != nil ||
		check != "ok" {
		return executionEvidence{}, 0, errMacOSEvidenceInvalid
	}
	var role, rawEpoch string
	if err := database.QueryRowContext(
		ctx,
		"SELECT value FROM store_metadata WHERE key = 'role'",
	).Scan(&role); err != nil || role != "agent" {
		return executionEvidence{}, 0, errMacOSEvidenceInvalid
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT value FROM store_metadata WHERE key = 'max_controller_epoch'",
	).Scan(&rawEpoch); err != nil {
		return executionEvidence{}, 0, errMacOSEvidenceInvalid
	}
	controllerEpoch, err := strconv.ParseUint(rawEpoch, 10, 64)
	if err != nil {
		return executionEvidence{}, 0, errMacOSEvidenceInvalid
	}
	var state string
	var pid, tombstone int
	var hostEpoch string
	var revision uint64
	err = database.QueryRowContext(
		ctx,
		`SELECT state, pid, containment_host_epoch, revision, tombstone
		 FROM runner_journal_records WHERE execution_id = ?`,
		executionID,
	).Scan(&state, &pid, &hostEpoch, &revision, &tombstone)
	if errors.Is(err, sql.ErrNoRows) {
		return executionEvidence{}, controllerEpoch, nil
	}
	if err != nil || !validRunnerState(runner.State(state)) ||
		pid < 0 || revision == 0 || (tombstone != 0 && tombstone != 1) ||
		!lowerHex(hostEpoch, 64) {
		return executionEvidence{}, 0, errMacOSEvidenceInvalid
	}
	return executionEvidence{
		Found: true, State: runner.State(state), PID: pid,
		HostEpoch: hostEpoch, Revision: revision, HasTombstone: tombstone == 1,
	}, controllerEpoch, nil
}

func validRunnerState(state runner.State) bool {
	switch state {
	case runner.StatePreparing, runner.StatePrepared, runner.StateStarting,
		runner.StateRunning, runner.StateCleaning, runner.StateReleased,
		runner.StateFailed, runner.StateCleanupFailed:
		return true
	default:
		return false
	}
}

func countOwnedRuntimeDirectories(
	executionID string,
	identity numericIdentity,
) (int, error) {
	expected := sha256.Sum256([]byte(executionID))
	expectedName := hex.EncodeToString(expected[:])
	return countExactDirectories(
		executionsRootPath,
		expectedName,
		identity.uid,
		identity.gid,
		false,
	)
}

func countOwnedFenceDirectories(executionID string) (int, error) {
	expected := sha256.Sum256([]byte(executionID))
	expectedName := "sparerunner-" + hex.EncodeToString(expected[:])
	return countExactDirectories(fenceRootPath, expectedName, 0, 0, true)
}

func countExactDirectories(
	root, expectedName string,
	expectedUID, expectedGID int,
	requireRootPrivate bool,
) (int, error) {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		!ownedByRoot(info) ||
		(requireRootPrivate && info.Mode().Perm() != 0o700) {
		return 0, errMacOSEvidenceInvalid
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, errMacOSEvidenceInvalid
	}
	count := 0
	for _, entry := range entries {
		if entry.Name() != expectedName || !entry.IsDir() ||
			entry.Type()&os.ModeSymlink != 0 {
			return 0, errMacOSEvidenceInvalid
		}
		member, err := os.Lstat(filepath.Join(root, entry.Name()))
		if err != nil || !ownedBy(member, expectedUID, expectedGID) ||
			member.Mode().Perm() != 0o700 {
			return 0, errMacOSEvidenceInvalid
		}
		count++
	}
	return count, nil
}

func capturePrivateMaterial(
	ctx context.Context,
	harnessPath string,
	identity numericIdentity,
) (privateMaterialEvidence, error) {
	locatorInfo, err := os.Lstat(nodeKeyLocatorPath)
	if err != nil || !locatorInfo.Mode().IsRegular() ||
		locatorInfo.Mode().Perm() != 0o600 || !ownedByRoot(locatorInfo) ||
		!singleLink(locatorInfo) {
		return privateMaterialEvidence{}, errMacOSEvidenceInvalid
	}
	locator, err := readStrictRegularFile(nodeKeyLocatorPath, 64<<10)
	if err != nil {
		return privateMaterialEvidence{}, errMacOSEvidenceInvalid
	}
	containsSecret := bytesContainPrivateMaterial(locator)
	material, err := enroll.LoadPrivateMaterial(nodeKeyLocatorPath)
	if err != nil || len(material) == 0 {
		clear(material)
		return privateMaterialEvidence{}, errMacOSEvidenceInvalid
	}
	clear(material)
	command := exec.CommandContext(
		ctx,
		harnessPath,
		privateMaterialProbeArgument,
		nodeKeyLocatorPath,
	)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	command.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    uint32(identity.uid),
			Gid:    uint32(identity.gid),
			Groups: []uint32{uint32(identity.gid)},
		},
	}
	err = command.Run()
	if err == nil {
		return privateMaterialEvidence{
			ServiceCanLoad: true, RunnerAccountDenied: true,
			LocatorContainsSecret: containsSecret,
		}, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == runnerCouldReadExitCode {
		return privateMaterialEvidence{}, errMacOSEvidenceInvalid
	}
	return privateMaterialEvidence{}, errMacOSEvidenceInvalid
}

func runPrivateMaterialProbe(args []string) (bool, int) {
	if len(args) == 0 || args[0] != privateMaterialProbeArgument {
		return false, 0
	}
	if len(args) != 2 || !canonicalAbsolutePath(args[1]) {
		return true, 1
	}
	material, err := enroll.LoadPrivateMaterial(args[1])
	if err != nil {
		return true, 0
	}
	clear(material)
	return true, runnerCouldReadExitCode
}

func bytesContainPrivateMaterial(contents []byte) bool {
	upper := strings.ToUpper(string(contents))
	return strings.Contains(upper, "PRIVATE KEY") ||
		strings.Contains(upper, "BEGIN CERTIFICATE") ||
		len(contents) > 4096
}

func macOSBootEpoch() (string, error) {
	boot, err := unix.SysctlTimeval("kern.boottime")
	if err != nil || boot == nil || boot.Sec <= 0 {
		return "", errMacOSEvidenceInvalid
	}
	value := strconv.FormatInt(boot.Sec, 10) + ":" +
		strconv.FormatInt(int64(boot.Usec), 10)
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:]), nil
}

func ownedByRoot(info os.FileInfo) bool {
	return ownedBy(info, 0, 0)
}

func ownedBy(info os.FileInfo, uid, gid int) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == uid && int(stat.Gid) == gid
}

func singleLink(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}

func classifyMacOSLiveError(error) string {
	return "acceptance_failed"
}
