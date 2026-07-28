package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/github"
)

const finalEvidenceMaxDelay = 5 * time.Minute

func validateFinalEvidence(
	config liveConfig,
	mode acceptanceMode,
	now time.Time,
) error {
	if err := config.validate(); err != nil || now.IsZero() {
		return errEvidenceInvalid
	}
	result, err := loadEvidenceFile[resultEvidence](config.EvidenceDirectory, resultFileName)
	if err != nil || validatePassingResult(result, config, mode, now) != nil {
		return errEvidenceInvalid
	}
	startedAt, _ := time.Parse(time.RFC3339Nano, result.StartedAt)
	finishedAt, _ := time.Parse(time.RFC3339Nano, result.FinishedAt)
	privateProof, err := loadPrivateRepositoryProof(config, startedAt)
	if err != nil ||
		strings.ToLower(privateProof.Repository) != result.PrivateRepository {
		return errEvidenceInvalid
	}
	provenance, err := loadEvidenceFile[provenanceEvidence](
		config.EvidenceDirectory,
		provenanceFileName,
	)
	if err != nil || validateProvenanceManifest(provenance, config, result, startedAt) != nil {
		return errEvidenceInvalid
	}
	authority, err := loadEvidenceFile[authorityEvidence](
		config.EvidenceDirectory,
		authorityFileName,
	)
	if err != nil || validateAuthorityManifest(authority, config, startedAt) != nil {
		return errEvidenceInvalid
	}

	before, err := loadEvidenceFile[processEvidence](
		config.EvidenceDirectory,
		processBeforeName,
	)
	beforeAnchor := startedAt
	beforeMaxAge := finalEvidenceMaxDelay
	if mode == modeCommitBeforeAck {
		beforeAnchor, _ = time.Parse(time.RFC3339Nano, result.AvailableObservedAt)
		beforeMaxAge = config.runTimeout()
	}
	if err != nil ||
		validateProcessManifest(before, "before") != nil ||
		validateProcessAuthority(before, authority, result.ExecutionID, "before") != nil ||
		validateBeforeTime(before.GeneratedAt, beforeAnchor, beforeMaxAge) != nil {
		return errEvidenceInvalid
	}

	switch mode {
	case modeNormal:
		if err := requireEvidenceAbsent(config.EvidenceDirectory, injectorFileName); err != nil {
			return err
		}
		for _, name := range []string{
			replayFileName,
			processRunningBeforeRestartName,
			processRunningAfterRestartName,
			restartStartedName,
		} {
			if err := requireEvidenceAbsent(config.EvidenceDirectory, name); err != nil {
				return err
			}
		}
		return validateCleanupSuccessEvidence(config.EvidenceDirectory, finishedAt, now, authority, result)
	case modeCommitBeforeAck:
		if err := requireEvidenceAbsent(config.EvidenceDirectory, injectorFileName); err != nil {
			return err
		}
		for _, name := range []string{
			processRunningBeforeRestartName,
			processRunningAfterRestartName,
			restartStartedName,
		} {
			if err := requireEvidenceAbsent(config.EvidenceDirectory, name); err != nil {
				return err
			}
		}
		replay, err := loadEvidenceFile[replayEvidence](
			config.EvidenceDirectory,
			replayFileName,
		)
		if err != nil || validateCompletedReplay(replay, result, startedAt, finishedAt) != nil {
			return errEvidenceInvalid
		}
		return validateCleanupSuccessEvidence(config.EvidenceDirectory, finishedAt, now, authority, result)
	case modeCleanupFailure:
		for _, name := range []string{
			replayFileName,
			processAfterName,
			processRunningBeforeRestartName,
			processRunningAfterRestartName,
			filesystemName,
			restartStartedName,
		} {
			if err := requireEvidenceAbsent(config.EvidenceDirectory, name); err != nil {
				return err
			}
		}
		injector, err := loadEvidenceFile[injectorEvidence](
			config.EvidenceDirectory,
			injectorFileName,
		)
		if err != nil || validateInjectorManifest(injector, startedAt, finishedAt) != nil {
			return errEvidenceInvalid
		}
		return nil
	case modeAgentRestart:
		if err := requireEvidenceAbsent(config.EvidenceDirectory, injectorFileName); err != nil {
			return err
		}
		if err := requireEvidenceAbsent(config.EvidenceDirectory, replayFileName); err != nil {
			return err
		}
		if err := validateAgentRestartEvidence(config, result, startedAt, finishedAt, authority); err != nil {
			return err
		}
		return validateCleanupSuccessEvidence(config.EvidenceDirectory, finishedAt, now, authority, result)
	default:
		return errEvidenceInvalid
	}
}

func validatePassingResult(
	result resultEvidence,
	config liveConfig,
	mode acceptanceMode,
	now time.Time,
) error {
	if result.Version != evidenceVersion ||
		result.Mode != string(mode) ||
		result.Status != "passed" ||
		result.ErrorClass != "" ||
		result.NodeID != config.NodeID {
		return errEvidenceInvalid
	}
	if result.ProvenanceCommitSHA != config.Provenance.ExpectedCommitSHA ||
		!lowerHexDigest(result.HarnessSHA256, 64) {
		return errEvidenceInvalid
	}
	repository, err := repositoryFromConfigURL(config.GitHub.ConfigURL)
	if err != nil || result.PrivateRepository != repository {
		return errEvidenceInvalid
	}
	startedAt, err := time.Parse(time.RFC3339Nano, result.StartedAt)
	if err != nil {
		return errEvidenceInvalid
	}
	finishedAt, err := time.Parse(time.RFC3339Nano, result.FinishedAt)
	if err != nil || finishedAt.Before(startedAt) ||
		finishedAt.Sub(startedAt) > config.runTimeout() ||
		finishedAt.After(now.Add(time.Minute)) ||
		now.Sub(finishedAt) > finalEvidenceMaxDelay {
		return errEvidenceInvalid
	}
	if result.TargetID == "" || result.ScaleSetID <= 0 ||
		result.ControllerEpoch == 0 || result.ExecutionID == "" ||
		result.RunnerRequestID <= 0 || result.AvailableToStartedMillis == nil ||
		*result.AvailableToStartedMillis < 0 ||
		*result.AvailableToStartedMillis > 60_000 {
		return errEvidenceInvalid
	}
	availableAt, err := time.Parse(time.RFC3339Nano, result.AvailableObservedAt)
	if err != nil {
		return errEvidenceInvalid
	}
	jobStartedAt, err := time.Parse(time.RFC3339Nano, result.JobStartedObservedAt)
	if err != nil || jobStartedAt.Before(availableAt) ||
		jobStartedAt.Sub(availableAt).Milliseconds() !=
			*result.AvailableToStartedMillis {
		return errEvidenceInvalid
	}
	jobCompletedAt, err := time.Parse(time.RFC3339Nano, result.JobCompletedObservedAt)
	if err != nil || jobCompletedAt.Before(jobStartedAt) ||
		jobCompletedAt.After(finishedAt) {
		return errEvidenceInvalid
	}
	requiredEvents := map[string]bool{
		string(github.MessageTypeJobAvailable): false,
		string(github.MessageTypeJobStarted):   false,
		string(github.MessageTypeJobCompleted): false,
	}
	for _, event := range result.ObservedEvents {
		required, ok := requiredEvents[event]
		if ok && required {
			return errEvidenceInvalid
		}
		switch github.MessageType(event) {
		case github.MessageTypeJobAvailable,
			github.MessageTypeJobAssigned,
			github.MessageTypeJobStarted,
			github.MessageTypeJobCompleted:
		default:
			return errEvidenceInvalid
		}
		if ok {
			requiredEvents[event] = true
		}
	}
	for _, found := range requiredEvents {
		if !found {
			return errEvidenceInvalid
		}
	}
	switch mode {
	case modeNormal, modeCommitBeforeAck, modeAgentRestart:
		if result.ExecutionState != string(domain.ExecutionReleased) ||
			result.NodeState != string(domain.NodeActive) ||
			result.ReservationCount != 0 {
			return errEvidenceInvalid
		}
	case modeCleanupFailure:
		if (result.ExecutionState != string(domain.ExecutionCleanupFailed) &&
			result.ExecutionState != string(domain.ExecutionQuarantined)) ||
			result.NodeState != string(domain.NodeQuarantined) ||
			result.ReservationCount != 1 {
			return errEvidenceInvalid
		}
	default:
		return errEvidenceInvalid
	}
	return nil
}

func validateCleanupSuccessEvidence(
	directory string,
	finishedAt time.Time,
	now time.Time,
	authority authorityEvidence,
	result resultEvidence,
) error {
	after, err := loadEvidenceFile[processEvidence](directory, processAfterName)
	if err != nil ||
		validateProcessManifest(after, "after") != nil ||
		validateProcessAuthority(after, authority, result.ExecutionID, "after") != nil ||
		validateAfterTime(after.GeneratedAt, finishedAt, now) != nil {
		return errEvidenceInvalid
	}
	filesystem, err := loadEvidenceFile[filesystemEvidence](directory, filesystemName)
	if err != nil ||
		validateFilesystemManifest(filesystem) != nil ||
		validateAfterTime(filesystem.GeneratedAt, finishedAt, now) != nil {
		return errEvidenceInvalid
	}
	return nil
}

func validateAuthorityManifest(
	authority authorityEvidence,
	config liveConfig,
	startedAt time.Time,
) error {
	if authority.Version != evidenceVersion || authority.Status != "passed" ||
		authority.RuntimeRoot != config.RuntimeRoot || authority.BootID == "" ||
		authority.RunnerUID <= 0 ||
		authority.Agent.Unit != "tewake-agent.service" ||
		authority.Supervisor.Unit != "tewake-supervisor.service" ||
		authority.Agent.RuntimeRoot != config.RuntimeRoot ||
		authority.Supervisor.RuntimeRoot != config.RuntimeRoot ||
		authority.Agent.MainPID <= 0 || authority.Supervisor.MainPID <= 0 ||
		authority.Agent.ProcessStartTicks == 0 ||
		authority.Supervisor.ProcessStartTicks == 0 ||
		authority.Agent.ExecutableSHA256 != config.Provenance.ExpectedInstalledAgentSHA256 ||
		authority.Supervisor.ExecutableSHA256 != config.Provenance.ExpectedInstalledAgentSHA256 ||
		authority.Agent.FragmentPath !=
			config.Provenance.ExpectedAgentUnitFragmentPath ||
		authority.Supervisor.FragmentPath !=
			config.Provenance.ExpectedSupervisorUnitFragmentPath ||
		authority.Agent.EffectiveUnitSHA256 != config.Provenance.ExpectedAgentUnitSHA256 ||
		authority.Supervisor.EffectiveUnitSHA256 !=
			config.Provenance.ExpectedSupervisorUnitSHA256 ||
		!stringSlicesEqual(authority.Agent.ExecStartArgv, expectedServiceArgv("serve", config.RuntimeRoot)) ||
		!stringSlicesEqual(
			authority.Supervisor.ExecStartArgv,
			expectedServiceArgv("supervisor", config.RuntimeRoot),
		) {
		return errEvidenceInvalid
	}
	return validateBeforeTime(authority.GeneratedAt, startedAt, finalEvidenceMaxDelay)
}

func validateProvenanceManifest(
	provenance provenanceEvidence,
	config liveConfig,
	result resultEvidence,
	startedAt time.Time,
) error {
	expectedRunnerPath, expectedRunner, err := expectedOfficialRunnerAuthority(
		config.RuntimeRoot,
	)
	if err != nil {
		return errEvidenceInvalid
	}
	if provenance.Version != evidenceVersion || provenance.Status != "passed" ||
		!provenance.WorktreeClean ||
		provenance.CommitSHA != config.Provenance.ExpectedCommitSHA ||
		provenance.CommitSHA != result.ProvenanceCommitSHA ||
		provenance.HarnessSHA256 != result.HarnessSHA256 ||
		provenance.HarnessVCSRevision != config.Provenance.ExpectedCommitSHA ||
		provenance.HarnessVCSModified ||
		!canonicalAbsolutePath(provenance.HarnessPath) ||
		provenance.InstalledAgentPath != "/usr/local/bin/tewake-agent" ||
		provenance.InstalledAgentSHA256 !=
			config.Provenance.ExpectedInstalledAgentSHA256 ||
		provenance.InstalledAgentVCSRevision != config.Provenance.ExpectedCommitSHA ||
		provenance.InstalledAgentVCSModified ||
		provenance.AgentUnit.Unit != "tewake-agent.service" ||
		provenance.AgentUnit.FragmentPath !=
			config.Provenance.ExpectedAgentUnitFragmentPath ||
		provenance.AgentUnit.SHA256 != config.Provenance.ExpectedAgentUnitSHA256 ||
		!stringSlicesEqual(
			provenance.AgentUnit.ExecStartArgv,
			expectedServiceArgv("serve", config.RuntimeRoot),
		) ||
		provenance.SupervisorUnit.Unit != "tewake-supervisor.service" ||
		provenance.SupervisorUnit.FragmentPath !=
			config.Provenance.ExpectedSupervisorUnitFragmentPath ||
		provenance.SupervisorUnit.SHA256 !=
			config.Provenance.ExpectedSupervisorUnitSHA256 ||
		!stringSlicesEqual(
			provenance.SupervisorUnit.ExecStartArgv,
			expectedServiceArgv("supervisor", config.RuntimeRoot),
		) ||
		provenance.RunnerPackagePath != expectedRunnerPath ||
		provenance.RunnerPackageSHA256 !=
			config.Provenance.ExpectedRunnerPackageSHA256 ||
		provenance.RunnerPackageSize != expectedRunner.Size {
		return errEvidenceInvalid
	}
	if !canonicalAbsolutePath(provenance.AgentUnit.FragmentPath) ||
		!canonicalAbsolutePath(provenance.SupervisorUnit.FragmentPath) {
		return errEvidenceInvalid
	}
	for _, path := range append(
		append([]string(nil), provenance.AgentUnit.DropInPaths...),
		provenance.SupervisorUnit.DropInPaths...,
	) {
		if !canonicalAbsolutePath(path) {
			return errEvidenceInvalid
		}
	}
	return validateBeforeTime(provenance.GeneratedAt, startedAt, finalEvidenceMaxDelay)
}

func validateProcessAuthority(
	evidence processEvidence,
	authority authorityEvidence,
	executionID string,
	phase string,
) error {
	for _, process := range evidence.Processes {
		if process.BootID != authority.BootID {
			return errEvidenceInvalid
		}
		switch process.Role {
		case "agent":
			if process.UID != authority.Agent.UID ||
				process.Executable != authority.Agent.Executable ||
				process.ExecutableSHA256 != authority.Agent.ExecutableSHA256 {
				return errEvidenceInvalid
			}
			if phase == "before" &&
				(process.PID != authority.Agent.MainPID ||
					process.StartTimeTicks != authority.Agent.ProcessStartTicks ||
					process.ControlGroup != authority.Agent.ControlGroup ||
					process.ExecutableSHA256 != authority.Agent.ExecutableSHA256) {
				return errEvidenceInvalid
			}
		case "supervisor":
			if process.PID != authority.Supervisor.MainPID ||
				process.UID != authority.Supervisor.UID ||
				process.StartTimeTicks != authority.Supervisor.ProcessStartTicks ||
				process.ControlGroup != authority.Supervisor.ControlGroup ||
				process.ExecutableSHA256 != authority.Supervisor.ExecutableSHA256 {
				return errEvidenceInvalid
			}
		case "runner_listener":
			digest := sha256.Sum256([]byte(executionID))
			expected := filepath.Join(
				authority.Supervisor.ControlGroup,
				"tewake",
				"tewake-"+hex.EncodeToString(digest[:]),
			)
			if process.UID != authority.RunnerUID ||
				process.ControlGroup != expected ||
				process.Executable != expectedRunnerListenerPath(
					authority.RuntimeRoot,
					executionID,
				) {
				return errEvidenceInvalid
			}
		}
	}
	return nil
}

func validateProcessManifest(evidence processEvidence, phase string) error {
	if evidence.Version != evidenceVersion ||
		evidence.Phase != phase ||
		evidence.Status != "passed" {
		return errEvidenceInvalid
	}
	var agents, supervisors, listeners int
	seenPIDs := make(map[int]struct{}, len(evidence.Processes))
	for _, process := range evidence.Processes {
		if process.PID <= 0 || process.StartTimeTicks == 0 ||
			process.BootID == "" || !canonicalAbsolutePath(process.Executable) ||
			len(process.ExecutableSHA256) != sha256.Size*2 {
			return errEvidenceInvalid
		}
		if _, found := seenPIDs[process.PID]; found {
			return errEvidenceInvalid
		}
		seenPIDs[process.PID] = struct{}{}
		switch process.Role {
		case "agent":
			agents++
			if process.UID == 0 {
				return errEvidenceInvalid
			}
			if process.SystemdUnit != "tewake-agent.service" {
				return errEvidenceInvalid
			}
		case "supervisor":
			supervisors++
			if process.UID != 0 {
				return errEvidenceInvalid
			}
			if process.SystemdUnit != "tewake-supervisor.service" {
				return errEvidenceInvalid
			}
		case "runner_listener":
			listeners++
			if process.SystemdUnit != "" {
				return errEvidenceInvalid
			}
		default:
			return errEvidenceInvalid
		}
	}
	expectedListeners := 0
	if phase == "running-before-restart" || phase == "running-after-restart" {
		expectedListeners = 1
	}
	if agents != 1 || supervisors != 1 || listeners != expectedListeners {
		return errEvidenceInvalid
	}
	return nil
}

func validateFilesystemManifest(evidence filesystemEvidence) error {
	if evidence.Version != evidenceVersion ||
		evidence.Status != "passed" ||
		!evidence.RuntimeRootPresent ||
		evidence.ExecutionEntries != 0 ||
		evidence.Symlinks != 0 ||
		evidence.WorkDirectories != 0 ||
		evidence.RunnerRegistrations != 0 ||
		evidence.CredentialFiles != 0 ||
		evidence.CredentialRSAParamFiles != 0 ||
		evidence.JITCanaryFiles != 0 {
		return errEvidenceInvalid
	}
	return nil
}

func validateCompletedReplay(
	replay replayEvidence,
	result resultEvidence,
	startedAt time.Time,
	finishedAt time.Time,
) error {
	if validateReplayEvidence(replay) != nil ||
		replay.Phase != "redelivered_same_execution" ||
		replay.TargetID != result.TargetID ||
		replay.NodeID != result.NodeID ||
		replay.ScaleSetID != result.ScaleSetID ||
		replay.RunnerRequestID != result.RunnerRequestID ||
		replay.ExecutionID != result.ExecutionID ||
		replay.CommitControllerEpoch >= result.ControllerEpoch ||
		replay.RedeliveryControllerEpoch != result.ControllerEpoch ||
		replay.AvailableObservedAt != result.AvailableObservedAt {
		return errEvidenceInvalid
	}
	commitAt, _ := time.Parse(time.RFC3339Nano, replay.CommitObservedAt)
	killedAt, _ := time.Parse(time.RFC3339Nano, replay.KilledBeforeAckObservedAt)
	redeliveredAt, _ := time.Parse(time.RFC3339Nano, replay.RedeliveryObservedAt)
	if commitAt.After(killedAt) ||
		killedAt.After(startedAt) ||
		redeliveredAt.Before(startedAt) ||
		redeliveredAt.After(finishedAt) {
		return errEvidenceInvalid
	}
	return nil
}

func validateAgentRestartEvidence(
	config liveConfig,
	result resultEvidence,
	startedAt time.Time,
	finishedAt time.Time,
	authority authorityEvidence,
) error {
	marker, err := loadEvidenceFile[restartStartedEvidence](
		config.EvidenceDirectory,
		restartStartedName,
	)
	if err != nil || marker.Version != evidenceVersion ||
		marker.ScaleSetID != result.ScaleSetID ||
		marker.RunnerRequestID != result.RunnerRequestID ||
		marker.ExecutionID != result.ExecutionID ||
		marker.ObservedAt != result.JobStartedObservedAt {
		return errEvidenceInvalid
	}
	jobStartedAt, _ := time.Parse(time.RFC3339Nano, result.JobStartedObservedAt)
	jobCompletedAt, _ := time.Parse(time.RFC3339Nano, result.JobCompletedObservedAt)
	if jobStartedAt.Before(startedAt) ||
		jobCompletedAt.After(finishedAt) ||
		jobCompletedAt.Sub(jobStartedAt) <
			time.Duration(config.AgentRestartMinimumRunningSeconds)*time.Second {
		return errEvidenceInvalid
	}
	before, err := loadEvidenceFile[processEvidence](
		config.EvidenceDirectory,
		processRunningBeforeRestartName,
	)
	if err != nil || validateProcessManifest(before, "running-before-restart") != nil {
		return errEvidenceInvalid
	}
	after, err := loadEvidenceFile[processEvidence](
		config.EvidenceDirectory,
		processRunningAfterRestartName,
	)
	if err != nil || validateProcessManifest(after, "running-after-restart") != nil {
		return errEvidenceInvalid
	}
	if validateProcessAuthority(before, authority, result.ExecutionID, "running-before-restart") != nil ||
		validateProcessAuthority(after, authority, result.ExecutionID, "running-after-restart") != nil {
		return errEvidenceInvalid
	}
	beforeAt, err := time.Parse(time.RFC3339Nano, before.GeneratedAt)
	if err != nil || beforeAt.Before(jobStartedAt) || beforeAt.After(jobCompletedAt) {
		return errEvidenceInvalid
	}
	afterAt, err := time.Parse(time.RFC3339Nano, after.GeneratedAt)
	if err != nil || afterAt.Before(beforeAt) || afterAt.After(jobCompletedAt) {
		return errEvidenceInvalid
	}
	beforeAgent, beforeSupervisor, beforeListener, ok := processRolePIDs(before)
	if !ok {
		return errEvidenceInvalid
	}
	afterAgent, afterSupervisor, afterListener, ok := processRolePIDs(after)
	if !ok ||
		beforeAgent == afterAgent ||
		beforeSupervisor != afterSupervisor ||
		beforeListener != afterListener {
		return errEvidenceInvalid
	}
	beforeListenerIdentity, _ := processByRole(before, "runner_listener")
	afterListenerIdentity, _ := processByRole(after, "runner_listener")
	beforeSupervisorIdentity, _ := processByRole(before, "supervisor")
	afterSupervisorIdentity, _ := processByRole(after, "supervisor")
	if beforeListenerIdentity.StartTimeTicks != afterListenerIdentity.StartTimeTicks ||
		beforeListenerIdentity.BootID != afterListenerIdentity.BootID ||
		beforeListenerIdentity.ControlGroup != afterListenerIdentity.ControlGroup ||
		beforeListenerIdentity.ExecutableSHA256 != afterListenerIdentity.ExecutableSHA256 ||
		beforeSupervisorIdentity.StartTimeTicks != afterSupervisorIdentity.StartTimeTicks ||
		beforeSupervisorIdentity.BootID != afterSupervisorIdentity.BootID {
		return errEvidenceInvalid
	}
	return nil
}

func processByRole(evidence processEvidence, role string) (observedProcess, bool) {
	for _, process := range evidence.Processes {
		if process.Role == role {
			return process, true
		}
	}
	return observedProcess{}, false
}

func processRolePIDs(evidence processEvidence) (agent, supervisor, listener int, ok bool) {
	for _, process := range evidence.Processes {
		switch process.Role {
		case "agent":
			agent = process.PID
		case "supervisor":
			supervisor = process.PID
		case "runner_listener":
			listener = process.PID
		}
	}
	return agent, supervisor, listener, agent > 0 && supervisor > 0 && listener > 0
}

func validateBeforeTime(raw string, startedAt time.Time, maxAge time.Duration) error {
	generatedAt, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil ||
		generatedAt.After(startedAt) ||
		maxAge <= 0 ||
		startedAt.Sub(generatedAt) > maxAge {
		return errEvidenceInvalid
	}
	return nil
}

func validateAfterTime(raw string, finishedAt time.Time, now time.Time) error {
	generatedAt, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil ||
		generatedAt.Before(finishedAt) ||
		generatedAt.After(now.Add(time.Minute)) ||
		now.Sub(generatedAt) > finalEvidenceMaxDelay {
		return errEvidenceInvalid
	}
	return nil
}

func loadEvidenceFile[T any](directory, name string) (T, error) {
	var value T
	switch name {
	case resultFileName,
		replayFileName,
		processBeforeName,
		processAfterName,
		processRunningBeforeRestartName,
		processRunningAfterRestartName,
		filesystemName,
		restartStartedName:
	case authorityFileName, provenanceFileName, injectorFileName:
	default:
		return value, errEvidenceInvalid
	}
	path := filepath.Join(directory, name)
	if err := decodeStrictRegularJSONFile(path, maxLiveConfigBytes, &value, true); err != nil {
		return value, errEvidenceInvalid
	}
	return value, nil
}

func requireEvidenceAbsent(directory, name string) error {
	switch name {
	case replayFileName,
		processAfterName,
		processRunningBeforeRestartName,
		processRunningAfterRestartName,
		filesystemName,
		restartStartedName,
		injectorFileName:
	default:
		return errEvidenceInvalid
	}
	_, err := os.Lstat(filepath.Join(directory, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return errEvidenceInvalid
}
