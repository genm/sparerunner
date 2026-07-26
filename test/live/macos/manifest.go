package main

import (
	"fmt"
	"slices"
	"time"

	"github.com/genm/tewake/internal/runner"
)

func validateScenario(
	config macOSLiveConfig,
	scenario acceptanceScenario,
	now time.Time,
	store *evidenceStore,
) error {
	if err := config.validate(); err != nil || store == nil {
		return errMacOSEvidenceInvalid
	}
	var phases []capturePhase
	switch scenario {
	case scenarioNormal:
		phases = []capturePhase{phaseBefore, phaseRunning, phaseAfter}
	case scenarioSleep:
		phases = []capturePhase{
			phaseRunningBeforeSleep,
			phaseRunningAfterWake,
			phaseAfter,
		}
	case scenarioReboot:
		phases = []capturePhase{phasePreReboot, phasePostReboot}
	default:
		return errMacOSEvidenceInvalid
	}
	evidence := make([]nodeEvidence, 0, len(phases))
	for _, phase := range phases {
		item, err := store.loadNode(phase)
		if err != nil || validateCaptureAgainstConfig(config, item) != nil {
			return errMacOSEvidenceInvalid
		}
		evidence = append(evidence, item)
	}
	if err := validateCaptureTimes(config, now, evidence); err != nil {
		return err
	}
	switch scenario {
	case scenarioNormal:
		if validateIdleBefore(evidence[0]) != nil ||
			validateRunning(evidence[1]) != nil ||
			validateReleased(evidence[2]) != nil ||
			!sameBoot(evidence...) {
			return errMacOSEvidenceInvalid
		}
	case scenarioSleep:
		before, afterWake, released := evidence[0], evidence[1], evidence[2]
		if validateRunning(before) != nil || validateRunning(afterWake) != nil ||
			validateReleased(released) != nil || !sameBoot(evidence...) ||
			before.Agent.PID != afterWake.Agent.PID ||
			before.Execution.PID != afterWake.Execution.PID ||
			before.Execution.Revision > afterWake.Execution.Revision ||
			!slices.Equal(processIDs(before.RunnerProcesses), processIDs(afterWake.RunnerProcesses)) {
			return errMacOSEvidenceInvalid
		}
	case scenarioReboot:
		before, after := evidence[0], evidence[1]
		if validateRunning(before) != nil || validateReleased(after) != nil ||
			before.BootEpoch == after.BootEpoch ||
			after.ControllerEpoch < before.ControllerEpoch {
			return errMacOSEvidenceInvalid
		}
	}
	return nil
}

func validateCaptureAgainstConfig(config macOSLiveConfig, evidence nodeEvidence) error {
	if validateNodeEvidenceShape(evidence) != nil ||
		evidence.Architecture != config.ExpectedArchitecture ||
		evidence.LaunchDaemonState != "running" ||
		evidence.AgentInstances != 1 ||
		evidence.ControllerEpoch == 0 ||
		!evidence.PrivateMaterial.ServiceCanLoad ||
		!evidence.PrivateMaterial.RunnerAccountDenied ||
		evidence.PrivateMaterial.LocatorContainsSecret ||
		evidence.Provenance != (provenanceEvidence{
			CommitSHA:           config.ExpectedCommitSHA,
			HarnessSHA256:       config.ExpectedHarnessSHA256,
			AgentSHA256:         config.ExpectedInstalledAgentSHA256,
			LaunchDaemonSHA256:  config.ExpectedLaunchDaemonSHA256,
			RunnerPackageSHA256: config.ExpectedRunnerPackageSHA256,
		}) {
		return errMacOSEvidenceInvalid
	}
	return nil
}

func validateCaptureTimes(
	config macOSLiveConfig,
	now time.Time,
	evidence []nodeEvidence,
) error {
	if now.Location() != time.UTC || len(evidence) == 0 {
		return errMacOSEvidenceInvalid
	}
	var previous time.Time
	for _, item := range evidence {
		captured, err := time.Parse(time.RFC3339Nano, item.CapturedAt)
		if err != nil || captured.After(now.Add(time.Minute)) ||
			now.Sub(captured) > time.Duration(config.MaximumRunSeconds)*time.Second ||
			(!previous.IsZero() && !captured.After(previous)) {
			return errMacOSEvidenceInvalid
		}
		previous = captured
	}
	return nil
}

func validateIdleBefore(evidence nodeEvidence) error {
	if evidence.Execution.Found || len(evidence.RunnerProcesses) != 0 ||
		evidence.ExecutionDirectories != 0 || evidence.FenceDirectories != 0 {
		return errMacOSEvidenceInvalid
	}
	return nil
}

func validateRunning(evidence nodeEvidence) error {
	if !evidence.Execution.Found ||
		evidence.Execution.State != runner.StateRunning ||
		evidence.Execution.PID <= 1 ||
		evidence.Execution.HostEpoch != evidence.BootEpoch ||
		evidence.Execution.HasTombstone ||
		len(evidence.RunnerProcesses) == 0 ||
		evidence.ExecutionDirectories != 1 ||
		evidence.FenceDirectories != 1 {
		return errMacOSEvidenceInvalid
	}
	for _, process := range evidence.RunnerProcesses {
		if process.PID == evidence.Execution.PID &&
			process.PGID == evidence.Execution.PID {
			return nil
		}
	}
	return fmt.Errorf("%w: journal PID is not the process-group leader", errMacOSEvidenceInvalid)
}

func validateReleased(evidence nodeEvidence) error {
	if !evidence.Execution.Found ||
		evidence.Execution.State != runner.StateReleased ||
		evidence.Execution.PID != 0 ||
		evidence.Execution.HasTombstone ||
		len(evidence.RunnerProcesses) != 0 ||
		evidence.ExecutionDirectories != 0 ||
		evidence.FenceDirectories != 0 {
		return errMacOSEvidenceInvalid
	}
	return nil
}

func sameBoot(evidence ...nodeEvidence) bool {
	if len(evidence) == 0 {
		return false
	}
	boot := evidence[0].BootEpoch
	for _, item := range evidence[1:] {
		if item.BootEpoch != boot {
			return false
		}
	}
	return true
}

func processIDs(processes []processEvidence) []int {
	result := make([]int, 0, len(processes))
	for _, process := range processes {
		result = append(result, process.PID)
	}
	return result
}
