//go:build darwin

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/genm/tewake/internal/runner"
)

func TestMacOSLiveManifestValidatesNormalSleepAndReboot(t *testing.T) {
	now := time.Date(2026, 7, 27, 3, 0, 0, 0, time.UTC)
	for _, scenario := range []acceptanceScenario{
		scenarioNormal,
		scenarioSleep,
		scenarioReboot,
	} {
		t.Run(string(scenario), func(t *testing.T) {
			config := validMacOSConfig(t, "arm64")
			if err := os.Mkdir(config.EvidenceDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			store, err := openEvidenceStore(config.EvidenceDirectory)
			if err != nil {
				t.Fatal(err)
			}
			for index, evidence := range scenarioEvidence(config, scenario, now) {
				evidence.CapturedAt = now.Add(
					time.Duration(index-len(scenarioEvidence(config, scenario, now))) *
						time.Minute,
				).Format(time.RFC3339Nano)
				if err := store.writeNode(evidence); err != nil {
					t.Fatal(err)
				}
			}
			if err := validateScenario(config, scenario, now, store); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMacOSLiveManifestRejectsDuplicateAgentAndFalseCleanup(t *testing.T) {
	now := time.Date(2026, 7, 27, 3, 0, 0, 0, time.UTC)
	for _, mutate := range []func(*nodeEvidence){
		func(evidence *nodeEvidence) { evidence.AgentInstances = 2 },
		func(evidence *nodeEvidence) { evidence.ExecutionDirectories = 1 },
		func(evidence *nodeEvidence) { evidence.PrivateMaterial.RunnerAccountDenied = false },
	} {
		config := validMacOSConfig(t, "arm64")
		if err := os.Mkdir(config.EvidenceDirectory, 0o700); err != nil {
			t.Fatal(err)
		}
		store, err := openEvidenceStore(config.EvidenceDirectory)
		if err != nil {
			t.Fatal(err)
		}
		items := scenarioEvidence(config, scenarioNormal, now)
		mutate(&items[len(items)-1])
		for index, evidence := range items {
			evidence.CapturedAt = now.Add(
				time.Duration(index-len(items)) * time.Minute,
			).Format(time.RFC3339Nano)
			if err := store.writeNode(evidence); err != nil {
				t.Fatal(err)
			}
		}
		if err := validateScenario(config, scenarioNormal, now, store); err == nil {
			t.Fatal("invalid evidence was accepted")
		}
	}
}

func TestMacOSEvidenceStoreRejectsOverwriteAndSecretFields(t *testing.T) {
	store, err := openEvidenceStore(filepath.Join(t.TempDir(), "evidence"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.writeJSON("safe.json", map[string]bool{"passed": true}); err != nil {
		t.Fatal(err)
	}
	if err := store.writeJSON("safe.json", map[string]bool{"passed": true}); err == nil {
		t.Fatal("evidence overwrite was accepted")
	}
	if err := store.writeJSON("unsafe.json", map[string]string{
		"jitConfig": "canary",
	}); err == nil {
		t.Fatal("secret-shaped evidence field was accepted")
	}
}

func scenarioEvidence(
	config macOSLiveConfig,
	scenario acceptanceScenario,
	now time.Time,
) []nodeEvidence {
	before := validNodeEvidence(config, phaseBefore, now, false, runner.State(""))
	runningBefore := validNodeEvidence(
		config,
		phaseRunningBeforeSleep,
		now,
		true,
		runner.StateRunning,
	)
	runningAfter := validNodeEvidence(
		config,
		phaseRunningAfterWake,
		now,
		true,
		runner.StateRunning,
	)
	released := validNodeEvidence(config, phaseAfter, now, true, runner.StateReleased)
	switch scenario {
	case scenarioNormal:
		runningAfter.Phase = phaseRunning
		return []nodeEvidence{before, runningAfter, released}
	case scenarioSleep:
		return []nodeEvidence{runningBefore, runningAfter, released}
	case scenarioReboot:
		runningBefore.Phase = phasePreReboot
		released.Phase = phasePostReboot
		released.BootEpoch = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		return []nodeEvidence{runningBefore, released}
	default:
		return nil
	}
}

func validNodeEvidence(
	config macOSLiveConfig,
	phase capturePhase,
	now time.Time,
	found bool,
	state runner.State,
) nodeEvidence {
	boot := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	evidence := nodeEvidence{
		Version:           macOSEvidenceVersion,
		Phase:             phase,
		CapturedAt:        now.Format(time.RFC3339Nano),
		BootEpoch:         boot,
		Architecture:      config.ExpectedArchitecture,
		LaunchDaemonState: "running",
		Agent:             processEvidence{PID: 100, PGID: 100, UID: 0},
		AgentInstances:    1,
		RunnerUID:         701,
		RunnerGID:         701,
		ControllerEpoch:   2,
		PrivateMaterial: privateMaterialEvidence{
			ServiceCanLoad: true, RunnerAccountDenied: true,
		},
		Provenance: provenanceEvidence{
			CommitSHA:           config.ExpectedCommitSHA,
			HarnessSHA256:       config.ExpectedHarnessSHA256,
			AgentSHA256:         config.ExpectedInstalledAgentSHA256,
			LaunchDaemonSHA256:  config.ExpectedLaunchDaemonSHA256,
			RunnerPackageSHA256: config.ExpectedRunnerPackageSHA256,
		},
	}
	if !found {
		return evidence
	}
	evidence.Execution = executionEvidence{
		Found: true, State: state, HostEpoch: boot, Revision: 4,
	}
	if state == runner.StateRunning {
		evidence.Execution.PID = 200
		evidence.ExecutionDirectories = 1
		evidence.FenceDirectories = 1
		evidence.RunnerProcesses = []processEvidence{
			{PID: 200, PGID: 200, UID: 701},
			{PID: 201, PGID: 200, UID: 701},
		}
	}
	return evidence
}
