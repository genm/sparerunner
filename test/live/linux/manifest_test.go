package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/github"
	"github.com/genm/tewake/internal/runner"
)

func TestValidateFinalEvidenceAcceptsScenarioSpecificManifest(t *testing.T) {
	now := time.Now().UTC()
	for _, mode := range []acceptanceMode{
		modeNormal,
		modeCommitBeforeAck,
		modeCleanupFailure,
		modeAgentRestart,
	} {
		t.Run(string(mode), func(t *testing.T) {
			config := writeValidFinalManifest(t, mode, now)
			if mode == modeCleanupFailure {
				injector, err := loadEvidenceFile[injectorEvidence](
					config.EvidenceDirectory,
					injectorFileName,
				)
				result, resultErr := loadEvidenceFile[resultEvidence](
					config.EvidenceDirectory,
					resultFileName,
				)
				startedAt, _ := time.Parse(time.RFC3339Nano, result.StartedAt)
				finishedAt, _ := time.Parse(time.RFC3339Nano, result.FinishedAt)
				preparedErr := validatePreparedInjector(injector)
				manifestErr := validateInjectorManifest(injector, startedAt, finishedAt)
				if err != nil || resultErr != nil || manifestErr != nil {
					t.Fatalf(
						"cleanup injector fixture invalid: prepared=%v manifest=%v started=%v finished=%v evidence=%#v",
						preparedErr,
						manifestErr,
						startedAt,
						finishedAt,
						injector,
					)
				}
			}
			if err := validateFinalEvidence(config, mode, now); err != nil {
				t.Fatalf("validateFinalEvidence() error = %v", err)
			}
		})
	}
}

func TestValidateFinalEvidenceFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	testCases := []struct {
		name   string
		mode   acceptanceMode
		mutate func(*testing.T, liveConfig)
	}{
		{
			name: "missing before evidence",
			mode: modeNormal,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				if err := os.Remove(filepath.Join(config.EvidenceDirectory, processBeforeName)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "stale before evidence",
			mode: modeNormal,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				value, err := loadEvidenceFile[processEvidence](
					config.EvidenceDirectory,
					processBeforeName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.GeneratedAt = now.Add(-20 * time.Minute).Format(time.RFC3339Nano)
				if err := evidence.writeJSON(processBeforeName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "scenario mismatch",
			mode: modeNormal,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				value, err := loadEvidenceFile[resultEvidence](
					config.EvidenceDirectory,
					resultFileName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.Mode = string(modeCleanupFailure)
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(resultFileName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "dirty provenance",
			mode: modeNormal,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				value, err := loadEvidenceFile[provenanceEvidence](
					config.EvidenceDirectory,
					provenanceFileName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.WorktreeClean = false
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(provenanceFileName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "altered provenance unit",
			mode: modeNormal,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				value, err := loadEvidenceFile[provenanceEvidence](
					config.EvidenceDirectory,
					provenanceFileName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.AgentUnit.SHA256 = strings.Repeat("9", 64)
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(provenanceFileName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "altered provenance package",
			mode: modeNormal,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				value, err := loadEvidenceFile[provenanceEvidence](
					config.EvidenceDirectory,
					provenanceFileName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.RunnerPackageSHA256 = strings.Repeat("9", 64)
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(provenanceFileName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "normal missing filesystem evidence",
			mode: modeNormal,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				if err := os.Remove(filepath.Join(config.EvidenceDirectory, filesystemName)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "commit missing replay evidence",
			mode: modeCommitBeforeAck,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				if err := os.Remove(filepath.Join(config.EvidenceDirectory, replayFileName)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "commit missing kill boundary",
			mode: modeCommitBeforeAck,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				value, err := loadEvidenceFile[replayEvidence](
					config.EvidenceDirectory,
					replayFileName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.KillExitStatus = 0
				value.KilledBeforeAckObservedAt = ""
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(replayFileName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "commit did not advance controller epoch",
			mode: modeCommitBeforeAck,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				value, err := loadEvidenceFile[replayEvidence](
					config.EvidenceDirectory,
					replayFileName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.CommitControllerEpoch = value.RedeliveryControllerEpoch
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(replayFileName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "cleanup contains cleanup success evidence",
			mode: modeCleanupFailure,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(processAfterName, validProcessEvidence(
					"after",
					now.Add(-30*time.Second),
				)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "cleanup result not quarantined",
			mode: modeCleanupFailure,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				value, err := loadEvidenceFile[resultEvidence](
					config.EvidenceDirectory,
					resultFileName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.NodeState = string(domain.NodeActive)
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(resultFileName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "agent restart missing running evidence",
			mode: modeAgentRestart,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				if err := os.Remove(filepath.Join(
					config.EvidenceDirectory,
					processRunningAfterRestartName,
				)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "agent restart did not preserve listener",
			mode: modeAgentRestart,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				value, err := loadEvidenceFile[processEvidence](
					config.EvidenceDirectory,
					processRunningAfterRestartName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.Processes[2].PID++
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(processRunningAfterRestartName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "agent restart changed installed agent binary",
			mode: modeAgentRestart,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				value, err := loadEvidenceFile[processEvidence](
					config.EvidenceDirectory,
					processRunningAfterRestartName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.Processes[0].ExecutableSHA256 = strings.Repeat("e", 64)
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(processRunningAfterRestartName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "agent restart listener pid was reused",
			mode: modeAgentRestart,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				value, err := loadEvidenceFile[processEvidence](
					config.EvidenceDirectory,
					processRunningAfterRestartName,
				)
				if err != nil {
					t.Fatal(err)
				}
				value.Processes[2].StartTimeTicks++
				evidence, err := openEvidenceStore(config.EvidenceDirectory)
				if err != nil {
					t.Fatal(err)
				}
				if err := evidence.writeJSON(processRunningAfterRestartName, value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "private repository proof missing",
			mode: modeNormal,
			mutate: func(t *testing.T, config liveConfig) {
				t.Helper()
				if err := os.Remove(config.GitHub.PrivateRepositoryProofFile); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			config := writeValidFinalManifest(t, testCase.mode, now)
			testCase.mutate(t, config)
			if err := validateFinalEvidence(config, testCase.mode, now); !errors.Is(err, errEvidenceInvalid) {
				t.Fatalf("validateFinalEvidence() error = %v, want errEvidenceInvalid", err)
			}
		})
	}
}

func writeValidFinalManifest(
	t *testing.T,
	mode acceptanceMode,
	now time.Time,
) liveConfig {
	t.Helper()
	root := t.TempDir()
	evidenceDirectory := filepath.Join(root, "evidence")
	evidence, err := openEvidenceStore(evidenceDirectory)
	if err != nil {
		t.Fatal(err)
	}
	config := liveConfig{
		Version:                           liveConfigVersion,
		ControllerStateDirectory:          filepath.Join(root, "controller"),
		AgentListenAddress:                "127.0.0.1:7443",
		EvidenceDirectory:                 evidenceDirectory,
		RuntimeRoot:                       filepath.Join(root, "runtime"),
		NodeReadyTimeoutSeconds:           60,
		RunTimeoutSeconds:                 600,
		AgentRestartMinimumRunningSeconds: 30,
		Provenance: provenanceConfig{
			ExpectedCommitSHA:                  strings.Repeat("1", 40),
			ExpectedInstalledAgentSHA256:       strings.Repeat("a", 64),
			ExpectedAgentUnitFragmentPath:      "/etc/systemd/system/tewake-agent.service",
			ExpectedAgentUnitSHA256:            strings.Repeat("c", 64),
			ExpectedSupervisorUnitFragmentPath: "/etc/systemd/system/tewake-supervisor.service",
			ExpectedSupervisorUnitSHA256:       strings.Repeat("d", 64),
		},
		GitHub: githubConfig{
			ConfigURL:                  "https://github.com/genm/tewake-private",
			ClientID:                   "client-id",
			InstallationID:             7,
			PrivateKeyFile:             filepath.Join(root, "credentials", "app-key.pem"),
			PrivateRepositoryProofFile: filepath.Join(evidenceDirectory, privateProofFileName),
			RunnerGroupID:              11,
			ScaleSetName:               liveScaleSetName,
			DisableUpdate:              true,
		},
		NodeID: "0123456789abcdef0123456789abcdef",
	}
	officialPackage, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	config.Provenance.ExpectedRunnerPackageSHA256 = officialPackage.Checksum
	if err := config.validate(); err != nil {
		t.Fatalf("test config invalid: %v", err)
	}
	startedAt := now.Add(-2 * time.Minute)
	availableAt := startedAt.Add(20 * time.Second)
	jobStartedAt := availableAt.Add(10 * time.Second)
	if mode == modeCommitBeforeAck {
		availableAt = startedAt.Add(-30 * time.Second)
		jobStartedAt = startedAt.Add(10 * time.Second)
	}
	finishedAt := now.Add(-time.Minute)
	writeJSONTestFile(t, config.GitHub.PrivateRepositoryProofFile, privateRepositoryProof{
		Version:    privateProofVersion,
		Repository: "genm/tewake-private",
		Visibility: "PRIVATE",
	}, 0o600)
	proofTime := startedAt.Add(-time.Minute)
	if err := os.Chtimes(
		config.GitHub.PrivateRepositoryProofFile,
		proofTime,
		proofTime,
	); err != nil {
		t.Fatal(err)
	}
	authority := validAuthorityEvidence(config, startedAt.Add(-time.Minute))
	if err := evidence.writeJSON(authorityFileName, authority); err != nil {
		t.Fatal(err)
	}
	harnessDigest := strings.Repeat("f", 64)
	runnerPath, runnerPackage, err := expectedOfficialRunnerAuthority(config.RuntimeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := evidence.writeJSON(provenanceFileName, provenanceEvidence{
		Version:                   evidenceVersion,
		Status:                    "passed",
		CommitSHA:                 config.Provenance.ExpectedCommitSHA,
		WorktreeClean:             true,
		HarnessPath:               "/run/tewake-live-linux.test/harness",
		HarnessSHA256:             harnessDigest,
		HarnessVCSRevision:        config.Provenance.ExpectedCommitSHA,
		InstalledAgentPath:        "/usr/local/bin/tewake-agent",
		InstalledAgentSHA256:      config.Provenance.ExpectedInstalledAgentSHA256,
		InstalledAgentVCSRevision: config.Provenance.ExpectedCommitSHA,
		AgentUnit: unitProvenance{
			Unit: "tewake-agent.service", FragmentPath: "/etc/systemd/system/tewake-agent.service",
			ExecStartArgv: authority.Agent.ExecStartArgv,
			SHA256:        config.Provenance.ExpectedAgentUnitSHA256,
		},
		SupervisorUnit: unitProvenance{
			Unit: "tewake-supervisor.service", FragmentPath: "/etc/systemd/system/tewake-supervisor.service",
			ExecStartArgv: authority.Supervisor.ExecStartArgv,
			SHA256:        config.Provenance.ExpectedSupervisorUnitSHA256,
		},
		RunnerPackagePath:   runnerPath,
		RunnerPackageSHA256: config.Provenance.ExpectedRunnerPackageSHA256,
		RunnerPackageSize:   runnerPackage.Size,
		GeneratedAt:         startedAt.Add(-time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	latencyMillis := jobStartedAt.Sub(availableAt).Milliseconds()
	result := resultEvidence{
		Version:                  evidenceVersion,
		Mode:                     string(mode),
		Status:                   "passed",
		TargetID:                 "target",
		PrivateRepository:        "genm/tewake-private",
		NodeID:                   config.NodeID,
		ScaleSetID:               41,
		ControllerEpoch:          3,
		ExecutionID:              "execution",
		ExecutionState:           string(domain.ExecutionReleased),
		NodeState:                string(domain.NodeActive),
		ReservationCount:         0,
		RunnerRequestID:          91,
		ObservedEvents:           []string{string(github.MessageTypeJobAvailable), string(github.MessageTypeJobCompleted), string(github.MessageTypeJobStarted)},
		AvailableObservedAt:      availableAt.Format(time.RFC3339Nano),
		JobStartedObservedAt:     jobStartedAt.Format(time.RFC3339Nano),
		JobCompletedObservedAt:   finishedAt.Format(time.RFC3339Nano),
		AvailableToStartedMillis: &latencyMillis,
		ProvenanceCommitSHA:      config.Provenance.ExpectedCommitSHA,
		HarnessSHA256:            harnessDigest,
		StartedAt:                startedAt.Format(time.RFC3339Nano),
		FinishedAt:               finishedAt.Format(time.RFC3339Nano),
	}
	if mode == modeCleanupFailure {
		result.ExecutionState = string(domain.ExecutionCleanupFailed)
		result.NodeState = string(domain.NodeQuarantined)
		result.ReservationCount = 1
		if err := evidence.writeJSON(injectorFileName, injectorEvidence{
			Version: evidenceVersion,
			Status:  "disarmed",
			Source: injectorFileEvidence{
				Path: "/opt/tewake-test/injector", SHA256: strings.Repeat("9", 64),
				Device: 1, Inode: 2, UID: 0, Mode: 0o500, Size: 100,
			},
			Copy: injectorFileEvidence{
				Path:   "/run/tewake-live-injectors/run-test/injector",
				SHA256: strings.Repeat("9", 64),
				Device: 1, Inode: 3, UID: 0, Mode: 0o500, Size: 100,
			},
			PreparedObservedAt: startedAt.Add(-time.Minute).Format(time.RFC3339Nano),
			ArmedObservedAt:    startedAt.Add(time.Second).Format(time.RFC3339Nano),
			DisarmedObservedAt: finishedAt.Add(time.Second).Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := evidence.writeResult(result); err != nil {
		t.Fatal(err)
	}
	if err := evidence.writeJSON(
		processBeforeName,
		validProcessEvidence("before", startedAt.Add(-time.Minute)),
	); err != nil {
		t.Fatal(err)
	}
	if mode == modeCleanupFailure {
		return config
	}
	if mode == modeAgentRestart {
		if err := evidence.writeJSON(restartStartedName, restartStartedEvidence{
			Version:         evidenceVersion,
			ScaleSetID:      result.ScaleSetID,
			RunnerRequestID: result.RunnerRequestID,
			ExecutionID:     result.ExecutionID,
			ObservedAt:      result.JobStartedObservedAt,
		}); err != nil {
			t.Fatal(err)
		}
		if err := evidence.writeJSON(
			processRunningBeforeRestartName,
			validRunningProcessEvidence(
				"running-before-restart",
				jobStartedAt.Add(5*time.Second),
				101,
				result.ExecutionID,
				authority,
			),
		); err != nil {
			t.Fatal(err)
		}
		if err := evidence.writeJSON(
			processRunningAfterRestartName,
			validRunningProcessEvidence(
				"running-after-restart",
				jobStartedAt.Add(15*time.Second),
				102,
				result.ExecutionID,
				authority,
			),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := evidence.writeJSON(
		processAfterName,
		validProcessEvidence("after", finishedAt.Add(10*time.Second)),
	); err != nil {
		t.Fatal(err)
	}
	if err := evidence.writeJSON(filesystemName, filesystemEvidence{
		Version:            evidenceVersion,
		Status:             "passed",
		RuntimeRootPresent: true,
		GeneratedAt:        finishedAt.Add(20 * time.Second).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if mode == modeCommitBeforeAck {
		if err := evidence.writeReplay(replayEvidence{
			Version:                   evidenceVersion,
			Phase:                     "redelivered_same_execution",
			TargetID:                  result.TargetID,
			NodeID:                    result.NodeID,
			ScaleSetID:                result.ScaleSetID,
			MessageID:                 71,
			RunnerRequestID:           result.RunnerRequestID,
			ExecutionID:               result.ExecutionID,
			AvailableObservedAt:       result.AvailableObservedAt,
			CommitControllerEpoch:     result.ControllerEpoch - 1,
			CommitObservedAt:          availableAt.Format(time.RFC3339Nano),
			KillExitStatus:            137,
			KilledBeforeAckObservedAt: availableAt.Add(time.Second).Format(time.RFC3339Nano),
			RedeliveryControllerEpoch: result.ControllerEpoch,
			RedeliveryObservedAt:      startedAt.Add(5 * time.Second).Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return config
}

func validProcessEvidence(phase string, generatedAt time.Time) processEvidence {
	return processEvidence{
		Version: evidenceVersion,
		Phase:   phase,
		Status:  "passed",
		Processes: []observedProcess{
			{
				PID: 101, UID: 1001, Role: "agent",
				SystemdUnit:    "tewake-agent.service",
				ControlGroup:   "/system.slice/tewake-agent.service",
				StartTimeTicks: 111, BootID: "01234567-89ab-cdef-0123-456789abcdef",
				Executable:       "/usr/local/bin/tewake-agent",
				ExecutableSHA256: strings.Repeat("a", 64),
			},
			{
				PID: 202, UID: 0, Role: "supervisor",
				SystemdUnit:    "tewake-supervisor.service",
				ControlGroup:   "/system.slice/tewake-supervisor.service",
				StartTimeTicks: 222, BootID: "01234567-89ab-cdef-0123-456789abcdef",
				Executable:       "/usr/local/bin/tewake-agent",
				ExecutableSHA256: strings.Repeat("a", 64),
			},
		},
		GeneratedAt: generatedAt.Format(time.RFC3339Nano),
	}
}

func validRunningProcessEvidence(
	phase string,
	generatedAt time.Time,
	agentPID int,
	executionID string,
	authority authorityEvidence,
) processEvidence {
	value := validProcessEvidence(phase, generatedAt)
	value.Processes = append(value.Processes, observedProcess{
		PID:  303,
		UID:  authority.RunnerUID,
		Role: "runner_listener",
		ControlGroup: func() string {
			digest := sha256.Sum256([]byte(executionID))
			return filepath.Join(
				authority.Supervisor.ControlGroup,
				"tewake",
				"tewake-"+hex.EncodeToString(digest[:]),
			)
		}(),
		StartTimeTicks:   333,
		BootID:           authority.BootID,
		Executable:       expectedRunnerListenerPath(authority.RuntimeRoot, executionID),
		ExecutableSHA256: strings.Repeat("b", 64),
	})
	value.Processes[0].PID = agentPID
	value.Processes[0].StartTimeTicks = uint64(agentPID + 10)
	return value
}

func validAuthorityEvidence(config liveConfig, generatedAt time.Time) authorityEvidence {
	return authorityEvidence{
		Version: evidenceVersion, Status: "passed",
		BootID:      "01234567-89ab-cdef-0123-456789abcdef",
		RuntimeRoot: config.RuntimeRoot, RunnerUID: 1002,
		Agent: serviceAuthority{
			Unit: "tewake-agent.service", Executable: "/usr/local/bin/tewake-agent",
			FragmentPath:        "/etc/systemd/system/tewake-agent.service",
			EffectiveUnitSHA256: config.Provenance.ExpectedAgentUnitSHA256,
			ExecStartArgv:       expectedServiceArgv("serve", config.RuntimeRoot),
			Subcommand:          "serve", RuntimeRoot: config.RuntimeRoot,
			MainPID: 101, UID: 1001, ControlGroup: "/system.slice/tewake-agent.service",
			ProcessStartTicks: 111, ExecutableSHA256: strings.Repeat("a", 64),
		},
		Supervisor: serviceAuthority{
			Unit: "tewake-supervisor.service", Executable: "/usr/local/bin/tewake-agent",
			FragmentPath:        "/etc/systemd/system/tewake-supervisor.service",
			EffectiveUnitSHA256: config.Provenance.ExpectedSupervisorUnitSHA256,
			ExecStartArgv:       expectedServiceArgv("supervisor", config.RuntimeRoot),
			Subcommand:          "supervisor", RuntimeRoot: config.RuntimeRoot,
			MainPID: 202, UID: 0, ControlGroup: "/system.slice/tewake-supervisor.service",
			ProcessStartTicks: 222, ExecutableSHA256: strings.Repeat("a", 64),
		},
		GeneratedAt: generatedAt.Format(time.RFC3339Nano),
	}
}
