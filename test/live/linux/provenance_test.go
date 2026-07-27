//go:build !windows

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCaptureProvenanceFailsClosedOnDirtyOrAlteredInputs(t *testing.T) {
	testCases := []struct {
		name   string
		mutate func(*liveConfig, *authorityEvidence, *fakeAuthorityProbe)
	}{
		{name: "dirty checkout", mutate: func(_ *liveConfig, _ *authorityEvidence, probe *fakeAuthorityProbe) {
			probe.clean = false
		}},
		{name: "wrong commit", mutate: func(_ *liveConfig, _ *authorityEvidence, probe *fakeAuthorityProbe) {
			probe.commit = strings.Repeat("9", 40)
		}},
		{name: "altered installed binary", mutate: func(_ *liveConfig, authority *authorityEvidence, _ *fakeAuthorityProbe) {
			authority.Agent.ExecutableSHA256 = strings.Repeat("9", 64)
		}},
		{name: "installed path differs from running binary", mutate: func(_ *liveConfig, _ *authorityEvidence, probe *fakeAuthorityProbe) {
			probe.fileDigests["/usr/local/bin/tewake-agent"] = strings.Repeat("9", 64)
		}},
		{name: "altered effective unit", mutate: func(_ *liveConfig, authority *authorityEvidence, _ *fakeAuthorityProbe) {
			authority.Supervisor.EffectiveUnitSHA256 = strings.Repeat("9", 64)
		}},
		{name: "altered runner authority", mutate: func(_ *liveConfig, _ *authorityEvidence, probe *fakeAuthorityProbe) {
			probe.runnerAuthority[1] = strings.Repeat("9", 64)
		}},
		{name: "decoy runner path", mutate: func(_ *liveConfig, _ *authorityEvidence, probe *fakeAuthorityProbe) {
			probe.runnerAuthority[0] = "/var/cache/tewake-agent/decoy/archive"
		}},
		{name: "stale harness commit", mutate: func(_ *liveConfig, _ *authorityEvidence, probe *fakeAuthorityProbe) {
			executable, _ := os.Executable()
			probe.buildVCS[executable] = [2]string{strings.Repeat("9", 40), "false"}
		}},
		{name: "modified harness build", mutate: func(config *liveConfig, _ *authorityEvidence, probe *fakeAuthorityProbe) {
			executable, _ := os.Executable()
			probe.buildVCS[executable] = [2]string{config.Provenance.ExpectedCommitSHA, "true"}
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			config, authority, probe, repository := provenanceFixture(t)
			testCase.mutate(&config, &authority, &probe)
			evidence, err := openEvidenceStore(config.EvidenceDirectory)
			if err != nil {
				t.Fatal(err)
			}
			if err := captureProvenanceEvidence(
				config,
				repository,
				authority,
				probe,
				evidence,
			); !errors.Is(err, errEvidenceInvalid) {
				t.Fatalf("captureProvenanceEvidence() error = %v, want errEvidenceInvalid", err)
			}
		})
	}
}

func TestCaptureProvenanceRecordsCleanExactInputs(t *testing.T) {
	config, authority, probe, repository := provenanceFixture(t)
	evidence, err := openEvidenceStore(config.EvidenceDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if err := captureProvenanceEvidence(config, repository, authority, probe, evidence); err != nil {
		t.Fatalf("captureProvenanceEvidence() error = %v", err)
	}
	recorded, err := loadEvidenceFile[provenanceEvidence](
		config.EvidenceDirectory,
		provenanceFileName,
	)
	if err != nil || recorded.CommitSHA != config.Provenance.ExpectedCommitSHA ||
		recorded.RunnerPackageSHA256 != config.Provenance.ExpectedRunnerPackageSHA256 ||
		recorded.AgentUnit.FragmentPath != authority.Agent.FragmentPath {
		t.Fatalf("provenance = %#v, error = %v", recorded, err)
	}
}

func provenanceFixture(
	t *testing.T,
) (liveConfig, authorityEvidence, fakeAuthorityProbe, string) {
	t.Helper()
	config := validTestConfig(t)
	runnerPath, officialPackage, err := expectedOfficialRunnerAuthority(config.RuntimeRoot)
	if err != nil {
		t.Fatal(err)
	}
	config.Provenance.ExpectedRunnerPackageSHA256 = officialPackage.Checksum
	config.Provenance.ExpectedInstalledAgentSHA256 = strings.Repeat("a", 64)
	config.Provenance.ExpectedAgentUnitSHA256 = strings.Repeat("b", 64)
	config.Provenance.ExpectedSupervisorUnitSHA256 = strings.Repeat("c", 64)
	authority := authorityEvidence{
		Version: evidenceVersion, Status: "passed",
		Agent: serviceAuthority{
			Unit: "tewake-agent.service", FragmentPath: "/etc/systemd/system/tewake-agent.service",
			EffectiveUnitSHA256: config.Provenance.ExpectedAgentUnitSHA256,
			ExecStartArgv:       expectedServiceArgv("serve", config.RuntimeRoot),
			Executable:          "/usr/local/bin/tewake-agent",
			ExecutableSHA256:    config.Provenance.ExpectedInstalledAgentSHA256,
		},
		Supervisor: serviceAuthority{
			Unit: "tewake-supervisor.service", FragmentPath: "/etc/systemd/system/tewake-supervisor.service",
			EffectiveUnitSHA256: config.Provenance.ExpectedSupervisorUnitSHA256,
			ExecStartArgv:       expectedServiceArgv("supervisor", config.RuntimeRoot),
			Executable:          "/usr/local/bin/tewake-agent",
			ExecutableSHA256:    config.Provenance.ExpectedInstalledAgentSHA256,
		},
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	probe := validAuthorityProbe()
	probe.fileDigests["/usr/local/bin/tewake-agent"] =
		config.Provenance.ExpectedInstalledAgentSHA256
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	probe.buildVCS[executable] = [2]string{
		config.Provenance.ExpectedCommitSHA,
		"false",
	}
	probe.trustedFiles[executable] = true
	probe.trustedFiles["/usr/local/bin/tewake-agent"] = true
	probe.buildVCS["/usr/local/bin/tewake-agent"] = [2]string{
		config.Provenance.ExpectedCommitSHA,
		"false",
	}
	probe.runnerAuthority = [3]string{
		runnerPath,
		officialPackage.Checksum,
		strconv.FormatInt(officialPackage.Size, 10),
	}
	probe.commit = config.Provenance.ExpectedCommitSHA
	probe.clean = true
	repository := filepath.Dir(config.ControllerStateDirectory)
	return config, authority, probe, repository
}
