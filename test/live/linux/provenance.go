package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/genm/sparerunner/internal/runner"
)

type unitProvenance struct {
	Unit          string   `json:"unit"`
	FragmentPath  string   `json:"fragmentPath"`
	DropInPaths   []string `json:"dropInPaths"`
	ExecStartArgv []string `json:"execStartArgv"`
	SHA256        string   `json:"sha256"`
}

func expectedOfficialRunnerAuthority(runtimeRoot string) (string, runner.Package, error) {
	pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
	if err != nil {
		return "", runner.Package{}, errEvidenceInvalid
	}
	key, err := pkg.CacheKey()
	if err != nil {
		return "", runner.Package{}, errEvidenceInvalid
	}
	return filepath.Join(runtimeRoot, ".sparerunner-official", key, "archive"), pkg, nil
}

type provenanceEvidence struct {
	Version                   int            `json:"version"`
	Status                    string         `json:"status"`
	CommitSHA                 string         `json:"commitSha"`
	WorktreeClean             bool           `json:"worktreeClean"`
	HarnessPath               string         `json:"harnessPath"`
	HarnessSHA256             string         `json:"harnessSha256"`
	HarnessVCSRevision        string         `json:"harnessVcsRevision"`
	HarnessVCSModified        bool           `json:"harnessVcsModified"`
	InstalledAgentPath        string         `json:"installedAgentPath"`
	InstalledAgentSHA256      string         `json:"installedAgentSha256"`
	InstalledAgentVCSRevision string         `json:"installedAgentVcsRevision"`
	InstalledAgentVCSModified bool           `json:"installedAgentVcsModified"`
	AgentUnit                 unitProvenance `json:"agentUnit"`
	SupervisorUnit            unitProvenance `json:"supervisorUnit"`
	RunnerPackagePath         string         `json:"runnerPackagePath"`
	RunnerPackageSHA256       string         `json:"runnerPackageSha256"`
	RunnerPackageSize         int64          `json:"runnerPackageSize"`
	GeneratedAt               string         `json:"generatedAt"`
}

func captureProvenanceEvidence(
	config liveConfig,
	repoRoot string,
	authority authorityEvidence,
	probe authorityProbe,
	evidence *evidenceStore,
) error {
	commit, clean, err := probe.gitState(repoRoot)
	if err != nil || !clean || commit != config.Provenance.ExpectedCommitSHA {
		return errEvidenceInvalid
	}
	harnessPath, err := os.Executable()
	if err != nil || !canonicalAbsolutePath(harnessPath) ||
		probe.trustedRootFile(harnessPath) != nil {
		return errEvidenceInvalid
	}
	harnessDigest, err := hashRegularFile(harnessPath)
	if err != nil {
		return errEvidenceInvalid
	}
	harnessRevision, harnessModified, err := probe.goBuildVCS(harnessPath)
	if err != nil || harnessModified || harnessRevision != commit {
		return errEvidenceInvalid
	}
	runnerPath, runnerDigest, runnerSize, err := probe.officialRunnerAuthority(
		config.RuntimeRoot,
	)
	expectedRunnerPath, officialPackage, expectedErr := expectedOfficialRunnerAuthority(
		config.RuntimeRoot,
	)
	if err != nil || expectedErr != nil || runnerPath != expectedRunnerPath ||
		runnerDigest != config.Provenance.ExpectedRunnerPackageSHA256 ||
		runnerSize != officialPackage.Size {
		return errEvidenceInvalid
	}
	if probe.trustedRootFile("/usr/local/bin/sparerunner-agent") != nil {
		return errEvidenceInvalid
	}
	installedDigest, err := probe.regularFileDigest("/usr/local/bin/sparerunner-agent")
	if err != nil ||
		installedDigest != config.Provenance.ExpectedInstalledAgentSHA256 {
		return errEvidenceInvalid
	}
	installedRevision, installedModified, err := probe.goBuildVCS(
		"/usr/local/bin/sparerunner-agent",
	)
	if err != nil || installedModified || installedRevision != commit {
		return errEvidenceInvalid
	}
	if authority.Agent.Executable != "/usr/local/bin/sparerunner-agent" ||
		authority.Supervisor.Executable != authority.Agent.Executable ||
		authority.Agent.ExecutableSHA256 != config.Provenance.ExpectedInstalledAgentSHA256 ||
		authority.Supervisor.ExecutableSHA256 != config.Provenance.ExpectedInstalledAgentSHA256 ||
		installedDigest != authority.Agent.ExecutableSHA256 ||
		authority.Agent.EffectiveUnitSHA256 != config.Provenance.ExpectedAgentUnitSHA256 ||
		authority.Supervisor.EffectiveUnitSHA256 !=
			config.Provenance.ExpectedSupervisorUnitSHA256 {
		return errEvidenceInvalid
	}
	return evidence.writeJSON(provenanceFileName, provenanceEvidence{
		Version:                   evidenceVersion,
		Status:                    "passed",
		CommitSHA:                 commit,
		WorktreeClean:             clean,
		HarnessPath:               harnessPath,
		HarnessSHA256:             harnessDigest,
		HarnessVCSRevision:        harnessRevision,
		HarnessVCSModified:        harnessModified,
		InstalledAgentPath:        authority.Agent.Executable,
		InstalledAgentSHA256:      installedDigest,
		InstalledAgentVCSRevision: installedRevision,
		InstalledAgentVCSModified: installedModified,
		AgentUnit: unitProvenance{
			Unit:          authority.Agent.Unit,
			FragmentPath:  authority.Agent.FragmentPath,
			DropInPaths:   authority.Agent.DropInPaths,
			ExecStartArgv: authority.Agent.ExecStartArgv,
			SHA256:        authority.Agent.EffectiveUnitSHA256,
		},
		SupervisorUnit: unitProvenance{
			Unit:          authority.Supervisor.Unit,
			FragmentPath:  authority.Supervisor.FragmentPath,
			DropInPaths:   authority.Supervisor.DropInPaths,
			ExecStartArgv: authority.Supervisor.ExecStartArgv,
			SHA256:        authority.Supervisor.EffectiveUnitSHA256,
		},
		RunnerPackagePath:   runnerPath,
		RunnerPackageSHA256: runnerDigest,
		RunnerPackageSize:   runnerSize,
		GeneratedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func hashRegularFile(path string) (string, error) {
	if !canonicalAbsolutePath(path) {
		return "", errEvidenceInvalid
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return "", errEvidenceInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return "", errEvidenceInvalid
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		_ = file.Close()
		return "", errEvidenceInvalid
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return "", errEvidenceInvalid
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
