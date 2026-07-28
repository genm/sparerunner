//go:build linux

package linux

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/genm/sparerunner/internal/runner"
)

const (
	workspaceTestAgentUID  = 21001
	workspaceTestAgentGID  = 21001
	workspaceTestRunnerUID = 22001
	workspaceTestRunnerGID = 22001
)

func newPrivilegedWorkspaceRoot(t *testing.T) (string, *os.Root, *OSWorkspace) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("workspace ownership handoff requires a root Supervisor test environment")
	}
	runtimeRoot, err := os.MkdirTemp("/var/lib", "tewake-workspace-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if strings.HasPrefix(runtimeRoot, "/var/lib/tewake-workspace-test-") {
			_ = os.RemoveAll(runtimeRoot)
			_ = os.RemoveAll(runtimeRoot + ".moved")
		}
	})
	if err := os.Chmod(runtimeRoot, 0o711); err != nil {
		t.Fatal(err)
	}
	executionsPath := filepath.Join(runtimeRoot, "executions")
	if err := os.Mkdir(executionsPath, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(executionsPath, workspaceTestAgentUID, workspaceTestAgentGID); err != nil {
		t.Fatal(err)
	}
	executions, err := os.OpenRoot(executionsPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = executions.Close() })
	workspace := NewOSWorkspace(
		workspaceTestAgentUID, workspaceTestAgentGID,
		workspaceTestRunnerUID, workspaceTestRunnerGID,
	)
	return runtimeRoot, executions, workspace
}

func newAuthorityTestWorkspace(
	t *testing.T,
	archiveData string,
) (*OSWorkspace, string, *int, *int) {
	t.Helper()
	cacheRoot, err := os.MkdirTemp("/var/cache", "tewake-authority-cache-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if strings.HasPrefix(cacheRoot, "/var/cache/tewake-authority-cache-test-") {
			_ = os.RemoveAll(cacheRoot)
		}
	})
	if err := os.Chown(cacheRoot, workspaceTestAgentUID, workspaceTestAgentGID); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cacheRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	const key = "test-official-authority"
	entry := filepath.Join(cacheRoot, "packages", key)
	if err := os.MkdirAll(entry, 0o700); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(entry, "archive")
	if err := os.WriteFile(archivePath, []byte(archiveData), 0o600); err != nil {
		t.Fatal(err)
	}
	copyCalls, verifyCalls := 0, 0
	pkg := runner.Package{Size: int64(len(archiveData))}
	workspace := &OSWorkspace{
		AgentUID: workspaceTestAgentUID, AgentGID: workspaceTestAgentGID,
		RunnerUID: workspaceTestRunnerUID, RunnerGID: workspaceTestRunnerGID,
		CacheRoot: cacheRoot,
		Package:   pkg,
		cacheKey: func(runner.Package) (string, error) {
			return key, nil
		},
		copyOfficialArchive: func(destination, source *os.File, _ runner.Package) error {
			copyCalls++
			if _, err := source.Seek(0, io.SeekStart); err != nil {
				return err
			}
			data, err := io.ReadAll(io.LimitReader(source, int64(len(archiveData)+1)))
			if err != nil || string(data) != archiveData {
				return runner.ErrPackageIntegrity
			}
			if _, err := destination.Write(data); err != nil {
				return err
			}
			return destination.Sync()
		},
		verifyOfficialArchive: func(archive *os.File, _ runner.Package) error {
			verifyCalls++
			if _, err := archive.Seek(0, io.SeekStart); err != nil {
				return err
			}
			data, err := io.ReadAll(io.LimitReader(archive, int64(len(archiveData)+1)))
			if err != nil || string(data) != archiveData {
				return runner.ErrPackageIntegrity
			}
			return nil
		},
		materializeArchive: func(destination *os.Root, archive *os.File, _ runner.Package) error {
			if _, err := archive.Seek(0, io.SeekStart); err != nil {
				return err
			}
			data, err := io.ReadAll(archive)
			if err != nil || string(data) != archiveData {
				return runner.ErrPackageIntegrity
			}
			return destination.WriteFile("run.sh", append([]byte("#!/bin/sh\n# "), data...), 0o700)
		},
	}
	t.Cleanup(func() {
		workspace.authorityMu.Lock()
		defer workspace.authorityMu.Unlock()
		if workspace.authority != nil {
			_ = workspace.authority.Close()
			workspace.authority = nil
		}
	})
	return workspace, archivePath, &copyCalls, &verifyCalls
}

func TestOfficialAuthorityPublishesOnceAndPrepareUsesRootCopy(t *testing.T) {
	runtimeRoot, executions, _ := newPrivilegedWorkspaceRoot(t)
	workspace, cacheArchive, copyCalls, verifyCalls := newAuthorityTestWorkspace(t, "verified archive")
	if err := workspace.ValidateOfficialAuthority(context.Background(), runtimeRoot); err != nil {
		t.Fatal(err)
	}
	authorityPath := filepath.Join(
		runtimeRoot, officialAuthorityDirectory, "test-official-authority", "archive",
	)
	info, err := os.Lstat(authorityPath)
	if err != nil || !safeOfficialAuthorityFile(info, int64(len("verified archive"))) {
		t.Fatalf("published authority info=%#v err=%v", info, err)
	}
	if err := workspace.ValidateOfficialAuthority(context.Background(), runtimeRoot); err != nil {
		t.Fatal(err)
	}
	if *copyCalls != 1 || *verifyCalls != 0 {
		t.Fatalf("healthy cached probes copied=%d rehashed=%d", *copyCalls, *verifyCalls)
	}
	if err := os.WriteFile(cacheArchive, []byte("attacker archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	name := strings.Repeat("8", 64)
	if _, err := workspace.PrepareOfficial(context.Background(), executions, name); err != nil {
		t.Fatal(err)
	}
	runScript, err := os.ReadFile(filepath.Join(runtimeRoot, "executions", name, "run.sh"))
	if err != nil || !strings.Contains(string(runScript), "verified archive") {
		t.Fatalf("PrepareOfficial followed Agent cache replacement: %q err=%v", runScript, err)
	}
	if err := os.Remove(authorityPath); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ValidateOfficialAuthority(context.Background(), runtimeRoot); err == nil {
		t.Fatal("readiness accepted a missing canonical authority path")
	}
}

func TestOfficialAuthorityRecoversKnownPublicationCrashResidue(t *testing.T) {
	const archiveData = "verified crash archive"
	for _, boundary := range []string{"temp-only", "archive-and-temp"} {
		t.Run(boundary, func(t *testing.T) {
			runtimeRoot, _, _ := newPrivilegedWorkspaceRoot(t)
			workspace, _, copyCalls, verifyCalls := newAuthorityTestWorkspace(t, archiveData)
			authorityDirectory := filepath.Join(
				runtimeRoot,
				officialAuthorityDirectory,
				"test-official-authority",
			)
			if err := os.MkdirAll(authorityDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			temporaryPath := filepath.Join(
				authorityDirectory,
				".archive-"+strings.Repeat("a", 32),
			)
			if err := os.WriteFile(temporaryPath, []byte(archiveData), 0o400); err != nil {
				t.Fatal(err)
			}
			archivePath := filepath.Join(authorityDirectory, "archive")
			if boundary == "archive-and-temp" {
				if err := os.Link(temporaryPath, archivePath); err != nil {
					t.Fatal(err)
				}
			}
			if err := workspace.ValidateOfficialAuthority(context.Background(), runtimeRoot); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(authorityDirectory)
			if err != nil || len(entries) != 1 || entries[0].Name() != "archive" {
				t.Fatalf("authority entries=%#v err=%v", entries, err)
			}
			info, err := os.Lstat(archivePath)
			if err != nil || !safeOfficialAuthorityFile(info, int64(len(archiveData))) {
				t.Fatalf("recovered authority info=%#v err=%v", info, err)
			}
			if *copyCalls != 0 || *verifyCalls != 2 {
				t.Fatalf("copyCalls=%d verifyCalls=%d", *copyCalls, *verifyCalls)
			}
		})
	}
}

func TestOfficialAuthorityUnknownPublicationResidueFailsClosed(t *testing.T) {
	runtimeRoot, _, _ := newPrivilegedWorkspaceRoot(t)
	workspace, _, copyCalls, _ := newAuthorityTestWorkspace(t, "verified archive")
	authorityDirectory := filepath.Join(
		runtimeRoot,
		officialAuthorityDirectory,
		"test-official-authority",
	)
	if err := os.MkdirAll(authorityDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(authorityDirectory, "unknown")
	if err := os.WriteFile(unknown, []byte("do not remove"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ValidateOfficialAuthority(
		context.Background(),
		runtimeRoot,
	); !errors.Is(err, runner.ErrPackageIntegrity) {
		t.Fatalf("ValidateOfficialAuthority error=%v", err)
	}
	if _, err := os.Lstat(unknown); err != nil {
		t.Fatalf("unknown residue was removed: %v", err)
	}
	if *copyCalls != 0 {
		t.Fatalf("unknown residue triggered %d cache copies", *copyCalls)
	}
}

func TestOfficialAuthorityMalformedTemporaryResidueFailsClosed(t *testing.T) {
	const archiveData = "verified archive"
	runtimeRoot, _, _ := newPrivilegedWorkspaceRoot(t)
	workspace, _, copyCalls, verifyCalls := newAuthorityTestWorkspace(t, archiveData)
	authorityDirectory := filepath.Join(
		runtimeRoot,
		officialAuthorityDirectory,
		"test-official-authority",
	)
	if err := os.MkdirAll(authorityDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	temporary := filepath.Join(
		authorityDirectory,
		".archive-"+strings.Repeat("b", 32),
	)
	if err := os.WriteFile(temporary, []byte("tampered archive"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ValidateOfficialAuthority(
		context.Background(),
		runtimeRoot,
	); !errors.Is(err, runner.ErrPackageIntegrity) {
		t.Fatalf("ValidateOfficialAuthority error=%v", err)
	}
	if _, err := os.Lstat(temporary); err != nil {
		t.Fatalf("malformed residue was removed: %v", err)
	}
	if *copyCalls != 0 || *verifyCalls != 1 {
		t.Fatalf("copyCalls=%d verifyCalls=%d", *copyCalls, *verifyCalls)
	}
}

func TestOfficialAuthorityFailsClosedWhenCacheArchiveIsAbsent(t *testing.T) {
	runtimeRoot, _, _ := newPrivilegedWorkspaceRoot(t)
	workspace, cacheArchive, _, _ := newAuthorityTestWorkspace(t, "verified archive")
	if err := os.Remove(cacheArchive); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ValidateOfficialAuthority(context.Background(), runtimeRoot); err == nil {
		t.Fatal("readiness accepted an absent Agent archive")
	}
}

func TestOfficialAuthorityRejectsExistingCorruptionWithoutRepeatedHashing(t *testing.T) {
	runtimeRoot, _, _ := newPrivilegedWorkspaceRoot(t)
	workspace, _, _, verifyCalls := newAuthorityTestWorkspace(t, "verified archive")
	authorityDirectory := filepath.Join(
		runtimeRoot, officialAuthorityDirectory, "test-official-authority",
	)
	if err := os.MkdirAll(authorityDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	authorityPath := filepath.Join(authorityDirectory, "archive")
	if err := os.WriteFile(authorityPath, []byte("corrupt archive!"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ValidateOfficialAuthority(context.Background(), runtimeRoot); err == nil {
		t.Fatal("readiness accepted a corrupt published authority")
	}
	if err := workspace.ValidateOfficialAuthority(context.Background(), runtimeRoot); err == nil {
		t.Fatal("readiness repaired a corrupt authority from the Agent cache")
	}
	if *verifyCalls != 1 {
		t.Fatalf("corrupt authority was rehashed %d times", *verifyCalls)
	}
}

func createAgentOwnedWorkspace(t *testing.T, executionsPath, name string, escapingSymlink bool) {
	t.Helper()
	root := filepath.Join(executionsPath, name)
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "run.sh"), []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	target := "../run.sh"
	if escapingSymlink {
		target = "../../../outside"
	}
	if err := os.Symlink(target, filepath.Join(root, "bin", "runner-link")); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{
		filepath.Join(root, "bin", "runner-link"),
		filepath.Join(root, "bin"),
		filepath.Join(root, "run.sh"),
		root,
	} {
		if err := os.Lchown(value, workspaceTestAgentUID, workspaceTestAgentGID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOSWorkspaceHandsCompleteAgentTreeToRunnerAndRemovesIt(t *testing.T) {
	runtimeRoot, executions, workspace := newPrivilegedWorkspaceRoot(t)
	name := strings.Repeat("a", 64)
	createAgentOwnedWorkspace(t, filepath.Join(runtimeRoot, "executions"), name, false)
	ref, err := workspace.Prepare(context.Background(), executions, name)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Backend != WorkspaceBackend || ref.OwnerID == "" {
		t.Fatalf("workspace ref=%#v", ref)
	}
	observed, err := workspace.Observe(context.Background(), executions, name)
	if err != nil || observed != ref {
		t.Fatalf("observed=%#v err=%v", observed, err)
	}
	runInfo, err := os.Lstat(filepath.Join(runtimeRoot, "executions", name, "run.sh"))
	if err != nil || !ownedBy(runInfo, workspaceTestRunnerUID, workspaceTestRunnerGID) {
		t.Fatalf("runner file was not handed off: info=%#v err=%v", runInfo, err)
	}
	if err := workspace.Remove(context.Background(), executions, name); err != nil {
		t.Fatal(err)
	}
	if _, err := executions.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace remains after cleanup: %v", err)
	}
}

func TestOSWorkspaceRejectsSymlinkEscapeBeforeOwnershipHandoff(t *testing.T) {
	runtimeRoot, executions, workspace := newPrivilegedWorkspaceRoot(t)
	name := strings.Repeat("b", 64)
	createAgentOwnedWorkspace(t, filepath.Join(runtimeRoot, "executions"), name, true)
	if _, err := workspace.Prepare(context.Background(), executions, name); err == nil {
		t.Fatal("escaping symlink was handed to the runner")
	}
	info, err := executions.Lstat(name)
	if err != nil || !rootOwned(info) {
		t.Fatalf("failed handoff was not quarantined under root ownership: info=%#v err=%v", info, err)
	}
}

func TestOSWorkspaceRejectsExternalHardLinkBeforeOwnershipHandoff(t *testing.T) {
	runtimeRoot, executions, workspace := newPrivilegedWorkspaceRoot(t)
	name := strings.Repeat("f", 64)
	createAgentOwnedWorkspace(t, filepath.Join(runtimeRoot, "executions"), name, false)

	agentState, err := os.MkdirTemp("/var/lib", "tewake-agent-state-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if strings.HasPrefix(agentState, "/var/lib/tewake-agent-state-test-") {
			_ = os.RemoveAll(agentState)
		}
	})
	if err := os.Chown(agentState, workspaceTestAgentUID, workspaceTestAgentGID); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(agentState, 0o700); err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(agentState, "agent-secret")
	if err := os.WriteFile(external, []byte("must-not-be-handed-to-runner"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(external, workspaceTestAgentUID, workspaceTestAgentGID); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(external, filepath.Join(runtimeRoot, "executions", name, "external-link")); err != nil {
		t.Fatal(err)
	}

	if _, err := workspace.Prepare(context.Background(), executions, name); err == nil {
		t.Fatal("external hard link was handed to the runner")
	}
	externalInfo, err := os.Lstat(external)
	if err != nil || !ownedBy(externalInfo, workspaceTestAgentUID, workspaceTestAgentGID) {
		t.Fatalf("external file ownership changed: info=%#v err=%v", externalInfo, err)
	}
}

func TestOSWorkspaceRejectsSecondTreeForSingleSlotIdentity(t *testing.T) {
	runtimeRoot, executions, workspace := newPrivilegedWorkspaceRoot(t)
	first := strings.Repeat("d", 64)
	second := strings.Repeat("e", 64)
	executionsPath := filepath.Join(runtimeRoot, "executions")
	createAgentOwnedWorkspace(t, executionsPath, first, false)
	createAgentOwnedWorkspace(t, executionsPath, second, false)
	if _, err := workspace.Prepare(context.Background(), executions, second); err == nil {
		t.Fatal("second prepared tree was handed to the same slot identity")
	}
	for _, name := range []string{first, second} {
		info, err := executions.Lstat(name)
		if err != nil || !ownedBy(info, workspaceTestAgentUID, workspaceTestAgentGID) {
			t.Fatalf("rejected tree %s changed ownership: info=%#v err=%v", name, info, err)
		}
	}
}

func TestOSWorkspaceRejectsReplacedExecutionsAncestor(t *testing.T) {
	runtimeRoot, executions, workspace := newPrivilegedWorkspaceRoot(t)
	name := strings.Repeat("c", 64)
	createAgentOwnedWorkspace(t, filepath.Join(runtimeRoot, "executions"), name, false)
	if err := os.Rename(runtimeRoot, runtimeRoot+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(runtimeRoot, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(runtimeRoot, "executions"), 0o711); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(filepath.Join(runtimeRoot, "executions"), workspaceTestAgentUID, workspaceTestAgentGID); err != nil {
		t.Fatal(err)
	}
	if _, err := workspace.Prepare(context.Background(), executions, name); err == nil {
		t.Fatal("replaced executions ancestor was accepted")
	}
}

func TestPinnedWorkspaceDoesNotFollowExecutionNameSwap(t *testing.T) {
	runtimeRoot, executions, workspace := newPrivilegedWorkspaceRoot(t)
	name := strings.Repeat("9", 64)
	executionsPath := filepath.Join(runtimeRoot, "executions")
	createAgentOwnedWorkspace(t, executionsPath, name, false)
	ref, err := workspace.Prepare(context.Background(), executions, name)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := workspace.PinLaunch(context.Background(), executions, name, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	original := filepath.Join(executionsPath, name+".original")
	if err := os.Rename(filepath.Join(executionsPath, name), original); err != nil {
		t.Fatal(err)
	}
	createAgentOwnedWorkspace(t, executionsPath, name, false)
	if err := os.WriteFile(filepath.Join(executionsPath, name, "run.sh"), []byte("attacker"), 0o700); err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(pinned.executable)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "#!/bin/sh\n" {
		t.Fatalf("pinned executable followed replacement: %q", content)
	}
	pinnedInfo, err := pinned.directory.Stat()
	if err != nil {
		t.Fatal(err)
	}
	originalInfo, err := os.Lstat(original)
	if err != nil || !os.SameFile(pinnedInfo, originalInfo) {
		t.Fatalf("pinned directory no longer names the original: info=%#v err=%v", originalInfo, err)
	}
}

func TestWorkspaceRemovalDeletesExecutionHomeCanaryBeforeNextJob(t *testing.T) {
	runtimeRoot, executions, workspace := newPrivilegedWorkspaceRoot(t)
	executionsPath := filepath.Join(runtimeRoot, "executions")
	first := strings.Repeat("7", 64)
	createAgentOwnedWorkspace(t, executionsPath, first, false)
	home := filepath.Join(executionsPath, first, ".tewake-home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "canary"), []byte("first job"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{filepath.Join(home, "canary"), home} {
		if err := os.Chown(name, workspaceTestAgentUID, workspaceTestAgentGID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := workspace.Prepare(context.Background(), executions, first); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Remove(context.Background(), executions, first); err != nil {
		t.Fatal(err)
	}

	second := strings.Repeat("8", 64)
	createAgentOwnedWorkspace(t, executionsPath, second, false)
	if _, err := os.Lstat(filepath.Join(executionsPath, second, ".tewake-home", "canary")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first job HOME canary survived into next workspace: %v", err)
	}
}
