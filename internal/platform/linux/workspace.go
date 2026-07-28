//go:build linux

package linux

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/genm/sparerunner/internal/runner"
)

// OSWorkspace is the Linux stat identity authority. It records the inode,
// device, and dedicated runner ownership rather than a mutable path.
//
// The systemd units pre-create the runtime root as root:root 0711 and its
// executions child as the unprivileged Agent identity with mode 0711. The
// Agent may create a placeholder tree, but the root Supervisor discards it,
// reconstructs the official package, and alone hands that tree to the dedicated
// runner identity.
type OSWorkspace struct {
	AgentUID  int
	AgentGID  int
	RunnerUID int
	RunnerGID int
	CacheRoot string
	Package   runner.Package

	authorityMu          sync.Mutex
	authority            *os.File
	authorityRuntimeRoot string
	authorityName        string
	authorityTerminalErr error

	cacheKey              func(runner.Package) (string, error)
	copyOfficialArchive   func(*os.File, *os.File, runner.Package) error
	verifyOfficialArchive func(*os.File, runner.Package) error
	materializeArchive    func(*os.Root, *os.File, runner.Package) error
}

const officialAuthorityDirectory = ".sparerunner-official"

func NewOSWorkspace(agentUID, agentGID, runnerUID, runnerGID int) *OSWorkspace {
	return &OSWorkspace{
		AgentUID: agentUID, AgentGID: agentGID,
		RunnerUID: runnerUID, RunnerGID: runnerGID,
	}
}

// NewVerifiedOSWorkspace creates the root-side package authority. The package
// is fixed from the Supervisor's own platform and the cache root is fixed by
// service configuration; neither value is accepted from an Agent request.
func NewVerifiedOSWorkspace(
	agentUID, agentGID, runnerUID, runnerGID int,
	cacheRoot string,
	pkg runner.Package,
) (*OSWorkspace, error) {
	if !filepath.IsAbs(cacheRoot) || filepath.Clean(cacheRoot) != cacheRoot || cacheRoot == "/" {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if _, err := pkg.CacheKey(); err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return &OSWorkspace{
		AgentUID: agentUID, AgentGID: agentGID,
		RunnerUID: runnerUID, RunnerGID: runnerGID,
		CacheRoot: cacheRoot, Package: pkg,
		cacheKey:              runner.Package.CacheKey,
		copyOfficialArchive:   runner.CopyAndVerifyOfficialArchive,
		verifyOfficialArchive: runner.VerifyOfficialArchive,
		materializeArchive:    runner.MaterializePinnedOfficialArchive,
	}, nil
}

func (workspace *OSWorkspace) RunnerIdentity() RunnerIdentity {
	return RunnerIdentity{UID: workspace.RunnerUID, GID: workspace.RunnerGID}
}

func (workspace *OSWorkspace) AgentIdentity() RunnerIdentity {
	return RunnerIdentity{UID: workspace.AgentUID, GID: workspace.AgentGID}
}

func (workspace *OSWorkspace) OfficialAuthorityConfigured() bool {
	if workspace == nil || workspace.CacheRoot == "" ||
		workspace.cacheKey == nil || workspace.copyOfficialArchive == nil ||
		workspace.verifyOfficialArchive == nil || workspace.materializeArchive == nil {
		return false
	}
	_, err := workspace.cacheKey(workspace.Package)
	return err == nil
}

// ValidateOfficialAuthority bootstraps the immutable root-side archive once,
// then performs only descriptor/path identity checks on subsequent heartbeats.
// The Agent cache is used solely as untrusted input for the initial copy.
func (workspace *OSWorkspace) ValidateOfficialAuthority(ctx context.Context, runtimeRoot string) error {
	if err := ctx.Err(); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if !workspace.OfficialAuthorityConfigured() ||
		workspace.ValidateRuntimeRoot(ctx, runtimeRoot) != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	workspace.authorityMu.Lock()
	defer workspace.authorityMu.Unlock()
	if workspace.authorityTerminalErr != nil {
		return workspace.authorityTerminalErr
	}
	if workspace.authority != nil {
		archive, err := workspace.openPinnedOfficialArchiveLocked(runtimeRoot)
		if err != nil {
			return err
		}
		return archive.Close()
	}
	key, err := workspace.cacheKey(workspace.Package)
	if err != nil || !validAuthorityKey(key) {
		return runner.ErrStrongOwnershipUnavailable
	}
	authorityName := path.Join(officialAuthorityDirectory, key, "archive")
	runtime, err := os.OpenRoot(runtimeRoot)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	defer runtime.Close()
	if err := ensureAuthorityDirectory(runtime, officialAuthorityDirectory); err != nil {
		return err
	}
	keyDirectory := path.Join(officialAuthorityDirectory, key)
	if err := ensureAuthorityDirectory(runtime, keyDirectory); err != nil {
		return err
	}
	authorityRoot, err := runtime.OpenRoot(keyDirectory)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	defer authorityRoot.Close()
	if err := workspace.recoverOfficialAuthorityPublication(
		authorityRoot,
		filepath.Join(runtimeRoot, keyDirectory),
	); err != nil {
		workspace.authorityTerminalErr = runner.ErrPackageIntegrity
		return workspace.authorityTerminalErr
	}
	if _, err := runtime.Lstat(authorityName); err == nil {
		if err := workspace.adoptExistingAuthority(runtime, runtimeRoot, authorityName); err != nil {
			// A published authority is never repaired from Agent-controlled bytes.
			workspace.authorityTerminalErr = runner.ErrPackageIntegrity
			return workspace.authorityTerminalErr
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return runner.ErrStrongOwnershipUnavailable
	}
	source, err := workspace.openOfficialArchive()
	if err != nil {
		return runner.ErrPackageIntegrity
	}
	defer source.Close()
	temporaryName, err := authorityTemporaryName()
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	temporary, err := authorityRoot.OpenFile(temporaryName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o400)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	published := false
	defer func() {
		if !published {
			_ = temporary.Close()
			_ = authorityRoot.Remove(temporaryName)
		}
	}()
	if err := temporary.Chown(0, 0); err != nil ||
		temporary.Chmod(0o400) != nil ||
		workspace.copyOfficialArchive(temporary, source, workspace.Package) != nil {
		return runner.ErrPackageIntegrity
	}
	temporaryInfo, err := temporary.Stat()
	if err != nil || !safeOfficialAuthorityFile(temporaryInfo, workspace.Package.Size) {
		return runner.ErrPackageIntegrity
	}
	if err := authorityRoot.Link(temporaryName, "archive"); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return runner.ErrStrongOwnershipUnavailable
		}
		_ = temporary.Close()
		_ = authorityRoot.Remove(temporaryName)
		published = true
		if err := workspace.adoptExistingAuthority(runtime, runtimeRoot, authorityName); err != nil {
			workspace.authorityTerminalErr = runner.ErrPackageIntegrity
			return workspace.authorityTerminalErr
		}
		return nil
	}
	if err := syncDirectory(filepath.Join(runtimeRoot, keyDirectory)); err != nil ||
		authorityRoot.Remove(temporaryName) != nil ||
		syncDirectory(filepath.Join(runtimeRoot, keyDirectory)) != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	publishedInfo, err := runtime.Lstat(authorityName)
	if err != nil || !safeOfficialAuthorityFile(publishedInfo, workspace.Package.Size) ||
		!os.SameFile(temporaryInfo, publishedInfo) {
		return runner.ErrPackageIntegrity
	}
	workspace.authority = temporary
	workspace.authorityRuntimeRoot = runtimeRoot
	workspace.authorityName = authorityName
	published = true
	return nil
}

func (workspace *OSWorkspace) recoverOfficialAuthorityPublication(
	authorityRoot *os.Root,
	authorityDirectory string,
) error {
	directory, err := authorityRoot.Open(".")
	if err != nil {
		return runner.ErrPackageIntegrity
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil || closeErr != nil {
		return runner.ErrPackageIntegrity
	}
	temporaryName := ""
	for _, entry := range entries {
		switch {
		case entry.Name() == "archive":
		case validAuthorityTemporaryName(entry.Name()):
			if temporaryName != "" {
				return runner.ErrPackageIntegrity
			}
			temporaryName = entry.Name()
		default:
			// This directory is a fixed root-owned publication boundary. Never
			// normalize or delete an entry not created by this protocol.
			return runner.ErrPackageIntegrity
		}
	}
	if temporaryName == "" {
		return nil
	}

	temporaryPathInfo, err := authorityRoot.Lstat(temporaryName)
	if err != nil || !safeOfficialPublicationFile(temporaryPathInfo, workspace.Package.Size) {
		return runner.ErrPackageIntegrity
	}
	temporary, err := authorityRoot.Open(temporaryName)
	if err != nil {
		return runner.ErrPackageIntegrity
	}
	defer temporary.Close()
	temporaryInfo, err := temporary.Stat()
	if err != nil || !safeOfficialPublicationFile(temporaryInfo, workspace.Package.Size) ||
		!os.SameFile(temporaryPathInfo, temporaryInfo) ||
		workspace.verifyOfficialArchive(temporary, workspace.Package) != nil {
		return runner.ErrPackageIntegrity
	}

	archivePathInfo, err := authorityRoot.Lstat("archive")
	switch {
	case errors.Is(err, os.ErrNotExist):
		if !singleLink(temporaryInfo) ||
			authorityRoot.Link(temporaryName, "archive") != nil ||
			syncDirectory(authorityDirectory) != nil ||
			authorityRoot.Remove(temporaryName) != nil ||
			syncDirectory(authorityDirectory) != nil {
			return runner.ErrPackageIntegrity
		}
	case err != nil:
		return runner.ErrPackageIntegrity
	default:
		archive, openErr := authorityRoot.Open("archive")
		if openErr != nil {
			return runner.ErrPackageIntegrity
		}
		archiveInfo, statErr := archive.Stat()
		archiveCloseErr := archive.Close()
		if statErr != nil || archiveCloseErr != nil ||
			!sameTwoLinkOfficialAuthorityFiles(
				workspace.Package.Size,
				temporaryInfo,
				archivePathInfo,
				archiveInfo,
			) ||
			authorityRoot.Remove(temporaryName) != nil ||
			syncDirectory(authorityDirectory) != nil {
			return runner.ErrPackageIntegrity
		}
	}
	recovered, err := authorityRoot.Lstat("archive")
	if err != nil || !safeOfficialAuthorityFile(recovered, workspace.Package.Size) ||
		!os.SameFile(temporaryInfo, recovered) {
		return runner.ErrPackageIntegrity
	}
	return nil
}

func (workspace *OSWorkspace) adoptExistingAuthority(
	runtime *os.Root,
	runtimeRoot, authorityName string,
) error {
	if err := validateAuthorityDirectories(runtime, authorityName); err != nil {
		return err
	}
	pathInfo, err := runtime.Lstat(authorityName)
	if err != nil || !safeOfficialAuthorityFile(pathInfo, workspace.Package.Size) {
		return runner.ErrPackageIntegrity
	}
	archive, err := runtime.Open(authorityName)
	if err != nil {
		return runner.ErrPackageIntegrity
	}
	opened, err := archive.Stat()
	if err != nil || !safeOfficialAuthorityFile(opened, workspace.Package.Size) ||
		!os.SameFile(pathInfo, opened) ||
		workspace.verifyOfficialArchive(archive, workspace.Package) != nil {
		_ = archive.Close()
		return runner.ErrPackageIntegrity
	}
	current, err := runtime.Lstat(authorityName)
	if err != nil || !safeOfficialAuthorityFile(current, workspace.Package.Size) ||
		!os.SameFile(opened, current) {
		_ = archive.Close()
		return runner.ErrPackageIntegrity
	}
	workspace.authority = archive
	workspace.authorityRuntimeRoot = runtimeRoot
	workspace.authorityName = authorityName
	return nil
}

func (workspace *OSWorkspace) openPinnedOfficialArchiveLocked(runtimeRoot string) (*os.File, error) {
	if workspace.authority == nil || workspace.authorityRuntimeRoot != runtimeRoot ||
		workspace.authorityName == "" {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	runtime, err := os.OpenRoot(runtimeRoot)
	if err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	defer runtime.Close()
	if err := validateAuthorityDirectories(runtime, workspace.authorityName); err != nil {
		return nil, err
	}
	pinnedInfo, pinErr := workspace.authority.Stat()
	pathInfo, pathErr := runtime.Lstat(workspace.authorityName)
	if pinErr != nil || pathErr != nil ||
		!safeOfficialAuthorityFile(pinnedInfo, workspace.Package.Size) ||
		!safeOfficialAuthorityFile(pathInfo, workspace.Package.Size) ||
		!os.SameFile(pinnedInfo, pathInfo) {
		return nil, runner.ErrPackageIntegrity
	}
	archive, err := runtime.Open(workspace.authorityName)
	if err != nil {
		return nil, runner.ErrPackageIntegrity
	}
	opened, err := archive.Stat()
	if err != nil || !safeOfficialAuthorityFile(opened, workspace.Package.Size) ||
		!os.SameFile(pinnedInfo, opened) {
		_ = archive.Close()
		return nil, runner.ErrPackageIntegrity
	}
	return archive, nil
}

func (workspace *OSWorkspace) openPinnedOfficialArchive(ctx context.Context) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	workspace.authorityMu.Lock()
	defer workspace.authorityMu.Unlock()
	return workspace.openPinnedOfficialArchiveLocked(workspace.authorityRuntimeRoot)
}

func ensureAuthorityDirectory(root *os.Root, name string) error {
	if _, err := root.Lstat(name); errors.Is(err, os.ErrNotExist) {
		if err := root.Mkdir(name, 0o700); err != nil {
			return runner.ErrStrongOwnershipUnavailable
		}
	} else if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	info, err := root.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		!rootOwned(info) || info.Mode().Perm() != 0o700 {
		return runner.ErrStrongOwnershipUnavailable
	}
	opened, err := root.OpenRoot(name)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	defer opened.Close()
	pinned, err := opened.Stat(".")
	if err != nil || !os.SameFile(info, pinned) {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func validateAuthorityDirectories(root *os.Root, authorityName string) error {
	keyDirectory := path.Dir(authorityName)
	for _, name := range []string{officialAuthorityDirectory, keyDirectory} {
		info, err := root.Lstat(name)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			!rootOwned(info) || info.Mode().Perm() != 0o700 {
			return runner.ErrStrongOwnershipUnavailable
		}
		opened, err := root.OpenRoot(name)
		if err != nil {
			return runner.ErrStrongOwnershipUnavailable
		}
		pinned, statErr := opened.Stat(".")
		_ = opened.Close()
		if statErr != nil || !os.SameFile(info, pinned) {
			return runner.ErrStrongOwnershipUnavailable
		}
	}
	return nil
}

func safeOfficialAuthorityFile(info os.FileInfo, size int64) bool {
	return info != nil && info.Mode().IsRegular() && rootOwned(info) &&
		info.Mode().Perm() == 0o400 && singleLink(info) && info.Size() == size
}

func safeOfficialPublicationFile(info os.FileInfo, size int64) bool {
	if info == nil || !info.Mode().IsRegular() || !rootOwned(info) ||
		info.Mode().Perm() != 0o400 || info.Size() != size {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Nlink == 1 || stat.Nlink == 2)
}

func sameTwoLinkOfficialAuthorityFiles(size int64, infos ...os.FileInfo) bool {
	if len(infos) == 0 {
		return false
	}
	for _, info := range infos {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 2 || !info.Mode().IsRegular() ||
			!rootOwned(info) || info.Mode().Perm() != 0o400 ||
			info.Size() != size || !os.SameFile(infos[0], info) {
			return false
		}
	}
	return true
}

func validAuthorityKey(key string) bool {
	return key != "" && len(key) <= 255 && key != "." && key != ".." &&
		path.Base(key) == key && !strings.ContainsRune(key, '\x00')
}

func authorityTemporaryName() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return ".archive-" + hex.EncodeToString(entropy[:]), nil
}

func validAuthorityTemporaryName(name string) bool {
	const prefix = ".archive-"
	return strings.HasPrefix(name, prefix) &&
		len(name) == len(prefix)+32 &&
		canonicalLowerHex(name[len(prefix):])
}

func (workspace *OSWorkspace) ValidateRuntimeRoot(ctx context.Context, root string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(root) {
		return errors.New("runtime root is not absolute")
	}
	cleaned := filepath.Clean(root)
	if cleaned != root || cleaned == "/" {
		return errors.New("runtime root must be canonical and scoped")
	}
	if workspace.AgentUID <= 0 || workspace.AgentGID <= 0 ||
		workspace.RunnerUID <= 0 || workspace.RunnerGID <= 0 ||
		workspace.AgentUID == workspace.RunnerUID ||
		workspace.AgentGID == workspace.RunnerGID {
		return errors.New("agent and runner identities must be distinct")
	}
	for current := cleaned; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !rootOwned(info) || info.Mode().Perm()&0o022 != 0 {
			return errors.New("runtime root ancestor is unsafe")
		}
		if current == "/" {
			break
		}
	}
	info, err := os.Lstat(cleaned)
	if err != nil || info.Mode().Perm() != 0o711 {
		return errors.New("runtime root must be root-owned 0711")
	}
	runtimeRoot, err := os.OpenRoot(cleaned)
	if err != nil {
		return errors.New("runtime root cannot be pinned")
	}
	defer runtimeRoot.Close()
	opened, err := runtimeRoot.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		return errors.New("runtime root changed while opening")
	}
	executions, err := runtimeRoot.Lstat("executions")
	if err != nil || !executions.IsDir() || executions.Mode()&os.ModeSymlink != 0 ||
		!ownedBy(executions, workspace.AgentUID, workspace.AgentGID) ||
		executions.Mode().Perm() != 0o711 {
		return errors.New("executions root must be agent-owned 0711")
	}
	executionsRoot, err := runtimeRoot.OpenRoot("executions")
	if err != nil {
		return errors.New("executions root cannot be pinned")
	}
	defer executionsRoot.Close()
	openedExecutions, err := executionsRoot.Stat(".")
	if err != nil || !os.SameFile(executions, openedExecutions) {
		return errors.New("executions root changed while opening")
	}
	return nil
}

func (workspace *OSWorkspace) Prepare(ctx context.Context, root *os.Root, name string) (runner.WorkspaceRef, error) {
	if err := ctx.Err(); err != nil {
		return runner.WorkspaceRef{}, err
	}
	if root == nil || !validWorkspaceName(name) || workspace.validateExecutionsRoot(ctx, root) != nil {
		return runner.WorkspaceRef{}, errors.New("missing or unsafe workspace root")
	}
	if busy, err := workspace.singleWorkspace(root, name, workspace.AgentUID, workspace.AgentGID); err != nil || busy {
		return runner.WorkspaceRef{}, errors.New("runner slot has another workspace")
	}
	before, err := root.Lstat(name)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 ||
		!ownedBy(before, workspace.AgentUID, workspace.AgentGID) ||
		before.Mode().Perm() != 0o700 {
		return runner.WorkspaceRef{}, errors.New("workspace must begin agent-owned 0700")
	}
	workspaceRoot, err := root.OpenRoot(name)
	if err != nil {
		return runner.WorkspaceRef{}, err
	}
	defer workspaceRoot.Close()

	// A package may contain auto-update state and writable runner files. Keep its
	// top directory root-owned 0700 while walking every descendant, then hand it
	// to the runner last. The temporary root ownership prevents the Agent from
	// changing paths after validation and keeps the runner from observing a
	// partly-owned tree. A failure deliberately leaves a root-owned quarantine.
	top, err := workspaceRoot.Open(".")
	if err != nil {
		return runner.WorkspaceRef{}, err
	}
	defer top.Close()
	opened, err := top.Stat()
	if err != nil || !opened.IsDir() || !os.SameFile(before, opened) {
		return runner.WorkspaceRef{}, errors.New("workspace changed before ownership setup")
	}
	if err := top.Chown(0, 0); err != nil {
		return runner.WorkspaceRef{}, err
	}
	if err := top.Chmod(0o700); err != nil {
		return runner.WorkspaceRef{}, err
	}
	if err := workspace.prepareLockedTree(ctx, workspaceRoot, top, workspace.AgentUID, workspace.AgentGID); err != nil {
		return runner.WorkspaceRef{}, err
	}
	return workspace.Observe(ctx, root, name)
}

// PrepareOfficial discards any Agent-created candidate and reconstructs the
// workspace from a root-owned, independently verified copy of the pinned
// official archive. It never accepts a prior runner-owned tree as package
// authority; idempotent command replay is owned by the Agent journal above this
// boundary.
func (workspace *OSWorkspace) PrepareOfficial(
	ctx context.Context,
	root *os.Root,
	name string,
) (runner.WorkspaceRef, error) {
	if err := ctx.Err(); err != nil {
		return runner.WorkspaceRef{}, err
	}
	if workspace.CacheRoot == "" || root == nil || !validWorkspaceName(name) ||
		workspace.validateExecutionsRoot(ctx, root) != nil {
		return runner.WorkspaceRef{}, errors.New("official workspace authority is unavailable")
	}
	if err := workspace.onlyCandidate(root, name); err != nil {
		return runner.WorkspaceRef{}, err
	}
	archive, err := workspace.openPinnedOfficialArchive(ctx)
	if err != nil {
		return runner.WorkspaceRef{}, err
	}
	defer archive.Close()

	// Removing an attacker-controlled symlink or tree is safe through os.Root.
	// The replacement is created by root and held by descriptor throughout
	// verification, extraction, and ownership handoff.
	_ = root.RemoveAll(name)
	if err := root.Mkdir(name, 0o700); err != nil {
		return runner.WorkspaceRef{}, err
	}
	before, err := root.Lstat(name)
	if err != nil || !before.IsDir() || !rootOwned(before) || before.Mode().Perm() != 0o700 {
		return runner.WorkspaceRef{}, errors.New("root workspace creation failed")
	}
	workspaceRoot, err := root.OpenRoot(name)
	if err != nil {
		return runner.WorkspaceRef{}, err
	}
	defer workspaceRoot.Close()
	top, err := workspaceRoot.Open(".")
	if err != nil {
		return runner.WorkspaceRef{}, err
	}
	defer top.Close()
	opened, err := top.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return runner.WorkspaceRef{}, errors.New("workspace name changed during creation")
	}
	if err := workspace.materializeArchive(workspaceRoot, archive, workspace.Package); err != nil {
		return runner.WorkspaceRef{}, err
	}
	for _, directory := range []string{
		".sparerunner-home",
		".sparerunner-home/.config",
		".sparerunner-home/.cache",
		".sparerunner-home/.tmp",
	} {
		if err := workspaceRoot.MkdirAll(directory, 0o700); err != nil {
			return runner.WorkspaceRef{}, err
		}
	}
	pathInfo, err := root.Lstat(name)
	if err != nil || !os.SameFile(opened, pathInfo) {
		return runner.WorkspaceRef{}, errors.New("workspace name changed before ownership handoff")
	}
	if err := workspace.prepareLockedTree(ctx, workspaceRoot, top, 0, 0); err != nil {
		return runner.WorkspaceRef{}, err
	}
	return workspace.Observe(ctx, root, name)
}

func (workspace *OSWorkspace) onlyCandidate(root *os.Root, candidate string) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !validWorkspaceName(entry.Name()) || entry.Name() != candidate {
			return errors.New("runner slot has another or unsafe workspace")
		}
	}
	return nil
}

func (workspace *OSWorkspace) openOfficialArchive() (*os.File, error) {
	if err := workspace.validateCacheRoot(); err != nil {
		return nil, err
	}
	key, err := workspace.cacheKey(workspace.Package)
	if err != nil {
		return nil, err
	}
	rootInfo, err := os.Lstat(workspace.CacheRoot)
	if err != nil {
		return nil, err
	}
	cache, err := os.OpenRoot(workspace.CacheRoot)
	if err != nil {
		return nil, err
	}
	defer cache.Close()
	openedRoot, err := cache.Stat(".")
	if err != nil || !os.SameFile(rootInfo, openedRoot) {
		return nil, errors.New("cache root changed while opening")
	}
	name := path.Join("packages", key, "archive")
	linkInfo, err := cache.Lstat(name)
	if err != nil || !linkInfo.Mode().IsRegular() || !singleLink(linkInfo) {
		return nil, errors.New("official archive path is unsafe")
	}
	archive, err := cache.Open(name)
	if err != nil {
		return nil, err
	}
	opened, err := archive.Stat()
	if err != nil || !opened.Mode().IsRegular() || !singleLink(opened) ||
		!os.SameFile(linkInfo, opened) {
		_ = archive.Close()
		return nil, errors.New("official archive changed while opening")
	}
	return archive, nil
}

func (workspace *OSWorkspace) validateCacheRoot() error {
	cleaned := filepath.Clean(workspace.CacheRoot)
	for current := cleaned; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("cache root ancestor is unsafe")
		}
		if current == cleaned {
			if !ownedBy(info, workspace.AgentUID, workspace.AgentGID) || info.Mode().Perm() != 0o700 {
				return errors.New("cache root must be agent-owned 0700")
			}
		} else if !rootOwned(info) || info.Mode().Perm()&0o022 != 0 {
			return errors.New("cache root ancestor must be root-owned and immutable")
		}
		if current == "/" {
			break
		}
	}
	return nil
}

// PinLaunch opens the workspace and run.sh before launch and verifies their
// inode/owner identity against the durable WorkspaceRef. The caller must keep
// both descriptors open until FileRuntime has duplicated them into the child.
func (workspace *OSWorkspace) PinLaunch(
	ctx context.Context,
	root *os.Root,
	name string,
	expected runner.WorkspaceRef,
) (*pinnedWorkspace, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if root == nil || !validWorkspaceName(name) || workspace.validateExecutionsRoot(ctx, root) != nil {
		return nil, errors.New("workspace root is unsafe")
	}
	directoryRoot, err := root.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	directory, err := directoryRoot.Open(".")
	if err != nil {
		_ = directoryRoot.Close()
		return nil, err
	}
	defer directoryRoot.Close()
	info, err := directory.Stat()
	pathInfo, pathErr := root.Lstat(name)
	if err != nil || pathErr != nil || !os.SameFile(info, pathInfo) ||
		workspaceRef(info, workspace.RunnerUID, workspace.RunnerGID) != expected {
		_ = directory.Close()
		return nil, errors.New("workspace identity changed before launch")
	}
	executable, err := directoryRoot.Open("run.sh")
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	executableInfo, err := executable.Stat()
	if err != nil || !executableInfo.Mode().IsRegular() || !singleLink(executableInfo) ||
		!ownedBy(executableInfo, workspace.RunnerUID, workspace.RunnerGID) ||
		executableInfo.Mode().Perm()&0o100 == 0 {
		_ = executable.Close()
		_ = directory.Close()
		return nil, errors.New("runner executable is unsafe")
	}
	return &pinnedWorkspace{directory: directory, executable: executable}, nil
}

func (workspace *OSWorkspace) SlotBusy(ctx context.Context, root *os.Root, candidate string) (bool, error) {
	if err := ctx.Err(); err != nil || root == nil || !validWorkspaceName(candidate) ||
		workspace.validateExecutionsRoot(ctx, root) != nil {
		return false, errors.New("executions root is unsafe")
	}
	return workspace.singleWorkspace(root, candidate, workspace.RunnerUID, workspace.RunnerGID)
}

func (workspace *OSWorkspace) singleWorkspace(root *os.Root, candidate string, uid, gid int) (bool, error) {
	directory, err := root.Open(".")
	if err != nil {
		return false, err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return false, err
	}
	found := false
	for _, entry := range entries {
		if !validWorkspaceName(entry.Name()) {
			return false, errors.New("executions root has an unsafe entry")
		}
		if entry.Name() != candidate {
			return true, nil
		}
		if found {
			return false, errors.New("duplicate workspace entry")
		}
		found = true
		info, err := root.Lstat(candidate)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			!ownedBy(info, uid, gid) || info.Mode().Perm() != 0o700 {
			return false, errors.New("candidate workspace identity is unsafe")
		}
	}
	if !found {
		return false, errors.New("candidate workspace is absent")
	}
	return false, nil
}

func (workspace *OSWorkspace) prepareLockedTree(
	ctx context.Context,
	root *os.Root,
	directory *os.File,
	sourceUID, sourceGID int,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "." || entry.Name() == ".." || strings.ContainsRune(entry.Name(), '/') {
			return errors.New("workspace has unsafe directory entry")
		}
		if err := workspace.prepareTree(ctx, root, entry.Name(), sourceUID, sourceGID); err != nil {
			return err
		}
	}
	if err := directory.Chown(workspace.RunnerUID, workspace.RunnerGID); err != nil {
		return err
	}
	return directory.Chmod(0o700)
}

func (workspace *OSWorkspace) prepareTree(
	ctx context.Context,
	root *os.Root,
	name string,
	sourceUID, sourceGID int,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := root.Lstat(name)
	if err != nil || !ownedBy(info, sourceUID, sourceGID) {
		return errors.New("workspace descendant has unexpected ownership")
	}
	switch {
	case info.IsDir() && info.Mode()&os.ModeSymlink == 0:
		directory, err := root.Open(name)
		if err != nil {
			return err
		}
		defer directory.Close()
		opened, err := directory.Stat()
		if err != nil || !opened.IsDir() || !os.SameFile(info, opened) {
			return errors.New("workspace directory changed during ownership setup")
		}
		entries, err := directory.ReadDir(-1)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Name() == "." || entry.Name() == ".." || strings.ContainsRune(entry.Name(), '/') {
				return errors.New("workspace has unsafe directory entry")
			}
			if err := workspace.prepareTree(ctx, root, path.Join(name, entry.Name()), sourceUID, sourceGID); err != nil {
				return err
			}
		}
		// Chown by descriptor only after the complete subtree has been prepared.
		if err := directory.Chown(workspace.RunnerUID, workspace.RunnerGID); err != nil {
			return err
		}
		return directory.Chmod(0o700)

	case info.Mode().IsRegular():
		file, err := root.Open(name)
		if err != nil {
			return err
		}
		defer file.Close()
		opened, err := file.Stat()
		if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) ||
			!singleLink(info) || !singleLink(opened) {
			return errors.New("workspace file changed during ownership setup")
		}
		if err := file.Chown(workspace.RunnerUID, workspace.RunnerGID); err != nil {
			return err
		}
		// The runner must be able to update its package, while group/other modes
		// cannot leak its files. Preserve only the owner's execute bit.
		return file.Chmod((info.Mode().Perm() & 0o100) | 0o600)

	case info.Mode()&os.ModeSymlink != 0:
		target, err := root.Readlink(name)
		if err != nil || filepath.IsAbs(target) {
			return errors.New("workspace symlink is unsafe")
		}
		// Root.Stat resolves only within this package root and rejects escapes.
		if _, err := root.Stat(name); err != nil {
			return errors.New("workspace symlink is dangling or escapes package")
		}
		if !singleLink(info) {
			return errors.New("workspace symlink has an external hard link")
		}
		return root.Lchown(name, workspace.RunnerUID, workspace.RunnerGID)
	default:
		// Devices, sockets, FIFOs, and other special files would let extracted
		// content cross the runner's filesystem/process boundary.
		return errors.New("workspace contains unsupported special file")
	}
}

func (workspace *OSWorkspace) Observe(ctx context.Context, root *os.Root, name string) (runner.WorkspaceRef, error) {
	if err := ctx.Err(); err != nil {
		return runner.WorkspaceRef{}, err
	}
	if root == nil || !validWorkspaceName(name) || workspace.validateExecutionsRoot(ctx, root) != nil {
		return runner.WorkspaceRef{}, errors.New("missing workspace root")
	}
	info, err := root.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return runner.WorkspaceRef{}, errors.New("workspace identity is unsafe")
	}
	ref := workspaceRef(info, workspace.RunnerUID, workspace.RunnerGID)
	if ref == (runner.WorkspaceRef{}) {
		return runner.WorkspaceRef{}, errors.New("workspace owner changed")
	}
	return ref, nil
}

func workspaceRef(info fs.FileInfo, uid, gid int) runner.WorkspaceRef {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != uid || int(stat.Gid) != gid {
		return runner.WorkspaceRef{}
	}
	return runner.WorkspaceRef{
		Backend: WorkspaceBackend,
		OwnerID: fmt.Sprintf("dev:%x:ino:%x:uid:%d:gid:%d", uint64(stat.Dev), uint64(stat.Ino), stat.Uid, stat.Gid),
	}
}

func (workspace *OSWorkspace) Remove(ctx context.Context, root *os.Root, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if root == nil || !validWorkspaceName(name) || workspace.validateExecutionsRoot(ctx, root) != nil {
		return errors.New("missing workspace root")
	}
	info, err := root.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		!ownedBy(info, workspace.RunnerUID, workspace.RunnerGID) ||
		info.Mode().Perm() != 0o700 {
		return errors.New("workspace identity is unsafe")
	}
	opened, err := root.OpenRoot(name)
	if err != nil {
		return errors.New("workspace cannot be pinned")
	}
	openedInfo, statErr := opened.Stat(".")
	closeErr := opened.Close()
	if statErr != nil || closeErr != nil || !os.SameFile(info, openedInfo) {
		return errors.New("workspace changed before removal")
	}
	if err := root.RemoveAll(name); err != nil {
		return err
	}
	if _, err := root.Lstat(name); !errors.Is(err, fs.ErrNotExist) {
		return errors.New("workspace remains after removal")
	}
	return nil
}

// Absent is a read-only proof used after removal or a durable finalized fence.
// It never treats an unsafe replacement as absence.
func (workspace *OSWorkspace) Absent(ctx context.Context, root *os.Root, name string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if root == nil || !validWorkspaceName(name) || workspace.validateExecutionsRoot(ctx, root) != nil {
		return false, errors.New("missing workspace root")
	}
	if _, err := root.Lstat(name); errors.Is(err, fs.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return false, nil
}

func rootOwned(info fs.FileInfo) bool {
	return ownedBy(info, 0, 0)
}

func ownedBy(info fs.FileInfo, uid, gid int) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == uid && int(stat.Gid) == gid
}

func singleLink(info fs.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}

func validWorkspaceName(name string) bool {
	if len(name) != 64 || filepath.Base(name) != name || strings.ContainsRune(name, '/') {
		return false
	}
	for _, character := range name {
		if !(character >= '0' && character <= '9') &&
			!(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func (workspace *OSWorkspace) validateExecutionsRoot(ctx context.Context, root *os.Root) error {
	info, err := root.Stat(".")
	if err != nil || !info.IsDir() ||
		!ownedBy(info, workspace.AgentUID, workspace.AgentGID) ||
		info.Mode().Perm() != 0o711 {
		return errors.New("executions root is unsafe")
	}
	pathInfo, err := os.Lstat(root.Name())
	if err != nil || !os.SameFile(info, pathInfo) {
		return errors.New("executions root path changed")
	}
	if filepath.Base(filepath.Clean(root.Name())) != "executions" {
		return errors.New("unexpected executions root")
	}
	return workspace.ValidateRuntimeRoot(ctx, filepath.Dir(filepath.Clean(root.Name())))
}

var _ Workspace = (*OSWorkspace)(nil)
var _ WorkspaceSlotAdmission = (*OSWorkspace)(nil)
var _ workspacePackageAuthority = (*OSWorkspace)(nil)
