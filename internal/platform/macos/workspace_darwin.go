//go:build darwin

package macos

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/genm/tewake/internal/runner"
)

// OSWorkspace transfers one extracted runner tree from the launch daemon
// identity to a dedicated non-login runner identity. The top directory is
// handed to the runner last so neither identity observes a partly-owned tree.
type OSWorkspace struct {
	AgentUID  int
	AgentGID  int
	RunnerUID int
	RunnerGID int
}

func NewOSWorkspace(agentUID, agentGID, runnerUID, runnerGID int) *OSWorkspace {
	return &OSWorkspace{
		AgentUID: agentUID, AgentGID: agentGID,
		RunnerUID: runnerUID, RunnerGID: runnerGID,
	}
}

func (workspace *OSWorkspace) AgentIdentity() RunnerIdentity {
	return RunnerIdentity{UID: workspace.AgentUID, GID: workspace.AgentGID}
}

func (workspace *OSWorkspace) RunnerIdentity() RunnerIdentity {
	return RunnerIdentity{UID: workspace.RunnerUID, GID: workspace.RunnerGID}
}

func (workspace *OSWorkspace) ValidateRuntimeRoot(ctx context.Context, root string) error {
	if ctx == nil || ctx.Err() != nil ||
		workspace.AgentUID < 0 || workspace.AgentGID < 0 ||
		workspace.RunnerUID <= 0 || workspace.RunnerGID <= 0 ||
		!filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return runner.ErrStrongOwnershipUnavailable
	}
	for current := root; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			info.Mode().Perm()&0o022 != 0 {
			return runner.ErrStrongOwnershipUnavailable
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || (int(stat.Uid) != 0 && int(stat.Uid) != workspace.AgentUID) {
			return runner.ErrStrongOwnershipUnavailable
		}
		if current == "/" {
			break
		}
	}
	info, err := os.Lstat(root)
	if err != nil || !ownedBy(info, workspace.AgentUID, workspace.AgentGID) ||
		info.Mode().Perm() != 0o711 {
		return runner.ErrStrongOwnershipUnavailable
	}
	runtimeRoot, err := os.OpenRoot(root)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	defer runtimeRoot.Close()
	opened, err := runtimeRoot.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		return runner.ErrStrongOwnershipUnavailable
	}
	executions, err := runtimeRoot.Lstat("executions")
	if err != nil || !executions.IsDir() || executions.Mode()&os.ModeSymlink != 0 ||
		!ownedBy(executions, workspace.AgentUID, workspace.AgentGID) ||
		executions.Mode().Perm() != 0o711 {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (workspace *OSWorkspace) Prepare(
	ctx context.Context,
	root *os.Root,
	name string,
) (runner.WorkspaceRef, error) {
	if err := workspace.validateExecutionsRoot(ctx, root); err != nil ||
		!validWorkspaceName(name) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	before, err := root.Lstat(name)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 ||
		!ownedBy(before, workspace.AgentUID, workspace.AgentGID) ||
		before.Mode().Perm() != 0o700 {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	workspaceRoot, err := root.OpenRoot(name)
	if err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	defer workspaceRoot.Close()
	top, err := workspaceRoot.Open(".")
	if err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	defer top.Close()
	opened, err := top.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	for _, directory := range []string{
		".tewake-home",
		".tewake-home/.config",
		".tewake-home/.cache",
		".tewake-home/.tmp",
	} {
		if err := workspaceRoot.MkdirAll(directory, 0o700); err != nil {
			return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
		}
	}
	if err := workspace.prepareTree(ctx, workspaceRoot, top); err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return workspace.Observe(ctx, root, name)
}

func (workspace *OSWorkspace) prepareTree(
	ctx context.Context,
	root *os.Root,
	top *os.File,
) error {
	if ctx == nil || ctx.Err() != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	entries, err := top.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !safeEntryName(entry.Name()) {
			return errors.New("workspace contains an unsafe entry")
		}
		if err := workspace.prepareDescendant(ctx, root, entry.Name()); err != nil {
			return err
		}
	}
	if err := top.Chown(workspace.RunnerUID, workspace.RunnerGID); err != nil {
		return err
	}
	return top.Chmod(0o700)
}

func (workspace *OSWorkspace) prepareDescendant(
	ctx context.Context,
	root *os.Root,
	name string,
) error {
	if ctx == nil || ctx.Err() != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	info, err := root.Lstat(name)
	if err != nil || !ownedBy(info, workspace.AgentUID, workspace.AgentGID) {
		return errors.New("workspace descendant owner changed")
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
			return errors.New("workspace directory changed")
		}
		entries, err := directory.ReadDir(-1)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !safeEntryName(entry.Name()) {
				return errors.New("workspace contains an unsafe entry")
			}
			if err := workspace.prepareDescendant(
				ctx,
				root,
				path.Join(name, entry.Name()),
			); err != nil {
				return err
			}
		}
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
			!singleLink(opened) {
			return errors.New("workspace file changed")
		}
		if err := file.Chown(workspace.RunnerUID, workspace.RunnerGID); err != nil {
			return err
		}
		return file.Chmod((info.Mode().Perm() & 0o100) | 0o600)

	case info.Mode()&os.ModeSymlink != 0:
		target, err := root.Readlink(name)
		if err != nil || filepath.IsAbs(target) || !singleLink(info) {
			return errors.New("workspace symlink is unsafe")
		}
		if _, err := root.Stat(name); err != nil {
			return errors.New("workspace symlink escapes or is dangling")
		}
		return root.Lchown(name, workspace.RunnerUID, workspace.RunnerGID)

	default:
		return errors.New("workspace contains a special file")
	}
}

func (workspace *OSWorkspace) Observe(
	ctx context.Context,
	root *os.Root,
	name string,
) (runner.WorkspaceRef, error) {
	if err := workspace.validateExecutionsRoot(ctx, root); err != nil ||
		!validWorkspaceName(name) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	info, err := root.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o700 {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	ref := workspaceRef(info, workspace.RunnerUID, workspace.RunnerGID)
	if ref == (runner.WorkspaceRef{}) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return ref, nil
}

func (workspace *OSWorkspace) PinLaunch(
	ctx context.Context,
	directory string,
	expected runner.WorkspaceRef,
) (*PinnedWorkspace, error) {
	if ctx == nil || ctx.Err() != nil || !filepath.IsAbs(directory) ||
		filepath.Clean(directory) != directory ||
		expected.Backend != WorkspaceBackend || expected.OwnerID == "" {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	parent := filepath.Dir(directory)
	name := filepath.Base(directory)
	root, err := os.OpenRoot(parent)
	if err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	defer root.Close()
	directoryRoot, err := root.OpenRoot(name)
	if err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	directoryHandle, err := directoryRoot.Open(".")
	if err != nil {
		_ = directoryRoot.Close()
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	info, err := directoryHandle.Stat()
	pathInfo, pathErr := root.Lstat(name)
	if err != nil || pathErr != nil || !os.SameFile(info, pathInfo) ||
		workspaceRef(info, workspace.RunnerUID, workspace.RunnerGID) != expected {
		_ = directoryHandle.Close()
		_ = directoryRoot.Close()
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	executable, err := directoryRoot.Open("run.sh")
	if err != nil {
		_ = directoryHandle.Close()
		_ = directoryRoot.Close()
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	executableInfo, err := executable.Stat()
	if err != nil || !executableInfo.Mode().IsRegular() ||
		!ownedBy(executableInfo, workspace.RunnerUID, workspace.RunnerGID) ||
		!singleLink(executableInfo) || executableInfo.Mode().Perm()&0o100 == 0 {
		_ = executable.Close()
		_ = directoryHandle.Close()
		_ = directoryRoot.Close()
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return &PinnedWorkspace{
		root:       directoryRoot,
		directory:  directoryHandle,
		executable: executable,
	}, nil
}

type PinnedWorkspace struct {
	root       *os.Root
	directory  *os.File
	executable *os.File
}

func (workspace *PinnedWorkspace) Directory() *os.File {
	if workspace == nil {
		return nil
	}
	return workspace.directory
}

func (workspace *PinnedWorkspace) Executable() *os.File {
	if workspace == nil {
		return nil
	}
	return workspace.executable
}

func (workspace *PinnedWorkspace) Close() error {
	if workspace == nil {
		return nil
	}
	var result error
	for _, close := range []func() error{
		workspace.executable.Close,
		workspace.directory.Close,
		workspace.root.Close,
	} {
		if err := close(); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (workspace *OSWorkspace) Remove(
	ctx context.Context,
	root *os.Root,
	name string,
) error {
	if err := workspace.validateExecutionsRoot(ctx, root); err != nil ||
		!validWorkspaceName(name) {
		return runner.ErrCleanupFailed
	}
	info, err := root.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		workspaceRef(info, workspace.RunnerUID, workspace.RunnerGID) == (runner.WorkspaceRef{}) {
		return runner.ErrCleanupFailed
	}
	opened, err := root.OpenRoot(name)
	if err != nil {
		return runner.ErrCleanupFailed
	}
	openedInfo, statErr := opened.Stat(".")
	closeErr := opened.Close()
	if statErr != nil || closeErr != nil || !os.SameFile(info, openedInfo) {
		return runner.ErrCleanupFailed
	}
	if err := root.RemoveAll(name); err != nil {
		return runner.ErrCleanupFailed
	}
	absent, err := workspace.Absent(ctx, root, name)
	if err != nil || !absent {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (workspace *OSWorkspace) Absent(
	ctx context.Context,
	root *os.Root,
	name string,
) (bool, error) {
	if err := workspace.validateExecutionsRoot(ctx, root); err != nil ||
		!validWorkspaceName(name) {
		return false, runner.ErrCleanupFailed
	}
	if _, err := root.Lstat(name); errors.Is(err, fs.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return false, nil
}

func (workspace *OSWorkspace) validateExecutionsRoot(
	ctx context.Context,
	root *os.Root,
) error {
	if ctx == nil || ctx.Err() != nil || root == nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	info, err := root.Stat(".")
	if err != nil || !info.IsDir() ||
		!ownedBy(info, workspace.AgentUID, workspace.AgentGID) ||
		info.Mode().Perm() != 0o711 ||
		filepath.Base(filepath.Clean(root.Name())) != "executions" {
		return runner.ErrStrongOwnershipUnavailable
	}
	pathInfo, err := os.Lstat(root.Name())
	if err != nil || !os.SameFile(info, pathInfo) {
		return runner.ErrStrongOwnershipUnavailable
	}
	return workspace.ValidateRuntimeRoot(ctx, filepath.Dir(filepath.Clean(root.Name())))
}

func workspaceRef(info fs.FileInfo, uid, gid int) runner.WorkspaceRef {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != uid || int(stat.Gid) != gid {
		return runner.WorkspaceRef{}
	}
	return runner.WorkspaceRef{
		Backend: WorkspaceBackend,
		OwnerID: fmt.Sprintf(
			"dev:%x:ino:%x:uid:%d:gid:%d",
			uint64(stat.Dev),
			uint64(stat.Ino),
			stat.Uid,
			stat.Gid,
		),
	}
}

func ownedBy(info fs.FileInfo, uid, gid int) bool {
	if info == nil {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == uid && int(stat.Gid) == gid
}

func singleLink(info fs.FileInfo) bool {
	if info == nil {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}

func validWorkspaceName(name string) bool {
	if len(name) != 64 || filepath.Base(name) != name ||
		strings.ContainsRune(name, '/') {
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

func safeEntryName(name string) bool {
	return name != "" && name != "." && name != ".." &&
		!strings.ContainsRune(name, '/') && !strings.ContainsRune(name, '\x00')
}

var _ Workspace = (*OSWorkspace)(nil)
