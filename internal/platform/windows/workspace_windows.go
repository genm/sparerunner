//go:build windows

package windows

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/genm/tewake/internal/runner"
	syswindows "golang.org/x/sys/windows"
)

// OSWorkspace records the Windows volume/file identity and protected DACL of
// each execution directory. Prepare is the only method allowed to change ACLs;
// Observe and Remove never repair an unexpected identity.
type OSWorkspace struct {
	runtimeRoot string
	serviceSID  string
	runnerSID   string
}

func NewOSWorkspace(
	runtimeRoot string,
	serviceSID string,
	runnerSID string,
) (*OSWorkspace, error) {
	if !filepath.IsAbs(runtimeRoot) ||
		filepath.Clean(runtimeRoot) != runtimeRoot ||
		serviceSID == "" ||
		runnerSID == "" ||
		!strings.EqualFold(serviceSID, "S-1-5-18") ||
		strings.EqualFold(serviceSID, runnerSID) {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if _, err := syswindows.StringToSid(serviceSID); err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	if _, err := syswindows.StringToSid(runnerSID); err != nil {
		return nil, runner.ErrStrongOwnershipUnavailable
	}
	return &OSWorkspace{
		runtimeRoot: runtimeRoot,
		serviceSID:  serviceSID,
		runnerSID:   runnerSID,
	}, nil
}

func (workspace *OSWorkspace) ValidateRuntimeRoot(
	ctx context.Context,
	root string,
) error {
	if workspace == nil || ctx == nil || ctx.Err() != nil ||
		root != workspace.runtimeRoot ||
		!noReparseComponents(root) {
		return runner.ErrStrongOwnershipUnavailable
	}
	allowed := workspace.allowedSIDs()
	if err := validateProtectedPath(
		root,
		workspace.serviceSID,
		allowed,
		workspace.runnerSID,
		true,
	); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func (workspace *OSWorkspace) Prepare(
	ctx context.Context,
	root *os.Root,
	name string,
) (runner.WorkspaceRef, error) {
	if workspace == nil || ctx == nil || ctx.Err() != nil ||
		root == nil || !validWorkspaceName(name) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	path := filepath.Join(root.Name(), name)
	if !pathWithinRoot(root.Name(), path) || !noReparseTree(path) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	if err := workspace.applyRunnerACL(path); err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return workspace.observePath(path)
}

func (workspace *OSWorkspace) Observe(
	ctx context.Context,
	root *os.Root,
	name string,
) (runner.WorkspaceRef, error) {
	if workspace == nil || ctx == nil || ctx.Err() != nil ||
		root == nil || !validWorkspaceName(name) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	path := filepath.Join(root.Name(), name)
	if !pathWithinRoot(root.Name(), path) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	return workspace.observePath(path)
}

func (workspace *OSWorkspace) Remove(
	ctx context.Context,
	root *os.Root,
	name string,
) error {
	if workspace == nil || ctx == nil || ctx.Err() != nil ||
		root == nil || !validWorkspaceName(name) {
		return runner.ErrCleanupFailed
	}
	path := filepath.Join(root.Name(), name)
	if !pathWithinRoot(root.Name(), path) || !noReparseTree(path) {
		return runner.ErrCleanupFailed
	}
	if _, err := workspace.observePath(path); err != nil {
		return runner.ErrCleanupFailed
	}
	if err := root.RemoveAll(name); err != nil {
		return runner.ErrCleanupFailed
	}
	if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		return runner.ErrCleanupFailed
	}
	return nil
}

func (workspace *OSWorkspace) observePath(path string) (runner.WorkspaceRef, error) {
	if !noReparseComponents(path) {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	pointer, err := syswindows.UTF16PtrFromString(path)
	if err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	handle, err := syswindows.CreateFile(
		pointer,
		syswindows.FILE_READ_ATTRIBUTES|syswindows.READ_CONTROL,
		syswindows.FILE_SHARE_READ|syswindows.FILE_SHARE_WRITE|syswindows.FILE_SHARE_DELETE,
		nil,
		syswindows.OPEN_EXISTING,
		syswindows.FILE_FLAG_BACKUP_SEMANTICS|syswindows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	defer syswindows.CloseHandle(handle)
	var info syswindows.ByHandleFileInformation
	if err := syswindows.GetFileInformationByHandle(handle, &info); err != nil ||
		info.FileAttributes&syswindows.FILE_ATTRIBUTE_DIRECTORY == 0 ||
		info.FileAttributes&syswindows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	if err := validateProtectedHandle(
		handle,
		workspace.runnerSID,
		workspace.allowedSIDs(),
		"",
		false,
	); err != nil {
		return runner.WorkspaceRef{}, runner.ErrStrongOwnershipUnavailable
	}
	identity := fmt.Sprintf(
		"%08x:%08x%08x:%s",
		info.VolumeSerialNumber,
		info.FileIndexHigh,
		info.FileIndexLow,
		strings.ToUpper(workspace.runnerSID),
	)
	digest := sha256.Sum256([]byte(identity))
	return runner.WorkspaceRef{
		Backend: WorkspaceBackend,
		OwnerID: hex.EncodeToString(digest[:]),
	}, nil
}

func (workspace *OSWorkspace) applyRunnerACL(root string) error {
	descriptor, err := syswindows.SecurityDescriptorFromString(fmt.Sprintf(
		"O:%sD:P(A;OICI;FA;;;%s)(A;OICI;FA;;;%s)(A;OICI;FA;;;BA)",
		workspace.runnerSID,
		workspace.serviceSID,
		workspace.runnerSID,
	))
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	var paths []string
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry == nil {
			return runner.ErrStrongOwnershipUnavailable
		}
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return runner.ErrStrongOwnershipUnavailable
		}
		pointer, err := syswindows.UTF16PtrFromString(path)
		if err != nil {
			return runner.ErrStrongOwnershipUnavailable
		}
		attributes, err := syswindows.GetFileAttributes(pointer)
		if err != nil || attributes&syswindows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
			return runner.ErrStrongOwnershipUnavailable
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	// Children are secured before their parent so a partial failure never opens
	// an inherited writable window at the execution root.
	for index := len(paths) - 1; index >= 0; index-- {
		if err := syswindows.SetNamedSecurityInfo(
			paths[index],
			syswindows.SE_FILE_OBJECT,
			syswindows.OWNER_SECURITY_INFORMATION|
				syswindows.DACL_SECURITY_INFORMATION|
				syswindows.PROTECTED_DACL_SECURITY_INFORMATION,
			owner,
			nil,
			dacl,
			nil,
		); err != nil {
			return runner.ErrStrongOwnershipUnavailable
		}
	}
	return nil
}

func (workspace *OSWorkspace) allowedSIDs() map[string]struct{} {
	return map[string]struct{}{
		strings.ToUpper(workspace.serviceSID): {},
		strings.ToUpper(workspace.runnerSID):  {},
		"S-1-5-18":                            {}, // LocalSystem
		"S-1-5-32-544":                        {}, // Builtin Administrators
	}
}

func validateProtectedPath(
	path string,
	expectedOwner string,
	allowed map[string]struct{},
	readOnlySID string,
	directory bool,
) error {
	pointer, err := syswindows.UTF16PtrFromString(path)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	attributes, err := syswindows.GetFileAttributes(pointer)
	if err != nil ||
		attributes&syswindows.FILE_ATTRIBUTE_REPARSE_POINT != 0 ||
		(directory && attributes&syswindows.FILE_ATTRIBUTE_DIRECTORY == 0) {
		return runner.ErrStrongOwnershipUnavailable
	}
	handle, err := syswindows.CreateFile(
		pointer,
		syswindows.FILE_READ_ATTRIBUTES|syswindows.READ_CONTROL,
		syswindows.FILE_SHARE_READ|syswindows.FILE_SHARE_WRITE|syswindows.FILE_SHARE_DELETE,
		nil,
		syswindows.OPEN_EXISTING,
		syswindows.FILE_FLAG_BACKUP_SEMANTICS|syswindows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	defer syswindows.CloseHandle(handle)
	return validateProtectedHandle(
		handle,
		expectedOwner,
		allowed,
		readOnlySID,
		directory,
	)
}

func validateProtectedHandle(
	handle syswindows.Handle,
	expectedOwner string,
	allowed map[string]struct{},
	readOnlySID string,
	_ bool,
) error {
	descriptor, err := syswindows.GetSecurityInfo(
		handle,
		syswindows.SE_FILE_OBJECT,
		syswindows.OWNER_SECURITY_INFORMATION|syswindows.DACL_SECURITY_INFORMATION,
	)
	if err != nil || descriptor == nil || !descriptor.IsValid() {
		return runner.ErrStrongOwnershipUnavailable
	}
	owner, defaulted, err := descriptor.Owner()
	if err != nil || owner == nil || defaulted ||
		!strings.EqualFold(owner.String(), expectedOwner) {
		return runner.ErrStrongOwnershipUnavailable
	}
	control, _, err := descriptor.Control()
	if err != nil || control&syswindows.SE_DACL_PROTECTED == 0 {
		return runner.ErrStrongOwnershipUnavailable
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil || dacl == nil || defaulted ||
		int(dacl.AceCount) != len(allowed) {
		return runner.ErrStrongOwnershipUnavailable
	}
	seenOwner := false
	seen := make(map[string]struct{}, len(allowed))
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *syswindows.ACCESS_ALLOWED_ACE
		if err := syswindows.GetAce(dacl, index, &ace); err != nil || ace == nil {
			return runner.ErrStrongOwnershipUnavailable
		}
		if ace.Header.AceFlags&syswindows.INHERITED_ACE != 0 {
			return runner.ErrStrongOwnershipUnavailable
		}
		if ace.Header.AceType != syswindows.ACCESS_ALLOWED_ACE_TYPE {
			return runner.ErrStrongOwnershipUnavailable
		}
		sid := (*syswindows.SID)(unsafe.Pointer(&ace.SidStart))
		if sid == nil || !sid.IsValid() {
			return runner.ErrStrongOwnershipUnavailable
		}
		sidText := strings.ToUpper(sid.String())
		if _, ok := allowed[sidText]; !ok {
			return runner.ErrStrongOwnershipUnavailable
		}
		if _, duplicate := seen[sidText]; duplicate {
			return runner.ErrStrongOwnershipUnavailable
		}
		seen[sidText] = struct{}{}
		if strings.EqualFold(sidText, expectedOwner) {
			seenOwner = true
		}
		if readOnlySID != "" && strings.EqualFold(sidText, readOnlySID) {
			if ace.Mask&runnerRootWriteMask != 0 ||
				ace.Mask&syswindows.FILE_GENERIC_READ != syswindows.FILE_GENERIC_READ ||
				ace.Mask&syswindows.FILE_GENERIC_EXECUTE != syswindows.FILE_GENERIC_EXECUTE {
				return runner.ErrStrongOwnershipUnavailable
			}
		} else if ace.Mask&windowsFileFullControl != windowsFileFullControl {
			return runner.ErrStrongOwnershipUnavailable
		}
	}
	if !seenOwner || len(seen) != len(allowed) {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

const windowsFileFullControl = 0x001f01ff

const runnerRootWriteMask = syswindows.FILE_WRITE_DATA |
	syswindows.FILE_APPEND_DATA |
	syswindows.FILE_WRITE_EA |
	syswindows.FILE_WRITE_ATTRIBUTES |
	fileDeleteChild |
	syswindows.DELETE |
	syswindows.WRITE_DAC |
	syswindows.WRITE_OWNER

const fileDeleteChild = 0x00000040

func noReparseComponents(path string) bool {
	clean := filepath.Clean(path)
	volume := filepath.VolumeName(clean)
	if volume == "" {
		return false
	}
	current := volume + string(filepath.Separator)
	relative := strings.TrimPrefix(clean, current)
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		pointer, err := syswindows.UTF16PtrFromString(current)
		if err != nil {
			return false
		}
		attributes, err := syswindows.GetFileAttributes(pointer)
		if err != nil || attributes&syswindows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
			return false
		}
	}
	return true
}

func noReparseTree(root string) bool {
	if !noReparseComponents(root) {
		return false
	}
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry == nil {
			return runner.ErrStrongOwnershipUnavailable
		}
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return runner.ErrStrongOwnershipUnavailable
		}
		pointer, err := syswindows.UTF16PtrFromString(path)
		if err != nil {
			return runner.ErrStrongOwnershipUnavailable
		}
		attributes, err := syswindows.GetFileAttributes(pointer)
		if err != nil || attributes&syswindows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
			return runner.ErrStrongOwnershipUnavailable
		}
		return nil
	}) == nil
}

func pathWithinRoot(root, path string) bool {
	relative, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	return err == nil && validWorkspaceName(relative)
}

var _ Workspace = (*OSWorkspace)(nil)
