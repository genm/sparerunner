//go:build windows

package runner

import (
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// requirePrivateCacheRoot enforces the installer-owned Windows ACL boundary.
// The runner identity is intentionally absent: workflows consume materialized
// copies and never gain authority over the shared verified archive cache.
func requirePrivateCacheRoot(root string) error {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return ErrStrongOwnershipUnavailable
	}
	if !privateWindowsCachePath(root) {
		return ErrStrongOwnershipUnavailable
	}
	pointer, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return ErrStrongOwnershipUnavailable
	}
	handle, err := windows.CreateFile(
		pointer,
		windows.FILE_READ_ATTRIBUTES|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return ErrStrongOwnershipUnavailable
	}
	defer windows.CloseHandle(handle)
	descriptor, err := windows.GetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil || descriptor == nil || !descriptor.IsValid() {
		return ErrStrongOwnershipUnavailable
	}
	current, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil || current == nil || current.User.Sid == nil {
		return ErrStrongOwnershipUnavailable
	}
	currentSID := strings.ToUpper(current.User.Sid.String())
	owner, defaulted, err := descriptor.Owner()
	if err != nil || owner == nil || defaulted ||
		!strings.EqualFold(owner.String(), currentSID) {
		return ErrStrongOwnershipUnavailable
	}
	control, _, err := descriptor.Control()
	if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
		return ErrStrongOwnershipUnavailable
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil || dacl == nil || defaulted {
		return ErrStrongOwnershipUnavailable
	}
	allowed := map[string]struct{}{
		currentSID:     {},
		"S-1-5-18":     {},
		"S-1-5-32-544": {},
	}
	if int(dacl.AceCount) != len(allowed) {
		return ErrStrongOwnershipUnavailable
	}
	seenCurrent := false
	seen := make(map[string]struct{}, len(allowed))
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, index, &ace); err != nil || ace == nil ||
			ace.Header.AceFlags&windows.INHERITED_ACE != 0 ||
			ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			return ErrStrongOwnershipUnavailable
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if sid == nil || !sid.IsValid() {
			return ErrStrongOwnershipUnavailable
		}
		text := strings.ToUpper(sid.String())
		if _, ok := allowed[text]; !ok {
			return ErrStrongOwnershipUnavailable
		}
		if _, duplicate := seen[text]; duplicate ||
			ace.Mask&windowsFileFullControl != windowsFileFullControl {
			return ErrStrongOwnershipUnavailable
		}
		seen[text] = struct{}{}
		if text == currentSID {
			seenCurrent = true
		}
	}
	if !seenCurrent || len(seen) != len(allowed) {
		return ErrStrongOwnershipUnavailable
	}
	return nil
}

const windowsFileFullControl = 0x001f01ff

func privateWindowsCachePath(path string) bool {
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
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return false
		}
		pointer, err := windows.UTF16PtrFromString(current)
		if err != nil {
			return false
		}
		attributes, err := windows.GetFileAttributes(pointer)
		if err != nil || attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
			return false
		}
	}
	return true
}
