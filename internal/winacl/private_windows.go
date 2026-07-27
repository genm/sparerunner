//go:build windows

// Package winacl owns the exact Windows ACL and reparse-point contract used by
// Tewake credential locators. It intentionally has no dependency on higher
// application packages so both enrollment and state initialization share one
// authority.
package winacl

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	syswindows "golang.org/x/sys/windows"
)

const fileFullControl = 0x001f01ff

var ErrUnsafePrivatePath = errors.New("Windows private material path is unsafe")

func privatePathError(reason string) error {
	if os.Getenv("TEWAKE_WINDOWS_DEBUG") == "1" {
		return fmt.Errorf("%w: %s", ErrUnsafePrivatePath, reason)
	}
	return ErrUnsafePrivatePath
}

func ValidatePrivateDirectory(path string) error {
	return validatePrivatePath(path, true)
}

func ValidatePrivateFile(path string) error {
	return validatePrivatePath(path, false)
}

// SecureEmptyPrivateDirectory gives a just-created directory the exact private
// ACL. It never repairs a non-empty or foreign-owned directory.
func SecureEmptyPrivateDirectory(path string) error {
	if !NoReparseComponents(path) {
		return ErrUnsafePrivatePath
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrUnsafePrivatePath
	}
	if err := ValidatePrivateDirectory(path); err == nil {
		return nil
	}
	current, err := CurrentProcessSID()
	if err != nil {
		return ErrUnsafePrivatePath
	}
	owner, err := pathOwner(path, true)
	if err != nil || !strings.EqualFold(owner, current) {
		return ErrUnsafePrivatePath
	}
	entries, err := os.ReadDir(path)
	if err != nil || len(entries) != 0 {
		return ErrUnsafePrivatePath
	}
	if err := setPrivateACL(path, true, current); err != nil {
		return ErrUnsafePrivatePath
	}
	entries, err = os.ReadDir(path)
	if err != nil || len(entries) != 0 {
		return ErrUnsafePrivatePath
	}
	return ValidatePrivateDirectory(path)
}

// SecurePrivateFile is used only for a newly created staging file before any
// ciphertext is written.
func SecurePrivateFile(path string) error {
	if !NoReparseComponents(path) {
		return ErrUnsafePrivatePath
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return ErrUnsafePrivatePath
	}
	current, err := CurrentProcessSID()
	if err != nil {
		return ErrUnsafePrivatePath
	}
	owner, err := pathOwner(path, false)
	if err != nil || !strings.EqualFold(owner, current) {
		return ErrUnsafePrivatePath
	}
	if err := setPrivateACL(path, false, current); err != nil {
		return ErrUnsafePrivatePath
	}
	return ValidatePrivateFile(path)
}

func CurrentProcessSID() (string, error) {
	user, err := syswindows.GetCurrentProcessToken().GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil || !user.User.Sid.IsValid() {
		return "", ErrUnsafePrivatePath
	}
	return user.User.Sid.String(), nil
}

func NoReparseComponents(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	volume := filepath.VolumeName(path)
	if volume == "" {
		return false
	}
	current := volume + string(filepath.Separator)
	relative := strings.TrimPrefix(path, current)
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

func validatePrivatePath(path string, directory bool) error {
	if !NoReparseComponents(path) {
		return privatePathError("reparse path")
	}
	handle, err := openSecurityHandle(path, directory, syswindows.READ_CONTROL)
	if err != nil {
		return privatePathError("security handle")
	}
	defer syswindows.CloseHandle(handle)
	descriptor, err := syswindows.GetSecurityInfo(
		handle,
		syswindows.SE_FILE_OBJECT,
		syswindows.OWNER_SECURITY_INFORMATION|syswindows.DACL_SECURITY_INFORMATION,
	)
	if err != nil || descriptor == nil || !descriptor.IsValid() {
		return privatePathError("security descriptor")
	}
	current, err := CurrentProcessSID()
	if err != nil {
		return ErrUnsafePrivatePath
	}
	owner, defaulted, err := descriptor.Owner()
	if err != nil || owner == nil || defaulted ||
		!strings.EqualFold(owner.String(), current) {
		return privatePathError("owner")
	}
	control, _, err := descriptor.Control()
	if err != nil || control&syswindows.SE_DACL_PROTECTED == 0 {
		return privatePathError("dacl protection")
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil || dacl == nil || defaulted {
		return privatePathError("dacl")
	}
	required := privateSIDs(current)
	if int(dacl.AceCount) != len(required) {
		return privatePathError(fmt.Sprintf("ace count %d", dacl.AceCount))
	}
	seen := make(map[string]struct{}, len(required))
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *syswindows.ACCESS_ALLOWED_ACE
		if err := syswindows.GetAce(dacl, index, &ace); err != nil ||
			ace == nil ||
			ace.Header.AceType != syswindows.ACCESS_ALLOWED_ACE_TYPE ||
			ace.Header.AceFlags&syswindows.INHERITED_ACE != 0 ||
			ace.Mask != fileFullControl {
			return privatePathError(fmt.Sprintf("ace %d contract", index))
		}
		expectedFlags := byte(0)
		if directory {
			expectedFlags = syswindows.OBJECT_INHERIT_ACE | syswindows.CONTAINER_INHERIT_ACE
		}
		if ace.Header.AceFlags != expectedFlags {
			return ErrUnsafePrivatePath
		}
		sid := (*syswindows.SID)(unsafe.Pointer(&ace.SidStart))
		if sid == nil || !sid.IsValid() {
			return privatePathError(fmt.Sprintf("ace %d sid", index))
		}
		text := strings.ToUpper(sid.String())
		if _, found := required[text]; !found {
			return privatePathError(fmt.Sprintf("ace %d sid set", index))
		}
		if _, duplicate := seen[text]; duplicate {
			return privatePathError(fmt.Sprintf("ace %d duplicate", index))
		}
		seen[text] = struct{}{}
	}
	if len(seen) != len(required) {
		return privatePathError("sid set incomplete")
	}
	return nil
}

func pathOwner(path string, directory bool) (string, error) {
	handle, err := openSecurityHandle(path, directory, syswindows.READ_CONTROL)
	if err != nil {
		return "", err
	}
	defer syswindows.CloseHandle(handle)
	descriptor, err := syswindows.GetSecurityInfo(
		handle,
		syswindows.SE_FILE_OBJECT,
		syswindows.OWNER_SECURITY_INFORMATION,
	)
	if err != nil || descriptor == nil {
		return "", ErrUnsafePrivatePath
	}
	owner, defaulted, err := descriptor.Owner()
	if err != nil || owner == nil || defaulted {
		return "", ErrUnsafePrivatePath
	}
	return owner.String(), nil
}

func setPrivateACL(path string, directory bool, ownerText string) error {
	flags := ""
	if directory {
		flags = "OICI"
	}
	var builder strings.Builder
	builder.WriteString("O:")
	builder.WriteString(ownerText)
	builder.WriteString("D:P")
	for _, sid := range orderedPrivateSIDs(ownerText) {
		builder.WriteString("(A;")
		builder.WriteString(flags)
		builder.WriteString(";FA;;;")
		builder.WriteString(sid)
		builder.WriteByte(')')
	}
	descriptor, err := syswindows.SecurityDescriptorFromString(builder.String())
	if err != nil {
		return ErrUnsafePrivatePath
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil {
		return ErrUnsafePrivatePath
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil {
		return ErrUnsafePrivatePath
	}
	if err := syswindows.SetNamedSecurityInfo(
		path,
		syswindows.SE_FILE_OBJECT,
		syswindows.OWNER_SECURITY_INFORMATION|
			syswindows.DACL_SECURITY_INFORMATION|
			syswindows.PROTECTED_DACL_SECURITY_INFORMATION,
		owner,
		nil,
		dacl,
		nil,
	); err != nil {
		return ErrUnsafePrivatePath
	}
	return nil
}

func openSecurityHandle(
	path string,
	directory bool,
	access uint32,
) (syswindows.Handle, error) {
	pointer, err := syswindows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	flags := uint32(syswindows.FILE_FLAG_OPEN_REPARSE_POINT)
	if directory {
		flags |= syswindows.FILE_FLAG_BACKUP_SEMANTICS
	}
	handle, err := syswindows.CreateFile(
		pointer,
		access,
		syswindows.FILE_SHARE_READ|syswindows.FILE_SHARE_WRITE|syswindows.FILE_SHARE_DELETE,
		nil,
		syswindows.OPEN_EXISTING,
		flags,
		0,
	)
	if err != nil {
		return 0, err
	}
	var info syswindows.ByHandleFileInformation
	if err := syswindows.GetFileInformationByHandle(handle, &info); err != nil ||
		info.FileAttributes&syswindows.FILE_ATTRIBUTE_REPARSE_POINT != 0 ||
		directory != (info.FileAttributes&syswindows.FILE_ATTRIBUTE_DIRECTORY != 0) {
		syswindows.CloseHandle(handle)
		return 0, ErrUnsafePrivatePath
	}
	return handle, nil
}

func privateSIDs(current string) map[string]struct{} {
	result := make(map[string]struct{}, 3)
	for _, sid := range orderedPrivateSIDs(current) {
		result[strings.ToUpper(sid)] = struct{}{}
	}
	return result
}

func orderedPrivateSIDs(current string) []string {
	result := []string{current}
	for _, sid := range []string{"S-1-5-18", "S-1-5-32-544"} {
		found := false
		for _, existing := range result {
			if strings.EqualFold(existing, sid) {
				found = true
				break
			}
		}
		if !found {
			result = append(result, sid)
		}
	}
	return result
}
