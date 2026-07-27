//go:build !windows

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

func validateTrustedPathChain(path string, leafMustBeDirectory bool) error {
	return validateTrustedPathChainOwners(
		path,
		leafMustBeDirectory,
		map[uint32]struct{}{0: {}, uint32(os.Geteuid()): {}},
	)
}

func validateTrustedPathChainForUID(path string, leafMustBeDirectory bool, uid uint32) error {
	return validateTrustedPathChainOwners(
		path,
		leafMustBeDirectory,
		map[uint32]struct{}{0: {}, uid: {}},
	)
}

func validateTrustedPathChainOwners(
	path string,
	leafMustBeDirectory bool,
	owners map[uint32]struct{},
) error {
	if !canonicalAbsolutePath(path) {
		return fmt.Errorf("%w: non-canonical trusted path", errEvidenceInvalid)
	}
	if leaf, err := os.Lstat(path); err != nil || leaf.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: unavailable or symlink leaf", errEvidenceInvalid)
	}
	if runtime.GOOS != "linux" {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return fmt.Errorf("%w: unresolved trusted path", errEvidenceInvalid)
		}
		path = resolved
	}
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: unavailable or symlink path %s", errEvidenceInvalid, current)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if _, allowed := owners[stat.Uid]; !ok || !allowed {
			return fmt.Errorf("%w: untrusted owner path %s uid %d", errEvidenceInvalid, current, stat.Uid)
		}
		if info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("%w: mutable parent path %s", errEvidenceInvalid, current)
		}
		if current == filepath.Clean(path) {
			if leafMustBeDirectory != info.IsDir() {
				return fmt.Errorf("%w: unexpected path type %s", errEvidenceInvalid, current)
			}
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	return nil
}

func openTrustedRegular(path string, requirePrivate bool) (*os.File, fs.FileInfo, error) {
	if err := validateTrustedPathChain(path, false); err != nil {
		return nil, nil, err
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, nil, errEvidenceInvalid
	}
	if requirePrivate && before.Mode().Perm() != 0o600 {
		return nil, nil, errEvidenceInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, errEvidenceInvalid
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		_ = file.Close()
		return nil, nil, errEvidenceInvalid
	}
	return file, after, nil
}

func trustedDirectory(path string, mode os.FileMode) error {
	if err := validateTrustedPathChain(path, true); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != mode {
		return errEvidenceInvalid
	}
	return nil
}

func (liveAuthorityProbe) trustedRootFile(path string) error {
	if err := validateTrustedPathChainForUID(path, false, 0); err != nil {
		return errEvidenceInvalid
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 {
		return errEvidenceInvalid
	}
	return nil
}
