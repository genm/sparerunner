//go:build darwin

package main

import (
	"os"
	"syscall"
)

func validateMacOSLiveFileAuthority(path string, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != mode ||
		(info.Mode()&os.ModeSymlink) != 0 {
		return errMacOSEvidenceInvalid
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() ||
		(info.Mode().IsRegular() && stat.Nlink != 1) ||
		(!info.Mode().IsRegular() && !info.IsDir()) {
		return errMacOSEvidenceInvalid
	}
	return nil
}
