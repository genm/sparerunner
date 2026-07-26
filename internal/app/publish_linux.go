//go:build linux

package app

import "golang.org/x/sys/unix"

func publishStateDirectory(staging, destination string) error {
	return unix.Renameat2(unix.AT_FDCWD, staging, unix.AT_FDCWD, destination, unix.RENAME_NOREPLACE)
}
