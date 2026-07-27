//go:build windows

package app

import syswindows "golang.org/x/sys/windows"

func publishStateDirectory(staging, destination string) error {
	source, err := syswindows.UTF16PtrFromString(staging)
	if err != nil {
		return err
	}
	target, err := syswindows.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}
	// No REPLACE_EXISTING flag: a concurrent initializer cannot be clobbered.
	// WRITE_THROUGH makes the directory publication the Windows durability
	// boundary after every contained file has synced itself.
	return syswindows.MoveFileEx(
		source,
		target,
		syswindows.MOVEFILE_WRITE_THROUGH,
	)
}
