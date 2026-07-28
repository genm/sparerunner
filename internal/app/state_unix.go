//go:build !windows

package app

import (
	"errors"
	"os"
	"syscall"
)

func privateStateDirectoryPlatform(_ string, info os.FileInfo) error {
	if info.Mode().Perm() != 0o700 {
		return errors.New("state directory is not private")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("state directory is not owned by the service user")
	}
	return nil
}

func initializePrivateStateDirectoryPlatform(
	path string,
	info os.FileInfo,
) error {
	return privateStateDirectoryPlatform(path, info)
}

func createPrivateStateDirectoryPlatform(path string) error {
	return os.MkdirAll(path, 0o700)
}

func createPrivateStateTempDirectory(parent string) (string, error) {
	return os.MkdirTemp(parent, ".sparerunner-controller-init-")
}
