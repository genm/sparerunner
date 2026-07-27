//go:build windows

package app

import (
	"errors"
	"os"

	"github.com/genm/tewake/internal/winacl"
)

func privateStateDirectoryPlatform(path string, _ os.FileInfo) error {
	return winacl.ValidatePrivateDirectory(path)
}

func createPrivateStateDirectoryPlatform(path string) error {
	return winacl.CreatePrivateDirectory(path)
}

func createPrivateStateTempDirectory(parent string) (string, error) {
	for attempt := 0; attempt < 16; attempt++ {
		path, err := os.MkdirTemp(parent, ".tewake-controller-init-")
		if err != nil {
			return "", err
		}
		if err := os.Remove(path); err != nil {
			return "", err
		}
		if err := winacl.CreatePrivateDirectory(path); err == nil {
			return path, nil
		} else if !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	return "", os.ErrExist
}

func initializePrivateStateDirectoryPlatform(
	path string,
	_ os.FileInfo,
) error {
	// os.MkdirAll creates a new Windows directory with inherited ACLs. Secure
	// only the just-created empty, current-owner directory.
	return winacl.SecureEmptyPrivateDirectory(path)
}
