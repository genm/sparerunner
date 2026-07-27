//go:build windows

package app

import (
	"os"

	"github.com/genm/tewake/internal/winacl"
)

func privateStateDirectoryPlatform(path string, _ os.FileInfo) error {
	return winacl.ValidatePrivateDirectory(path)
}

func initializePrivateStateDirectoryPlatform(
	path string,
	_ os.FileInfo,
) error {
	// os.MkdirAll creates a new Windows directory with inherited ACLs. Secure
	// only the just-created empty, current-owner directory.
	return winacl.SecureEmptyPrivateDirectory(path)
}
