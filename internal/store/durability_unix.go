//go:build !windows

package store

import "os"

const directorySyncSupported = true

// syncDirectory makes the preceding link or unlink durable on filesystems that
// expose directory fsync through the portable os.File surface.
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
