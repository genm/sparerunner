//go:build windows

package store

// Go's os.File.Sync cannot portably open and flush a Windows directory handle.
// File contents are still synced before the atomic hard-link publication; the
// service installer and live recovery tests own the stronger Windows durability
// proof when the platform integration is implemented.
const directorySyncSupported = false

func syncDirectory(string) error { return nil }
