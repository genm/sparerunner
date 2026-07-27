//go:build windows

package app

// Windows has no portable directory fsync. Credential locators use
// FILE_FLAG_WRITE_THROUGH, SQLite owns its WAL durability, and controller
// directory publication uses MoveFileEx(MOVEFILE_WRITE_THROUGH).
func syncDirectory(string) error { return nil }
