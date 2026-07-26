//go:build windows

package app

import "os"

// DPAPI plus installer-owned ACL validation replaces Unix mode checks in
// twk-009. Until then the default credential persistence fails before this
// directory can hold private material.
func privateStateDirectoryPlatform(os.FileInfo) error { return nil }
