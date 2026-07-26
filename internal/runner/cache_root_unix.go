//go:build darwin || linux

package runner

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func requirePrivateCacheRoot(root string) error {
	rootInfo, err := os.Lstat(root)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("runner cache root is unsafe")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return errors.New("runner cache root is unsafe")
	}
	current := filepath.Clean(resolved)
	cacheRoot := current
	for {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("runner cache ancestor is unsafe")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || (stat.Uid != 0 && stat.Uid != uint32(os.Geteuid())) {
			return errors.New("runner cache ancestor has an untrusted owner")
		}
		if current == cacheRoot && (stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm() != 0o700) {
			return errors.New("runner cache root is not private")
		}
		if info.Mode().Perm()&0o022 != 0 && !(stat.Uid == 0 && info.Mode()&os.ModeSticky != 0) {
			return errors.New("runner cache ancestor is writable by another user")
		}
		if current == "/" {
			return nil
		}
		current = filepath.Dir(current)
	}
}
