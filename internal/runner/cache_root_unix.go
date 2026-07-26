//go:build darwin || linux

package runner

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func requirePrivateCacheRoot(root string) error {
	if !filepath.IsAbs(root) {
		return errors.New("runner cache root is unsafe")
	}
	cacheRoot := filepath.Clean(root)
	current := string(filepath.Separator)
	components := strings.Split(strings.TrimPrefix(cacheRoot, string(filepath.Separator)), string(filepath.Separator))
	if cacheRoot == string(filepath.Separator) {
		components = nil
	}
	for index := -1; index < len(components); index++ {
		if index >= 0 {
			current = filepath.Join(current, components[index])
		}
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
		isCacheRoot := current == cacheRoot
		if isCacheRoot && (stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm() != 0o700) {
			return errors.New("runner cache root is not private")
		}
		if info.Mode().Perm()&0o022 != 0 && !(stat.Uid == 0 && info.Mode()&os.ModeSticky != 0) {
			return errors.New("runner cache ancestor is writable by another user")
		}
	}
	return nil
}
