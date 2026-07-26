//go:build darwin || linux

package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestProductionCacheRejectsWritableAndSymlinkedRootsBeforeDownload(t *testing.T) {
	pkg, err := OfficialPackage(CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	for name, root := range unsafeCacheRoots(t) {
		t.Run(name, func(t *testing.T) {
			fetcher := &bytesFetcher{}
			cache := Cache{Root: root, Fetcher: fetcher}
			if _, err := cache.Ensure(context.Background(), pkg); err != ErrPackageIntegrity {
				t.Fatalf("Ensure error = %v", err)
			}
			if fetcher.calls != 0 {
				t.Fatalf("unsafe cache attempted %d downloads", fetcher.calls)
			}
		})
	}
}

func unsafeCacheRoots(t *testing.T) map[string]string {
	t.Helper()
	exposedRoot := filepath.Join(t.TempDir(), "exposed")
	if err := os.Mkdir(exposedRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	writableParent := filepath.Join(t.TempDir(), "writable")
	if err := os.Mkdir(writableParent, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(writableParent, 0o777); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(writableParent, "cache")
	if err := os.Mkdir(child, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "cache-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"non-private root":  exposedRoot,
		"writable ancestor": writableParent + string(filepath.Separator) + "cache",
		"symlink root":      link,
	}
}

func TestPrivateCacheRootAcceptsOwned0700Directory(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := requirePrivateCacheRoot(root); err != nil {
		t.Fatalf("private cache root rejected: %v", err)
	}
}
