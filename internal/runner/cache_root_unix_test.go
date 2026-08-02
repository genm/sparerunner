//go:build darwin || linux

package runner

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	exposedRoot := filepath.Join(canonicalTempDir(t), "exposed")
	if err := os.Mkdir(exposedRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	// Mkdir applies the process umask. Set the mode explicitly so this fixture
	// remains non-private under hardened developer and CI environments.
	if err := os.Chmod(exposedRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	writableParent := filepath.Join(canonicalTempDir(t), "writable")
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
	target := filepath.Join(canonicalTempDir(t), "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(canonicalTempDir(t), "cache-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	ancestorTarget := filepath.Join(canonicalTempDir(t), "ancestor-target")
	if err := os.Mkdir(ancestorTarget, 0o700); err != nil {
		t.Fatal(err)
	}
	ancestorBase := canonicalTempDir(t)
	ancestorLink := filepath.Join(ancestorBase, "ancestor-link")
	if err := os.Symlink(ancestorTarget, ancestorLink); err != nil {
		t.Fatal(err)
	}
	ancestorChild := filepath.Join(ancestorTarget, "cache")
	if err := os.Mkdir(ancestorChild, 0o700); err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"non-private root":  exposedRoot,
		"writable ancestor": writableParent + string(filepath.Separator) + "cache",
		"symlink root":      link,
		"symlink ancestor":  filepath.Join(ancestorLink, "cache"),
	}
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestPrivateCacheRootAcceptsOwned0700Directory(t *testing.T) {
	root := canonicalTempDir(t)
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := requirePrivateCacheRoot(root); err != nil {
		t.Fatalf("private cache root rejected: %v", err)
	}
}

func TestProductionCacheDoesNotCreateMissingRoot(t *testing.T) {
	parent := canonicalTempDir(t)
	root := filepath.Join(parent, "missing-cache")
	fetcher := &bytesFetcher{}
	pkg, err := OfficialPackage(CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (Cache{Root: root, Fetcher: fetcher}).Ensure(context.Background(), pkg); !errors.Is(err, ErrPackageIntegrity) {
		t.Fatalf("Ensure error = %v", err)
	}
	if fetcher.calls != 0 {
		t.Fatalf("missing root attempted %d downloads", fetcher.calls)
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cache root was created before validation: %v", err)
	}
}

func TestPreparedPackageUsesVerifiedOpenObjectAfterPathReplacement(t *testing.T) {
	const trusted = "#!/trusted\n"
	archive := tarArchive(
		t,
		[]tar.Header{{Name: "run.sh", Mode: 0o755, Size: int64(len(trusted)), Typeflag: tar.TypeReg}},
		[][]byte{[]byte(trusted)},
	)
	sum := sha256.Sum256(archive)
	pkg := Package{
		Version:  "test",
		Platform: Platform{"test", "test"},
		Asset:    "runner.tar.gz",
		Checksum: hex.EncodeToString(sum[:]),
		Size:     int64(len(archive)),
		Format:   ArchiveTarGz,
	}
	cacheRoot := canonicalTempDir(t)
	cache := Cache{
		Root:          cacheRoot,
		Fetcher:       &bytesFetcher{data: archive},
		verifyPackage: func(value Package) bool { return value == pkg },
	}
	prepared, err := cache.Ensure(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	root, err := os.OpenRoot(cacheRoot)
	if err != nil {
		t.Fatal(err)
	}
	entry := "packages/" + pkg.key()
	if err := thawCacheEntry(root, entry); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(cacheRoot, filepath.FromSlash(entry), "archive")
	heldPath := archivePath + ".held"
	if err := os.Rename(archivePath, heldPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archivePath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	destination, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Materialize(destination); err != nil {
		destination.Close()
		t.Fatal(err)
	}
	got, err := destination.ReadFile("run.sh")
	if closeErr := destination.Close(); err == nil {
		err = closeErr
	}
	if err != nil || string(got) != trusted {
		t.Fatalf("materialized runner = %q, %v", got, err)
	}
}
