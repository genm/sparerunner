package runner

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Cache stores verified official artifacts separately from per-execution roots.
// Its content is never handed to the runner process, which prevents JIT-created
// credentials and runner self-mutation from contaminating the shared cache.
type Cache struct {
	Root    string
	Fetcher Fetcher

	// verifyPackage is an internal test seam. Production always uses Package.valid,
	// which limits the downloader to the six audited official assets.
	verifyPackage func(Package) bool
}

type cacheManifest struct {
	Version  string   `json:"version"`
	Platform Platform `json:"platform"`
	Asset    string   `json:"asset"`
	Checksum string   `json:"checksum"`
	Size     int64    `json:"size"`
	Format   string   `json:"format"`
}

// Ensure returns the immutable content directory. A complete cache entry becomes
// visible only through an atomic directory rename; concurrent creators either win
// that rename or discard their verified temporary copy and use the winner.
func (c Cache) Ensure(ctx context.Context, pkg Package) (ArchiveRef, error) {
	if c.Root == "" || c.Fetcher == nil || !c.validPackage(pkg) {
		return ArchiveRef{}, ErrInvalidRequest
	}
	if err := os.MkdirAll(c.Root, 0o700); err != nil {
		return ArchiveRef{}, ErrPackageIntegrity
	}
	root, err := os.OpenRoot(c.Root)
	if err != nil {
		return ArchiveRef{}, ErrPackageIntegrity
	}
	defer root.Close()
	if err := root.MkdirAll("packages", 0o700); err != nil {
		return ArchiveRef{}, ErrPackageIntegrity
	}
	entry := path.Join("packages", pkg.key())
	if validCacheEntry(root, entry, pkg) {
		return ArchiveRef{Directory: filepath.Join(c.Root, entry), Archive: "archive"}, nil
	}

	temporary, err := cacheTemporaryName(root)
	if err != nil {
		return ArchiveRef{}, ErrPackageIntegrity
	}
	defer func() {
		_ = thawCacheEntry(root, temporary)
		_ = root.RemoveAll(temporary)
	}()
	if err := root.Mkdir(temporary, 0o700); err != nil {
		return ArchiveRef{}, ErrPackageIntegrity
	}
	tempRoot, err := root.OpenRoot(temporary)
	if err != nil {
		return ArchiveRef{}, ErrPackageIntegrity
	}
	defer tempRoot.Close()
	if err := c.downloadAndExtract(ctx, tempRoot, pkg); err != nil {
		return ArchiveRef{}, err
	}
	manifest, err := json.Marshal(cacheManifest{pkg.Version, pkg.Platform, pkg.Asset, pkg.Checksum, pkg.Size, string(pkg.Format)})
	if err != nil || tempRoot.WriteFile("manifest.json", manifest, 0o600) != nil {
		return ArchiveRef{}, ErrPackageIntegrity
	}
	if err := freezeCacheEntry(tempRoot); err != nil {
		return ArchiveRef{}, ErrPackageIntegrity
	}
	// Renaming a populated directory onto another populated directory fails on
	// supported hosts. That gives one complete winner without a stale lock file.
	if err := root.Rename(temporary, entry); err != nil {
		if !validCacheEntry(root, entry, pkg) {
			return ArchiveRef{}, ErrPackageIntegrity
		}
	}
	return ArchiveRef{Directory: filepath.Join(c.Root, entry), Archive: "archive"}, nil
}

// rebuildCachedContent treats the pinned archive, not the mutable extracted
// tree or manifest, as the trust anchor before any runner receives its files.
func rebuildCachedContent(root *os.Root, entry string, pkg Package) error {
	if err := thawCacheEntry(root, entry); err != nil {
		return ErrPackageIntegrity
	}
	entryRoot, err := root.OpenRoot(entry)
	if err != nil {
		return ErrPackageIntegrity
	}
	defer entryRoot.Close()
	if err := entryRoot.RemoveAll("content"); err != nil {
		return ErrPackageIntegrity
	}
	if err := entryRoot.Mkdir("content", 0o700); err != nil {
		return ErrPackageIntegrity
	}
	content, err := entryRoot.OpenRoot("content")
	if err != nil {
		return ErrPackageIntegrity
	}
	defer content.Close()
	archive, err := entryRoot.Open("archive")
	if err != nil {
		return ErrPackageIntegrity
	}
	defer archive.Close()
	if err := extractArchive(content, archive, pkg.Format); err != nil {
		return err
	}
	if err := freezeCacheEntry(entryRoot); err != nil {
		return ErrPackageIntegrity
	}
	return nil
}

func freezeCacheEntry(root *os.Root) error {
	return fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || name == "." {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.IsDir() {
			return root.Chmod(name, 0o555)
		}
		mode := os.FileMode(0o444)
		if info.Mode().Perm()&0o111 != 0 {
			mode = 0o555
		}
		return root.Chmod(name, mode)
	})
}

func thawCacheEntry(root *os.Root, name string) error {
	entry, err := root.OpenRoot(name)
	if err != nil {
		return err
	}
	defer entry.Close()
	return fs.WalkDir(entry.FS(), ".", func(member string, directory fs.DirEntry, walkErr error) error {
		if walkErr != nil || !directory.IsDir() {
			return walkErr
		}
		return entry.Chmod(member, 0o700)
	})
}

func (c Cache) validPackage(pkg Package) bool {
	if c.verifyPackage != nil {
		return c.verifyPackage(pkg)
	}
	return pkg.valid()
}

func (c Cache) downloadAndExtract(ctx context.Context, root *os.Root, pkg Package) error {
	body, err := c.Fetcher.Fetch(ctx, pkg)
	if err != nil {
		return ErrDownloadPolicy
	}
	defer body.Close()
	archive, err := root.OpenFile("archive", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return ErrPackageIntegrity
	}
	hash := newChecksumWriter(archive)
	// The release API's immutable asset size is pinned with the SHA-256. Read at
	// most size+1 bytes, then require the exact size: this is a release-derived
	// disk boundary, not an arbitrary product quota.
	bytesCopied, copyErr := io.Copy(hash, io.LimitReader(body, pkg.Size+1))
	closeErr := archive.Close()
	if copyErr != nil || closeErr != nil || bytesCopied != pkg.Size || !hash.matches(pkg.Checksum) {
		return ErrPackageIntegrity
	}
	return nil
}

func validCacheEntry(root *os.Root, entry string, pkg Package) bool {
	entryRoot, err := root.OpenRoot(entry)
	if err != nil {
		return false
	}
	defer entryRoot.Close()
	data, err := entryRoot.ReadFile("manifest.json")
	if err != nil {
		return false
	}
	var manifest cacheManifest
	if json.Unmarshal(data, &manifest) != nil || manifest != (cacheManifest{pkg.Version, pkg.Platform, pkg.Asset, pkg.Checksum, pkg.Size, string(pkg.Format)}) {
		return false
	}
	info, err := entryRoot.Stat("archive")
	if err != nil || !info.Mode().IsRegular() || info.Size() != pkg.Size {
		return false
	}
	archive, err := entryRoot.Open("archive")
	if err != nil {
		return false
	}
	defer archive.Close()
	hash := newChecksumWriter(io.Discard)
	bytesCopied, err := io.Copy(hash, io.LimitReader(archive, pkg.Size+1))
	if err != nil || bytesCopied != pkg.Size || !hash.matches(pkg.Checksum) {
		return false
	}
	return true
}

func cacheTemporaryName(root *os.Root) (string, error) {
	for range 8 {
		var bytes [16]byte
		if _, err := rand.Read(bytes[:]); err != nil {
			return "", err
		}
		name := "packages/.building-" + fmt.Sprintf("%x", bytes)
		if _, err := root.Lstat(name); errors.Is(err, fs.ErrNotExist) {
			return name, nil
		}
	}
	return "", errors.New("temporary cache name collision")
}

func safeArchiveName(name string) (string, error) {
	name = strings.TrimPrefix(name, "./")
	if name == "" || strings.Contains(name, "\\") || path.IsAbs(name) {
		return "", ErrUnsafeArchive
	}
	for _, component := range strings.Split(name, "/") {
		if component == ".." {
			return "", ErrUnsafeArchive
		}
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", ErrUnsafeArchive
	}
	return clean, nil
}

func safeLinkTarget(member, target string) error {
	if target == "" || strings.Contains(target, "\\") || path.IsAbs(target) {
		return ErrUnsafeArchive
	}
	resolved := path.Clean(path.Join(path.Dir(member), target))
	if resolved == ".." || strings.HasPrefix(resolved, "../") || path.IsAbs(resolved) {
		return ErrUnsafeArchive
	}
	return nil
}

func makeParent(root *os.Root, name string) error {
	parent := path.Dir(name)
	if parent == "." {
		return nil
	}
	return root.MkdirAll(parent, 0o700)
}

// copyTree copies rather than links cache content. Linux v2.336.0 was inspected
// on 2026-07-26: it contains relative node/npm symlinks and no hard links. The
// copier preserves only relative links that remain under the destination root.
func copyTree(sourcePath string, destination *os.Root) error {
	source, err := os.OpenRoot(sourcePath)
	if err != nil {
		return ErrPackageIntegrity
	}
	defer source.Close()
	return fs.WalkDir(source.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return ErrPackageIntegrity
		}
		if name == "." {
			return nil
		}
		if _, err := safeArchiveName(name); err != nil {
			return err
		}
		if entry.IsDir() {
			if err := destination.MkdirAll(name, 0o700); err != nil {
				return ErrPackageIntegrity
			}
			return nil
		}
		mode := entry.Type()
		if mode&os.ModeSymlink != 0 {
			target, err := source.Readlink(name)
			if err != nil || safeLinkTarget(name, target) != nil || makeParent(destination, name) != nil {
				return ErrUnsafeArchive
			}
			if err := destination.Symlink(target, name); err != nil {
				return ErrPackageIntegrity
			}
			return nil
		}
		if !mode.IsRegular() {
			return ErrUnsafeArchive
		}
		info, err := entry.Info()
		if err != nil {
			return ErrPackageIntegrity
		}
		return copyRegular(source, destination, name, info.Mode().Perm())
	})
}

func copyRegular(source, destination *os.Root, name string, mode fs.FileMode) error {
	if err := makeParent(destination, name); err != nil {
		return ErrPackageIntegrity
	}
	input, err := source.Open(name)
	if err != nil {
		return ErrPackageIntegrity
	}
	defer input.Close()
	output, err := destination.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode.Perm())
	if err != nil {
		return ErrPackageIntegrity
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		return ErrPackageIntegrity
	}
	return nil
}
