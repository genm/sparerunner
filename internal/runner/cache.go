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
	"sync"
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

type cachedPackage struct {
	mu       sync.Mutex
	archive  *os.File
	format   ArchiveFormat
	consumed bool
}

func (prepared *cachedPackage) Materialize(destination *os.Root) error {
	if destination == nil {
		return ErrInvalidRequest
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.archive == nil || prepared.consumed {
		return ErrPackageIntegrity
	}
	prepared.consumed = true
	return extractArchive(destination, prepared.archive, prepared.format)
}

func (prepared *cachedPackage) Close() error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.archive == nil {
		return nil
	}
	err := prepared.archive.Close()
	prepared.archive = nil
	return err
}

// MaterializeOfficialArchive copies an untrusted cache archive into a
// destination-owned file, verifies the exact pinned size and SHA-256 there, and
// extracts only from that immutable-by-caller copy. This is the privileged
// Supervisor boundary: the Agent may own and replace its cache entry, but it
// cannot change the bytes after the root-owned copy has been verified.
func MaterializeOfficialArchive(destination *os.Root, source *os.File, pkg Package) error {
	if destination == nil || source == nil || !pkg.valid() {
		return ErrInvalidRequest
	}
	const archiveName = ".tewake-official-runner-archive"
	archive, err := destination.OpenFile(archiveName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o400)
	if err != nil {
		return ErrPackageIntegrity
	}
	cleaned := false
	defer func() {
		if !cleaned {
			_ = archive.Close()
			_ = destination.Remove(archiveName)
		}
	}()
	if err := CopyAndVerifyOfficialArchive(archive, source, pkg); err != nil {
		return err
	}
	if err := MaterializePinnedOfficialArchive(destination, archive, pkg); err != nil {
		return err
	}
	if err := archive.Close(); err != nil {
		return ErrPackageIntegrity
	}
	if err := destination.Remove(archiveName); err != nil {
		return ErrPackageIntegrity
	}
	cleaned = true
	return nil
}

// CopyAndVerifyOfficialArchive creates the root-side immutable authority from an
// untrusted Agent cache descriptor. Callers must publish the destination only
// after this exact-size and SHA-256 verification succeeds.
func CopyAndVerifyOfficialArchive(destination, source *os.File, pkg Package) error {
	if destination == nil || source == nil || !pkg.valid() {
		return ErrInvalidRequest
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return ErrPackageIntegrity
	}
	if _, err := destination.Seek(0, io.SeekStart); err != nil {
		return ErrPackageIntegrity
	}
	hash := newChecksumWriter(destination)
	bytesCopied, copyErr := io.Copy(hash, io.LimitReader(source, pkg.Size+1))
	if copyErr != nil || bytesCopied != pkg.Size || !hash.matches(pkg.Checksum) {
		return ErrPackageIntegrity
	}
	if err := destination.Sync(); err != nil {
		return ErrPackageIntegrity
	}
	return nil
}

// VerifyOfficialArchive performs the one full verification required when a
// Supervisor first adopts an authority published by an earlier process.
func VerifyOfficialArchive(archive *os.File, pkg Package) error {
	if archive == nil || !pkg.valid() {
		return ErrInvalidRequest
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return ErrPackageIntegrity
	}
	hash := newChecksumWriter(io.Discard)
	bytesRead, readErr := io.Copy(hash, io.LimitReader(archive, pkg.Size+1))
	if readErr != nil || bytesRead != pkg.Size || !hash.matches(pkg.Checksum) {
		return ErrPackageIntegrity
	}
	return nil
}

// MaterializePinnedOfficialArchive extracts a previously verified immutable
// root-side authority. It deliberately avoids hashing a large archive for every
// execution; the privileged caller must keep and revalidate the authority's
// descriptor identity before calling this function.
func MaterializePinnedOfficialArchive(destination *os.Root, archive *os.File, pkg Package) error {
	if destination == nil || archive == nil || !pkg.valid() {
		return ErrInvalidRequest
	}
	info, err := archive.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != pkg.Size {
		return ErrPackageIntegrity
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return ErrPackageIntegrity
	}
	return extractArchive(destination, archive, pkg.Format)
}

var cacheGates = struct {
	sync.Mutex
	entries map[string]*cacheGate
}{entries: make(map[string]*cacheGate)}

type cacheGate struct {
	token chan struct{}
	refs  int
}

// ValidateCacheRoot verifies the same private ownership boundary used by
// Cache.Ensure without downloading a runner package. Service startup uses it to
// fail closed before the node can advertise native capacity.
func ValidateCacheRoot(root string) error {
	return requirePrivateCacheRoot(root)
}

// Ensure returns a single-use capability backed by the exact archive file
// descriptor whose size and SHA-256 were verified. A complete cache entry becomes
// visible only through an atomic directory rename; concurrent creators either win
// that rename or discard their temporary copy and open the verified winner.
func (c Cache) Ensure(ctx context.Context, pkg Package) (PreparedPackage, error) {
	if c.Root == "" || c.Fetcher == nil || !c.validPackage(pkg) {
		return nil, ErrInvalidRequest
	}
	if !filepath.IsAbs(c.Root) {
		return nil, ErrPackageIntegrity
	}
	c.Root = filepath.Clean(c.Root)
	if c.verifyPackage == nil {
		if err := requirePrivateCacheRoot(c.Root); err != nil {
			return nil, ErrPackageIntegrity
		}
	}
	rootInfo, err := os.Lstat(c.Root)
	if err != nil {
		return nil, ErrPackageIntegrity
	}
	root, err := os.OpenRoot(c.Root)
	if err != nil {
		return nil, ErrPackageIntegrity
	}
	defer root.Close()
	openedInfo, err := root.Stat(".")
	if err != nil || !os.SameFile(rootInfo, openedInfo) {
		return nil, ErrPackageIntegrity
	}
	if err := root.MkdirAll("packages", 0o700); err != nil {
		return nil, ErrPackageIntegrity
	}
	entry := path.Join("packages", pkg.key())
	if archive, valid := openValidCacheEntry(root, entry, pkg); valid {
		return &cachedPackage{archive: archive, format: pkg.Format}, nil
	}
	release, err := acquireCacheGate(ctx, filepath.Join(c.Root, entry))
	if err != nil {
		return nil, err
	}
	defer release()
	// Another caller in this process may have completed the immutable entry
	// while this caller waited. Separate agent processes still converge through
	// the atomic no-replacement rename below.
	if archive, valid := openValidCacheEntry(root, entry, pkg); valid {
		return &cachedPackage{archive: archive, format: pkg.Format}, nil
	}

	temporary, err := cacheTemporaryName(root)
	if err != nil {
		return nil, ErrPackageIntegrity
	}
	defer func() {
		_ = thawCacheEntry(root, temporary)
		_ = root.RemoveAll(temporary)
	}()
	if err := root.Mkdir(temporary, 0o700); err != nil {
		return nil, ErrPackageIntegrity
	}
	tempRoot, err := root.OpenRoot(temporary)
	if err != nil {
		return nil, ErrPackageIntegrity
	}
	defer tempRoot.Close()
	if err := c.downloadAndExtract(ctx, tempRoot, pkg); err != nil {
		return nil, err
	}
	manifest, err := json.Marshal(cacheManifest{pkg.Version, pkg.Platform, pkg.Asset, pkg.Checksum, pkg.Size, string(pkg.Format)})
	if err != nil || tempRoot.WriteFile("manifest.json", manifest, 0o600) != nil {
		return nil, ErrPackageIntegrity
	}
	if err := freezeCacheEntry(tempRoot); err != nil {
		return nil, ErrPackageIntegrity
	}
	// Renaming a populated directory onto another populated directory fails on
	// supported hosts. That gives one complete winner without a stale lock file.
	if err := root.Rename(temporary, entry); err != nil {
		if archive, valid := openValidCacheEntry(root, entry, pkg); valid {
			return &cachedPackage{archive: archive, format: pkg.Format}, nil
		}
		return nil, ErrPackageIntegrity
	}
	archive, valid := openValidCacheEntry(root, entry, pkg)
	if !valid {
		return nil, ErrPackageIntegrity
	}
	return &cachedPackage{archive: archive, format: pkg.Format}, nil
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

func acquireCacheGate(ctx context.Context, key string) (func(), error) {
	cacheGates.Lock()
	gate := cacheGates.entries[key]
	if gate == nil {
		gate = &cacheGate{token: make(chan struct{}, 1)}
		cacheGates.entries[key] = gate
	}
	gate.refs++
	cacheGates.Unlock()

	select {
	case gate.token <- struct{}{}:
		return func() {
			<-gate.token
			cacheGates.Lock()
			gate.refs--
			if gate.refs == 0 {
				delete(cacheGates.entries, key)
			}
			cacheGates.Unlock()
		}, nil
	case <-ctx.Done():
		cacheGates.Lock()
		gate.refs--
		if gate.refs == 0 {
			delete(cacheGates.entries, key)
		}
		cacheGates.Unlock()
		return nil, ctx.Err()
	}
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

func openValidCacheEntry(root *os.Root, entry string, pkg Package) (*os.File, bool) {
	entryRoot, err := root.OpenRoot(entry)
	if err != nil {
		return nil, false
	}
	defer entryRoot.Close()
	data, err := entryRoot.ReadFile("manifest.json")
	if err != nil {
		return nil, false
	}
	var manifest cacheManifest
	if json.Unmarshal(data, &manifest) != nil || manifest != (cacheManifest{pkg.Version, pkg.Platform, pkg.Asset, pkg.Checksum, pkg.Size, string(pkg.Format)}) {
		return nil, false
	}
	linkInfo, err := entryRoot.Lstat("archive")
	if err != nil || !linkInfo.Mode().IsRegular() || linkInfo.Size() != pkg.Size {
		return nil, false
	}
	archive, err := entryRoot.Open("archive")
	if err != nil {
		return nil, false
	}
	valid := false
	defer func() {
		if !valid {
			_ = archive.Close()
		}
	}()
	openedInfo, err := archive.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() || openedInfo.Size() != pkg.Size || !os.SameFile(linkInfo, openedInfo) {
		return nil, false
	}
	hash := newChecksumWriter(io.Discard)
	bytesCopied, err := io.Copy(hash, io.LimitReader(archive, pkg.Size+1))
	if err != nil || bytesCopied != pkg.Size || !hash.matches(pkg.Checksum) {
		return nil, false
	}
	afterInfo, err := archive.Stat()
	if err != nil || !afterInfo.Mode().IsRegular() || afterInfo.Size() != pkg.Size || !os.SameFile(openedInfo, afterInfo) {
		return nil, false
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return nil, false
	}
	valid = true
	return archive, true
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
