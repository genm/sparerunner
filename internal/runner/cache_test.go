package runner

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type bytesFetcher struct {
	data  []byte
	mu    sync.Mutex
	calls int
}

type chunkFetcher struct{ chunks [][]byte }

func (f chunkFetcher) Fetch(_ context.Context, _ Package) (io.ReadCloser, error) {
	return io.NopCloser(&chunkReader{chunks: f.chunks}), nil
}

type chunkReader struct {
	chunks [][]byte
	index  int
}

func (r *chunkReader) Read(destination []byte) (int, error) {
	for r.index < len(r.chunks) && len(r.chunks[r.index]) == 0 {
		r.index++
	}
	if r.index == len(r.chunks) {
		return 0, io.EOF
	}
	count := copy(destination, r.chunks[r.index])
	r.chunks[r.index] = r.chunks[r.index][count:]
	return count, nil
}

func (f *bytesFetcher) Fetch(_ context.Context, _ Package) (io.ReadCloser, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func TestOfficialPackagesArePinned(t *testing.T) {
	for _, platform := range []Platform{{"linux", "amd64"}, {"linux", "arm64"}, {"darwin", "amd64"}, {"darwin", "arm64"}, {"windows", "amd64"}, {"windows", "arm64"}} {
		pkg, err := OfficialPackage(platform)
		if err != nil || !pkg.valid() || pkg.Version != OfficialRunnerVersion {
			t.Fatalf("OfficialPackage(%+v) = %#v, %v", platform, pkg, err)
		}
	}
	if _, err := OfficialPackage(Platform{"linux", "386"}); err != ErrUnsupportedPlatform {
		t.Fatalf("unsupported package error = %v", err)
	}
}

func TestCacheConcurrentWinnerIsComplete(t *testing.T) {
	archive := tarArchive(t, []tar.Header{{Name: "run.sh", Mode: 0o755, Size: 7, Typeflag: tar.TypeReg}}, [][]byte{[]byte("#!/bin\n")})
	sum := sha256.Sum256(archive)
	pkg := Package{Version: "test", Platform: Platform{"test", "test"}, Asset: "runner.tar.gz", Checksum: hex.EncodeToString(sum[:]), Size: int64(len(archive)), Format: ArchiveTarGz}
	fetcher := &bytesFetcher{data: archive}
	cache := Cache{Root: t.TempDir(), Fetcher: fetcher, verifyPackage: func(value Package) bool { return value == pkg }}
	const callers = 32
	packages := make(chan PreparedPackage, callers)
	errs := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := cache.Ensure(context.Background(), pkg)
			packages <- result
			errs <- err
		}()
	}
	group.Wait()
	close(packages)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for prepared := range packages {
		if prepared == nil {
			t.Fatal("cache returned a nil prepared package")
		}
		destination, err := os.OpenRoot(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := prepared.Materialize(destination); err != nil {
			destination.Close()
			t.Fatal(err)
		}
		if err := destination.Close(); err != nil {
			t.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
	}
	entryPath := filepath.Join(cache.Root, "packages", pkg.key())
	if _, err := os.Stat(filepath.Join(entryPath, "archive")); err != nil {
		t.Fatalf("winner content unavailable: %v", err)
	}
	if fetcher.calls != 1 {
		t.Fatalf("concurrent first download fetched %d times, want 1", fetcher.calls)
	}
	root, err := os.OpenRoot(cache.Root)
	if err != nil {
		t.Fatal(err)
	}
	verified, valid := openValidCacheEntry(root, "packages/"+pkg.key(), pkg)
	if !valid {
		t.Fatal("cache winner is not a complete verified entry")
	}
	if err := verified.Close(); err != nil {
		t.Fatal(err)
	}
	// Production never mutates verified cache content. The test unlocks its own
	// temporary entry solely so testing.T can remove its sandbox afterward.
	if err := thawCacheEntry(root, "packages/"+pkg.key()); err != nil {
		t.Fatal(err)
	}
	root.Close()
}

func TestCacheRejectsChecksumMismatch(t *testing.T) {
	archive := tarArchive(t, []tar.Header{{Name: "run.sh", Mode: 0o755, Size: 7, Typeflag: tar.TypeReg}}, [][]byte{[]byte("#!/bin\n")})
	pkg := Package{Version: "test", Platform: Platform{"test", "test"}, Asset: "runner.tar.gz", Checksum: strings.Repeat("0", 64), Size: int64(len(archive)), Format: ArchiveTarGz}
	cache := Cache{Root: t.TempDir(), Fetcher: &bytesFetcher{data: archive}, verifyPackage: func(value Package) bool { return value == pkg }}
	if _, err := cache.Ensure(context.Background(), pkg); err != ErrPackageIntegrity {
		t.Fatalf("Ensure checksum mismatch error = %v", err)
	}
}

func TestCacheRequiresExactOfficialAssetSize(t *testing.T) {
	archive := tarArchive(t, []tar.Header{{Name: "run.sh", Mode: 0o755, Size: 7, Typeflag: tar.TypeReg}}, [][]byte{[]byte("#!/bin\n")})
	sum := sha256.Sum256(archive)
	pkg := Package{Version: "test", Platform: Platform{"test", "test"}, Asset: "runner.tar.gz", Checksum: hex.EncodeToString(sum[:]), Size: int64(len(archive)), Format: ArchiveTarGz}
	for name, fetcher := range map[string]Fetcher{
		"short":     &bytesFetcher{data: archive[:len(archive)-1]},
		"oversized": &bytesFetcher{data: append(append([]byte{}, archive...), 0)},
		"chunked":   chunkFetcher{chunks: [][]byte{archive[:3], archive[3:17], archive[17:]}},
	} {
		t.Run(name, func(t *testing.T) {
			cache := Cache{Root: t.TempDir(), Fetcher: fetcher, verifyPackage: func(value Package) bool { return value == pkg }}
			prepared, err := cache.Ensure(context.Background(), pkg)
			if name == "chunked" {
				if err != nil {
					t.Fatalf("chunked Ensure error = %v", err)
				}
				if closeErr := prepared.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				root, openErr := os.OpenRoot(cache.Root)
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer root.Close()
				if thawErr := thawCacheEntry(root, "packages/"+pkg.key()); thawErr != nil {
					t.Fatal(thawErr)
				}
				return
			}
			if err != ErrPackageIntegrity {
				t.Fatalf("Ensure error = %v, want ErrPackageIntegrity", err)
			}
		})
	}
}

func TestCacheHitRejectsTamperedArtifact(t *testing.T) {
	archive := tarArchive(t, []tar.Header{{Name: "run.sh", Mode: 0o755, Size: 7, Typeflag: tar.TypeReg}}, [][]byte{[]byte("#!/bin\n")})
	sum := sha256.Sum256(archive)
	pkg := Package{Version: "test", Platform: Platform{"test", "test"}, Asset: "runner.tar.gz", Checksum: hex.EncodeToString(sum[:]), Size: int64(len(archive)), Format: ArchiveTarGz}
	fetcher := &bytesFetcher{data: archive}
	cache := Cache{Root: t.TempDir(), Fetcher: fetcher, verifyPackage: func(value Package) bool { return value == pkg }}
	prepared, err := cache.Ensure(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(cache.Root)
	if err != nil {
		t.Fatal(err)
	}
	entry := "packages/" + pkg.key()
	if err := thawCacheEntry(root, entry); err != nil {
		t.Fatal(err)
	}
	entryRoot, err := root.OpenRoot(entry)
	if err != nil {
		t.Fatal(err)
	}
	if err := entryRoot.Chmod("archive", 0o600); err != nil {
		t.Fatal(err)
	}
	mutated := append([]byte{}, archive...)
	mutated[0] ^= 0xff
	if err := entryRoot.WriteFile("archive", mutated, 0o600); err != nil {
		t.Fatal(err)
	}
	entryRoot.Close()
	root.Close()
	if _, err := cache.Ensure(context.Background(), pkg); err != ErrPackageIntegrity {
		t.Fatalf("tampered cache error = %v, want ErrPackageIntegrity", err)
	}
	if fetcher.calls != 2 {
		t.Fatalf("tampered cache did not force a fresh verification; fetch calls = %d", fetcher.calls)
	}
}

func TestExtractRejectsTraversalAndLinkEscape(t *testing.T) {
	for _, header := range []tar.Header{
		{Name: "../escape", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg},
		{Name: "link", Linkname: "../../escape", Typeflag: tar.TypeSymlink},
		{Name: "fifo", Typeflag: tar.TypeFifo},
	} {
		t.Run(header.Name, func(t *testing.T) {
			data := tarArchive(t, []tar.Header{header}, [][]byte{[]byte("x")})
			root, err := os.OpenRoot(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			if err := extractTarGz(root, bytes.NewReader(data)); err != ErrUnsafeArchive {
				t.Fatalf("extract error = %v, want ErrUnsafeArchive", err)
			}
		})
	}
}

func TestExtractPreservesRelativeSymlinkInsideRoot(t *testing.T) {
	data := tarArchive(t, []tar.Header{
		{Name: "bin/target", Mode: 0o755, Size: 2, Typeflag: tar.TypeReg},
		{Name: "bin/link", Linkname: "target", Typeflag: tar.TypeSymlink},
	}, [][]byte{[]byte("ok"), nil})
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := extractTarGz(root, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	got, err := root.ReadFile("bin/link")
	if err != nil || string(got) != "ok" {
		t.Fatalf("symlink content = %q, %v", got, err)
	}
}

func BenchmarkCacheHit(b *testing.B) {
	archive := tarArchive(b, []tar.Header{{Name: "run.sh", Mode: 0o755, Size: 7, Typeflag: tar.TypeReg}}, [][]byte{[]byte("#!/bin\n")})
	sum := sha256.Sum256(archive)
	pkg := Package{Version: "test", Platform: Platform{"test", "test"}, Asset: "runner.tar.gz", Checksum: hex.EncodeToString(sum[:]), Size: int64(len(archive)), Format: ArchiveTarGz}
	fetcher := &bytesFetcher{data: archive}
	cache := Cache{Root: b.TempDir(), Fetcher: fetcher, verifyPackage: func(value Package) bool { return value == pkg }}
	prepared, err := cache.Ensure(context.Background(), pkg)
	if err != nil {
		b.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		root, err := os.OpenRoot(cache.Root)
		if err != nil {
			b.Error(err)
			return
		}
		defer root.Close()
		if err := thawCacheEntry(root, "packages/"+pkg.key()); err != nil {
			b.Error(err)
		}
	})
	b.ResetTimer()
	for b.Loop() {
		prepared, err := cache.Ensure(context.Background(), pkg)
		if err != nil {
			b.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if fetcher.calls != 1 {
		b.Fatalf("cache hit fetched %d times", fetcher.calls)
	}
}

func tarArchive(t testing.TB, headers []tar.Header, bodies [][]byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	writer := tar.NewWriter(gzipWriter)
	for index, header := range headers {
		if err := writer.WriteHeader(&header); err != nil {
			t.Fatal(err)
		}
		if header.Size > 0 && len(bodies[index]) > 0 {
			if _, err := writer.Write(bodies[index]); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
