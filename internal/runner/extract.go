package runner

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"strings"
)

type checksumWriter struct {
	writer io.Writer
	hash   hashWriter
}

type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func newChecksumWriter(writer io.Writer) *checksumWriter {
	return &checksumWriter{writer: writer, hash: sha256.New()}
}

func (w *checksumWriter) Write(data []byte) (int, error) {
	if _, err := w.hash.Write(data); err != nil {
		return 0, err
	}
	return w.writer.Write(data)
}

func (w *checksumWriter) matches(expected string) bool {
	return strings.EqualFold(hex.EncodeToString(w.hash.Sum(nil)), expected)
}

func extractArchive(root *os.Root, archive *os.File, format ArchiveFormat) error {
	switch format {
	case ArchiveTarGz:
		return extractTarGz(root, archive)
	case ArchiveZIP:
		info, err := archive.Stat()
		if err != nil {
			return ErrPackageIntegrity
		}
		return extractZIP(root, archive, info.Size())
	default:
		return ErrUnsafeArchive
	}
}

func extractTarGz(root *os.Root, archive io.Reader) error {
	gzipReader, err := gzip.NewReader(archive)
	if err != nil {
		return ErrUnsafeArchive
	}
	defer gzipReader.Close()
	reader := tar.NewReader(gzipReader)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return ErrUnsafeArchive
		}
		// The inspected official tarball begins with "./" as a root directory
		// marker. It is not a writable archive member and needs no extraction.
		if (header.Name == "." || header.Name == "./") && header.Typeflag == tar.TypeDir {
			continue
		}
		name, err := safeArchiveName(header.Name)
		if err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := root.MkdirAll(name, header.FileInfo().Mode().Perm()); err != nil {
				return ErrPackageIntegrity
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := writeArchiveRegular(root, name, reader, header.FileInfo().Mode().Perm()); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := writeArchiveSymlink(root, name, header.Linkname); err != nil {
				return err
			}
		// The official Linux v2.336.0 archive listing has relative symlinks but
		// no hard links. Reject hard/special members rather than silently creating
		// aliases to files that an archive does not need for correct installation.
		case tar.TypeLink, tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
			return ErrUnsafeArchive
		default:
			return ErrUnsafeArchive
		}
	}
}

func extractZIP(root *os.Root, archive *os.File, size int64) error {
	reader, err := zip.NewReader(archive, size)
	if err != nil {
		return ErrUnsafeArchive
	}
	for _, file := range reader.File {
		if (file.Name == "." || file.Name == "./") && file.FileInfo().IsDir() {
			continue
		}
		name, err := safeArchiveName(file.Name)
		if err != nil {
			return err
		}
		mode := file.Mode()
		if file.FileInfo().IsDir() {
			if err := root.MkdirAll(name, mode.Perm()); err != nil {
				return ErrPackageIntegrity
			}
			continue
		}
		if mode&os.ModeSymlink != 0 {
			input, err := file.Open()
			if err != nil {
				return ErrUnsafeArchive
			}
			target, readErr := io.ReadAll(input)
			closeErr := input.Close()
			if readErr != nil || closeErr != nil {
				return ErrUnsafeArchive
			}
			if err := writeArchiveSymlink(root, name, string(target)); err != nil {
				return err
			}
			continue
		}
		if !mode.IsRegular() {
			return ErrUnsafeArchive
		}
		input, err := file.Open()
		if err != nil {
			return ErrUnsafeArchive
		}
		err = writeArchiveRegular(root, name, input, mode.Perm())
		closeErr := input.Close()
		if err != nil || closeErr != nil {
			if err != nil {
				return err
			}
			return ErrUnsafeArchive
		}
	}
	return nil
}

func writeArchiveRegular(root *os.Root, name string, source io.Reader, mode fs.FileMode) error {
	if err := makeParent(root, name); err != nil {
		return ErrPackageIntegrity
	}
	if _, err := root.Lstat(name); err == nil {
		return ErrUnsafeArchive
	}
	file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode.Perm())
	if err != nil {
		return ErrPackageIntegrity
	}
	// Archive bytes have already passed the exact official release size and
	// SHA-256 checks before extraction. No unrelated product quota is imposed.
	_, copyErr := io.Copy(file, source)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return ErrPackageIntegrity
	}
	return nil
}

func writeArchiveSymlink(root *os.Root, name, target string) error {
	if err := safeLinkTarget(name, target); err != nil {
		return err
	}
	if err := makeParent(root, name); err != nil {
		return ErrPackageIntegrity
	}
	if _, err := root.Lstat(name); err == nil {
		return ErrUnsafeArchive
	}
	if err := root.Symlink(target, name); err != nil {
		return ErrPackageIntegrity
	}
	return nil
}
