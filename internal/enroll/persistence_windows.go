//go:build windows

package enroll

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/genm/sparerunner/internal/winacl"
	syswindows "golang.org/x/sys/windows"
)

const windowsDPAPIMagic = "TWKDPAPI\x01"

var windowsDPAPIEntropy = []byte("sparerunner/private-material/windows/v1")

func persistPrivateMaterial(path string, contents []byte) error {
	// SavePrivateMaterial does not take ownership of its caller's buffer. In
	// particular, Agent enrollment compares an existing locator with the same
	// encoded bytes when CREATE_NEW reports a replay.
	plaintext := append([]byte(nil), contents...)
	defer clear(plaintext)
	ciphertext, err := protectWindowsPrivateMaterial(plaintext)
	if err != nil {
		return err
	}
	defer clear(ciphertext)
	return atomicWindowsPrivateFile(path, ciphertext)
}

func loadPrivateMaterial(path string) ([]byte, error) {
	if err := requirePrivateRegularFile(path); err != nil {
		return nil, err
	}
	ciphertext, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	defer clear(ciphertext)
	if !bytes.HasPrefix(ciphertext, []byte(windowsDPAPIMagic)) ||
		len(ciphertext) == len(windowsDPAPIMagic) {
		return nil, errors.New("invalid Windows private material envelope")
	}
	return unprotectWindowsPrivateMaterial(ciphertext[len(windowsDPAPIMagic):])
}

func removePrivateMaterial(path string) error {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if parentErr := requirePrivateDirectory(filepath.Dir(path)); parentErr != nil {
			if errors.Is(parentErr, os.ErrNotExist) {
				return nil
			}
			return parentErr
		}
		return nil
	} else if err != nil {
		return err
	}
	if err := requirePrivateRegularFile(path); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func requirePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return winacl.ErrUnsafePrivatePath
	}
	return winacl.ValidatePrivateDirectory(path)
}

func requirePrivateRegularFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return winacl.ErrUnsafePrivatePath
	}
	return winacl.ValidatePrivateFile(path)
}

func syncDirectory(path string) error {
	// Windows does not expose portable directory fsync. Publication itself uses
	// CREATE_NEW plus FILE_FLAG_WRITE_THROUGH, so this hook validates that the
	// still-owning directory has not changed authority.
	return requirePrivateDirectory(path)
}

func atomicWindowsPrivateFile(path string, contents []byte) error {
	parent := filepath.Dir(path)
	_, parentErr := os.Lstat(parent)
	parentCreated := errors.Is(parentErr, os.ErrNotExist)
	if parentErr != nil && !parentCreated {
		return parentErr
	}
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	if parentCreated {
		if err := winacl.SecureEmptyPrivateDirectory(parent); err != nil {
			return err
		}
	}
	if err := requirePrivateDirectory(parent); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return errors.New("private material already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := winacl.CreatePrivateFile(path)
	if err != nil {
		if errors.Is(err, syswindows.ERROR_FILE_EXISTS) ||
			errors.Is(err, syswindows.ERROR_ALREADY_EXISTS) {
			return errors.New("private material already exists")
		}
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	published := false
	defer func() {
		if !published {
			_ = os.Remove(path)
		}
	}()
	if written, err := file.Write(contents); err != nil || written != len(contents) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	if err := requirePrivateRegularFile(path); err != nil {
		return err
	}
	published = true
	return nil
}

func protectWindowsPrivateMaterial(plaintext []byte) ([]byte, error) {
	if len(plaintext) == 0 || uint64(len(plaintext)) > math.MaxUint32 {
		return nil, errors.New("invalid Windows private material")
	}
	input := syswindows.DataBlob{
		Size: uint32(len(plaintext)),
		Data: &plaintext[0],
	}
	entropy := syswindows.DataBlob{
		Size: uint32(len(windowsDPAPIEntropy)),
		Data: &windowsDPAPIEntropy[0],
	}
	var output syswindows.DataBlob
	if err := syswindows.CryptProtectData(
		&input,
		nil,
		&entropy,
		0,
		nil,
		syswindows.CRYPTPROTECT_UI_FORBIDDEN,
		&output,
	); err != nil || output.Data == nil || output.Size == 0 {
		if output.Data != nil {
			_, _ = syswindows.LocalFree(
				syswindows.Handle(uintptr(unsafe.Pointer(output.Data))),
			)
		}
		return nil, errors.New("protect Windows private material")
	}
	protected := unsafe.Slice(output.Data, output.Size)
	result := make([]byte, len(windowsDPAPIMagic)+len(protected))
	copy(result, windowsDPAPIMagic)
	copy(result[len(windowsDPAPIMagic):], protected)
	clear(protected)
	if _, err := syswindows.LocalFree(
		syswindows.Handle(uintptr(unsafe.Pointer(output.Data))),
	); err != nil {
		clear(result)
		return nil, errors.New("release protected Windows private material")
	}
	return result, nil
}

func unprotectWindowsPrivateMaterial(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) == 0 || uint64(len(ciphertext)) > math.MaxUint32 {
		return nil, errors.New("invalid Windows private material")
	}
	input := syswindows.DataBlob{
		Size: uint32(len(ciphertext)),
		Data: &ciphertext[0],
	}
	entropy := syswindows.DataBlob{
		Size: uint32(len(windowsDPAPIEntropy)),
		Data: &windowsDPAPIEntropy[0],
	}
	var output syswindows.DataBlob
	if err := syswindows.CryptUnprotectData(
		&input,
		nil,
		&entropy,
		0,
		nil,
		syswindows.CRYPTPROTECT_UI_FORBIDDEN,
		&output,
	); err != nil || output.Data == nil || output.Size == 0 {
		if output.Data != nil {
			_, _ = syswindows.LocalFree(
				syswindows.Handle(uintptr(unsafe.Pointer(output.Data))),
			)
		}
		return nil, errors.New("unprotect Windows private material")
	}
	plaintextView := unsafe.Slice(output.Data, output.Size)
	plaintext := append([]byte(nil), plaintextView...)
	clear(plaintextView)
	if _, err := syswindows.LocalFree(
		syswindows.Handle(uintptr(unsafe.Pointer(output.Data))),
	); err != nil {
		clear(plaintext)
		return nil, errors.New("release unprotected Windows private material")
	}
	return plaintext, nil
}
