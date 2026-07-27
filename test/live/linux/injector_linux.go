//go:build linux

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

const injectorRunParent = "/run/tewake-live-injectors"

type injectorFileEvidence struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	UID    uint32 `json:"uid"`
	Mode   uint32 `json:"mode"`
	Size   int64  `json:"size"`
}

type injectorEvidence struct {
	Version            int                  `json:"version"`
	Status             string               `json:"status"`
	Source             injectorFileEvidence `json:"source"`
	Copy               injectorFileEvidence `json:"copy"`
	PreparedObservedAt string               `json:"preparedObservedAt"`
	ArmedObservedAt    string               `json:"armedObservedAt,omitempty"`
	DisarmedObservedAt string               `json:"disarmedObservedAt,omitempty"`
}

func prepareInjector(config liveConfig, sourcePath string) error {
	if os.Geteuid() != 0 || !canonicalAbsolutePath(sourcePath) {
		return errEvidenceInvalid
	}
	if err := validateTrustedPathChainForUID(sourcePath, false, 0); err != nil {
		return errEvidenceInvalid
	}
	source, sourceInfo, err := openTrustedInjector(sourcePath, 0)
	if err != nil {
		return err
	}
	defer source.Close()
	if err := os.MkdirAll(injectorRunParent, 0o700); err != nil {
		return errEvidenceInvalid
	}
	if err := os.Chmod(injectorRunParent, 0o700); err != nil {
		return errEvidenceInvalid
	}
	if err := validateTrustedPathChainForUID(injectorRunParent, true, 0); err != nil {
		return errEvidenceInvalid
	}
	runDirectory, err := os.MkdirTemp(injectorRunParent, "run-")
	if err != nil {
		return errEvidenceInvalid
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(runDirectory)
		}
	}()
	if err := os.Chmod(runDirectory, 0o700); err != nil {
		return errEvidenceInvalid
	}
	copyPath := filepath.Join(runDirectory, "injector")
	copyFile, err := os.OpenFile(copyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o500)
	if err != nil {
		return errEvidenceInvalid
	}
	sourceDigest := sha256.New()
	writer := io.MultiWriter(copyFile, sourceDigest)
	_, copyErr := io.Copy(writer, source)
	syncErr := copyFile.Sync()
	closeErr := copyFile.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		return errEvidenceInvalid
	}
	if !injectorSourceUnchanged(sourcePath, sourceInfo) {
		return errEvidenceInvalid
	}
	directory, err := os.Open(runDirectory)
	if err != nil {
		return errEvidenceInvalid
	}
	syncErr = directory.Sync()
	closeErr = directory.Close()
	if syncErr != nil || closeErr != nil {
		return errEvidenceInvalid
	}
	sourceMetadata, err := injectorMetadata(
		sourcePath,
		sourceInfo,
		hex.EncodeToString(sourceDigest.Sum(nil)),
		0,
	)
	if err != nil {
		return err
	}
	copyHandle, copyInfo, err := openTrustedInjector(copyPath, 0)
	if err != nil {
		return err
	}
	copyDigest, err := digestOpenFile(copyHandle)
	closeErr = copyHandle.Close()
	if err != nil || closeErr != nil || copyDigest != sourceMetadata.SHA256 {
		return errEvidenceInvalid
	}
	copyMetadata, err := injectorMetadata(copyPath, copyInfo, copyDigest, 0)
	if err != nil {
		return err
	}
	evidence, err := openEvidenceStore(config.EvidenceDirectory)
	if err != nil {
		return err
	}
	if _, statErr := os.Lstat(filepath.Join(config.EvidenceDirectory, injectorFileName)); !errors.Is(statErr, os.ErrNotExist) {
		return errEvidenceInvalid
	}
	if err := evidence.writeJSON(injectorFileName, injectorEvidence{
		Version:            evidenceVersion,
		Status:             "prepared",
		Source:             sourceMetadata,
		Copy:               copyMetadata,
		PreparedObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return err
	}
	keep = true
	return nil
}

func injectorSourceUnchanged(path string, openedInfo os.FileInfo) bool {
	current, err := os.Lstat(path)
	return err == nil && current.Mode()&os.ModeSymlink == 0 &&
		os.SameFile(openedInfo, current)
}

func executeInjector(config liveConfig, operation string) error {
	if os.Geteuid() != 0 || (operation != "arm" && operation != "disarm") {
		return errEvidenceInvalid
	}
	evidenceStore, err := openEvidenceStore(config.EvidenceDirectory)
	if err != nil {
		return err
	}
	evidence, err := loadEvidenceFile[injectorEvidence](
		config.EvidenceDirectory,
		injectorFileName,
	)
	if err != nil || validatePreparedInjector(evidence) != nil {
		return errEvidenceInvalid
	}
	handle, metadata, err := verifyInjectorFile(evidence.Copy, 0)
	if err != nil {
		return err
	}
	if metadata != evidence.Copy {
		_ = handle.Close()
		return errEvidenceInvalid
	}
	command := exec.Command("/proc/self/fd/3", operation)
	command.ExtraFiles = []*os.File{handle}
	runErr := command.Run()
	closeErr := handle.Close()
	if runErr != nil || closeErr != nil {
		return errEvidenceInvalid
	}
	observedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if operation == "arm" {
		if evidence.ArmedObservedAt != "" || evidence.DisarmedObservedAt != "" {
			return errEvidenceInvalid
		}
		evidence.Status = "armed"
		evidence.ArmedObservedAt = observedAt
	} else {
		if evidence.ArmedObservedAt == "" || evidence.DisarmedObservedAt != "" {
			return errEvidenceInvalid
		}
		evidence.Status = "disarmed"
		evidence.DisarmedObservedAt = observedAt
	}
	return evidenceStore.writeJSON(injectorFileName, evidence)
}

func cleanupInjectorCopy(config liveConfig) error {
	evidence, err := loadEvidenceFile[injectorEvidence](
		config.EvidenceDirectory,
		injectorFileName,
	)
	if err != nil ||
		(evidence.Status != "prepared" && evidence.Status != "disarmed") ||
		validatePreparedInjector(evidence) != nil {
		return errEvidenceInvalid
	}
	handle, _, err := verifyInjectorFile(evidence.Copy, 0)
	if err != nil {
		return err
	}
	if err := handle.Close(); err != nil {
		return errEvidenceInvalid
	}
	if err := os.Remove(evidence.Copy.Path); err != nil {
		return errEvidenceInvalid
	}
	return os.Remove(filepath.Dir(evidence.Copy.Path))
}

func openTrustedInjector(path string, uid uint32) (*os.File, os.FileInfo, error) {
	if err := validateTrustedPathChainForUID(path, false, uid); err != nil {
		return nil, nil, errEvidenceInvalid
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0o022 != 0 ||
		before.Mode().Perm()&0o100 == 0 {
		return nil, nil, errEvidenceInvalid
	}
	stat, ok := before.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid {
		return nil, nil, errEvidenceInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, errEvidenceInvalid
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		_ = file.Close()
		return nil, nil, errEvidenceInvalid
	}
	return file, after, nil
}

func digestOpenFile(file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", errEvidenceInvalid
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", errEvidenceInvalid
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func injectorMetadata(
	path string,
	info os.FileInfo,
	digest string,
	expectedUID uint32,
) (injectorFileEvidence, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != expectedUID || info.Mode().Perm()&0o022 != 0 ||
		!lowerHexDigest(digest, 64) {
		return injectorFileEvidence{}, errEvidenceInvalid
	}
	return injectorFileEvidence{
		Path: path, SHA256: digest, Device: uint64(stat.Dev), Inode: stat.Ino,
		UID: stat.Uid, Mode: uint32(info.Mode().Perm()), Size: info.Size(),
	}, nil
}

func verifyInjectorFile(
	expected injectorFileEvidence,
	expectedUID uint32,
) (*os.File, injectorFileEvidence, error) {
	handle, info, err := openTrustedInjector(expected.Path, expectedUID)
	if err != nil {
		return nil, injectorFileEvidence{}, err
	}
	digest, err := digestOpenFile(handle)
	if err != nil {
		_ = handle.Close()
		return nil, injectorFileEvidence{}, errEvidenceInvalid
	}
	metadata, err := injectorMetadata(expected.Path, info, digest, expectedUID)
	if err != nil || metadata != expected {
		_ = handle.Close()
		return nil, injectorFileEvidence{}, errEvidenceInvalid
	}
	return handle, metadata, nil
}

func validatePreparedInjector(evidence injectorEvidence) error {
	if evidence.Version != evidenceVersion ||
		(evidence.Status != "prepared" && evidence.Status != "armed" &&
			evidence.Status != "disarmed") ||
		evidence.Source.UID != 0 || evidence.Copy.UID != 0 ||
		evidence.Source.SHA256 != evidence.Copy.SHA256 ||
		evidence.Source.Device == 0 || evidence.Source.Inode == 0 ||
		evidence.Copy.Device == 0 || evidence.Copy.Inode == 0 ||
		evidence.Source.Size <= 0 || evidence.Copy.Size != evidence.Source.Size ||
		evidence.Copy.Mode != 0o500 ||
		!canonicalAbsolutePath(evidence.Source.Path) ||
		!canonicalAbsolutePath(evidence.Copy.Path) ||
		filepath.Dir(filepath.Dir(evidence.Copy.Path)) != injectorRunParent {
		return errEvidenceInvalid
	}
	return nil
}

func validateInjectorManifest(
	evidence injectorEvidence,
	startedAt time.Time,
	finishedAt time.Time,
) error {
	if validatePreparedInjector(evidence) != nil || evidence.Status != "disarmed" {
		return errEvidenceInvalid
	}
	preparedAt, err := time.Parse(time.RFC3339Nano, evidence.PreparedObservedAt)
	if err != nil || preparedAt.After(startedAt) {
		return errEvidenceInvalid
	}
	armedAt, err := time.Parse(time.RFC3339Nano, evidence.ArmedObservedAt)
	if err != nil || armedAt.Before(preparedAt) || armedAt.After(finishedAt) {
		return errEvidenceInvalid
	}
	disarmedAt, err := time.Parse(time.RFC3339Nano, evidence.DisarmedObservedAt)
	if err != nil || disarmedAt.Before(armedAt) || disarmedAt.After(finishedAt.Add(time.Minute)) {
		return errEvidenceInvalid
	}
	return nil
}
