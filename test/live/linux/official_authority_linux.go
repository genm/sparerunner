//go:build linux

package main

import (
	"syscall"
)

func (liveAuthorityProbe) officialRunnerAuthority(
	runtimeRoot string,
) (string, string, int64, error) {
	path, pkg, err := expectedOfficialRunnerAuthority(runtimeRoot)
	if err != nil {
		return "", "", 0, errEvidenceInvalid
	}
	if err := validateTrustedPathChainForUID(path, false, 0); err != nil {
		return "", "", 0, errEvidenceInvalid
	}
	file, info, err := openTrustedRegular(path, false)
	if err != nil {
		return "", "", 0, errEvidenceInvalid
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || stat.Nlink != 1 ||
		info.Mode().Perm() != 0o400 || info.Size() != pkg.Size {
		_ = file.Close()
		return "", "", 0, errEvidenceInvalid
	}
	digest, err := digestOpenFile(file)
	closeErr := file.Close()
	if err != nil || closeErr != nil || digest != pkg.Checksum {
		return "", "", 0, errEvidenceInvalid
	}
	return path, digest, info.Size(), nil
}
