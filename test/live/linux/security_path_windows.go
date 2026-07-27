//go:build windows

package main

import (
	"io/fs"
	"os"
)

func validateTrustedPathChain(string, bool) error               { return errEvidenceInvalid }
func validateTrustedPathChainForUID(string, bool, uint32) error { return errEvidenceInvalid }

func openTrustedRegular(string, bool) (*os.File, fs.FileInfo, error) {
	return nil, nil, errEvidenceInvalid
}

func trustedDirectory(string, os.FileMode) error { return errEvidenceInvalid }

func (liveAuthorityProbe) trustedRootFile(string) error { return errEvidenceInvalid }
