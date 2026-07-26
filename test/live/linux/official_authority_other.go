//go:build !linux

package main

func (liveAuthorityProbe) officialRunnerAuthority(string) (string, string, int64, error) {
	return "", "", 0, errEvidenceInvalid
}
