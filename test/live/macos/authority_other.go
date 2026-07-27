//go:build !darwin

package main

import "os"

func validateMacOSLiveFileAuthority(string, os.FileMode) error {
	return nil
}
