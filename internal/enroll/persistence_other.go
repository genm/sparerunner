//go:build !linux && !darwin

package enroll

import (
	"errors"
	"os"
)

func persistPrivateMaterial(string, []byte) error {
	return errors.New("private material requires platform credential store adapter")
}
func loadPrivateMaterial(string) ([]byte, error) {
	return nil, errors.New("private material requires platform credential store adapter")
}
func removePrivateMaterial(path string) error {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return errors.New("private material requires platform credential store adapter")
}
func requirePrivateDirectory(string) error {
	return errors.New("private material requires platform credential store adapter")
}
func requirePrivateRegularFile(string) error {
	return errors.New("private material requires platform credential store adapter")
}
func syncDirectory(string) error {
	return errors.New("private material requires platform credential store adapter")
}
