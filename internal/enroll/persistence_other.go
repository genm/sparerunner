//go:build !linux

package enroll

import "errors"

func requirePrivateDirectory(string) error {
	return errors.New("private material requires platform credential store adapter")
}
func requirePrivateRegularFile(string) error {
	return errors.New("private material requires platform credential store adapter")
}
func syncDirectory(string) error {
	return errors.New("private material requires platform credential store adapter")
}
