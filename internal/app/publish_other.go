//go:build !linux && !windows

package app

import "errors"

func publishStateDirectory(string, string) error {
	return errors.New("atomic controller state publication requires the platform credential adapter")
}
