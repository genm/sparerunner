//go:build !darwin && !linux && !windows

package runner

func requirePrivateCacheRoot(string) error {
	return ErrStrongOwnershipUnavailable
}
