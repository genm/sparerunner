//go:build !darwin && !linux

package runner

func requirePrivateCacheRoot(string) error {
	return ErrStrongOwnershipUnavailable
}
