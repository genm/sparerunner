//go:build !darwin && !linux && !windows

package runner

import "context"

type unsupportedSupervisor struct{}

func newPlatformSupervisor() Supervisor                       { return unsupportedSupervisor{} }
func (unsupportedSupervisor) StrongDescendantOwnership() bool { return false }
func (unsupportedSupervisor) Start(context.Context, StartRequest) (Process, error) {
	return Process{}, ErrStrongOwnershipUnavailable
}
func (unsupportedSupervisor) Stop(context.Context, Process) error {
	return ErrStrongOwnershipUnavailable
}
func (unsupportedSupervisor) Alive(Process) (bool, error) {
	return false, ErrStrongOwnershipUnavailable
}
