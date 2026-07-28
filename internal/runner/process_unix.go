//go:build darwin || linux

package runner

import "context"

// A process group is not a production containment boundary: a workflow may
// setsid(2) out of it, and PID/Wait observations are not durable ownership.
// Platform adapters replace this fail-closed implementation in spr-007/008.
type unixSupervisor struct{}

func newPlatformSupervisor() Supervisor { return unixSupervisor{} }

func (unixSupervisor) StrongDescendantOwnership() bool { return false }
func (unixSupervisor) WorkspaceBackend() string        { return "" }
func (unixSupervisor) PrepareContainment(context.Context, string) (ContainmentRef, error) {
	return ContainmentRef{}, ErrStrongOwnershipUnavailable
}
func (unixSupervisor) Start(context.Context, StartRequest) (Process, error) {
	return Process{}, ErrStrongOwnershipUnavailable
}
func (unixSupervisor) Stop(context.Context, Process) error {
	return ErrStrongOwnershipUnavailable
}
func (unixSupervisor) Alive(Process) (bool, error) {
	return false, ErrStrongOwnershipUnavailable
}
