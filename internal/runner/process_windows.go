//go:build windows

package runner

import "context"

// twk-009 installs a Windows Job Object adapter. Until then, a bare PID has no
// trustworthy descendant ownership, so runtime admission and hosted tests fail
// closed rather than pretending process cleanup is complete.
type windowsSupervisor struct{}

func newPlatformSupervisor() Supervisor                   { return windowsSupervisor{} }
func (windowsSupervisor) StrongDescendantOwnership() bool { return false }
func (windowsSupervisor) WorkspaceBackend() string        { return "" }
func (windowsSupervisor) PrepareContainment(context.Context, string) (ContainmentRef, error) {
	return ContainmentRef{}, ErrStrongOwnershipUnavailable
}
func (windowsSupervisor) Start(context.Context, StartRequest) (Process, error) {
	return Process{}, ErrStrongOwnershipUnavailable
}
func (windowsSupervisor) Stop(context.Context, Process) error { return ErrStrongOwnershipUnavailable }
func (windowsSupervisor) Alive(Process) (bool, error)         { return false, ErrStrongOwnershipUnavailable }
