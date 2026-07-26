//go:build darwin || linux

package runner

import (
	"context"
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type unixSupervisor struct {
	mu        sync.Mutex
	processes map[int]*exec.Cmd
}

func newPlatformSupervisor() Supervisor { return &unixSupervisor{processes: make(map[int]*exec.Cmd)} }

func (*unixSupervisor) StrongDescendantOwnership() bool { return true }

func (s *unixSupervisor) Start(_ context.Context, request StartRequest) (Process, error) {
	if request.Executable == "" || request.Directory == "" {
		return Process{}, ErrInvalidRequest
	}
	command := exec.Command(request.Executable, request.Arguments...)
	command.Dir = request.Directory
	command.Stdout = nil
	command.Stderr = nil
	command.Stdin = nil
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		return Process{}, ErrInvalidRequest
	}
	pid := command.Process.Pid
	s.mu.Lock()
	s.processes[pid] = command
	s.mu.Unlock()
	go func() {
		_ = command.Wait()
		s.mu.Lock()
		delete(s.processes, pid)
		s.mu.Unlock()
	}()
	return Process{PID: pid}, nil
}

func (s *unixSupervisor) Stop(ctx context.Context, process Process) error {
	if process.PID <= 0 {
		return ErrInvalidRequest
	}
	s.mu.Lock()
	_, owned := s.processes[process.PID]
	s.mu.Unlock()
	// PID/PGID are not durable ownership tokens. After an agent restart, or once
	// Wait has reaped a listener, a reused PID must never signal an unrelated host
	// process. Reconciliation quarantines instead of guessing at process ownership.
	if !owned {
		return ErrReconciliationRequired
	}
	// The listener is its own PGID. Negative PID targets descendants created by
	// the runner and avoids leaving a job child after the listener exits.
	err := syscall.Kill(-process.PID, syscall.SIGTERM)
	if err != nil && !errors.Is(err, syscall.ESRCH) {
		return ErrCleanupFailed
	}
	err = syscall.Kill(-process.PID, syscall.SIGKILL)
	if err != nil && !errors.Is(err, syscall.ESRCH) {
		return ErrCleanupFailed
	}
	for {
		alive, aliveErr := groupAlive(process)
		if aliveErr != nil || !alive {
			return aliveErr
		}
		select {
		case <-ctx.Done():
			return ErrCleanupFailed
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *unixSupervisor) Alive(process Process) (bool, error) {
	if process.PID <= 0 {
		return false, ErrInvalidRequest
	}
	s.mu.Lock()
	_, owned := s.processes[process.PID]
	s.mu.Unlock()
	if !owned {
		return false, ErrReconciliationRequired
	}
	return groupAlive(process)
}

func groupAlive(process Process) (bool, error) {
	err := syscall.Kill(-process.PID, 0)
	if err == nil || errors.Is(err, syscall.EPERM) {
		return true, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	return false, ErrCleanupFailed
}
