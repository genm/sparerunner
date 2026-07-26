//go:build darwin || linux

package runner

import (
	"context"
	"errors"
	"testing"
)

func TestUnixSupervisorOperationsFailClosedWithoutPlatformContainment(t *testing.T) {
	supervisor := NewSupervisor()
	if _, err := supervisor.PrepareContainment(context.Background(), "execution"); !errors.Is(err, ErrStrongOwnershipUnavailable) {
		t.Fatalf("PrepareContainment error = %v", err)
	}
	if _, err := supervisor.Start(context.Background(), StartRequest{}); !errors.Is(err, ErrStrongOwnershipUnavailable) {
		t.Fatalf("Start error = %v", err)
	}
	if err := supervisor.Stop(context.Background(), Process{}); !errors.Is(err, ErrStrongOwnershipUnavailable) {
		t.Fatalf("Stop error = %v", err)
	}
	if _, err := supervisor.Alive(Process{}); !errors.Is(err, ErrStrongOwnershipUnavailable) {
		t.Fatalf("Alive error = %v", err)
	}
}

func TestUnixSupervisorRejectsSetsidEscapeAdmission(t *testing.T) {
	// A child can call setsid and leave a process group; this utility must never
	// advertise strong containment regardless of leader/child behavior.
	if NewSupervisor().StrongDescendantOwnership() {
		t.Fatal("bare Unix process group admitted as strong")
	}
}
