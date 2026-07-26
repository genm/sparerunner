package app

import (
	"net"
	"strings"
	"testing"
)

func TestAdminListenerRejectsNonLoopbackExposure(t *testing.T) {
	loopback, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer loopback.Close()
	if err := ValidateAdminListener(loopback); err != nil {
		t.Fatalf("loopback listener rejected: %v", err)
	}

	exposed, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer exposed.Close()
	if err := ValidateAdminListener(exposed); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("non-loopback listener error = %v", err)
	}
}
