//go:build windows

package nodectl_test

import (
	"errors"
	"testing"

	"github.com/genm/tewake/internal/nodectl"
)

// Windows has no verified peer-credential socket yet, so both halves of the
// contract must fail closed rather than offering an unauthenticated endpoint.
func TestWindowsLocalControlFailsClosed(t *testing.T) {
	if _, err := nodectl.Listen(t.TempDir()); !errors.Is(err, nodectl.ErrEndpointUnsupported) {
		t.Fatalf("listen did not fail closed: %v", err)
	}
	client := nodectl.Client{StateDirectory: t.TempDir(), Source: nodectl.SourceTray}
	_, err := client.Status()
	var controlErr *nodectl.Error
	if !errors.As(err, &controlErr) || controlErr.Class != nodectl.ErrorClassEndpointUnsupported {
		t.Fatalf("client did not fail closed: %v", err)
	}
}
