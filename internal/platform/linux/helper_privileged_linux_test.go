//go:build linux && tewake_privileged

package linux

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// This test crosses the production root-owned socket boundary. Keeping it in
// the explicit privileged suite prevents a non-root unit-test process from
// weakening that boundary merely to exercise Supervisor shutdown.
func TestHelperServeShutdownInvokesGlobalRuntimeCleanup(t *testing.T) {
	server, runtime, policy := directFenceServer(t)
	socketDirectory, err := os.MkdirTemp("/run", "tewake-shutdown-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(socketDirectory)
	if err := os.Chown(socketDirectory, 0, policy.AgentGID); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	policy.SocketPath = filepath.Join(socketDirectory, "supervisor.sock")
	server.policy.SocketPath = policy.SocketPath
	listener, err := ListenHelperSocket(policy)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(policy.SocketPath)
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- server.Serve(ctx, listener)
	}()
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve shutdown error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve shutdown did not finish")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.shutdownCalls != 1 {
		t.Fatalf("runtime shutdown calls=%d", runtime.shutdownCalls)
	}
}
