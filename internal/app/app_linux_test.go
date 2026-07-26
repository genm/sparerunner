//go:build linux

package app

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/genm/tewake/internal/enroll"
)

func TestInitServeJoinAndAgentReconnect(t *testing.T) {
	ctx := context.Background()
	root := privateTestDirectory(t)
	controllerDirectory := filepath.Join(root, "controller")
	code, err := InitializeController(ctx, controllerDirectory, nil)
	if err != nil {
		t.Fatal(err)
	}
	if code == "" {
		t.Fatal("init did not return join code")
	}
	if _, err := InitializeController(ctx, controllerDirectory, nil); !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatalf("second init error = %v", err)
	}
	controller, err := OpenController(ctx, controllerDirectory, true)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverContext, stopServer := context.WithCancel(ctx)
	serverResult := make(chan error, 1)
	go func() {
		serverResult <- ServeController(serverContext, controller, ControllerServeOptions{AgentListener: listener})
	}()
	endpoint := "https://" + listener.Addr().String()
	agentDirectory := filepath.Join(root, "agent")
	nodeID, err := JoinAgent(ctx, JoinOptions{
		StateDirectory:    agentDirectory,
		JoinCode:          code,
		Controller:        endpoint,
		ConnectionTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if nodeID == "" {
		t.Fatal("join returned no node ID")
	}
	if replayedNodeID, err := JoinAgent(ctx, JoinOptions{
		StateDirectory:    agentDirectory,
		JoinCode:          code,
		Controller:        endpoint,
		ConnectionTimeout: 3 * time.Second,
	}); err != nil || replayedNodeID != nodeID {
		t.Fatalf("joined state confirmation replay = %q, %v", replayedNodeID, err)
	}
	eventually(t, func() bool { return controller.Sessions.Count() == 0 })
	assertDirectoryDoesNotContain(t, agentDirectory, []byte(code))

	agentContext, stopAgent := context.WithCancel(ctx)
	agentResult := make(chan error, 1)
	go func() {
		agentResult <- ServeAgent(agentContext, AgentServeOptions{
			StateDirectory:    agentDirectory,
			ConnectionTimeout: 3 * time.Second,
			ReconnectDelay:    10 * time.Millisecond,
		})
	}()
	eventually(t, func() bool { return controller.Sessions.Count() == 1 })
	stopAgent()
	select {
	case err := <-agentResult:
		if err != nil {
			t.Fatalf("agent shutdown = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not stop")
	}
	eventually(t, func() bool { return controller.Sessions.Count() == 0 })
	stopServer()
	select {
	case err := <-serverResult:
		if err != nil {
			t.Fatalf("controller shutdown = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("controller did not stop")
	}

	enrolled, err := OpenAgent(ctx, agentDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer enrolled.Close()
	if err := enrolled.CredentialReady(ctx); err != nil {
		t.Fatalf("enrolled credential readiness: %v", err)
	}
	if err := os.Remove(filepath.Join(agentDirectory, agentKeyFile)); err != nil {
		t.Fatal(err)
	}
	if err := enrolled.CredentialReady(ctx); !errors.Is(err, ErrAgentCredentialUnavailable) {
		t.Fatalf("deleted durable credential readiness: %v", err)
	}
}

func TestControllerRestartInvalidatesUnusedCodeButNotFirstStartCode(t *testing.T) {
	ctx := context.Background()
	controllerDirectory := filepath.Join(privateTestDirectory(t), "controller")
	firstCode, err := InitializeController(ctx, controllerDirectory, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := OpenController(ctx, controllerDirectory, true)
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := testCSR(t)
	if _, err := first.Service.Enroll(ctx, firstCode, csr); err != nil {
		t.Fatalf("first-start code was invalidated: %v", err)
	}
	unusedCode, err := first.CreateJoinCode(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenController(ctx, controllerDirectory, true)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err := restarted.Service.Enroll(ctx, unusedCode, csr); !errors.Is(err, enroll.ErrTokenNotFound) {
		t.Fatalf("unused pre-restart code error = %v", err)
	}
}

func TestControllerInitializationRollsBackPrivateMaterialBeforeRemovingStage(t *testing.T) {
	ctx := context.Background()
	root := privateTestDirectory(t)
	controllerDirectory := filepath.Join(root, "controller")
	if _, err := InitializeController(
		ctx,
		controllerDirectory,
		[]string{"https://controller.example.test/path-is-not-allowed"},
	); err == nil {
		t.Fatal("invalid endpoint hint unexpectedly initialized the controller")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed initialization retained staging entries: %v", entries)
	}
	if code, err := InitializeController(ctx, controllerDirectory, nil); err != nil || code == "" {
		t.Fatalf("retry after rollback: code=%q err=%v", code, err)
	}
}

func privateTestDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func testCSR(t *testing.T) ([]byte, []byte) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := enroll.CreateNodeCSR(key)
	if err != nil {
		t.Fatal(err)
	}
	return csr, key
}

func assertDirectoryDoesNotContain(t *testing.T, root string, canary []byte) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, canary) {
			t.Fatalf("join code persisted in %s", filepath.Base(path))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
