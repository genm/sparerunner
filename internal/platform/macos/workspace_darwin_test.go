//go:build darwin

package macos

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/genm/sparerunner/internal/runner"
)

func TestOSWorkspacePinsIdentityAndRemovesPreparedTree(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("this test exercises same-identity chown without root")
	}
	rootPath, executions, name := testRuntimeTree(t)
	uid, gid := os.Geteuid(), os.Getegid()
	workspace := NewOSWorkspace(uid, gid, uid, gid)

	root, err := os.OpenRoot(executions)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ref, err := workspace.Prepare(context.Background(), root, name)
	if err != nil || ref.Backend != WorkspaceBackend || ref.OwnerID == "" {
		t.Fatalf("Prepare=%#v err=%v", ref, err)
	}
	observed, err := workspace.Observe(context.Background(), root, name)
	if err != nil || observed != ref {
		t.Fatalf("Observe=%#v err=%v", observed, err)
	}
	pinned, err := workspace.PinLaunch(
		context.Background(),
		filepath.Join(executions, name),
		ref,
	)
	if err != nil || pinned.Directory() == nil || pinned.Executable() == nil {
		t.Fatalf("PinLaunch=%v err=%v", pinned, err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Remove(context.Background(), root, name); err != nil {
		t.Fatal(err)
	}
	if absent, err := workspace.Absent(context.Background(), root, name); err != nil || !absent {
		t.Fatalf("Absent=%v err=%v", absent, err)
	}
	if _, err := os.Stat(rootPath); err != nil {
		t.Fatalf("runtime root was removed: %v", err)
	}
}

func TestOSWorkspaceRejectsSymlinkEscapeWithoutChangingTarget(t *testing.T) {
	_, executions, name := testRuntimeTree(t)
	external := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(external, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(executions, name, "escape")); err != nil {
		t.Fatal(err)
	}
	uid, gid := os.Geteuid(), os.Getegid()
	workspace := NewOSWorkspace(uid, gid, uid, gid)
	root, err := os.OpenRoot(executions)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := workspace.Prepare(context.Background(), root, name); !errors.Is(
		err,
		runner.ErrStrongOwnershipUnavailable,
	) {
		t.Fatalf("Prepare error=%v", err)
	}
	contents, err := os.ReadFile(external)
	if err != nil || string(contents) != "outside" {
		t.Fatalf("external target changed: %q err=%v", contents, err)
	}
}

func TestOSWorkspaceRejectsReplacedExecutionRoot(t *testing.T) {
	_, executions, name := testRuntimeTree(t)
	uid, gid := os.Geteuid(), os.Getegid()
	workspace := NewOSWorkspace(uid, gid, uid, gid)
	root, err := os.OpenRoot(executions)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	replacement := executions + "-replacement"
	if err := os.Rename(executions, replacement); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(executions, 0o711); err != nil {
		t.Fatal(err)
	}
	if _, err := workspace.Prepare(context.Background(), root, name); !errors.Is(
		err,
		runner.ErrStrongOwnershipUnavailable,
	) {
		t.Fatalf("Prepare error=%v", err)
	}
}

func testRuntimeTree(t *testing.T) (string, string, string) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("darwin workspace test")
	}
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rootPath := filepath.Join(base, "runtime")
	executions := filepath.Join(rootPath, "executions")
	if err := os.MkdirAll(executions, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(rootPath, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(executions, 0o711); err != nil {
		t.Fatal(err)
	}
	name := strings.Repeat("a", 64)
	workspace := filepath.Join(executions, name)
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(workspace, "run.sh"),
		[]byte("#!/bin/bash\nexit 0\n"),
		0o700,
	); err != nil {
		t.Fatal(err)
	}
	return rootPath, executions, name
}
