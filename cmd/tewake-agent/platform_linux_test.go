//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/genm/tewake/internal/app"
	"github.com/genm/tewake/internal/runner"
)

type prewarmTestCache struct {
	prepared runner.PreparedPackage
	err      error
	calls    int
}

func (cache *prewarmTestCache) Ensure(context.Context, runner.Package) (runner.PreparedPackage, error) {
	cache.calls++
	return cache.prepared, cache.err
}

type prewarmTestPrepared struct {
	closeErr error
	closed   *bool
}

func (prewarmTestPrepared) Materialize(*os.Root) error {
	return errors.New("prewarm must not materialize the package")
}

func (prepared prewarmTestPrepared) Close() error {
	*prepared.closed = true
	return prepared.closeErr
}

func TestPrewarmOfficialPackageRequiresVerifiedCapabilityAndClosesIt(t *testing.T) {
	closed := false
	cache := &prewarmTestCache{
		prepared: prewarmTestPrepared{closed: &closed},
	}
	if err := prewarmOfficialPackage(context.Background(), cache, runner.Package{}); err != nil {
		t.Fatal(err)
	}
	if cache.calls != 1 || !closed {
		t.Fatalf("calls=%d closed=%v", cache.calls, closed)
	}
}

func TestPrewarmOfficialPackageFailsClosed(t *testing.T) {
	fetchErr := errors.New("fetch failed")
	if err := prewarmOfficialPackage(context.Background(), &prewarmTestCache{err: fetchErr}, runner.Package{}); !errors.Is(err, fetchErr) {
		t.Fatalf("fetch error=%v", err)
	}
	closed := false
	closeErr := errors.New("close failed")
	cache := &prewarmTestCache{
		prepared: prewarmTestPrepared{closed: &closed, closeErr: closeErr},
	}
	if err := prewarmOfficialPackage(context.Background(), cache, runner.Package{}); !errors.Is(err, closeErr) {
		t.Fatalf("close error=%v", err)
	}
	if !closed {
		t.Fatal("failed close path did not release the capability")
	}
}

func TestOptionalNativeRunnerFactoryDegradesToZeroCapacity(t *testing.T) {
	unavailable := errors.New("supervisor unavailable")
	build := func(context.Context, *app.AgentState) (*app.AgentCommandRuntime, error) {
		return nil, unavailable
	}
	runtime, err := optionalNativeRunnerFactory(false, build)(
		context.Background(),
		&app.AgentState{},
	)
	if err != nil || runtime != nil {
		t.Fatalf("optional runtime = (%#v, %v), want (nil, nil)", runtime, err)
	}
	if _, err := optionalNativeRunnerFactory(true, build)(
		context.Background(),
		&app.AgentState{},
	); !errors.Is(err, unavailable) {
		t.Fatalf("required runtime error = %v, want unavailable", err)
	}
}

// The shared-identity runner drops UID isolation, so it must never appear
// merely because the privileged supervisor is missing. Only the explicit flag
// selects it.
func TestSharedRunnerIdentityIsNeverTheDefault(t *testing.T) {
	options := defaultNativeRunnerOptions()
	if options.SharedRunnerIdentity {
		t.Fatal("the shared-identity runner must never be a default")
	}
}

// Accepting both the flag and the privileged supervisor socket and silently
// picking one would leave the owner believing the node is isolated.
func TestSharedRunnerIdentityRejectsPrivilegedOptions(t *testing.T) {
	for _, name := range []string{"supervisor-socket", "runner-identity-service"} {
		options := defaultNativeRunnerOptions()
		options.SharedRunnerIdentity = true
		options.ExplicitFlags = map[string]bool{name: true}
		if _, err := platformCommandRuntime(options); err == nil {
			t.Fatalf("--%s combined with --allow-shared-runner-identity must be rejected", name)
		}
	}
}

func TestUserDataRootPrefersXDGDataHome(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", "/home/example/.local/share")
	root, err := userDataRoot()
	if err != nil || root != "/home/example/.local/share/tewake" {
		t.Fatalf("userDataRoot = %q, %v", root, err)
	}
	t.Setenv("XDG_DATA_HOME", "relative/share")
	if _, err := userDataRoot(); err == nil {
		t.Fatal("a relative XDG_DATA_HOME must be rejected")
	}
	t.Setenv("XDG_DATA_HOME", "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	root, err = userDataRoot()
	if err != nil || root != filepath.Join(home, ".local", "share", "tewake") {
		t.Fatalf("userDataRoot = %q, %v", root, err)
	}
}
