//go:build darwin

package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/genm/sparerunner/internal/app"
	"github.com/genm/sparerunner/internal/runner"
)

type darwinPrewarmCache struct {
	prepared runner.PreparedPackage
	err      error
	calls    int
}

func (cache *darwinPrewarmCache) Ensure(
	context.Context,
	runner.Package,
) (runner.PreparedPackage, error) {
	cache.calls++
	return cache.prepared, cache.err
}

type darwinPreparedPackage struct {
	closed *bool
	err    error
}

func (darwinPreparedPackage) Materialize(*os.Root) error {
	return errors.New("prewarm must not materialize")
}
func (prepared darwinPreparedPackage) Close() error {
	*prepared.closed = true
	return prepared.err
}

func TestDarwinDefaultsAreCanonicalSystemPaths(t *testing.T) {
	options := defaultNativeRunnerOptions()
	if !canonicalAbsolutePath(options.CacheRoot) ||
		!canonicalAbsolutePath(options.RuntimeRoot) ||
		options.SupervisorSocket != "" ||
		!strings.HasPrefix(options.RuntimeRoot, "/Library/") {
		t.Fatalf("defaults=%#v", options)
	}
}

func TestDarwinSelectsBothOfficialArchitectures(t *testing.T) {
	amd64, err := darwinOfficialPackage("amd64")
	if err != nil {
		t.Fatal(err)
	}
	arm64, err := darwinOfficialPackage("arm64")
	if err != nil {
		t.Fatal(err)
	}
	if amd64.Platform.Arch != "amd64" ||
		amd64.Asset != "actions-runner-osx-x64-2.336.0.tar.gz" ||
		arm64.Platform.Arch != "arm64" ||
		arm64.Asset != "actions-runner-osx-arm64-2.336.0.tar.gz" {
		t.Fatalf("amd64=%#v arm64=%#v", amd64, arm64)
	}
	if _, err := darwinOfficialPackage("386"); !errors.Is(
		err,
		runner.ErrUnsupportedPlatform,
	) {
		t.Fatalf("unsupported architecture error=%v", err)
	}
}

func TestDarwinPrewarmRequiresAndClosesVerifiedPackage(t *testing.T) {
	closed := false
	cache := &darwinPrewarmCache{
		prepared: darwinPreparedPackage{closed: &closed},
	}
	if err := prewarmDarwinOfficialPackage(
		context.Background(),
		cache,
		runner.Package{},
	); err != nil {
		t.Fatal(err)
	}
	if cache.calls != 1 || !closed {
		t.Fatalf("calls=%d closed=%v", cache.calls, closed)
	}
	if err := prewarmDarwinOfficialPackage(
		context.Background(),
		nil,
		runner.Package{},
	); !errors.Is(err, runner.ErrPackageIntegrity) {
		t.Fatalf("nil cache error=%v", err)
	}
}

func TestOptionalDarwinRuntimeAdvertisesZeroCapacityOnFailure(t *testing.T) {
	unavailable := runner.ErrStrongOwnershipUnavailable
	build := func(
		context.Context,
		*app.AgentState,
	) (*app.AgentCommandRuntime, error) {
		return nil, unavailable
	}
	commandRuntime, err := optionalDarwinNativeRunnerFactory(false, build)(
		context.Background(),
		&app.AgentState{},
	)
	if err != nil || commandRuntime != nil {
		t.Fatalf("optional runtime=%#v err=%v", commandRuntime, err)
	}
	if _, err := optionalDarwinNativeRunnerFactory(true, build)(
		context.Background(),
		&app.AgentState{},
	); !errors.Is(err, unavailable) {
		t.Fatalf("required runtime error=%v", err)
	}
}

func TestDarwinRequiredRuntimeRejectsIgnoredSupervisorSocket(t *testing.T) {
	options := defaultNativeRunnerOptions()
	options.SupervisorSocket = "/tmp/not-a-darwin-supervisor.sock"
	options.Required = true
	if _, err := platformCommandRuntime(options); err == nil {
		t.Fatal("ignored supervisor socket was accepted")
	}
}
