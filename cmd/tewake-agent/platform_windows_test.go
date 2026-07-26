//go:build windows

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/genm/tewake/internal/app"
	platformwindows "github.com/genm/tewake/internal/platform/windows"
	"github.com/genm/tewake/internal/runner"
	"golang.org/x/sys/windows/svc"
)

type windowsPrewarmCache struct {
	prepared runner.PreparedPackage
	err      error
	calls    int
}

func (cache *windowsPrewarmCache) Ensure(
	context.Context,
	runner.Package,
) (runner.PreparedPackage, error) {
	cache.calls++
	return cache.prepared, cache.err
}

type windowsPrewarmPrepared struct {
	closed   *bool
	closeErr error
}

func (windowsPrewarmPrepared) Materialize(*os.Root) error {
	return errors.New("prewarm must not materialize")
}
func (prepared windowsPrewarmPrepared) Close() error {
	*prepared.closed = true
	return prepared.closeErr
}

func TestPrewarmWindowsPackageClosesVerifiedCapability(t *testing.T) {
	closed := false
	cache := &windowsPrewarmCache{
		prepared: windowsPrewarmPrepared{closed: &closed},
	}
	if err := prewarmWindowsPackage(
		context.Background(),
		cache,
		runner.Package{},
	); err != nil {
		t.Fatal(err)
	}
	if cache.calls != 1 || !closed {
		t.Fatalf("calls=%d closed=%v", cache.calls, closed)
	}
}

func TestPrewarmWindowsPackageFailsClosed(t *testing.T) {
	fetchErr := errors.New("fetch failed")
	if err := prewarmWindowsPackage(
		context.Background(),
		&windowsPrewarmCache{err: fetchErr},
		runner.Package{},
	); !errors.Is(err, fetchErr) {
		t.Fatalf("fetch error = %v", err)
	}
	closed := false
	closeErr := errors.New("close failed")
	if err := prewarmWindowsPackage(
		context.Background(),
		&windowsPrewarmCache{
			prepared: windowsPrewarmPrepared{
				closed:   &closed,
				closeErr: closeErr,
			},
		},
		runner.Package{},
	); !errors.Is(err, closeErr) {
		t.Fatalf("close error = %v", err)
	}
	if !closed {
		t.Fatal("verified package capability was not closed")
	}
}

func TestOptionalWindowsRuntimeWithdrawsCapacityButRequiredFails(t *testing.T) {
	unavailable := errors.New("runner identity service unavailable")
	build := func(
		context.Context,
		*app.AgentState,
	) (*app.AgentCommandRuntime, error) {
		return nil, unavailable
	}
	commandRuntime, err := optionalWindowsNativeRunnerFactory(false, build)(
		context.Background(),
		&app.AgentState{},
	)
	if err != nil || commandRuntime != nil {
		t.Fatalf("optional runtime = (%#v, %v)", commandRuntime, err)
	}
	if _, err := optionalWindowsNativeRunnerFactory(true, build)(
		context.Background(),
		&app.AgentState{},
	); !errors.Is(err, unavailable) {
		t.Fatalf("required runtime error = %v", err)
	}
}

func TestDefaultWindowsPathsAreAbsoluteAndUseDedicatedIdentityService(t *testing.T) {
	options := defaultNativeRunnerOptions()
	if !filepath.IsAbs(options.CacheRoot) ||
		!filepath.IsAbs(options.RuntimeRoot) ||
		options.RunnerIdentityService != defaultWindowsRunnerIdentityService {
		t.Fatalf("default options = %+v", options)
	}
}

func TestRunnerIdentityServiceReportsRunningAndStops(t *testing.T) {
	requests := make(chan svc.ChangeRequest, 2)
	changes := make(chan svc.Status, 4)
	done := make(chan uint32, 1)
	go func() {
		_, code := (&runnerIdentityServiceHandler{}).Execute(nil, requests, changes)
		done <- code
	}()
	if status := receiveServiceStatus(t, changes); status.State != svc.StartPending {
		t.Fatalf("first status = %+v", status)
	}
	if status := receiveServiceStatus(t, changes); status.State != svc.Running ||
		status.Accepts&(svc.AcceptStop|svc.AcceptShutdown) == 0 {
		t.Fatalf("running status = %+v", status)
	}
	requests <- svc.ChangeRequest{Cmd: svc.Interrogate}
	if status := receiveServiceStatus(t, changes); status.State != svc.Running {
		t.Fatalf("interrogate status = %+v", status)
	}
	requests <- svc.ChangeRequest{Cmd: svc.Stop}
	if status := receiveServiceStatus(t, changes); status.State != svc.StopPending {
		t.Fatalf("stop status = %+v", status)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("service exit code = %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner identity service did not stop")
	}
}

func TestAgentServiceFailsClosedWhenBootstrapFails(t *testing.T) {
	bootstrapErr := errors.New("bootstrap pipe unavailable")
	changes := make(chan svc.Status, 4)
	done := make(chan uint32, 1)
	handler := &agentServiceHandler{
		parent: context.Background(),
		options: app.AgentServeOptions{
			StateDirectory: filepath.Join(t.TempDir(), "missing"),
		},
		bootstrap: func(context.Context) (*platformwindows.BootstrapRequest, error) {
			return nil, bootstrapErr
		},
	}
	go func() {
		_, code := handler.Execute(nil, make(chan svc.ChangeRequest), changes)
		done <- code
	}()
	if status := receiveServiceStatus(t, changes); status.State != svc.StartPending {
		t.Fatalf("first status = %+v", status)
	}
	if status := receiveServiceStatus(t, changes); status.State != svc.Running {
		t.Fatalf("running status = %+v", status)
	}
	select {
	case code := <-done:
		if code == 0 {
			t.Fatal("missing credential state reported service success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent service hid startup failure")
	}
}

func TestAgentServiceStopCancelsBootstrapWait(t *testing.T) {
	changes := make(chan svc.Status, 4)
	requests := make(chan svc.ChangeRequest, 1)
	done := make(chan uint32, 1)
	handler := &agentServiceHandler{
		parent: context.Background(),
		options: app.AgentServeOptions{
			StateDirectory: filepath.Join(t.TempDir(), "missing"),
		},
		bootstrap: func(ctx context.Context) (*platformwindows.BootstrapRequest, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	go func() {
		_, code := handler.Execute(nil, requests, changes)
		done <- code
	}()
	if status := receiveServiceStatus(t, changes); status.State != svc.StartPending {
		t.Fatalf("first status = %+v", status)
	}
	if status := receiveServiceStatus(t, changes); status.State != svc.Running {
		t.Fatalf("running status = %+v", status)
	}
	requests <- svc.ChangeRequest{Cmd: svc.Stop}
	if status := receiveServiceStatus(t, changes); status.State != svc.StopPending {
		t.Fatalf("stop status = %+v", status)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("service cancellation exit code = %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent service did not cancel bootstrap wait")
	}
}

func receiveServiceStatus(t *testing.T, changes <-chan svc.Status) svc.Status {
	t.Helper()
	select {
	case status := <-changes:
		return status
	case <-time.After(2 * time.Second):
		t.Fatal("service status was not reported")
		return svc.Status{}
	}
}
