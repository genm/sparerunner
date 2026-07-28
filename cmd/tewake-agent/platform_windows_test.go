//go:build windows

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/app"
	platformwindows "github.com/genm/sparerunner/internal/platform/windows"
	"github.com/genm/sparerunner/internal/runner"
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

type fakeWindowsBootstrapRequest struct {
	options      platformwindows.BootstrapJoinOptions
	disconnected chan struct{}
	complete     func(string, error) error
}

func (request *fakeWindowsBootstrapRequest) JoinOptions() platformwindows.BootstrapJoinOptions {
	return request.options
}

func (request *fakeWindowsBootstrapRequest) Disconnected() <-chan struct{} {
	return request.disconnected
}

func (request *fakeWindowsBootstrapRequest) Complete(
	nodeID string,
	err error,
) error {
	return request.complete(nodeID, err)
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
		bootstrap: func(context.Context) (windowsBootstrapRequest, error) {
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
		bootstrap: func(ctx context.Context) (windowsBootstrapRequest, error) {
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

func TestWindowsEnrollmentAckFailureExitsAndRestartUsesDurableState(
	t *testing.T,
) {
	stateDirectory := t.TempDir()
	durableState := filepath.Join(stateDirectory, "durable-enrollment")
	ackErr := platformwindows.ErrBootstrapUnavailable
	serveAfterRestart := errors.New("serve after restart")
	bootstrapCalls := 0
	joinCalls := 0
	serveCalls := 0
	disconnected := make(chan struct{})
	request := &fakeWindowsBootstrapRequest{
		options: platformwindows.BootstrapJoinOptions{
			JoinCode:          "twk_test",
			Controller:        "https://controller.example.test:7443",
			DiscoveryTimeout:  time.Second,
			ConnectionTimeout: 2 * time.Second,
		},
		disconnected: disconnected,
		complete: func(nodeID string, joinErr error) error {
			if nodeID != "durable-node" || joinErr != nil {
				t.Fatalf("completion = (%q, %v)", nodeID, joinErr)
			}
			if _, err := os.Stat(durableState); err != nil {
				t.Fatalf("completion preceded durable state: %v", err)
			}
			return ackErr
		},
	}
	operations := windowsAgentOperations{
		initialized: func(
			context.Context,
			string,
		) (bool, error) {
			_, err := os.Stat(durableState)
			if errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			return err == nil, err
		},
		join: func(
			_ context.Context,
			options app.JoinOptions,
		) (string, error) {
			joinCalls++
			if options.StateDirectory != stateDirectory ||
				options.JoinCode != request.options.JoinCode ||
				options.Controller != request.options.Controller {
				t.Fatalf("join options = %+v", options)
			}
			if err := os.WriteFile(durableState, []byte("durable"), 0o600); err != nil {
				t.Fatal(err)
			}
			return "durable-node", nil
		},
		serve: func(
			context.Context,
			app.AgentServeOptions,
		) error {
			serveCalls++
			return serveAfterRestart
		},
	}
	bootstrap := func(context.Context) (windowsBootstrapRequest, error) {
		bootstrapCalls++
		return request, nil
	}
	options := app.AgentServeOptions{StateDirectory: stateDirectory}
	if err := serveWindowsAgentWithOperations(
		context.Background(),
		options,
		bootstrap,
		operations,
	); !errors.Is(err, ackErr) {
		t.Fatalf("acknowledgement failure = %v", err)
	}
	if bootstrapCalls != 1 || joinCalls != 1 || serveCalls != 0 {
		t.Fatalf(
			"first start calls bootstrap=%d join=%d serve=%d",
			bootstrapCalls,
			joinCalls,
			serveCalls,
		)
	}
	if err := serveWindowsAgentWithOperations(
		context.Background(),
		options,
		func(context.Context) (windowsBootstrapRequest, error) {
			t.Fatal("restart requested enrollment after durable state")
			return nil, nil
		},
		operations,
	); !errors.Is(err, serveAfterRestart) {
		t.Fatalf("restart serve error = %v", err)
	}
	if bootstrapCalls != 1 || joinCalls != 1 || serveCalls != 1 {
		t.Fatalf(
			"restart calls bootstrap=%d join=%d serve=%d",
			bootstrapCalls,
			joinCalls,
			serveCalls,
		)
	}
}

func TestAgentServiceReportsHelperFailureToSCM(t *testing.T) {
	helperErr := platformwindows.ErrBootstrapUnavailable
	changes := make(chan svc.Status, 4)
	done := make(chan uint32, 1)
	handler := &agentServiceHandler{
		parent: context.Background(),
		options: app.AgentServeOptions{
			StateDirectory: t.TempDir(),
		},
		serve: func(
			context.Context,
			app.AgentServeOptions,
			windowsAgentBootstrap,
		) error {
			return helperErr
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
			t.Fatal("helper failure reported SCM success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent service hid helper failure")
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
