//go:build windows

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/genm/sparerunner/internal/app"
	platformwindows "github.com/genm/sparerunner/internal/platform/windows"
	"github.com/genm/sparerunner/internal/runner"
	"github.com/spf13/cobra"
	"golang.org/x/sys/windows/svc"
)

const (
	defaultWindowsAgentService          = "TewakeAgent"
	defaultWindowsRunnerIdentityService = "TewakeRunnerIdentity"
)

func runPlatformLauncherHelper([]string) (bool, error) {
	return false, nil
}

func defaultNativeRunnerOptions() nativeRunnerOptions {
	programData := os.Getenv("ProgramData")
	if !filepath.IsAbs(programData) {
		programData = `C:\ProgramData`
	}
	root := filepath.Join(programData, "Tewake")
	return nativeRunnerOptions{
		CacheRoot:             filepath.Join(root, "cache"),
		RuntimeRoot:           filepath.Join(root, "runtime"),
		RunnerIdentityService: defaultWindowsRunnerIdentityService,
	}
}

func platformCommandRuntime(
	options nativeRunnerOptions,
) (func(context.Context, *app.AgentState) (*app.AgentCommandRuntime, error), error) {
	if options.SharedRunnerIdentity {
		return nil, errors.New("--allow-shared-runner-identity is only supported on Linux")
	}
	if !filepath.IsAbs(options.CacheRoot) ||
		!filepath.IsAbs(options.RuntimeRoot) ||
		options.RunnerIdentityService == "" {
		if options.Required {
			return nil, errors.New("Windows native runner configuration is incomplete")
		}
		return nil, nil
	}
	build := func(
		ctx context.Context,
		state *app.AgentState,
	) (*app.AgentCommandRuntime, error) {
		if err := runner.ValidateCacheRoot(options.CacheRoot); err != nil {
			return nil, err
		}
		pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
		if err != nil {
			return nil, err
		}
		cache := runner.Cache{
			Root:    options.CacheRoot,
			Fetcher: runner.NewHTTPFetcher(),
		}
		if err := prewarmWindowsPackage(ctx, cache, pkg); err != nil {
			return nil, err
		}
		jobRuntime, runnerSID, err := platformwindows.NewJobRuntime(
			ctx,
			options.RuntimeRoot,
			options.RunnerIdentityService,
		)
		if err != nil {
			return nil, err
		}
		workspace, err := platformwindows.NewOSWorkspace(
			options.RuntimeRoot,
			jobRuntime.ServiceSID(),
			runnerSID,
		)
		if err != nil {
			_ = jobRuntime.Close()
			return nil, err
		}
		adapter, err := platformwindows.New(jobRuntime, workspace)
		if err != nil {
			_ = jobRuntime.Close()
			return nil, err
		}
		manager, err := runner.NewManager(runner.Options{
			RuntimeRoot: options.RuntimeRoot,
			Cache:       cache,
			Journal:     state.Store.RunnerJournal(),
			Supervisor:  adapter,
			Cleaner:     adapter,
		})
		if err != nil {
			_ = jobRuntime.Close()
			return nil, err
		}
		lifecycle, err := bindNativeRunnerCredential(manager, state.CredentialReady)
		if err != nil {
			_ = jobRuntime.Close()
			return nil, err
		}
		commandRuntime, err := app.NewAgentCommandRuntime(
			state.NodeID,
			state.Store,
			lifecycle,
			pkg,
		)
		if err != nil {
			_ = jobRuntime.Close()
			return nil, err
		}
		return commandRuntime, nil
	}
	return optionalWindowsNativeRunnerFactory(options.Required, build), nil
}

func optionalWindowsNativeRunnerFactory(
	required bool,
	build func(context.Context, *app.AgentState) (*app.AgentCommandRuntime, error),
) func(context.Context, *app.AgentState) (*app.AgentCommandRuntime, error) {
	return func(
		ctx context.Context,
		state *app.AgentState,
	) (*app.AgentCommandRuntime, error) {
		commandRuntime, err := build(ctx, state)
		if err != nil && !required {
			return nil, nil
		}
		return commandRuntime, err
	}
}

type windowsPackageCache interface {
	Ensure(context.Context, runner.Package) (runner.PreparedPackage, error)
}

func prewarmWindowsPackage(
	ctx context.Context,
	cache windowsPackageCache,
	pkg runner.Package,
) error {
	if cache == nil {
		return runner.ErrPackageIntegrity
	}
	prepared, err := cache.Ensure(ctx, pkg)
	if err != nil {
		return err
	}
	if prepared == nil {
		return runner.ErrPackageIntegrity
	}
	return prepared.Close()
}

func platformCommands() []*cobra.Command {
	var (
		role              string
		serviceName       string
		stateDirectory    string
		connectionTimeout time.Duration
		reconnectDelay    time.Duration
	)
	native := defaultNativeRunnerOptions()
	command := &cobra.Command{
		Use:    "windows-service",
		Short:  "Run under the Windows Service Control Manager",
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(command *cobra.Command, _ []string) error {
			switch role {
			case "agent":
				directory, err := resolveAgentStateDirectory(stateDirectory)
				if err != nil {
					return err
				}
				factory, err := platformCommandRuntime(native)
				if err != nil {
					return err
				}
				if serviceName == "" {
					serviceName = defaultWindowsAgentService
				}
				return svc.Run(serviceName, &agentServiceHandler{
					parent: command.Context(),
					options: app.AgentServeOptions{
						StateDirectory:    directory,
						ConnectionTimeout: connectionTimeout,
						ReconnectDelay:    reconnectDelay,
						CommandRuntime:    factory,
					},
					bootstrap: receiveWindowsBootstrapRequest,
				})
			case "runner-identity":
				if serviceName == "" {
					serviceName = defaultWindowsRunnerIdentityService
				}
				return svc.Run(serviceName, &runnerIdentityServiceHandler{})
			default:
				return errors.New("Windows service role is invalid")
			}
		},
	}
	command.Flags().StringVar(&role, "role", "", "fixed service role")
	command.Flags().StringVar(&serviceName, "service-name", "", "SCM service name")
	command.Flags().StringVar(&stateDirectory, "state-dir", "", "agent state directory")
	command.Flags().DurationVar(
		&connectionTimeout,
		"connection-timeout",
		app.DefaultConnectTimeout,
		"controller connection deadline",
	)
	command.Flags().DurationVar(
		&reconnectDelay,
		"reconnect-delay",
		app.DefaultReconnectDelay,
		"delay between reconnect attempts",
	)
	command.Flags().StringVar(&native.CacheRoot, "cache-root", native.CacheRoot, "verified runner package cache")
	command.Flags().StringVar(&native.RuntimeRoot, "runtime-root", native.RuntimeRoot, "native runner execution root")
	command.Flags().StringVar(
		&native.RunnerIdentityService,
		"runner-identity-service",
		native.RunnerIdentityService,
		"dedicated runner token service",
	)
	command.Flags().BoolVar(
		&native.Required,
		"require-native-runner",
		true,
		"fail startup unless the native runner boundary is available",
	)
	return []*cobra.Command{command}
}

type agentServiceHandler struct {
	parent    context.Context
	options   app.AgentServeOptions
	bootstrap windowsAgentBootstrap
	serve     func(
		context.Context,
		app.AgentServeOptions,
		windowsAgentBootstrap,
	) error
}

func (handler *agentServiceHandler) Execute(
	_ []string,
	requests <-chan svc.ChangeRequest,
	changes chan<- svc.Status,
) (bool, uint32) {
	parent := handler.parent
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	changes <- svc.Status{State: svc.StartPending}
	done := make(chan error, 1)
	serve := handler.serve
	if serve == nil {
		serve = serveWindowsAgent
	}
	go func() {
		done <- serve(ctx, handler.options, handler.bootstrap)
	}()
	running := svc.Status{
		State:   svc.Running,
		Accepts: svc.AcceptStop | svc.AcceptShutdown,
	}
	changes <- running
	for {
		select {
		case err := <-done:
			if err != nil {
				return false, 1
			}
			return false, 0
		case request := <-requests:
			switch request.Cmd {
			case svc.Interrogate:
				changes <- running
			case svc.Stop, svc.Shutdown:
				changes <- svc.Status{State: svc.StopPending}
				cancel()
				if err := <-done; err != nil {
					return false, 1
				}
				return false, 0
			}
		}
	}
}

type windowsBootstrapRequest interface {
	JoinOptions() platformwindows.BootstrapJoinOptions
	Disconnected() <-chan struct{}
	Complete(string, error) error
}

type windowsAgentBootstrap func(
	context.Context,
) (windowsBootstrapRequest, error)

type windowsAgentOperations struct {
	initialized func(context.Context, string) (bool, error)
	join        func(context.Context, app.JoinOptions) (string, error)
	serve       func(context.Context, app.AgentServeOptions) error
}

func receiveWindowsBootstrapRequest(
	ctx context.Context,
) (windowsBootstrapRequest, error) {
	return platformwindows.ReceiveBootstrapRequest(ctx)
}

func defaultWindowsAgentOperations() windowsAgentOperations {
	return windowsAgentOperations{
		initialized: func(
			ctx context.Context,
			directory string,
		) (bool, error) {
			state, err := app.OpenAgent(ctx, directory)
			if errors.Is(err, app.ErrNotInitialized) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if err := state.Close(); err != nil {
				return false, err
			}
			return true, nil
		},
		join:  app.JoinAgent,
		serve: app.ServeAgent,
	}
}

func serveWindowsAgent(
	ctx context.Context,
	options app.AgentServeOptions,
	bootstrap windowsAgentBootstrap,
) error {
	return serveWindowsAgentWithOperations(
		ctx,
		options,
		bootstrap,
		defaultWindowsAgentOperations(),
	)
}

func serveWindowsAgentWithOperations(
	ctx context.Context,
	options app.AgentServeOptions,
	bootstrap windowsAgentBootstrap,
	operations windowsAgentOperations,
) error {
	initialized, err := operations.initialized(ctx, options.StateDirectory)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if initialized {
		return operations.serve(ctx, options)
	}
	if ctx.Err() != nil {
		return nil
	}
	if bootstrap == nil {
		return app.ErrNotInitialized
	}
	request, err := bootstrap(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if request == nil {
		return platformwindows.ErrBootstrapProtocol
	}
	joinOptions := request.JoinOptions()
	joinTimeout := joinOptions.ConnectionTimeout
	if joinTimeout <= 0 {
		joinTimeout = options.ConnectionTimeout
	}
	joinContext, cancelJoin := context.WithCancel(ctx)
	joinFinished := make(chan struct{})
	go func() {
		select {
		case <-request.Disconnected():
			cancelJoin()
		case <-joinFinished:
		}
	}()
	nodeID, joinErr := operations.join(joinContext, app.JoinOptions{
		StateDirectory:    options.StateDirectory,
		JoinCode:          joinOptions.JoinCode,
		Controller:        joinOptions.Controller,
		DiscoveryTimeout:  joinOptions.DiscoveryTimeout,
		ConnectionTimeout: joinTimeout,
	})
	joinOptions.JoinCode = ""
	close(joinFinished)
	cancelJoin()
	if joinOptions.ConnectionTimeout > 0 {
		// The initiating CLI's explicit deadline belongs to this enrollment,
		// while subsequent service reconnects keep the SCM configuration.
		options.ConnectionTimeout = joinOptions.ConnectionTimeout
	}
	ackErr := request.Complete(nodeID, joinErr)
	if joinErr != nil {
		return joinErr
	}
	if ackErr != nil {
		if ctx.Err() != nil {
			return nil
		}
		// Enrollment is durable, but the initiating CLI did not observe it.
		// Exiting non-zero lets SCM recovery restart from that durable state
		// instead of reporting a synthetic successful bootstrap.
		return ackErr
	}
	return operations.serve(ctx, options)
}

type runnerIdentityServiceHandler struct{}

func (*runnerIdentityServiceHandler) Execute(
	_ []string,
	requests <-chan svc.ChangeRequest,
	changes chan<- svc.Status,
) (bool, uint32) {
	running := svc.Status{
		State:   svc.Running,
		Accepts: svc.AcceptStop | svc.AcceptShutdown,
	}
	changes <- svc.Status{State: svc.StartPending}
	changes <- running
	for request := range requests {
		switch request.Cmd {
		case svc.Interrogate:
			changes <- running
		case svc.Stop, svc.Shutdown:
			changes <- svc.Status{State: svc.StopPending}
			return false, 0
		}
	}
	return false, 0
}

var (
	_ svc.Handler = (*agentServiceHandler)(nil)
	_ svc.Handler = (*runnerIdentityServiceHandler)(nil)
)
