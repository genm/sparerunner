//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	"github.com/genm/sparerunner/internal/app"
	"github.com/genm/sparerunner/internal/platform/linux"
	"github.com/genm/sparerunner/internal/runner"
	"github.com/spf13/cobra"
)

const defaultLinuxRunnerUser = "tewake-runner-0"

func runPlatformLauncherHelper(args []string) (bool, error) {
	return linux.RunExecLauncherHelper(args)
}

func defaultNativeRunnerOptions() nativeRunnerOptions {
	return nativeRunnerOptions{
		CacheRoot:        "/var/cache/tewake-agent",
		RuntimeRoot:      "/var/lib/tewake-runtime",
		SupervisorSocket: "/run/tewake-supervisor/supervisor.sock",
	}
}

func platformCommandRuntime(options nativeRunnerOptions) (func(context.Context, *app.AgentState) (*app.AgentCommandRuntime, error), error) {
	if options.SharedRunnerIdentity {
		return sharedIdentityCommandRuntime(options)
	}
	if !filepath.IsAbs(options.CacheRoot) || !filepath.IsAbs(options.RuntimeRoot) ||
		!filepath.IsAbs(options.SupervisorSocket) {
		if options.Required {
			return nil, errors.New("native runner paths must be absolute")
		}
		return nil, nil
	}
	build := func(ctx context.Context, state *app.AgentState) (*app.AgentCommandRuntime, error) {
		agent := linux.RunnerIdentity{UID: os.Geteuid(), GID: os.Getegid()}
		slot, err := lookupLinuxIdentity(defaultLinuxRunnerUser)
		if err != nil {
			return nil, err
		}
		policy := linux.HelperPolicy{
			SocketPath:  options.SupervisorSocket,
			RuntimeRoot: options.RuntimeRoot,
			CacheRoot:   options.CacheRoot,
			AgentUID:    agent.UID,
			AgentGID:    agent.GID,
			RunnerUID:   slot.UID,
			RunnerGID:   slot.GID,
		}
		helper, err := linux.NewHelperClient(policy)
		if err != nil {
			return nil, err
		}
		if err := ensurePrivateDirectory(options.CacheRoot); err != nil {
			return nil, err
		}
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
		// Capacity is not advertised until the exact pinned archive is locally
		// verified and the root Helper has adopted its immutable authority.
		if err := prewarmOfficialPackage(ctx, cache, pkg); err != nil {
			return nil, err
		}
		if err := helper.ValidateRuntimeRoot(ctx, options.RuntimeRoot); err != nil {
			return nil, err
		}
		adapter, err := linux.New(linux.Config{
			Identity: linux.StaticIdentity(slot),
		}, helper, helper)
		if err != nil {
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
			return nil, err
		}
		lifecycle, err := bindNativeRunnerCredential(manager, state.CredentialReady)
		if err != nil {
			return nil, err
		}
		return app.NewAgentCommandRuntime(state.NodeID, state.Store, lifecycle, pkg)
	}
	return optionalNativeRunnerFactory(options.Required, build), nil
}

func optionalNativeRunnerFactory(
	required bool,
	build func(context.Context, *app.AgentState) (*app.AgentCommandRuntime, error),
) func(context.Context, *app.AgentState) (*app.AgentCommandRuntime, error) {
	return func(
		ctx context.Context,
		state *app.AgentState,
	) (*app.AgentCommandRuntime, error) {
		runtime, err := build(ctx, state)
		if err != nil && !required {
			// A missing optional Supervisor, slot identity, or package authority
			// is safe only as zero capacity. The packaged systemd service sets
			// required=true and therefore still fails closed and restarts.
			return nil, nil
		}
		return runtime, err
	}
}

// sharedIdentityCommandRuntime builds the opt-in native runner that executes
// jobs under this Agent's own Unix credential. It is selected only by an
// explicit --allow-shared-runner-identity and never as a fallback: when the
// privileged Supervisor is missing, the node still advertises zero capacity.
//
// The one property it drops is UID separation between the Agent and the job.
// Descendant ownership, the start fence, exec-boundary workspace verification,
// one-shot JIT delivery, and verified cleanup are unchanged, and construction
// fails closed if the cgroup delegation that backs them cannot be proven.
func sharedIdentityCommandRuntime(options nativeRunnerOptions) (func(context.Context, *app.AgentState) (*app.AgentCommandRuntime, error), error) {
	// The privileged Supervisor socket names a boundary this mode does not have.
	// Accepting both and picking one silently would leave the owner believing the
	// node is isolated when it is not.
	for _, name := range []string{"supervisor-socket", "runner-identity-service"} {
		if options.ExplicitFlags[name] {
			return nil, fmt.Errorf(
				"--allow-shared-runner-identity cannot be combined with --%s: "+
					"the shared-identity runner has no privileged supervisor boundary",
				name,
			)
		}
	}
	dataRoot, err := userDataRoot()
	if err != nil {
		return nil, err
	}
	cacheRoot := filepath.Join(dataRoot, "cache")
	if options.ExplicitFlags["cache-root"] {
		cacheRoot = options.CacheRoot
	}
	runtimeRoot := filepath.Join(dataRoot, "runtime")
	if options.ExplicitFlags["runtime-root"] {
		runtimeRoot = options.RuntimeRoot
	}
	fenceRoot := filepath.Join(dataRoot, "fences")
	if !filepath.IsAbs(cacheRoot) || !filepath.IsAbs(runtimeRoot) {
		return nil, errors.New("native runner paths must be absolute")
	}
	cacheRoot = filepath.Clean(cacheRoot)
	runtimeRoot = filepath.Clean(runtimeRoot)

	build := func(ctx context.Context, state *app.AgentState) (*app.AgentCommandRuntime, error) {
		identity := linux.RunnerIdentity{UID: os.Geteuid(), GID: os.Getegid()}
		if identity.UID <= 0 || identity.GID <= 0 {
			return nil, errors.New(
				"shared runner identity requires an unprivileged agent; " +
					"run the root supervisor for a dedicated runner account",
			)
		}
		workspace, err := linux.NewRootlessWorkspace(identity.UID, identity.GID)
		if err != nil {
			return nil, err
		}
		for _, directory := range []string{
			dataRoot,
			cacheRoot,
			runtimeRoot,
			filepath.Join(runtimeRoot, "executions"),
			fenceRoot,
		} {
			if err := ensurePrivateDirectory(directory); err != nil {
				return nil, err
			}
		}
		if err := runner.ValidateCacheRoot(cacheRoot); err != nil {
			return nil, err
		}
		pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
		if err != nil {
			return nil, err
		}
		cache := runner.Cache{Root: cacheRoot, Fetcher: runner.NewHTTPFetcher()}
		// Capacity is not advertised until the exact pinned archive is locally
		// verified, exactly as in the privileged mode.
		if err := prewarmOfficialPackage(ctx, cache, pkg); err != nil {
			return nil, err
		}
		executable, err := os.Executable()
		if err != nil {
			return nil, runner.ErrStrongOwnershipUnavailable
		}
		launcher, err := linux.NewSharedIdentityLauncher(executable, identity)
		if err != nil {
			return nil, err
		}
		nativeRuntime, err := linux.NewRootlessRuntime(fenceRoot, runtimeRoot, launcher, workspace)
		if err != nil {
			return nil, err
		}
		adapter, err := linux.NewRootless(nativeRuntime, workspace, 0)
		if err != nil {
			return nil, err
		}
		manager, err := runner.NewManager(runner.Options{
			RuntimeRoot: runtimeRoot,
			Cache:       cache,
			Journal:     state.Store.RunnerJournal(),
			Supervisor:  adapter,
			Cleaner:     adapter,
		})
		if err != nil {
			return nil, err
		}
		lifecycle, err := bindNativeRunnerCredential(manager, state.CredentialReady)
		if err != nil {
			return nil, err
		}
		return app.NewAgentCommandRuntime(state.NodeID, state.Store, lifecycle, pkg)
	}
	return optionalNativeRunnerFactory(options.Required, build), nil
}

// userDataRoot resolves the owner's own data directory. The shared-identity
// mode keeps every durable root under it rather than under /var, because it has
// no privileged component that could create or own a system path.
func userDataRoot() (string, error) {
	if explicit := os.Getenv("XDG_DATA_HOME"); explicit != "" {
		if !filepath.IsAbs(explicit) {
			return "", errors.New("XDG_DATA_HOME must be an absolute path")
		}
		return filepath.Join(filepath.Clean(explicit), "tewake"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || !filepath.IsAbs(home) {
		return "", errors.New("resolve user home directory for shared runner identity roots")
	}
	return filepath.Join(filepath.Clean(home), ".local", "share", "tewake"), nil
}

type officialPackageCache interface {
	Ensure(context.Context, runner.Package) (runner.PreparedPackage, error)
}

func prewarmOfficialPackage(ctx context.Context, cache officialPackageCache, pkg runner.Package) error {
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
	var socketPath, runtimeRoot, cacheRoot, fenceRoot, runnerUser, agentUser string
	command := &cobra.Command{
		Use:    "supervisor",
		Short:  "Run the local privileged native runner supervisor",
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(command *cobra.Command, _ []string) error {
			if os.Geteuid() != 0 {
				return errors.New("native runner supervisor requires root")
			}
			agent, err := lookupLinuxIdentity(agentUser)
			if err != nil {
				return err
			}
			slot, err := lookupLinuxIdentity(runnerUser)
			if err != nil {
				return err
			}
			policy := linux.HelperPolicy{
				SocketPath:  socketPath,
				RuntimeRoot: runtimeRoot,
				CacheRoot:   cacheRoot,
				AgentUID:    agent.UID,
				AgentGID:    agent.GID,
				RunnerUID:   slot.UID,
				RunnerGID:   slot.GID,
			}
			executable, err := os.Executable()
			if err != nil {
				return runner.ErrStrongOwnershipUnavailable
			}
			launcher, err := linux.NewExecLauncher(executable)
			if err != nil {
				return err
			}
			nativeRuntime, err := linux.NewSystemdFileRuntime(fenceRoot, launcher)
			if err != nil {
				return err
			}
			pkg, err := runner.OfficialPackage(runner.CurrentPlatform())
			if err != nil {
				return err
			}
			workspace, err := linux.NewVerifiedOSWorkspace(
				agent.UID, agent.GID, slot.UID, slot.GID, cacheRoot, pkg,
			)
			if err != nil {
				return err
			}
			server, err := linux.NewHelperServer(policy, nativeRuntime, workspace)
			if err != nil {
				return err
			}
			listener, err := linux.ListenHelperSocket(policy)
			if err != nil {
				return err
			}
			defer listener.Close()
			return server.Serve(command.Context(), listener)
		},
	}
	command.Flags().StringVar(&socketPath, "socket", "/run/tewake-supervisor/supervisor.sock", "root-created local Agent socket")
	command.Flags().StringVar(&runtimeRoot, "runtime-root", "/var/lib/tewake-runtime", "root-owned runner execution root")
	command.Flags().StringVar(&cacheRoot, "cache-root", "/var/cache/tewake-agent", "Agent-owned official runner archive cache")
	command.Flags().StringVar(&fenceRoot, "fence-root", "/var/lib/tewake-supervisor/fences", "root-only durable fence directory")
	command.Flags().StringVar(&runnerUser, "runner-user", defaultLinuxRunnerUser, "dedicated native runner slot account")
	command.Flags().StringVar(&agentUser, "agent-user", "tewake-agent", "unprivileged Agent account")
	return []*cobra.Command{command}
}

func lookupLinuxIdentity(name string) (linux.RunnerIdentity, error) {
	if name == "" {
		return linux.RunnerIdentity{}, runner.ErrStrongOwnershipUnavailable
	}
	account, err := user.Lookup(name)
	if err != nil {
		return linux.RunnerIdentity{}, runner.ErrStrongOwnershipUnavailable
	}
	uid, uidErr := strconv.Atoi(account.Uid)
	gid, gidErr := strconv.Atoi(account.Gid)
	if uidErr != nil || gidErr != nil || uid <= 0 || gid <= 0 {
		return linux.RunnerIdentity{}, runner.ErrStrongOwnershipUnavailable
	}
	return linux.RunnerIdentity{UID: uid, GID: gid}, nil
}

func ensurePrivateDirectory(path string) error {
	if !filepath.IsAbs(path) {
		return runner.ErrPackageIntegrity
	}
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return runner.ErrPackageIntegrity
		}
	} else if err != nil {
		return runner.ErrPackageIntegrity
	}
	return nil
}
