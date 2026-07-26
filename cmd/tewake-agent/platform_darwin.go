//go:build darwin

package main

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	"github.com/genm/tewake/internal/app"
	"github.com/genm/tewake/internal/platform/macos"
	"github.com/genm/tewake/internal/runner"
	"github.com/spf13/cobra"
)

const (
	defaultMacOSRunnerUser = "tewake-runner-0"
	defaultMacOSFenceRoot  = "/Library/Application Support/Tewake/fences"
)

func runPlatformLauncherHelper(args []string) (bool, error) {
	return macos.RunExecLauncherHelper(args)
}

func defaultNativeRunnerOptions() nativeRunnerOptions {
	return nativeRunnerOptions{
		CacheRoot:   "/Library/Caches/com.genm.tewake/runner",
		RuntimeRoot: "/Library/Application Support/Tewake/runtime",
	}
}

func platformCommandRuntime(
	options nativeRunnerOptions,
) (func(context.Context, *app.AgentState) (*app.AgentCommandRuntime, error), error) {
	if !canonicalAbsolutePath(options.CacheRoot) ||
		!canonicalAbsolutePath(options.RuntimeRoot) ||
		options.SupervisorSocket != "" {
		if options.Required {
			return nil, errors.New("macOS native runner paths are invalid")
		}
		return nil, nil
	}
	build := func(
		ctx context.Context,
		state *app.AgentState,
	) (*app.AgentCommandRuntime, error) {
		if os.Geteuid() != 0 {
			return nil, runner.ErrStrongOwnershipUnavailable
		}
		slot, err := lookupMacOSIdentity(defaultMacOSRunnerUser)
		if err != nil {
			return nil, err
		}
		if err := ensureDarwinPrivateDirectory(options.CacheRoot); err != nil {
			return nil, err
		}
		if err := runner.ValidateCacheRoot(options.CacheRoot); err != nil {
			return nil, err
		}
		pkg, err := darwinOfficialPackage(runner.CurrentPlatform().Arch)
		if err != nil {
			return nil, err
		}
		cache := runner.Cache{
			Root:    options.CacheRoot,
			Fetcher: runner.NewHTTPFetcher(),
		}
		if err := prewarmDarwinOfficialPackage(ctx, cache, pkg); err != nil {
			return nil, err
		}
		executable, err := os.Executable()
		if err != nil {
			return nil, runner.ErrStrongOwnershipUnavailable
		}
		launcher, err := macos.NewExecLauncher(executable)
		if err != nil {
			return nil, err
		}
		workspace := macos.NewOSWorkspace(
			os.Geteuid(),
			os.Getegid(),
			slot.UID,
			slot.GID,
		)
		nativeRuntime, err := macos.NewFileRuntime(
			defaultMacOSFenceRoot,
			launcher,
			slot,
			workspace,
		)
		if err != nil {
			return nil, err
		}
		adapter, err := macos.New(
			macos.Config{Identity: macos.StaticIdentity(slot)},
			nativeRuntime,
			workspace,
		)
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
		return app.NewAgentCommandRuntime(state.NodeID, state.Store, manager, pkg)
	}
	return optionalDarwinNativeRunnerFactory(options.Required, build), nil
}

func optionalDarwinNativeRunnerFactory(
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

type darwinPackageCache interface {
	Ensure(context.Context, runner.Package) (runner.PreparedPackage, error)
}

func prewarmDarwinOfficialPackage(
	ctx context.Context,
	cache darwinPackageCache,
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

func darwinOfficialPackage(architecture string) (runner.Package, error) {
	return runner.OfficialPackage(runner.Platform{OS: "darwin", Arch: architecture})
}

func lookupMacOSIdentity(name string) (macos.RunnerIdentity, error) {
	if name == "" {
		return macos.RunnerIdentity{}, runner.ErrStrongOwnershipUnavailable
	}
	account, err := user.Lookup(name)
	if err != nil {
		return macos.RunnerIdentity{}, runner.ErrStrongOwnershipUnavailable
	}
	uid, uidErr := strconv.Atoi(account.Uid)
	gid, gidErr := strconv.Atoi(account.Gid)
	if uidErr != nil || gidErr != nil || uid <= 0 || gid <= 0 {
		return macos.RunnerIdentity{}, runner.ErrStrongOwnershipUnavailable
	}
	return macos.RunnerIdentity{UID: uid, GID: gid}, nil
}

func ensureDarwinPrivateDirectory(path string) error {
	if !canonicalAbsolutePath(path) {
		return runner.ErrStrongOwnershipUnavailable
	}
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return runner.ErrStrongOwnershipUnavailable
		}
	} else if err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return runner.ErrStrongOwnershipUnavailable
	}
	return nil
}

func canonicalAbsolutePath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/"
}

func platformCommands() []*cobra.Command { return nil }

