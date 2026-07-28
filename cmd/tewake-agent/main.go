package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/genm/sparerunner/internal/app"
	"github.com/genm/sparerunner/internal/buildinfo"
	"github.com/spf13/cobra"
)

func main() {
	if handled, err := runPlatformLauncherHelper(os.Args[1:]); handled {
		if err != nil {
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if exitCode := runContext(ctx, os.Args[1:], os.Stdout, os.Stderr); exitCode != 0 {
		os.Exit(exitCode)
	}
}

func run(args []string, stdout, stderr io.Writer) int {
	return runContext(context.Background(), args, stdout, stderr)
}

func runContext(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	root := newRootCommand(stdout, stderr)
	root.SetArgs(args)
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func newRootCommand(stdout, stderr io.Writer) *cobra.Command {
	root := &cobra.Command{
		Use:           "tewake-agent",
		Short:         "Run a Tewake node agent",
		SilenceErrors: true,
		SilenceUsage:  true,
		Version:       buildinfo.String(),
		RunE: func(*cobra.Command, []string) error {
			return errors.New("an agent command is required")
		},
	}
	root.SetOut(stdout)
	root.SetErr(stderr)
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(command *cobra.Command, _ []string) {
			fmt.Fprintln(command.OutOrStdout(), buildinfo.String())
		},
	})
	var stateDirectory string
	var connectionTimeout, reconnectDelay time.Duration
	var localControl bool
	var ownerUIDs []int
	native := defaultNativeRunnerOptions()
	serve := &cobra.Command{
		Use:   "serve",
		Short: "Connect this enrolled node to its controller",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			directory, err := resolveAgentStateDirectory(stateDirectory)
			if err != nil {
				return err
			}
			native.ExplicitFlags = explicitPathFlags(command)
			commandRuntime, err := platformCommandRuntime(native)
			if err != nil {
				return err
			}
			return app.ServeAgent(command.Context(), app.AgentServeOptions{
				StateDirectory:       directory,
				ConnectionTimeout:    connectionTimeout,
				ReconnectDelay:       reconnectDelay,
				CommandRuntime:       commandRuntime,
				SharedRunnerIdentity: native.SharedRunnerIdentity,
				LocalControl: app.AgentLocalControlOptions{
					Enabled:   localControl,
					OwnerUIDs: ownerUIDs,
				},
			})
		},
	}
	serve.Flags().StringVar(&stateDirectory, "state-dir", "", "agent state directory (default: OS user config directory)")
	serve.Flags().DurationVar(&connectionTimeout, "connection-timeout", app.DefaultConnectTimeout, "controller connection deadline")
	serve.Flags().DurationVar(&reconnectDelay, "reconnect-delay", app.DefaultReconnectDelay, "delay between reconnect attempts")
	serve.Flags().StringVar(&native.CacheRoot, "cache-root", native.CacheRoot, "verified runner package cache")
	serve.Flags().StringVar(&native.RuntimeRoot, "runtime-root", native.RuntimeRoot, "native runner execution root")
	serve.Flags().StringVar(&native.SupervisorSocket, "supervisor-socket", native.SupervisorSocket, "local privileged supervisor socket")
	serve.Flags().StringVar(&native.RunnerIdentityService, "runner-identity-service", native.RunnerIdentityService, "Windows runner identity service")
	serve.Flags().BoolVar(&native.Required, "require-native-runner", false, "fail startup unless the native runner boundary is available")
	serve.Flags().BoolVar(
		&native.SharedRunnerIdentity,
		"allow-shared-runner-identity",
		false,
		"Linux only: run jobs under this agent's own user instead of a dedicated runner account. "+
			"Drops UID isolation between the agent and the job; every other containment "+
			"and cleanup guarantee is unchanged. Requires a systemd user cgroup delegation "+
			"and is reported to the fleet as sharedRunnerIdentity.",
	)
	serve.Flags().BoolVar(&localControl, "local-control", false, "serve the same-host availability endpoint used by the tray, launcher, and CLI")
	serve.Flags().IntSliceVar(&ownerUIDs, "owner-uid", nil, "additional local user IDs authorized to control this node's availability")
	root.AddCommand(serve)
	root.AddCommand(platformCommands()...)
	return root
}

type nativeRunnerOptions struct {
	CacheRoot             string
	RuntimeRoot           string
	SupervisorSocket      string
	RunnerIdentityService string
	Required              bool
	// SharedRunnerIdentity selects the Linux shared-identity native runner: the
	// job executes under the Agent's own Unix credential instead of a dedicated
	// non-login account. It exists only for owners who cannot install a root
	// Supervisor service, is never a fallback, and is reported as node state.
	SharedRunnerIdentity bool
	// ExplicitFlags records which path flags the owner actually set, so the
	// shared-identity mode can substitute user-owned defaults for the
	// system-service ones without ever silently overriding an explicit choice.
	ExplicitFlags map[string]bool
}

// explicitPathFlags records which root/socket flags the owner actually typed.
// A mode that substitutes different defaults must never overwrite a value the
// owner chose, and a mode that is incompatible with one must reject it loudly
// rather than ignore it.
func explicitPathFlags(command *cobra.Command) map[string]bool {
	names := []string{"cache-root", "runtime-root", "supervisor-socket", "runner-identity-service"}
	explicit := make(map[string]bool, len(names))
	for _, name := range names {
		explicit[name] = command.Flags().Changed(name)
	}
	return explicit
}

func resolveAgentStateDirectory(explicit string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}
	config, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve OS user configuration directory: %w", err)
	}
	return filepath.Join(config, "tewake", "agent"), nil
}
