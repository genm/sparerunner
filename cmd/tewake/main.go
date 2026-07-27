package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/genm/tewake/internal/app"
	"github.com/genm/tewake/internal/buildinfo"
	"github.com/spf13/cobra"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runContext(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	return runContext(context.Background(), args, stdout, stderr)
}

func runContext(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	root := newRootCommand(stdout, stderr)
	root.SetArgs(args)
	return root.ExecuteContext(ctx)
}

func newRootCommand(stdout, stderr io.Writer) *cobra.Command {
	root := &cobra.Command{
		Use:           "tewake",
		Short:         "Orchestrate trusted GitHub Actions runners across computers you own",
		SilenceErrors: true,
		SilenceUsage:  true,
		Version:       buildinfo.String(),
		RunE: func(*cobra.Command, []string) error {
			return errors.New("a Tewake command is required")
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
	root.AddCommand(newInitCommand())
	root.AddCommand(newServeCommand())
	root.AddCommand(newJoinCommand())
	root.AddCommand(newNodeCommand())
	return root
}

func newInitCommand() *cobra.Command {
	var stateDirectory string
	var hints []string
	command := &cobra.Command{
		Use:   "init",
		Short: "Initialize a Tewake controller",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			directory, err := resolveStateDirectory(stateDirectory, "controller")
			if err != nil {
				return err
			}
			code, err := app.InitializeController(command.Context(), directory, hints)
			if err != nil {
				return err
			}
			fmt.Fprintf(command.OutOrStdout(), "Controller initialized in %s\n", directory)
			fmt.Fprintf(command.OutOrStdout(), "tewake join %s\n", code)
			return nil
		},
	}
	command.Flags().StringVar(&stateDirectory, "state-dir", "", "controller state directory (default: OS user config directory)")
	command.Flags().StringSliceVar(&hints, "hint", nil, "HTTPS controller endpoint hint embedded in the join code")
	return command
}

func newServeCommand() *cobra.Command {
	var stateDirectory, agentAddress, adminAddress string
	var advertise bool
	var readHeaderTimeout time.Duration
	command := &cobra.Command{
		Use:   "serve",
		Short: "Run the Tewake controller and embedded Web UI",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			directory, err := resolveStateDirectory(stateDirectory, "controller")
			if err != nil {
				return err
			}
			state, err := app.OpenController(command.Context(), directory, true)
			if err != nil {
				return err
			}
			defer state.Close()
			agentListener, err := net.Listen("tcp", agentAddress)
			if err != nil {
				return fmt.Errorf("listen for agents: %w", err)
			}
			var adminListener net.Listener
			if adminAddress != "" {
				adminListener, err = net.Listen("tcp", adminAddress)
				if err != nil {
					_ = agentListener.Close()
					return fmt.Errorf("listen for administration: %w", err)
				}
				if err := app.ValidateAdminListener(adminListener); err != nil {
					_ = adminListener.Close()
					_ = agentListener.Close()
					return err
				}
			}
			fmt.Fprintf(command.OutOrStdout(), "Agent endpoint: https://%s\n", agentListener.Addr())
			if adminListener != nil {
				fmt.Fprintf(command.OutOrStdout(), "Web UI: http://%s\n", adminListener.Addr())
			}
			return app.ServeController(command.Context(), state, app.ControllerServeOptions{
				AgentListener:     agentListener,
				AdminListener:     adminListener,
				AdvertiseMDNS:     advertise,
				ReadHeaderTimeout: readHeaderTimeout,
			})
		},
	}
	command.Flags().StringVar(&stateDirectory, "state-dir", "", "controller state directory (default: OS user config directory)")
	command.Flags().StringVar(&agentAddress, "agent-listen", ":7443", "HTTPS/mTLS listener for agent enrollment and sessions")
	command.Flags().StringVar(&adminAddress, "admin-listen", "127.0.0.1:7442", "loopback Web UI listener; empty disables it")
	command.Flags().BoolVar(&advertise, "mdns", true, "advertise the agent endpoint through mDNS discovery")
	command.Flags().DurationVar(&readHeaderTimeout, "read-header-timeout", app.DefaultHTTPReadHeaderTimeout, "HTTP header read deadline")
	return command
}

func newJoinCommand() *cobra.Command {
	return newJoinCommandForPlatform(runtime.GOOS, app.JoinAgent)
}

type joinAgentFunc func(context.Context, app.JoinOptions) (string, error)

func newJoinCommandForPlatform(goos string, joinAgent joinAgentFunc) *cobra.Command {
	var stateDirectory, controller string
	var discoveryTimeout, connectionTimeout time.Duration
	command := &cobra.Command{
		Use:   "join [join-code]",
		Short: "Enroll this computer with a Tewake controller",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			directory, err := resolveStateDirectory(stateDirectory, "agent")
			if err != nil {
				return err
			}
			nodeID, err := joinAgent(command.Context(), app.JoinOptions{
				StateDirectory:    directory,
				JoinCode:          args[0],
				Controller:        controller,
				DiscoveryTimeout:  discoveryTimeout,
				ConnectionTimeout: connectionTimeout,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(command.OutOrStdout(), "Node %s joined successfully\n", nodeID)
			writeJoinServiceHint(command.OutOrStdout(), goos, directory)
			return nil
		},
	}
	command.Flags().StringVar(&stateDirectory, "state-dir", "", "agent state directory (default: OS user config directory)")
	command.Flags().StringVar(&controller, "controller", "", "explicit HTTPS controller endpoint; otherwise use code hints and mDNS")
	command.Flags().DurationVar(&discoveryTimeout, "discovery-timeout", app.DefaultDiscoveryTimeout, "mDNS discovery deadline")
	command.Flags().DurationVar(&connectionTimeout, "connection-timeout", app.DefaultConnectTimeout, "per-controller enrollment and confirmation deadline")
	return command
}

func writeJoinServiceHint(output io.Writer, goos, stateDirectory string) {
	const macOSServiceState = "/Library/Application Support/Tewake/agent"
	if goos == "darwin" && filepath.Clean(stateDirectory) == macOSServiceState {
		fmt.Fprintln(
			output,
			"launchd manages this Agent. Activate it with: sudo /bin/launchctl kickstart -k system/com.genm.tewake.agent",
		)
		return
	}
	fmt.Fprintf(output, "Start it with: tewake-agent serve --state-dir %s\n", stateDirectory)
}

func newNodeCommand() *cobra.Command {
	node := &cobra.Command{Use: "node", Short: "Manage enrolled nodes"}
	var stateDirectory string
	var hints []string
	add := &cobra.Command{
		Use:   "add",
		Short: "Create a one-time node join code",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			directory, err := resolveStateDirectory(stateDirectory, "controller")
			if err != nil {
				return err
			}
			state, err := app.OpenController(command.Context(), directory, false)
			if err != nil {
				return err
			}
			defer state.Close()
			code, err := state.CreateJoinCode(command.Context(), hints)
			if err != nil {
				return err
			}
			fmt.Fprintf(command.OutOrStdout(), "tewake join %s\n", code)
			return nil
		},
	}
	add.Flags().StringVar(&stateDirectory, "state-dir", "", "controller state directory (default: OS user config directory)")
	add.Flags().StringSliceVar(&hints, "hint", nil, "HTTPS controller endpoint hint embedded in the join code")
	node.AddCommand(add)
	return node
}

func resolveStateDirectory(explicit, role string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}
	config, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve OS user configuration directory: %w", err)
	}
	return filepath.Join(config, "tewake", role), nil
}
