package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	pathpkg "path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/genm/sparerunner/internal/app"
	"github.com/genm/sparerunner/internal/auth"
	"github.com/genm/sparerunner/internal/buildinfo"
	"github.com/genm/sparerunner/internal/releaseevidence"
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
	return runWithInput(args, os.Stdin, stdout, stderr)
}

func runWithInput(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	return runContextWithInput(context.Background(), args, stdin, stdout, stderr)
}

func runContext(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runContextWithInput(ctx, args, os.Stdin, stdout, stderr)
}

func runContextWithInput(
	ctx context.Context,
	args []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
) error {
	root := newRootCommand(stdout, stderr)
	root.SetIn(stdin)
	root.SetArgs(args)
	return root.ExecuteContext(ctx)
}

func newRootCommand(stdout, stderr io.Writer) *cobra.Command {
	root := &cobra.Command{
		Use:           "sprun",
		Short:         "Orchestrate trusted GitHub Actions runners across computers you own",
		SilenceErrors: true,
		SilenceUsage:  true,
		Version:       buildinfo.String(),
		RunE: func(*cobra.Command, []string) error {
			return errors.New("a SpareRunner command is required")
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
	root.AddCommand(newGitHubCommand())
	root.AddCommand(newUICommand())
	root.AddCommand(newConfigCommand())
	root.AddCommand(newDoctorCommand())
	root.AddCommand(newEvidenceCommand())
	return root
}

func newEvidenceCommand() *cobra.Command {
	evidence := &cobra.Command{
		Use:   "evidence",
		Short: "Validate machine-readable live release evidence",
	}
	var file string
	validate := &cobra.Command{
		Use:   "validate",
		Short: "Validate a task-014 cross-platform evidence manifest",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			manifest, err := releaseevidence.ValidateFile(file)
			if err != nil {
				return err
			}
			fmt.Fprintf(command.OutOrStdout(), "Valid release evidence: %s\n", manifest)
			return nil
		},
	}
	validate.Flags().StringVar(&file, "file", "", "path to a trusted live evidence manifest")
	if err := validate.MarkFlagRequired("file"); err != nil {
		panic(err)
	}
	evidence.AddCommand(validate)
	return evidence
}

func newInitCommand() *cobra.Command {
	var stateDirectory string
	var hints []string
	command := &cobra.Command{
		Use:   "init",
		Short: "Initialize a SpareRunner controller",
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
			fmt.Fprintf(command.OutOrStdout(), "sprun join %s\n", code)
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
		Short: "Run the SpareRunner controller and embedded Web UI",
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
	return newJoinCommandForPlatform(runtime.GOOS, platformJoinAgent)
}

type joinAgentFunc func(context.Context, app.JoinOptions) (string, error)

func newJoinCommandForPlatform(goos string, joinAgent joinAgentFunc) *cobra.Command {
	var stateDirectory, controller string
	var discoveryTimeout, connectionTimeout time.Duration
	command := &cobra.Command{
		Use:   "join [join-code]",
		Short: "Enroll this computer with a SpareRunner controller",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			directory, err := resolveStateDirectoryForPlatform(stateDirectory, "agent", goos)
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
			printPlatformJoinNextStep(command.OutOrStdout(), goos, directory)
			return nil
		},
	}
	command.Flags().StringVar(&stateDirectory, "state-dir", "", "agent state directory (default: OS user config directory)")
	command.Flags().StringVar(&controller, "controller", "", "explicit HTTPS controller endpoint; otherwise use code hints and mDNS")
	command.Flags().DurationVar(&discoveryTimeout, "discovery-timeout", app.DefaultDiscoveryTimeout, "mDNS discovery deadline")
	command.Flags().DurationVar(&connectionTimeout, "connection-timeout", app.DefaultConnectTimeout, "per-controller enrollment and confirmation deadline")
	return command
}

func printPlatformJoinNextStep(output io.Writer, goos, stateDirectory string) {
	const macOSServiceState = "/Library/Application Support/SpareRunner/agent"
	switch {
	case goos == "windows":
		fmt.Fprintln(output, "SpareRunnerAgent service is enrolled and running.")
	case goos == "darwin" && stateDirectory == macOSServiceState:
		// The path is a platform contract, not a host path to normalize. Comparing
		// it verbatim keeps cross-compiled CLI tests from treating a macOS path as a
		// Windows drive-relative path while preserving the Darwin service hint.
		fmt.Fprintln(
			output,
			"launchd manages this Agent. Activate it with: sudo /bin/launchctl kickstart -k system/com.genm.sparerunner.agent",
		)
	default:
		fmt.Fprintf(output, "Start it with: sparerunner-agent serve --state-dir %s\n", stateDirectory)
	}
}

func newNodeCommand() *cobra.Command {
	node := &cobra.Command{Use: "node", Short: "Manage enrolled nodes"}
	var adminURL, stateDirectory string
	var hints []string
	add := &cobra.Command{
		Use:   "add",
		Short: "Create a one-time node join code",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			client, err := newOwnerManagementAPIClient(adminURL, stateDirectory)
			if err != nil {
				return err
			}
			return client.withSession(command.Context(), func(ctx context.Context) error {
				code, err := client.createJoinCode(ctx, hints)
				if err != nil {
					return err
				}
				fmt.Fprintf(command.OutOrStdout(), "sprun join %s\n", code)
				return nil
			})
		},
	}
	add.Flags().StringVar(&adminURL, "admin-url", defaultAdminURL, "loopback management API base, including the /api/v1 path")
	add.Flags().StringVar(&stateDirectory, "state-dir", "", "controller state directory used to authorize the local admin session")
	add.Flags().StringSliceVar(&hints, "hint", nil, "HTTPS controller endpoint hint embedded in the join code")
	node.AddCommand(add)
	for _, availability := range newNodeAvailabilityCommands() {
		node.AddCommand(availability)
	}
	return node
}

func newUICommand() *cobra.Command {
	var adminURL, stateDirectory string
	ui := &cobra.Command{
		Use:   "ui",
		Short: "Authorize access to the loopback management UI",
	}
	authorize := &cobra.Command{
		Use:   "authorize <handoff-code>",
		Short: "Authorize one browser-generated handoff",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if !auth.ValidBrowserHandoffCodeEncoding(args[0]) {
				return errors.New("browser handoff code is invalid")
			}
			client, err := newOwnerManagementAPIClient(adminURL, stateDirectory)
			if err != nil {
				return err
			}
			return client.withSession(command.Context(), func(ctx context.Context) error {
				if err := client.authorizeBrowserHandoff(ctx, args[0]); err != nil {
					return err
				}
				fmt.Fprintln(
					command.OutOrStdout(),
					"Browser authorized. Return to SpareRunner in the browser.",
				)
				return nil
			})
		},
	}
	authorize.Flags().StringVar(
		&adminURL,
		"admin-url",
		defaultAdminURL,
		"loopback management API base, including the /api/v1 path",
	)
	authorize.Flags().StringVar(
		&stateDirectory,
		"state-dir",
		"",
		"controller state directory used to authorize the local admin session",
	)
	ui.AddCommand(authorize)
	return ui
}

func newConfigCommand() *cobra.Command {
	var adminURL, stateDirectory string
	configuration := &cobra.Command{
		Use:   "config",
		Short: "Export or apply the desired controller configuration",
	}
	configuration.PersistentFlags().StringVar(
		&adminURL,
		"admin-url",
		defaultAdminURL,
		"loopback management API base, including the /api/v1 path",
	)
	configuration.PersistentFlags().StringVar(
		&stateDirectory,
		"state-dir",
		"",
		"controller state directory used to authorize the local admin session",
	)
	configuration.AddCommand(&cobra.Command{
		Use:   "export",
		Short: "Export the non-secret desired configuration as YAML",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			client, err := newOwnerManagementAPIClient(adminURL, stateDirectory)
			if err != nil {
				return err
			}
			return client.withSession(command.Context(), func(ctx context.Context) error {
				payload, err := client.exportConfiguration(ctx)
				if err != nil {
					return err
				}
				if _, err := command.OutOrStdout().Write(payload); err != nil {
					return errors.New("write configuration export")
				}
				return nil
			})
		},
	})
	configuration.AddCommand(&cobra.Command{
		Use:   "apply <file|->",
		Short: "Atomically apply a versioned configuration document",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			payload, mediaType, revision, err := loadConfigurationPayload(
				args[0],
				command.InOrStdin(),
			)
			if err != nil {
				return err
			}
			client, err := newOwnerManagementAPIClient(adminURL, stateDirectory)
			if err != nil {
				return err
			}
			return client.withSession(command.Context(), func(ctx context.Context) error {
				if err := client.applyConfiguration(
					ctx,
					payload,
					mediaType,
					revision,
				); err != nil {
					return err
				}
				fmt.Fprintln(command.OutOrStdout(), "Configuration applied.")
				return nil
			})
		},
	})
	return configuration
}

func resolveStateDirectory(explicit, role string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}
	config, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve OS user configuration directory: %w", err)
	}
	return filepath.Join(config, "sparerunner", role), nil
}

func resolveStateDirectoryForPlatform(explicit, role, goos string) (string, error) {
	if explicit != "" && goos == "darwin" {
		// Tests and packaging adapters may exercise Darwin contracts from a
		// non-Darwin host; use POSIX path semantics for that explicit contract.
		if !strings.HasPrefix(explicit, "/") {
			return "", fmt.Errorf("macOS state directory must be absolute: %s", explicit)
		}
		return pathpkg.Clean(explicit), nil
	}
	return resolveStateDirectory(explicit, role)
}
