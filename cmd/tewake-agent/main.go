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

	"github.com/genm/tewake/internal/app"
	"github.com/genm/tewake/internal/buildinfo"
	"github.com/spf13/cobra"
)

func main() {
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
	serve := &cobra.Command{
		Use:   "serve",
		Short: "Connect this enrolled node to its controller",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			directory, err := resolveAgentStateDirectory(stateDirectory)
			if err != nil {
				return err
			}
			return app.ServeAgent(command.Context(), app.AgentServeOptions{
				StateDirectory:    directory,
				ConnectionTimeout: connectionTimeout,
				ReconnectDelay:    reconnectDelay,
			})
		},
	}
	serve.Flags().StringVar(&stateDirectory, "state-dir", "", "agent state directory (default: OS user config directory)")
	serve.Flags().DurationVar(&connectionTimeout, "connection-timeout", app.DefaultConnectTimeout, "controller connection deadline")
	serve.Flags().DurationVar(&reconnectDelay, "reconnect-delay", app.DefaultReconnectDelay, "delay between reconnect attempts")
	root.AddCommand(serve)
	return root
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
