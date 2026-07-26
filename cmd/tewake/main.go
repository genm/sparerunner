package main

import (
	"fmt"
	"io"
	"os"

	"github.com/genm/tewake/internal/buildinfo"
	"github.com/spf13/cobra"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	root := newRootCommand(stdout, stderr)
	root.SetArgs(args)
	return root.Execute()
}

func newRootCommand(stdout, stderr io.Writer) *cobra.Command {
	root := &cobra.Command{
		Use:           "tewake",
		Short:         "Orchestrate trusted GitHub Actions runners across computers you own",
		SilenceErrors: true,
		SilenceUsage:  true,
		Version:       buildinfo.String(),
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
	root.AddCommand(notImplementedCommand("init", "Initialize a Tewake controller"))
	root.AddCommand(notImplementedCommand("serve", "Run the Tewake controller"))
	root.AddCommand(notImplementedCommand("join [join-code]", "Join this computer to a Tewake controller"))
	return root
}

func notImplementedCommand(use, short string) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		RunE: func(_ *cobra.Command, _ []string) error {
			return fmt.Errorf("%s is not implemented in this development snapshot", use)
		},
	}
}
