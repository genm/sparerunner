package main

import (
	"fmt"
	"os"

	"github.com/genm/tewake/internal/buildinfo"
	"github.com/spf13/cobra"
)

func main() {
	root := newRootCommand()
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "tewake",
		Short:         "Orchestrate trusted GitHub Actions runners across computers you own",
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(buildinfo.String())
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
