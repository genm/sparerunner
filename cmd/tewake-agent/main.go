package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/genm/tewake/internal/buildinfo"
)

func main() {
	if exitCode := run(os.Args[1:], os.Stdout, os.Stderr); exitCode != 0 {
		os.Exit(exitCode)
	}
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("tewake-agent", flag.ContinueOnError)
	flags.SetOutput(stderr)
	showVersion := flags.Bool("version", false, "print version information")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	if *showVersion {
		fmt.Fprintln(stdout, buildinfo.String())
		return 0
	}

	fmt.Fprintln(stderr, "tewake-agent runtime is not implemented in this development snapshot")
	return 1
}
