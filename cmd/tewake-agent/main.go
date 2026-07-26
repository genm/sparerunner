package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/genm/tewake/internal/buildinfo"
)

func main() {
	showVersion := flag.Bool("version", false, "print version information")
	flag.Parse()

	if *showVersion {
		fmt.Println(buildinfo.String())
		return
	}

	fmt.Fprintln(os.Stderr, "tewake-agent runtime is not implemented in this development snapshot")
	os.Exit(1)
}
