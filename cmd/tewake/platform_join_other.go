//go:build !windows

package main

import (
	"context"
	"fmt"
	"io"

	"github.com/genm/tewake/internal/app"
)

func platformJoinAgent(
	ctx context.Context,
	options app.JoinOptions,
) (string, error) {
	return app.JoinAgent(ctx, options)
}

func printPlatformJoinNextStep(output io.Writer, goos, stateDirectory string) {
	const macOSServiceState = "/Library/Application Support/Tewake/agent"
	// The path is a platform contract, not a host path to normalize. Comparing
	// it verbatim keeps cross-compiled CLI tests from treating a macOS path as a
	// Windows drive-relative path while preserving the Darwin service hint.
	if goos == "darwin" && stateDirectory == macOSServiceState {
		fmt.Fprintln(
			output,
			"launchd manages this Agent. Activate it with: sudo /bin/launchctl kickstart -k system/com.genm.tewake.agent",
		)
		return
	}
	fmt.Fprintf(
		output,
		"Start it with: tewake-agent serve --state-dir %s\n",
		stateDirectory,
	)
}
