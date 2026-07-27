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

func printPlatformJoinNextStep(output io.Writer, stateDirectory string) {
	fmt.Fprintf(
		output,
		"Start it with: tewake-agent serve --state-dir %s\n",
		stateDirectory,
	)
}
