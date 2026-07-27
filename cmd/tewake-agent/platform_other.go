//go:build !linux

package main

import (
	"context"
	"errors"

	"github.com/genm/tewake/internal/app"
	"github.com/spf13/cobra"
)

func runPlatformLauncherHelper([]string) (bool, error) {
	return false, nil
}

func defaultNativeRunnerOptions() nativeRunnerOptions {
	return nativeRunnerOptions{}
}

func platformCommandRuntime(options nativeRunnerOptions) (func(context.Context, *app.AgentState) (*app.AgentCommandRuntime, error), error) {
	if options.Required {
		return nil, errors.New("native runner is not implemented for this platform")
	}
	return nil, nil
}

func platformCommands() []*cobra.Command {
	return nil
}
