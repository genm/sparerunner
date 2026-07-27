package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/genm/tewake/internal/nodectl"
	"github.com/spf13/cobra"
)

// The CLI is the one implementation of the local control contract that desktop
// launchers integrate against. Its --json document is versioned and non-secret,
// and every failure exits non-zero with a machine-readable error class.

type availabilityFailure struct {
	ProtocolVersion int    `json:"protocolVersion"`
	OK              bool   `json:"ok"`
	ErrorClass      string `json:"errorClass"`
	Message         string `json:"message"`
}

func newNodeAvailabilityCommands() []*cobra.Command {
	commands := []struct {
		use   string
		short string
		call  func(nodectl.Client) (nodectl.Status, error)
	}{
		{
			use:   "status",
			short: "Show whether this computer accepts new fleet jobs",
			call:  func(client nodectl.Client) (nodectl.Status, error) { return client.Status() },
		},
		{
			use:   "pause",
			short: "Stop accepting new jobs on this computer without cancelling a running job",
			call:  func(client nodectl.Client) (nodectl.Status, error) { return client.Pause() },
		},
		{
			use:   "resume",
			short: "Resume accepting new jobs on this computer",
			call:  func(client nodectl.Client) (nodectl.Status, error) { return client.Resume() },
		},
	}
	result := make([]*cobra.Command, 0, len(commands))
	for _, definition := range commands {
		result = append(result, newNodeAvailabilityCommand(definition.use, definition.short, definition.call))
	}
	return result
}

func newNodeAvailabilityCommand(
	use, short string,
	call func(nodectl.Client) (nodectl.Status, error),
) *cobra.Command {
	var stateDirectory, source string
	var emitJSON bool
	var timeout time.Duration
	command := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			directory, err := resolveStateDirectory(stateDirectory, "agent")
			if err != nil {
				return err
			}
			status, callErr := call(nodectl.Client{
				StateDirectory: directory,
				Source:         nodectl.Source(source),
				Timeout:        timeout,
			})
			if callErr != nil {
				if emitJSON {
					// The failure document goes to stdout beside the success
					// document, so a launcher parses exactly one stream and is
					// never handed a JSON body interleaved with human-readable
					// error text. The non-zero exit code still reports failure.
					writeAvailabilityFailure(command.OutOrStdout(), callErr)
				}
				return callErr
			}
			if emitJSON {
				return writeAvailabilityJSON(command.OutOrStdout(), status)
			}
			writeAvailabilityText(command.OutOrStdout(), status)
			return nil
		},
	}
	command.Flags().StringVar(&stateDirectory, "state-dir", "", "agent state directory (default: OS user config directory)")
	command.Flags().BoolVar(&emitJSON, "json", false, "emit the versioned machine-readable status document")
	command.Flags().StringVar(&source, "source", string(nodectl.SourceCLI), "requesting surface recorded with the change: cli, tray, or raycast")
	command.Flags().DurationVar(&timeout, "timeout", nodectl.RequestTimeout, "local control request deadline")
	return command
}

func writeAvailabilityJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func writeAvailabilityFailure(writer io.Writer, err error) {
	class := nodectl.ErrorClassAgentDegraded
	var controlErr *nodectl.Error
	if errors.As(err, &controlErr) {
		class = controlErr.Class
	}
	_ = writeAvailabilityJSON(writer, availabilityFailure{
		ProtocolVersion: nodectl.ProtocolVersion,
		OK:              false,
		ErrorClass:      class,
		Message:         err.Error(),
	})
}

func writeAvailabilityText(writer io.Writer, status nodectl.Status) {
	fmt.Fprintf(writer, "Node:       %s\n", status.NodeID)
	fmt.Fprintf(writer, "Accepting:  %s\n", availabilityHeadline(status))
	fmt.Fprintf(writer, "Controller: %s\n", connectionText(status.ControllerConnected))
	fmt.Fprintf(writer, "Runner:     %s\n", readinessText(status.NativeRunnerReady))
	if len(status.RunningExecutions) == 0 {
		fmt.Fprintln(writer, "Running:    none")
		return
	}
	fmt.Fprintf(writer, "Running:    %d execution(s)\n", len(status.RunningExecutions))
	for _, execution := range status.RunningExecutions {
		fmt.Fprintf(writer, "  - %s (%s)\n", execution.ExecutionID, execution.State)
	}
}

// availabilityHeadline never collapses pending or stopped into "yes". A
// resume that the controller has not confirmed is not acceptance.
func availabilityHeadline(status nodectl.Status) string {
	switch {
	case !status.Intent.Accepts():
		return "no (stopped by this computer's owner)"
	case status.PendingResume:
		return "pending (resume is not confirmed by the controller yet)"
	case !status.NativeRunnerReady:
		return "no (native runner is unavailable)"
	default:
		return "yes"
	}
}

func connectionText(connected bool) string {
	if connected {
		return "connected"
	}
	return "disconnected"
}

func readinessText(ready bool) string {
	if ready {
		return "ready"
	}
	return "unavailable"
}
