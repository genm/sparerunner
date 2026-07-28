package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/genm/tewake/internal/domain"
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
	result := make([]*cobra.Command, 0, len(commands)+1)
	for _, definition := range commands {
		result = append(result, newNodeAvailabilityCommand(definition.use, definition.short, definition.call))
	}
	return append(result, newNodeTargetsCommand())
}

// newNodeTargetsCommand is the node owner's per-Target surface. With no flag it
// lists; with exactly one of --exclude/--include it mutates. Both forms return
// the same versioned status document, so a launcher parses one shape.
func newNodeTargetsCommand() *cobra.Command {
	var stateDirectory, source string
	var exclude, include bool
	var emitJSON bool
	var timeout time.Duration
	command := &cobra.Command{
		Use:   "targets [targetId]",
		Short: "List the GitHub Targets this computer serves, or exclude/include one",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			mutating := exclude || include
			// Refusing an ambiguous invocation is the point: silently picking one
			// of two opposite mutations would change fleet capacity by accident.
			if exclude && include {
				return errors.New("use exactly one of --exclude or --include")
			}
			if mutating && len(args) != 1 {
				return errors.New("--exclude and --include require exactly one target ID")
			}
			if !mutating && len(args) != 0 {
				return errors.New("a target ID requires --exclude or --include")
			}
			directory, err := resolveStateDirectory(stateDirectory, "agent")
			if err != nil {
				return err
			}
			client := nodectl.Client{
				StateDirectory: directory,
				Source:         nodectl.Source(source),
				Timeout:        timeout,
			}
			var status nodectl.Status
			var callErr error
			switch {
			case exclude:
				status, callErr = client.Exclude(domain.TargetID(args[0]))
			case include:
				status, callErr = client.Include(domain.TargetID(args[0]))
			default:
				status, callErr = client.Targets()
			}
			if callErr != nil {
				if emitJSON {
					writeAvailabilityFailure(command.OutOrStdout(), callErr)
				}
				return callErr
			}
			if emitJSON {
				return writeAvailabilityJSON(command.OutOrStdout(), status)
			}
			writeNodeTargetsText(command.OutOrStdout(), status)
			return nil
		},
	}
	command.Flags().StringVar(&stateDirectory, "state-dir", "", "agent state directory (default: OS user config directory)")
	command.Flags().BoolVar(&emitJSON, "json", false, "emit the versioned machine-readable status document")
	command.Flags().StringVar(&source, "source", string(nodectl.SourceCLI), "requesting surface recorded with the change: cli, tray, or raycast")
	command.Flags().DurationVar(&timeout, "timeout", nodectl.RequestTimeout, "local control request deadline")
	command.Flags().BoolVar(&exclude, "exclude", false, "stop serving the given GitHub Target from this computer")
	command.Flags().BoolVar(&include, "include", false, "serve the given GitHub Target from this computer again")
	return command
}

func writeNodeTargetsText(writer io.Writer, status nodectl.Status) {
	fmt.Fprintf(writer, "Node:       %s\n", status.NodeID)
	fmt.Fprintf(writer, "Controller: %s\n", connectionText(status.ControllerConnected))
	writeAvailabilityTargets(writer, status.Targets())
	writeUnknownExclusions(writer, status.UnknownExclusions)
}

// writeUnknownExclusions renders exclusions the controller has never listed as
// eligible for this node. They are display fact, not error: an owner may
// exclude a Target while offline or before the first heartbeat round trip.
func writeUnknownExclusions(writer io.Writer, unknown []domain.TargetID) {
	if len(unknown) == 0 {
		return
	}
	fmt.Fprintf(writer, "Excluded:   %d not-currently-eligible target(s)\n", len(unknown))
	for _, targetID := range unknown {
		fmt.Fprintf(writer, "  - %s (excluded, not currently eligible)\n", targetID)
	}
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
	// Only the weaker mode prints a line. The isolated mode is the expectation
	// this tool has always described, so it stays silent; naming the drop
	// explicitly is what stops an operator from mistaking one for the other.
	if status.SharedRunnerIdentity {
		fmt.Fprintln(
			writer,
			"Isolation:  shared runner identity (jobs run as the agent user; no UID isolation)",
		)
	}
	if len(status.RunningExecutions) == 0 {
		fmt.Fprintln(writer, "Running:    none")
	} else {
		fmt.Fprintf(writer, "Running:    %d execution(s)\n", len(status.RunningExecutions))
		for _, execution := range status.RunningExecutions {
			fmt.Fprintf(
				writer,
				"  - %s (%s)%s\n",
				execution.ExecutionID,
				execution.State,
				runningExecutionScope(execution),
			)
		}
	}
	writeAvailabilityTargets(writer, status.Targets())
	writeUnknownExclusions(writer, status.UnknownExclusions)
}

// runningExecutionScope names the org/repo a job belongs to. An execution
// admitted before target attribution existed renders without one rather than
// with an invented scope.
func runningExecutionScope(execution nodectl.RunningExecution) string {
	if execution.Scope == "" || execution.ScopeKind == "" {
		return ""
	}
	return fmt.Sprintf(" %s [%s]", execution.Scope, execution.ScopeKind)
}

// writeAvailabilityTargets renders the eligible-target list a heartbeat ack
// last confirmed. An absent or empty list is display fact, not an error: it
// means either no configured Target currently matches this node's platform,
// or the Agent has not completed its first heartbeat round trip yet.
func writeAvailabilityTargets(writer io.Writer, targets []nodectl.EligibleTarget) {
	if len(targets) == 0 {
		fmt.Fprintln(writer, "Targets:    none reported")
		return
	}
	fmt.Fprintf(writer, "Targets:    %d scope(s)\n", len(targets))
	for _, target := range targets {
		fmt.Fprintf(writer, "  - %s [%s]%s\n", target.Scope, target.ScopeKind, targetStateSuffix(target))
	}
}

// targetStateSuffix never collapses a pending state into a settled one. An
// exclusion the controller has not adopted is still locally effective, so it
// reads as excluded-and-syncing; an inclusion it has not released is not yet
// served, so it reads as pending rather than as available.
func targetStateSuffix(target nodectl.EligibleTarget) string {
	switch {
	case target.LocallyExcluded && target.Pending:
		return " (excluded — syncing)"
	case target.LocallyExcluded:
		return " (excluded)"
	case target.Pending:
		// Not locally excluded but still adopted as excluded by the controller:
		// the owner's re-inclusion has not been released yet.
		return " (include pending)"
	default:
		return ""
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
