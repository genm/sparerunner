package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/genm/sparerunner/internal/api/gen"
	"github.com/genm/sparerunner/internal/nodectl"
	"github.com/spf13/cobra"
)

// sprun doctor composes read surfaces that already exist — the controller state
// boundary, the loopback management session, the setup read model, and the
// nodectl local control contract — into one fail-closed report. It introduces
// no new API surface, no new privilege, and no mutation. Findings are
// three-valued: an absent subject (no controller initialized here, no agent
// installed here) is an explicit unavailable finding, never a failure and never
// a healthy default.

const doctorReportVersion = 1

const (
	doctorStatusPass        = "pass"
	doctorStatusFail        = "fail"
	doctorStatusUnavailable = "unavailable"
)

const (
	doctorCheckControllerState   = "controller_state"
	doctorCheckManagementSession = "management_session"
	doctorCheckGitHubAuthority   = "github_authority"
	doctorCheckAgentEndpoint     = "agent_endpoint"
)

type doctorFinding struct {
	Check  string `json:"check"`
	Status string `json:"status"`
	Detail string `json:"detail"`
	// ErrorClass carries the machine-readable class the underlying surface
	// already emits (today only the nodectl contract defines one).
	ErrorClass string `json:"errorClass,omitempty"`
}

type doctorReport struct {
	DoctorVersion int             `json:"doctorVersion"`
	OK            bool            `json:"ok"`
	Findings      []doctorFinding `json:"findings"`
}

type doctorOptions struct {
	adminURL           string
	controllerStateDir string
	agentStateDir      string
	agentTimeout       time.Duration
}

func newDoctorCommand() *cobra.Command {
	var adminURL, stateDirectory, agentStateDirectory string
	var emitJSON bool
	var agentTimeout time.Duration
	command := &cobra.Command{
		Use:   "doctor",
		Short: "Diagnose the controller, GitHub authority, and this computer's agent",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			report := runDoctor(command.Context(), doctorOptions{
				adminURL:           adminURL,
				controllerStateDir: stateDirectory,
				agentStateDir:      agentStateDirectory,
				agentTimeout:       agentTimeout,
			})
			if emitJSON {
				if err := writeAvailabilityJSON(command.OutOrStdout(), report); err != nil {
					return err
				}
			} else {
				writeDoctorText(command.OutOrStdout(), report)
			}
			if failed := report.failedCheckCount(); failed > 0 {
				return fmt.Errorf("doctor: %d check(s) failed", failed)
			}
			return nil
		},
	}
	command.Flags().StringVar(&adminURL, "admin-url", defaultAdminURL, "loopback management API base, including the /api/v1 path")
	command.Flags().StringVar(&stateDirectory, "state-dir", "", "controller state directory (default: OS user config directory)")
	command.Flags().StringVar(&agentStateDirectory, "agent-state-dir", "", "agent state directory (default: OS user config directory)")
	command.Flags().BoolVar(&emitJSON, "json", false, "emit the versioned machine-readable report")
	command.Flags().DurationVar(&agentTimeout, "timeout", nodectl.RequestTimeout, "local agent control request deadline")
	return command
}

func (report doctorReport) failedCheckCount() int {
	failed := 0
	for _, finding := range report.Findings {
		if finding.Status == doctorStatusFail {
			failed++
		}
	}
	return failed
}

func runDoctor(ctx context.Context, options doctorOptions) doctorReport {
	findings := make([]doctorFinding, 0, 4)
	controller := diagnoseControllerState(options.controllerStateDir)
	findings = append(findings, controller)
	if controller.Status == doctorStatusPass {
		session, github := diagnoseManagement(ctx, options)
		findings = append(findings, session, github)
	} else {
		detail := "controller state is absent on this computer"
		findings = append(findings,
			doctorFinding{Check: doctorCheckManagementSession, Status: doctorStatusUnavailable, Detail: detail},
			doctorFinding{Check: doctorCheckGitHubAuthority, Status: doctorStatusUnavailable, Detail: detail},
		)
	}
	findings = append(findings, diagnoseAgentEndpoint(options.agentStateDir, options.agentTimeout))
	return doctorReport{
		DoctorVersion: doctorReportVersion,
		OK:            doctorFindingsHealthy(findings),
		Findings:      findings,
	}
}

func doctorFindingsHealthy(findings []doctorFinding) bool {
	for _, finding := range findings {
		if finding.Status == doctorStatusFail {
			return false
		}
	}
	return true
}

// diagnoseControllerState reports whether this computer holds controller state
// at all. Absence is the normal state of a node-only computer, so it is an
// unavailable finding rather than a failure.
func diagnoseControllerState(explicit string) doctorFinding {
	finding := doctorFinding{Check: doctorCheckControllerState}
	directory, err := resolveStateDirectory(explicit, "controller")
	if err != nil {
		finding.Status = doctorStatusFail
		finding.Detail = err.Error()
		return finding
	}
	info, statErr := os.Stat(directory)
	switch {
	case errors.Is(statErr, os.ErrNotExist):
		finding.Status = doctorStatusUnavailable
		finding.Detail = "controller is not initialized on this computer"
	case statErr != nil:
		finding.Status = doctorStatusFail
		finding.Detail = "controller state directory is unreadable"
	case !info.IsDir():
		finding.Status = doctorStatusFail
		finding.Detail = "controller state path is not a directory"
	default:
		finding.Status = doctorStatusPass
		finding.Detail = "controller state is present at " + directory
	}
	return finding
}

// diagnoseManagement proves the whole owner path in one round trip: the
// owner-proof boundary, the loopback listener, session bootstrap, and the
// safe overview and setup reads. GitHub authority is unavailable, not failed,
// while the session it depends on cannot be established.
func diagnoseManagement(ctx context.Context, options doctorOptions) (doctorFinding, doctorFinding) {
	session := doctorFinding{Check: doctorCheckManagementSession}
	github := doctorFinding{Check: doctorCheckGitHubAuthority}
	sessionUnavailable := func(detail string) {
		session.Status = doctorStatusFail
		session.Detail = detail
		github.Status = doctorStatusUnavailable
		github.Detail = "management session is unavailable"
	}
	client, err := newOwnerManagementAPIClient(options.adminURL, options.controllerStateDir)
	if err != nil {
		sessionUnavailable(err.Error())
		return session, github
	}
	operationErr := client.withSession(ctx, func(ctx context.Context) error {
		overview, err := client.fetchOverview(ctx)
		if err != nil {
			sessionUnavailable(err.Error())
			return nil
		}
		session.Status = doctorStatusPass
		session.Detail = fmt.Sprintf(
			"controller %s serves %d node(s) and %d target(s)",
			overview.Version, overview.NodeCount, overview.TargetCount,
		)
		setup, err := client.fetchSetup(ctx)
		if err != nil {
			github.Status = doctorStatusFail
			github.Detail = err.Error()
			return nil
		}
		github.Status, github.Detail = summarizeGitHubAuthority(setup)
		return nil
	})
	if operationErr != nil {
		// Bootstrap or logout failed. A session whose end cannot be proven is
		// not a passing session, and a read that never happened is unavailable.
		session.Status = doctorStatusFail
		session.Detail = operationErr.Error()
		if github.Status == "" {
			github.Status = doctorStatusUnavailable
			github.Detail = "management session is unavailable"
		}
	}
	return session, github
}

func summarizeGitHubAuthority(setup gen.Setup) (string, string) {
	codes := make([]string, 0, len(setup.Conditions))
	for _, condition := range setup.Conditions {
		codes = append(codes, condition.Code)
	}
	switch setup.GithubAppState {
	case gen.SetupGithubAppStateConnected:
		if len(codes) > 0 {
			return doctorStatusFail,
				"GitHub App is connected with active conditions: " + strings.Join(codes, ", ")
		}
		return doctorStatusPass, fmt.Sprintf(
			"GitHub App is connected; %d target(s) configured", setup.TargetCount,
		)
	case gen.SetupGithubAppStateDisconnected:
		return doctorStatusUnavailable, "no GitHub App is connected"
	case gen.SetupGithubAppStateDegraded:
		detail := "GitHub App authority is degraded"
		if len(codes) > 0 {
			detail += ": " + strings.Join(codes, ", ")
		}
		return doctorStatusFail, detail
	default:
		// An unknown state is never rendered as healthy.
		return doctorStatusFail, "management API setup response is invalid"
	}
}

// diagnoseAgentEndpoint treats the agent state directory as the installed
// signal: a computer that never joined has no directory and reports an
// explicit not-installed finding, while a present directory whose control
// endpoint does not answer is a failure with the nodectl error class.
func diagnoseAgentEndpoint(explicit string, timeout time.Duration) doctorFinding {
	finding := doctorFinding{Check: doctorCheckAgentEndpoint}
	directory, err := resolveStateDirectory(explicit, "agent")
	if err != nil {
		finding.Status = doctorStatusFail
		finding.Detail = err.Error()
		return finding
	}
	info, statErr := os.Stat(directory)
	switch {
	case errors.Is(statErr, os.ErrNotExist):
		finding.Status = doctorStatusUnavailable
		finding.Detail = "no agent is installed on this computer"
		return finding
	case statErr != nil:
		finding.Status = doctorStatusFail
		finding.Detail = "agent state directory is unreadable"
		return finding
	case !info.IsDir():
		finding.Status = doctorStatusFail
		finding.Detail = "agent state path is not a directory"
		return finding
	}
	status, callErr := nodectl.Client{
		StateDirectory: directory,
		Source:         nodectl.SourceCLI,
		Timeout:        timeout,
	}.Status()
	if callErr != nil {
		finding.Status = doctorStatusFail
		finding.Detail = callErr.Error()
		finding.ErrorClass = nodectl.ErrorClassAgentDegraded
		var controlErr *nodectl.Error
		if errors.As(callErr, &controlErr) {
			finding.ErrorClass = controlErr.Class
		}
		return finding
	}
	finding.Status = doctorStatusPass
	finding.Detail = fmt.Sprintf(
		"agent %s accepts new jobs: %s", status.NodeID, availabilityHeadline(status),
	)
	return finding
}

func writeDoctorText(writer io.Writer, report doctorReport) {
	for _, finding := range report.Findings {
		class := ""
		if finding.ErrorClass != "" {
			class = " [" + finding.ErrorClass + "]"
		}
		fmt.Fprintf(
			writer,
			"%-20s %-11s %s%s\n",
			finding.Check+":", finding.Status, finding.Detail, class,
		)
	}
	if report.OK {
		fmt.Fprintln(writer, "No failing checks.")
	} else {
		fmt.Fprintln(writer, "Failing checks found. Fix the failed layer and run doctor again.")
	}
}

func (client *managementAPIClient) fetchOverview(ctx context.Context) (gen.Overview, error) {
	response, err := client.client.GetOverview(ctx)
	if err != nil {
		return gen.Overview{}, errors.New("management API overview request failed")
	}
	body, readErr := readManagementResponse(response)
	if response == nil {
		return gen.Overview{}, readErr
	}
	if response.StatusCode != http.StatusOK {
		return gen.Overview{}, managementStatusError("read overview", response.StatusCode)
	}
	if readErr != nil {
		return gen.Overview{}, readErr
	}
	if !hasMediaType(response.Header, "application/json") {
		return gen.Overview{}, errors.New("management API overview response is invalid")
	}
	var overview gen.Overview
	if err := decodeStrictJSON(body, &overview); err != nil {
		return gen.Overview{}, errors.New("management API overview response is invalid")
	}
	return overview, nil
}

func (client *managementAPIClient) fetchSetup(ctx context.Context) (gen.Setup, error) {
	response, err := client.client.GetSetup(ctx)
	if err != nil {
		return gen.Setup{}, errors.New("management API setup request failed")
	}
	body, readErr := readManagementResponse(response)
	if response == nil {
		return gen.Setup{}, readErr
	}
	if response.StatusCode != http.StatusOK {
		return gen.Setup{}, managementStatusError("read setup", response.StatusCode)
	}
	if readErr != nil {
		return gen.Setup{}, readErr
	}
	if !hasMediaType(response.Header, "application/json") {
		return gen.Setup{}, errors.New("management API setup response is invalid")
	}
	var setup gen.Setup
	if err := decodeStrictJSON(body, &setup); err != nil {
		return gen.Setup{}, errors.New("management API setup response is invalid")
	}
	return setup, nil
}
