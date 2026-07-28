package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/genm/sparerunner/internal/auth"
	"github.com/genm/sparerunner/internal/nodectl"
)

func decodeDoctorReport(t *testing.T, stdout *bytes.Buffer) doctorReport {
	t.Helper()
	var report doctorReport
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		t.Fatalf("decode doctor report: %v\n%s", err, stdout.String())
	}
	if report.DoctorVersion != doctorReportVersion {
		t.Fatalf("doctor version = %d, want %d", report.DoctorVersion, doctorReportVersion)
	}
	return report
}

func doctorFindingByCheck(t *testing.T, report doctorReport, check string) doctorFinding {
	t.Helper()
	for _, finding := range report.Findings {
		if finding.Check == check {
			return finding
		}
	}
	t.Fatalf("doctor report has no %q finding: %#v", check, report.Findings)
	return doctorFinding{}
}

func newDoctorTestServer(t *testing.T, setup map[string]any) *httptest.Server {
	t.Helper()
	return newDoctorTestServerWithOverview(t, map[string]any{
		"activeRuns":         0,
		"conditions":         []any{},
		"configuredCapacity": 2,
		"controllerEpoch":    "7",
		"nodeCount":          2,
		"targetCount":        1,
		"version":            "test-version",
	}, setup)
}

func newDoctorTestServerWithOverview(
	t *testing.T,
	overview map[string]any,
	setup map[string]any,
) *httptest.Server {
	t.Helper()
	return newManagementCLITestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Fatalf("operation request = %s %s", request.Method, request.URL.Path)
		}
		if _, err := request.Cookie(auth.SessionCookieName); err != nil {
			t.Fatalf("operation session cookie: %v", err)
		}
		switch request.URL.Path {
		case "/api/v1/overview":
			writeJSON(writer, http.StatusOK, overview)
		case "/api/v1/setup":
			writeJSON(writer, http.StatusOK, setup)
		default:
			t.Fatalf("operation request path = %s", request.URL.Path)
		}
	})
}

func connectedDoctorSetup(conditions []any) map[string]any {
	return map[string]any{
		"conditions":            conditions,
		"controllerInitialized": true,
		"githubAppState":        "connected",
		"manifestFlowSupported": true,
		"nodeCount":             2,
		"targetCount":           1,
	}
}

func missingAgentStateDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "missing-agent")
}

func TestDoctorReportsHealthyControllerAndAbsentAgent(t *testing.T) {
	server := newDoctorTestServer(t, connectedDoctorSetup([]any{}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := run(withManagementState(t, []string{
		"doctor", "--json",
		"--admin-url", server.URL + "/api/v1",
		"--agent-state-dir", missingAgentStateDir(t),
	}), &stdout, &stderr)
	if err != nil {
		t.Fatalf("doctor: %v", err)
	}
	report := decodeDoctorReport(t, &stdout)
	if !report.OK {
		t.Fatalf("report.OK = false: %#v", report.Findings)
	}
	controller := doctorFindingByCheck(t, report, doctorCheckControllerState)
	if controller.Status != doctorStatusPass {
		t.Fatalf("controller finding = %#v", controller)
	}
	session := doctorFindingByCheck(t, report, doctorCheckManagementSession)
	if session.Status != doctorStatusPass ||
		!strings.Contains(session.Detail, "test-version") ||
		!strings.Contains(session.Detail, "2 node(s)") {
		t.Fatalf("session finding = %#v", session)
	}
	github := doctorFindingByCheck(t, report, doctorCheckGitHubAuthority)
	if github.Status != doctorStatusPass ||
		!strings.Contains(github.Detail, "connected") {
		t.Fatalf("github finding = %#v", github)
	}
	agent := doctorFindingByCheck(t, report, doctorCheckAgentEndpoint)
	if agent.Status != doctorStatusUnavailable ||
		agent.Detail != "no agent is installed on this computer" ||
		agent.ErrorClass != "" {
		t.Fatalf("agent finding = %#v", agent)
	}
}

func TestDoctorTextOutputReportsEveryCheckOnce(t *testing.T) {
	server := newDoctorTestServer(t, connectedDoctorSetup([]any{}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := run(withManagementState(t, []string{
		"doctor",
		"--admin-url", server.URL + "/api/v1",
		"--agent-state-dir", missingAgentStateDir(t),
	}), &stdout, &stderr)
	if err != nil {
		t.Fatalf("doctor: %v", err)
	}
	text := stdout.String()
	for _, check := range []string{
		doctorCheckControllerState,
		doctorCheckManagementSession,
		doctorCheckGitHubAuthority,
		doctorCheckAgentEndpoint,
	} {
		if strings.Count(text, check+":") != 1 {
			t.Fatalf("text output does not report %q exactly once:\n%s", check, text)
		}
	}
	if !strings.Contains(text, "No failing checks.") {
		t.Fatalf("text output lacks the healthy summary:\n%s", text)
	}
}

func TestDoctorReportsDisconnectedGitHubAsUnavailableNotFailure(t *testing.T) {
	setup := connectedDoctorSetup([]any{})
	setup["githubAppState"] = "disconnected"
	server := newDoctorTestServer(t, setup)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := run(withManagementState(t, []string{
		"doctor", "--json",
		"--admin-url", server.URL + "/api/v1",
		"--agent-state-dir", missingAgentStateDir(t),
	}), &stdout, &stderr)
	if err != nil {
		t.Fatalf("doctor: %v", err)
	}
	report := decodeDoctorReport(t, &stdout)
	if !report.OK {
		t.Fatalf("report.OK = false: %#v", report.Findings)
	}
	github := doctorFindingByCheck(t, report, doctorCheckGitHubAuthority)
	if github.Status != doctorStatusUnavailable ||
		github.Detail != "no GitHub App is connected" {
		t.Fatalf("github finding = %#v", github)
	}
}

func TestDoctorFailsOnDegradedOrConditionedGitHubAuthority(t *testing.T) {
	tests := map[string]map[string]any{
		"degraded": func() map[string]any {
			setup := connectedDoctorSetup([]any{})
			setup["githubAppState"] = "degraded"
			return setup
		}(),
		"connected with conditions": connectedDoctorSetup([]any{
			map[string]any{
				"code":   "github_target_runtime_unverified",
				"status": "unavailable",
			},
		}),
		"unknown state": func() map[string]any {
			setup := connectedDoctorSetup([]any{})
			setup["githubAppState"] = "surprise"
			return setup
		}(),
	}
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			server := newDoctorTestServer(t, setup)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			err := run(withManagementState(t, []string{
				"doctor", "--json",
				"--admin-url", server.URL + "/api/v1",
				"--agent-state-dir", missingAgentStateDir(t),
			}), &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), "1 check(s) failed") {
				t.Fatalf("doctor error = %v", err)
			}
			report := decodeDoctorReport(t, &stdout)
			if report.OK {
				t.Fatalf("report.OK = true: %#v", report.Findings)
			}
			github := doctorFindingByCheck(t, report, doctorCheckGitHubAuthority)
			if github.Status != doctorStatusFail {
				t.Fatalf("github finding = %#v", github)
			}
			if name == "connected with conditions" &&
				!strings.Contains(github.Detail, "github_target_runtime_unverified") {
				t.Fatalf("github finding lacks the condition code: %#v", github)
			}
		})
	}
}

func TestDoctorFailsWhenManagementListenerIsUnreachable(t *testing.T) {
	server := newDoctorTestServer(t, connectedDoctorSetup([]any{}))
	adminURL := server.URL + "/api/v1"
	server.Close()

	var stdout, stderr bytes.Buffer
	err := run(withManagementState(t, []string{
		"doctor", "--json",
		"--admin-url", adminURL,
		"--agent-state-dir", missingAgentStateDir(t),
	}), &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "1 check(s) failed") {
		t.Fatalf("doctor error = %v", err)
	}
	report := decodeDoctorReport(t, &stdout)
	if report.OK {
		t.Fatalf("report.OK = true: %#v", report.Findings)
	}
	session := doctorFindingByCheck(t, report, doctorCheckManagementSession)
	if session.Status != doctorStatusFail {
		t.Fatalf("session finding = %#v", session)
	}
	github := doctorFindingByCheck(t, report, doctorCheckGitHubAuthority)
	if github.Status != doctorStatusUnavailable ||
		github.Detail != "management session is unavailable" {
		t.Fatalf("github finding = %#v", github)
	}
}

func TestDoctorReportsUninitializedControllerAsUnavailable(t *testing.T) {
	base := t.TempDir()
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"doctor", "--json",
		"--state-dir", filepath.Join(base, "missing-controller"),
		"--agent-state-dir", filepath.Join(base, "missing-agent"),
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("doctor: %v", err)
	}
	report := decodeDoctorReport(t, &stdout)
	if !report.OK {
		t.Fatalf("report.OK = false: %#v", report.Findings)
	}
	for _, finding := range report.Findings {
		if finding.Status != doctorStatusUnavailable {
			t.Fatalf("finding = %#v, want unavailable", finding)
		}
	}
}

func TestDoctorFailsWithNodectlClassWhenAgentEndpointDoesNotAnswer(t *testing.T) {
	server := newDoctorTestServer(t, connectedDoctorSetup([]any{}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := run(withManagementState(t, []string{
		"doctor", "--json",
		"--admin-url", server.URL + "/api/v1",
		"--agent-state-dir", t.TempDir(),
	}), &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "1 check(s) failed") {
		t.Fatalf("doctor error = %v", err)
	}
	report := decodeDoctorReport(t, &stdout)
	if report.OK {
		t.Fatalf("report.OK = true: %#v", report.Findings)
	}
	agent := doctorFindingByCheck(t, report, doctorCheckAgentEndpoint)
	if agent.Status != doctorStatusFail ||
		agent.ErrorClass != nodectl.ErrorClassEndpointUnavailable {
		t.Fatalf("agent finding = %#v", agent)
	}
}
