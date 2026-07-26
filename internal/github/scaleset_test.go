package github

import (
	"errors"
	"testing"

	"github.com/actions/scaleset"
)

func TestNewAppClientAcceptsOnlyGitHubDotComScopes(t *testing.T) {
	validURLs := []string{
		"https://github.com/example-org",
		"https://github.com/example-org/tewake",
	}
	for _, configURL := range validURLs {
		t.Run(configURL, func(t *testing.T) {
			client, err := NewAppClient(AppClientConfig{
				GitHubConfigURL: configURL,
				ClientID:        "client-id",
				InstallationID:  1,
				PrivateKey:      NewAppPrivateKey("private-key-canary"),
			})
			if err != nil {
				t.Fatalf("NewAppClient() error = %v", err)
			}
			if client == nil {
				t.Fatal("NewAppClient() = nil, want client")
			}
		})
	}
}

func TestNewAppClientRejectsUnsafeOrMalformedGitHubConfigURLs(t *testing.T) {
	testCases := []struct {
		name      string
		configURL string
	}{
		{name: "http scheme", configURL: "http://github.com/example-org"},
		{name: "non GitHub host", configURL: "https://github.example.test/example-org"},
		{name: "localhost", configURL: "https://localhost/example-org"},
		{name: "port", configURL: "https://github.com:443/example-org"},
		{name: "userinfo", configURL: "https://user@github.com/example-org"},
		{name: "query", configURL: "https://github.com/example-org?redirect=https://example.test"},
		{name: "blank query", configURL: "https://github.com/example-org?"},
		{name: "fragment", configURL: "https://github.com/example-org#fragment"},
		{name: "blank fragment", configURL: "https://github.com/example-org#"},
		{name: "missing path", configURL: "https://github.com"},
		{name: "too many path parts", configURL: "https://github.com/example-org/tewake/settings"},
		{name: "encoded path", configURL: "https://github.com/example-org%2Ftewake"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := NewAppClient(AppClientConfig{
				GitHubConfigURL: testCase.configURL,
				ClientID:        "client-id",
				InstallationID:  1,
				PrivateKey:      NewAppPrivateKey("private-key-canary"),
			})
			if err == nil {
				t.Fatalf("NewAppClient(%q) succeeded, want fail-closed validation error", testCase.configURL)
			}
		})
	}
}

func TestFromMessageRejectsMissingOrInconsistentStatistics(t *testing.T) {
	testCases := []struct {
		name       string
		statistics *scaleset.RunnerScaleSetStatistic
	}{
		{name: "missing statistics", statistics: nil},
		{name: "negative value", statistics: &scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: -1}},
		{name: "running exceeds assigned", statistics: &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1, TotalRunningJobs: 2}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := fromMessage(7, &scaleset.RunnerScaleSetMessage{MessageID: 11, Statistics: testCase.statistics})
			if !errors.Is(err, ErrInvalidStatistics) {
				t.Fatalf("fromMessage() error = %v, want ErrInvalidStatistics", err)
			}
		})
	}
}

func TestFromScaleSetPreservesMissingStatisticsAsUnknown(t *testing.T) {
	converted, err := fromScaleSet(scaleset.RunnerScaleSet{ID: 7, Name: "tewake", RunnerGroupID: 1, Labels: []scaleset.Label{{Name: "tewake"}}})
	if err != nil {
		t.Fatalf("fromScaleSet() error = %v", err)
	}
	if converted.Statistics != nil {
		t.Fatalf("Statistics = %#v, want nil for unknown upstream state", converted.Statistics)
	}
}

func TestToScaleSetLeavesLabelTypeForOfficialClientDefault(t *testing.T) {
	converted := toScaleSet(ScaleSet{ID: 7, Name: "tewake", Labels: []string{"tewake", "tewake-linux"}})
	if len(converted.Labels) != 2 {
		t.Fatalf("labels = %#v, want two labels", converted.Labels)
	}
	for _, label := range converted.Labels {
		if label.Type != "" {
			t.Fatalf("label %#v has Type %q, want empty so the official client applies its default", label, label.Type)
		}
	}
}

func TestValidateJITResultRejectsMismatchedRunnerIdentity(t *testing.T) {
	request := JITRequest{ScaleSetID: 7, Name: "runner-7", WorkFolder: "_work"}
	valid := &scaleset.RunnerScaleSetJitRunnerConfig{EncodedJITConfig: "opaque", Runner: &scaleset.RunnerReference{ID: 1, Name: request.Name, RunnerScaleSetID: int(request.ScaleSetID)}}
	if err := validateJITResult(valid, request); err != nil {
		t.Fatalf("valid JIT result error = %v", err)
	}
	for _, result := range []*scaleset.RunnerScaleSetJitRunnerConfig{
		{EncodedJITConfig: "opaque", Runner: &scaleset.RunnerReference{ID: 0, Name: request.Name, RunnerScaleSetID: 7}},
		{EncodedJITConfig: "opaque", Runner: &scaleset.RunnerReference{ID: 1, Name: "other", RunnerScaleSetID: 7}},
		{EncodedJITConfig: "opaque", Runner: &scaleset.RunnerReference{ID: 1, Name: request.Name, RunnerScaleSetID: 8}},
	} {
		if !errors.Is(validateJITResult(result, request), ErrInvalidPreviewResponse) {
			t.Fatal("mismatched JIT result accepted")
		}
	}
}
