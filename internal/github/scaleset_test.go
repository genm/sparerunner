package github

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-retryablehttp"
)

func TestNewAppClientAcceptsOnlyGitHubDotComScopes(t *testing.T) {
	validURLs := []string{
		"https://github.com/example-org",
		"https://github.com/example-org/sparerunner",
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
		{name: "too many path parts", configURL: "https://github.com/example-org/sparerunner/settings"},
		{name: "encoded path", configURL: "https://github.com/example-org%2Fsparerunner"},
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
	converted, err := fromScaleSet(scaleset.RunnerScaleSet{ID: 7, Name: "sparerunner", RunnerGroupID: 1, Labels: []scaleset.Label{{Name: "sparerunner"}}})
	if err != nil {
		t.Fatalf("fromScaleSet() error = %v", err)
	}
	if converted.Statistics != nil {
		t.Fatalf("Statistics = %#v, want nil for unknown upstream state", converted.Statistics)
	}
}

func TestToScaleSetLeavesLabelTypeForOfficialClientDefault(t *testing.T) {
	converted := toScaleSet(ScaleSet{ID: 7, Name: "sparerunner", Labels: []string{"sparerunner", "sparerunner-linux"}})
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

func TestGenerateJITConfigExposesFinalProviderHTTPStatus(t *testing.T) {
	vendorError := errors.New(
		"vendor failure https://api.example.test/actions Bearer secret-token")
	client := &Client{
		generateJITConfig: func(
			ctx context.Context,
			_ *scaleset.RunnerScaleSetJitRunnerSetting,
			_ int,
		) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
			recordResponseStatus(t, ctx, http.StatusServiceUnavailable)
			return nil, vendorError
		},
	}
	_, err := client.GenerateJITConfig(context.Background(), JITRequest{
		ScaleSetID: 7,
		Name:       "sparerunner-runner",
		WorkFolder: "_work",
	})
	var statusError *ProviderHTTPStatusError
	if !errors.As(err, &statusError) ||
		statusError.StatusCode != http.StatusServiceUnavailable ||
		!errors.Is(err, vendorError) {
		t.Fatalf("JIT provider status error = %#v", err)
	}
	if strings.Contains(err.Error(), "api.example.test") ||
		strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("JIT provider error exposed sensitive vendor text: %q", err)
	}
}

func TestAcquireIDValidation(t *testing.T) {
	if err := validateAcquireIDs([]int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]int64{{}, {0}, {1, 1}} {
		if !errors.Is(validateAcquireIDs(ids), ErrInvalidPreviewResponse) {
			t.Fatalf("accepted %v", ids)
		}
	}
	if err := validateAcquireResponse([]int64{1, 2}, []int64{2}); err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]int64{{0}, {3}, {1, 1}} {
		if !errors.Is(validateAcquireResponse([]int64{1, 2}, ids), ErrInvalidPreviewResponse) {
			t.Fatalf("accepted response %v", ids)
		}
	}
}

func TestMessageSessionRejectsMissingOrZeroSession(t *testing.T) {
	if _, err := (&MessageSession{}).Snapshot(); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("nil session error=%v", err)
	}
	if _, err := (&MessageSession{client: &scaleset.MessageSessionClient{}, scaleSetID: 7}).Snapshot(); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("zero session error=%v", err)
	}
}

func TestMessageSessionOperationsFailClosedWithoutSession(t *testing.T) {
	var session *MessageSession
	if _, err := session.Poll(context.Background(), 0, 0); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("Poll() error = %v, want ErrInvalidSession", err)
	}
	if err := session.DeleteMessage(context.Background(), 1); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("DeleteMessage() error = %v, want ErrInvalidSession", err)
	}
	if _, err := session.AcquireJobs(context.Background(), []int64{1}); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("AcquireJobs() error = %v, want ErrInvalidSession", err)
	}
	if err := session.Close(context.Background()); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("Close() error = %v, want ErrInvalidSession", err)
	}
}

func validMessageSession() *MessageSession {
	return &MessageSession{
		client:     &scaleset.MessageSessionClient{},
		scaleSetID: 7,
		owner:      "example-org",
		readSession: func() scaleset.RunnerScaleSetSession {
			return scaleset.RunnerScaleSetSession{OwnerName: "example-org", RunnerScaleSet: &scaleset.RunnerScaleSet{ID: 7}}
		},
	}
}

func TestMessageSessionOperationErrorsExposeFinalProviderHTTPStatus(t *testing.T) {
	vendorError := errors.New("vendor failure https://queue.example.test/messages Bearer secret-token")
	testCases := []struct {
		name       string
		statusCode int
		call       func(*MessageSession) error
	}{
		{
			name:       "poll rate limited",
			statusCode: http.StatusTooManyRequests,
			call: func(session *MessageSession) error {
				session.getMessage = func(ctx context.Context, _ int, _ int) (*scaleset.RunnerScaleSetMessage, error) {
					recordResponseStatus(t, ctx, http.StatusTooManyRequests)
					return nil, vendorError
				}
				_, err := session.Poll(context.Background(), 0, 1)
				return err
			},
		},
		{
			name:       "delete unavailable",
			statusCode: http.StatusInternalServerError,
			call: func(session *MessageSession) error {
				session.deleteMessage = func(ctx context.Context, _ int) error {
					recordResponseStatus(t, ctx, http.StatusInternalServerError)
					return vendorError
				}
				return session.DeleteMessage(context.Background(), 1)
			},
		},
		{
			name:       "acquire unauthorized",
			statusCode: http.StatusUnauthorized,
			call: func(session *MessageSession) error {
				session.acquireJobs = func(ctx context.Context, _ []int64) ([]int64, error) {
					recordResponseStatus(t, ctx, http.StatusUnauthorized)
					return nil, vendorError
				}
				_, err := session.AcquireJobs(context.Background(), []int64{1})
				return err
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.call(validMessageSession())
			var statusError *ProviderHTTPStatusError
			if !errors.As(err, &statusError) {
				t.Fatalf("error = %v, want ProviderHTTPStatusError", err)
			}
			if statusError.StatusCode != testCase.statusCode {
				t.Fatalf("status = %d, want %d", statusError.StatusCode, testCase.statusCode)
			}
			if !errors.Is(err, vendorError) {
				t.Fatalf("error = %v, want original vendor error", err)
			}
			if strings.Contains(err.Error(), "queue.example.test") || strings.Contains(err.Error(), "secret-token") {
				t.Fatalf("provider status error exposed sensitive vendor text: %q", err)
			}
		})
	}
}

func TestMessageSessionKeepsNetworkErrorsUnwrappedByProviderStatus(t *testing.T) {
	vendorError := errors.New("network failure")
	session := validMessageSession()
	session.getMessage = func(context.Context, int, int) (*scaleset.RunnerScaleSetMessage, error) {
		return nil, vendorError
	}
	_, err := session.Poll(context.Background(), 0, 1)
	if !errors.Is(err, vendorError) {
		t.Fatalf("error = %v, want original network error", err)
	}
	var statusError *ProviderHTTPStatusError
	if errors.As(err, &statusError) {
		t.Fatalf("network error was classified with provider status: %#v", statusError)
	}
}

func TestMessageSessionSuccessfulOperationsRemainUnchanged(t *testing.T) {
	session := validMessageSession()
	session.getMessage = func(ctx context.Context, _ int, _ int) (*scaleset.RunnerScaleSetMessage, error) {
		recordResponseStatus(t, ctx, http.StatusAccepted)
		return nil, nil
	}
	if message, err := session.Poll(context.Background(), 0, 1); err != nil || message != nil {
		t.Fatalf("Poll() = (%#v, %v), want (nil, nil)", message, err)
	}

	session = validMessageSession()
	session.deleteMessage = func(ctx context.Context, _ int) error {
		recordResponseStatus(t, ctx, http.StatusNoContent)
		return nil
	}
	if err := session.DeleteMessage(context.Background(), 1); err != nil {
		t.Fatalf("DeleteMessage() error = %v", err)
	}

	session = validMessageSession()
	session.acquireJobs = func(ctx context.Context, _ []int64) ([]int64, error) {
		recordResponseStatus(t, ctx, http.StatusOK)
		return []int64{1}, nil
	}
	if ids, err := session.AcquireJobs(context.Background(), []int64{1}); err != nil || len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("AcquireJobs() = (%v, %v), want ([1], nil)", ids, err)
	}
}

func TestMessageSessionUsesFinalResponseStatusAndDoesNotFailSuccessfulOperations(t *testing.T) {
	session := validMessageSession()
	session.getMessage = func(ctx context.Context, _ int, _ int) (*scaleset.RunnerScaleSetMessage, error) {
		recordResponseStatus(t, ctx, http.StatusUnauthorized)
		recordResponseStatus(t, ctx, http.StatusInternalServerError)
		return nil, errors.New("vendor failure")
	}
	_, err := session.Poll(context.Background(), 0, 1)
	var statusError *ProviderHTTPStatusError
	if !errors.As(err, &statusError) || statusError.StatusCode != http.StatusInternalServerError {
		t.Fatalf("error = %v, want final status %d", err, http.StatusInternalServerError)
	}

	session = validMessageSession()
	session.getMessage = func(ctx context.Context, _ int, _ int) (*scaleset.RunnerScaleSetMessage, error) {
		recordResponseStatus(t, ctx, http.StatusUnauthorized)
		recordResponseStatus(t, ctx, http.StatusOK)
		return nil, nil
	}
	if _, err := session.Poll(context.Background(), 0, 1); err != nil {
		t.Fatalf("successful final response error = %v", err)
	}
}

func TestOpenMessageSessionUsesFreshHardenedClientPerConcurrentSession(t *testing.T) {
	const sessions = 32
	var (
		mu      sync.Mutex
		clients []*retryablehttp.Client
	)
	client := &Client{
		newSessionRetryableClient: newHardenedRetryableClient,
		openMessageSession: func(_ context.Context, _ int, _ string, retryableClient *retryablehttp.Client) (*scaleset.MessageSessionClient, error) {
			mu.Lock()
			defer mu.Unlock()
			clients = append(clients, retryableClient)
			return &scaleset.MessageSessionClient{}, nil
		},
		readMessageSession: func(*scaleset.MessageSessionClient) scaleset.RunnerScaleSetSession {
			return scaleset.RunnerScaleSetSession{OwnerName: "example-org", RunnerScaleSet: &scaleset.RunnerScaleSet{ID: 7}}
		},
	}

	var group sync.WaitGroup
	for id := 1; id <= sessions; id++ {
		group.Add(1)
		go func(id int) {
			defer group.Done()
			if _, err := client.OpenMessageSession(context.Background(), 7, "example-org"); err != nil {
				t.Errorf("OpenMessageSession(%d) error = %v", id, err)
			}
		}(id)
	}
	group.Wait()

	if len(clients) != sessions {
		t.Fatalf("opened clients = %d, want %d", len(clients), sessions)
	}
	for index, current := range clients {
		if current == nil || current.HTTPClient == nil || current.HTTPClient.Transport == nil {
			t.Fatalf("session client %d is incomplete: %#v", index, current)
		}
		for _, previous := range clients[:index] {
			if current == previous || current.HTTPClient == previous.HTTPClient || current.HTTPClient.Transport == previous.HTTPClient.Transport {
				t.Fatal("message sessions share mutable retryable HTTP client state")
			}
		}
	}
}

func TestValidateSessionBindingRejectsInitialAndRefreshedMismatches(t *testing.T) {
	valid := scaleset.RunnerScaleSetSession{
		OwnerName:      "example-org",
		RunnerScaleSet: &scaleset.RunnerScaleSet{ID: 7},
	}
	if err := validateSessionBinding(valid, 7, "example-org"); err != nil {
		t.Fatalf("initial binding error = %v", err)
	}

	for _, refreshed := range []scaleset.RunnerScaleSetSession{
		{OwnerName: "example-org"},
		{OwnerName: "other-org", RunnerScaleSet: &scaleset.RunnerScaleSet{ID: 7}},
		{OwnerName: "example-org", RunnerScaleSet: &scaleset.RunnerScaleSet{ID: 8}},
	} {
		if !errors.Is(validateSessionBinding(refreshed, 7, "example-org"), ErrInvalidSession) {
			t.Fatalf("refreshed session %#v accepted", refreshed)
		}
	}
}

func TestMessageSessionSnapshotRejectsRefreshedBindingMismatch(t *testing.T) {
	current := scaleset.RunnerScaleSetSession{
		OwnerName:      "example-org",
		RunnerScaleSet: &scaleset.RunnerScaleSet{ID: 7},
		Statistics:     &scaleset.RunnerScaleSetStatistic{},
	}
	session := &MessageSession{
		client:     &scaleset.MessageSessionClient{},
		scaleSetID: 7,
		owner:      "example-org",
		readSession: func() scaleset.RunnerScaleSetSession {
			return current
		},
	}
	if _, err := session.currentSession(); err != nil {
		t.Fatalf("initial session error = %v", err)
	}
	current.RunnerScaleSet = &scaleset.RunnerScaleSet{ID: 8}
	if _, err := session.currentSession(); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("refreshed mismatched session error = %v, want ErrInvalidSession", err)
	}
}

func TestMessageSessionRejectsMismatchedBindingBeforeMutation(t *testing.T) {
	deleteCalls := 0
	acquireCalls := 0
	session := &MessageSession{
		client:     &scaleset.MessageSessionClient{},
		scaleSetID: 7,
		owner:      "example-org",
		readSession: func() scaleset.RunnerScaleSetSession {
			return scaleset.RunnerScaleSetSession{OwnerName: "other-org", RunnerScaleSet: &scaleset.RunnerScaleSet{ID: 8}}
		},
		deleteMessage: func(context.Context, int) error {
			deleteCalls++
			return nil
		},
		acquireJobs: func(context.Context, []int64) ([]int64, error) {
			acquireCalls++
			return []int64{1}, nil
		},
	}
	if err := session.DeleteMessage(context.Background(), 1); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("DeleteMessage() error = %v, want ErrInvalidSession", err)
	}
	if _, err := session.AcquireJobs(context.Background(), []int64{1}); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("AcquireJobs() error = %v, want ErrInvalidSession", err)
	}
	if deleteCalls != 0 || acquireCalls != 0 {
		t.Fatalf("mutations invoked despite mismatched binding: delete=%d acquire=%d", deleteCalls, acquireCalls)
	}
}

func TestValidateExpectedScaleSetRequiresUpdateIDAndDisableUpdateExactness(t *testing.T) {
	requested := ScaleSet{ID: 7, Name: "sparerunner", RunnerGroupID: 1, Labels: []string{"sparerunner"}, DisableUpdate: true}
	if err := validateExpectedScaleSet(requested, requested, true); err != nil {
		t.Fatalf("matching update rejected: %v", err)
	}
	for _, response := range []ScaleSet{
		{ID: 8, Name: "sparerunner", RunnerGroupID: 1, Labels: []string{"sparerunner"}, DisableUpdate: true},
		{ID: 7, Name: "sparerunner", RunnerGroupID: 1, Labels: []string{"sparerunner"}, DisableUpdate: false},
	} {
		if !errors.Is(validateExpectedScaleSet(response, requested, true), ErrInvalidPreviewResponse) {
			t.Fatalf("invalid update response accepted: %#v", response)
		}
	}
	created := requested
	created.ID = 8
	if err := validateExpectedScaleSet(created, requested, false); err != nil {
		t.Fatalf("created response with assigned ID rejected: %v", err)
	}
}
