package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"time"
)

const (
	officialRunnerLatestReleaseURL = "https://api.github.com/repos/actions/runner/releases/latest"
	maxRunnerReleaseResponseBody   = 64 << 10 // 64 KiB is ample for GitHub's release metadata, never assets.
	maxRunnerReleaseTagLength      = 65       // leading "v" plus the persisted 64-byte version ceiling.
	runnerReleaseRequestTimeout    = 30 * time.Second
)

var (
	ErrRunnerReleaseRequest          = errors.New("GitHub runner release request failed")
	ErrInvalidRunnerReleaseResponse  = errors.New("invalid GitHub runner release response")
	ErrRunnerReleaseResponseTooLarge = errors.New("GitHub runner release response exceeds adapter body limit")
	ErrRunnerReleaseClock            = errors.New("GitHub runner release observation clock is unavailable")
	ErrRunnerReleaseClient           = errors.New("GitHub runner release HTTP client is invalid")
)

var stableRunnerReleaseTag = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// RunnerRelease is one successful read of GitHub's latest stable actions/runner
// release. Version omits the provider's leading "v" so it can be compared with
// the official runner's own version string without local normalization.
type RunnerRelease struct {
	Version     string
	PublishedAt time.Time
	ObservedAt  time.Time
}

// RunnerReleaseObserver reads the fixed GitHub.com actions/runner latest
// release endpoint. It is deliberately independent of GitHub App credentials.
type RunnerReleaseObserver struct {
	client *http.Client
	now    func() time.Time
}

// NewRunnerReleaseObserver returns the production observer. The underlying
// transport applies Tewake's GitHub endpoint and DNS checks, ignores environment
// proxies, rejects redirects, and does not retry the read behind the caller.
func NewRunnerReleaseObserver() *RunnerReleaseObserver {
	client := newHardenedRetryableClient().StandardClient()
	client.Timeout = runnerReleaseRequestTimeout
	return &RunnerReleaseObserver{client: client, now: time.Now}
}

// NewRunnerReleaseObserverWithHTTPClient exposes the HTTP and clock seams for
// deterministic contract tests. The endpoint remains fixed and redirects are
// rejected even when a caller supplies a client. A nil/default transport is
// rejected so production code cannot accidentally regain environment proxies.
func NewRunnerReleaseObserverWithHTTPClient(client *http.Client, now func() time.Time) (*RunnerReleaseObserver, error) {
	if client == nil || client.Transport == nil || now == nil {
		return nil, ErrRunnerReleaseClient
	}
	clone := *client
	clone.CheckRedirect = rejectRedirect
	return &RunnerReleaseObserver{client: &clone, now: now}, nil
}

// Latest reads and validates the latest stable actions/runner release. Response
// bodies, provider URLs, and transport error strings are not included in the
// returned error's default text. The underlying cause remains available to
// errors.Is/errors.As for immediate classification.
func (observer *RunnerReleaseObserver) Latest(ctx context.Context) (RunnerRelease, error) {
	if observer == nil || observer.client == nil || observer.now == nil {
		return RunnerRelease{}, ErrRunnerReleaseClient
	}
	if ctx == nil {
		return RunnerRelease{}, safeRunnerReleaseError(ErrRunnerReleaseRequest, errors.New("nil context"))
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, officialRunnerLatestReleaseURL, nil)
	if err != nil {
		return RunnerRelease{}, safeRunnerReleaseError(ErrRunnerReleaseRequest, err)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("User-Agent", "tewake-runner-release-observer")

	response, err := observer.client.Do(request)
	if err != nil {
		return RunnerRelease{}, safeRunnerReleaseError(ErrRunnerReleaseRequest, err)
	}
	if response == nil || response.Body == nil {
		return RunnerRelease{}, ErrInvalidRunnerReleaseResponse
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return RunnerRelease{}, &ProviderHTTPStatusError{
			StatusCode: response.StatusCode,
			Err:        ErrRunnerReleaseRequest,
		}
	}

	body, err := readRunnerReleaseBody(response.Body)
	if err != nil {
		return RunnerRelease{}, err
	}

	var payload struct {
		TagName     string `json:"tag_name"`
		PublishedAt string `json:"published_at"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return RunnerRelease{}, safeRunnerReleaseError(ErrInvalidRunnerReleaseResponse, err)
	}

	version, ok := parseStableRunnerReleaseTag(payload.TagName)
	if !ok {
		return RunnerRelease{}, ErrInvalidRunnerReleaseResponse
	}
	publishedAt, err := time.Parse(time.RFC3339, payload.PublishedAt)
	if err != nil || publishedAt.IsZero() {
		return RunnerRelease{}, safeRunnerReleaseError(ErrInvalidRunnerReleaseResponse, err)
	}
	publishedAt = publishedAt.UTC()

	observedAt := observer.now().UTC()
	if observedAt.IsZero() {
		return RunnerRelease{}, ErrRunnerReleaseClock
	}
	if publishedAt.After(observedAt) {
		return RunnerRelease{}, ErrInvalidRunnerReleaseResponse
	}

	return RunnerRelease{
		Version:     version,
		PublishedAt: publishedAt,
		ObservedAt:  observedAt,
	}, nil
}

func readRunnerReleaseBody(body io.Reader) ([]byte, error) {
	limited := &io.LimitedReader{R: body, N: maxRunnerReleaseResponseBody + 1}
	content, err := io.ReadAll(limited)
	if err != nil {
		return nil, safeRunnerReleaseError(ErrInvalidRunnerReleaseResponse, err)
	}
	if len(content) > maxRunnerReleaseResponseBody {
		return nil, ErrRunnerReleaseResponseTooLarge
	}
	return content, nil
}

func parseStableRunnerReleaseTag(tag string) (string, bool) {
	if len(tag) > maxRunnerReleaseTagLength {
		return "", false
	}
	matches := stableRunnerReleaseTag.FindStringSubmatch(tag)
	if matches == nil {
		return "", false
	}
	return tag[1:], true
}

type runnerReleaseError struct {
	public error
	cause  error
}

func safeRunnerReleaseError(public, cause error) error {
	if cause == nil {
		return public
	}
	return &runnerReleaseError{public: public, cause: cause}
}

func (failure *runnerReleaseError) Error() string {
	if failure == nil || failure.public == nil {
		return "GitHub runner release observation failed"
	}
	return failure.public.Error()
}

func (failure *runnerReleaseError) Unwrap() []error {
	if failure == nil {
		return nil
	}
	return []error{failure.public, failure.cause}
}
