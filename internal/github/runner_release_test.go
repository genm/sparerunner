package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type runnerReleaseRoundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip runnerReleaseRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

type runnerReleaseTrackingBody struct {
	io.Reader
	closed int
}

func (body *runnerReleaseTrackingBody) Close() error {
	body.closed++
	return nil
}

func newRunnerReleaseTestObserver(t *testing.T, now time.Time, result func(*http.Request) (*http.Response, error)) *RunnerReleaseObserver {
	t.Helper()
	observer, err := NewRunnerReleaseObserverWithHTTPClient(
		&http.Client{Transport: runnerReleaseRoundTripFunc(result)},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	return observer
}

func TestRunnerReleaseObserverReadsFixedOfficialLatestRelease(t *testing.T) {
	now := time.Date(2026, time.July, 27, 12, 0, 0, 123, time.FixedZone("test", 9*60*60))
	publishedAt := time.Date(2026, time.July, 26, 2, 3, 4, 0, time.UTC)
	body := &runnerReleaseTrackingBody{Reader: strings.NewReader(`{
		"tag_name": "v2.336.0",
		"published_at": "2026-07-26T02:03:04Z",
		"assets": [{"name": "actions-runner-linux-x64-2.336.0.tar.gz"}]
	}`)}
	observer := newRunnerReleaseTestObserver(t, now, func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.String() != officialRunnerLatestReleaseURL {
			t.Fatalf("request = %s %s", request.Method, request.URL)
		}
		if request.Header.Get("Accept") != "application/vnd.github+json" ||
			request.Header.Get("X-GitHub-Api-Version") != "2022-11-28" ||
			request.Header.Get("User-Agent") == "" {
			t.Fatalf("headers = %#v", request.Header)
		}
		if request.Header.Get("Authorization") != "" {
			t.Fatal("release observation unexpectedly sent authorization")
		}
		return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
	})

	got, err := observer.Latest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != "2.336.0" || !got.PublishedAt.Equal(publishedAt) || !got.ObservedAt.Equal(now) {
		t.Fatalf("release = %#v", got)
	}
	if body.closed != 1 {
		t.Fatalf("response close calls = %d, want 1", body.closed)
	}
}

func TestRunnerReleaseObserverReturnsSafeProviderStatus(t *testing.T) {
	for _, statusCode := range []int{http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			observer := newRunnerReleaseTestObserver(t, time.Now(), func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: statusCode,
					Body:       io.NopCloser(strings.NewReader(`{"message":"provider-body-canary"}`)),
				}, nil
			})
			_, err := observer.Latest(context.Background())
			var statusFailure *ProviderHTTPStatusError
			if !errors.As(err, &statusFailure) || statusFailure.StatusCode != statusCode ||
				!errors.Is(err, ErrRunnerReleaseRequest) {
				t.Fatalf("error = %#v", err)
			}
			if strings.Contains(err.Error(), "provider-body-canary") ||
				strings.Contains(err.Error(), officialRunnerLatestReleaseURL) {
				t.Fatalf("unsafe error = %q", err)
			}
		})
	}
}

func TestRunnerReleaseObserverReturnsSafeTransportFailure(t *testing.T) {
	canary := errors.New("GET https://api.github.com/private?token=transport-secret-canary")
	observer := newRunnerReleaseTestObserver(t, time.Now(), func(*http.Request) (*http.Response, error) {
		return nil, canary
	})
	_, err := observer.Latest(context.Background())
	if !errors.Is(err, ErrRunnerReleaseRequest) || !errors.Is(err, canary) {
		t.Fatalf("error = %#v", err)
	}
	if strings.Contains(err.Error(), "transport-secret-canary") ||
		strings.Contains(err.Error(), "api.github.com") {
		t.Fatalf("unsafe error = %q", err)
	}
}

func TestRunnerReleaseObserverRejectsMalformedJSONWithoutDisclosingBody(t *testing.T) {
	observer := newRunnerReleaseTestObserver(t, time.Now(), func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"tag_name":"malformed-body-canary"`)),
		}, nil
	})
	_, err := observer.Latest(context.Background())
	if !errors.Is(err, ErrInvalidRunnerReleaseResponse) {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), "malformed-body-canary") {
		t.Fatalf("unsafe error = %q", err)
	}
}

func TestRunnerReleaseObserverRejectsOversizeResponse(t *testing.T) {
	body := &runnerReleaseTrackingBody{Reader: bytes.NewReader(bytes.Repeat([]byte("x"), maxRunnerReleaseResponseBody+1))}
	observer := newRunnerReleaseTestObserver(t, time.Now(), func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
	})
	_, err := observer.Latest(context.Background())
	if !errors.Is(err, ErrRunnerReleaseResponseTooLarge) {
		t.Fatalf("error = %v", err)
	}
	if body.closed != 1 {
		t.Fatalf("response close calls = %d, want 1", body.closed)
	}
}

func TestRunnerReleaseObserverRejectsFuturePublishedAt(t *testing.T) {
	now := time.Date(2026, time.July, 27, 0, 0, 0, 0, time.UTC)
	observer := newRunnerReleaseTestObserver(t, now, func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(
				`{"tag_name":"v2.336.0","published_at":"2026-07-27T00:00:01Z"}`,
			)),
		}, nil
	})
	if _, err := observer.Latest(context.Background()); !errors.Is(err, ErrInvalidRunnerReleaseResponse) {
		t.Fatalf("error = %v", err)
	}
}

func TestRunnerReleaseObserverRejectsInvalidTags(t *testing.T) {
	now := time.Date(2026, time.July, 27, 0, 0, 0, 0, time.UTC)
	for _, tag := range []string{
		"2.336.0",
		"v2.336",
		"v2.336.0.1",
		"v02.336.0",
		"v2.0336.0",
		"v2.336.00",
		"v2.336.0-beta",
		"v2.336.0\n",
		"v" + strings.Repeat("1", maxRunnerReleaseTagLength),
	} {
		t.Run(strings.ReplaceAll(tag, "\n", "newline"), func(t *testing.T) {
			payload := `{"tag_name":` + string(mustJSON(t, tag)) + `,"published_at":"2026-07-26T00:00:00Z"}`
			observer := newRunnerReleaseTestObserver(t, now, func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(payload))}, nil
			})
			if _, err := observer.Latest(context.Background()); !errors.Is(err, ErrInvalidRunnerReleaseResponse) {
				t.Fatalf("tag %q error = %v", tag, err)
			}
		})
	}
}

func mustJSON(t *testing.T, value string) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
