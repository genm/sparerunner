package github

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type fakeResolver struct{ addresses []netip.Addr }

func (r fakeResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return r.addresses, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func recordResponseStatus(t *testing.T, ctx context.Context, statusCode int) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := endpointTransport{next: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: statusCode, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEndpointTransportRecordsFinalResponseStatusPerOperation(t *testing.T) {
	ctx, recorder := withProviderStatusRecorder(context.Background())
	recordResponseStatus(t, ctx, http.StatusUnauthorized)
	recordResponseStatus(t, ctx, http.StatusServiceUnavailable)
	if statusCode := recorder.responseStatusCode(); statusCode != http.StatusServiceUnavailable {
		t.Fatalf("recorded status = %d, want %d", statusCode, http.StatusServiceUnavailable)
	}
}

func TestEndpointTransportClearsEarlierStatusWhenFinalRequestHasNoResponse(t *testing.T) {
	ctx, recorder := withProviderStatusRecorder(context.Background())
	recordResponseStatus(t, ctx, http.StatusOK)
	transport := endpointTransport{next: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network unavailable")
	})}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://api.github.com/test",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.RoundTrip(request); err == nil {
		t.Fatal("final network failure returned success")
	}
	if statusCode := recorder.responseStatusCode(); statusCode != 0 {
		t.Fatalf(
			"final network failure retained prior HTTP status %d",
			statusCode,
		)
	}
}

func TestEndpointTransportClearsEarlierStatusWhenFinalEndpointIsRejected(t *testing.T) {
	ctx, recorder := withProviderStatusRecorder(context.Background())
	recordResponseStatus(t, ctx, http.StatusOK)
	transport := endpointTransport{next: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("unsafe endpoint reached the network")
		return nil, nil
	})}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://attacker.invalid/test",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.RoundTrip(request); !errors.Is(err, ErrUnsafeGitHubEndpoint) {
		t.Fatalf("unsafe endpoint error = %v", err)
	}
	if statusCode := recorder.responseStatusCode(); statusCode != 0 {
		t.Fatalf(
			"unsafe final request retained prior HTTP status %d",
			statusCode,
		)
	}
}

func TestEndpointTransportRecordsResponseStatusConcurrentlyWithoutCrossTalk(t *testing.T) {
	const requests = 64
	transport := endpointTransport{next: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		statusCode, err := strconv.Atoi(request.Header.Get("X-Test-Status"))
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: statusCode, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	errors := make(chan error, requests)
	for index := 0; index < requests; index++ {
		go func(index int) {
			ctx, recorder := withProviderStatusRecorder(context.Background())
			wantStatus := http.StatusBadRequest + index
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/test", nil)
			if err == nil {
				request.Header.Set("X-Test-Status", strconv.Itoa(wantStatus))
				response, roundTripErr := transport.RoundTrip(request)
				if roundTripErr != nil {
					err = roundTripErr
				} else {
					err = response.Body.Close()
				}
			}
			if err == nil && recorder.responseStatusCode() != wantStatus {
				err = fmt.Errorf("recorded status = %d, want %d", recorder.responseStatusCode(), wantStatus)
			}
			errors <- err
		}(index)
	}
	for range requests {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
}

func TestFiniteOperationContextTimesOut(t *testing.T) {
	ctx, cancel := withFiniteOperationTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("context error = %v, want deadline exceeded", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("finite GitHub operation context did not time out")
	}
}

func TestFiniteOperationContextUsesDefaultDeadlineAndPreservesNilValidationPath(t *testing.T) {
	startedAt := time.Now()
	ctx, cancel := WithFiniteOperationTimeout(context.Background())
	defer cancel()
	finishedAt := time.Now()
	deadline, ok := ctx.Deadline()
	if !ok ||
		deadline.Before(startedAt.Add(finiteOperationTimeout)) ||
		deadline.After(finishedAt.Add(finiteOperationTimeout)) {
		t.Fatalf("deadline = (%v, %t), want within the finite operation budget", deadline, ok)
	}

	nilContext, nilCancel := WithFiniteOperationTimeout(nil)
	defer nilCancel()
	if nilContext != nil {
		t.Fatalf("nil context became %#v, want existing caller validation path preserved", nilContext)
	}
}

func TestHardenedClientLeavesGlobalTimeoutUnsetForLongPolls(t *testing.T) {
	client := newHardenedRetryableClientWith(
		fakeResolver{},
		func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("not used") },
		roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}),
	)
	if timeout := client.StandardClient().Timeout; timeout != 0 {
		t.Fatalf("global HTTP timeout = %s, want zero so GetMessage can long poll", timeout)
	}
}

func TestTransportRejectsCredentialEndpointsBeforeRoundTrip(t *testing.T) {
	called := false
	transport := endpointTransport{next: roundTripFunc(func(*http.Request) (*http.Response, error) { called = true; return nil, nil })}
	for _, raw := range []string{"http://api.github.com/x", "https://localhost/x", "https://evil.actions.githubusercontent.com.attacker.test/x", "https://api.github.com:444/x"} {
		u, _ := url.Parse(raw)
		request := &http.Request{URL: u, Header: http.Header{"Authorization": []string{"Bearer canary"}}}
		if _, err := transport.RoundTrip(request); !errors.Is(err, ErrUnsafeGitHubEndpoint) {
			t.Fatalf("%s error = %v", raw, err)
		}
	}
	if called {
		t.Fatal("unsafe endpoint reached next transport")
	}
}

func TestTransportAllowsGitHubAPIsAndActionsHost(t *testing.T) {
	for _, raw := range []string{"https://api.github.com/app/installations", "https://pipelines.actions.githubusercontent.com/queue"} {
		u, _ := url.Parse(raw)
		transport := endpointTransport{next: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		})}
		if _, err := transport.RoundTrip(&http.Request{URL: u, Header: http.Header{"Authorization": []string{"Bearer canary"}}}); err != nil {
			t.Fatalf("%s error = %v", raw, err)
		}
	}
}

func TestVettedDialerRejectsPrivateAndDialsVettedIP(t *testing.T) {
	dialed := ""
	dial := func(_ context.Context, _ string, address string) (net.Conn, error) {
		dialed = address
		left, _ := net.Pipe()
		return left, nil
	}
	blocked := vettedDialer(fakeResolver{addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}, dial)
	if _, err := blocked(context.Background(), "tcp", "api.github.com:443"); !errors.Is(err, ErrUnsafeGitHubEndpoint) {
		t.Fatalf("private dial error = %v", err)
	}
	allowed := vettedDialer(fakeResolver{addresses: []netip.Addr{netip.MustParseAddr("140.82.112.6")}}, dial)
	connection, err := allowed(context.Background(), "tcp", "api.github.com:443")
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
	if dialed != "140.82.112.6:443" {
		t.Fatalf("dialed %q", dialed)
	}
}

func TestVettedDialerFallsBackAcrossVettedAnswers(t *testing.T) {
	firstStarted := make(chan struct{})
	firstCancelled := make(chan struct{})
	dial := func(ctx context.Context, _ string, address string) (net.Conn, error) {
		if address == "140.82.112.6:443" {
			close(firstStarted)
			<-ctx.Done()
			close(firstCancelled)
			return nil, ctx.Err()
		}
		<-firstStarted
		left, _ := net.Pipe()
		return left, nil
	}
	dialer := vettedDialer(fakeResolver{addresses: []netip.Addr{
		netip.MustParseAddr("140.82.112.6"),
		netip.MustParseAddr("140.82.113.6"),
	}}, dial)
	connection, err := dialer(context.Background(), "tcp", "api.github.com:443")
	if err != nil {
		t.Fatalf("vettedDialer() error = %v", err)
	}
	defer connection.Close()
	select {
	case <-firstCancelled:
	case <-time.After(time.Second):
		t.Fatal("blocked first vetted dial was not cancelled after fallback succeeded")
	}
}

type closeTrackingReadCloser struct {
	io.Reader
	closed atomic.Int32
}

func (b *closeTrackingReadCloser) Close() error {
	b.closed.Add(1)
	return nil
}

func TestHardenedRetryableStandardClientClosesOversizeResponse(t *testing.T) {
	attempts := 0
	body := &closeTrackingReadCloser{Reader: bytes.NewReader(make([]byte, maxPreviewResponseBody+1))}
	client := newHardenedRetryableClientWith(fakeResolver{addresses: []netip.Addr{netip.MustParseAddr("140.82.112.6")}}, func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("not used") }, roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body}, nil
	}))
	request, _ := http.NewRequest(http.MethodPost, "https://api.github.com/test", nil)
	response, err := client.StandardClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(response.Body)
	response.Body.Close()
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("body error = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("POST attempts = %d, want 1", attempts)
	}
	if body.closed.Load() != 1 {
		t.Fatalf("oversize response close calls = %d, want 1", body.closed.Load())
	}
}

func TestHardenedRetryableStandardClientDoesNotRetryMutations(t *testing.T) {
	failures := []struct {
		name        string
		result      func() (*http.Response, error)
		wantRequest bool
	}{
		{name: "5xx", wantRequest: true, result: func() (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}},
		{name: "transport error", result: func() (*http.Response, error) { return nil, errors.New("transport canary") }},
		{name: "redirect", result: func() (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{"Location": []string{"https://api.github.com/redirect"}}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}},
	}
	for _, method := range []string{http.MethodPost, http.MethodPatch, http.MethodDelete} {
		for _, failure := range failures {
			t.Run(method+" "+failure.name, func(t *testing.T) {
				attempts := 0
				client := newHardenedRetryableClientWith(fakeResolver{}, func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("not used") }, roundTripFunc(func(*http.Request) (*http.Response, error) {
					attempts++
					return failure.result()
				}))
				request, _ := http.NewRequest(method, "https://api.github.com/test", nil)
				response, err := client.StandardClient().Do(request)
				if response != nil && response.Body != nil {
					response.Body.Close()
				}
				if attempts != 1 {
					t.Fatalf("%s %s attempts = %d, want 1", method, failure.name, attempts)
				}
				if failure.wantRequest && err != nil {
					t.Fatalf("%s %s request error = %v", method, failure.name, err)
				}
				if !failure.wantRequest && err == nil {
					t.Fatalf("%s %s succeeded, want failure", method, failure.name)
				}
			})
		}
	}
}

func TestRedirectPolicyRejectsEveryRedirect(t *testing.T) {
	from, _ := http.NewRequest(http.MethodGet, "https://api.github.com/a", nil)
	for _, raw := range []string{"https://api.github.com/b", "https://evil.test/a"} {
		to, _ := http.NewRequest(http.MethodGet, raw, nil)
		if !errors.Is(rejectRedirect(to, []*http.Request{from}), ErrUnsafeGitHubEndpoint) {
			t.Fatalf("redirect to %s accepted", raw)
		}
	}
}

func TestHostAndSpecialIPPolicy(t *testing.T) {
	if allowedGitHubHost("API.GITHUB.COM") {
		t.Fatal("case variant accepted")
	}
	for _, raw := range []string{
		"::ffff:127.0.0.1",
		"100.64.0.1",
		"192.0.0.1",
		"192.0.2.1",
		"198.18.0.1",
		"240.0.0.1",
		"64:ff9b::1",
		"100::1",
		"2001::1",
		"2001:db8::1",
		"2002::1",
		"3fff::1",
		"fec0::1",
	} {
		if !unsafeIP(netip.MustParseAddr(raw)) {
			t.Fatalf("special IP %s accepted", raw)
		}
	}
	if unsafeIP(netip.MustParseAddr("140.82.112.6")) {
		t.Fatal("public IP rejected")
	}
}
