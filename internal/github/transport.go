package github

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

const (
	maxPreviewResponseBody = 1 << 20 // 1 MiB: JSON control-plane responses, never runner logs or artifacts.
	maxVettedDialAttempts  = 4       // Bound untrusted DNS fan-out while racing safe answers under the request context.
	// Keep finite control-plane operations within the same request budget used
	// by the runner-release observer. GetMessage long polling deliberately does
	// not use this budget.
	finiteOperationTimeout = 30 * time.Second
)

var (
	ErrUnsafeGitHubEndpoint = errors.New("unsafe GitHub endpoint")
	ErrResponseTooLarge     = errors.New("GitHub response exceeds adapter body limit")
)

type resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}
type dialContext func(context.Context, string, string) (net.Conn, error)

// newHardenedRetryableClient is the production HTTP seam used by the preview
// client. Environment proxies are intentionally not consulted: credentials are
// sent only after endpoint validation and a vetted direct dial.
func newHardenedRetryableClient() *retryablehttp.Client {
	return newHardenedRetryableClientWith(net.DefaultResolver, (&net.Dialer{}).DialContext, nil)
}

func newHardenedRetryableClientWith(r resolver, dial dialContext, next http.RoundTripper) *retryablehttp.Client {
	if next == nil {
		next = &http.Transport{Proxy: nil, DialContext: vettedDialer(r, dial)}
	}
	policy := endpointTransport{next: next}
	outer := &http.Transport{Proxy: nil}
	outer.RegisterProtocol("https", policy)
	outer.RegisterProtocol("http", policy)
	client := &http.Client{Transport: outer, CheckRedirect: rejectRedirect}
	return &retryablehttp.Client{HTTPClient: client, RetryMax: 0, CheckRetry: neverRetry, Logger: nil}
}

func neverRetry(context.Context, *http.Response, error) (bool, error) { return false, nil }

// WithFiniteOperationTimeout returns a child context for a finite GitHub
// control-plane operation. Callers must not use it for GetMessage long polling.
// A nil context is retained so existing API boundary validation can reject it.
func WithFiniteOperationTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return withFiniteOperationTimeout(ctx, finiteOperationTimeout)
}

func withFiniteOperationTimeout(
	ctx context.Context,
	timeout time.Duration,
) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return nil, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// Redirects can replay a mutating request after the first endpoint already applied
// it. Canonical GitHub API endpoints must respond directly; reconciliation, not an
// HTTP redirect or retry, owns recovery from an ambiguous result.
func rejectRedirect(*http.Request, []*http.Request) error { return ErrUnsafeGitHubEndpoint }

type endpointTransport struct{ next http.RoundTripper }

func (t endpointTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil {
		return nil, ErrUnsafeGitHubEndpoint
	}
	// One logical scale-set operation may first refresh a token and then issue
	// the actual API request. Clear the earlier response before every attempt,
	// including endpoint validation, so a final transport/rejection failure
	// cannot inherit a successful refresh status.
	resetProviderResponseStatus(request.Context())
	if err := validateEndpoint(request.URL); err != nil {
		return nil, err
	}
	response, err := t.next.RoundTrip(request)
	if err != nil || response == nil {
		return response, err
	}
	recordProviderResponseStatus(request.Context(), response.StatusCode)
	response.Body = &boundedReadCloser{ReadCloser: response.Body, remaining: maxPreviewResponseBody}
	return response, nil
}

// providerStatusRecorder is scoped to one public message-session operation.
// The official client can refresh an expired token and issue several requests;
// retaining the final response lets callers classify only the operation's
// terminal provider failure without parsing the vendor's error text.
type providerStatusRecorder struct{ status atomic.Int64 }

type providerStatusRecorderContextKey struct{}

func withProviderStatusRecorder(ctx context.Context) (context.Context, *providerStatusRecorder) {
	recorder := &providerStatusRecorder{}
	if ctx == nil {
		// Preserve the official client's existing invalid-context failure path:
		// its request construction remains inside contain at the call site.
		return nil, recorder
	}
	return context.WithValue(ctx, providerStatusRecorderContextKey{}, recorder), recorder
}

func recordProviderResponseStatus(ctx context.Context, statusCode int) {
	if statusCode <= 0 {
		return
	}
	recorder, _ := ctx.Value(providerStatusRecorderContextKey{}).(*providerStatusRecorder)
	if recorder != nil {
		recorder.status.Store(int64(statusCode))
	}
}

func resetProviderResponseStatus(ctx context.Context) {
	recorder, _ := ctx.Value(providerStatusRecorderContextKey{}).(*providerStatusRecorder)
	if recorder != nil {
		recorder.status.Store(0)
	}
}

func (r *providerStatusRecorder) responseStatusCode() int {
	if r == nil {
		return 0
	}
	return int(r.status.Load())
}

func validateEndpoint(endpoint *url.URL) error {
	if endpoint == nil || endpoint.Scheme != "https" || endpoint.User != nil || endpoint.Port() != "" || !allowedGitHubHost(endpoint.Hostname()) {
		return ErrUnsafeGitHubEndpoint
	}
	return nil
}

func allowedGitHubHost(host string) bool {
	if host != strings.ToLower(host) {
		return false
	}
	if host == "github.com" || host == "api.github.com" {
		return true
	}
	const suffix = ".actions.githubusercontent.com"
	return strings.HasSuffix(host, suffix) && len(strings.TrimSuffix(host, suffix)) > 0
}

func vettedDialer(r resolver, dial dialContext) dialContext {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || port != "443" || !allowedGitHubHost(host) {
			return nil, ErrUnsafeGitHubEndpoint
		}
		addresses, err := r.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		candidates := make([]netip.Addr, 0, min(len(addresses), maxVettedDialAttempts))
		for _, address := range addresses {
			if unsafeIP(address) || len(candidates) == maxVettedDialAttempts {
				continue
			}
			candidates = append(candidates, address)
		}
		if len(candidates) == 0 {
			return nil, ErrUnsafeGitHubEndpoint
		}

		// A valid DNS answer can still blackhole a TCP connect. Race a small,
		// vetted set under the caller context so one stalled answer cannot prevent
		// fallback; cancellation stops losing default-net.Dialer attempts.
		raceContext, cancel := context.WithCancel(ctx)
		defer cancel()
		type dialResult struct {
			connection net.Conn
			err        error
		}
		results := make(chan dialResult)
		for _, candidate := range candidates {
			go func(candidate netip.Addr) {
				connection, err := dial(raceContext, network, net.JoinHostPort(candidate.String(), port))
				select {
				case results <- dialResult{connection: connection, err: err}:
				case <-raceContext.Done():
					if connection != nil {
						_ = connection.Close()
					}
				}
			}(candidate)
		}
		var lastDialError error
		for range candidates {
			select {
			case result := <-results:
				if result.err == nil {
					return result.connection, nil
				}
				lastDialError = result.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, lastDialError
	}
}

func unsafeIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	for _, prefix := range blockedSpecialPrefixes {
		if prefix.Contains(ip) {
			return true
		}
	}
	return false
}

var blockedSpecialPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // shared address space
	netip.MustParsePrefix("192.0.0.0/24"),  // IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),  // documentation
	netip.MustParsePrefix("192.31.196.0/24"),
	netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.175.48.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"), // benchmarking
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"), // reserved
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
	netip.MustParsePrefix("fec0::/10"),
}

type boundedReadCloser struct {
	io.ReadCloser
	remaining int64
	closeOnce sync.Once
	closeErr  error
}

func (b *boundedReadCloser) Read(data []byte) (int, error) {
	if b.remaining < 0 {
		return 0, ErrResponseTooLarge
	}
	if int64(len(data)) > b.remaining+1 {
		data = data[:b.remaining+1]
	}
	n, err := b.ReadCloser.Read(data)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		_ = b.Close()
		return max(0, n-1), ErrResponseTooLarge
	}
	return n, err
}

func (b *boundedReadCloser) Close() error {
	b.closeOnce.Do(func() { b.closeErr = b.ReadCloser.Close() })
	return b.closeErr
}

func endpointError(endpoint *url.URL) error {
	return fmt.Errorf("%w: %s", ErrUnsafeGitHubEndpoint, endpoint.Hostname())
}
