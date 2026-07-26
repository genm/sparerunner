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

	"github.com/hashicorp/go-retryablehttp"
)

const maxPreviewResponseBody = 1 << 20 // 1 MiB: JSON control-plane responses, never runner logs or artifacts.

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
	client := &http.Client{Transport: outer, CheckRedirect: sameOriginRedirect}
	return &retryablehttp.Client{HTTPClient: client, RetryMax: 0, CheckRetry: neverRetry, Logger: nil}
}

func neverRetry(context.Context, *http.Response, error) (bool, error) { return false, nil }

func sameOriginRedirect(request *http.Request, via []*http.Request) error {
	if len(via) == 0 || request.URL.Scheme != via[0].URL.Scheme || request.URL.Host != via[0].URL.Host {
		return ErrUnsafeGitHubEndpoint
	}
	return nil
}

type endpointTransport struct{ next http.RoundTripper }

func (t endpointTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := validateEndpoint(request.URL); err != nil {
		return nil, err
	}
	response, err := t.next.RoundTrip(request)
	if err != nil || response == nil {
		return response, err
	}
	response.Body = &boundedReadCloser{ReadCloser: response.Body, remaining: maxPreviewResponseBody}
	return response, nil
}

func validateEndpoint(endpoint *url.URL) error {
	if endpoint == nil || endpoint.Scheme != "https" || endpoint.User != nil || endpoint.Port() != "" || !allowedGitHubHost(endpoint.Hostname()) {
		return ErrUnsafeGitHubEndpoint
	}
	return nil
}

func allowedGitHubHost(host string) bool {
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
		for _, address := range addresses {
			if unsafeIP(address) {
				continue
			}
			return dial(ctx, network, net.JoinHostPort(address.String(), port))
		}
		return nil, ErrUnsafeGitHubEndpoint
	}
}

func unsafeIP(ip netip.Addr) bool {
	return !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast()
}

type boundedReadCloser struct {
	io.ReadCloser
	remaining int64
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
		return max(0, n-1), ErrResponseTooLarge
	}
	return n, err
}

func (b *boundedReadCloser) Close() error { return b.ReadCloser.Close() }

func endpointError(endpoint *url.URL) error {
	return fmt.Errorf("%w: %s", ErrUnsafeGitHubEndpoint, endpoint.Hostname())
}
