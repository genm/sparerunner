package github

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"testing"
)

type fakeResolver struct{ addresses []netip.Addr }

func (r fakeResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return r.addresses, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

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

func TestHardenedRetryClientLimitsBodyAndDoesNotRetryPOST(t *testing.T) {
	attempts := 0
	client := newHardenedRetryableClientWith(fakeResolver{addresses: []netip.Addr{netip.MustParseAddr("140.82.112.6")}}, func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("not used") }, roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(bytes.NewReader(make([]byte, maxPreviewResponseBody+1)))}, nil
	}))
	request, _ := http.NewRequest(http.MethodPost, "https://api.github.com/test", nil)
	response, err := client.HTTPClient.Do(request)
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
