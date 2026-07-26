package runner

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// HTTPFetcher accepts only the pinned github.com release URL and its signed
// release-assets redirect. ProxyFromEnvironment is deliberately not used: an
// agent must not forward ambient proxy credentials while fetching runner code.
type HTTPFetcher struct {
	client *http.Client
}

func NewHTTPFetcher() *HTTPFetcher {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.DisableKeepAlives = false
	return &HTTPFetcher{client: &http.Client{Transport: transport, CheckRedirect: checkReleaseRedirect}}
}

func (f *HTTPFetcher) Fetch(ctx context.Context, pkg Package) (io.ReadCloser, error) {
	if f == nil || f.client == nil || !pkg.valid() {
		return nil, ErrDownloadPolicy
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, pkg.URL(), nil)
	if err != nil {
		return nil, ErrDownloadPolicy
	}
	request.Header.Set("Accept", "application/octet-stream")
	response, err := f.client.Do(request)
	if err != nil {
		return nil, ErrDownloadPolicy
	}
	if response.StatusCode != http.StatusOK || !isReleaseAssetURL(response.Request.URL) {
		response.Body.Close()
		return nil, ErrDownloadPolicy
	}
	return response.Body, nil
}

func checkReleaseRedirect(request *http.Request, via []*http.Request) error {
	// GitHub currently makes exactly one redirect. Bound the control path so a
	// compromised redirect cannot turn this downloader into a generic client.
	if len(via) != 1 || !isPinnedReleaseURL(via[0].URL) || !isReleaseAssetURL(request.URL) {
		return ErrDownloadPolicy
	}
	return nil
}

func isPinnedReleaseURL(value *url.URL) bool {
	return value != nil && value.Scheme == "https" && value.Host == "github.com" &&
		strings.HasPrefix(value.EscapedPath(), "/actions/runner/releases/download/v"+OfficialRunnerVersion+"/actions-runner-")
}

func isReleaseAssetURL(value *url.URL) bool {
	return value != nil && value.Scheme == "https" && value.Host == "release-assets.githubusercontent.com" &&
		strings.HasPrefix(value.EscapedPath(), "/github-production-release-asset/")
}
