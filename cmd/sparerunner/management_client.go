package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/genm/sparerunner/internal/api/gen"
	"github.com/genm/sparerunner/internal/app"
	"github.com/genm/sparerunner/internal/auth"
	"github.com/genm/sparerunner/internal/config"
	"github.com/genm/sparerunner/internal/enroll"
)

const (
	defaultAdminURL       = "http://127.0.0.1:7442/api/v1"
	managementHTTPTimeout = 30 * time.Second
)

// The message names the three constraints rather than echoing the caller's
// value: the accepted form is canonical, so a caller who passes the browser's
// own origin without the API path, or a hostname instead of a loopback
// literal, otherwise gets a rejection with nothing to correct. It deliberately
// contains no URL literal, because reflecting caller-supplied input back into
// an error is exactly what the surrounding rejection tests forbid.
var errInvalidAdminURL = errors.New(
	"admin URL is invalid: expected the http scheme, a loopback IP literal " +
		"host, and the /api/v1 path with no trailing slash or query",
)

var loadManagementBootstrapProof = app.LoadManagementBootstrapProof

type managementAPIClient struct {
	client         *gen.Client
	jar            http.CookieJar
	baseURL        *url.URL
	bootstrapProof string
	csrf           string
}

func newManagementAPIClient(
	rawURL string,
	bootstrapProof string,
) (*managementAPIClient, error) {
	baseURL, origin, err := validateAdminURL(rawURL)
	if err != nil {
		return nil, err
	}
	if !canonicalManagementBootstrapProof(bootstrapProof) {
		return nil, errors.New("admin bootstrap proof is invalid")
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, errors.New("initialize management API cookie jar")
	}
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("initialize management API transport")
	}
	transport := defaultTransport.Clone()
	// The management endpoint is an exact loopback IP authority. Environment
	// proxies must never receive its administrator cookie or CSRF credential.
	transport.Proxy = nil
	httpClient := &http.Client{
		Jar:       jar,
		Timeout:   managementHTTPTimeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			// The configured loopback authority is exact. Following a redirect
			// would silently move the administrator credential to another URL.
			return http.ErrUseLastResponse
		},
	}
	client, err := gen.NewClient(
		baseURL.String(),
		gen.WithHTTPClient(httpClient),
		gen.WithRequestEditorFn(func(_ context.Context, request *http.Request) error {
			request.Header.Set("Origin", origin)
			return nil
		}),
	)
	if err != nil {
		return nil, errors.New("initialize management API client")
	}
	return &managementAPIClient{
		client:         client,
		jar:            jar,
		baseURL:        baseURL,
		bootstrapProof: bootstrapProof,
	}, nil
}

func newOwnerManagementAPIClient(
	rawURL string,
	stateDirectory string,
) (*managementAPIClient, error) {
	_, origin, err := validateAdminURL(rawURL)
	if err != nil {
		return nil, err
	}
	directory, err := resolveStateDirectory(stateDirectory, "controller")
	if err != nil {
		return nil, err
	}
	proof, err := loadManagementBootstrapProof(directory, origin)
	if err != nil {
		return nil, err
	}
	return newManagementAPIClient(rawURL, proof)
}

func validateAdminURL(raw string) (*url.URL, string, error) {
	parsed, err := url.Parse(raw)
	if err != nil ||
		parsed == nil ||
		parsed.Scheme != "http" ||
		parsed.User != nil ||
		parsed.Opaque != "" ||
		parsed.Host == "" ||
		parsed.Path != "/api/v1" ||
		parsed.RawPath != "" ||
		parsed.RawQuery != "" ||
		parsed.ForceQuery ||
		parsed.Fragment != "" {
		return nil, "", errInvalidAdminURL
	}

	hostname := parsed.Hostname()
	ip := net.ParseIP(hostname)
	if ip == nil || !ip.IsLoopback() {
		return nil, "", errInvalidAdminURL
	}
	canonicalHostname := ip.String()

	port := parsed.Port()
	canonicalHost := canonicalHostname
	if port != "" {
		number, err := strconv.ParseUint(port, 10, 16)
		if err != nil || number == 0 || strconv.FormatUint(number, 10) != port {
			return nil, "", errInvalidAdminURL
		}
		canonicalHost = net.JoinHostPort(canonicalHostname, port)
	} else if strings.Contains(canonicalHostname, ":") {
		canonicalHost = "[" + canonicalHostname + "]"
	}
	origin := "http://" + canonicalHost
	canonicalURL := origin + "/api/v1"
	if parsed.Host != canonicalHost || raw != canonicalURL {
		return nil, "", errInvalidAdminURL
	}
	return parsed, origin, nil
}

func (client *managementAPIClient) bootstrap(ctx context.Context) error {
	if client == nil || !canonicalManagementBootstrapProof(client.bootstrapProof) {
		return errors.New("management API bootstrap proof is unavailable")
	}
	proof := client.bootstrapProof
	client.bootstrapProof = ""
	response, err := client.client.CreateSession(ctx, &gen.CreateSessionParams{
		XSpareRunnerAdminBootstrap: gen.AdminBootstrap(proof),
	})
	if err != nil {
		return errors.New("management API session request failed")
	}
	body, readErr := readManagementResponse(response)
	if response == nil {
		return readErr
	}
	if response.StatusCode != http.StatusCreated {
		return managementStatusError("create session", response.StatusCode)
	}
	if readErr != nil {
		return readErr
	}
	if !hasMediaType(response.Header, "application/json") {
		return errors.New("management API session response is invalid")
	}
	var session gen.Session
	if err := decodeStrictJSON(body, &session); err != nil ||
		session.Authenticated != gen.SessionAuthenticated(true) ||
		!safeOpaqueValue(session.CsrfToken) {
		return errors.New("management API session response is invalid")
	}
	responseCookies := response.Cookies()
	storedCookies := client.jar.Cookies(client.baseURL)
	if len(responseCookies) != 1 ||
		responseCookies[0].Name != auth.SessionCookieName ||
		!safeOpaqueValue(responseCookies[0].Value) ||
		len(storedCookies) != 1 ||
		storedCookies[0].Name != auth.SessionCookieName ||
		storedCookies[0].Value != responseCookies[0].Value {
		return errors.New("management API session cookie is missing")
	}
	client.csrf = session.CsrfToken
	return nil
}

func (client *managementAPIClient) withSession(
	ctx context.Context,
	operation func(context.Context) error,
) (operationErr error) {
	if err := client.bootstrap(ctx); err != nil {
		return err
	}
	defer func() {
		logoutContext, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			managementHTTPTimeout,
		)
		defer cancel()
		logoutErr := client.logout(logoutContext)
		if operationErr == nil && logoutErr != nil {
			operationErr = logoutErr
		}
	}()
	return operation(ctx)
}

func (client *managementAPIClient) logout(ctx context.Context) error {
	if client == nil || !safeOpaqueValue(client.csrf) {
		return errors.New("management API session is unavailable")
	}
	csrf := client.csrf
	client.csrf = ""
	response, err := client.client.DeleteSession(ctx, &gen.DeleteSessionParams{
		XSpareRunnerCSRF: csrf,
	})
	if err != nil {
		return errors.New("management API logout request failed")
	}
	body, readErr := readManagementResponse(response)
	if response == nil {
		return readErr
	}
	if response.StatusCode != http.StatusNoContent {
		return managementStatusError("delete session", response.StatusCode)
	}
	if readErr != nil {
		return readErr
	}
	if len(body) != 0 {
		return errors.New("management API logout response is invalid")
	}
	return nil
}

func canonicalManagementBootstrapProof(value string) bool {
	return auth.ValidBootstrapProofEncoding(value)
}

func (client *managementAPIClient) createJoinCode(
	ctx context.Context,
	hints []string,
) (string, error) {
	response, err := client.client.CreateJoinCode(
		ctx,
		&gen.CreateJoinCodeParams{XSpareRunnerCSRF: client.csrf},
		gen.CreateJoinCodeRequest{EndpointHints: append([]string(nil), hints...)},
	)
	if err != nil {
		return "", errors.New("management API join-code request failed")
	}
	body, readErr := readManagementResponse(response)
	if response == nil {
		return "", readErr
	}
	if response.StatusCode != http.StatusCreated {
		return "", managementStatusError("create join code", response.StatusCode)
	}
	if readErr != nil {
		return "", readErr
	}
	if !hasMediaType(response.Header, "application/json") {
		return "", errors.New("management API join-code response is invalid")
	}
	var delivery gen.JoinCodeDelivery
	if err := decodeStrictJSON(body, &delivery); err != nil ||
		!canonicalLowerHex(delivery.TokenId, 32) {
		return "", errors.New("management API join-code response is invalid")
	}
	joinCode, err := enroll.DecodeJoinCode(delivery.Code)
	if err != nil {
		return "", errors.New("management API join-code response is invalid")
	}
	tokenID := joinCode.TokenID()
	if hex.EncodeToString(tokenID[:]) != delivery.TokenId {
		return "", errors.New("management API join-code response is invalid")
	}
	return delivery.Code, nil
}

func (client *managementAPIClient) authorizeBrowserHandoff(
	ctx context.Context,
	code string,
) error {
	if client == nil || !safeOpaqueValue(client.csrf) ||
		!auth.ValidBrowserHandoffCodeEncoding(code) {
		return errors.New("browser handoff authorization is invalid")
	}
	response, err := client.client.AuthorizeBrowserHandoff(
		ctx,
		&gen.AuthorizeBrowserHandoffParams{XSpareRunnerCSRF: client.csrf},
		gen.AuthorizeBrowserHandoffRequest{Code: code},
	)
	if err != nil {
		return errors.New("management API browser handoff authorization request failed")
	}
	body, readErr := readManagementResponse(response)
	if response == nil {
		return readErr
	}
	if response.StatusCode != http.StatusNoContent {
		return managementStatusError("authorize browser handoff", response.StatusCode)
	}
	if readErr != nil {
		return readErr
	}
	if len(body) != 0 {
		return errors.New("management API browser handoff authorization response is invalid")
	}
	return nil
}

func (client *managementAPIClient) exportConfiguration(
	ctx context.Context,
) ([]byte, error) {
	response, err := client.client.ExportConfiguration(ctx)
	if err != nil {
		return nil, errors.New("management API configuration export request failed")
	}
	body, readErr := readManagementResponse(response)
	if response == nil {
		return nil, readErr
	}
	if response.StatusCode != http.StatusOK {
		return nil, managementStatusError("export configuration", response.StatusCode)
	}
	if readErr != nil {
		return nil, readErr
	}
	if !hasMediaType(response.Header, "application/yaml") {
		return nil, errors.New("management API configuration export response is invalid")
	}
	if _, err := config.DecodeYAML(bytes.NewReader(body)); err != nil {
		return nil, errors.New("management API configuration export response is invalid")
	}
	return body, nil
}

func (client *managementAPIClient) applyConfiguration(
	ctx context.Context,
	payload []byte,
	mediaType string,
	revision uint64,
) error {
	response, err := client.client.ApplyConfigurationWithBody(
		ctx,
		&gen.ApplyConfigurationParams{
			IfMatch:          fmt.Sprintf(`"cfg-%d"`, revision),
			XSpareRunnerCSRF: client.csrf,
		},
		mediaType,
		bytes.NewReader(payload),
	)
	if err != nil {
		return errors.New("management API configuration apply request failed")
	}
	body, readErr := readManagementResponse(response)
	if response == nil {
		return readErr
	}
	if response.StatusCode != http.StatusOK {
		return managementStatusError("apply configuration", response.StatusCode)
	}
	if readErr != nil {
		return readErr
	}
	if !hasMediaType(response.Header, "application/json") {
		return errors.New("management API configuration apply response is invalid")
	}
	applied, err := config.DecodeJSON(bytes.NewReader(body))
	if err != nil ||
		revision == math.MaxUint64 ||
		uint64(applied.Revision) != revision+1 {
		return errors.New("management API configuration apply response is invalid")
	}
	return nil
}

func loadConfigurationPayload(
	source string,
	stdin io.Reader,
) ([]byte, string, uint64, error) {
	var (
		reader io.Reader
		close  func() error
	)
	if source == "-" {
		if stdin == nil {
			return nil, "", 0, errors.New("read configuration input")
		}
		reader = stdin
	} else {
		file, err := os.Open(source)
		if err != nil {
			return nil, "", 0, errors.New("open configuration input")
		}
		reader = file
		close = file.Close
	}
	if close != nil {
		defer close()
	}
	payload, err := readBoundedConfigurationInput(reader)
	if err != nil {
		return nil, "", 0, err
	}

	mediaType := configurationMediaType(source, payload)
	var document config.Configuration
	switch mediaType {
	case "application/json":
		document, err = config.DecodeJSON(bytes.NewReader(payload))
	case "application/yaml":
		document, err = config.DecodeYAML(bytes.NewReader(payload))
	default:
		err = config.ErrInvalidConfiguration
	}
	if err != nil {
		return nil, "", 0, fmt.Errorf("decode configuration input: %w", err)
	}
	return payload, mediaType, uint64(document.Revision), nil
}

func configurationMediaType(source string, payload []byte) string {
	switch strings.ToLower(filepath.Ext(source)) {
	case ".json":
		return "application/json"
	case ".yaml", ".yml":
		return "application/yaml"
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		return "application/json"
	}
	return "application/yaml"
}

func readBoundedConfigurationInput(reader io.Reader) ([]byte, error) {
	limited := &io.LimitedReader{
		R: reader,
		N: config.RequestBodyLimitBytes + 1,
	}
	payload, err := io.ReadAll(limited)
	if err != nil {
		return nil, errors.New("read configuration input")
	}
	if int64(len(payload)) > config.RequestBodyLimitBytes {
		return nil, config.ErrPayloadTooLarge
	}
	return payload, nil
}

func readManagementResponse(response *http.Response) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, errors.New("management API response is invalid")
	}
	defer response.Body.Close()
	limited := &io.LimitedReader{
		R: response.Body,
		N: config.RequestBodyLimitBytes + 1,
	}
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, errors.New("read management API response")
	}
	if int64(len(body)) > config.RequestBodyLimitBytes {
		return nil, errors.New("management API response exceeds the transport byte budget")
	}
	return body, nil
}

func hasMediaType(header http.Header, expected string) bool {
	mediaType, _, err := mime.ParseMediaType(header.Get("Content-Type"))
	return err == nil && mediaType == expected
}

func decodeStrictJSON(body []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}
	return nil
}

func safeOpaqueValue(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func canonicalLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') &&
			(character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func managementStatusError(operation string, status int) error {
	return fmt.Errorf("management API %s failed with HTTP %d", operation, status)
}
