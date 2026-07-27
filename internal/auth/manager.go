// Package auth owns the single-admin browser session boundary for the
// loopback management API. The browser credential is derived from the
// controller's root session key and is never the root key itself.
package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	rootKeySize         = 32
	sessionNonceSize    = 32
	bootstrapNonceSize  = 16
	sessionMACSize      = sha256.Size
	sessionVersion      = byte(1)
	bootstrapVersion    = "twb1"
	sessionWireSize     = 1 + sessionNonceSize + sessionMACSize
	csrfWireSize        = sha256.Size
	SessionCookieName   = "tewake_admin_session"
	CSRFHeaderName      = "X-Tewake-CSRF"
	BootstrapHeaderName = "X-Tewake-Admin-Bootstrap"

	sessionMACDomain   = "tewake/admin-session/v1\x00"
	csrfMACDomain      = "tewake/admin-csrf/v1\x00"
	bootstrapMACDomain = "tewake/admin-bootstrap/v1\x00"

	// DefaultSessionTTL bounds a stolen browser credential. Sessions are also
	// process-local and are therefore invalidated by Controller restart.
	DefaultSessionTTL = 12 * time.Hour

	// BootstrapProofTTL and bootstrapFutureSkew bound the owner-only proof used
	// to mint a session. Every valid proof contains a fresh nonce and is
	// consumed exactly once, so observing one request cannot create a reusable
	// administrator credential.
	BootstrapProofTTL   = 2 * time.Minute
	bootstrapFutureSkew = 30 * time.Second
)

var (
	ErrInvalidRootKey         = errors.New("admin session root key is invalid")
	ErrInvalidCanonicalOrigin = errors.New("admin canonical origin is invalid")
	ErrCookieSecurityMismatch = errors.New("admin cookie security does not match canonical origin")
	ErrMisdirectedHost        = errors.New("request host does not match the admin authority")
	ErrUnauthenticated        = errors.New("admin session is missing or invalid")
	ErrForbiddenOrigin        = errors.New("request origin does not match the admin origin")
	ErrInvalidCSRF            = errors.New("CSRF token is missing or invalid")
	ErrMethodNotAllowed       = errors.New("request method is not allowed")
	ErrSessionClock           = errors.New("admin session clock is unavailable")
)

// Manager issues and validates process-local browser sessions for exactly one
// canonical loopback origin. It intentionally reads Request.Host and Origin
// directly; proxy forwarding headers are outside this trust boundary.
type Manager struct {
	root                [rootKeySize]byte
	canonicalOrigin     string
	canonicalHost       string
	secureCookie        bool
	random              io.Reader
	now                 func() time.Time
	sessionMu           sync.Mutex
	activeSessions      map[[sessionNonceSize]byte]time.Time
	usedBootstrapNonces map[[bootstrapNonceSize]byte]time.Time
}

// Session is proof that Manager authenticated a browser cookie. Its nonce is
// deliberately opaque so callers cannot serialize or log the credential.
type Session struct {
	nonce         [sessionNonceSize]byte
	authenticated bool
}

// NewManager constructs a single-origin session manager. The origin must be a
// canonical HTTP(S) URL whose host is localhost or a loopback IP address. A
// Secure cookie is mandatory for HTTPS and intentionally disabled for HTTP.
func NewManager(root [rootKeySize]byte, canonicalOrigin string, secureCookie bool) (*Manager, error) {
	if subtle.ConstantTimeCompare(root[:], make([]byte, rootKeySize)) == 1 {
		return nil, ErrInvalidRootKey
	}
	origin, host, scheme, err := validateCanonicalOrigin(canonicalOrigin)
	if err != nil {
		return nil, err
	}
	if secureCookie != (scheme == "https") {
		return nil, ErrCookieSecurityMismatch
	}
	return &Manager{
		root:                root,
		canonicalOrigin:     origin,
		canonicalHost:       host,
		secureCookie:        secureCookie,
		random:              rand.Reader,
		now:                 time.Now,
		activeSessions:      make(map[[sessionNonceSize]byte]time.Time),
		usedBootstrapNonces: make(map[[bootstrapNonceSize]byte]time.Time),
	}, nil
}

// CanonicalOrigin is the only Origin value accepted by this manager.
func (manager *Manager) CanonicalOrigin() string {
	if manager == nil {
		return ""
	}
	return manager.canonicalOrigin
}

// CanonicalHost is the only HTTP Host authority accepted by this manager.
func (manager *Manager) CanonicalHost() string {
	if manager == nil {
		return ""
	}
	return manager.canonicalHost
}

// ValidateBootstrap protects POST /api/v1/session before a browser credential
// exists. Exact Host validation happens first so a DNS-rebound request cannot
// use a valid-looking Origin to bootstrap access.
func (manager *Manager) ValidateBootstrap(request *http.Request) error {
	if err := manager.ValidateHost(request); err != nil {
		return err
	}
	if request.Method != http.MethodPost {
		return ErrMethodNotAllowed
	}
	if err := manager.ValidateOrigin(request); err != nil {
		return err
	}
	if !manager.validBootstrapHeader(request) {
		return ErrUnauthenticated
	}
	return nil
}

// ValidateHost rejects any authority other than the configured listener
// authority. Forwarded and X-Forwarded-* headers are intentionally ignored.
func (manager *Manager) ValidateHost(request *http.Request) error {
	if manager == nil || request == nil || request.Host != manager.canonicalHost {
		return ErrMisdirectedHost
	}
	return nil
}

// ValidateOrigin requires one and only one exact Origin header.
func (manager *Manager) ValidateOrigin(request *http.Request) error {
	if manager == nil || request == nil {
		return ErrForbiddenOrigin
	}
	origins := request.Header.Values("Origin")
	if len(origins) != 1 || origins[0] != manager.canonicalOrigin {
		return ErrForbiddenOrigin
	}
	return nil
}

// IssueSession creates a browser-session cookie backed by a fresh random nonce.
// The cookie has no Domain, Expires, or Max-Age, so it is host-only and lasts
// for the browser session.
func (manager *Manager) IssueSession() (Session, *http.Cookie, error) {
	if manager == nil || manager.random == nil || manager.now == nil {
		return Session{}, nil, ErrUnauthenticated
	}
	now := manager.now()
	if now.IsZero() {
		return Session{}, nil, ErrSessionClock
	}
	session := Session{authenticated: true}
	if _, err := io.ReadFull(manager.random, session.nonce[:]); err != nil {
		return Session{}, nil, fmt.Errorf("generate admin session nonce: %w", err)
	}

	payload := make([]byte, 1+sessionNonceSize)
	payload[0] = sessionVersion
	copy(payload[1:], session.nonce[:])
	tag := manager.sessionMAC(payload)
	wire := make([]byte, 0, sessionWireSize)
	wire = append(wire, payload...)
	wire = append(wire, tag[:]...)

	manager.sessionMu.Lock()
	for nonce, expiresAt := range manager.activeSessions {
		if !now.Before(expiresAt) {
			delete(manager.activeSessions, nonce)
		}
	}
	manager.activeSessions[session.nonce] = now.Add(DefaultSessionTTL)
	manager.sessionMu.Unlock()
	return session, manager.sessionCookie(base64.RawURLEncoding.EncodeToString(wire)), nil
}

// Authenticate enforces Host before inspecting the session cookie. Duplicate
// cookies are rejected because choosing one would make intermediary parsing
// differences part of the authentication decision.
func (manager *Manager) Authenticate(request *http.Request) (Session, error) {
	if err := manager.ValidateHost(request); err != nil {
		return Session{}, err
	}
	var value string
	count := 0
	for _, cookie := range request.Cookies() {
		if cookie.Name != SessionCookieName {
			continue
		}
		count++
		value = cookie.Value
	}
	if count != 1 {
		return Session{}, ErrUnauthenticated
	}

	wire, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil ||
		len(wire) != sessionWireSize ||
		base64.RawURLEncoding.EncodeToString(wire) != value ||
		wire[0] != sessionVersion {
		return Session{}, ErrUnauthenticated
	}
	payload := wire[:1+sessionNonceSize]
	suppliedMAC := wire[1+sessionNonceSize:]
	expectedMAC := manager.sessionMAC(payload)
	if subtle.ConstantTimeCompare(suppliedMAC, expectedMAC[:]) != 1 {
		return Session{}, ErrUnauthenticated
	}

	session := Session{authenticated: true}
	copy(session.nonce[:], payload[1:])
	if !manager.sessionActive(session) {
		return Session{}, ErrUnauthenticated
	}
	return session, nil
}

// AuthorizeMutation applies the management mutation checks in their security
// order: exact Host, authenticated session, exact Origin, then CSRF token.
func (manager *Manager) AuthorizeMutation(request *http.Request) (Session, error) {
	session, err := manager.Authenticate(request)
	if err != nil {
		return Session{}, err
	}
	if err := manager.ValidateOrigin(request); err != nil {
		return Session{}, err
	}
	if !manager.validCSRFHeader(request, session) {
		return Session{}, ErrInvalidCSRF
	}
	return session, nil
}

// AuthorizeSameOriginRead applies Host, authentication, and Origin checks for
// streaming reads such as SSE, which keep a credential-bearing connection open.
func (manager *Manager) AuthorizeSameOriginRead(request *http.Request) (Session, error) {
	session, err := manager.Authenticate(request)
	if err != nil {
		return Session{}, err
	}
	if err := manager.ValidateOrigin(request); err != nil {
		return Session{}, err
	}
	return session, nil
}

// CSRFToken derives a token for the authenticated session and this manager's
// exact origin. The root and session nonce are never returned.
func (manager *Manager) CSRFToken(session Session) (string, error) {
	if manager == nil || !session.authenticated {
		return "", ErrUnauthenticated
	}
	tag := manager.csrfMAC(session)
	return base64.RawURLEncoding.EncodeToString(tag[:]), nil
}

// ExpireSessionCookie returns a host-only deletion cookie with the same
// security attributes as the issued browser-session cookie.
func (manager *Manager) ExpireSessionCookie() *http.Cookie {
	cookie := manager.sessionCookie("")
	cookie.MaxAge = -1
	cookie.Expires = time.Unix(1, 0).UTC()
	return cookie
}

// RevokeSession invalidates the exact authenticated nonce before the browser
// deletion cookie is returned. A copied cookie therefore cannot survive logout.
func (manager *Manager) RevokeSession(session Session) error {
	if manager == nil || !session.authenticated {
		return ErrUnauthenticated
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	delete(manager.activeSessions, session.nonce)
	return nil
}

// NewBootstrapProof derives a short-lived, one-use credential accepted by
// POST /api/v1/session. Callers must obtain root from the Controller's
// owner-only credential boundary; the proof must never be placed in command
// arguments, logs, or durable config.
func NewBootstrapProof(
	root [rootKeySize]byte,
	canonicalOrigin string,
	issuedAt time.Time,
	random io.Reader,
) (string, error) {
	if subtle.ConstantTimeCompare(root[:], make([]byte, rootKeySize)) == 1 {
		return "", ErrInvalidRootKey
	}
	origin, _, _, err := validateCanonicalOrigin(canonicalOrigin)
	if err != nil {
		return "", err
	}
	if issuedAt.IsZero() || issuedAt.Unix() <= 0 {
		return "", ErrSessionClock
	}
	if random == nil {
		return "", ErrUnauthenticated
	}
	var nonce [bootstrapNonceSize]byte
	if _, err := io.ReadFull(random, nonce[:]); err != nil {
		return "", fmt.Errorf("generate admin bootstrap nonce: %w", err)
	}
	timestamp := strconv.FormatInt(issuedAt.UTC().Unix(), 10)
	tag := bootstrapMAC(root, origin, timestamp, nonce)
	return strings.Join([]string{
		bootstrapVersion,
		timestamp,
		base64.RawURLEncoding.EncodeToString(nonce[:]),
		base64.RawURLEncoding.EncodeToString(tag[:]),
	}, "."), nil
}

// ValidBootstrapProofEncoding validates only the canonical public wire shape.
// It does not authenticate the proof or reveal any credential material.
func ValidBootstrapProofEncoding(value string) bool {
	_, _, _, ok := parseBootstrapProof(value)
	return ok
}

// HTTPStatus maps authentication boundary errors to their stable API status.
// Unknown errors remain server failures rather than becoming permissive client
// errors.
func HTTPStatus(err error) int {
	switch {
	case errors.Is(err, ErrMisdirectedHost):
		return http.StatusMisdirectedRequest
	case errors.Is(err, ErrUnauthenticated):
		return http.StatusUnauthorized
	case errors.Is(err, ErrForbiddenOrigin), errors.Is(err, ErrInvalidCSRF):
		return http.StatusForbidden
	case errors.Is(err, ErrMethodNotAllowed):
		return http.StatusMethodNotAllowed
	default:
		return http.StatusInternalServerError
	}
}

func (manager *Manager) String() string {
	if manager == nil {
		return "admin-auth{credentials:redacted}"
	}
	return fmt.Sprintf(
		"admin-auth{origin:%q,secure_cookie:%t,credentials:redacted}",
		manager.canonicalOrigin,
		manager.secureCookie,
	)
}

func (manager *Manager) GoString() string     { return manager.String() }
func (manager *Manager) LogValue() slog.Value { return slog.StringValue(manager.String()) }
func (manager *Manager) MarshalJSON() ([]byte, error) {
	origin := ""
	if manager != nil {
		origin = manager.canonicalOrigin
	}
	return json.Marshal(struct {
		Origin      string `json:"origin"`
		Credentials string `json:"credentials"`
	}{
		Origin:      origin,
		Credentials: "redacted",
	})
}

func (session Session) String() string       { return "admin-session{credentials:redacted}" }
func (session Session) GoString() string     { return session.String() }
func (session Session) LogValue() slog.Value { return slog.StringValue(session.String()) }
func (session Session) MarshalJSON() ([]byte, error) {
	return []byte(`{"credentials":"redacted"}`), nil
}

func (manager *Manager) sessionMAC(payload []byte) [sessionMACSize]byte {
	mac := hmac.New(sha256.New, manager.root[:])
	_, _ = mac.Write([]byte(sessionMACDomain))
	_, _ = mac.Write(payload)
	return [sessionMACSize]byte(mac.Sum(nil))
}

func (manager *Manager) csrfMAC(session Session) [csrfWireSize]byte {
	mac := hmac.New(sha256.New, manager.root[:])
	_, _ = mac.Write([]byte(csrfMACDomain))
	_, _ = mac.Write([]byte{sessionVersion})
	_, _ = mac.Write(session.nonce[:])
	_, _ = mac.Write([]byte(manager.canonicalOrigin))
	return [csrfWireSize]byte(mac.Sum(nil))
}

func (manager *Manager) validCSRFHeader(request *http.Request, session Session) bool {
	if request == nil || !session.authenticated {
		return false
	}
	values := request.Header.Values(CSRFHeaderName)
	if len(values) != 1 {
		return false
	}
	supplied, err := base64.RawURLEncoding.DecodeString(values[0])
	if err != nil ||
		len(supplied) != csrfWireSize ||
		base64.RawURLEncoding.EncodeToString(supplied) != values[0] {
		return false
	}
	expected := manager.csrfMAC(session)
	return subtle.ConstantTimeCompare(supplied, expected[:]) == 1
}

func (manager *Manager) validBootstrapHeader(request *http.Request) bool {
	if manager == nil || request == nil || manager.now == nil {
		return false
	}
	values := request.Header.Values(BootstrapHeaderName)
	if len(values) != 1 {
		return false
	}
	issuedAt, nonce, supplied, ok := parseBootstrapProof(values[0])
	if !ok {
		return false
	}
	now := manager.now()
	if now.IsZero() ||
		issuedAt < now.Add(-BootstrapProofTTL).Unix() ||
		issuedAt > now.Add(bootstrapFutureSkew).Unix() {
		return false
	}
	timestamp := strconv.FormatInt(issuedAt, 10)
	expected := bootstrapMAC(manager.root, manager.canonicalOrigin, timestamp, nonce)
	if subtle.ConstantTimeCompare(supplied[:], expected[:]) != 1 {
		return false
	}

	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	for usedNonce, expiresAt := range manager.usedBootstrapNonces {
		if !now.Before(expiresAt) {
			delete(manager.usedBootstrapNonces, usedNonce)
		}
	}
	if _, replayed := manager.usedBootstrapNonces[nonce]; replayed {
		return false
	}
	manager.usedBootstrapNonces[nonce] = time.Unix(issuedAt, 0).Add(BootstrapProofTTL)
	return true
}

func (manager *Manager) sessionActive(session Session) bool {
	if manager == nil || manager.now == nil || !session.authenticated {
		return false
	}
	now := manager.now()
	if now.IsZero() {
		return false
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	expiresAt, exists := manager.activeSessions[session.nonce]
	if !exists || !now.Before(expiresAt) {
		delete(manager.activeSessions, session.nonce)
		return false
	}
	return true
}

func bootstrapMAC(
	root [rootKeySize]byte,
	canonicalOrigin string,
	timestamp string,
	nonce [bootstrapNonceSize]byte,
) [sha256.Size]byte {
	mac := hmac.New(sha256.New, root[:])
	_, _ = mac.Write([]byte(bootstrapMACDomain))
	_, _ = mac.Write([]byte(canonicalOrigin))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(nonce[:])
	return [sha256.Size]byte(mac.Sum(nil))
}

func parseBootstrapProof(
	value string,
) (int64, [bootstrapNonceSize]byte, [sha256.Size]byte, bool) {
	var nonce [bootstrapNonceSize]byte
	var tag [sha256.Size]byte
	parts := strings.Split(value, ".")
	if len(parts) != 4 || parts[0] != bootstrapVersion {
		return 0, nonce, tag, false
	}
	issuedAt, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || issuedAt <= 0 || strconv.FormatInt(issuedAt, 10) != parts[1] {
		return 0, nonce, tag, false
	}
	nonceBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil ||
		len(nonceBytes) != bootstrapNonceSize ||
		base64.RawURLEncoding.EncodeToString(nonceBytes) != parts[2] {
		return 0, nonce, tag, false
	}
	tagBytes, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil ||
		len(tagBytes) != sha256.Size ||
		base64.RawURLEncoding.EncodeToString(tagBytes) != parts[3] {
		return 0, nonce, tag, false
	}
	copy(nonce[:], nonceBytes)
	copy(tag[:], tagBytes)
	return issuedAt, nonce, tag, true
}

func (manager *Manager) sessionCookie(value string) *http.Cookie {
	return &http.Cookie{
		Name:     SessionCookieName,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   manager != nil && manager.secureCookie,
		SameSite: http.SameSiteStrictMode,
	}
}

func validateCanonicalOrigin(raw string) (origin, host, scheme string, err error) {
	parsed, parseErr := url.Parse(raw)
	if parseErr != nil ||
		parsed == nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.User != nil ||
		parsed.Opaque != "" ||
		parsed.Host == "" ||
		parsed.Path != "" ||
		parsed.RawPath != "" ||
		parsed.RawQuery != "" ||
		parsed.ForceQuery ||
		parsed.Fragment != "" {
		return "", "", "", ErrInvalidCanonicalOrigin
	}

	hostname := parsed.Hostname()
	canonicalHostname := ""
	if ip := net.ParseIP(hostname); ip != nil {
		if !ip.IsLoopback() {
			return "", "", "", ErrInvalidCanonicalOrigin
		}
		canonicalHostname = ip.String()
	} else if hostname == "localhost" {
		canonicalHostname = hostname
	} else {
		return "", "", "", ErrInvalidCanonicalOrigin
	}

	port := parsed.Port()
	if port != "" {
		number, portErr := strconv.ParseUint(port, 10, 16)
		if portErr != nil || number == 0 || strconv.FormatUint(number, 10) != port {
			return "", "", "", ErrInvalidCanonicalOrigin
		}
		host = net.JoinHostPort(canonicalHostname, port)
	} else if strings.Contains(canonicalHostname, ":") {
		host = "[" + canonicalHostname + "]"
	} else {
		host = canonicalHostname
	}
	scheme = parsed.Scheme
	origin = scheme + "://" + host
	if parsed.Host != host || raw != origin {
		return "", "", "", ErrInvalidCanonicalOrigin
	}
	return origin, host, scheme, nil
}
