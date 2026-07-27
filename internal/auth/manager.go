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
	rootKeySize              = 32
	sessionNonceSize         = 32
	bootstrapNonceSize       = 16
	browserHandoffIDSize     = 16
	browserHandoffSecretSize = 32
	sessionMACSize           = sha256.Size
	sessionVersion           = byte(1)
	bootstrapVersion         = "twb1"
	browserHandoffVersion    = "twh1"
	sessionWireSize          = 1 + sessionNonceSize + sessionMACSize
	csrfWireSize             = sha256.Size
	SessionCookieName        = "tewake_admin_session"
	CSRFHeaderName           = "X-Tewake-CSRF"
	BootstrapHeaderName      = "X-Tewake-Admin-Bootstrap"

	sessionMACDomain        = "tewake/admin-session/v1\x00"
	csrfMACDomain           = "tewake/admin-csrf/v1\x00"
	bootstrapMACDomain      = "tewake/admin-bootstrap/v1\x00"
	browserHandoffMACDomain = "tewake/browser-handoff/v1\x00"

	// DefaultSessionTTL bounds a stolen browser credential. Sessions are also
	// process-local and are therefore invalidated by Controller restart.
	DefaultSessionTTL = 12 * time.Hour

	// BootstrapProofTTL and bootstrapFutureSkew bound the owner-only proof used
	// to mint a session. Every valid proof contains a fresh nonce and is
	// consumed exactly once, so observing one request cannot create a reusable
	// administrator credential.
	BootstrapProofTTL   = 2 * time.Minute
	bootstrapFutureSkew = 30 * time.Second

	// BrowserHandoffTTL reuses the existing two-minute owner-authority transfer
	// bound. Approval never extends it.
	BrowserHandoffTTL = BootstrapProofTTL
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
	ErrInvalidSessionTTL      = errors.New("admin session TTL is invalid")
	ErrInvalidBrowserHandoff  = errors.New("browser handoff is invalid")
	ErrExpiredBrowserHandoff  = errors.New("browser handoff has expired")
	ErrBrowserHandoffPending  = errors.New("browser handoff is pending owner approval")
	ErrBrowserHandoffClaiming = errors.New("browser handoff claim is already in progress")
	ErrBrowserHandoffClaimed  = errors.New("browser handoff was already claimed")
	ErrBrowserHandoffFence    = errors.New("browser handoff operation fence is invalid")
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
	sessionTTL          time.Duration
	sessionMu           sync.Mutex
	activeSessions      map[[sessionNonceSize]byte]*sessionEntry
	usedBootstrapNonces map[[bootstrapNonceSize]byte]time.Time
	browserHandoffKey   [rootKeySize]byte
	browserHandoffMu    sync.Mutex
	browserHandoffs     map[[browserHandoffIDSize]byte]browserHandoffEntry
	browserHandoffFence uint64
}

// Session is proof that Manager authenticated a browser cookie. Its nonce is
// deliberately opaque so callers cannot serialize or log the credential.
type Session struct {
	nonce         [sessionNonceSize]byte
	authenticated bool
}

type sessionEntry struct {
	expiresAt time.Time
	revoked   chan struct{}
}

type browserHandoffPhase uint8

const (
	browserHandoffApproving browserHandoffPhase = iota + 1
	browserHandoffApproved
	browserHandoffClaiming
	browserHandoffClaimed
)

type browserHandoffEntry struct {
	claimDigest [sha256.Size]byte
	expiresAt   time.Time
	phase       browserHandoffPhase
	fence       uint64
}

// BrowserHandoff is the safe authority result projected by the API into its
// transport DTO. Code is deliberately available only through an explicit
// accessor so generic logging and JSON serialization remain redacted.
type BrowserHandoff struct {
	code      string
	expiresAt time.Time
}

func (handoff BrowserHandoff) Code() string         { return handoff.code }
func (handoff BrowserHandoff) ExpiresAt() time.Time { return handoff.expiresAt }
func (handoff BrowserHandoff) String() string {
	return fmt.Sprintf(
		"browser-handoff{expires_at:%q,credentials:redacted}",
		handoff.expiresAt.UTC().Format(time.RFC3339),
	)
}
func (handoff BrowserHandoff) GoString() string     { return handoff.String() }
func (handoff BrowserHandoff) LogValue() slog.Value { return slog.StringValue(handoff.String()) }
func (handoff BrowserHandoff) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ExpiresAt   time.Time `json:"expiresAt"`
		Credentials string    `json:"credentials"`
	}{
		ExpiresAt:   handoff.expiresAt,
		Credentials: "redacted",
	})
}

// BrowserHandoffApproval is an opaque two-phase fence. NeedsAudit is true only
// for the request that moved the handoff into Approving. A retry after commit
// returns a no-op fence so the API can avoid duplicate durable audit evidence.
type BrowserHandoffApproval struct {
	id         [browserHandoffIDSize]byte
	fence      uint64
	needsAudit bool
}

func (approval BrowserHandoffApproval) NeedsAudit() bool { return approval.needsAudit }
func (approval BrowserHandoffApproval) String() string {
	return fmt.Sprintf(
		"browser-handoff-approval{needs_audit:%t,credentials:redacted}",
		approval.needsAudit,
	)
}
func (approval BrowserHandoffApproval) GoString() string { return approval.String() }
func (approval BrowserHandoffApproval) LogValue() slog.Value {
	return slog.StringValue(approval.String())
}
func (approval BrowserHandoffApproval) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		NeedsAudit  bool   `json:"needsAudit"`
		Credentials string `json:"credentials"`
	}{
		NeedsAudit:  approval.needsAudit,
		Credentials: "redacted",
	})
}

// BrowserHandoffClaim is an opaque claim fence held while the API issues a
// session and commits authentication audit evidence.
type BrowserHandoffClaim struct {
	id    [browserHandoffIDSize]byte
	fence uint64
}

func (claim BrowserHandoffClaim) String() string {
	return "browser-handoff-claim{credentials:redacted}"
}
func (claim BrowserHandoffClaim) GoString() string     { return claim.String() }
func (claim BrowserHandoffClaim) LogValue() slog.Value { return slog.StringValue(claim.String()) }
func (claim BrowserHandoffClaim) MarshalJSON() ([]byte, error) {
	return []byte(`{"credentials":"redacted"}`), nil
}

type parsedBrowserHandoff struct {
	issuedAt    time.Time
	expiresAt   time.Time
	id          [browserHandoffIDSize]byte
	claimDigest [sha256.Size]byte
}

// BrowserHandoffPendingError carries only the verified public expiry needed by
// a 202 transport response. It contains neither the code nor claim material.
type BrowserHandoffPendingError struct {
	expiresAt time.Time
}

func (pending *BrowserHandoffPendingError) Error() string {
	return ErrBrowserHandoffPending.Error()
}
func (pending *BrowserHandoffPendingError) Is(target error) bool {
	return target == ErrBrowserHandoffPending
}
func (pending *BrowserHandoffPendingError) ExpiresAt() time.Time {
	if pending == nil {
		return time.Time{}
	}
	return pending.expiresAt
}
func (pending *BrowserHandoffPendingError) String() string {
	return fmt.Sprintf(
		"browser-handoff-pending{expires_at:%q,credentials:redacted}",
		pending.ExpiresAt().UTC().Format(time.RFC3339),
	)
}
func (pending *BrowserHandoffPendingError) GoString() string { return pending.String() }
func (pending *BrowserHandoffPendingError) LogValue() slog.Value {
	return slog.StringValue(pending.String())
}
func (pending *BrowserHandoffPendingError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ExpiresAt   time.Time `json:"expiresAt"`
		Credentials string    `json:"credentials"`
	}{
		ExpiresAt:   pending.ExpiresAt(),
		Credentials: "redacted",
	})
}

// NewManager constructs a single-origin session manager. The origin must be a
// canonical HTTP(S) URL whose host is localhost or a loopback IP address. A
// Secure cookie is mandatory for HTTPS and intentionally disabled for HTTP.
func NewManager(root [rootKeySize]byte, canonicalOrigin string, secureCookie bool) (*Manager, error) {
	return newManager(root, canonicalOrigin, secureCookie, DefaultSessionTTL)
}

// NewManagerWithSessionTTL constructs a manager with a shorter explicit session
// bound. The production default remains the maximum allowed lifetime; callers
// cannot use this entrypoint to weaken that credential bound.
func NewManagerWithSessionTTL(
	root [rootKeySize]byte,
	canonicalOrigin string,
	secureCookie bool,
	sessionTTL time.Duration,
) (*Manager, error) {
	if sessionTTL <= 0 || sessionTTL > DefaultSessionTTL {
		return nil, ErrInvalidSessionTTL
	}
	return newManager(root, canonicalOrigin, secureCookie, sessionTTL)
}

func newManager(
	root [rootKeySize]byte,
	canonicalOrigin string,
	secureCookie bool,
	sessionTTL time.Duration,
) (*Manager, error) {
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
	manager := &Manager{
		root:                root,
		canonicalOrigin:     origin,
		canonicalHost:       host,
		secureCookie:        secureCookie,
		random:              rand.Reader,
		now:                 time.Now,
		sessionTTL:          sessionTTL,
		activeSessions:      make(map[[sessionNonceSize]byte]*sessionEntry),
		usedBootstrapNonces: make(map[[bootstrapNonceSize]byte]time.Time),
		browserHandoffs:     make(map[[browserHandoffIDSize]byte]browserHandoffEntry),
	}
	// A process-only signing key makes every outstanding browser handoff
	// unverifiable after Controller restart without persisting another secret.
	if _, err := io.ReadFull(manager.random, manager.browserHandoffKey[:]); err != nil {
		return nil, fmt.Errorf("generate browser handoff signing key: %w", err)
	}
	return manager, nil
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
	for nonce, entry := range manager.activeSessions {
		if !now.Before(entry.expiresAt) {
			manager.revokeSessionLocked(nonce)
		}
	}
	manager.activeSessions[session.nonce] = &sessionEntry{
		expiresAt: now.Add(manager.sessionTTL),
		revoked:   make(chan struct{}),
	}
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

// AuthorizeSameOriginRead applies Host, authentication, and per-session CSRF
// checks for streaming reads. Browsers omit Origin on a same-origin EventSource
// GET, while a custom CSRF header remains browser-portable and cannot cross this
// API's no-CORS boundary.
func (manager *Manager) AuthorizeSameOriginRead(request *http.Request) (Session, error) {
	session, err := manager.Authenticate(request)
	if err != nil {
		return Session{}, err
	}
	if !manager.validCSRFHeader(request, session) {
		return Session{}, ErrInvalidCSRF
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
	manager.revokeSessionLocked(session.nonce)
	return nil
}

// WatchSession returns the absolute authenticated deadline and a signal closed
// on exact server-side revocation. Streaming owners must stop at either boundary
// so scheduler delay cannot extend a previously authenticated response.
func (manager *Manager) WatchSession(
	session Session,
) (time.Time, <-chan struct{}, error) {
	if manager == nil || manager.now == nil || !session.authenticated {
		return time.Time{}, nil, ErrUnauthenticated
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	now := manager.now()
	if now.IsZero() {
		return time.Time{}, nil, ErrSessionClock
	}
	entry, exists := manager.activeSessions[session.nonce]
	if !exists || !now.Before(entry.expiresAt) {
		manager.revokeSessionLocked(session.nonce)
		return time.Time{}, nil, ErrUnauthenticated
	}
	return entry.expiresAt, entry.revoked, nil
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

// IssueBrowserHandoff signs a browser-generated claim digest without retaining
// unauthenticated request state. The browser keeps the preimage only in memory;
// the public code alone can never claim an administrator session.
func (manager *Manager) IssueBrowserHandoff(
	claimDigest [sha256.Size]byte,
) (BrowserHandoff, error) {
	if manager == nil || manager.random == nil || manager.now == nil {
		return BrowserHandoff{}, ErrInvalidBrowserHandoff
	}
	now := manager.now()
	if now.IsZero() {
		return BrowserHandoff{}, ErrSessionClock
	}
	issuedAt := now.UTC().Truncate(time.Second)
	var id [browserHandoffIDSize]byte
	if _, err := io.ReadFull(manager.random, id[:]); err != nil {
		return BrowserHandoff{}, fmt.Errorf("generate browser handoff id: %w", err)
	}
	timestamp := strconv.FormatInt(issuedAt.Unix(), 10)
	tag := manager.browserHandoffMAC(timestamp, id, claimDigest)
	code := strings.Join([]string{
		browserHandoffVersion,
		timestamp,
		base64.RawURLEncoding.EncodeToString(id[:]),
		base64.RawURLEncoding.EncodeToString(claimDigest[:]),
		base64.RawURLEncoding.EncodeToString(tag[:]),
	}, ".")

	manager.browserHandoffMu.Lock()
	manager.expireBrowserHandoffsLocked(now)
	manager.browserHandoffMu.Unlock()
	return BrowserHandoff{
		code:      code,
		expiresAt: issuedAt.Add(BrowserHandoffTTL),
	}, nil
}

// ApproveBrowserHandoff reserves the code for durable authorization audit. The
// caller must commit only after audit persistence succeeds, or roll back on
// every failure path. Already committed approval is an idempotent no-op.
func (manager *Manager) ApproveBrowserHandoff(
	code string,
) (BrowserHandoffApproval, error) {
	parsed, err := manager.authenticateBrowserHandoff(code)
	if err != nil {
		return BrowserHandoffApproval{}, err
	}
	now := manager.now()
	manager.browserHandoffMu.Lock()
	defer manager.browserHandoffMu.Unlock()
	manager.expireBrowserHandoffsLocked(now)

	entry, exists := manager.browserHandoffs[parsed.id]
	if exists && (entry.claimDigest != parsed.claimDigest ||
		!entry.expiresAt.Equal(parsed.expiresAt)) {
		return BrowserHandoffApproval{}, ErrInvalidBrowserHandoff
	}
	if exists {
		switch entry.phase {
		case browserHandoffApproving:
			return BrowserHandoffApproval{}, &BrowserHandoffPendingError{
				expiresAt: parsed.expiresAt,
			}
		case browserHandoffApproved, browserHandoffClaiming, browserHandoffClaimed:
			return BrowserHandoffApproval{id: parsed.id}, nil
		default:
			return BrowserHandoffApproval{}, ErrInvalidBrowserHandoff
		}
	}

	entry = browserHandoffEntry{
		claimDigest: parsed.claimDigest,
		expiresAt:   parsed.expiresAt,
		phase:       browserHandoffApproving,
		fence:       manager.nextBrowserHandoffFenceLocked(),
	}
	manager.browserHandoffs[parsed.id] = entry
	return BrowserHandoffApproval{
		id:         parsed.id,
		fence:      entry.fence,
		needsAudit: true,
	}, nil
}

func (manager *Manager) CommitBrowserHandoffApproval(
	approval BrowserHandoffApproval,
) error {
	if !approval.needsAudit {
		return nil
	}
	if manager == nil || manager.now == nil {
		return ErrBrowserHandoffFence
	}
	now := manager.now()
	manager.browserHandoffMu.Lock()
	defer manager.browserHandoffMu.Unlock()
	entry, exists := manager.browserHandoffs[approval.id]
	if !exists ||
		entry.phase != browserHandoffApproving ||
		entry.fence != approval.fence {
		return ErrBrowserHandoffFence
	}
	if !now.Before(entry.expiresAt) {
		delete(manager.browserHandoffs, approval.id)
		return ErrExpiredBrowserHandoff
	}
	entry.phase = browserHandoffApproved
	manager.browserHandoffs[approval.id] = entry
	return nil
}

func (manager *Manager) RollbackBrowserHandoffApproval(
	approval BrowserHandoffApproval,
) error {
	if !approval.needsAudit {
		return nil
	}
	if manager == nil {
		return ErrBrowserHandoffFence
	}
	manager.browserHandoffMu.Lock()
	defer manager.browserHandoffMu.Unlock()
	entry, exists := manager.browserHandoffs[approval.id]
	if !exists ||
		entry.phase != browserHandoffApproving ||
		entry.fence != approval.fence {
		return ErrBrowserHandoffFence
	}
	// Issued handoffs are stateless, so rollback removes the temporary
	// approval fence instead of retaining attacker-driven pending entries.
	delete(manager.browserHandoffs, approval.id)
	return nil
}

// ClaimBrowserHandoff validates the browser-held preimage and atomically fences
// one caller. The caller issues a session and persists authentication audit
// before committing; rollback restores the previously approved state.
func (manager *Manager) ClaimBrowserHandoff(
	code string,
	secret [browserHandoffSecretSize]byte,
) (BrowserHandoffClaim, error) {
	parsed, err := manager.authenticateBrowserHandoff(code)
	if err != nil {
		return BrowserHandoffClaim{}, err
	}
	suppliedDigest := sha256.Sum256(secret[:])
	if subtle.ConstantTimeCompare(suppliedDigest[:], parsed.claimDigest[:]) != 1 {
		return BrowserHandoffClaim{}, ErrInvalidBrowserHandoff
	}

	now := manager.now()
	manager.browserHandoffMu.Lock()
	defer manager.browserHandoffMu.Unlock()
	manager.expireBrowserHandoffsLocked(now)
	entry, exists := manager.browserHandoffs[parsed.id]
	if !exists {
		return BrowserHandoffClaim{}, &BrowserHandoffPendingError{
			expiresAt: parsed.expiresAt,
		}
	}
	if entry.claimDigest != parsed.claimDigest ||
		!entry.expiresAt.Equal(parsed.expiresAt) {
		return BrowserHandoffClaim{}, ErrInvalidBrowserHandoff
	}
	switch entry.phase {
	case browserHandoffApproving:
		return BrowserHandoffClaim{}, &BrowserHandoffPendingError{
			expiresAt: parsed.expiresAt,
		}
	case browserHandoffApproved:
		entry.fence = manager.nextBrowserHandoffFenceLocked()
		entry.phase = browserHandoffClaiming
		manager.browserHandoffs[parsed.id] = entry
		return BrowserHandoffClaim{id: parsed.id, fence: entry.fence}, nil
	case browserHandoffClaiming:
		return BrowserHandoffClaim{}, ErrBrowserHandoffClaiming
	case browserHandoffClaimed:
		return BrowserHandoffClaim{}, ErrBrowserHandoffClaimed
	default:
		return BrowserHandoffClaim{}, ErrInvalidBrowserHandoff
	}
}

func (manager *Manager) CommitBrowserHandoffClaim(claim BrowserHandoffClaim) error {
	if manager == nil || manager.now == nil {
		return ErrBrowserHandoffFence
	}
	now := manager.now()
	manager.browserHandoffMu.Lock()
	defer manager.browserHandoffMu.Unlock()
	entry, exists := manager.browserHandoffs[claim.id]
	if !exists ||
		entry.phase != browserHandoffClaiming ||
		entry.fence != claim.fence {
		return ErrBrowserHandoffFence
	}
	if !now.Before(entry.expiresAt) {
		delete(manager.browserHandoffs, claim.id)
		return ErrExpiredBrowserHandoff
	}
	entry.phase = browserHandoffClaimed
	manager.browserHandoffs[claim.id] = entry
	return nil
}

func (manager *Manager) RollbackBrowserHandoffClaim(claim BrowserHandoffClaim) error {
	if manager == nil {
		return ErrBrowserHandoffFence
	}
	manager.browserHandoffMu.Lock()
	defer manager.browserHandoffMu.Unlock()
	entry, exists := manager.browserHandoffs[claim.id]
	if !exists ||
		entry.phase != browserHandoffClaiming ||
		entry.fence != claim.fence {
		return ErrBrowserHandoffFence
	}
	if manager.now == nil || !manager.now().Before(entry.expiresAt) {
		delete(manager.browserHandoffs, claim.id)
		return ErrExpiredBrowserHandoff
	}
	entry.phase = browserHandoffApproved
	manager.browserHandoffs[claim.id] = entry
	return nil
}

// ValidBrowserHandoffCodeEncoding validates only the canonical public wire
// shape for early CLI feedback. It never authenticates the process-local MAC.
func ValidBrowserHandoffCodeEncoding(code string) bool {
	parts := strings.Split(code, ".")
	if len(parts) != 5 || parts[0] != browserHandoffVersion {
		return false
	}
	issuedUnix, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil ||
		issuedUnix <= 0 ||
		strconv.FormatInt(issuedUnix, 10) != parts[1] {
		return false
	}
	if _, ok := decodeCanonicalBrowserHandoffPart(parts[2], browserHandoffIDSize); !ok {
		return false
	}
	if _, ok := decodeCanonicalBrowserHandoffPart(parts[3], sha256.Size); !ok {
		return false
	}
	if _, ok := decodeCanonicalBrowserHandoffPart(parts[4], sha256.Size); !ok {
		return false
	}
	return true
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
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	now := manager.now()
	if now.IsZero() {
		return false
	}
	entry, exists := manager.activeSessions[session.nonce]
	if !exists || !now.Before(entry.expiresAt) {
		manager.revokeSessionLocked(session.nonce)
		return false
	}
	return true
}

func (manager *Manager) revokeSessionLocked(nonce [sessionNonceSize]byte) {
	entry, exists := manager.activeSessions[nonce]
	if !exists {
		return
	}
	delete(manager.activeSessions, nonce)
	close(entry.revoked)
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

func (manager *Manager) browserHandoffMAC(
	timestamp string,
	id [browserHandoffIDSize]byte,
	claimDigest [sha256.Size]byte,
) [sha256.Size]byte {
	mac := hmac.New(sha256.New, manager.browserHandoffKey[:])
	_, _ = mac.Write([]byte(browserHandoffMACDomain))
	_, _ = mac.Write([]byte(manager.canonicalOrigin))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(id[:])
	_, _ = mac.Write(claimDigest[:])
	return [sha256.Size]byte(mac.Sum(nil))
}

func (manager *Manager) authenticateBrowserHandoff(
	code string,
) (parsedBrowserHandoff, error) {
	var parsed parsedBrowserHandoff
	if manager == nil || manager.now == nil {
		return parsed, ErrInvalidBrowserHandoff
	}
	parts := strings.Split(code, ".")
	if len(parts) != 5 || parts[0] != browserHandoffVersion {
		return parsed, ErrInvalidBrowserHandoff
	}
	issuedUnix, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil ||
		issuedUnix <= 0 ||
		strconv.FormatInt(issuedUnix, 10) != parts[1] {
		return parsed, ErrInvalidBrowserHandoff
	}
	idBytes, ok := decodeCanonicalBrowserHandoffPart(parts[2], browserHandoffIDSize)
	if !ok {
		return parsed, ErrInvalidBrowserHandoff
	}
	var id [browserHandoffIDSize]byte
	copy(id[:], idBytes)
	digestBytes, ok := decodeCanonicalBrowserHandoffPart(parts[3], sha256.Size)
	if !ok {
		return parsed, ErrInvalidBrowserHandoff
	}
	var digest [sha256.Size]byte
	copy(digest[:], digestBytes)
	suppliedMACBytes, ok := decodeCanonicalBrowserHandoffPart(parts[4], sha256.Size)
	if !ok {
		return parsed, ErrInvalidBrowserHandoff
	}
	var suppliedMAC [sha256.Size]byte
	copy(suppliedMAC[:], suppliedMACBytes)
	expectedMAC := manager.browserHandoffMAC(parts[1], id, digest)
	if subtle.ConstantTimeCompare(suppliedMAC[:], expectedMAC[:]) != 1 {
		return parsed, ErrInvalidBrowserHandoff
	}

	now := manager.now()
	if now.IsZero() {
		return parsed, ErrSessionClock
	}
	parsed = parsedBrowserHandoff{
		issuedAt:    time.Unix(issuedUnix, 0).UTC(),
		expiresAt:   time.Unix(issuedUnix, 0).UTC().Add(BrowserHandoffTTL),
		id:          id,
		claimDigest: digest,
	}
	if parsed.issuedAt.After(now.Add(bootstrapFutureSkew)) {
		return parsedBrowserHandoff{}, ErrInvalidBrowserHandoff
	}
	if !now.Before(parsed.expiresAt) {
		return parsedBrowserHandoff{}, ErrExpiredBrowserHandoff
	}
	return parsed, nil
}

func decodeCanonicalBrowserHandoffPart(value string, size int) ([]byte, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil ||
		len(raw) != size ||
		base64.RawURLEncoding.EncodeToString(raw) != value {
		return nil, false
	}
	return raw, true
}

func (manager *Manager) expireBrowserHandoffsLocked(now time.Time) {
	for id, entry := range manager.browserHandoffs {
		if !now.Before(entry.expiresAt) {
			delete(manager.browserHandoffs, id)
		}
	}
}

func (manager *Manager) nextBrowserHandoffFenceLocked() uint64 {
	// Manager-global fences prevent an old rolled-back approval from matching a
	// newly-created entry for the same signed code (the classic ABA case).
	manager.browserHandoffFence++
	if manager.browserHandoffFence == 0 {
		manager.browserHandoffFence++
	}
	return manager.browserHandoffFence
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
