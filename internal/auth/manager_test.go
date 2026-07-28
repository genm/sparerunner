package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testOrigin = "http://127.0.0.1:7442"

func TestManagerIssuesAndAuthenticatesSameOriginSession(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	session, cookie, err := manager.IssueSession()
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	if cookie.Name != SessionCookieName ||
		cookie.Path != "/" ||
		!cookie.HttpOnly ||
		cookie.SameSite != http.SameSiteStrictMode ||
		cookie.Secure ||
		cookie.Domain != "" ||
		cookie.Expires.IsZero() == false ||
		cookie.MaxAge != 0 {
		t.Fatalf("unexpected session cookie attributes: %#v", cookie)
	}

	raw, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil {
		t.Fatalf("decode session cookie: %v", err)
	}
	if got, want := len(raw), sessionWireSize; got != want {
		t.Fatalf("session cookie size = %d, want %d", got, want)
	}
	if raw[0] != sessionVersion {
		t.Fatalf("session version = %d, want %d", raw[0], sessionVersion)
	}
	if base64.RawURLEncoding.EncodeToString(raw) != cookie.Value {
		t.Fatal("session cookie is not canonical raw base64url")
	}

	request := sameOriginRequest(http.MethodPut, "/api/v1/config")
	request.AddCookie(cookie)
	csrf, err := manager.CSRFToken(session)
	if err != nil {
		t.Fatalf("create CSRF token: %v", err)
	}
	request.Header.Set(CSRFHeaderName, csrf)

	authenticated, err := manager.AuthorizeMutation(request)
	if err != nil {
		t.Fatalf("authorize mutation: %v", err)
	}
	if authenticated != session {
		t.Fatal("authenticated session differs from issued session")
	}
}

func TestManagerSupportsSecureHostOnlyCookie(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, "https://[::1]:7443", true)
	_, cookie, err := manager.IssueSession()
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	if !cookie.Secure {
		t.Fatal("secure session cookie is not marked Secure")
	}
	if cookie.Domain != "" {
		t.Fatalf("session cookie Domain = %q, want host-only cookie", cookie.Domain)
	}
}

func TestValidateBootstrapRequiresExactHostOriginAndPost(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	now := time.Unix(1_700_000_000, 0).UTC()
	manager.now = func() time.Time { return now }

	tests := []struct {
		name   string
		proof  func(int) string
		mutate func(*http.Request)
		want   error
		status int
	}{
		{
			name: "valid",
		},
		{
			name: "wrong host even with forwarded hints",
			mutate: func(request *http.Request) {
				request.Host = "localhost:7442"
				request.Header.Set("Forwarded", "host=127.0.0.1:7442;proto=http")
				request.Header.Set("X-Forwarded-Host", "127.0.0.1:7442")
				request.Header.Set("X-Forwarded-Proto", "http")
			},
			want:   ErrMisdirectedHost,
			status: http.StatusMisdirectedRequest,
		},
		{
			name: "missing origin",
			mutate: func(request *http.Request) {
				request.Header.Del("Origin")
			},
			want:   ErrForbiddenOrigin,
			status: http.StatusForbidden,
		},
		{
			name: "wrong origin",
			mutate: func(request *http.Request) {
				request.Header.Set("Origin", "http://localhost:7442")
			},
			want:   ErrForbiddenOrigin,
			status: http.StatusForbidden,
		},
		{
			name: "multiple origins",
			mutate: func(request *http.Request) {
				request.Header.Add("Origin", testOrigin)
			},
			want:   ErrForbiddenOrigin,
			status: http.StatusForbidden,
		},
		{
			name: "wrong method",
			mutate: func(request *http.Request) {
				request.Method = http.MethodGet
			},
			want:   ErrMethodNotAllowed,
			status: http.StatusMethodNotAllowed,
		},
		{
			name: "missing owner proof",
			proof: func(int) string {
				return ""
			},
			want:   ErrUnauthenticated,
			status: http.StatusUnauthorized,
		},
		{
			name: "proof signed by another root",
			proof: func(index int) string {
				root := testRoot()
				root[0] ^= 0xff
				return mustBootstrapProof(t, root, testOrigin, now, byte(index+1))
			},
			want:   ErrUnauthenticated,
			status: http.StatusUnauthorized,
		},
		{
			name: "expired proof",
			proof: func(index int) string {
				return mustBootstrapProof(
					t,
					testRoot(),
					testOrigin,
					now.Add(-BootstrapProofTTL-time.Second),
					byte(index+1),
				)
			},
			want:   ErrUnauthenticated,
			status: http.StatusUnauthorized,
		},
		{
			name: "proof too far in future",
			proof: func(index int) string {
				return mustBootstrapProof(
					t,
					testRoot(),
					testOrigin,
					now.Add(bootstrapFutureSkew+time.Second),
					byte(index+1),
				)
			},
			want:   ErrUnauthenticated,
			status: http.StatusUnauthorized,
		},
		{
			name: "non-canonical proof",
			proof: func(index int) string {
				return mustBootstrapProof(t, testRoot(), testOrigin, now, byte(index+1)) + "="
			},
			want:   ErrUnauthenticated,
			status: http.StatusUnauthorized,
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request := sameOriginRequest(http.MethodPost, "/api/v1/session")
			proof := mustBootstrapProof(t, testRoot(), testOrigin, now, byte(index+1))
			if test.proof != nil {
				proof = test.proof(index)
			}
			if proof != "" {
				request.Header.Set(BootstrapHeaderName, proof)
			}
			if test.mutate != nil {
				test.mutate(request)
			}
			err := manager.ValidateBootstrap(request)
			if test.want == nil {
				if err != nil {
					t.Fatalf("validate bootstrap: %v", err)
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if got := HTTPStatus(err); got != test.status {
				t.Fatalf("HTTP status = %d, want %d", got, test.status)
			}
		})
	}
}

func TestBootstrapProofIsShortLivedAndConsumedExactlyOnce(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0).UTC()
	manager := newTestManager(t, testOrigin, false)
	manager.now = func() time.Time { return now }
	proof := mustBootstrapProof(t, testRoot(), testOrigin, now, 0x44)
	if !ValidBootstrapProofEncoding(proof) {
		t.Fatal("fresh proof does not have canonical encoding")
	}

	request := sameOriginRequest(http.MethodPost, "/api/v1/session")
	request.Header.Set(BootstrapHeaderName, proof)
	if err := manager.ValidateBootstrap(request); err != nil {
		t.Fatalf("validate fresh proof: %v", err)
	}
	if err := manager.ValidateBootstrap(request); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("replayed proof error = %v, want %v", err, ErrUnauthenticated)
	}

	now = now.Add(BootstrapProofTTL + time.Second)
	nextProof := mustBootstrapProof(t, testRoot(), testOrigin, now, 0x45)
	nextRequest := sameOriginRequest(http.MethodPost, "/api/v1/session")
	nextRequest.Header.Set(BootstrapHeaderName, nextProof)
	if err := manager.ValidateBootstrap(nextRequest); err != nil {
		t.Fatalf("validate next proof: %v", err)
	}
	if got := len(manager.usedBootstrapNonces); got != 1 {
		t.Fatalf("retained bootstrap nonces = %d, want 1 current nonce", got)
	}
}

func TestManagerRevokesExpiresAndInvalidatesSessionsOnRestart(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0).UTC()
	manager := newTestManager(t, testOrigin, false)
	manager.now = func() time.Time { return now }
	session, cookie, err := manager.IssueSession()
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	request := sameOriginRequest(http.MethodGet, "/api/v1/config")
	request.AddCookie(cookie)
	if _, err := manager.Authenticate(request); err != nil {
		t.Fatalf("authenticate current session: %v", err)
	}

	restarted := newTestManager(t, testOrigin, false)
	restarted.now = manager.now
	if _, err := restarted.Authenticate(request); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("restart authentication error = %v, want %v", err, ErrUnauthenticated)
	}

	if err := manager.RevokeSession(session); err != nil {
		t.Fatalf("revoke session: %v", err)
	}
	if _, err := manager.Authenticate(request); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked authentication error = %v, want %v", err, ErrUnauthenticated)
	}

	_, expiringCookie, err := manager.IssueSession()
	if err != nil {
		t.Fatalf("issue expiring session: %v", err)
	}
	expiringRequest := sameOriginRequest(http.MethodGet, "/api/v1/config")
	expiringRequest.AddCookie(expiringCookie)
	now = now.Add(DefaultSessionTTL)
	if _, err := manager.Authenticate(expiringRequest); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("expired authentication error = %v, want %v", err, ErrUnauthenticated)
	}

	if _, _, err := manager.IssueSession(); err != nil {
		t.Fatalf("issue after expiry: %v", err)
	}
	if got := len(manager.activeSessions); got != 1 {
		t.Fatalf("retained sessions = %d, want 1 current session", got)
	}
}

func TestWatchSessionBindsRemainingLifetimeAndExactRevocation(t *testing.T) {
	t.Parallel()

	manager, err := NewManagerWithSessionTTL(testRoot(), testOrigin, false, 5*time.Minute)
	if err != nil {
		t.Fatalf("create short-lived manager: %v", err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	manager.now = func() time.Time { return now }
	session, _, err := manager.IssueSession()
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	expiresAt, revoked, err := manager.WatchSession(session)
	if err != nil {
		t.Fatalf("watch session: %v", err)
	}
	if want := now.Add(5 * time.Minute); !expiresAt.Equal(want) {
		t.Fatalf("session deadline = %s, want %s", expiresAt, want)
	}
	select {
	case <-revoked:
		t.Fatal("fresh session revocation signal was already closed")
	default:
	}
	if err := manager.RevokeSession(session); err != nil {
		t.Fatalf("revoke session: %v", err)
	}
	select {
	case <-revoked:
	default:
		t.Fatal("exact session revocation did not close the watcher")
	}

	for _, ttl := range []time.Duration{-time.Second, 0, DefaultSessionTTL + time.Nanosecond} {
		if _, err := NewManagerWithSessionTTL(testRoot(), testOrigin, false, ttl); !errors.Is(
			err,
			ErrInvalidSessionTTL,
		) {
			t.Fatalf("invalid TTL %s error = %v, want %v", ttl, err, ErrInvalidSessionTTL)
		}
	}
}

func TestAuthenticateRejectsMalformedForgedAndDuplicateCookies(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	_, cookie, err := manager.IssueSession()
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	forgedRaw, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil {
		t.Fatalf("decode session cookie: %v", err)
	}
	forgedRaw[len(forgedRaw)-1] ^= 0xff

	otherRoot := testRoot()
	otherRoot[0] ^= 0xff
	otherManager, err := NewManager(otherRoot, testOrigin, false)
	if err != nil {
		t.Fatalf("create manager with another root: %v", err)
	}
	_, otherCookie, err := otherManager.IssueSession()
	if err != nil {
		t.Fatalf("issue session with another root: %v", err)
	}

	tests := []struct {
		name    string
		cookies []*http.Cookie
	}{
		{name: "missing"},
		{name: "invalid alphabet", cookies: []*http.Cookie{{Name: SessionCookieName, Value: "not/a/token"}}},
		{name: "padded base64", cookies: []*http.Cookie{{Name: SessionCookieName, Value: cookie.Value + "="}}},
		{name: "truncated", cookies: []*http.Cookie{{Name: SessionCookieName, Value: cookie.Value[:len(cookie.Value)-2]}}},
		{name: "forged MAC", cookies: []*http.Cookie{{Name: SessionCookieName, Value: base64.RawURLEncoding.EncodeToString(forgedRaw)}}},
		{name: "wrong root", cookies: []*http.Cookie{otherCookie}},
		{name: "duplicate", cookies: []*http.Cookie{cookie, cookie}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request := sameOriginRequest(http.MethodGet, "/api/v1/session")
			for _, candidate := range test.cookies {
				request.AddCookie(candidate)
			}
			_, err := manager.Authenticate(request)
			if !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("authenticate error = %v, want %v", err, ErrUnauthenticated)
			}
			if got := HTTPStatus(err); got != http.StatusUnauthorized {
				t.Fatalf("HTTP status = %d, want %d", got, http.StatusUnauthorized)
			}
			if strings.Contains(err.Error(), cookie.Value) {
				t.Fatal("authentication error leaked the session token")
			}
		})
	}
}

func TestAuthorizeMutationEnforcesHostAuthOriginCSRFOrdering(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	session, cookie, err := manager.IssueSession()
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	csrf, err := manager.CSRFToken(session)
	if err != nil {
		t.Fatalf("create CSRF token: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*http.Request)
		want   error
		status int
	}{
		{
			name: "bad host wins before missing auth",
			mutate: func(request *http.Request) {
				request.Host = "localhost:7442"
			},
			want:   ErrMisdirectedHost,
			status: http.StatusMisdirectedRequest,
		},
		{
			name: "missing auth wins before bad origin",
			mutate: func(request *http.Request) {
				request.Header.Set("Origin", "http://localhost:7442")
			},
			want:   ErrUnauthenticated,
			status: http.StatusUnauthorized,
		},
		{
			name: "bad origin wins before missing CSRF",
			mutate: func(request *http.Request) {
				request.AddCookie(cookie)
				request.Header.Set("Origin", "http://localhost:7442")
			},
			want:   ErrForbiddenOrigin,
			status: http.StatusForbidden,
		},
		{
			name: "missing CSRF",
			mutate: func(request *http.Request) {
				request.AddCookie(cookie)
			},
			want:   ErrInvalidCSRF,
			status: http.StatusForbidden,
		},
		{
			name: "wrong CSRF",
			mutate: func(request *http.Request) {
				request.AddCookie(cookie)
				request.Header.Set(CSRFHeaderName, strings.Repeat("A", len(csrf)))
			},
			want:   ErrInvalidCSRF,
			status: http.StatusForbidden,
		},
		{
			name: "session credential is not a CSRF token",
			mutate: func(request *http.Request) {
				request.AddCookie(cookie)
				request.Header.Set(CSRFHeaderName, cookie.Value)
			},
			want:   ErrInvalidCSRF,
			status: http.StatusForbidden,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request := sameOriginRequest(http.MethodPut, "/api/v1/config")
			test.mutate(request)
			_, err := manager.AuthorizeMutation(request)
			if !errors.Is(err, test.want) {
				t.Fatalf("authorize mutation error = %v, want %v", err, test.want)
			}
			if got := HTTPStatus(err); got != test.status {
				t.Fatalf("HTTP status = %d, want %d", got, test.status)
			}
		})
	}
}

func TestAuthorizeSameOriginReadRequiresAuthenticatedSessionCSRF(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	session, cookie, err := manager.IssueSession()
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	csrf, err := manager.CSRFToken(session)
	if err != nil {
		t.Fatalf("derive CSRF: %v", err)
	}
	request := sameOriginRequest(http.MethodGet, "/api/v1/events")
	request.Header.Del("Origin")
	request.AddCookie(cookie)
	request.Header.Set(CSRFHeaderName, csrf)
	if _, err := manager.AuthorizeSameOriginRead(request); err != nil {
		t.Fatalf("authorize browser-compatible same-origin read without Origin header: %v", err)
	}

	request.Header.Del(CSRFHeaderName)
	if _, err := manager.AuthorizeSameOriginRead(request); !errors.Is(err, ErrInvalidCSRF) {
		t.Fatalf("missing stream CSRF error = %v, want %v", err, ErrInvalidCSRF)
	}
	request.Header.Set(CSRFHeaderName, csrf+"x")
	if _, err := manager.AuthorizeSameOriginRead(request); !errors.Is(err, ErrInvalidCSRF) {
		t.Fatalf("invalid stream CSRF error = %v, want %v", err, ErrInvalidCSRF)
	}
}

func TestManagerAndSessionFormattingRedactsRootAndNonce(t *testing.T) {
	t.Parallel()

	root := [rootKeySize]byte{}
	copy(root[:], bytes.Repeat([]byte("S"), rootKeySize))
	manager, err := NewManager(root, testOrigin, false)
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	session, _, err := manager.IssueSession()
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}

	var logOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logOutput, nil))
	logger.Info("credentials", "manager", manager, "session", session)
	managerJSON, err := json.Marshal(manager)
	if err != nil {
		t.Fatalf("marshal manager: %v", err)
	}
	sessionJSON, err := json.Marshal(session)
	if err != nil {
		t.Fatalf("marshal session: %v", err)
	}
	formatted := strings.Join([]string{
		fmt.Sprintf("%v", manager),
		fmt.Sprintf("%#v", manager),
		fmt.Sprintf("%v", session),
		fmt.Sprintf("%#v", session),
		string(managerJSON),
		string(sessionJSON),
		logOutput.String(),
	}, "\n")

	rootRepresentations := []string{
		string(root[:]),
		base64.RawURLEncoding.EncodeToString(root[:]),
		fmt.Sprint(root),
	}
	for _, representation := range rootRepresentations {
		if representation != "" && strings.Contains(formatted, representation) {
			t.Fatalf("formatted credentials leaked root representation %q", representation)
		}
	}
	if !strings.Contains(formatted, "redacted") {
		t.Fatalf("formatted credentials do not signal redaction: %s", formatted)
	}
}

func TestManagerRejectsNonCanonicalOrNonLoopbackOriginAndZeroRoot(t *testing.T) {
	t.Parallel()

	root := testRoot()
	origins := []string{
		"http://example.test:7442",
		"http://127.0.0.1:7442/",
		"http://LOCALHOST:7442",
		"http://localhost:07442",
		"http://user@localhost:7442",
		"http://localhost:7442/path",
		"http://localhost:7442?query=yes",
		"ftp://localhost:7442",
	}
	for _, origin := range origins {
		t.Run(origin, func(t *testing.T) {
			t.Parallel()
			if _, err := NewManager(root, origin, false); !errors.Is(err, ErrInvalidCanonicalOrigin) {
				t.Fatalf("NewManager(%q) error = %v, want %v", origin, err, ErrInvalidCanonicalOrigin)
			}
		})
	}

	if _, err := NewManager([rootKeySize]byte{}, testOrigin, false); !errors.Is(err, ErrInvalidRootKey) {
		t.Fatalf("zero root error = %v, want %v", err, ErrInvalidRootKey)
	}
	if _, err := NewManager(root, "https://localhost:7442", false); !errors.Is(err, ErrCookieSecurityMismatch) {
		t.Fatalf("insecure HTTPS cookie error = %v, want %v", err, ErrCookieSecurityMismatch)
	}
	if _, err := NewManager(root, "http://localhost:7442", true); !errors.Is(err, ErrCookieSecurityMismatch) {
		t.Fatalf("Secure HTTP cookie error = %v, want %v", err, ErrCookieSecurityMismatch)
	}
}

func TestCSRFTokenRejectsZeroValueSession(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	if _, err := manager.CSRFToken(Session{}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("zero session error = %v, want %v", err, ErrUnauthenticated)
	}
}

func TestIssueSessionFailsClosedWhenEntropyIsUnavailable(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	manager.random = failingReader{}
	session, cookie, err := manager.IssueSession()
	if err == nil {
		t.Fatal("issue session unexpectedly succeeded without entropy")
	}
	if session != (Session{}) || cookie != nil {
		t.Fatalf("failed issue returned credential material: session=%v cookie=%v", session, cookie)
	}
}

func TestBrowserHandoffApprovalAndClaimAreTwoPhase(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	now := time.Unix(1_750_000_000, 0).UTC()
	manager.now = func() time.Time { return now }
	manager.random = bytes.NewReader(bytes.Repeat([]byte{0x42}, browserHandoffIDSize))

	secret := [browserHandoffSecretSize]byte{1, 2, 3, 4}
	digest := sha256.Sum256(secret[:])
	handoff, err := manager.IssueBrowserHandoff(digest)
	if err != nil {
		t.Fatalf("issue browser handoff: %v", err)
	}
	if !ValidBrowserHandoffCodeEncoding(handoff.Code()) {
		t.Fatal("issued browser handoff does not have canonical public encoding")
	}
	if !strings.HasPrefix(handoff.Code(), browserHandoffVersion+".") {
		t.Fatalf("handoff code = %q, want %s wire", handoff.Code(), browserHandoffVersion)
	}
	if got, want := handoff.ExpiresAt(), now.Add(BrowserHandoffTTL); !got.Equal(want) {
		t.Fatalf("handoff expiry = %s, want %s", got, want)
	}

	if _, err := manager.ClaimBrowserHandoff(handoff.Code(), secret); !errors.Is(err, ErrBrowserHandoffPending) {
		t.Fatalf("claim before approval error = %v, want pending", err)
	} else {
		var pending *BrowserHandoffPendingError
		if !errors.As(err, &pending) || !pending.ExpiresAt().Equal(handoff.ExpiresAt()) {
			t.Fatalf("pending error = %#v, want verified expiry %s", err, handoff.ExpiresAt())
		}
	}

	approval, err := manager.ApproveBrowserHandoff(handoff.Code())
	if err != nil {
		t.Fatalf("reserve approval: %v", err)
	}
	if !approval.NeedsAudit() {
		t.Fatal("new approval does not require audit")
	}
	if _, err := manager.ClaimBrowserHandoff(handoff.Code(), secret); !errors.Is(err, ErrBrowserHandoffPending) {
		t.Fatalf("claim while approval is fenced error = %v, want pending", err)
	}
	staleApproval := approval
	if err := manager.RollbackBrowserHandoffApproval(approval); err != nil {
		t.Fatalf("rollback approval: %v", err)
	}
	if _, err := manager.ClaimBrowserHandoff(handoff.Code(), secret); !errors.Is(err, ErrBrowserHandoffPending) {
		t.Fatalf("claim after approval rollback error = %v, want pending", err)
	}

	approval, err = manager.ApproveBrowserHandoff(handoff.Code())
	if err != nil {
		t.Fatalf("reserve approval again: %v", err)
	}
	if err := manager.CommitBrowserHandoffApproval(staleApproval); !errors.Is(err, ErrBrowserHandoffFence) {
		t.Fatalf("stale approval commit error = %v, want fence rejection", err)
	}
	if err := manager.CommitBrowserHandoffApproval(approval); err != nil {
		t.Fatalf("commit approval: %v", err)
	}
	retry, err := manager.ApproveBrowserHandoff(handoff.Code())
	if err != nil {
		t.Fatalf("retry committed approval: %v", err)
	}
	if retry.NeedsAudit() {
		t.Fatal("committed approval retry requested duplicate audit")
	}

	claim, err := manager.ClaimBrowserHandoff(handoff.Code(), secret)
	if err != nil {
		t.Fatalf("reserve claim: %v", err)
	}
	if _, err := manager.ClaimBrowserHandoff(handoff.Code(), secret); !errors.Is(err, ErrBrowserHandoffClaiming) {
		t.Fatalf("parallel claim error = %v, want claiming", err)
	}
	staleClaim := claim
	if err := manager.RollbackBrowserHandoffClaim(claim); err != nil {
		t.Fatalf("rollback claim: %v", err)
	}
	claim, err = manager.ClaimBrowserHandoff(handoff.Code(), secret)
	if err != nil {
		t.Fatalf("reserve claim again: %v", err)
	}
	if err := manager.CommitBrowserHandoffClaim(staleClaim); !errors.Is(err, ErrBrowserHandoffFence) {
		t.Fatalf("stale claim commit error = %v, want fence rejection", err)
	}
	if err := manager.CommitBrowserHandoffClaim(claim); err != nil {
		t.Fatalf("commit claim: %v", err)
	}
	if _, err := manager.ClaimBrowserHandoff(handoff.Code(), secret); !errors.Is(err, ErrBrowserHandoffClaimed) {
		t.Fatalf("claim replay error = %v, want claimed", err)
	}
}

func TestBrowserHandoffRejectsTamperSecretExpiryAndRestart(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	now := time.Unix(1_750_000_000, 0).UTC()
	manager.now = func() time.Time { return now }
	manager.random = bytes.NewReader(bytes.Repeat([]byte{0x21}, browserHandoffIDSize))
	secret := [browserHandoffSecretSize]byte{8, 7, 6, 5}
	digest := sha256.Sum256(secret[:])
	handoff, err := manager.IssueBrowserHandoff(digest)
	if err != nil {
		t.Fatalf("issue browser handoff: %v", err)
	}

	wrongSecret := secret
	wrongSecret[0] ^= 0xff
	if _, err := manager.ClaimBrowserHandoff(handoff.Code(), wrongSecret); !errors.Is(err, ErrInvalidBrowserHandoff) {
		t.Fatalf("wrong secret error = %v, want invalid", err)
	}
	tamperedSuffix := "A"
	if strings.HasSuffix(handoff.Code(), tamperedSuffix) {
		tamperedSuffix = "E"
	}
	tampered := handoff.Code()[:len(handoff.Code())-1] + tamperedSuffix
	if _, err := manager.ApproveBrowserHandoff(tampered); !errors.Is(err, ErrInvalidBrowserHandoff) {
		t.Fatalf("tampered code error = %v, want invalid", err)
	}
	if !ValidBrowserHandoffCodeEncoding(tampered) {
		t.Fatal("canonical encoding check unexpectedly authenticated a tampered MAC")
	}
	for _, invalid := range []string{
		"",
		"twh0" + strings.TrimPrefix(handoff.Code(), browserHandoffVersion),
		strings.Replace(handoff.Code(), ".", "..", 1),
		handoff.Code() + "=",
	} {
		if ValidBrowserHandoffCodeEncoding(invalid) {
			t.Fatalf("invalid browser handoff encoding accepted: %q", invalid)
		}
	}

	now = handoff.ExpiresAt()
	if _, err := manager.ApproveBrowserHandoff(handoff.Code()); !errors.Is(err, ErrExpiredBrowserHandoff) {
		t.Fatalf("expired code error = %v, want expired", err)
	}

	restarted := newTestManager(t, testOrigin, false)
	restarted.now = func() time.Time { return handoff.ExpiresAt().Add(-time.Second) }
	if _, err := restarted.ApproveBrowserHandoff(handoff.Code()); !errors.Is(err, ErrInvalidBrowserHandoff) {
		t.Fatalf("restarted manager error = %v, want invalid", err)
	}
}

func TestBrowserHandoffConcurrentClaimHasExactlyOneFence(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	now := time.Unix(1_750_000_000, 0).UTC()
	manager.now = func() time.Time { return now }
	manager.random = bytes.NewReader(bytes.Repeat([]byte{0x33}, browserHandoffIDSize))
	secret := [browserHandoffSecretSize]byte{9, 8, 7, 6}
	handoff, err := manager.IssueBrowserHandoff(sha256.Sum256(secret[:]))
	if err != nil {
		t.Fatalf("issue browser handoff: %v", err)
	}
	approval, err := manager.ApproveBrowserHandoff(handoff.Code())
	if err != nil {
		t.Fatalf("reserve approval: %v", err)
	}
	if err := manager.CommitBrowserHandoffApproval(approval); err != nil {
		t.Fatalf("commit approval: %v", err)
	}

	const contenders = 32
	start := make(chan struct{})
	results := make(chan error, contenders)
	claims := make(chan BrowserHandoffClaim, contenders)
	var wait sync.WaitGroup
	for range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			claim, claimErr := manager.ClaimBrowserHandoff(handoff.Code(), secret)
			if claimErr == nil {
				claims <- claim
			}
			results <- claimErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(claims)

	successes := 0
	for claimErr := range results {
		switch {
		case claimErr == nil:
			successes++
		case errors.Is(claimErr, ErrBrowserHandoffClaiming):
		default:
			t.Fatalf("parallel claim error = %v", claimErr)
		}
	}
	if successes != 1 {
		t.Fatalf("successful claim fences = %d, want 1", successes)
	}
	claim := <-claims
	if err := manager.CommitBrowserHandoffClaim(claim); err != nil {
		t.Fatalf("commit winning claim: %v", err)
	}
}

func TestBrowserHandoffExpiryDuringFinalizationLeavesNoAuthority(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	now := time.Unix(1_750_000_000, 0).UTC()
	manager.now = func() time.Time { return now }
	manager.random = bytes.NewReader(bytes.Repeat([]byte{0x4d}, browserHandoffIDSize*2))
	secret := [browserHandoffSecretSize]byte{4, 3, 2, 1}

	approvalHandoff, err := manager.IssueBrowserHandoff(sha256.Sum256(secret[:]))
	if err != nil {
		t.Fatalf("issue approval handoff: %v", err)
	}
	approval, err := manager.ApproveBrowserHandoff(approvalHandoff.Code())
	if err != nil {
		t.Fatalf("reserve approval: %v", err)
	}
	now = approvalHandoff.ExpiresAt()
	if err := manager.CommitBrowserHandoffApproval(approval); !errors.Is(
		err,
		ErrExpiredBrowserHandoff,
	) {
		t.Fatalf("approval commit at expiry error = %v, want expired", err)
	}
	if _, err := manager.ClaimBrowserHandoff(
		approvalHandoff.Code(),
		secret,
	); !errors.Is(err, ErrExpiredBrowserHandoff) {
		t.Fatalf("expired approval became claimable: %v", err)
	}

	now = time.Unix(1_750_000_100, 0).UTC()
	claimHandoff, err := manager.IssueBrowserHandoff(sha256.Sum256(secret[:]))
	if err != nil {
		t.Fatalf("issue claim handoff: %v", err)
	}
	approval, err = manager.ApproveBrowserHandoff(claimHandoff.Code())
	if err != nil {
		t.Fatalf("reserve claim approval: %v", err)
	}
	if err := manager.CommitBrowserHandoffApproval(approval); err != nil {
		t.Fatalf("commit claim approval: %v", err)
	}
	claim, err := manager.ClaimBrowserHandoff(claimHandoff.Code(), secret)
	if err != nil {
		t.Fatalf("reserve claim: %v", err)
	}
	now = claimHandoff.ExpiresAt()
	if err := manager.CommitBrowserHandoffClaim(claim); !errors.Is(
		err,
		ErrExpiredBrowserHandoff,
	) {
		t.Fatalf("claim commit at expiry error = %v, want expired", err)
	}
	if _, err := manager.ClaimBrowserHandoff(
		claimHandoff.Code(),
		secret,
	); !errors.Is(err, ErrExpiredBrowserHandoff) {
		t.Fatalf("expired claim remained reusable: %v", err)
	}
}

func TestBrowserHandoffValuesRedactCodeDigestAndFences(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	manager.random = bytes.NewReader(bytes.Repeat([]byte{0x5a}, browserHandoffIDSize))
	secret := [browserHandoffSecretSize]byte{1, 3, 3, 7}
	digest := sha256.Sum256(secret[:])
	handoff, err := manager.IssueBrowserHandoff(digest)
	if err != nil {
		t.Fatalf("issue browser handoff: %v", err)
	}
	approval, err := manager.ApproveBrowserHandoff(handoff.Code())
	if err != nil {
		t.Fatalf("reserve approval: %v", err)
	}
	_, pendingErr := manager.ClaimBrowserHandoff(handoff.Code(), secret)
	if !errors.Is(pendingErr, ErrBrowserHandoffPending) {
		t.Fatalf("claim during approval error = %v, want pending", pendingErr)
	}

	handoffJSON, err := json.Marshal(handoff)
	if err != nil {
		t.Fatalf("marshal handoff: %v", err)
	}
	approvalJSON, err := json.Marshal(approval)
	if err != nil {
		t.Fatalf("marshal approval: %v", err)
	}
	pendingJSON, err := json.Marshal(pendingErr)
	if err != nil {
		t.Fatalf("marshal pending error: %v", err)
	}
	formatted := strings.Join([]string{
		fmt.Sprint(handoff),
		fmt.Sprintf("%#v", handoff),
		string(handoffJSON),
		fmt.Sprint(approval),
		fmt.Sprintf("%#v", approval),
		string(approvalJSON),
		fmt.Sprint(pendingErr),
		fmt.Sprintf("%#v", pendingErr),
		string(pendingJSON),
	}, "\n")
	for _, sensitive := range []string{
		handoff.Code(),
		base64.RawURLEncoding.EncodeToString(digest[:]),
		base64.RawURLEncoding.EncodeToString(secret[:]),
	} {
		if strings.Contains(formatted, sensitive) {
			t.Fatalf("formatted browser handoff leaked %q", sensitive)
		}
	}
	if !strings.Contains(formatted, "redacted") {
		t.Fatalf("browser handoff values do not signal redaction: %s", formatted)
	}
}

func TestIssueBrowserHandoffFailsClosedWithoutEntropyOrClock(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	manager.random = failingReader{}
	handoff, err := manager.IssueBrowserHandoff(sha256.Sum256([]byte("claim")))
	if err == nil {
		t.Fatal("browser handoff unexpectedly succeeded without entropy")
	}
	if handoff != (BrowserHandoff{}) {
		t.Fatalf("failed issue returned handoff material: %v", handoff)
	}

	manager = newTestManager(t, testOrigin, false)
	manager.now = func() time.Time { return time.Time{} }
	handoff, err = manager.IssueBrowserHandoff(sha256.Sum256([]byte("claim")))
	if !errors.Is(err, ErrSessionClock) {
		t.Fatalf("zero clock error = %v, want %v", err, ErrSessionClock)
	}
	if handoff != (BrowserHandoff{}) {
		t.Fatalf("failed issue returned handoff material: %v", handoff)
	}
}

func TestIssueBrowserHandoffAcceptsCanonicalZeroDigestWithoutGrantingClaim(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, testOrigin, false)
	manager.random = bytes.NewReader(bytes.Repeat([]byte{0x7c}, browserHandoffIDSize))
	handoff, err := manager.IssueBrowserHandoff([sha256.Size]byte{})
	if err != nil {
		t.Fatalf("issue zero digest handoff: %v", err)
	}
	if !ValidBrowserHandoffCodeEncoding(handoff.Code()) {
		t.Fatal("zero digest handoff is not canonically encoded")
	}
	approval, err := manager.ApproveBrowserHandoff(handoff.Code())
	if err != nil {
		t.Fatalf("reserve zero digest approval: %v", err)
	}
	if err := manager.CommitBrowserHandoffApproval(approval); err != nil {
		t.Fatalf("commit zero digest approval: %v", err)
	}
	if _, err := manager.ClaimBrowserHandoff(
		handoff.Code(),
		[browserHandoffSecretSize]byte{},
	); !errors.Is(err, ErrInvalidBrowserHandoff) {
		t.Fatalf("zero secret claim error = %v, want invalid digest match", err)
	}
}

func newTestManager(t *testing.T, origin string, secure bool) *Manager {
	t.Helper()

	manager, err := NewManager(testRoot(), origin, secure)
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	return manager
}

func testRoot() [rootKeySize]byte {
	root := [rootKeySize]byte{}
	for index := range root {
		root[index] = byte(index + 1)
	}
	return root
}

func sameOriginRequest(method, path string) *http.Request {
	request := httptest.NewRequest(method, testOrigin+path, nil)
	request.Header.Set("Origin", testOrigin)
	return request
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("entropy unavailable")
}

func mustBootstrapProof(
	t *testing.T,
	root [rootKeySize]byte,
	origin string,
	issuedAt time.Time,
	fill byte,
) string {
	t.Helper()

	proof, err := NewBootstrapProof(
		root,
		origin,
		issuedAt,
		bytes.NewReader(bytes.Repeat([]byte{fill}, bootstrapNonceSize)),
	)
	if err != nil {
		t.Fatalf("create bootstrap proof: %v", err)
	}
	return proof
}
