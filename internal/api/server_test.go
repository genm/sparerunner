package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/genm/sparerunner/internal/api/gen"
	"github.com/genm/sparerunner/internal/auth"
	"github.com/genm/sparerunner/internal/domain"
)

const apiTestOrigin = "http://127.0.0.1:7442"

func TestBrowserHandoffRequiresOwnerApprovalAndIssuesExactlyOneSession(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	var secret [32]byte
	for index := range secret {
		secret[index] = byte(index + 1)
	}
	handoff := createAPIBrowserHandoff(t, handler, secret)

	pending := claimAPIBrowserHandoff(t, handler, handoff.Code, secret)
	if pending.Code != http.StatusAccepted {
		t.Fatalf("pending claim = %d %s", pending.Code, pending.Body.String())
	}
	if cookies := pending.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("pending claim issued cookies: %#v", cookies)
	}
	var pendingState gen.BrowserHandoffPending
	if err := json.Unmarshal(pending.Body.Bytes(), &pendingState); err != nil {
		t.Fatal(err)
	}
	if pendingState.State != gen.BrowserHandoffPendingStatePending ||
		!pendingState.ExpiresAt.Equal(handoff.ExpiresAt) {
		t.Fatalf("pending state = %#v, handoff = %#v", pendingState, handoff)
	}

	wrongSecret := secret
	wrongSecret[0] ^= 0xff
	wrong := claimAPIBrowserHandoff(t, handler, handoff.Code, wrongSecret)
	assertProblem(t, wrong, http.StatusUnauthorized, "browser_handoff_invalid")
	if cookies := wrong.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("wrong-secret claim issued cookies: %#v", cookies)
	}

	cookie, csrf := bootstrapAPISession(t, handler)
	authorize := authorizeAPIBrowserHandoff(
		t,
		handler,
		cookie,
		csrf,
		handoff.Code,
	)
	if authorize.Code != http.StatusNoContent || authorize.Body.Len() != 0 {
		t.Fatalf("authorization = %d %q", authorize.Code, authorize.Body.String())
	}
	backend.mu.Lock()
	auditCountAfterApproval := len(backend.audits)
	approvalAudit := backend.audits[auditCountAfterApproval-1]
	backend.mu.Unlock()
	if approvalAudit.Action != "browser_handoff_authorized" ||
		approvalAudit.ResourceID != "" {
		t.Fatalf("approval audit = %#v", approvalAudit)
	}

	retry := authorizeAPIBrowserHandoff(t, handler, cookie, csrf, handoff.Code)
	if retry.Code != http.StatusNoContent {
		t.Fatalf("idempotent authorization = %d %s", retry.Code, retry.Body.String())
	}
	backend.mu.Lock()
	if len(backend.audits) != auditCountAfterApproval {
		t.Fatalf(
			"idempotent authorization appended an audit: %#v",
			backend.audits,
		)
	}
	backend.mu.Unlock()

	claimed := claimAPIBrowserHandoff(t, handler, handoff.Code, secret)
	if claimed.Code != http.StatusCreated {
		t.Fatalf("approved claim = %d %s", claimed.Code, claimed.Body.String())
	}
	cookies := claimed.Result().Cookies()
	if len(cookies) != 1 ||
		cookies[0].Name != auth.SessionCookieName ||
		cookies[0].Value == "" {
		t.Fatalf("approved claim cookies = %#v", cookies)
	}
	if strings.Contains(claimed.Body.String(), base64.RawURLEncoding.EncodeToString(secret[:])) ||
		strings.Contains(claimed.Body.String(), handoff.Code) {
		t.Fatalf("claim response reflected handoff material: %s", claimed.Body.String())
	}

	replayed := claimAPIBrowserHandoff(t, handler, handoff.Code, secret)
	assertProblem(
		t,
		replayed,
		http.StatusConflict,
		"browser_handoff_already_claimed",
	)
	if cookies := replayed.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("replayed claim issued cookies: %#v", cookies)
	}
}

func TestBrowserHandoffConcurrentHTTPClaimsIssueExactlyOneSession(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	secret := [32]byte{9, 8, 7, 6}
	handoff := createAPIBrowserHandoff(t, handler, secret)
	cookie, csrf := bootstrapAPISession(t, handler)
	authorized := authorizeAPIBrowserHandoff(t, handler, cookie, csrf, handoff.Code)
	if authorized.Code != http.StatusNoContent {
		t.Fatalf("authorization = %d %s", authorized.Code, authorized.Body.String())
	}

	encodedSecret := base64.RawURLEncoding.EncodeToString(secret[:])
	body, err := json.Marshal(gen.ClaimBrowserHandoffRequest{
		Code:        handoff.Code,
		ClaimSecret: &encodedSecret,
	})
	if err != nil {
		t.Fatal(err)
	}

	const contenders = 16
	start := make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, contenders)
	var wait sync.WaitGroup
	for range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			request := httptest.NewRequest(
				http.MethodPost,
				apiTestOrigin+"/api/v1/browser-handoffs/claim",
				bytes.NewReader(body),
			)
			request.Header.Set("Origin", apiTestOrigin)
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			responses <- response
		}()
	}
	close(start)
	wait.Wait()
	close(responses)

	created := 0
	for response := range responses {
		switch response.Code {
		case http.StatusCreated:
			created++
			if cookies := response.Result().Cookies(); len(cookies) != 1 ||
				cookies[0].Name != auth.SessionCookieName {
				t.Fatalf("winning claim cookies = %#v", cookies)
			}
		case http.StatusConflict:
			if cookies := response.Result().Cookies(); len(cookies) != 0 {
				t.Fatalf("losing claim issued cookies: %#v", cookies)
			}
			var problem gen.Problem
			if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
				t.Fatal(err)
			}
			if problem.Code != "browser_handoff_claim_in_progress" &&
				problem.Code != "browser_handoff_already_claimed" {
				t.Fatalf("losing claim problem = %#v", problem)
			}
		default:
			t.Fatalf("parallel claim = %d %s", response.Code, response.Body.String())
		}
	}
	if created != 1 {
		t.Fatalf("created sessions = %d, want exactly one", created)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()
	authenticationAudits := 0
	for _, event := range backend.audits {
		if event.Action == "authentication_succeeded" {
			authenticationAudits++
		}
	}
	// One audit belongs to the owner CLI bootstrap and one to the winning browser.
	if authenticationAudits != 2 {
		t.Fatalf("authentication audits = %d, want two", authenticationAudits)
	}
}

func TestBrowserHandoffClaimsBrowserDigestKnownVector(t *testing.T) {
	t.Parallel()

	handler, _ := newAPITestHandler(t)
	var secret [32]byte
	for index := range secret {
		secret[index] = byte(index)
	}
	const browserDigest = "Yw3NKWbEM2aRElRIu7JbT_QSpJxzLbLIq8G4WBvXEN0"
	handoff := createAPIBrowserHandoff(t, handler, secret)
	parts := strings.Split(handoff.Code, ".")
	if len(parts) != 5 || parts[3] != browserDigest {
		t.Fatalf("handoff digest = %#v, want browser vector %q", parts, browserDigest)
	}

	cookie, csrf := bootstrapAPISession(t, handler)
	authorized := authorizeAPIBrowserHandoff(t, handler, cookie, csrf, handoff.Code)
	if authorized.Code != http.StatusNoContent {
		t.Fatalf("authorization = %d %s", authorized.Code, authorized.Body.String())
	}
	claimed := claimAPIBrowserHandoff(t, handler, handoff.Code, secret)
	if claimed.Code != http.StatusCreated {
		t.Fatalf("known-vector claim = %d %s", claimed.Code, claimed.Body.String())
	}
}

func TestBrowserHandoffChecksHostAndOriginBeforeClaimMaterial(t *testing.T) {
	t.Parallel()

	handler, _ := newAPITestHandler(t)
	canary := "claim-secret-canary.example.test"
	tests := []struct {
		name   string
		target string
		origin string
		status int
		code   string
	}{
		{
			name:   "wrong host",
			target: "http://localhost:7442/api/v1/browser-handoffs/claim",
			origin: apiTestOrigin,
			status: http.StatusMisdirectedRequest,
			code:   "misdirected_host",
		},
		{
			name:   "wrong origin",
			target: apiTestOrigin + "/api/v1/browser-handoffs/claim",
			origin: "http://127.0.0.1:7443",
			status: http.StatusForbidden,
			code:   "request_forbidden",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodPost,
				test.target,
				strings.NewReader(`{"code":"`+canary+`","claimSecret":"`+canary+`"}`),
			)
			request.Header.Set("Origin", test.origin)
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			assertSecretFreeProblem(t, response, test.status, test.code, canary)
			if cookies := response.Result().Cookies(); len(cookies) != 0 {
				t.Fatalf("rejected claim issued cookies: %#v", cookies)
			}
		})
	}
}

func TestBrowserHandoffAuditFailuresRollBackAuthority(t *testing.T) {
	t.Parallel()

	t.Run("approval", func(t *testing.T) {
		handler, backend := newAPITestHandler(t)
		secret := [32]byte{1, 2, 3}
		handoff := createAPIBrowserHandoff(t, handler, secret)
		cookie, csrf := bootstrapAPISession(t, handler)
		backend.mu.Lock()
		backend.auditErr = errors.New("approval audit secret-canary")
		backend.mu.Unlock()

		failed := authorizeAPIBrowserHandoff(
			t,
			handler,
			cookie,
			csrf,
			handoff.Code,
		)
		assertSecretFreeProblem(
			t,
			failed,
			http.StatusServiceUnavailable,
			"management_unavailable",
			"secret-canary",
		)
		pending := claimAPIBrowserHandoff(t, handler, handoff.Code, secret)
		if pending.Code != http.StatusAccepted {
			t.Fatalf("failed approval became claimable: %d %s", pending.Code, pending.Body.String())
		}

		backend.mu.Lock()
		backend.auditErr = nil
		backend.mu.Unlock()
		succeeded := authorizeAPIBrowserHandoff(
			t,
			handler,
			cookie,
			csrf,
			handoff.Code,
		)
		if succeeded.Code != http.StatusNoContent {
			t.Fatalf("authorization after rollback = %d %s", succeeded.Code, succeeded.Body.String())
		}
	})

	t.Run("authentication", func(t *testing.T) {
		handler, backend := newAPITestHandler(t)
		secret := [32]byte{4, 5, 6}
		handoff := createAPIBrowserHandoff(t, handler, secret)
		cookie, csrf := bootstrapAPISession(t, handler)
		authorized := authorizeAPIBrowserHandoff(
			t,
			handler,
			cookie,
			csrf,
			handoff.Code,
		)
		if authorized.Code != http.StatusNoContent {
			t.Fatalf("authorization = %d %s", authorized.Code, authorized.Body.String())
		}
		backend.mu.Lock()
		backend.auditErr = errors.New("authentication audit secret-canary")
		backend.mu.Unlock()

		failed := claimAPIBrowserHandoff(t, handler, handoff.Code, secret)
		assertSecretFreeProblem(
			t,
			failed,
			http.StatusServiceUnavailable,
			"management_unavailable",
			"secret-canary",
		)
		if cookies := failed.Result().Cookies(); len(cookies) != 0 {
			t.Fatalf("failed authentication audit issued cookies: %#v", cookies)
		}

		backend.mu.Lock()
		backend.auditErr = nil
		backend.mu.Unlock()
		retry := claimAPIBrowserHandoff(t, handler, handoff.Code, secret)
		if retry.Code != http.StatusCreated {
			t.Fatalf("claim after audit rollback = %d %s", retry.Code, retry.Body.String())
		}
	})
}

func TestSessionBootstrapIsExplicitExactOriginAndStaticReadsDoNotIssueCookie(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	staticRequest := httptest.NewRequest(http.MethodGet, apiTestOrigin+"/", nil)
	staticResponse := httptest.NewRecorder()
	handler.ServeHTTP(staticResponse, staticRequest)
	if staticResponse.Code != http.StatusOK || staticResponse.Body.String() != "embedded UI" {
		t.Fatalf("static response = %d %q", staticResponse.Code, staticResponse.Body.String())
	}
	if cookies := staticResponse.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("static read issued cookies: %#v", cookies)
	}
	for name, want := range map[string]string{
		"Cache-Control":                "no-store",
		"Cross-Origin-Opener-Policy":   "same-origin",
		"Cross-Origin-Resource-Policy": "same-origin",
		"Permissions-Policy":           "camera=(), microphone=(), geolocation=()",
		"Referrer-Policy":              "no-referrer",
		"X-Content-Type-Options":       "nosniff",
		"X-Frame-Options":              "DENY",
	} {
		if got := staticResponse.Header().Get(name); got != want {
			t.Fatalf("static %s = %q, want %q", name, got, want)
		}
	}
	csp := staticResponse.Header().Get("Content-Security-Policy")
	for _, directive := range []string{
		"default-src 'self'",
		"base-uri 'none'",
		"frame-ancestors 'none'",
		"form-action 'self' https://github.com",
		"connect-src 'self'",
	} {
		if !strings.Contains(csp, directive) {
			t.Fatalf("static CSP %q is missing %q", csp, directive)
		}
	}

	wrongHost := httptest.NewRequest(http.MethodPost, "http://localhost:7442/api/v1/session", nil)
	wrongHost.Header.Set("Origin", apiTestOrigin)
	wrongHost.Header.Set("Forwarded", "host=127.0.0.1:7442;proto=http")
	wrongHost.Header.Set("X-Forwarded-Host", "127.0.0.1:7442")
	setAPIBootstrapProof(t, wrongHost)
	wrongResponse := httptest.NewRecorder()
	handler.ServeHTTP(wrongResponse, wrongHost)
	assertProblem(t, wrongResponse, http.StatusMisdirectedRequest, "misdirected_host")
	if cookies := wrongResponse.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("misdirected bootstrap issued cookies: %#v", cookies)
	}

	missingProof := httptest.NewRequest(http.MethodPost, apiTestOrigin+"/api/v1/session", nil)
	missingProof.Header.Set("Origin", apiTestOrigin)
	missingProofResponse := httptest.NewRecorder()
	handler.ServeHTTP(missingProofResponse, missingProof)
	assertProblem(t, missingProofResponse, http.StatusUnauthorized, "authentication_failed")
	if cookies := missingProofResponse.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("ownerless bootstrap issued cookies: %#v", cookies)
	}

	cookie, csrf := bootstrapAPISession(t, handler)
	if cookie == nil || csrf == "" {
		t.Fatal("session bootstrap did not return the cookie and CSRF pair")
	}
	if len(backend.audits) != 3 {
		t.Fatalf("audit events = %#v, want two rejections and successful authentication", backend.audits)
	}
	if got := backend.audits[len(backend.audits)-1].Action; got != "authentication_succeeded" {
		t.Fatalf("last bootstrap audit action = %q", got)
	}
	if handler.(*server).events.currentGeneration() != 3 {
		t.Fatal("audit append did not invalidate management readers")
	}
}

func TestAuditPersistenceFailurePublishesHealthInvalidation(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	backend.auditErr = errors.New("audit secret-canary")
	before := handler.(*server).events.currentGeneration()
	request := httptest.NewRequest(http.MethodPost, apiTestOrigin+"/api/v1/session", nil)
	request.Header.Set("Origin", apiTestOrigin)
	setAPIBootstrapProof(t, request)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertProblem(t, response, http.StatusServiceUnavailable, "management_unavailable")
	if strings.Contains(response.Body.String(), "secret-canary") {
		t.Fatalf("audit failure leaked through response: %s", response.Body.String())
	}
	if handler.(*server).events.currentGeneration() != before+1 {
		t.Fatal("audit persistence failure did not invalidate health readers")
	}
}

func TestAuditPersistenceIgnoresClientCancellationWithinServerTimeout(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(
		http.MethodPost,
		apiTestOrigin+"/api/v1/session",
		nil,
	).WithContext(ctx)
	request.Header.Set("Origin", apiTestOrigin)
	setAPIBootstrapProof(t, request)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("cancelled-client bootstrap = %d %s", response.Code, response.Body.String())
	}
	backend.mu.Lock()
	auditContextErr := backend.auditContextErr
	backend.mu.Unlock()
	if auditContextErr != nil {
		t.Fatalf("client cancellation reached audit authority: %v", auditContextErr)
	}
}

func TestDeleteSessionRevokesCopiedCookieServerSide(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	cookie, csrf := bootstrapAPISession(t, handler)
	logout := httptest.NewRequest(http.MethodDelete, apiTestOrigin+"/api/v1/session", nil)
	logout.Header.Set("Origin", apiTestOrigin)
	logout.Header.Set(auth.CSRFHeaderName, csrf)
	logout.AddCookie(cookie)
	logoutResponse := httptest.NewRecorder()
	handler.ServeHTTP(logoutResponse, logout)
	if logoutResponse.Code != http.StatusNoContent {
		t.Fatalf("logout response = %d %s", logoutResponse.Code, logoutResponse.Body.String())
	}
	deletions := logoutResponse.Result().Cookies()
	if len(deletions) != 1 ||
		deletions[0].Name != auth.SessionCookieName ||
		deletions[0].MaxAge != -1 {
		t.Fatalf("logout cookie = %#v, want exact deletion cookie", deletions)
	}

	reuse := httptest.NewRequest(http.MethodGet, apiTestOrigin+"/api/v1/session", nil)
	reuse.Header.Set("Origin", apiTestOrigin)
	reuse.AddCookie(cookie)
	reuseResponse := httptest.NewRecorder()
	handler.ServeHTTP(reuseResponse, reuse)
	assertProblem(t, reuseResponse, http.StatusUnauthorized, "authentication_failed")
	if got := len(backend.audits); got != 3 {
		t.Fatalf("audit events = %d, want bootstrap, logout, and rejected reuse", got)
	}
}

func TestDeleteSessionRevokesCopiedCookieWhenAuditIsUnavailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(*apiTestBackend)
	}{
		{
			name: "audit append fails",
			configure: func(backend *apiTestBackend) {
				backend.auditErr = errors.New("audit unavailable")
			},
		},
		{
			name: "audit authority is already degraded",
			configure: func(backend *apiTestBackend) {
				backend.auditHealthy = false
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newAPITestHandler(t)
			cookie, csrf := bootstrapAPISession(t, handler)
			backend.mu.Lock()
			test.configure(backend)
			backend.mu.Unlock()

			logout := httptest.NewRequest(
				http.MethodDelete,
				apiTestOrigin+"/api/v1/session",
				nil,
			)
			logout.Header.Set("Origin", apiTestOrigin)
			logout.Header.Set(auth.CSRFHeaderName, csrf)
			logout.AddCookie(cookie)
			logoutResponse := httptest.NewRecorder()
			handler.ServeHTTP(logoutResponse, logout)
			assertProblem(
				t,
				logoutResponse,
				http.StatusServiceUnavailable,
				"management_unavailable",
			)
			deletions := logoutResponse.Result().Cookies()
			if len(deletions) != 1 ||
				deletions[0].Name != auth.SessionCookieName ||
				deletions[0].MaxAge != -1 {
				t.Fatalf(
					"logout cookie = %#v, want exact deletion cookie despite audit outage",
					deletions,
				)
			}

			reuse := httptest.NewRequest(
				http.MethodGet,
				apiTestOrigin+"/api/v1/session",
				nil,
			)
			reuse.Header.Set("Origin", apiTestOrigin)
			reuse.AddCookie(cookie)
			reuseResponse := httptest.NewRecorder()
			handler.ServeHTTP(reuseResponse, reuse)
			assertProblem(
				t,
				reuseResponse,
				http.StatusUnauthorized,
				"authentication_failed",
			)
		})
	}
}

func TestAuditEventsAuthenticatesBeforeQueryValidation(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	request := httptest.NewRequest(
		http.MethodGet,
		apiTestOrigin+"/api/v1/audit-events?cursor=not-a-cursor&limit=0",
		nil,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertProblem(t, response, http.StatusUnauthorized, "authentication_failed")

	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.auditCalls != 0 {
		t.Fatalf("unauthenticated query reached audit reader %d times", backend.auditCalls)
	}
}

func TestAuditEventsRejectsNonCanonicalOrUnboundedQuery(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	cookie, _ := bootstrapAPISession(t, handler)
	for _, query := range []string{
		"cursor=not-a-cursor",
		"cursor=aud1_AAAAAAAAAAA",
		"cursor=aud1_AAAAAAAAAAE%3D",
		"cursor=aud1_AAAAAAAAAAE&cursor=aud1_AAAAAAAAAAI",
		"limit=0",
		"limit=501",
		"limit=01",
		"limit=invalid",
		"limit=1&limit=2",
	} {
		request := httptest.NewRequest(
			http.MethodGet,
			apiTestOrigin+"/api/v1/audit-events?"+query,
			nil,
		)
		request.AddCookie(cookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		assertProblem(t, response, http.StatusBadRequest, "invalid_query")
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.auditCalls != 0 {
		t.Fatalf("invalid query reached audit reader %d times", backend.auditCalls)
	}
}

func TestAuditEventsPagesWithBoundedDefaultsAndOpaqueCursor(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	cookie, _ := bootstrapAPISession(t, handler)
	nextAfter := uint64(2)
	resumeAfter := uint64(2)
	backend.mu.Lock()
	backend.auditPage = AuditPage{NextAfter: &nextAfter, ResumeAfter: &resumeAfter}
	backend.mu.Unlock()

	firstRequest := httptest.NewRequest(
		http.MethodGet,
		apiTestOrigin+"/api/v1/audit-events",
		nil,
	)
	firstRequest.AddCookie(cookie)
	firstResponse := httptest.NewRecorder()
	handler.ServeHTTP(firstResponse, firstRequest)
	if firstResponse.Code != http.StatusOK {
		t.Fatalf("first page = %d %s", firstResponse.Code, firstResponse.Body.String())
	}
	var first gen.AuditEventPage
	if err := json.Unmarshal(firstResponse.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Events == nil ||
		len(first.Events) != 0 ||
		first.NextCursor == nil ||
		first.ResumeCursor == nil ||
		*first.ResumeCursor != *first.NextCursor {
		t.Fatalf("first page = %#v", first)
	}
	backend.mu.Lock()
	if backend.auditAfter != 0 || backend.auditLimit != DefaultAuditPageSize {
		t.Fatalf(
			"default page request = after %d limit %d",
			backend.auditAfter,
			backend.auditLimit,
		)
	}
	backend.auditPage = AuditPage{}
	backend.mu.Unlock()

	secondRequest := httptest.NewRequest(
		http.MethodGet,
		apiTestOrigin+"/api/v1/audit-events?cursor="+*first.NextCursor+"&limit=500",
		nil,
	)
	secondRequest.AddCookie(cookie)
	secondResponse := httptest.NewRecorder()
	handler.ServeHTTP(secondResponse, secondRequest)
	if secondResponse.Code != http.StatusOK {
		t.Fatalf("second page = %d %s", secondResponse.Code, secondResponse.Body.String())
	}
	var second gen.AuditEventPage
	if err := json.Unmarshal(secondResponse.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if second.Events == nil || len(second.Events) != 0 || second.NextCursor != nil {
		t.Fatalf("second page = %#v", second)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.auditAfter != nextAfter || backend.auditLimit != MaximumAuditPageSize {
		t.Fatalf(
			"next page request = after %d limit %d",
			backend.auditAfter,
			backend.auditLimit,
		)
	}
}

func TestAuthenticationFailureAuditIsCoalescedPerReasonAndWindow(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	now := time.Unix(1_700_000_000, 0).UTC()
	handler.(*server).rejectionAudits.now = func() time.Time { return now }
	for attempt := 0; attempt < 25; attempt++ {
		request := httptest.NewRequest(
			http.MethodPost,
			apiTestOrigin+"/api/v1/session",
			nil,
		)
		request.Header.Set("Origin", apiTestOrigin)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		assertProblem(t, response, http.StatusUnauthorized, "authentication_failed")
	}
	if got := len(backend.audits); got != 1 {
		t.Fatalf("coalesced authentication audits = %d, want 1", got)
	}

	now = now.Add(rejectionAuditWindow)
	request := httptest.NewRequest(
		http.MethodPost,
		apiTestOrigin+"/api/v1/session",
		nil,
	)
	request.Header.Set("Origin", apiTestOrigin)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertProblem(t, response, http.StatusUnauthorized, "authentication_failed")
	if got := len(backend.audits); got != 2 {
		t.Fatalf("next-window authentication audits = %d, want 2", got)
	}
}

func TestAuditEventsAcceptsMinimumPageLimit(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	cookie, _ := bootstrapAPISession(t, handler)
	request := httptest.NewRequest(
		http.MethodGet,
		apiTestOrigin+"/api/v1/audit-events?limit=1",
		nil,
	)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("minimum page = %d %s", response.Code, response.Body.String())
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.auditAfter != 0 || backend.auditLimit != 1 {
		t.Fatalf(
			"minimum page request = after %d limit %d",
			backend.auditAfter,
			backend.auditLimit,
		)
	}
}

func TestAuditCursorIsCanonicalAcrossSequenceBoundaries(t *testing.T) {
	t.Parallel()

	for _, sequence := range []uint64{1, math.MaxInt64} {
		cursor, err := encodeAuditCursor(sequence)
		if err != nil {
			t.Fatalf("encode %d: %v", sequence, err)
		}
		decoded, err := decodeAuditCursor(cursor)
		if err != nil || decoded != sequence {
			t.Fatalf("cursor %q decoded as %d, %v", cursor, decoded, err)
		}
	}
	for _, sequence := range []uint64{0, uint64(math.MaxInt64) + 1} {
		if _, err := encodeAuditCursor(sequence); !errors.Is(err, errInvalidAuditQuery) {
			t.Fatalf("encode invalid sequence %d error = %v", sequence, err)
		}
	}
}

func TestMutationAuthorizationOrderingAndNoCORS(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	cookie, csrf := bootstrapAPISession(t, handler)
	body := validAPIConfigurationBody()

	tests := []struct {
		name       string
		cookie     *http.Cookie
		origin     string
		csrf       string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "missing session wins before cross origin",
			origin:     "http://attacker.invalid",
			csrf:       "invalid",
			wantStatus: http.StatusUnauthorized,
			wantCode:   "authentication_failed",
		},
		{
			name:       "authenticated cross origin",
			cookie:     cookie,
			origin:     "http://attacker.invalid",
			csrf:       csrf,
			wantStatus: http.StatusForbidden,
			wantCode:   "request_forbidden",
		},
		{
			name:       "authenticated missing csrf",
			cookie:     cookie,
			origin:     apiTestOrigin,
			wantStatus: http.StatusForbidden,
			wantCode:   "request_forbidden",
		},
		{
			name:       "authenticated valid mutation",
			cookie:     cookie,
			origin:     apiTestOrigin,
			csrf:       csrf,
			wantStatus: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodPut,
				apiTestOrigin+"/api/v1/configuration",
				bytes.NewReader(body),
			)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("If-Match", `"cfg-0"`)
			request.Header.Set("Origin", test.origin)
			if test.csrf != "" {
				request.Header.Set(auth.CSRFHeaderName, test.csrf)
			}
			if test.cookie != nil {
				request.AddCookie(test.cookie)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if test.wantCode == "" {
				if response.Code != test.wantStatus {
					t.Fatalf("response = %d %s", response.Code, response.Body.String())
				}
			} else {
				assertProblem(t, response, test.wantStatus, test.wantCode)
			}
			for key := range response.Header() {
				if strings.HasPrefix(strings.ToLower(key), "access-control-") {
					t.Fatalf("management response emitted CORS header %q", key)
				}
			}
		})
	}
	if backend.applyCalls != 1 {
		t.Fatalf("backend apply calls = %d, want one authorized mutation", backend.applyCalls)
	}
}

func TestConfigurationRevisionConflictAndBackendErrorAreSafe(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	cookie, csrf := bootstrapAPISession(t, handler)
	backend.applyErr = &RevisionConflict{Expected: 0, Current: 7}

	response := applyAPIConfiguration(t, handler, cookie, csrf, validAPIConfigurationBody())
	assertProblem(t, response, http.StatusConflict, "configuration_revision_conflict")
	var conflict gen.Problem
	if err := json.Unmarshal(response.Body.Bytes(), &conflict); err != nil {
		t.Fatal(err)
	}
	if conflict.CurrentRevision == nil || *conflict.CurrentRevision != "7" {
		t.Fatalf("current revision = %#v, want 7", conflict.CurrentRevision)
	}

	backend.applyErr = errors.New("Authorization: secret-canary")
	response = applyAPIConfiguration(t, handler, cookie, csrf, validAPIConfigurationBody())
	assertProblem(t, response, http.StatusServiceUnavailable, "management_unavailable")
	if strings.Contains(response.Body.String(), "secret-canary") {
		t.Fatalf("backend error leaked through problem response: %s", response.Body.String())
	}

	beforeGeneration := handler.(*server).events.currentGeneration()
	backend.applyErr = &CommittedMutationError{Current: 8}
	response = applyAPIConfiguration(t, handler, cookie, csrf, validAPIConfigurationBody())
	assertProblem(t, response, http.StatusServiceUnavailable, "mutation_committed_reload_required")
	var committed gen.Problem
	if err := json.Unmarshal(response.Body.Bytes(), &committed); err != nil {
		t.Fatal(err)
	}
	if committed.CurrentRevision == nil || *committed.CurrentRevision != "8" {
		t.Fatalf("committed current revision = %#v, want 8", committed.CurrentRevision)
	}
	if handler.(*server).events.currentGeneration() != beforeGeneration+1 {
		t.Fatal("committed mutation failure did not invalidate live clients")
	}
}

func TestAuditDegradationBlocksMutationBeforeBackendApply(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	cookie, csrf := bootstrapAPISession(t, handler)
	backend.auditHealthy = false
	before := backend.applyCalls

	response := applyAPIConfiguration(t, handler, cookie, csrf, validAPIConfigurationBody())
	assertProblem(t, response, http.StatusServiceUnavailable, "management_unavailable")
	if backend.applyCalls != before {
		t.Fatal("degraded audit authority reached the configuration mutation")
	}
}

func TestRequestBodyBudgetRejectsContentLengthAndChunkedOverflow(t *testing.T) {
	t.Parallel()

	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunked=%t", chunked), func(t *testing.T) {
			handler, backend := newAPITestHandler(t)
			cookie, csrf := bootstrapAPISession(t, handler)
			payload := strings.Repeat("x", int(MaximumRequestBodyBytes)+1)
			var body io.Reader = strings.NewReader(payload)
			request := httptest.NewRequest(
				http.MethodPut,
				apiTestOrigin+"/api/v1/configuration",
				body,
			)
			if chunked {
				request.ContentLength = -1
			}
			request.Header.Set("Content-Type", "application/yaml")
			request.Header.Set("If-Match", `"cfg-0"`)
			request.Header.Set("Origin", apiTestOrigin)
			request.Header.Set(auth.CSRFHeaderName, csrf)
			request.AddCookie(cookie)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			assertProblem(t, response, http.StatusRequestEntityTooLarge, "payload_too_large")
			if backend.applyCalls != 0 {
				t.Fatal("over-limit request reached the backend")
			}
		})
	}
}

func TestJoinCodeIsDeliveredOnceAndUnknownFieldsFailClosed(t *testing.T) {
	t.Parallel()

	handler, _ := newAPITestHandler(t)
	cookie, csrf := bootstrapAPISession(t, handler)
	wrongMedia := httptest.NewRequest(
		http.MethodPost,
		apiTestOrigin+"/api/v1/join-codes",
		strings.NewReader(`{"endpointHints":[]}`),
	)
	wrongMedia.Header.Set("Content-Type", "text/plain")
	wrongMedia.Header.Set("Origin", apiTestOrigin)
	wrongMedia.Header.Set(auth.CSRFHeaderName, csrf)
	wrongMedia.AddCookie(cookie)
	wrongMediaResponse := httptest.NewRecorder()
	handler.ServeHTTP(wrongMediaResponse, wrongMedia)
	assertProblem(
		t,
		wrongMediaResponse,
		http.StatusUnsupportedMediaType,
		"unsupported_media_type",
	)

	request := httptest.NewRequest(
		http.MethodPost,
		apiTestOrigin+"/api/v1/join-codes",
		strings.NewReader(`{"endpointHints":[],"secret":"must-not-be-accepted"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", apiTestOrigin)
	request.Header.Set(auth.CSRFHeaderName, csrf)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertProblem(t, response, http.StatusBadRequest, "invalid_body")

	request = httptest.NewRequest(
		http.MethodPost,
		apiTestOrigin+"/api/v1/join-codes",
		strings.NewReader(`{"endpointHints":["controller.example.test:7443"]}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", apiTestOrigin)
	request.Header.Set(auth.CSRFHeaderName, csrf)
	request.AddCookie(cookie)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("join-code response = %d %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("join-code Cache-Control = %q", response.Header().Get("Cache-Control"))
	}
	if !strings.Contains(response.Body.String(), "spr_one-time-canary") {
		t.Fatalf("one-time response omitted code: %s", response.Body.String())
	}

	read := httptest.NewRequest(http.MethodGet, apiTestOrigin+"/api/v1/join-codes", nil)
	read.AddCookie(cookie)
	readResponse := httptest.NewRecorder()
	handler.ServeHTTP(readResponse, read)
	assertProblem(t, readResponse, http.StatusNotFound, "not_found")
	if strings.Contains(readResponse.Body.String(), "one-time-canary") {
		t.Fatal("later join-code read replayed the credential")
	}
}

func TestSSERequiresSessionCSRFAndResetsUnknownCursor(t *testing.T) {
	t.Parallel()

	handler, _ := newAPITestHandler(t)
	cookie, csrf := bootstrapAPISession(t, handler)

	missingCSRF := httptest.NewRequest(http.MethodGet, apiTestOrigin+"/api/v1/events", nil)
	missingCSRF.AddCookie(cookie)
	missingResponse := httptest.NewRecorder()
	handler.ServeHTTP(missingResponse, missingCSRF)
	assertProblem(t, missingResponse, http.StatusForbidden, "request_forbidden")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest(
		http.MethodGet,
		apiTestOrigin+"/api/v1/events?cursor=unknown",
		nil,
	).WithContext(ctx)
	request.Header.Set(auth.CSRFHeaderName, csrf)
	request.AddCookie(cookie)
	response := newSSETestWriter()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(response, request)
	}()
	response.waitForFlush(t)
	if body := response.body(); !strings.Contains(body, "event: reset") ||
		!strings.Contains(body, `"resources":["setup","overview","nodes"`) {
		t.Fatalf("unknown cursor did not produce reset event: %s", body)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE handler did not stop after context cancellation")
	}
}

func TestSSEWithoutCursorResetsAndDoesNotLoseSubscribeRevisionRace(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	cookie, csrf := bootstrapAPISession(t, handler)
	apiServer := handler.(*server)

	resetContext, cancelReset := context.WithCancel(context.Background())
	resetRequest := httptest.NewRequest(
		http.MethodGet,
		apiTestOrigin+"/api/v1/events",
		nil,
	).WithContext(resetContext)
	resetRequest.Header.Set(auth.CSRFHeaderName, csrf)
	resetRequest.AddCookie(cookie)
	resetResponse := newSSETestWriter()
	resetDone := make(chan struct{})
	go func() {
		defer close(resetDone)
		handler.ServeHTTP(resetResponse, resetRequest)
	}()
	resetResponse.waitForFlush(t)
	if body := resetResponse.body(); !strings.Contains(body, "event: reset") ||
		!strings.Contains(body, `"resources":["setup","overview","nodes"`) {
		t.Fatalf("cursorless stream did not level-trigger a reset: %s", body)
	}
	cancelReset()
	<-resetDone

	current := apiServer.cursor(
		backend.configuration.Revision,
		apiServer.events.currentGeneration(),
	)
	backend.mu.Lock()
	backend.currentRevisionHook = apiServer.events.Publish
	backend.mu.Unlock()
	raceContext, cancelRace := context.WithCancel(context.Background())
	defer cancelRace()
	raceRequest := httptest.NewRequest(
		http.MethodGet,
		apiTestOrigin+"/api/v1/events?cursor="+current,
		nil,
	).WithContext(raceContext)
	raceRequest.Header.Set(auth.CSRFHeaderName, csrf)
	raceRequest.AddCookie(cookie)
	raceResponse := newSSETestWriter()
	raceDone := make(chan struct{})
	go func() {
		defer close(raceDone)
		handler.ServeHTTP(raceResponse, raceRequest)
	}()
	raceResponse.waitForFlush(t)
	raceResponse.waitForFlush(t)
	if body := raceResponse.body(); !strings.Contains(body, "event: ready") ||
		!strings.Contains(body, "event: invalidate") {
		t.Fatalf("subscribe/revision race lost invalidation: %s", body)
	}
	cancelRace()
	select {
	case <-raceDone:
	case <-time.After(2 * time.Second):
		t.Fatal("racing SSE handler did not stop after cancellation")
	}
}

func TestSSEClosesWhenSessionExpires(t *testing.T) {
	t.Parallel()

	manager, err := auth.NewManagerWithSessionTTL(
		apiTestRoot(),
		apiTestOrigin,
		false,
		500*time.Millisecond,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := newAPITestHandlerWithAuth(t, manager)
	cookie, csrf := bootstrapAPISession(t, handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest(
		http.MethodGet,
		apiTestOrigin+"/api/v1/events",
		nil,
	).WithContext(ctx)
	request.Header.Set(auth.CSRFHeaderName, csrf)
	request.AddCookie(cookie)
	response := newSSETestWriter()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(response, request)
	}()
	response.waitForFlush(t)
	waitForSSEClose(t, done)

	reconnect := httptest.NewRequest(http.MethodGet, apiTestOrigin+"/api/v1/events", nil)
	reconnect.Header.Set(auth.CSRFHeaderName, csrf)
	reconnect.AddCookie(cookie)
	reconnectResponse := httptest.NewRecorder()
	handler.ServeHTTP(reconnectResponse, reconnect)
	assertProblem(t, reconnectResponse, http.StatusUnauthorized, "authentication_failed")
}

func TestSSEClosesWhenSessionIsRevoked(t *testing.T) {
	t.Parallel()

	handler, _ := newAPITestHandler(t)
	cookie, csrf := bootstrapAPISession(t, handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest(
		http.MethodGet,
		apiTestOrigin+"/api/v1/events",
		nil,
	).WithContext(ctx)
	request.Header.Set(auth.CSRFHeaderName, csrf)
	request.AddCookie(cookie)
	response := newSSETestWriter()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(response, request)
	}()
	response.waitForFlush(t)

	logout := httptest.NewRequest(http.MethodDelete, apiTestOrigin+"/api/v1/session", nil)
	logout.Header.Set("Origin", apiTestOrigin)
	logout.Header.Set(auth.CSRFHeaderName, csrf)
	logout.AddCookie(cookie)
	logoutResponse := httptest.NewRecorder()
	handler.ServeHTTP(logoutResponse, logout)
	if logoutResponse.Code != http.StatusNoContent {
		t.Fatalf("logout = %d %s", logoutResponse.Code, logoutResponse.Body.String())
	}
	waitForSSEClose(t, done)

	reconnect := httptest.NewRequest(http.MethodGet, apiTestOrigin+"/api/v1/events", nil)
	reconnect.Header.Set(auth.CSRFHeaderName, csrf)
	reconnect.AddCookie(cookie)
	reconnectResponse := httptest.NewRecorder()
	handler.ServeHTTP(reconnectResponse, reconnect)
	assertProblem(t, reconnectResponse, http.StatusUnauthorized, "authentication_failed")
}

func TestIfMatchIsRequiredAndCanonical(t *testing.T) {
	t.Parallel()

	handler, _ := newAPITestHandler(t)
	cookie, csrf := bootstrapAPISession(t, handler)
	for _, value := range []string{"", `W/"cfg-0"`, `"cfg-00"`, `"cfg--1"`, `"cfg-0", "cfg-1"`} {
		request := httptest.NewRequest(
			http.MethodPut,
			apiTestOrigin+"/api/v1/configuration",
			bytes.NewReader(validAPIConfigurationBody()),
		)
		request.Header.Set("Content-Type", "application/json")
		if value != "" {
			request.Header.Set("If-Match", value)
		}
		request.Header.Set("Origin", apiTestOrigin)
		request.Header.Set(auth.CSRFHeaderName, csrf)
		request.AddCookie(cookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if value == "" {
			assertProblem(t, response, http.StatusPreconditionRequired, "precondition_required")
		} else {
			assertProblem(t, response, http.StatusBadRequest, "invalid_precondition")
		}
	}
}

func TestGeneratedOpenAPIOperationsAreAllRuntimeRouted(t *testing.T) {
	t.Parallel()

	document, err := gen.GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	if err := document.Validate(context.Background()); err != nil {
		t.Fatalf("generated OpenAPI document is invalid: %v", err)
	}
	handler, _ := newAPITestHandler(t)
	var checked int
	for _, path := range document.Paths.Keys() {
		item := document.Paths.Value(path)
		for method := range item.Operations() {
			runtimePath := strings.NewReplacer(
				"{tokenId}", strings.Repeat("a", 32),
				"{nodeId}", "node-a",
			).Replace(path)
			request := httptest.NewRequest(
				strings.ToUpper(method),
				apiTestOrigin+Prefix+runtimePath,
				nil,
			)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code == http.StatusNotFound {
				t.Fatalf(
					"generated operation %s %s is absent from runtime routing: %s",
					method,
					path,
					response.Body.String(),
				)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("generated OpenAPI document contains no operations")
	}
}

func TestProblemInstanceNeverReflectsCredentialBearingPath(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	const canary = "spr_secret-canary"

	unauthorized := httptest.NewRequest(
		http.MethodDelete,
		apiTestOrigin+"/api/v1/join-codes/"+canary,
		nil,
	)
	unauthorizedResponse := httptest.NewRecorder()
	handler.ServeHTTP(unauthorizedResponse, unauthorized)
	assertSecretFreeProblem(
		t,
		unauthorizedResponse,
		http.StatusUnauthorized,
		"authentication_failed",
		canary,
	)

	cookie, csrf := bootstrapAPISession(t, handler)
	notFound := httptest.NewRequest(
		http.MethodDelete,
		apiTestOrigin+"/api/v1/join-codes/"+canary,
		nil,
	)
	notFound.Header.Set("Origin", apiTestOrigin)
	notFound.Header.Set(auth.CSRFHeaderName, csrf)
	notFound.AddCookie(cookie)
	notFoundResponse := httptest.NewRecorder()
	handler.ServeHTTP(notFoundResponse, notFound)
	assertSecretFreeProblem(
		t,
		notFoundResponse,
		http.StatusNotFound,
		"join_code_not_found",
		canary,
	)

	for _, test := range []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{
			name: "validation",
			err: &ValidationError{Violations: []FieldViolation{{
				Field: "nodeId", Code: "invalid_node", Message: "The node is invalid.",
			}}},
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "validation_failed",
		},
		{
			name:       "unavailable",
			err:        errors.New("provider detail must remain private"),
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "management_unavailable",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend.mu.Lock()
			backend.nodeErr = test.err
			backend.mu.Unlock()
			request := httptest.NewRequest(
				http.MethodPost,
				apiTestOrigin+"/api/v1/nodes/"+canary+"/drain",
				nil,
			)
			request.Header.Set("If-Match", `"cfg-0"`)
			request.Header.Set("Origin", apiTestOrigin)
			request.Header.Set(auth.CSRFHeaderName, csrf)
			request.AddCookie(cookie)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			assertSecretFreeProblem(
				t,
				response,
				test.wantStatus,
				test.wantCode,
				canary,
			)
		})
	}
}

func TestNodeMutationRejectsUnexpectedBodyBeforeBackend(t *testing.T) {
	t.Parallel()

	handler, backend := newAPITestHandler(t)
	cookie, csrf := bootstrapAPISession(t, handler)
	request := httptest.NewRequest(
		http.MethodPost,
		apiTestOrigin+"/api/v1/nodes/node-a/drain",
		strings.NewReader(`{}`),
	)
	request.Header.Set("If-Match", `"cfg-0"`)
	request.Header.Set("Origin", apiTestOrigin)
	request.Header.Set(auth.CSRFHeaderName, csrf)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertProblem(t, response, http.StatusBadRequest, "invalid_body")
	backend.mu.Lock()
	nodeCalls := backend.nodeCalls
	backend.mu.Unlock()
	if nodeCalls != 0 {
		t.Fatalf("unexpected node body reached backend %d times", nodeCalls)
	}
}

type apiTestBackend struct {
	mu                  sync.Mutex
	auditHealthy        bool
	audits              []AuditInput
	auditErr            error
	auditContextErr     error
	auditPage           AuditPage
	auditAfter          uint64
	auditLimit          int
	auditCalls          int
	applyCalls          int
	applyErr            error
	nodeCalls           int
	nodeErr             error
	configuration       gen.Configuration
	currentRevisionHook func()
}

func (backend *apiTestBackend) RecordAudit(ctx context.Context, event AuditInput) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.audits = append(backend.audits, event)
	backend.auditContextErr = ctx.Err()
	return backend.auditErr
}

func (backend *apiTestBackend) AuditHealthy() bool {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.auditHealthy
}

func (backend *apiTestBackend) Setup(context.Context) (gen.Setup, error) {
	return gen.Setup{
		ControllerInitialized: true,
		GithubAppState:        gen.SetupGithubAppStateDisconnected,
		ManifestFlowSupported: false,
		NodeCount:             0,
		TargetCount:           0,
		Conditions:            []gen.Condition{},
	}, nil
}

func (backend *apiTestBackend) Overview(context.Context) (gen.Overview, error) {
	return gen.Overview{
		Version: "test", ControllerEpoch: "1", Conditions: []gen.Condition{},
	}, nil
}

func (backend *apiTestBackend) Nodes(context.Context) ([]gen.Node, gen.Revision, error) {
	return []gen.Node{}, backend.configuration.Revision, nil
}

func (backend *apiTestBackend) Targets(context.Context) ([]gen.Target, gen.Revision, error) {
	return []gen.Target{}, backend.configuration.Revision, nil
}

func (backend *apiTestBackend) Runs(context.Context) ([]gen.Run, error) {
	return []gen.Run{}, nil
}

func (backend *apiTestBackend) AuditEvents(
	_ context.Context,
	after uint64,
	limit int,
) (AuditPage, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.auditAfter = after
	backend.auditLimit = limit
	backend.auditCalls++
	return backend.auditPage, nil
}

func (backend *apiTestBackend) ReadConfiguration(context.Context) (gen.Configuration, error) {
	return backend.configuration, nil
}

func (backend *apiTestBackend) ApplyConfiguration(
	_ context.Context,
	_ uint64,
	_ string,
	_ []byte,
	_ string,
) (gen.Configuration, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.applyCalls++
	if backend.applyErr != nil {
		return gen.Configuration{}, backend.applyErr
	}
	backend.configuration.Revision = "1"
	return backend.configuration, nil
}

func (backend *apiTestBackend) ExportConfiguration(context.Context) ([]byte, gen.Revision, error) {
	return []byte("schemaVersion: 1\nrevision: \"0\"\n"), backend.configuration.Revision, nil
}

func (backend *apiTestBackend) CreateJoinCode(
	context.Context,
	[]string,
	string,
) (string, string, error) {
	return strings.Repeat("a", 32), "spr_one-time-canary", nil
}

func (backend *apiTestBackend) CancelJoinCode(context.Context, string, string) error {
	return nil
}

func (backend *apiTestBackend) SetNodeAdministrativeState(
	context.Context,
	domain.NodeID,
	domain.NodeAdministrativeState,
	uint64,
	string,
) (gen.Node, gen.Revision, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.nodeCalls++
	if backend.nodeErr != nil {
		return gen.Node{}, "", backend.nodeErr
	}
	return gen.Node{}, backend.configuration.Revision, nil
}

func (backend *apiTestBackend) CurrentRevision(context.Context) (gen.Revision, error) {
	backend.mu.Lock()
	revision := backend.configuration.Revision
	hook := backend.currentRevisionHook
	backend.currentRevisionHook = nil
	backend.mu.Unlock()
	if hook != nil {
		hook()
	}
	return revision, nil
}

func newAPITestHandler(t *testing.T) (http.Handler, *apiTestBackend) {
	t.Helper()
	manager, err := auth.NewManager(apiTestRoot(), apiTestOrigin, false)
	if err != nil {
		t.Fatal(err)
	}
	return newAPITestHandlerWithAuth(t, manager)
}

func newAPITestHandlerWithAuth(
	t *testing.T,
	manager *auth.Manager,
) (http.Handler, *apiTestBackend) {
	t.Helper()
	configuration := gen.Configuration{
		SchemaVersion:  gen.ConfigurationSchemaVersionN1,
		Revision:       "0",
		Scheduler:      gen.SchedulerConfiguration{},
		Nodes:          []gen.NodeConfiguration{},
		RunnerProfiles: []gen.RunnerProfile{},
		Targets:        []gen.TargetConfiguration{},
	}
	backend := &apiTestBackend{auditHealthy: true, configuration: configuration}
	handler, err := NewHandler(Options{
		Auth:    manager,
		Backend: backend,
		Events:  NewEventBus(),
		UI: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(writer, "embedded UI")
		}),
		Epoch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler, backend
}

func bootstrapAPISession(t *testing.T, handler http.Handler) (*http.Cookie, string) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, apiTestOrigin+"/api/v1/session", nil)
	request.Header.Set("Origin", apiTestOrigin)
	setAPIBootstrapProof(t, request)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("bootstrap response = %d %s", response.Code, response.Body.String())
	}
	cookies := response.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("bootstrap cookies = %#v", cookies)
	}
	var session gen.Session
	if err := json.Unmarshal(response.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	return cookies[0], session.CsrfToken
}

func createAPIBrowserHandoff(
	t *testing.T,
	handler http.Handler,
	secret [32]byte,
) gen.BrowserHandoff {
	t.Helper()
	digest := sha256.Sum256(secret[:])
	body, err := json.Marshal(gen.CreateBrowserHandoffRequest{
		ClaimDigest: base64.RawURLEncoding.EncodeToString(digest[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		apiTestOrigin+"/api/v1/browser-handoffs",
		bytes.NewReader(body),
	)
	request.Header.Set("Origin", apiTestOrigin)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create browser handoff = %d %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("handoff cache control = %q", response.Header().Get("Cache-Control"))
	}
	if cookies := response.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("handoff issuance created cookies: %#v", cookies)
	}
	var handoff gen.BrowserHandoff
	if err := json.Unmarshal(response.Body.Bytes(), &handoff); err != nil {
		t.Fatal(err)
	}
	if !auth.ValidBrowserHandoffCodeEncoding(handoff.Code) ||
		handoff.State != gen.BrowserHandoffStatePending ||
		handoff.ExpiresAt.IsZero() {
		t.Fatalf("browser handoff = %#v", handoff)
	}
	return handoff
}

func authorizeAPIBrowserHandoff(
	t *testing.T,
	handler http.Handler,
	cookie *http.Cookie,
	csrf string,
	code string,
) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(gen.AuthorizeBrowserHandoffRequest{Code: code})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		apiTestOrigin+"/api/v1/browser-handoff-authorizations",
		bytes.NewReader(body),
	)
	request.Header.Set("Origin", apiTestOrigin)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(auth.CSRFHeaderName, csrf)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func claimAPIBrowserHandoff(
	t *testing.T,
	handler http.Handler,
	code string,
	secret [32]byte,
) *httptest.ResponseRecorder {
	t.Helper()
	encodedSecret := base64.RawURLEncoding.EncodeToString(secret[:])
	body, err := json.Marshal(gen.ClaimBrowserHandoffRequest{
		Code:        code,
		ClaimSecret: &encodedSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		apiTestOrigin+"/api/v1/browser-handoffs/claim",
		bytes.NewReader(body),
	)
	request.Header.Set("Origin", apiTestOrigin)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func setAPIBootstrapProof(t *testing.T, request *http.Request) {
	t.Helper()

	proof, err := auth.NewBootstrapProof(apiTestRoot(), apiTestOrigin, time.Now(), rand.Reader)
	if err != nil {
		t.Fatalf("create API bootstrap proof: %v", err)
	}
	request.Header.Set(auth.BootstrapHeaderName, proof)
}

func apiTestRoot() [32]byte {
	var root [32]byte
	for index := range root {
		root[index] = byte(index + 1)
	}
	return root
}

func validAPIConfigurationBody() []byte {
	return []byte(`{"schemaVersion":1,"revision":"0","scheduler":{},"nodes":[],"runnerProfiles":[],"targets":[]}`)
}

func applyAPIConfiguration(
	t *testing.T,
	handler http.Handler,
	cookie *http.Cookie,
	csrf string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodPut,
		apiTestOrigin+"/api/v1/configuration",
		bytes.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("If-Match", `"cfg-0"`)
	request.Header.Set("Origin", apiTestOrigin)
	request.Header.Set(auth.CSRFHeaderName, csrf)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertProblem(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("response status = %d, want %d: %s", response.Code, status, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("problem content type = %q", response.Header().Get("Content-Type"))
	}
	var problem gen.Problem
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem: %v: %s", err, response.Body.String())
	}
	if problem.Code != code || problem.Status != status {
		t.Fatalf("problem = %#v, want code=%q status=%d", problem, code, status)
	}
}

func assertSecretFreeProblem(
	t *testing.T,
	response *httptest.ResponseRecorder,
	status int,
	code string,
	canary string,
) {
	t.Helper()
	assertProblem(t, response, status, code)
	if strings.Contains(response.Body.String(), canary) {
		t.Fatalf("problem response reflected credential-bearing path: %s", response.Body.String())
	}
	var problem gen.Problem
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatal(err)
	}
	requestID := response.Header().Get("X-Request-ID")
	if problem.Instance != "urn:sparerunner:request:"+requestID {
		t.Fatalf("problem instance = %q, request ID = %q", problem.Instance, requestID)
	}
}

type sseTestWriter struct {
	mu      sync.Mutex
	header  http.Header
	buffer  bytes.Buffer
	status  int
	flushed chan struct{}
}

func newSSETestWriter() *sseTestWriter {
	return &sseTestWriter{header: make(http.Header), flushed: make(chan struct{}, 4)}
}

func (writer *sseTestWriter) Header() http.Header {
	return writer.header
}

func (writer *sseTestWriter) WriteHeader(status int) {
	writer.mu.Lock()
	writer.status = status
	writer.mu.Unlock()
}

func (writer *sseTestWriter) Write(payload []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.status == 0 {
		writer.status = http.StatusOK
	}
	return writer.buffer.Write(payload)
}

func (writer *sseTestWriter) Flush() {
	select {
	case writer.flushed <- struct{}{}:
	default:
	}
}

func (writer *sseTestWriter) body() string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.buffer.String()
}

func (writer *sseTestWriter) waitForFlush(t *testing.T) {
	t.Helper()
	select {
	case <-writer.flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SSE flush")
	}
}

func waitForSSEClose(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for authenticated SSE stream to close")
	}
}
