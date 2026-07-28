// Package api owns the versioned loopback management HTTP boundary. It exposes
// only generated transport DTOs and delegates durable authority to Backend.
package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/genm/sparerunner/internal/api/gen"
	"github.com/genm/sparerunner/internal/auth"
	"github.com/genm/sparerunner/internal/domain"
)

const (
	Prefix                  = "/api/v1"
	MaximumRequestBodyBytes = int64(16 << 20)
	DefaultAuditPageSize    = 100
	MaximumAuditPageSize    = 500
	auditPersistenceTimeout = 5 * time.Second
	auditCursorPrefix       = "aud1_"
	auditCursorPayloadSize  = 8
	rejectionAuditWindow    = time.Minute
)

var (
	ErrBackendUnavailable     = errors.New("management backend is unavailable")
	ErrResourceNotFound       = errors.New("management resource was not found")
	ErrDomainConflict         = errors.New("management operation conflicts with current state")
	ErrGitHubCallbackConflict = errors.New("GitHub App callback state conflicts with current state")
)

// RevisionConflict is the safe optimistic-lock failure returned by a Backend.
type RevisionConflict struct {
	Expected uint64
	Current  uint64
}

func (conflict *RevisionConflict) Error() string {
	return "management configuration revision is stale"
}

// CommittedMutationError means SQLite and audit authority advanced, but the
// live projection or response could not be confirmed. The revision lets clients
// reload instead of blindly retrying an operation that may already have effect.
type CommittedMutationError struct {
	Current uint64
}

func (committed *CommittedMutationError) Error() string {
	return "management mutation committed but could not be confirmed"
}

// FieldViolation is an allowlisted transport validation error.
type FieldViolation struct {
	Field   string
	Code    string
	Message string
}

// ValidationError contains no raw request or provider error.
type ValidationError struct {
	Violations []FieldViolation
}

func (validation *ValidationError) Error() string {
	return "management request failed validation"
}

// AuditInput deliberately has no raw detail, body, header, or credential field.
type AuditInput struct {
	Actor        string
	Action       string
	Outcome      string
	ResourceType string
	ResourceID   string
	ErrorCode    string
	RequestID    string
}

type AuditPage struct {
	Events      []gen.AuditEvent
	NextAfter   *uint64
	ResumeAfter *uint64
}

// Backend is the sole authority used by the HTTP adapter. Implementations keep
// database transactions, GitHub verification, and reconciliation out of handlers.
type Backend interface {
	RecordAudit(context.Context, AuditInput) error
	AuditHealthy() bool
	Setup(context.Context) (gen.Setup, error)
	Overview(context.Context) (gen.Overview, error)
	Nodes(context.Context) ([]gen.Node, gen.Revision, error)
	Targets(context.Context) ([]gen.Target, gen.Revision, error)
	Runs(context.Context) ([]gen.Run, error)
	AuditEvents(context.Context, uint64, int) (AuditPage, error)
	ReadConfiguration(context.Context) (gen.Configuration, error)
	ApplyConfiguration(context.Context, uint64, string, []byte, string) (gen.Configuration, error)
	ExportConfiguration(context.Context) ([]byte, gen.Revision, error)
	CreateJoinCode(context.Context, []string, string) (tokenID, code string, err error)
	CancelJoinCode(context.Context, string, string) error
	SetNodeAdministrativeState(context.Context, domain.NodeID, domain.NodeAdministrativeState, uint64, string) (gen.Node, gen.Revision, error)
	CurrentRevision(context.Context) (gen.Revision, error)
}

// GitHubBackend is optional so existing embedders and focused API tests do not
// need to implement provider operations. A Controller enables it only after
// the credential-store and signed Manifest boundaries are initialized.
type GitHubBackend interface {
	StartGitHubAppManifest(context.Context, string, string) (gen.GitHubManifestStart, error)
	CompleteGitHubAppManifest(context.Context, string, string) error
	ListGitHubInstallations(context.Context) (gen.GitHubInstallationList, error)
	CreateGitHubTarget(context.Context, uint64, gen.CreateGitHubTargetRequest, string) (gen.GitHubTargetMutation, error)
}

// EventBus coalesces invalidations for each slow subscriber. It retains no event
// history; an unknown cursor is handled by a level-triggered reset.
type EventBus struct {
	mu         sync.Mutex
	generation uint64
	subs       map[chan struct{}]struct{}
}

func NewEventBus() *EventBus {
	return &EventBus{subs: make(map[chan struct{}]struct{})}
}

func (bus *EventBus) Publish() {
	if bus == nil {
		return
	}
	bus.mu.Lock()
	bus.generation++
	for subscriber := range bus.subs {
		select {
		case subscriber <- struct{}{}:
		default:
		}
	}
	bus.mu.Unlock()
}

func (bus *EventBus) subscribe() (<-chan struct{}, uint64, func()) {
	if bus == nil {
		closed := make(chan struct{})
		close(closed)
		return closed, 0, func() {}
	}
	channel := make(chan struct{}, 1)
	bus.mu.Lock()
	bus.subs[channel] = struct{}{}
	generation := bus.generation
	bus.mu.Unlock()
	return channel, generation, func() {
		bus.mu.Lock()
		delete(bus.subs, channel)
		bus.mu.Unlock()
	}
}

func (bus *EventBus) currentGeneration() uint64 {
	if bus == nil {
		return 0
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return bus.generation
}

// Generation returns the current process-local invalidation high-water mark.
// It is safe for health fan-in verification and carries no event payload.
func (bus *EventBus) Generation() uint64 {
	return bus.currentGeneration()
}

type Options struct {
	Auth    *auth.Manager
	Backend Backend
	Events  *EventBus
	UI      http.Handler
	Epoch   uint64
}

type server struct {
	auth            *auth.Manager
	backend         Backend
	events          *EventBus
	ui              http.Handler
	epoch           uint64
	rejectionAudits rejectionAuditGuard
}

type rejectionAuditGuard struct {
	mu              sync.Mutex
	now             func() time.Time
	windowStartedAt time.Time
	codes           map[string]struct{}
}

func NewHandler(options Options) (http.Handler, error) {
	if options.Auth == nil || options.Backend == nil || options.Events == nil ||
		options.UI == nil || options.Epoch == 0 {
		return nil, errors.New("management API dependencies are incomplete")
	}
	return &server{
		auth:    options.Auth,
		backend: options.Backend,
		events:  options.Events,
		ui:      options.UI,
		epoch:   options.Epoch,
		rejectionAudits: rejectionAuditGuard{
			now:   time.Now,
			codes: make(map[string]struct{}),
		},
	}, nil
}

func (server *server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	requestID, err := newRequestID()
	if err != nil {
		requestID = "req_unavailable"
	}
	writer.Header().Set("X-Request-ID", requestID)
	if err := server.auth.ValidateHost(request); err != nil {
		server.recordRejection(request.Context(), requestID, "host_rejected")
		server.writeAuthProblem(writer, request, requestID, err)
		return
	}
	if !strings.HasPrefix(request.URL.Path, Prefix+"/") &&
		request.URL.Path != Prefix {
		setManagementStaticHeaders(writer.Header())
		server.ui.ServeHTTP(writer, request)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	server.route(writer, request, requestID)
}

func setManagementStaticHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set(
		"Content-Security-Policy",
		"default-src 'self'; base-uri 'none'; object-src 'none'; "+
			"frame-ancestors 'none'; form-action 'self' https://github.com; script-src 'self'; "+
			"style-src 'self'; connect-src 'self'",
	)
	header.Set("Cross-Origin-Opener-Policy", "same-origin")
	header.Set("Cross-Origin-Resource-Policy", "same-origin")
	header.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("X-Frame-Options", "DENY")
}

func (server *server) route(writer http.ResponseWriter, request *http.Request, requestID string) {
	path := strings.TrimPrefix(request.URL.Path, Prefix)
	contract := &generatedServerContract{server: server, requestID: requestID}
	switch {
	case path == "/session" && request.Method == http.MethodPost:
		contract.CreateSession(writer, request, gen.CreateSessionParams{})
	case path == "/session" && request.Method == http.MethodGet:
		contract.GetSession(writer, request)
	case path == "/session" && request.Method == http.MethodDelete:
		contract.DeleteSession(writer, request, gen.DeleteSessionParams{})
	case path == "/browser-handoffs" && request.Method == http.MethodPost:
		contract.CreateBrowserHandoff(writer, request)
	case path == "/browser-handoffs/claim" && request.Method == http.MethodPost:
		contract.ClaimBrowserHandoff(writer, request)
	case path == "/browser-handoff-authorizations" &&
		request.Method == http.MethodPost:
		contract.AuthorizeBrowserHandoff(
			writer,
			request,
			gen.AuthorizeBrowserHandoffParams{},
		)
	case path == "/setup" && request.Method == http.MethodGet:
		contract.GetSetup(writer, request)
	case path == "/github/app/manifest" && request.Method == http.MethodPost:
		contract.StartGitHubAppManifest(writer, request, gen.StartGitHubAppManifestParams{})
	case path == "/github/app/callback" && request.Method == http.MethodGet:
		contract.CompleteGitHubAppManifest(writer, request, gen.CompleteGitHubAppManifestParams{})
	case path == "/github/installations" && request.Method == http.MethodGet:
		contract.ListGitHubInstallations(writer, request)
	case path == "/github/targets" && request.Method == http.MethodPost:
		contract.CreateGitHubTarget(writer, request, gen.CreateGitHubTargetParams{})
	case path == "/overview" && request.Method == http.MethodGet:
		contract.GetOverview(writer, request)
	case path == "/nodes" && request.Method == http.MethodGet:
		contract.ListNodes(writer, request)
	case path == "/targets" && request.Method == http.MethodGet:
		contract.ListTargets(writer, request)
	case path == "/runs" && request.Method == http.MethodGet:
		contract.ListRuns(writer, request)
	case path == "/audit-events" && request.Method == http.MethodGet:
		contract.ListAuditEvents(writer, request, gen.ListAuditEventsParams{})
	case path == "/join-codes" && request.Method == http.MethodPost:
		contract.CreateJoinCode(writer, request, gen.CreateJoinCodeParams{})
	case strings.HasPrefix(path, "/join-codes/") && request.Method == http.MethodDelete:
		contract.CancelJoinCode(writer, request, "", gen.CancelJoinCodeParams{})
	case path == "/configuration" && request.Method == http.MethodGet:
		contract.GetConfiguration(writer, request)
	case path == "/configuration" && request.Method == http.MethodPut:
		contract.ApplyConfiguration(writer, request, gen.ApplyConfigurationParams{})
	case path == "/configuration/export" && request.Method == http.MethodGet:
		contract.ExportConfiguration(writer, request)
	case path == "/events" && request.Method == http.MethodGet:
		contract.StreamEvents(writer, request, gen.StreamEventsParams{})
	case strings.HasPrefix(path, "/nodes/") &&
		strings.HasSuffix(path, "/drain") &&
		request.Method == http.MethodPost:
		contract.DrainNode(writer, request, "", gen.DrainNodeParams{})
	case strings.HasPrefix(path, "/nodes/") &&
		strings.HasSuffix(path, "/resume") &&
		request.Method == http.MethodPost:
		contract.ResumeNode(writer, request, "", gen.ResumeNodeParams{})
	case strings.HasPrefix(path, "/nodes/") &&
		strings.HasSuffix(path, "/revoke") &&
		request.Method == http.MethodPost:
		contract.RevokeNode(writer, request, "", gen.RevokeNodeParams{})
	default:
		server.writeProblem(writer, request, requestID, http.StatusNotFound,
			"not_found", "Resource not found", "The requested management resource does not exist.", nil)
	}
}

func (server *server) createSession(writer http.ResponseWriter, request *http.Request, requestID string) {
	if err := server.auth.ValidateBootstrap(request); err != nil {
		server.recordRejection(request.Context(), requestID, "authentication_failed")
		server.writeAuthProblem(writer, request, requestID, err)
		return
	}
	if err := requireEmptyBody(writer, request); err != nil {
		server.writeProblem(writer, request, requestID, http.StatusBadRequest,
			"invalid_session_request", "Invalid session request", "The session request body must be empty.", nil)
		return
	}
	session, cookie, err := server.auth.IssueSession()
	if err != nil {
		server.writeProblem(writer, request, requestID, http.StatusServiceUnavailable,
			"session_unavailable", "Session unavailable", "The administrator session could not be created.", nil)
		return
	}
	if err := server.recordAudit(request.Context(), AuditInput{
		Actor: "single_admin", Action: "authentication_succeeded", Outcome: "succeeded",
		ResourceType: "controller", RequestID: requestID,
	}); err != nil {
		_ = server.auth.RevokeSession(session)
		server.writeUnavailable(writer, request, requestID)
		return
	}
	csrf, err := server.auth.CSRFToken(session)
	if err != nil {
		_ = server.auth.RevokeSession(session)
		server.writeUnavailable(writer, request, requestID)
		return
	}
	http.SetCookie(writer, cookie)
	server.writeJSON(writer, http.StatusCreated, gen.Session{
		Authenticated: gen.SessionAuthenticated(true),
		CsrfToken:     csrf,
	})
}

func (server *server) getSession(writer http.ResponseWriter, request *http.Request, requestID string) {
	session, err := server.auth.Authenticate(request)
	if err != nil {
		server.recordRejection(request.Context(), requestID, "authentication_failed")
		server.writeAuthProblem(writer, request, requestID, err)
		return
	}
	csrf, err := server.auth.CSRFToken(session)
	if err != nil {
		server.writeUnavailable(writer, request, requestID)
		return
	}
	server.writeJSON(writer, http.StatusOK, gen.Session{
		Authenticated: gen.SessionAuthenticated(true),
		CsrfToken:     csrf,
	})
}

func (server *server) deleteSession(writer http.ResponseWriter, request *http.Request, requestID string) {
	session, err := server.auth.AuthorizeMutation(request)
	if err != nil {
		server.recordRejection(request.Context(), requestID, "mutation_rejected")
		server.writeAuthProblem(writer, request, requestID, err)
		return
	}
	// Logout reduces authority, so it must not depend on the health of the
	// external audit/store boundary. Revoke the process-local credential and
	// clear the browser cookie before attempting any fallible persistence.
	revokeErr := server.auth.RevokeSession(session)
	http.SetCookie(writer, server.auth.ExpireSessionCookie())
	if revokeErr != nil || !server.backend.AuditHealthy() {
		server.writeUnavailable(writer, request, requestID)
		return
	}
	if err := server.recordAudit(request.Context(), AuditInput{
		Actor: "single_admin", Action: "session_ended", Outcome: "succeeded",
		ResourceType: "controller", RequestID: requestID,
	}); err != nil {
		server.writeUnavailable(writer, request, requestID)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (server *server) createBrowserHandoff(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
) {
	if err := server.auth.ValidateOrigin(request); err != nil {
		server.recordRejection(request.Context(), requestID, "request_forbidden")
		server.writeAuthProblem(writer, request, requestID, err)
		return
	}
	var input gen.CreateBrowserHandoffRequest
	if err := decodeJSONBody(writer, request, &input); err != nil {
		server.writeDecodeError(writer, request, requestID, err)
		return
	}
	digest, ok := decodeCanonicalBase64URL32(input.ClaimDigest)
	if !ok {
		server.writeProblemWithFields(
			writer,
			request,
			requestID,
			http.StatusUnprocessableEntity,
			"validation_failed",
			"Request validation failed",
			"Correct the listed fields and retry.",
			[]gen.FieldError{{
				Field:   "claimDigest",
				Code:    "invalid_encoding",
				Message: "The claim digest must be one canonical SHA-256 value.",
			}},
		)
		return
	}
	handoff, err := server.auth.IssueBrowserHandoff(digest)
	if err != nil {
		server.writeUnavailable(writer, request, requestID)
		return
	}
	server.writeJSON(writer, http.StatusCreated, gen.BrowserHandoff{
		Code:      handoff.Code(),
		State:     gen.BrowserHandoffStatePending,
		ExpiresAt: handoff.ExpiresAt(),
	})
}

func (server *server) authorizeBrowserHandoff(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
) {
	if _, authorized := server.authorizeMutation(
		writer,
		request,
		requestID,
	); !authorized {
		return
	}
	var input gen.AuthorizeBrowserHandoffRequest
	if err := decodeJSONBody(writer, request, &input); err != nil {
		server.writeDecodeError(writer, request, requestID, err)
		return
	}
	if !auth.ValidBrowserHandoffCodeEncoding(input.Code) {
		server.writeBrowserHandoffValidation(
			writer,
			request,
			requestID,
			"code",
		)
		return
	}
	approval, err := server.auth.ApproveBrowserHandoff(input.Code)
	if err != nil {
		server.writeBrowserHandoffError(writer, request, requestID, err, true)
		return
	}
	if !approval.NeedsAudit() {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	if err := server.recordAudit(request.Context(), AuditInput{
		Actor: "single_admin", Action: "browser_handoff_authorized",
		Outcome: "succeeded", ResourceType: "controller", RequestID: requestID,
	}); err != nil {
		_ = server.auth.RollbackBrowserHandoffApproval(approval)
		server.writeUnavailable(writer, request, requestID)
		return
	}
	if err := server.auth.CommitBrowserHandoffApproval(approval); err != nil {
		server.writeUnavailable(writer, request, requestID)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (server *server) claimBrowserHandoff(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
) {
	if err := server.auth.ValidateOrigin(request); err != nil {
		server.recordRejection(request.Context(), requestID, "request_forbidden")
		server.writeAuthProblem(writer, request, requestID, err)
		return
	}
	var input gen.ClaimBrowserHandoffRequest
	if err := decodeJSONBody(writer, request, &input); err != nil {
		server.writeDecodeError(writer, request, requestID, err)
		return
	}
	if !auth.ValidBrowserHandoffCodeEncoding(input.Code) {
		server.writeBrowserHandoffValidation(
			writer,
			request,
			requestID,
			"code",
		)
		return
	}
	if input.ClaimSecret == nil {
		server.writeBrowserHandoffValidation(
			writer,
			request,
			requestID,
			"claimSecret",
		)
		return
	}
	secret, ok := decodeCanonicalBase64URL32(*input.ClaimSecret)
	if !ok {
		server.writeBrowserHandoffValidation(
			writer,
			request,
			requestID,
			"claimSecret",
		)
		return
	}
	claim, err := server.auth.ClaimBrowserHandoff(input.Code, secret)
	if err != nil {
		server.writeBrowserHandoffError(writer, request, requestID, err, false)
		return
	}
	session, cookie, err := server.auth.IssueSession()
	if err != nil {
		_ = server.auth.RollbackBrowserHandoffClaim(claim)
		server.writeUnavailable(writer, request, requestID)
		return
	}
	revokeAndRollback := func() {
		_ = server.auth.RevokeSession(session)
		_ = server.auth.RollbackBrowserHandoffClaim(claim)
	}
	if err := server.recordAudit(request.Context(), AuditInput{
		Actor: "single_admin", Action: "authentication_succeeded",
		Outcome: "succeeded", ResourceType: "controller", RequestID: requestID,
	}); err != nil {
		revokeAndRollback()
		server.writeUnavailable(writer, request, requestID)
		return
	}
	csrf, err := server.auth.CSRFToken(session)
	if err != nil {
		revokeAndRollback()
		server.writeUnavailable(writer, request, requestID)
		return
	}
	if err := server.auth.CommitBrowserHandoffClaim(claim); err != nil {
		_ = server.auth.RevokeSession(session)
		server.writeUnavailable(writer, request, requestID)
		return
	}
	http.SetCookie(writer, cookie)
	server.writeJSON(writer, http.StatusCreated, gen.Session{
		Authenticated: gen.SessionAuthenticated(true),
		CsrfToken:     csrf,
	})
}

func (server *server) writeBrowserHandoffValidation(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
	field string,
) {
	server.writeProblemWithFields(
		writer,
		request,
		requestID,
		http.StatusUnprocessableEntity,
		"validation_failed",
		"Request validation failed",
		"Correct the listed fields and retry.",
		[]gen.FieldError{{
			Field:   field,
			Code:    "invalid_encoding",
			Message: "The browser handoff value is not canonically encoded.",
		}},
	)
}

func (server *server) writeBrowserHandoffError(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
	err error,
	ownerAuthorized bool,
) {
	var pending *auth.BrowserHandoffPendingError
	switch {
	case errors.As(err, &pending):
		if ownerAuthorized {
			server.writeProblem(
				writer,
				request,
				requestID,
				http.StatusConflict,
				"browser_handoff_pending",
				"Browser handoff authorization is in progress",
				"Retry after the current owner authorization finishes.",
				nil,
			)
			return
		}
		server.writeJSON(writer, http.StatusAccepted, gen.BrowserHandoffPending{
			State:     gen.BrowserHandoffPendingStatePending,
			ExpiresAt: pending.ExpiresAt(),
		})
	case errors.Is(err, auth.ErrExpiredBrowserHandoff):
		server.writeProblem(
			writer,
			request,
			requestID,
			http.StatusGone,
			"browser_handoff_expired",
			"Browser handoff expired",
			"Create and authorize a new browser handoff.",
			nil,
		)
	case errors.Is(err, auth.ErrBrowserHandoffClaiming):
		server.writeProblem(
			writer,
			request,
			requestID,
			http.StatusConflict,
			"browser_handoff_claim_in_progress",
			"Browser handoff claim is in progress",
			"Wait for the active claim before checking the current session.",
			nil,
		)
	case errors.Is(err, auth.ErrBrowserHandoffClaimed):
		server.writeProblem(
			writer,
			request,
			requestID,
			http.StatusConflict,
			"browser_handoff_already_claimed",
			"Browser handoff was already claimed",
			"Read the current session or create a new browser handoff.",
			nil,
		)
	case errors.Is(err, auth.ErrInvalidBrowserHandoff):
		server.recordRejection(request.Context(), requestID, "authentication_failed")
		if ownerAuthorized {
			server.writeBrowserHandoffValidation(
				writer,
				request,
				requestID,
				"code",
			)
			return
		}
		server.writeProblem(
			writer,
			request,
			requestID,
			http.StatusUnauthorized,
			"browser_handoff_invalid",
			"Browser handoff is invalid",
			"Create and authorize a new browser handoff.",
			nil,
		)
	default:
		server.writeUnavailable(writer, request, requestID)
	}
}

func (server *server) read(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
	read func(context.Context) (any, error),
) {
	if !server.authenticate(writer, request, requestID) {
		return
	}
	value, err := read(request.Context())
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.writeJSON(writer, http.StatusOK, value)
}

func (server *server) listNodes(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !server.authenticate(writer, request, requestID) {
		return
	}
	nodes, revision, err := server.backend.Nodes(request.Context())
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.writeJSON(writer, http.StatusOK, struct {
		Nodes                 []gen.Node   `json:"nodes"`
		ConfigurationRevision gen.Revision `json:"configurationRevision"`
	}{Nodes: nonNil(nodes), ConfigurationRevision: revision})
}

func (server *server) listTargets(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !server.authenticate(writer, request, requestID) {
		return
	}
	targets, revision, err := server.backend.Targets(request.Context())
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.writeJSON(writer, http.StatusOK, struct {
		Targets               []gen.Target `json:"targets"`
		ConfigurationRevision gen.Revision `json:"configurationRevision"`
	}{Targets: nonNil(targets), ConfigurationRevision: revision})
}

func (server *server) startGitHubAppManifest(writer http.ResponseWriter, request *http.Request, requestID string) {
	if _, authorized := server.authorizeMutation(writer, request, requestID); !authorized {
		return
	}
	backend, ok := server.backend.(GitHubBackend)
	if !ok {
		server.writeUnavailable(writer, request, requestID)
		return
	}
	var input gen.GitHubManifestStartRequest
	if err := decodeJSONBody(writer, request, &input); err != nil {
		server.writeDecodeError(writer, request, requestID, err)
		return
	}
	callback := requestScheme(request) + "://" + request.Host + Prefix + "/github/app/callback"
	account := ""
	if input.RegistrationAccount != nil {
		account = *input.RegistrationAccount
	}
	result, err := backend.StartGitHubAppManifest(request.Context(), callback, account)
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.writeJSON(writer, http.StatusOK, result)
}

func (server *server) completeGitHubAppManifest(writer http.ResponseWriter, request *http.Request, requestID string) {
	backend, ok := server.backend.(GitHubBackend)
	if !ok {
		server.writeUnavailable(writer, request, requestID)
		return
	}
	code := request.URL.Query().Get("code")
	state := request.URL.Query().Get("state")
	if code == "" || state == "" {
		server.writeProblem(writer, request, requestID, http.StatusBadRequest,
			"invalid_body", "Invalid GitHub callback", "The GitHub callback is missing its one-time state.", nil)
		return
	}
	if err := backend.CompleteGitHubAppManifest(request.Context(), code, state); err != nil {
		if errors.Is(err, ErrDomainConflict) || errors.Is(err, ErrGitHubCallbackConflict) {
			server.writeProblem(writer, request, requestID, http.StatusConflict,
				"state_conflict", "GitHub callback was already used", "Start a new GitHub App connection.", nil)
			return
		}
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	http.Redirect(writer, request, "/", http.StatusFound)
}

func (server *server) listGitHubInstallations(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !server.authenticate(writer, request, requestID) {
		return
	}
	backend, ok := server.backend.(GitHubBackend)
	if !ok {
		server.writeUnavailable(writer, request, requestID)
		return
	}
	result, err := backend.ListGitHubInstallations(request.Context())
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.writeJSON(writer, http.StatusOK, result)
}

func (server *server) createGitHubTarget(writer http.ResponseWriter, request *http.Request, requestID string) {
	if _, authorized := server.authorizeMutation(writer, request, requestID); !authorized {
		return
	}
	backend, ok := server.backend.(GitHubBackend)
	if !ok {
		server.writeUnavailable(writer, request, requestID)
		return
	}
	expected, ok := server.requireRevision(writer, request, requestID)
	if !ok {
		return
	}
	var input gen.CreateGitHubTargetRequest
	if err := decodeJSONBody(writer, request, &input); err != nil {
		server.writeDecodeError(writer, request, requestID, err)
		return
	}
	result, err := backend.CreateGitHubTarget(request.Context(), expected, input, requestID)
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.events.Publish()
	server.writeJSON(writer, http.StatusOK, result)
}

func requestScheme(request *http.Request) string {
	if request != nil && request.TLS != nil {
		return "https"
	}
	return "http"
}

func (server *server) listRuns(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !server.authenticate(writer, request, requestID) {
		return
	}
	runs, err := server.backend.Runs(request.Context())
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.writeJSON(writer, http.StatusOK, struct {
		Runs []gen.Run `json:"runs"`
	}{Runs: nonNil(runs)})
}

func (server *server) listAuditEvents(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !server.authenticate(writer, request, requestID) {
		return
	}
	params, after, err := bindAuditEventsParams(request)
	if err != nil {
		server.writeProblem(writer, request, requestID, http.StatusBadRequest,
			"invalid_query", "Invalid query",
			"Use the canonical audit cursor and a page limit from 1 through 500.", nil)
		return
	}
	limit := DefaultAuditPageSize
	if params.Limit != nil {
		limit = *params.Limit
	}
	page, err := server.backend.AuditEvents(request.Context(), after, limit)
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	response := gen.AuditEventPage{Events: nonNil(page.Events)}
	if page.NextAfter != nil {
		cursor, err := encodeAuditCursor(*page.NextAfter)
		if err != nil {
			server.writeUnavailable(writer, request, requestID)
			return
		}
		response.NextCursor = &cursor
	}
	if page.ResumeAfter != nil {
		cursor, err := encodeAuditCursor(*page.ResumeAfter)
		if err != nil {
			server.writeUnavailable(writer, request, requestID)
			return
		}
		response.ResumeCursor = &cursor
	}
	server.writeJSON(writer, http.StatusOK, response)
}

func (server *server) createJoinCode(writer http.ResponseWriter, request *http.Request, requestID string) {
	if _, authorized := server.authorizeMutation(writer, request, requestID); !authorized {
		return
	}
	var body gen.CreateJoinCodeRequest
	if err := decodeJSONBody(writer, request, &body); err != nil {
		server.writeDecodeError(writer, request, requestID, err)
		return
	}
	tokenID, code, err := server.backend.CreateJoinCode(request.Context(), body.EndpointHints, requestID)
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.events.Publish()
	server.writeJSON(writer, http.StatusCreated, gen.JoinCodeDelivery{TokenId: tokenID, Code: code})
}

func (server *server) cancelJoinCode(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
	tokenID string,
) {
	if _, authorized := server.authorizeMutation(writer, request, requestID); !authorized {
		return
	}
	if len(tokenID) != 32 {
		server.writeProblem(writer, request, requestID, http.StatusNotFound,
			"join_code_not_found", "Join code not found", "The join code is not available.", nil)
		return
	}
	if _, err := hex.DecodeString(tokenID); err != nil {
		server.writeProblem(writer, request, requestID, http.StatusNotFound,
			"join_code_not_found", "Join code not found", "The join code is not available.", nil)
		return
	}
	if err := server.backend.CancelJoinCode(request.Context(), tokenID, requestID); err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.events.Publish()
	writer.WriteHeader(http.StatusNoContent)
}

func (server *server) getConfiguration(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !server.authenticate(writer, request, requestID) {
		return
	}
	configuration, err := server.backend.ReadConfiguration(request.Context())
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	writer.Header().Set("ETag", etag(configuration.Revision))
	server.writeJSON(writer, http.StatusOK, configuration)
}

func (server *server) applyConfiguration(writer http.ResponseWriter, request *http.Request, requestID string) {
	if _, authorized := server.authorizeMutation(writer, request, requestID); !authorized {
		return
	}
	expected, ok := server.requireRevision(writer, request, requestID)
	if !ok {
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || (mediaType != "application/json" && mediaType != "application/yaml") {
		server.writeProblem(writer, request, requestID, http.StatusUnsupportedMediaType,
			"unsupported_media_type", "Unsupported media type", "Use application/json or application/yaml.", nil)
		return
	}
	payload, err := readBoundedBody(writer, request)
	if err != nil {
		server.writeDecodeError(writer, request, requestID, err)
		return
	}
	configuration, err := server.backend.ApplyConfiguration(
		request.Context(), expected, mediaType, payload, requestID,
	)
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.events.Publish()
	writer.Header().Set("ETag", etag(configuration.Revision))
	server.writeJSON(writer, http.StatusOK, configuration)
}

func (server *server) exportConfiguration(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !server.authenticate(writer, request, requestID) {
		return
	}
	payload, revision, err := server.backend.ExportConfiguration(request.Context())
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	writer.Header().Set("Content-Type", "application/yaml")
	writer.Header().Set("ETag", etag(revision))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(payload)
}

func (server *server) mutateNode(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
	path string,
) {
	parts := strings.Split(strings.TrimPrefix(path, "/nodes/"), "/")
	if len(parts) != 2 || parts[0] == "" {
		server.writeProblem(writer, request, requestID, http.StatusNotFound,
			"not_found", "Resource not found", "The requested node operation does not exist.", nil)
		return
	}
	var state domain.NodeAdministrativeState
	switch parts[1] {
	case "drain":
		state = domain.NodeDraining
	case "resume":
		state = domain.NodeActive
	case "revoke":
		state = domain.NodeRevoked
	default:
		server.writeProblem(writer, request, requestID, http.StatusNotFound,
			"not_found", "Resource not found", "The requested node operation does not exist.", nil)
		return
	}
	if _, authorized := server.authorizeMutation(writer, request, requestID); !authorized {
		return
	}
	if err := requireEmptyBody(writer, request); err != nil {
		server.writeDecodeError(writer, request, requestID, err)
		return
	}
	expected, ok := server.requireRevision(writer, request, requestID)
	if !ok {
		return
	}
	node, revision, err := server.backend.SetNodeAdministrativeState(
		request.Context(), domain.NodeID(parts[0]), state, expected, requestID,
	)
	if err != nil {
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	server.events.Publish()
	writer.Header().Set("ETag", etag(revision))
	server.writeJSON(writer, http.StatusOK, struct {
		Node                  gen.Node     `json:"node"`
		ConfigurationRevision gen.Revision `json:"configurationRevision"`
	}{Node: node, ConfigurationRevision: revision})
}

func (server *server) streamEvents(writer http.ResponseWriter, request *http.Request, requestID string) {
	session, err := server.auth.AuthorizeSameOriginRead(request)
	if err != nil {
		server.recordRejection(request.Context(), requestID, "event_stream_rejected")
		server.writeAuthProblem(writer, request, requestID, err)
		return
	}
	expiresAt, revoked, err := server.auth.WatchSession(session)
	if err != nil {
		server.recordRejection(request.Context(), requestID, "event_stream_rejected")
		server.writeAuthProblem(writer, request, requestID, err)
		return
	}
	streamContext, stopStream := context.WithDeadline(request.Context(), expiresAt)
	defer stopStream()
	go func() {
		select {
		case <-revoked:
			stopStream()
		case <-streamContext.Done():
		}
	}()
	flusher, ok := writer.(http.Flusher)
	if !ok {
		server.writeUnavailable(writer, request, requestID)
		return
	}
	// Subscribe before reading the durable revision. A publication racing with
	// the read is then either reflected in the initial reset or remains queued
	// as an invalidate event; it can never disappear between the two steps.
	changes, generation, unsubscribe := server.events.subscribe()
	defer unsubscribe()
	revision, err := server.backend.CurrentRevision(streamContext)
	if err != nil {
		if streamContext.Err() != nil {
			return
		}
		server.writeBackendError(writer, request, requestID, err)
		return
	}
	select {
	case <-streamContext.Done():
		return
	default:
	}

	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Accel-Buffering", "no")
	current := server.cursor(revision, generation)
	supplied := request.Header.Get("Last-Event-ID")
	if query := request.URL.Query().Get("cursor"); query != "" {
		supplied = query
	}
	eventType := "reset"
	resources := allResources()
	if supplied == current {
		eventType = "ready"
		resources = []gen.SSEEventDataResources{}
	}
	if err := writeSSE(writer, eventType, current, resources); err != nil {
		return
	}
	flusher.Flush()

	for {
		select {
		case <-streamContext.Done():
			return
		case _, open := <-changes:
			if !open {
				return
			}
			revision, err = server.backend.CurrentRevision(streamContext)
			if err != nil {
				return
			}
			current = server.cursor(revision, server.events.currentGeneration())
			if err := writeSSE(writer, "invalidate", current, allResources()); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (server *server) authenticate(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
) bool {
	if _, err := server.auth.Authenticate(request); err != nil {
		server.recordRejection(request.Context(), requestID, "authentication_failed")
		server.writeAuthProblem(writer, request, requestID, err)
		return false
	}
	return true
}

func (server *server) authorizeMutation(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
) (auth.Session, bool) {
	session, err := server.auth.AuthorizeMutation(request)
	if err != nil {
		server.recordRejection(request.Context(), requestID, "mutation_rejected")
		server.writeAuthProblem(writer, request, requestID, err)
		return auth.Session{}, false
	}
	if !server.backend.AuditHealthy() {
		server.writeUnavailable(writer, request, requestID)
		return auth.Session{}, false
	}
	return session, true
}

func (server *server) requireRevision(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
) (uint64, bool) {
	raw := request.Header.Get("If-Match")
	if raw == "" {
		server.writeProblem(writer, request, requestID, http.StatusPreconditionRequired,
			"precondition_required", "Configuration precondition required",
			"Reload the configuration and send its ETag in If-Match.", nil)
		return 0, false
	}
	if !strings.HasPrefix(raw, `"cfg-`) || !strings.HasSuffix(raw, `"`) {
		server.writeProblem(writer, request, requestID, http.StatusBadRequest,
			"invalid_precondition", "Invalid configuration precondition",
			"If-Match must contain one canonical SpareRunner configuration ETag.", nil)
		return 0, false
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(raw, `"cfg-`), `"`)
	revision, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || strconv.FormatUint(revision, 10) != digits {
		server.writeProblem(writer, request, requestID, http.StatusBadRequest,
			"invalid_precondition", "Invalid configuration precondition",
			"If-Match must contain one canonical SpareRunner configuration ETag.", nil)
		return 0, false
	}
	return revision, true
}

func (server *server) recordRejection(ctx context.Context, requestID, code string) {
	if server == nil || !server.rejectionAudits.claim(code) {
		return
	}
	_ = server.recordAudit(ctx, AuditInput{
		Actor: "anonymous", Action: "authentication_failed", Outcome: "rejected",
		ResourceType: "controller", ErrorCode: code, RequestID: requestID,
	})
}

func (guard *rejectionAuditGuard) claim(code string) bool {
	if guard == nil || guard.now == nil || code == "" {
		return false
	}
	now := guard.now()
	if now.IsZero() {
		return false
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.windowStartedAt.IsZero() ||
		now.Before(guard.windowStartedAt) ||
		!now.Before(guard.windowStartedAt.Add(rejectionAuditWindow)) {
		guard.windowStartedAt = now
		clear(guard.codes)
	}
	if _, exists := guard.codes[code]; exists {
		return false
	}
	guard.codes[code] = struct{}{}
	return true
}

func (server *server) recordAudit(ctx context.Context, input AuditInput) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Audit evidence belongs to the server-side operation, not to the lifetime
	// of a client socket. Ignore client cancellation but retain a hard bound so
	// a broken store cannot pin an HTTP handler indefinitely.
	auditContext, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		auditPersistenceTimeout,
	)
	defer cancel()
	err := server.backend.RecordAudit(auditContext, input)
	// Successful appends update the audit list; failures update audit health.
	// Both are observable state changes even though the audit log itself remains
	// the durable authority.
	server.events.Publish()
	return err
}

func (server *server) writeAuthProblem(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
	err error,
) {
	status := auth.HTTPStatus(err)
	code, title, detail := "authentication_failed", "Authentication failed", "A valid administrator session is required."
	switch status {
	case http.StatusMisdirectedRequest:
		code, title, detail = "misdirected_host", "Misdirected request", "The request Host does not match this management listener."
	case http.StatusForbidden:
		code, title, detail = "request_forbidden", "Request forbidden", "The request Origin or CSRF token is invalid."
	case http.StatusMethodNotAllowed:
		code, title, detail = "method_not_allowed", "Method not allowed", "Use the documented method for this management operation."
	}
	server.writeProblem(writer, request, requestID, status, code, title, detail, nil)
}

func (server *server) writeBackendError(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
	err error,
) {
	var conflict *RevisionConflict
	var committed *CommittedMutationError
	var validation *ValidationError
	switch {
	case errors.As(err, &conflict):
		current := gen.Revision(strconv.FormatUint(conflict.Current, 10))
		server.writeProblem(writer, request, requestID, http.StatusConflict,
			"configuration_revision_conflict", "Configuration revision is stale",
			"Reload the current configuration and retry the mutation.", &current)
	case errors.As(err, &committed):
		current := gen.Revision(strconv.FormatUint(committed.Current, 10))
		server.events.Publish()
		server.writeProblem(writer, request, requestID, http.StatusServiceUnavailable,
			"mutation_committed_reload_required", "Mutation committed; reload required",
			"The durable mutation committed, but its live projection or response could not be confirmed. Reload current state before taking another action.",
			&current)
	case errors.As(err, &validation):
		fields := make([]gen.FieldError, 0, len(validation.Violations))
		for _, violation := range validation.Violations {
			fields = append(fields, gen.FieldError{
				Field: violation.Field, Code: violation.Code, Message: violation.Message,
			})
		}
		server.writeProblemWithFields(writer, request, requestID, http.StatusUnprocessableEntity,
			"validation_failed", "Request validation failed",
			"Correct the listed fields and retry.", fields)
	case errors.Is(err, ErrResourceNotFound):
		server.writeProblem(writer, request, requestID, http.StatusNotFound,
			"not_found", "Resource not found", "The requested resource is not available.", nil)
	case errors.Is(err, ErrDomainConflict):
		server.writeProblem(writer, request, requestID, http.StatusConflict,
			"state_conflict", "Operation conflicts with current state",
			"Reload the current state and resolve the conflict before retrying.", nil)
	default:
		server.writeUnavailable(writer, request, requestID)
	}
}

func (server *server) writeDecodeError(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
	err error,
) {
	if errors.Is(err, errBodyTooLarge) {
		server.writeProblem(writer, request, requestID, http.StatusRequestEntityTooLarge,
			"payload_too_large", "Request body is too large",
			"The request exceeds the measured management configuration transport budget.", nil)
		return
	}
	if errors.Is(err, errUnsupportedJSONMediaType) {
		server.writeProblem(writer, request, requestID, http.StatusUnsupportedMediaType,
			"unsupported_media_type", "Unsupported media type",
			"Use application/json for this management operation.", nil)
		return
	}
	server.writeProblem(writer, request, requestID, http.StatusBadRequest,
		"invalid_body", "Invalid request body",
		"The request body is malformed, truncated, or contains unsupported fields.", nil)
}

func (server *server) writeUnavailable(
	writer http.ResponseWriter,
	request *http.Request,
	requestID string,
) {
	server.writeProblem(writer, request, requestID, http.StatusServiceUnavailable,
		"management_unavailable", "Management authority is unavailable",
		"The operation did not complete as a confirmed success. Reload current state before retrying.", nil)
}

func (server *server) writeProblem(
	writer http.ResponseWriter,
	_ *http.Request,
	requestID string,
	status int,
	code string,
	title string,
	detail string,
	current *gen.Revision,
) {
	problem := gen.Problem{
		Type:      "https://sparerunner.dev/problems/" + strings.ReplaceAll(code, "_", "-"),
		Title:     title,
		Status:    status,
		Code:      code,
		Detail:    detail,
		Instance:  "urn:sparerunner:request:" + requestID,
		RequestId: requestID,
	}
	if current != nil {
		problem.CurrentRevision = current
	}
	server.writeProblemJSON(writer, status, problem)
}

func (server *server) writeProblemWithFields(
	writer http.ResponseWriter,
	_ *http.Request,
	requestID string,
	status int,
	code string,
	title string,
	detail string,
	fields []gen.FieldError,
) {
	problem := gen.Problem{
		Type:      "https://sparerunner.dev/problems/" + strings.ReplaceAll(code, "_", "-"),
		Title:     title,
		Status:    status,
		Code:      code,
		Detail:    detail,
		Instance:  "urn:sparerunner:request:" + requestID,
		RequestId: requestID,
		Errors:    &fields,
	}
	server.writeProblemJSON(writer, status, problem)
}

func (server *server) writeProblemJSON(writer http.ResponseWriter, status int, problem gen.Problem) {
	writer.Header().Set("Content-Type", "application/problem+json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(problem)
}

func (server *server) writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

var (
	errBodyTooLarge             = errors.New("management request body is too large")
	errUnsupportedJSONMediaType = errors.New("management request must use application/json")
)

func readBoundedBody(writer http.ResponseWriter, request *http.Request) ([]byte, error) {
	if request.ContentLength > MaximumRequestBodyBytes {
		return nil, errBodyTooLarge
	}
	request.Body = http.MaxBytesReader(writer, request.Body, MaximumRequestBodyBytes)
	payload, err := io.ReadAll(request.Body)
	if err != nil {
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			return nil, errBodyTooLarge
		}
		return nil, err
	}
	return payload, nil
}

func decodeJSONBody(writer http.ResponseWriter, request *http.Request, destination any) error {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errUnsupportedJSONMediaType
	}
	payload, err := readBoundedBody(writer, request)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request contains trailing JSON")
	}
	return nil
}

func requireEmptyBody(writer http.ResponseWriter, request *http.Request) error {
	payload, err := readBoundedBody(writer, request)
	if err != nil {
		return err
	}
	if len(payload) != 0 {
		return errors.New("session bootstrap body is not empty")
	}
	return nil
}

func decodeCanonicalBase64URL32(value string) ([32]byte, bool) {
	var result [32]byte
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil ||
		len(decoded) != len(result) ||
		base64.RawURLEncoding.EncodeToString(decoded) != value {
		return result, false
	}
	copy(result[:], decoded)
	return result, true
}

var errInvalidAuditQuery = errors.New("audit page query is invalid")

func bindAuditEventsParams(
	request *http.Request,
) (gen.ListAuditEventsParams, uint64, error) {
	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return gen.ListAuditEventsParams{}, 0, errInvalidAuditQuery
	}
	var params gen.ListAuditEventsParams
	var after uint64
	if cursorValues, present := values["cursor"]; present {
		if len(cursorValues) != 1 {
			return gen.ListAuditEventsParams{}, 0, errInvalidAuditQuery
		}
		cursor := cursorValues[0]
		after, err = decodeAuditCursor(cursor)
		if err != nil {
			return gen.ListAuditEventsParams{}, 0, errInvalidAuditQuery
		}
		params.Cursor = &cursor
	}
	if limitValues, present := values["limit"]; present {
		if len(limitValues) != 1 {
			return gen.ListAuditEventsParams{}, 0, errInvalidAuditQuery
		}
		raw := limitValues[0]
		limit, err := strconv.Atoi(raw)
		if err != nil ||
			strconv.Itoa(limit) != raw ||
			limit < 1 ||
			limit > MaximumAuditPageSize {
			return gen.ListAuditEventsParams{}, 0, errInvalidAuditQuery
		}
		params.Limit = &limit
	}
	return params, after, nil
}

func encodeAuditCursor(sequence uint64) (string, error) {
	if sequence == 0 || sequence > math.MaxInt64 {
		return "", errInvalidAuditQuery
	}
	var payload [auditCursorPayloadSize]byte
	binary.BigEndian.PutUint64(payload[:], sequence)
	return auditCursorPrefix + base64.RawURLEncoding.EncodeToString(payload[:]), nil
}

func decodeAuditCursor(cursor string) (uint64, error) {
	const encodedPayloadSize = 11
	if len(cursor) != len(auditCursorPrefix)+encodedPayloadSize ||
		!strings.HasPrefix(cursor, auditCursorPrefix) {
		return 0, errInvalidAuditQuery
	}
	payload, err := base64.RawURLEncoding.DecodeString(cursor[len(auditCursorPrefix):])
	if err != nil || len(payload) != auditCursorPayloadSize {
		return 0, errInvalidAuditQuery
	}
	sequence := binary.BigEndian.Uint64(payload)
	canonical, err := encodeAuditCursor(sequence)
	if err != nil || canonical != cursor {
		return 0, errInvalidAuditQuery
	}
	return sequence, nil
}

func newRequestID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "req_" + hex.EncodeToString(value[:]), nil
}

func etag(revision gen.Revision) string {
	return fmt.Sprintf(`"cfg-%s"`, revision)
}

func (server *server) cursor(revision gen.Revision, generation uint64) string {
	return fmt.Sprintf("%d:%s:%d", server.epoch, revision, generation)
}

func writeSSE(
	writer io.Writer,
	eventType string,
	cursor string,
	resources []gen.SSEEventDataResources,
) error {
	payload, err := json.Marshal(gen.SSEEventData{
		SchemaVersion: gen.SSEEventDataSchemaVersion(1),
		Cursor:        cursor,
		Resources:     resources,
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(writer, "id: %s\nevent: %s\ndata: %s\n\n", cursor, eventType, payload)
	return err
}

func allResources() []gen.SSEEventDataResources {
	return []gen.SSEEventDataResources{
		"setup", "overview", "nodes", "targets", "runs", "configuration", "audit_events",
	}
}

func nonNil[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}
