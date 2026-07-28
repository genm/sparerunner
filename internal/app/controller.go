package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	managementapi "github.com/genm/tewake/internal/api"
	"github.com/genm/tewake/internal/auth"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
	"github.com/genm/tewake/internal/webui"
)

const DefaultHTTPReadHeaderTimeout = 10 * time.Second

type ControllerServeOptions struct {
	AgentListener     net.Listener
	AdminListener     net.Listener
	AdvertiseMDNS     bool
	ReadHeaderTimeout time.Duration
}

func ServeController(ctx context.Context, state *ControllerState, options ControllerServeOptions) error {
	if state == nil || state.Store == nil || state.AgentBroker == nil || options.AgentListener == nil {
		return errors.New("controller serve dependencies are incomplete")
	}
	if err := ValidateAdminListener(options.AdminListener); err != nil {
		return err
	}
	serveContext, cancel := context.WithCancel(ctx)
	defer cancel()
	readHeaderTimeout := options.ReadHeaderTimeout
	if readHeaderTimeout <= 0 {
		// Match Go's established default outbound TLS handshake budget. This is
		// an operator-overridable transport protection, not a job or fleet limit.
		readHeaderTimeout = DefaultHTTPReadHeaderTimeout
	}
	serverTLS, err := transport.ControllerServerTLSConfig(state.Identity)
	if err != nil {
		return err
	}
	agentServer := &http.Server{
		Handler:           controllerAgentHandler(state),
		ReadHeaderTimeout: readHeaderTimeout,
		MaxHeaderBytes:    http.DefaultMaxHeaderBytes,
	}
	var adminServer *http.Server
	if options.AdminListener != nil {
		origin, err := adminListenerOrigin(options.AdminListener)
		if err != nil {
			return err
		}
		adminAuth, err := auth.NewManager(state.AdminSession, origin, false)
		if err != nil {
			return err
		}
		backend, err := newManagementBackend(state, state.TargetVerifier)
		if err != nil {
			return err
		}
		events := managementapi.NewEventBus()
		handler, err := managementapi.NewHandler(managementapi.Options{
			Auth:    adminAuth,
			Backend: backend,
			Events:  events,
			UI:      embeddedUIHandler(),
			Epoch:   state.Epoch,
		})
		if err != nil {
			return err
		}
		adminServer = &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: readHeaderTimeout,
			MaxHeaderBytes:    http.DefaultMaxHeaderBytes,
		}
		go publishManagementInvalidations(serveContext, state, events)
	}
	var advertiser transport.Advertiser
	if options.AdvertiseMDNS {
		port, err := listenerPort(options.AgentListener)
		if err != nil {
			return err
		}
		fingerprint := state.Identity.CAFingerprint()
		advertiser, err = transport.StartMDNSAdvertiser("tewake-"+hex.EncodeToString(fingerprint[:4]), port, nil)
		if err != nil {
			return err
		}
		defer advertiser.Close()
	}

	// A controller with no GitHub App authority, no reconciler, or no provider
	// runs no coordinators. That is the ordinary disconnected state, not a
	// failure, so it must never stop the agent or admin listeners.
	var fleet *ControllerFleet
	if provider := NewGitHubAuthorityFleetProvider(state.GitHubAuthority); provider != nil &&
		state.Reconciler != nil {
		fleet, err = NewControllerFleet(state, provider, slog.Default())
		if err != nil {
			return err
		}
	}

	serverCount := 1
	if adminServer != nil {
		serverCount++
	}
	if fleet != nil {
		serverCount++
	}
	results := make(chan error, serverCount)
	go func() {
		err := agentServer.Serve(tls.NewListener(options.AgentListener, serverTLS))
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		results <- err
	}()
	if adminServer != nil {
		go func() {
			err := adminServer.Serve(options.AdminListener)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			results <- err
		}()
	}
	if fleet != nil {
		go func() {
			// The fleet joins the same error/shutdown group as the servers, but
			// its per-Target failures are already absorbed by bounded backoff,
			// so only a shutdown-time session error reaches this channel.
			results <- fleet.Run(serveContext)
		}()
	}
	go func() {
		<-serveContext.Done()
		state.AgentBroker.Close()
		state.Sessions.CloseAll()
		_ = agentServer.Close()
		if adminServer != nil {
			_ = adminServer.Close()
		}
	}()
	var firstErr error
	for range serverCount {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
			cancel()
			state.AgentBroker.Close()
			state.Sessions.CloseAll()
			_ = agentServer.Close()
			if adminServer != nil {
				_ = adminServer.Close()
			}
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return nil
}

// ValidateAdminListener keeps the unauthenticated management surface on
// loopback. A future LAN management mode must terminate authenticated TLS at
// this boundary or explicitly trust an authenticated reverse proxy.
func ValidateAdminListener(listener net.Listener) error {
	if listener == nil {
		return nil
	}
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address.IP == nil || !address.IP.IsLoopback() {
		return errors.New("management listener must use a loopback address")
	}
	return nil
}

func adminListenerOrigin(listener net.Listener) (string, error) {
	if err := ValidateAdminListener(listener); err != nil {
		return "", err
	}
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address.Port < 1 || address.Port > 65535 {
		return "", errors.New("management listener has an invalid TCP authority")
	}
	host := net.JoinHostPort(address.IP.String(), strconv.Itoa(address.Port))
	return "http://" + host, nil
}

func publishManagementInvalidations(
	ctx context.Context,
	state *ControllerState,
	events *managementapi.EventBus,
) {
	if state == nil || state.Store == nil || events == nil {
		return
	}
	auditChanged := state.Store.ManagementAuditChange()
	for {
		var reconcilerChanged <-chan struct{}
		if state.Reconciler != nil {
			reconcilerChanged = state.Reconciler.Change()
		}
		select {
		case <-ctx.Done():
			return
		case <-reconcilerChanged:
			events.Publish()
		case <-auditChanged:
			// Audit degradation is process-terminal for this store. Publish
			// exactly once, then disable this select arm so the closed channel
			// cannot create a busy loop.
			events.Publish()
			auditChanged = nil
		}
	}
}

func controllerAgentHandler(state *ControllerState) http.Handler {
	enrollment := auditedEnrollmentHTTPHandler(state)
	agentAudit := newAgentSessionAuditGuard(time.Now)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/enroll":
			enrollment.ServeHTTP(writer, request)
		case "/":
			if request.Method != http.MethodGet {
				http.Error(writer, "invalid agent request", http.StatusBadRequest)
				return
			}
			handler := func(ctx context.Context, session *transport.AuthenticatedSession) error {
				err := state.AgentBroker.serveSession(ctx, session)
				if errors.Is(err, transport.ErrProtocolVersion) ||
					errors.Is(err, ErrAgentProtocol) {
					return transport.AgentProtocolRejection(session.Credential(), err)
				}
				return err
			}
			if err := transport.UpgradeAuthenticatedWithSessions(writer, request, state.Store, handler, state.Sessions); err != nil {
				appendAgentSessionRejectionAudit(request, state, agentAudit, err)
				if transport.SessionWasUpgraded(err) {
					return
				}
				// After an upgrade this write is ignored by net/http; before an
				// upgrade this produces an explicit fail-closed response.
				http.Error(writer, "agent session rejected", http.StatusUnauthorized)
			}
		default:
			http.NotFound(writer, request)
		}
	})
}

func appendAgentSessionRejectionAudit(
	request *http.Request,
	state *ControllerState,
	guard *agentSessionAuditGuard,
	err error,
) {
	nodeID, kind, rejected := transport.AgentSessionRejection(err)
	if !rejected || request == nil || state == nil || state.Store == nil ||
		!guard.claim(nodeID, kind) {
		return
	}
	errorCode := store.AuditErrorAgentProtocolRejected
	if kind == transport.AgentSessionCredentialRejected {
		errorCode = store.AuditErrorNodeCredentialRejected
	}
	auditContext, cancel := detachedManagementProjectionContext(request.Context())
	defer cancel()
	_, _ = state.Store.AppendAuditEvent(auditContext, store.AuditRecord{
		Actor:        store.AuditActorNode,
		Action:       store.AuditActionAgentSessionRejected,
		Outcome:      store.AuditOutcomeRejected,
		ResourceKind: store.AuditResourceNode,
		ResourceID:   nodeID,
		ErrorCode:    errorCode,
		RequestID:    newEnrollmentAuditRequestID(),
	})
}

func auditedEnrollmentHTTPHandler(state *ControllerState) http.Handler {
	enrollment := transport.EnrollmentHandler(state.Service)
	guard := newEnrollmentRequestGuard(time.Now)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !guard.admit(request.RemoteAddr) {
			writer.Header().Set(
				"Retry-After",
				strconv.Itoa(int(enrollmentRequestWindow/time.Second)),
			)
			http.Error(writer, "enrollment rate limit exceeded", http.StatusTooManyRequests)
			appendEnrollmentAudit(
				request,
				state,
				guard,
				enrollmentAuditRejected,
				store.AuditActionEnrollmentRejected,
				store.AuditOutcomeRejected,
				store.AuditErrorEnrollmentRateLimited,
			)
			return
		}
		captured := &enrollmentStatusWriter{ResponseWriter: writer}
		enrollment.ServeHTTP(captured, request)
		if captured.status == http.StatusCreated {
			return
		}
		if captured.status == http.StatusServiceUnavailable {
			appendEnrollmentAudit(
				request,
				state,
				guard,
				enrollmentAuditUnavailable,
				store.AuditActionEnrollmentUnavailable,
				store.AuditOutcomeFailed,
				store.AuditErrorEnrollmentUnavailable,
			)
			return
		}
		appendEnrollmentAudit(
			request,
			state,
			guard,
			enrollmentAuditRejected,
			store.AuditActionEnrollmentRejected,
			store.AuditOutcomeRejected,
			store.AuditErrorEnrollmentRejected,
		)
	})
}

func appendEnrollmentAudit(
	request *http.Request,
	state *ControllerState,
	guard *enrollmentRequestGuard,
	kind enrollmentAuditKind,
	action store.AuditAction,
	outcome store.AuditOutcome,
	errorCode store.AuditErrorCode,
) {
	if request == nil || state == nil || state.Store == nil || !guard.claimAudit(kind) {
		return
	}
	auditContext, cancel := detachedManagementProjectionContext(request.Context())
	defer cancel()
	// Aggregate markers deliberately retain no token ID, CSR, path, source
	// address, count, or provider detail. Successful enrollment is recorded
	// atomically by ConsumeEnrollmentWithAudit.
	_, _ = state.Store.AppendAuditEvent(auditContext, store.AuditRecord{
		Actor:        store.AuditActorAnonymous,
		Action:       action,
		Outcome:      outcome,
		ResourceKind: store.AuditResourceController,
		ErrorCode:    errorCode,
		RequestID:    newEnrollmentAuditRequestID(),
	})
}

type enrollmentStatusWriter struct {
	http.ResponseWriter
	status int
}

func (writer *enrollmentStatusWriter) WriteHeader(status int) {
	if writer.status == 0 {
		writer.status = status
	}
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *enrollmentStatusWriter) Write(payload []byte) (int, error) {
	if writer.status == 0 {
		writer.status = http.StatusOK
	}
	return writer.ResponseWriter.Write(payload)
}

func decodeStrictJSON(payload []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func randomMessageID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func embeddedUIHandler() http.Handler {
	assets, err := fs.Sub(webui.Assets, "assets")
	if err != nil {
		return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			http.Error(writer, "embedded UI unavailable", http.StatusServiceUnavailable)
		})
	}
	return http.FileServer(http.FS(assets))
}

func listenerPort(listener net.Listener) (int, error) {
	_, rawPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return 0, err
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("invalid controller listener port")
	}
	return port, nil
}
