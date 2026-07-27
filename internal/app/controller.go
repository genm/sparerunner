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
	"net"
	"net/http"
	"strconv"
	"time"

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
		adminServer = &http.Server{
			Handler:           embeddedUIHandler(),
			ReadHeaderTimeout: readHeaderTimeout,
			MaxHeaderBytes:    http.DefaultMaxHeaderBytes,
		}
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

	serverCount := 1
	if adminServer != nil {
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

func controllerAgentHandler(state *ControllerState) http.Handler {
	enrollment := transport.EnrollmentHandler(state.Service)
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
				return state.AgentBroker.serveSession(ctx, session)
			}
			if err := transport.UpgradeAuthenticatedWithSessions(writer, request, state.Store, handler, state.Sessions); err != nil {
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
