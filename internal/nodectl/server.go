package nodectl

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/genm/sparerunner/internal/domain"
)

// RequestTimeout bounds one same-host exchange. A desktop client that stops
// reading must not hold an Agent goroutine open.
const RequestTimeout = 5 * time.Second

// responseDrainGrace bounds the ordered teardown in write. A client reads its
// response and closes immediately, so the wait is microseconds in practice. The
// bound exists so a client that never closes cannot hold a server goroutine, or
// delay Close, for the whole request timeout.
const responseDrainGrace = 250 * time.Millisecond

// Controller is the Agent-side implementation of the allowlisted operations.
// Status must return observation only. SetIntent and SetTargetExclusion must
// durably record the decision before they return, because a caller that sees a
// new value must be seeing durable state rather than an optimistic guess.
type Controller interface {
	Status(ctx context.Context) (Status, error)
	SetIntent(ctx context.Context, intent domain.AvailabilityIntent, source Source) (Status, error)
	SetTargetExclusion(ctx context.Context, targetID domain.TargetID, excluded bool, source Source) (Status, error)
}

// Authorizer decides whether a kernel-reported peer identity is an authorized
// node owner. It fails closed: an unidentifiable peer is never authorized.
type Authorizer interface {
	Authorize(peer Peer) error
}

// Peer is the OS identity of the connecting process as reported by the kernel,
// not by the client. Exactly one identity field is meaningful per platform: Unix
// reports UID and leaves SID empty, Windows reports SID and leaves UID at -1,
// because the two systems have no common principal type to collapse them into.
type Peer struct {
	UID int
	PID int
	SID string
}

// UIDAllowlist authorizes an explicit set of local user IDs.
type UIDAllowlist struct {
	UIDs map[int]struct{}
}

func NewUIDAllowlist(uids ...int) UIDAllowlist {
	allowlist := UIDAllowlist{UIDs: make(map[int]struct{}, len(uids))}
	for _, uid := range uids {
		if uid >= 0 {
			allowlist.UIDs[uid] = struct{}{}
		}
	}
	return allowlist
}

func (allowlist UIDAllowlist) Authorize(peer Peer) error {
	if peer.UID < 0 || len(allowlist.UIDs) == 0 {
		return ErrUnauthorizedPeer
	}
	if _, allowed := allowlist.UIDs[peer.UID]; !allowed {
		return ErrUnauthorizedPeer
	}
	return nil
}

// SIDAllowlist authorizes an explicit set of Windows security identifiers. It is
// the Windows counterpart of UIDAllowlist: the pipe DACL keeps an unrelated
// local account from reaching the endpoint at all, and this decides which of the
// accounts that can reach it are node owners.
type SIDAllowlist struct {
	SIDs map[string]struct{}
}

// NewSIDAllowlist normalizes case because a SID string is compared, not parsed,
// here. Malformed input is rejected where the DACL is built, so a value that
// cannot name a principal never reaches a listening endpoint.
func NewSIDAllowlist(sids ...string) SIDAllowlist {
	allowlist := SIDAllowlist{SIDs: make(map[string]struct{}, len(sids))}
	for _, sid := range sids {
		if trimmed := strings.TrimSpace(sid); trimmed != "" {
			allowlist.SIDs[strings.ToUpper(trimmed)] = struct{}{}
		}
	}
	return allowlist
}

func (allowlist SIDAllowlist) Authorize(peer Peer) error {
	if peer.SID == "" || len(allowlist.SIDs) == 0 {
		return ErrUnauthorizedPeer
	}
	if _, allowed := allowlist.SIDs[strings.ToUpper(peer.SID)]; !allowed {
		return ErrUnauthorizedPeer
	}
	return nil
}

type ServerOptions struct {
	Controller Controller
	Authorizer Authorizer
	Logger     *slog.Logger
}

type Server struct {
	listener   net.Listener
	controller Controller
	authorizer Authorizer
	logger     *slog.Logger

	closeOnce sync.Once
	wg        sync.WaitGroup
}

// Serve takes ownership of an already-authorized same-host listener. Callers
// obtain it from Listen so the endpoint's ownership and permissions are decided
// in one place.
func Serve(listener net.Listener, options ServerOptions) (*Server, error) {
	if listener == nil || options.Controller == nil || options.Authorizer == nil {
		return nil, ErrInvalidRequest
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	server := &Server{
		listener:   listener,
		controller: options.Controller,
		authorizer: options.Authorizer,
		logger:     logger,
	}
	server.wg.Add(1)
	go server.acceptLoop()
	return server, nil
}

func (server *Server) Close() error {
	var err error
	server.closeOnce.Do(func() {
		err = server.listener.Close()
	})
	server.wg.Wait()
	return err
}

func (server *Server) acceptLoop() {
	defer server.wg.Done()
	for {
		connection, err := server.listener.Accept()
		if err != nil {
			return
		}
		server.wg.Add(1)
		go func() {
			defer server.wg.Done()
			defer connection.Close()
			server.handle(connection)
		}()
	}
}

func (server *Server) handle(connection net.Conn) {
	deadline := time.Now().Add(RequestTimeout)
	_ = connection.SetDeadline(deadline)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	peer, err := PeerIdentity(connection)
	if err == nil {
		err = server.authorizer.Authorize(peer)
	}
	if err != nil {
		// Refusing without a state change is the whole point of this boundary,
		// so the rejection is recorded rather than silently dropped.
		server.logger.Warn(
			"node control peer rejected",
			"error_class", ErrorClassUnauthorizedPeer,
			"peer_uid", peer.UID,
			"peer_pid", peer.PID,
			"peer_sid", peer.SID,
		)
		server.write(connection, failure(ErrUnauthorizedPeer))
		return
	}

	payload, err := readFrame(connection)
	if err != nil {
		server.write(connection, failure(ErrInvalidRequest))
		return
	}
	var request Request
	if err := decodeStrict(payload, &request); err != nil {
		server.write(connection, failure(err))
		return
	}
	if err := request.Validate(); err != nil {
		server.write(connection, failure(err))
		return
	}

	status, err := server.execute(ctx, request)
	if err != nil {
		server.logger.Warn(
			"node control request failed",
			"error_class", errorClassFor(err),
			"operation", string(request.Operation),
			"source", string(request.Source),
		)
		server.write(connection, failure(err))
		return
	}
	status.ProtocolVersion = ProtocolVersion
	server.write(connection, Response{
		ProtocolVersion: ProtocolVersion,
		OK:              true,
		Status:          &status,
	})
}

func (server *Server) execute(ctx context.Context, request Request) (Status, error) {
	switch request.Operation {
	case OperationStatus, OperationTargets:
		// The per-Target view is part of the one status document, so both verbs
		// read the same observation rather than two divergent projections.
		return server.controller.Status(ctx)
	case OperationExclude:
		return server.controller.SetTargetExclusion(ctx, request.TargetID, true, request.Source)
	case OperationInclude:
		return server.controller.SetTargetExclusion(ctx, request.TargetID, false, request.Source)
	case OperationPause:
		return server.controller.SetIntent(ctx, domain.AvailabilityStopped, request.Source)
	case OperationResume:
		return server.controller.SetIntent(ctx, domain.AvailabilityAccepting, request.Source)
	default:
		return Status{}, ErrUnsupportedOperation
	}
}

func (server *Server) write(connection net.Conn, response Response) {
	payload, err := json.Marshal(response)
	if err != nil {
		return
	}
	payload = append(payload, '\n')
	if _, err := connection.Write(payload); err != nil {
		return
	}
	// Closing a Windows named pipe instance discards what is still buffered in
	// it, in both directions. A peer refused before its request was ever read
	// therefore loses the rejection along with its own unread request and sees a
	// timeout instead of unauthorized_peer, which turns a fail-closed verdict
	// into an ambiguous one. A Unix socket keeps the written response readable
	// across the close, which is why only the refusal path exposed this.
	// Consuming the remaining request and letting the client close first makes
	// the teardown ordered on every platform.
	_ = connection.SetReadDeadline(time.Now().Add(responseDrainGrace))
	_, _ = io.Copy(io.Discard, connection)
}

func failure(err error) Response {
	class := errorClassFor(err)
	message := err.Error()
	if class == ErrorClassAgentDegraded {
		// Internal failures are reported by class only. Their text can name
		// local paths and store internals that no desktop client needs.
		message = ErrAgentDegraded.Error()
	}
	return Response{
		ProtocolVersion: ProtocolVersion,
		OK:              false,
		ErrorClass:      class,
		Message:         message,
	}
}

func readFrame(reader io.Reader) ([]byte, error) {
	limited := io.LimitReader(reader, MaxMessageBytes+1)
	buffer := make([]byte, 0, 512)
	chunk := make([]byte, 512)
	for {
		read, err := limited.Read(chunk)
		if read > 0 {
			buffer = append(buffer, chunk[:read]...)
			if len(buffer) > MaxMessageBytes {
				return nil, ErrInvalidRequest
			}
			if index := indexNewline(buffer); index >= 0 {
				return buffer[:index], nil
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(buffer) > 0 {
				return buffer, nil
			}
			return nil, ErrInvalidRequest
		}
	}
}

func indexNewline(buffer []byte) int {
	for index, value := range buffer {
		if value == '\n' {
			return index
		}
	}
	return -1
}
