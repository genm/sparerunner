package app

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/enroll"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

const (
	DefaultDiscoveryTimeout       = 3 * time.Second
	DefaultConnectTimeout         = 30 * time.Second
	DefaultReconnectDelay         = 5 * time.Second
	DefaultAgentHeartbeatInterval = time.Second
	DefaultAgentReadinessTimeout  = 500 * time.Millisecond
)

var ErrAgentRuntimeDegraded = errors.New("agent runner runtime is degraded")

type JoinOptions struct {
	StateDirectory    string
	JoinCode          string
	Controller        string
	Discoverer        transport.Discoverer
	DiscoveryTimeout  time.Duration
	ConnectionTimeout time.Duration
}

func (options JoinOptions) String() string {
	return fmt.Sprintf("join-options{state:%q,controller:%q,join-code:redacted}", options.StateDirectory, options.Controller)
}
func (options JoinOptions) GoString() string     { return options.String() }
func (options JoinOptions) LogValue() slog.Value { return slog.StringValue(options.String()) }
func (options JoinOptions) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		StateDirectory    string        `json:"stateDirectory"`
		Controller        string        `json:"controller,omitempty"`
		DiscoveryTimeout  time.Duration `json:"discoveryTimeout"`
		ConnectionTimeout time.Duration `json:"connectionTimeout"`
	}{
		StateDirectory:    options.StateDirectory,
		Controller:        options.Controller,
		DiscoveryTimeout:  options.DiscoveryTimeout,
		ConnectionTimeout: options.ConnectionTimeout,
	})
}

func JoinAgent(ctx context.Context, options JoinOptions) (string, error) {
	code, err := enroll.DecodeJoinCode(options.JoinCode)
	if err != nil {
		return "", err
	}
	state, configured, err := prepareAgent(ctx, options.StateDirectory)
	if err != nil {
		return "", err
	}
	defer state.Close()
	timeout := options.ConnectionTimeout
	if timeout <= 0 {
		timeout = DefaultConnectTimeout
	}
	if configured {
		ca, err := x509.ParseCertificate(state.CADER)
		if err != nil || sha256.Sum256(ca.Raw) != code.CAFingerprint() {
			return "", errors.New("existing node belongs to a different controller")
		}
		endpoint := state.Endpoint
		if options.Controller != "" {
			secure, err := canonicalControllerEndpoint(options.Controller, "https")
			if err != nil {
				return "", err
			}
			endpoint = websocketEndpoint(secure)
		}
		confirmContext, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if err := confirmAgent(confirmContext, state, endpoint); err != nil {
			return "", err
		}
		return state.NodeID, nil
	}

	csr, err := enroll.CreateNodeCSR(state.PrivateKey)
	if err != nil {
		return "", err
	}
	candidates, err := enrollmentCandidates(ctx, code, options)
	if err != nil {
		return "", err
	}
	var response transport.EnrollmentResponse
	var successfulEndpoint string
	for _, candidate := range candidates {
		attemptContext, cancel := context.WithTimeout(ctx, timeout)
		response, err = (transport.EnrollmentClient{}).Enroll(attemptContext, candidate, options.JoinCode, csr)
		cancel()
		if err == nil {
			successfulEndpoint = candidate
			break
		}
	}
	if successfulEndpoint == "" {
		return "", errors.New("no controller candidate completed pinned enrollment")
	}
	state.NodeID = response.NodeID
	state.Endpoint = websocketEndpoint(successfulEndpoint)
	state.CertificateDER = response.CertificateDER
	state.CADER = response.CACertificateDER
	if err := persistAgentConfig(state); err != nil {
		return "", err
	}
	confirmContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := confirmAgent(confirmContext, state, state.Endpoint); err != nil {
		return "", err
	}
	return state.NodeID, nil
}

type AgentServeOptions struct {
	StateDirectory string
	// LocalControl serves the node-local availability endpoint for the tray,
	// launcher, and CLI. It is disabled by default so an unconfigured owner
	// identity never widens the Agent's local surface implicitly.
	LocalControl      AgentLocalControlOptions
	ConnectionTimeout time.Duration
	ReconnectDelay    time.Duration
	HeartbeatInterval time.Duration
	ReadinessTimeout  time.Duration
	Logger            *slog.Logger
	CommandRuntime    func(context.Context, *AgentState) (*AgentCommandRuntime, error)
}

func ServeAgent(ctx context.Context, options AgentServeOptions) error {
	state, err := OpenAgent(ctx, options.StateDirectory)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer state.Close()
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	availability, err := newAgentAvailability(ctx, state.Store, domain.NodeID(state.NodeID))
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if options.LocalControl.Enabled {
		control, err := startAgentLocalControl(state.Directory, availability, options.LocalControl, logger)
		if err != nil {
			// The owner's control surface is part of this node's contract. A
			// half-open endpoint would silently strand the tray, so startup
			// fails rather than running without it.
			return err
		}
		defer control.Close()
	}
	var commandRuntime *AgentCommandRuntime
	if options.CommandRuntime != nil {
		commandRuntime, err = options.CommandRuntime(ctx, state)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if commandRuntime == nil {
			// Optional native mode is an explicit degraded state: the Agent may
			// remain reachable for diagnostics, but every snapshot and heartbeat
			// advertises zero native capacity until a service restart can rebuild
			// the local ownership boundary.
			logger.Warn(
				"agent native runner unavailable",
				"state", "degraded",
				"error_class", "native_runner_unavailable",
			)
		} else {
			if err := commandRuntime.Start(ctx); err != nil {
				return err
			}
		}
	}
	connectTimeout := options.ConnectionTimeout
	if connectTimeout <= 0 {
		connectTimeout = DefaultConnectTimeout
	}
	reconnectDelay := options.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = DefaultReconnectDelay
	}
	offline := false
	connectedOnce := false
	for {
		connectContext, cancel := context.WithTimeout(ctx, connectTimeout)
		connection, err := dialAgent(connectContext, state, state.Endpoint)
		cancel()
		if err == nil {
			if offline || !connectedOnce {
				logger.Info("agent controller connection established", "state", "online", "node_id", state.NodeID)
			}
			offline = false
			connectedOnce = true
			availability.setConnected(true)
			err = runAgentSessionWithOptions(ctx, connection, state, commandRuntime, agentSessionOptions{
				heartbeatInterval: options.HeartbeatInterval,
				readinessTimeout:  options.ReadinessTimeout,
				availability:      availability,
			})
			availability.setConnected(false)
			connection.CloseNow()
		}
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, ErrAgentRuntimeDegraded) {
			// Reconnecting cannot repair a local journal/outbox authority
			// failure. Exit so the service manager can restart into the durable
			// startup recovery path.
			return ErrAgentRuntimeDegraded
		}
		if !offline {
			logger.Warn("agent controller connection degraded", "state", "offline", "error_class", "controller_connection_failed")
			offline = true
		}
		timer := time.NewTimer(reconnectDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func confirmAgent(ctx context.Context, state *AgentState, endpoint string) error {
	connection, err := dialAgent(ctx, state, endpoint)
	if err != nil {
		return errors.New("controller rejected node confirmation")
	}
	defer connection.CloseNow()
	return sendAgentMessage(ctx, connection, transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: state.NodeID})
}

func runAgentSession(ctx context.Context, connection *websocket.Conn, state *AgentState, commandRuntime *AgentCommandRuntime) error {
	return runAgentSessionWithOptions(ctx, connection, state, commandRuntime, agentSessionOptions{})
}

type agentSessionOptions struct {
	heartbeatInterval time.Duration
	readinessTimeout  time.Duration
	availability      *agentAvailability
}

func (options agentSessionOptions) normalized() agentSessionOptions {
	if options.heartbeatInterval <= 0 {
		options.heartbeatInterval = DefaultAgentHeartbeatInterval
	}
	if options.readinessTimeout <= 0 {
		options.readinessTimeout = DefaultAgentReadinessTimeout
	}
	return options
}

func runAgentSessionWithOptions(
	ctx context.Context,
	connection *websocket.Conn,
	state *AgentState,
	commandRuntime *AgentCommandRuntime,
	options agentSessionOptions,
) error {
	options = options.normalized()
	if err := sendAgentMessage(ctx, connection, transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: state.NodeID}); err != nil {
		return err
	}
	runnerVersion := runner.OfficialRunnerVersion
	if commandRuntime != nil {
		runnerVersion = commandRuntime.RunnerVersion()
	}
	nativeReady := probeAgentRuntimeReadiness(ctx, commandRuntime, options.readinessTimeout)
	intent := domain.AvailabilityAccepting
	var excludedTargets []domain.TargetID
	if options.availability != nil {
		options.availability.setNativeReady(nativeReady)
		intent = options.availability.Intent()
		excludedTargets = options.availability.ExcludedTargets()
	}
	snapshot, err := buildAgentSnapshot(
		ctx,
		state,
		runnerVersion,
		// Capacity is the conjunction of native readiness and the owner's
		// intent, so a stopped computer advertises nothing even when its
		// runtime is perfectly healthy.
		nativeReady && intent.Accepts(),
		intent,
		excludedTargets,
	)
	if err != nil {
		return ErrAgentRuntimeDegraded
	}
	if err := sendAgentMessage(ctx, connection, transport.MessageSnapshot, snapshot); err != nil {
		return err
	}
	return runAgentSessionActor(ctx, connection, domain.NodeID(state.NodeID), commandRuntime, options)
}

func probeAgentRuntimeReadiness(
	ctx context.Context,
	commandRuntime *AgentCommandRuntime,
	timeout time.Duration,
) bool {
	if commandRuntime == nil {
		return false
	}
	probeContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return commandRuntime.Ready(probeContext)
}

func buildAgentSnapshot(
	ctx context.Context,
	state *AgentState,
	runnerVersion string,
	nativeRunnerReady bool,
	availabilityIntent domain.AvailabilityIntent,
	excludedTargets []domain.TargetID,
) (transport.AgentSnapshot, error) {
	if state == nil || state.Store == nil || state.NodeID == "" ||
		runnerVersion == "" {
		return transport.AgentSnapshot{}, transport.ErrInvalidCommand
	}
	journal, err := state.Store.Snapshot(ctx)
	if err != nil {
		return transport.AgentSnapshot{}, err
	}
	snapshot := transport.AgentSnapshot{
		NodeID:             domain.NodeID(state.NodeID),
		RunnerVersion:      runnerVersion,
		NativeRunnerReady:  nativeRunnerReady,
		AvailabilityIntent: availabilityIntent,
		// The owner's exclusion set travels on every snapshot so the controller
		// adopts it in the same transaction that records the snapshot, before
		// any capacity is advertised after a reconnect.
		ExcludedTargets:    excludedTargets,
		MaxControllerEpoch: journal.MaxControllerEpoch,
		Commands:           journal.Commands,
	}
	snapshot.OS, snapshot.Arch, err = agentPlatform(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return transport.AgentSnapshot{}, err
	}
	for _, observation := range journal.Observations {
		snapshot.Observations = append(snapshot.Observations, transport.AgentExecutionObservation{
			ExecutionID:        observation.ExecutionID,
			State:              observation.State,
			ObservedAtUnixNano: observation.ObservedAtUnixNano,
		})
	}
	for _, tombstone := range journal.CleanupTombstones {
		snapshot.CleanupTombstones = append(snapshot.CleanupTombstones, transport.AgentCleanupTombstone{
			ExecutionID:        tombstone.ExecutionID,
			FailureCode:        tombstone.FailureCode,
			RecordedAtUnixNano: tombstone.RecordedAtUnixNano,
		})
	}
	if err := snapshot.Validate(); err != nil {
		return transport.AgentSnapshot{}, err
	}
	return snapshot, nil
}

func agentPlatform(goos, goarch string) (domain.OperatingSystem, domain.Architecture, error) {
	var operatingSystem domain.OperatingSystem
	switch goos {
	case "linux":
		operatingSystem = domain.OSLinux
	case "darwin":
		operatingSystem = domain.OSMacOS
	case "windows":
		operatingSystem = domain.OSWindows
	default:
		return "", "", runner.ErrUnsupportedPlatform
	}
	var architecture domain.Architecture
	switch goarch {
	case "amd64":
		architecture = domain.ArchAMD64
	case "arm64":
		architecture = domain.ArchARM64
	default:
		return "", "", runner.ErrUnsupportedPlatform
	}
	return operatingSystem, architecture, nil
}

type agentSessionRead struct {
	envelope transport.Envelope
	err      error
}

func runAgentSessionActor(
	ctx context.Context,
	connection *websocket.Conn,
	nodeID domain.NodeID,
	commandRuntime *AgentCommandRuntime,
	options agentSessionOptions,
) error {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeat := time.NewTimer(options.heartbeatInterval)
	defer heartbeat.Stop()
	reads := make(chan agentSessionRead, 1)
	go func() {
		for {
			envelope, err := transport.ReadEnvelope(sessionCtx, connection)
			select {
			case reads <- agentSessionRead{envelope: envelope, err: err}:
			case <-sessionCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	awaitingUpdateAck := ""
	awaitingHeartbeatAck := ""
	inFlightIntent := domain.AvailabilityAccepting
	for {
		if commandRuntime != nil && awaitingUpdateAck == "" {
			pending, err := commandRuntime.PendingUpdates(ctx)
			if err != nil {
				return ErrAgentRuntimeDegraded
			}
			if len(pending) > 0 {
				update := transportExecutionUpdate(pending[0].Update)
				payload, err := transport.EncodeExecutionUpdate(update)
				if err != nil {
					return errors.New("agent execution update is invalid")
				}
				writeErr := transport.WriteEnvelope(sessionCtx, connection, transport.Envelope{
					ProtocolVersion: transport.ProtocolVersion,
					MessageID:       pending[0].MessageID,
					Type:            transport.MessageExecutionUpdate,
					Payload:         payload,
				})
				clear(payload)
				if writeErr != nil {
					return writeErr
				}
				awaitingUpdateAck = pending[0].MessageID
			}
		}

		var updateReady <-chan struct{}
		if commandRuntime != nil {
			updateReady = commandRuntime.UpdateReady()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeat.C:
			if awaitingHeartbeatAck != "" {
				return errors.New("controller heartbeat acknowledgement missing")
			}
			heartbeatNativeReady := probeAgentRuntimeReadiness(
				ctx,
				commandRuntime,
				options.readinessTimeout,
			)
			heartbeatIntent := domain.AvailabilityAccepting
			var heartbeatExcluded []domain.TargetID
			if options.availability != nil {
				options.availability.setNativeReady(heartbeatNativeReady)
				heartbeatIntent = options.availability.Intent()
				heartbeatExcluded = options.availability.ExcludedTargets()
			}
			payload, err := transport.EncodeAgentHeartbeat(transport.AgentHeartbeat{
				NodeID:             nodeID,
				NativeRunnerReady:  heartbeatNativeReady && heartbeatIntent.Accepts(),
				AvailabilityIntent: heartbeatIntent,
				// An exclusion made while connected reaches the controller at
				// heartbeat cadence rather than waiting for the next reconnect.
				ExcludedTargets: heartbeatExcluded,
			})
			if err != nil {
				return errors.New("agent heartbeat is invalid")
			}
			messageID, err := randomMessageID()
			if err != nil {
				clear(payload)
				return err
			}
			writeErr := transport.WriteEnvelope(sessionCtx, connection, transport.Envelope{
				ProtocolVersion: transport.ProtocolVersion,
				MessageID:       messageID,
				Type:            transport.MessageHeartbeat,
				Payload:         payload,
			})
			clear(payload)
			if writeErr != nil {
				return writeErr
			}
			awaitingHeartbeatAck = messageID
			inFlightIntent = heartbeatIntent
			// The ACK gets a full heartbeat interval even when the live
			// readiness probe itself was slow.
			heartbeat.Reset(options.heartbeatInterval)
		case <-updateReady:
			continue
		case read := <-reads:
			if read.err != nil {
				return read.err
			}
			envelope := read.envelope
			// Command identities are exact-deduplicated by the durable command
			// journal, while ACKs are matched to the single in-flight update.
			// Keeping every envelope ID for the lifetime of a healthy Agent
			// session would otherwise grow memory once per completed job.
			switch envelope.Type {
			case transport.MessagePrepare, transport.MessageStart, transport.MessageCancel:
				if commandRuntime == nil {
					clear(envelope.Payload)
					return errors.New("native runner is unavailable")
				}
				if err := dispatchAgentCommand(ctx, options.readinessTimeout, commandRuntime, &envelope, func(messageID string) error {
					return writeAgentAck(sessionCtx, connection, messageID)
				}); err != nil {
					return err
				}
			case transport.MessageAck:
				var acknowledgement struct {
					MessageID string `json:"messageId"`
					// EligibleTargets rides the heartbeat's own ack rather than a
					// separate message type. A nil field means "no refresh, keep
					// the previously known list"; a non-nil-but-empty field means
					// "the controller confirmed zero eligible targets". Omitting
					// this field previously made the whole session die the first
					// time a heartbeat ack carried any eligible targets at all,
					// because decodeStrictJSON disallows unknown fields.
					EligibleTargets []transport.EligibleTarget `json:"eligibleTargets"`
				}
				if err := decodeStrictJSON(envelope.Payload, &acknowledgement); err != nil {
					clear(envelope.Payload)
					return errors.New("controller acknowledgement mismatch")
				}
				clear(envelope.Payload)
				matchesHeartbeat := awaitingHeartbeatAck != "" &&
					acknowledgement.MessageID == awaitingHeartbeatAck
				matchesUpdate := awaitingUpdateAck != "" &&
					acknowledgement.MessageID == awaitingUpdateAck
				if matchesHeartbeat == matchesUpdate {
					return errors.New("controller acknowledgement mismatch")
				}
				if matchesHeartbeat {
					awaitingHeartbeatAck = ""
					// The Controller has now observed this exact intent, which
					// is the only evidence that lets a resume stop reporting as
					// pending.
					if options.availability != nil {
						options.availability.confirm(inFlightIntent)
					}
					if acknowledgement.EligibleTargets != nil {
						// A malformed list fails the session rather than silently
						// dropping it: an Agent must never display a corrupted
						// eligible-target set to the owner.
						if err := transport.ValidateEligibleTargets(acknowledgement.EligibleTargets); err != nil {
							return errors.New("controller acknowledgement mismatch")
						}
						if options.availability != nil {
							options.availability.setEligibleTargets(acknowledgement.EligibleTargets, true)
						}
					}
					continue
				}
				if commandRuntime == nil {
					return errors.New("controller acknowledgement mismatch")
				}
				if err := commandRuntime.AcknowledgeUpdate(ctx, awaitingUpdateAck); err != nil {
					return ErrAgentRuntimeDegraded
				}
				awaitingUpdateAck = ""
			default:
				clear(envelope.Payload)
				return errors.New("controller sent an unsupported agent command")
			}
		}
	}
}

func dispatchAgentCommand(
	ctx context.Context,
	readinessTimeout time.Duration,
	runtime *AgentCommandRuntime,
	envelope *transport.Envelope,
	acknowledge func(string) error,
) error {
	if runtime == nil || acknowledge == nil {
		return transport.ErrInvalidCommand
	}
	messageID := envelope.MessageID
	accepted, err := runtime.accept(ctx, readinessTimeout, envelope)
	if err != nil {
		return errors.New("controller command was rejected")
	}
	// A durable command identity alone is not proof that a one-shot JIT crossed
	// the platform fence. Execute synchronously to a durable lifecycle outbox
	// result before ACK; startup reconciliation resolves a process crash in the
	// remaining commit-to-exec window without persisting the secret.
	update, executeErr := accepted.Execute(ctx)
	pending, pendingErr := runtime.PendingUpdates(ctx)
	if pendingErr != nil || !containsExecutionUpdate(pending, storeExecutionUpdate(update)) {
		return ErrAgentRuntimeDegraded
	}
	// Classified runner failures are delivered through the durable update. They
	// still acknowledge command admission; only missing durable evidence rejects
	// the transport command.
	_ = executeErr
	return acknowledge(messageID)
}

func containsExecutionUpdate(
	pending []store.PendingExecutionUpdate,
	want store.ExecutionUpdateRecord,
) bool {
	for _, update := range pending {
		if update.Update == want {
			return true
		}
	}
	return false
}

func writeAgentAck(ctx context.Context, connection *websocket.Conn, acknowledgedMessageID string) error {
	payload, err := json.Marshal(struct {
		MessageID string `json:"messageId"`
	}{MessageID: acknowledgedMessageID})
	if err != nil {
		return err
	}
	messageID, err := randomMessageID()
	if err != nil {
		return err
	}
	return transport.WriteEnvelope(ctx, connection, transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       messageID,
		Type:            transport.MessageAck,
		Payload:         payload,
	})
}

func dialAgent(ctx context.Context, state *AgentState, endpoint string) (*websocket.Conn, error) {
	certificate, err := transport.NodeTLSCertificate(state.PrivateKey, state.CertificateDER, state.CADER)
	if err != nil {
		return nil, err
	}
	ca, err := x509.ParseCertificate(state.CADER)
	if err != nil {
		return nil, err
	}
	config, err := transport.NodeClientTLSConfig(certificate, ca)
	if err != nil {
		return nil, err
	}
	connection, _, err := transport.DialNodeWSS(ctx, endpoint, config)
	return connection, err
}

func sendAgentMessage(ctx context.Context, connection *websocket.Conn, messageType transport.MessageType, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	messageID, err := randomMessageID()
	if err != nil {
		return err
	}
	if err := transport.WriteEnvelope(ctx, connection, transport.Envelope{
		ProtocolVersion: transport.ProtocolVersion,
		MessageID:       messageID,
		Type:            messageType,
		Payload:         payload,
	}); err != nil {
		return err
	}
	ack, err := transport.ReadEnvelope(ctx, connection)
	if err != nil || ack.Type != transport.MessageAck {
		return errors.New("controller did not acknowledge agent message")
	}
	var ackPayload struct {
		MessageID string `json:"messageId"`
		// EligibleTargets may arrive on this ack path too (the controller does
		// not distinguish it from a heartbeat ack encoder). It is harmless here:
		// this path only confirms the Hello/Snapshot ack, so the field is
		// accepted and ignored rather than routed anywhere.
		EligibleTargets []transport.EligibleTarget `json:"eligibleTargets"`
	}
	if err := decodeStrictJSON(ack.Payload, &ackPayload); err != nil || ackPayload.MessageID != messageID {
		return errors.New("controller acknowledgement mismatch")
	}
	return nil
}

func enrollmentCandidates(ctx context.Context, code enroll.JoinCode, options JoinOptions) ([]string, error) {
	if options.Controller != "" {
		endpoint, err := canonicalControllerEndpoint(options.Controller, "https")
		if err != nil {
			return nil, err
		}
		return []string{endpoint}, nil
	}
	set := make(map[string]struct{})
	for _, hint := range code.EndpointHints() {
		endpoint := hint
		if !strings.HasPrefix(endpoint, "https://") {
			endpoint = "https://" + endpoint
		}
		if canonical, err := canonicalControllerEndpoint(endpoint, "https"); err == nil {
			set[canonical] = struct{}{}
		}
	}
	discoverer := options.Discoverer
	if discoverer == nil {
		timeout := options.DiscoveryTimeout
		if timeout <= 0 {
			timeout = DefaultDiscoveryTimeout
		}
		discoverer = transport.MDNSDiscoverer{Timeout: timeout}
	}
	candidates, discoveryErr := discoverer.Discover(ctx)
	if discoveryErr == nil {
		for _, candidate := range candidates {
			if canonical, err := canonicalControllerEndpoint("https://"+candidate.Address, "https"); err == nil {
				set[canonical] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(set))
	for endpoint := range set {
		result = append(result, endpoint)
	}
	sort.Strings(result)
	if len(result) == 0 {
		if discoveryErr != nil {
			return nil, errors.New("controller discovery failed")
		}
		return nil, errors.New("controller discovery returned no candidates")
	}
	return result, nil
}

func websocketEndpoint(enrollmentEndpoint string) string {
	endpoint, _ := url.Parse(enrollmentEndpoint)
	endpoint.Scheme = "wss"
	endpoint.Path = ""
	endpoint.RawPath = ""
	return endpoint.String()
}
