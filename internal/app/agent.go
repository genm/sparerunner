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
	"github.com/genm/tewake/internal/enroll"
	"github.com/genm/tewake/internal/transport"
)

const (
	DefaultDiscoveryTimeout = 3 * time.Second
	DefaultConnectTimeout   = 30 * time.Second
	DefaultReconnectDelay   = 5 * time.Second
)

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
	StateDirectory    string
	ConnectionTimeout time.Duration
	ReconnectDelay    time.Duration
	Logger            *slog.Logger
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
	connectTimeout := options.ConnectionTimeout
	if connectTimeout <= 0 {
		connectTimeout = DefaultConnectTimeout
	}
	reconnectDelay := options.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = DefaultReconnectDelay
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
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
			err = runAgentSession(ctx, connection, state)
			connection.CloseNow()
		}
		if ctx.Err() != nil {
			return nil
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

func runAgentSession(ctx context.Context, connection *websocket.Conn, state *AgentState) error {
	if err := sendAgentMessage(ctx, connection, transport.MessageHello, struct {
		NodeID string `json:"nodeId"`
	}{NodeID: state.NodeID}); err != nil {
		return err
	}
	if err := sendAgentMessage(ctx, connection, transport.MessageSnapshot, struct {
		NodeID string `json:"nodeId"`
		OS     string `json:"os"`
		Arch   string `json:"arch"`
	}{NodeID: state.NodeID, OS: runtime.GOOS, Arch: runtime.GOARCH}); err != nil {
		return err
	}
	for {
		if _, err := transport.ReadEnvelope(ctx, connection); err != nil {
			return err
		}
	}
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
