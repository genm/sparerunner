// Package app composes Tewake's enrollment, transport, and durable stores into
// executable controller and agent flows. Domain and transport packages remain
// independently testable; this package owns process-level wiring.
package app

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/enroll"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

const (
	controllerIdentityFile = "controller-identity.pem"
	controllerDigestFile   = "enrollment-digest.key"
	controllerSessionFile  = "admin-session.key"
	controllerDatabaseFile = "controller.db"
	agentKeyFile           = "node-private-key.pem"
	agentConfigFile        = "node.json"
	agentDatabaseFile      = "agent.db"
)

var (
	ErrAlreadyInitialized = errors.New("tewake state is already initialized")
	ErrNotInitialized     = errors.New("tewake state is not initialized")
)

type ControllerState struct {
	Directory    string
	Identity     enroll.ControllerIdentity
	Store        *store.ControllerStore
	Service      enroll.Service
	Sessions     *transport.ActiveSessionRegistry
	AgentBroker  *AgentBroker
	AdminSession [32]byte
	Epoch        uint64
}

func (state ControllerState) String() string {
	return fmt.Sprintf("controller-state{directory:%q,epoch:%d,credentials:redacted}", state.Directory, state.Epoch)
}
func (state ControllerState) GoString() string     { return state.String() }
func (state ControllerState) LogValue() slog.Value { return slog.StringValue(state.String()) }
func (state ControllerState) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Directory string `json:"directory"`
		Epoch     uint64 `json:"epoch"`
	}{Directory: state.Directory, Epoch: state.Epoch})
}

// InitializeController publishes a complete state directory atomically. A
// failed initializer removes only its own uniquely-created staging directory.
func InitializeController(ctx context.Context, directory string, hints []string) (string, error) {
	directory, err := absoluteStateDirectory(directory)
	if err != nil {
		return "", err
	}
	if _, err := os.Lstat(directory); err == nil {
		return "", ErrAlreadyInitialized
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	parent := filepath.Dir(directory)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", err
	}
	staging, err := os.MkdirTemp(parent, ".tewake-controller-init-")
	if err != nil {
		return "", err
	}
	if err := os.Chmod(staging, 0o700); err != nil {
		_ = os.RemoveAll(staging)
		return "", err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()

	identity, err := enroll.NewControllerIdentity(timeNow(), rand.Reader)
	if err != nil {
		return "", err
	}
	if err := identity.Save(filepath.Join(staging, controllerIdentityFile)); err != nil {
		return "", err
	}
	digestKey, err := randomSecret()
	if err != nil {
		return "", err
	}
	if err := enroll.SavePrivateMaterial(filepath.Join(staging, controllerDigestFile), digestKey[:]); err != nil {
		return "", err
	}
	adminSession, err := randomSecret()
	if err != nil {
		return "", err
	}
	if err := enroll.SavePrivateMaterial(filepath.Join(staging, controllerSessionFile), adminSession[:]); err != nil {
		return "", err
	}
	controllerStore, err := store.OpenController(ctx, filepath.Join(staging, controllerDatabaseFile), store.Options{})
	if err != nil {
		if controllerStore != nil {
			_ = controllerStore.Close()
		}
		return "", err
	}
	epoch, err := controllerStore.EnrollmentEpoch(ctx)
	if err != nil {
		_ = controllerStore.Close()
		return "", err
	}
	service, err := enroll.NewService(controllerStore, identity, digestKey, uint64(epoch))
	if err != nil {
		_ = controllerStore.Close()
		return "", err
	}
	code, err := service.CreateJoinCode(ctx, hints)
	if closeErr := controllerStore.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	if err := syncDirectory(staging); err != nil {
		return "", err
	}
	if err := publishStateDirectory(staging, directory); err != nil {
		return "", fmt.Errorf("publish controller state: %w", err)
	}
	published = true
	if err := syncDirectory(parent); err != nil {
		return "", err
	}
	return code, nil
}

func OpenController(ctx context.Context, directory string, activate bool) (*ControllerState, error) {
	directory, err := absoluteStateDirectory(directory)
	if err != nil {
		return nil, err
	}
	identity, err := enroll.LoadControllerIdentity(filepath.Join(directory, controllerIdentityFile))
	if err != nil {
		return nil, fmt.Errorf("%w: controller identity: %v", ErrNotInitialized, err)
	}
	digestBytes, err := enroll.LoadPrivateMaterial(filepath.Join(directory, controllerDigestFile))
	if err != nil || len(digestBytes) != 32 {
		return nil, fmt.Errorf("%w: controller enrollment key", ErrNotInitialized)
	}
	var digestKey [32]byte
	copy(digestKey[:], digestBytes)
	sessionBytes, err := enroll.LoadPrivateMaterial(filepath.Join(directory, controllerSessionFile))
	if err != nil || len(sessionBytes) != 32 {
		return nil, fmt.Errorf("%w: controller admin session", ErrNotInitialized)
	}
	var adminSession [32]byte
	copy(adminSession[:], sessionBytes)
	controllerStore, err := store.OpenController(ctx, filepath.Join(directory, controllerDatabaseFile), store.Options{})
	if err != nil {
		if controllerStore != nil {
			_ = controllerStore.Close()
		}
		return nil, err
	}
	var epoch uint64
	if activate {
		active, advanceErr := controllerStore.AdvanceEpoch(ctx)
		if advanceErr != nil {
			_ = controllerStore.Close()
			return nil, advanceErr
		}
		epoch = uint64(active)
	} else {
		current, currentErr := controllerStore.EnrollmentEpoch(ctx)
		if currentErr != nil {
			_ = controllerStore.Close()
			return nil, currentErr
		}
		epoch = uint64(current)
	}
	service, err := enroll.NewService(controllerStore, identity, digestKey, epoch)
	if err != nil {
		_ = controllerStore.Close()
		return nil, err
	}
	sessions := transport.NewActiveSessionRegistry()
	controllerStore.SetCredentialRevocationHook(sessions.Revoke)
	return &ControllerState{
		Directory:    directory,
		Identity:     identity,
		Store:        controllerStore,
		Service:      service,
		Sessions:     sessions,
		AgentBroker:  NewAgentBroker(domain.ControllerEpoch(epoch), newStoreBackedAgentConsumers(controllerStore)),
		AdminSession: adminSession,
		Epoch:        epoch,
	}, nil
}

func (state *ControllerState) Close() error {
	if state == nil || state.Store == nil {
		return nil
	}
	return state.Store.Close()
}

func (state *ControllerState) CreateJoinCode(ctx context.Context, hints []string) (string, error) {
	if state == nil {
		return "", ErrNotInitialized
	}
	return state.Service.CreateJoinCode(ctx, hints)
}

type AgentState struct {
	Directory      string
	Store          *store.AgentStore
	PrivateKey     ed25519.PrivateKey
	NodeID         string
	Endpoint       string
	CertificateDER []byte
	CADER          []byte
}

func (state AgentState) String() string {
	return fmt.Sprintf("agent-state{directory:%q,node:%q,endpoint:%q,credentials:redacted}", state.Directory, state.NodeID, state.Endpoint)
}
func (state AgentState) GoString() string     { return state.String() }
func (state AgentState) LogValue() slog.Value { return slog.StringValue(state.String()) }
func (state AgentState) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Directory string `json:"directory"`
		NodeID    string `json:"nodeId,omitempty"`
		Endpoint  string `json:"endpoint,omitempty"`
	}{Directory: state.Directory, NodeID: state.NodeID, Endpoint: state.Endpoint})
}

type agentConfig struct {
	Version     int    `json:"version"`
	NodeID      string `json:"nodeId"`
	Endpoint    string `json:"endpoint"`
	Certificate string `json:"certificate"`
	CA          string `json:"ca"`
}

func OpenAgent(ctx context.Context, directory string) (*AgentState, error) {
	directory, err := absoluteStateDirectory(directory)
	if err != nil {
		return nil, err
	}
	key, err := enroll.LoadNodePrivateKey(filepath.Join(directory, agentKeyFile))
	if err != nil {
		return nil, fmt.Errorf("%w: node private key: %v", ErrNotInitialized, err)
	}
	configBytes, err := enroll.LoadPrivateMaterial(filepath.Join(directory, agentConfigFile))
	if err != nil {
		return nil, fmt.Errorf("%w: node configuration: %v", ErrNotInitialized, err)
	}
	config, certificateDER, caDER, err := decodeAgentConfig(configBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: node configuration", ErrNotInitialized)
	}
	if _, err := transport.NodeTLSCertificate(key, certificateDER, caDER); err != nil {
		return nil, fmt.Errorf("%w: node credential: %v", ErrNotInitialized, err)
	}
	agentStore, err := store.OpenAgent(ctx, filepath.Join(directory, agentDatabaseFile), store.Options{})
	if err != nil {
		if agentStore != nil {
			_ = agentStore.Close()
		}
		return nil, err
	}
	return &AgentState{
		Directory:      directory,
		Store:          agentStore,
		PrivateKey:     key,
		NodeID:         config.NodeID,
		Endpoint:       config.Endpoint,
		CertificateDER: certificateDER,
		CADER:          caDER,
	}, nil
}

func (state *AgentState) Close() error {
	if state == nil || state.Store == nil {
		return nil
	}
	return state.Store.Close()
}

func prepareAgent(ctx context.Context, directory string) (*AgentState, bool, error) {
	directory, err := absoluteStateDirectory(directory)
	if err != nil {
		return nil, false, err
	}
	if err := ensurePrivateStateDirectory(directory); err != nil {
		return nil, false, err
	}
	keyPath := filepath.Join(directory, agentKeyFile)
	key, err := enroll.LoadNodePrivateKey(keyPath)
	switch {
	case err == nil:
	case errors.Is(err, os.ErrNotExist):
		key, err = enroll.GenerateAndPersistNodeKey(keyPath, rand.Reader)
	default:
		return nil, false, err
	}
	if err != nil {
		return nil, false, err
	}
	agentStore, err := store.OpenAgent(ctx, filepath.Join(directory, agentDatabaseFile), store.Options{})
	if err != nil {
		if agentStore != nil {
			_ = agentStore.Close()
		}
		return nil, false, err
	}
	state := &AgentState{Directory: directory, Store: agentStore, PrivateKey: key}
	configBytes, configErr := enroll.LoadPrivateMaterial(filepath.Join(directory, agentConfigFile))
	if errors.Is(configErr, os.ErrNotExist) {
		return state, false, nil
	}
	if configErr != nil {
		_ = agentStore.Close()
		return nil, false, configErr
	}
	config, certificateDER, caDER, err := decodeAgentConfig(configBytes)
	if err != nil {
		_ = agentStore.Close()
		return nil, false, err
	}
	if _, err := transport.NodeTLSCertificate(key, certificateDER, caDER); err != nil {
		_ = agentStore.Close()
		return nil, false, err
	}
	state.NodeID = config.NodeID
	state.Endpoint = config.Endpoint
	state.CertificateDER = certificateDER
	state.CADER = caDER
	return state, true, nil
}

func persistAgentConfig(state *AgentState) error {
	config := agentConfig{
		Version:     1,
		NodeID:      state.NodeID,
		Endpoint:    state.Endpoint,
		Certificate: base64.RawStdEncoding.EncodeToString(state.CertificateDER),
		CA:          base64.RawStdEncoding.EncodeToString(state.CADER),
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return err
	}
	path := filepath.Join(state.Directory, agentConfigFile)
	if err := enroll.SavePrivateMaterial(path, encoded); err != nil {
		existing, loadErr := enroll.LoadPrivateMaterial(path)
		if loadErr != nil || !bytes.Equal(existing, encoded) {
			return err
		}
	}
	return syncDirectory(state.Directory)
}

func decodeAgentConfig(payload []byte) (agentConfig, []byte, []byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var config agentConfig
	if err := decoder.Decode(&config); err != nil {
		return agentConfig{}, nil, nil, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return agentConfig{}, nil, nil, err
	}
	if config.Version != 1 || config.NodeID == "" {
		return agentConfig{}, nil, nil, errors.New("invalid node configuration")
	}
	endpoint, err := canonicalControllerEndpoint(config.Endpoint, "wss")
	if err != nil || endpoint != config.Endpoint {
		return agentConfig{}, nil, nil, errors.New("invalid node endpoint")
	}
	certificateDER, err := decodeCanonicalBase64(config.Certificate)
	if err != nil {
		return agentConfig{}, nil, nil, err
	}
	caDER, err := decodeCanonicalBase64(config.CA)
	if err != nil {
		return agentConfig{}, nil, nil, err
	}
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		return agentConfig{}, nil, nil, err
	}
	nodeID, _, _, err := enroll.NodeCredentialIdentity(certificate)
	if err != nil || nodeID != config.NodeID {
		return agentConfig{}, nil, nil, errors.New("node configuration identity mismatch")
	}
	return config, certificateDER, caDER, nil
}

func absoluteStateDirectory(directory string) (string, error) {
	if directory == "" {
		return "", errors.New("state directory is required")
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return "", err
	}
	return filepath.Clean(absolute), nil
}

func ensurePrivateStateDirectory(directory string) error {
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(directory, 0o700)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("state directory is unsafe")
	}
	return privateStateDirectoryPlatform(info)
}

func randomSecret() ([32]byte, error) {
	var secret [32]byte
	_, err := io.ReadFull(rand.Reader, secret[:])
	return secret, err
}

func canonicalControllerEndpoint(raw, scheme string) (string, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Scheme != scheme || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return "", errors.New("invalid controller endpoint")
	}
	endpoint.Path = ""
	return endpoint.String(), nil
}

func decodeCanonicalBase64(raw string) ([]byte, error) {
	decoded, err := base64.RawStdEncoding.DecodeString(raw)
	if err != nil || len(decoded) == 0 || base64.RawStdEncoding.EncodeToString(decoded) != raw {
		return nil, errors.New("invalid encoded node material")
	}
	return decoded, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

var timeNow = time.Now
