package enroll

import (
	"context"
	"crypto/subtle"
	"errors"
	"math"
	"sync"
	"time"
)

var (
	ErrTokenNotFound                 = errors.New("enrollment token is unavailable")
	ErrTokenEpochMismatch            = errors.New("enrollment token controller epoch mismatch")
	ErrControllerFingerprintMismatch = errors.New("controller fingerprint mismatch")
	ErrTokenSecretMismatch           = errors.New("enrollment token is unavailable")
	ErrNodeExists                    = errors.New("node already exists")
	ErrCredentialRejected            = errors.New("node credential is not current")
	ErrNodeNotFound                  = errors.New("node not found")
)

// TokenRecord contains no secret. A production Registry must persist this
// digest and its consumption in the same transaction as the node and credential.
type TokenRecord struct {
	ID           [16]byte
	SecretDigest [32]byte
	Epoch        uint64
}

type Credential struct {
	NodeID    string
	Serial    string
	Epoch     uint64
	NotBefore time.Time
	NotAfter  time.Time
}

type NodeRecord struct {
	NodeID           string
	Credential       Credential
	Revoked          bool
	PublicKeyDigest  [32]byte
	CertificateDER   []byte
	CACertificateDER []byte
}

// Registry is the persistence boundary for enrollment. ConsumeEnrollment and
// RenewCredential are required to be single database transactions in the SQLite
// adapter: callers must never observe a consumed token without its credential,
// or a new serial while the former serial remains current.
type Registry interface {
	CreateToken(context.Context, TokenRecord) error
	ConsumeEnrollment(context.Context, TokenRecord, NodeRecord) error
	ReplayEnrollment(context.Context, TokenRecord, [32]byte) (NodeRecord, error)
	FinalizeEnrollment(context.Context, Credential) error
	CancelToken(context.Context, [16]byte) error
	LookupNode(context.Context, string) (NodeRecord, error)
	CurrentCredential(context.Context, string) (Credential, error)
	RenewCredential(context.Context, string, Credential, Credential, time.Time) error
	RevokeNode(context.Context, string) (Credential, error)
	AuthorizeCredential(context.Context, Credential, time.Time) error
}

// MemoryRegistry is a concurrency-safe contract test implementation. It mirrors
// the required atomic mutation boundary, but is not controller persistence.
type MemoryRegistry struct {
	mu           sync.Mutex
	tokens       map[[16]byte]TokenRecord
	nodes        map[string]NodeRecord
	consumed     map[[16]byte]TokenRecord
	consumedNode map[[16]byte]string
}

func NewMemoryRegistry() *MemoryRegistry {
	return &MemoryRegistry{tokens: make(map[[16]byte]TokenRecord), nodes: make(map[string]NodeRecord), consumed: make(map[[16]byte]TokenRecord), consumedNode: make(map[[16]byte]string)}
}

func (registry *MemoryRegistry) CreateToken(_ context.Context, token TokenRecord) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.tokens[token.ID]; exists {
		return ErrTokenNotFound
	}
	registry.tokens[token.ID] = token
	return nil
}

func (registry *MemoryRegistry) ConsumeEnrollment(_ context.Context, supplied TokenRecord, node NodeRecord) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	token, exists := registry.tokens[supplied.ID]
	if !exists {
		return ErrTokenNotFound
	}
	if token.Epoch != supplied.Epoch {
		return ErrTokenEpochMismatch
	}
	if subtle.ConstantTimeCompare(token.SecretDigest[:], supplied.SecretDigest[:]) != 1 {
		return ErrTokenSecretMismatch
	}
	if _, exists := registry.nodes[node.NodeID]; exists {
		return ErrNodeExists
	}
	if node.Credential.NodeID != node.NodeID || node.Credential.Serial == "" || node.Credential.Epoch == 0 {
		return ErrCredentialRejected
	}
	delete(registry.tokens, supplied.ID)
	registry.nodes[node.NodeID] = node
	registry.consumed[supplied.ID] = supplied
	registry.consumedNode[supplied.ID] = node.NodeID
	return nil
}

func (registry *MemoryRegistry) ReplayEnrollment(_ context.Context, supplied TokenRecord, csrDigest [32]byte) (NodeRecord, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	consumed, exists := registry.consumed[supplied.ID]
	if !exists || consumed.Epoch != supplied.Epoch || subtle.ConstantTimeCompare(consumed.SecretDigest[:], supplied.SecretDigest[:]) != 1 {
		return NodeRecord{}, ErrTokenNotFound
	}
	node, found := registry.nodes[registry.consumedNode[supplied.ID]]
	if found && node.PublicKeyDigest == csrDigest && !node.Revoked {
		return node, nil
	}
	return NodeRecord{}, ErrTokenNotFound
}

func (registry *MemoryRegistry) FinalizeEnrollment(_ context.Context, credential Credential) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	node, found := registry.nodes[credential.NodeID]
	if !found || node.Revoked || !sameCredential(node.Credential, credential) {
		return ErrCredentialRejected
	}
	for tokenID, nodeID := range registry.consumedNode {
		if nodeID == credential.NodeID {
			delete(registry.consumedNode, tokenID)
			delete(registry.consumed, tokenID)
		}
	}
	return nil
}

func (registry *MemoryRegistry) CancelToken(_ context.Context, tokenID [16]byte) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.tokens[tokenID]; !exists {
		return ErrTokenNotFound
	}
	delete(registry.tokens, tokenID)
	return nil
}

func (registry *MemoryRegistry) LookupNode(_ context.Context, nodeID string) (NodeRecord, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	node, exists := registry.nodes[nodeID]
	if !exists {
		return NodeRecord{}, ErrNodeNotFound
	}
	return node, nil
}

func (registry *MemoryRegistry) CurrentCredential(ctx context.Context, nodeID string) (Credential, error) {
	node, err := registry.LookupNode(ctx, nodeID)
	if err != nil {
		return Credential{}, err
	}
	if node.Revoked {
		return Credential{}, ErrCredentialRejected
	}
	return node.Credential, nil
}

func (registry *MemoryRegistry) RenewCredential(_ context.Context, nodeID string, expected, replacement Credential, now time.Time) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	node, exists := registry.nodes[nodeID]
	if !exists {
		return ErrNodeNotFound
	}
	if node.Revoked || !sameCredential(node.Credential, expected) || now.Before(expected.NotBefore) || !now.Before(expected.NotAfter) {
		return ErrCredentialRejected
	}
	if replacement.NodeID != nodeID || replacement.Epoch != expected.Epoch || replacement.Serial == expected.Serial || !replacement.NotAfter.After(replacement.NotBefore) {
		return ErrCredentialRejected
	}
	node.Credential = replacement
	registry.nodes[nodeID] = node
	return nil
}

func (registry *MemoryRegistry) RevokeNode(_ context.Context, nodeID string) (Credential, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	node, exists := registry.nodes[nodeID]
	if !exists {
		return Credential{}, ErrNodeNotFound
	}
	if node.Credential.Epoch == math.MaxUint64 {
		return Credential{}, ErrCredentialRejected
	}
	node.Revoked = true
	node.Credential.Epoch++
	registry.nodes[nodeID] = node
	return node.Credential, nil
}

func (registry *MemoryRegistry) AuthorizeCredential(_ context.Context, credential Credential, now time.Time) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	node, exists := registry.nodes[credential.NodeID]
	if !exists || node.Revoked || !sameCredential(node.Credential, credential) || now.Before(credential.NotBefore) || !now.Before(credential.NotAfter) {
		return ErrCredentialRejected
	}
	return nil
}

func sameCredential(left, right Credential) bool {
	return left.NodeID == right.NodeID && left.Serial == right.Serial && left.Epoch == right.Epoch && left.NotBefore.Equal(right.NotBefore) && left.NotAfter.Equal(right.NotAfter)
}
