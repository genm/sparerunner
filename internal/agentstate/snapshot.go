// Package agentstate owns the canonical, secret-free Agent snapshot identity
// shared by the transport and SQLite adapters. Keeping the digest wire shape
// below both layers prevents either adapter from becoming the other's
// dependency merely to prove an exact snapshot.
package agentstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/genm/tewake/internal/domain"
)

type Observation struct {
	ExecutionID        domain.ExecutionID    `json:"executionId"`
	State              domain.ExecutionState `json:"state"`
	ObservedAtUnixNano int64                 `json:"observedAtUnixNano"`
}

type CleanupTombstone struct {
	ExecutionID        domain.ExecutionID        `json:"executionId"`
	FailureCode        domain.CleanupFailureCode `json:"failureCode"`
	RecordedAtUnixNano int64                     `json:"recordedAtUnixNano"`
}

type Snapshot struct {
	NodeID             domain.NodeID          `json:"nodeId"`
	OS                 domain.OperatingSystem `json:"os"`
	Arch               domain.Architecture    `json:"arch"`
	RunnerVersion      string                 `json:"runnerVersion"`
	MaxControllerEpoch domain.ControllerEpoch `json:"maxControllerEpoch"`
	Commands           []domain.Command       `json:"commands"`
	Observations       []Observation          `json:"observations"`
	CleanupTombstones  []CleanupTombstone     `json:"cleanupTombstones"`
}

func Digest(snapshot Snapshot) (string, error) {
	// JSON distinguishes nil slices (null) from empty slices ([]), but the two
	// adapters construct the same empty journal through different Go paths.
	// Normalize emptiness at the digest owner so representation details can
	// never suppress an otherwise reconciled node's capacity.
	if len(snapshot.Commands) == 0 {
		snapshot.Commands = nil
	}
	if len(snapshot.Observations) == 0 {
		snapshot.Observations = nil
	}
	if len(snapshot.CleanupTombstones) == 0 {
		snapshot.CleanupTombstones = nil
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	return hex.EncodeToString(digest[:]), nil
}
