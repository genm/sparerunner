// Package domain defines Tewake's infrastructure-independent scheduling contracts.
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
)

const (
	DefaultMaxRunners = 1

	// MacOSNativeRunnerMaxRunners is the Controller-side physical-capability
	// ceiling for the first macOS native adapter. That adapter provisions one
	// exclusive runner UID and therefore owns exactly slot 0.
	//
	// The current Agent protocol reports readiness as a boolean. When it grows a
	// reconciled nativeRunnerCapacity observation, that authenticated snapshot
	// becomes the physical-capability SSOT and configured MaxRunners must be
	// validated against it before slot topology is built.
	MacOSNativeRunnerMaxRunners = 1
)

type (
	NodeID          string
	TargetID        string
	RunnerProfileID string
	ExecutionID     string
	CommandID       string
	ControllerEpoch uint64
)

// ValidationError has stable machine-readable fields for API and store adapters.
type ValidationError struct {
	Code    string
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("domain validation failed: code=%s field=%s: %s", e.Code, e.Field, e.Message)
}

func invalid(code, field, message string) error {
	return &ValidationError{Code: code, Field: field, Message: message}
}

func required(value, field string) error {
	if strings.TrimSpace(value) == "" {
		return invalid("required", field, "must not be empty")
	}
	return nil
}

type OperatingSystem string

const (
	OSLinux   OperatingSystem = "linux"
	OSMacOS   OperatingSystem = "macos"
	OSWindows OperatingSystem = "windows"
)

func (o OperatingSystem) Validate(field string) error {
	switch o {
	case OSLinux, OSMacOS, OSWindows:
		return nil
	default:
		return invalid("invalid_operating_system", field, "must be linux, macos, or windows")
	}
}

type Architecture string

const (
	ArchAMD64 Architecture = "amd64"
	ArchARM64 Architecture = "arm64"
)

func (a Architecture) Validate(field string) error {
	switch a {
	case ArchAMD64, ArchARM64:
		return nil
	default:
		return invalid("invalid_architecture", field, "must be amd64 or arm64")
	}
}

type NodeAdministrativeState string

const (
	NodeActive      NodeAdministrativeState = "active"
	NodeDraining    NodeAdministrativeState = "draining"
	NodeQuarantined NodeAdministrativeState = "quarantined"
	NodeRevoked     NodeAdministrativeState = "revoked"
)

func (s NodeAdministrativeState) Validate(field string) error {
	switch s {
	case NodeActive, NodeDraining, NodeQuarantined, NodeRevoked:
		return nil
	default:
		return invalid("invalid_node_administrative_state", field, "must be active, draining, quarantined, or revoked")
	}
}

type NodeObservedState string

const (
	NodeOnline      NodeObservedState = "online"
	NodeOffline     NodeObservedState = "offline"
	NodeStale       NodeObservedState = "stale"
	NodeReconciling NodeObservedState = "reconciling"
)

func (s NodeObservedState) Validate(field string) error {
	switch s {
	case NodeOnline, NodeOffline, NodeStale, NodeReconciling:
		return nil
	default:
		return invalid("invalid_node_observed_state", field, "must be online, offline, stale, or reconciling")
	}
}

// Node is a persistent enrolled computer. Its observed fields intentionally stay
// outside this value so transports do not treat a stale observation as authority.
type Node struct {
	ID                  NodeID
	DisplayName         string
	CertificateSerial   string
	CredentialEpoch     uint64
	OS                  OperatingSystem
	Architecture        Architecture
	MaxRunners          int
	AdministrativeState NodeAdministrativeState
	ObservedState       NodeObservedState
}

func (n Node) Validate() error {
	if err := required(string(n.ID), "node.id"); err != nil {
		return err
	}
	if err := required(n.DisplayName, "node.display_name"); err != nil {
		return err
	}
	if err := n.OS.Validate("node.os"); err != nil {
		return err
	}
	if err := n.Architecture.Validate("node.architecture"); err != nil {
		return err
	}
	if n.MaxRunners < 1 {
		return invalid("invalid_max_runners", "node.max_runners", "must be at least one")
	}
	if n.OS == OSMacOS && n.MaxRunners > MacOSNativeRunnerMaxRunners {
		return invalid(
			"native_runner_capacity_exceeded",
			"node.max_runners",
			"macOS native mode currently owns exactly one runner slot",
		)
	}
	if err := n.AdministrativeState.Validate("node.administrative_state"); err != nil {
		return err
	}
	return n.ObservedState.Validate("node.observed_state")
}

type TargetScopeKind string

const (
	TargetRepository   TargetScopeKind = "repository"
	TargetOrganization TargetScopeKind = "organization"
)

type TargetVisibility string

const TargetPrivate TargetVisibility = "private"

// GitHubTarget is pre-verified configuration. GitHub I/O that verifies privacy and
// runner-group safety belongs to the adapter; this contract refuses unverified input.
type GitHubTarget struct {
	ID                    TargetID
	InstallationID        string
	ScopeKind             TargetScopeKind
	Scope                 string
	Visibility            TargetVisibility
	RunnerGroupAccessSafe bool
	ScaleSetName          string
	RunnerProfileID       RunnerProfileID
}

func (t GitHubTarget) Validate() error {
	if err := required(string(t.ID), "target.id"); err != nil {
		return err
	}
	if err := required(t.InstallationID, "target.installation_id"); err != nil {
		return err
	}
	switch t.ScopeKind {
	case TargetRepository, TargetOrganization:
	default:
		return invalid("invalid_target_scope_kind", "target.scope_kind", "must be repository or organization")
	}
	if err := required(t.Scope, "target.scope"); err != nil {
		return err
	}
	if t.Visibility != TargetPrivate {
		return invalid("target_not_private", "target.visibility", "must be verified private")
	}
	if !t.RunnerGroupAccessSafe {
		return invalid("unsafe_runner_group_access", "target.runner_group_access_safe", "must be verified safe")
	}
	if err := required(t.ScaleSetName, "target.scale_set_name"); err != nil {
		return err
	}
	return required(string(t.RunnerProfileID), "target.runner_profile_id")
}

type RuntimeKind string

const RuntimeNative RuntimeKind = "native"

type RunnerVersionPolicy string

const (
	RunnerVersionAutoUpdate RunnerVersionPolicy = "auto_update"
	RunnerVersionPinned     RunnerVersionPolicy = "pinned"
)

// RunnerProfile links the public scale-set label to platform eligibility.
type RunnerProfile struct {
	ID                      RunnerProfileID
	Label                   string
	OS                      *OperatingSystem
	Architecture            *Architecture
	MinAvailableMemoryBytes uint64
	VersionPolicy           RunnerVersionPolicy
	Runtime                 RuntimeKind
}

func (p RunnerProfile) Validate() error {
	if err := required(string(p.ID), "runner_profile.id"); err != nil {
		return err
	}
	if err := required(p.Label, "runner_profile.label"); err != nil {
		return err
	}
	if p.OS != nil {
		if err := p.OS.Validate("runner_profile.os"); err != nil {
			return err
		}
	}
	if p.Architecture != nil {
		if err := p.Architecture.Validate("runner_profile.architecture"); err != nil {
			return err
		}
	}
	switch p.VersionPolicy {
	case RunnerVersionAutoUpdate, RunnerVersionPinned:
	default:
		return invalid("invalid_runner_version_policy", "runner_profile.version_policy", "must be auto_update or pinned")
	}
	if p.Runtime != RuntimeNative {
		return invalid("unsupported_runner_runtime", "runner_profile.runtime", "must be native")
	}
	return nil
}

func (e ControllerEpoch) Validate() error {
	if e == 0 {
		return invalid("invalid_controller_epoch", "controller_epoch", "must be greater than zero")
	}
	return nil
}

func NextControllerEpoch(previous ControllerEpoch) (ControllerEpoch, error) {
	if previous == ControllerEpoch(math.MaxUint64) {
		return 0, invalid("controller_epoch_exhausted", "controller_epoch", "cannot advance past uint64 maximum")
	}
	return previous + 1, nil
}

// PayloadDigest returns the canonical SHA-256 digest recorded for a command payload.
// The payload itself is intentionally not retained by this domain contract.
func PayloadDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
