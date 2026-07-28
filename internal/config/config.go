// Package config owns the versioned, non-secret management configuration
// document. Provider verification results and provider-assigned scale-set IDs
// intentionally stay outside this package.
package config

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/genm/sparerunner/internal/domain"
)

const (
	SchemaVersion = 1

	// RequestBodyLimitBytes is a byte-level transport memory boundary, not a
	// fleet-size quota. A 2026-07-26 compact-JSON measurement of 10k Nodes,
	// 10k profiles, and 10k Targets was 4,478,990 bytes. The executable
	// measurement in config_test.go preserves more than 3x headroom.
	RequestBodyLimitBytes int64 = 16 * 1024 * 1024
)

var (
	ErrInvalidConfiguration = errors.New("management configuration is invalid")
	ErrInvalidJSON          = errors.New("management configuration JSON is invalid")
	ErrInvalidYAML          = errors.New("management configuration YAML is invalid")
	ErrPayloadTooLarge      = errors.New("management configuration exceeds the transport byte budget")
)

// ValidationError gives the API a stable field and code without echoing an
// untrusted value from a configuration body.
type ValidationError struct {
	Code  string
	Field string
}

func (err *ValidationError) Error() string {
	return fmt.Sprintf("%v: code=%s field=%s", ErrInvalidConfiguration, err.Code, err.Field)
}

func (err *ValidationError) Unwrap() error {
	return ErrInvalidConfiguration
}

// DecimalUint64 uses a quoted canonical decimal representation in JSON to
// preserve values across JavaScript clients, while YAML uses an integer scalar.
type DecimalUint64 uint64

type Configuration struct {
	SchemaVersion  int                          `json:"schemaVersion" yaml:"schemaVersion"`
	Revision       DecimalUint64                `json:"revision" yaml:"revision"`
	Scheduler      SchedulerConfiguration       `json:"scheduler" yaml:"scheduler"`
	Nodes          []NodeConfiguration          `json:"nodes" yaml:"nodes"`
	RunnerProfiles []RunnerProfileConfiguration `json:"runnerProfiles" yaml:"runnerProfiles"`
	Targets        []GitHubTargetConfiguration  `json:"targets" yaml:"targets"`
}

type SchedulerConfiguration struct {
	MaxRunners *int `json:"maxRunners,omitempty" yaml:"maxRunners,omitempty"`
}

type NodeConfiguration struct {
	ID          domain.NodeID `json:"id" yaml:"id"`
	DisplayName string        `json:"displayName" yaml:"displayName"`
	MaxRunners  int           `json:"maxRunners" yaml:"maxRunners"`
}

// RunnerProfileConfiguration is the serializable projection of
// domain.RunnerProfile. It contains eligibility and update policy only, never a
// package credential or downloaded runner material.
type RunnerProfileConfiguration struct {
	ID                      domain.RunnerProfileID     `json:"id" yaml:"id"`
	Label                   string                     `json:"label" yaml:"label"`
	OperatingSystem         *domain.OperatingSystem    `json:"operatingSystem,omitempty" yaml:"operatingSystem,omitempty"`
	Architecture            *domain.Architecture       `json:"architecture,omitempty" yaml:"architecture,omitempty"`
	MinAvailableMemoryBytes DecimalUint64              `json:"minAvailableMemoryBytes" yaml:"minAvailableMemoryBytes"`
	VersionPolicy           domain.RunnerVersionPolicy `json:"versionPolicy" yaml:"versionPolicy"`
	Runtime                 domain.RuntimeKind         `json:"runtime" yaml:"runtime"`
}

// GitHubTargetConfiguration is intentionally not domain.GitHubTarget. Only a
// provider/store authority may add verified private visibility, safe
// runner-group access, and the SpareRunner-owned scale-set ID before persistence.
type GitHubTargetConfiguration struct {
	ID              domain.TargetID        `json:"id" yaml:"id"`
	InstallationID  string                 `json:"installationId" yaml:"installationId"`
	ScopeKind       domain.TargetScopeKind `json:"scopeKind" yaml:"scopeKind"`
	Scope           string                 `json:"scope" yaml:"scope"`
	ScaleSetName    string                 `json:"scaleSetName" yaml:"scaleSetName"`
	RunnerProfileID domain.RunnerProfileID `json:"runnerProfileId" yaml:"runnerProfileId"`
}

// AsDomain returns the infrastructure-independent runner profile value.
func (profile RunnerProfileConfiguration) AsDomain() domain.RunnerProfile {
	result := domain.RunnerProfile{
		ID:                      profile.ID,
		Label:                   profile.Label,
		MinAvailableMemoryBytes: uint64(profile.MinAvailableMemoryBytes),
		VersionPolicy:           profile.VersionPolicy,
		Runtime:                 profile.Runtime,
	}
	if profile.OperatingSystem != nil {
		value := *profile.OperatingSystem
		result.OS = &value
	}
	if profile.Architecture != nil {
		value := *profile.Architecture
		result.Architecture = &value
	}
	return result
}

// RunnerProfileFromDomain creates the non-secret serializable projection.
func RunnerProfileFromDomain(profile domain.RunnerProfile) RunnerProfileConfiguration {
	result := RunnerProfileConfiguration{
		ID:                      profile.ID,
		Label:                   profile.Label,
		MinAvailableMemoryBytes: DecimalUint64(profile.MinAvailableMemoryBytes),
		VersionPolicy:           profile.VersionPolicy,
		Runtime:                 profile.Runtime,
	}
	if profile.OS != nil {
		value := *profile.OS
		result.OperatingSystem = &value
	}
	if profile.Architecture != nil {
		value := *profile.Architecture
		result.Architecture = &value
	}
	return result
}

// TargetFromVerifiedDomain is export-only: domain validation proves that the
// source already carries private visibility and safe runner-group authority.
// No inverse conversion exists because external configuration cannot assert
// those facts.
func TargetFromVerifiedDomain(target domain.GitHubTarget) (GitHubTargetConfiguration, error) {
	if err := target.Validate(); err != nil {
		return GitHubTargetConfiguration{}, invalid("target_not_verified", "targets")
	}
	return GitHubTargetConfiguration{
		ID:              target.ID,
		InstallationID:  target.InstallationID,
		ScopeKind:       target.ScopeKind,
		Scope:           target.Scope,
		ScaleSetName:    target.ScaleSetName,
		RunnerProfileID: target.RunnerProfileID,
	}, nil
}

func (configuration Configuration) Validate() error {
	if configuration.SchemaVersion != SchemaVersion {
		return invalid("unsupported_schema_version", "schemaVersion")
	}
	if configuration.Scheduler.MaxRunners != nil &&
		*configuration.Scheduler.MaxRunners < 1 {
		return invalid("invalid_max_runners", "scheduler.maxRunners")
	}

	nodeIDs := make(map[domain.NodeID]struct{}, len(configuration.Nodes))
	for index, node := range configuration.Nodes {
		field := fmt.Sprintf("nodes[%d]", index)
		if !canonicalIdentifier(string(node.ID)) {
			return invalid("required", field+".id")
		}
		if strings.TrimSpace(node.DisplayName) == "" {
			return invalid("required", field+".displayName")
		}
		if node.MaxRunners < 1 {
			return invalid("invalid_max_runners", field+".maxRunners")
		}
		if _, exists := nodeIDs[node.ID]; exists {
			return invalid("duplicate_node_id", field+".id")
		}
		nodeIDs[node.ID] = struct{}{}
	}

	profileIDs := make(
		map[domain.RunnerProfileID]RunnerProfileConfiguration,
		len(configuration.RunnerProfiles),
	)
	profileLabels := make(map[string]struct{}, len(configuration.RunnerProfiles))
	for index, profile := range configuration.RunnerProfiles {
		field := fmt.Sprintf("runnerProfiles[%d]", index)
		if !canonicalIdentifier(string(profile.ID)) {
			return invalid("required", field+".id")
		}
		if !canonicalIdentifier(profile.Label) {
			return invalid("required", field+".label")
		}
		if _, exists := profileIDs[profile.ID]; exists {
			return invalid("duplicate_runner_profile_id", field+".id")
		}
		normalizedLabel := normalizeGitHubName(profile.Label)
		if _, exists := profileLabels[normalizedLabel]; exists {
			return invalid("duplicate_runner_profile_label", field+".label")
		}
		if err := profile.AsDomain().Validate(); err != nil {
			return invalid("invalid_runner_profile", field)
		}
		profileIDs[profile.ID] = profile
		profileLabels[normalizedLabel] = struct{}{}
	}

	targetIDs := make(map[domain.TargetID]struct{}, len(configuration.Targets))
	routes := make(map[string]*targetRoutes)
	for index, target := range configuration.Targets {
		field := fmt.Sprintf("targets[%d]", index)
		if !canonicalIdentifier(string(target.ID)) {
			return invalid("required", field+".id")
		}
		if _, exists := targetIDs[target.ID]; exists {
			return invalid("duplicate_target_id", field+".id")
		}
		targetIDs[target.ID] = struct{}{}
		if !canonicalIdentifier(target.InstallationID) {
			return invalid("required", field+".installationId")
		}
		if !canonicalIdentifier(target.ScaleSetName) {
			return invalid("required", field+".scaleSetName")
		}
		if !canonicalIdentifier(string(target.RunnerProfileID)) {
			return invalid("required", field+".runnerProfileId")
		}
		profile, exists := profileIDs[target.RunnerProfileID]
		if !exists {
			return invalid("unknown_runner_profile", field+".runnerProfileId")
		}
		if !strings.EqualFold(target.ScaleSetName, profile.Label) {
			return invalid("scale_set_profile_mismatch", field+".scaleSetName")
		}

		scope, owner, err := validatedTargetScope(target.ScopeKind, target.Scope)
		if err != nil {
			return invalid(err.code, field+"."+err.field)
		}
		label := normalizeGitHubName(profile.Label)
		route := routes[label]
		if route == nil {
			route = newTargetRoutes()
			routes[label] = route
		}
		if route.overlaps(target.ScopeKind, scope, owner) {
			return invalid("overlapping_target_routing", field+".scope")
		}
		route.add(target.ScopeKind, scope, owner)
	}
	return nil
}

type scopeValidationError struct {
	code  string
	field string
}

func validatedTargetScope(
	kind domain.TargetScopeKind,
	raw string,
) (scope string, owner string, err *scopeValidationError) {
	if !canonicalIdentifier(raw) {
		return "", "", &scopeValidationError{code: "required", field: "scope"}
	}
	switch kind {
	case domain.TargetOrganization:
		if strings.Contains(raw, "/") {
			return "", "", &scopeValidationError{
				code:  "invalid_organization_scope",
				field: "scope",
			}
		}
		scope = normalizeGitHubName(raw)
		return scope, scope, nil
	case domain.TargetRepository:
		parts := strings.Split(raw, "/")
		if len(parts) != 2 ||
			!canonicalIdentifier(parts[0]) ||
			!canonicalIdentifier(parts[1]) {
			return "", "", &scopeValidationError{
				code:  "invalid_repository_scope",
				field: "scope",
			}
		}
		owner = normalizeGitHubName(parts[0])
		scope = owner + "/" + normalizeGitHubName(parts[1])
		return scope, owner, nil
	default:
		return "", "", &scopeValidationError{
			code:  "invalid_target_scope_kind",
			field: "scopeKind",
		}
	}
}

type targetRoutes struct {
	organizations    map[string]struct{}
	repositories     map[string]struct{}
	repositoryOwners map[string]struct{}
}

func newTargetRoutes() *targetRoutes {
	return &targetRoutes{
		organizations:    make(map[string]struct{}),
		repositories:     make(map[string]struct{}),
		repositoryOwners: make(map[string]struct{}),
	}
}

func (routes *targetRoutes) overlaps(
	kind domain.TargetScopeKind,
	scope string,
	owner string,
) bool {
	switch kind {
	case domain.TargetOrganization:
		_, duplicateOrganization := routes.organizations[scope]
		_, containsRepository := routes.repositoryOwners[owner]
		return duplicateOrganization || containsRepository
	case domain.TargetRepository:
		_, duplicateRepository := routes.repositories[scope]
		_, organizationContainsRepository := routes.organizations[owner]
		return duplicateRepository || organizationContainsRepository
	default:
		return true
	}
}

func (routes *targetRoutes) add(
	kind domain.TargetScopeKind,
	scope string,
	owner string,
) {
	switch kind {
	case domain.TargetOrganization:
		routes.organizations[scope] = struct{}{}
	case domain.TargetRepository:
		routes.repositories[scope] = struct{}{}
		routes.repositoryOwners[owner] = struct{}{}
	}
}

func canonicalConfiguration(configuration Configuration) Configuration {
	result := configuration
	result.Nodes = append([]NodeConfiguration(nil), configuration.Nodes...)
	result.RunnerProfiles = append(
		[]RunnerProfileConfiguration(nil),
		configuration.RunnerProfiles...,
	)
	result.Targets = append(
		[]GitHubTargetConfiguration(nil),
		configuration.Targets...,
	)
	if result.Nodes == nil {
		result.Nodes = []NodeConfiguration{}
	}
	if result.RunnerProfiles == nil {
		result.RunnerProfiles = []RunnerProfileConfiguration{}
	}
	if result.Targets == nil {
		result.Targets = []GitHubTargetConfiguration{}
	}
	sort.Slice(result.Nodes, func(left, right int) bool {
		return result.Nodes[left].ID < result.Nodes[right].ID
	})
	sort.Slice(result.RunnerProfiles, func(left, right int) bool {
		return result.RunnerProfiles[left].ID < result.RunnerProfiles[right].ID
	})
	sort.Slice(result.Targets, func(left, right int) bool {
		return result.Targets[left].ID < result.Targets[right].ID
	})
	return result
}

func invalid(code, field string) error {
	return &ValidationError{Code: code, Field: field}
}

func canonicalIdentifier(value string) bool {
	return value != "" && strings.TrimSpace(value) == value
}

func normalizeGitHubName(value string) string {
	return strings.ToLower(value)
}
