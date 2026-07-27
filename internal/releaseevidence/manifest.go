// Package releaseevidence validates the machine-readable evidence required by
// the first cross-platform Tewake release. A manifest is an assertion about
// observations made by a trusted live harness; this package never creates a
// passing manifest or substitutes local mocks for missing observations.
package releaseevidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ManifestVersion = 1
	ManifestKind    = "tewake.cross-platform-security.v1"
	maxManifestSize = 512 << 10
)

var (
	ErrInvalid = errors.New("release evidence manifest is invalid")

	commitPattern = regexp.MustCompile(`^[0-9a-fA-F]{40,64}$`)
)

// Manifest is the only accepted aggregate format for the TWK-014 live gate.
// It deliberately contains observations and identifiers, never credentials,
// tokens, JIT material, or free-form diagnostic payloads.
type Manifest struct {
	Version       int            `json:"version"`
	Kind          string         `json:"kind"`
	Status        string         `json:"status"`
	GeneratedAt   string         `json:"generatedAt"`
	CommitSHA     string         `json:"commitSha"`
	Platforms     []Platform     `json:"platforms"`
	Installations []Installation `json:"installations"`
	Scenarios     []Scenario     `json:"scenarios"`
	SecretScan    SecretScan     `json:"secretScan"`
}

type Platform struct {
	OS               string `json:"os"`
	Status           string `json:"status"`
	GenericRouting   bool   `json:"genericRouting"`
	OSProfileRouting bool   `json:"osProfileRouting"`
	ObservedAt       string `json:"observedAt"`
}

type Installation struct {
	ID          string `json:"id"`
	AccountKind string `json:"accountKind"`
	Status      string `json:"status"`
}

type Scenario struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	ObservedAt string `json:"observedAt"`
}

type SecretScan struct {
	Status   string   `json:"status"`
	Findings int      `json:"findings"`
	Surfaces []string `json:"surfaces"`
}

// ValidateFile reads and validates a manifest without following symlinks.
// Errors intentionally do not include file contents or field values so a
// malformed evidence file cannot echo a secret into CI logs.
func ValidateFile(path string) (Manifest, error) {
	var manifest Manifest
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return manifest, ErrInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return manifest, ErrInvalid
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, maxManifestSize+1))
	if err != nil || len(contents) > maxManifestSize || containsSecretMaterial(contents) {
		return manifest, ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, ErrInvalid
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return manifest, ErrInvalid
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateManifest(manifest Manifest) error {
	if manifest.Version != ManifestVersion || manifest.Kind != ManifestKind ||
		manifest.Status != "passed" || manifest.GeneratedAt == "" ||
		manifest.CommitSHA == "" || !commitPattern.MatchString(manifest.CommitSHA) {
		return ErrInvalid
	}
	if generatedAt, err := time.Parse(time.RFC3339Nano, manifest.GeneratedAt); err != nil || generatedAt.IsZero() {
		return ErrInvalid
	}
	if err := validatePlatforms(manifest.Platforms); err != nil {
		return err
	}
	if err := validateInstallations(manifest.Installations); err != nil {
		return err
	}
	if err := validateScenarios(manifest.Scenarios); err != nil {
		return err
	}
	if manifest.SecretScan.Status != "passed" || manifest.SecretScan.Findings != 0 ||
		!containsAll(manifest.SecretScan.Surfaces, requiredSecretSurfaces) {
		return ErrInvalid
	}
	return nil
}

var requiredPlatforms = []string{"linux", "macos", "windows"}

func validatePlatforms(platforms []Platform) error {
	if len(platforms) != len(requiredPlatforms) {
		return ErrInvalid
	}
	seen := make(map[string]struct{}, len(platforms))
	for _, platform := range platforms {
		if !contains(requiredPlatforms, platform.OS) || platform.Status != "passed" ||
			!platform.GenericRouting || !platform.OSProfileRouting || platform.ObservedAt == "" {
			return ErrInvalid
		}
		if _, ok := seen[platform.OS]; ok {
			return ErrInvalid
		}
		seen[platform.OS] = struct{}{}
		if observedAt, err := time.Parse(time.RFC3339Nano, platform.ObservedAt); err != nil || observedAt.IsZero() {
			return ErrInvalid
		}
	}
	for _, required := range requiredPlatforms {
		if _, ok := seen[required]; !ok {
			return ErrInvalid
		}
	}
	return nil
}

func validateInstallations(installations []Installation) error {
	if len(installations) < 2 {
		return ErrInvalid
	}
	seen := make(map[string]struct{}, len(installations))
	for _, installation := range installations {
		if installation.ID == "" || installation.Status != "passed" ||
			(installation.AccountKind != "user" && installation.AccountKind != "organization") {
			return ErrInvalid
		}
		if _, ok := seen[installation.ID]; ok {
			return ErrInvalid
		}
		seen[installation.ID] = struct{}{}
	}
	return nil
}

var requiredScenarios = []string{
	"public-target-rejected",
	"unsafe-runner-group-rejected",
	"secret-canary-scan",
	"restart",
	"disconnect",
	"drain",
	"stale-state",
	"quarantine",
}

func validateScenarios(scenarios []Scenario) error {
	if len(scenarios) != len(requiredScenarios) {
		return ErrInvalid
	}
	seen := make(map[string]struct{}, len(scenarios))
	for _, scenario := range scenarios {
		if !contains(requiredScenarios, scenario.Name) || scenario.Status != "passed" || scenario.ObservedAt == "" {
			return ErrInvalid
		}
		if _, ok := seen[scenario.Name]; ok {
			return ErrInvalid
		}
		seen[scenario.Name] = struct{}{}
		if observedAt, err := time.Parse(time.RFC3339Nano, scenario.ObservedAt); err != nil || observedAt.IsZero() {
			return ErrInvalid
		}
	}
	for _, required := range requiredScenarios {
		if _, ok := seen[required]; !ok {
			return ErrInvalid
		}
	}
	return nil
}

var requiredSecretSurfaces = []string{"database", "journal", "logs", "metrics", "ui", "diagnostics"}

func containsAll(have, required []string) bool {
	seen := make(map[string]struct{}, len(have))
	for _, value := range have {
		seen[value] = struct{}{}
	}
	for _, value := range required {
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsSecretMaterial(contents []byte) bool {
	lower := strings.ToLower(string(contents))
	for _, marker := range []string{
		"jit-secret-canary", "ghs_", "ghp_", "github_pat_", "-----begin ",
		"privatekey", "installationtoken", "jointoken", "clientsecret",
		"authorization: bearer", "password",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// RequiredPlatforms returns a sorted copy for documentation and tooling.
func RequiredPlatforms() []string {
	platforms := append([]string(nil), requiredPlatforms...)
	sort.Strings(platforms)
	return platforms
}

// RequiredScenarios returns a sorted copy for documentation and tooling.
func RequiredScenarios() []string {
	scenarios := append([]string(nil), requiredScenarios...)
	sort.Strings(scenarios)
	return scenarios
}

// RequiredSecretSurfaces returns a sorted copy for documentation and tooling.
func RequiredSecretSurfaces() []string {
	surfaces := append([]string(nil), requiredSecretSurfaces...)
	sort.Strings(surfaces)
	return surfaces
}

func (m Manifest) String() string {
	return fmt.Sprintf("%s (%d platforms, %d installations)", m.Kind, len(m.Platforms), len(m.Installations))
}
