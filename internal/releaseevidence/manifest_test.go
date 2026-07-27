package releaseevidence

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateFileAcceptsCompleteCrossPlatformEvidence(t *testing.T) {
	path := writeManifest(t, completeManifest())
	manifest, err := ValidateFile(path)
	if err != nil {
		t.Fatalf("ValidateFile() error = %v", err)
	}
	if manifest.Kind != ManifestKind || len(manifest.Platforms) != 3 || len(manifest.Installations) != 2 {
		t.Fatalf("validated manifest = %#v", manifest)
	}
}

func TestValidateFileRejectsMissingPlatformEvidence(t *testing.T) {
	manifest := completeManifest()
	manifest.Platforms = manifest.Platforms[:2]
	if err := expectInvalid(t, manifest); err == nil {
		t.Fatal("ValidateFile() accepted incomplete platform evidence")
	}
}

func TestValidateFileRejectsDuplicateInstallationAndScenario(t *testing.T) {
	manifest := completeManifest()
	manifest.Installations[1].ID = manifest.Installations[0].ID
	if err := expectInvalid(t, manifest); err == nil {
		t.Fatal("ValidateFile() accepted duplicate installation evidence")
	}

	manifest = completeManifest()
	manifest.Scenarios[1].Name = manifest.Scenarios[0].Name
	if err := expectInvalid(t, manifest); err == nil {
		t.Fatal("ValidateFile() accepted duplicate scenario evidence")
	}
}

func TestValidateFileRejectsSecretCanaryAndUnknownFields(t *testing.T) {
	manifest := completeManifest()
	manifest.CommitSHA = "jit-secret-canary"
	if err := expectInvalid(t, manifest); err == nil {
		t.Fatal("ValidateFile() accepted secret canary")
	}

	path := writeManifest(t, completeManifest())
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents = append(contents[:len(contents)-1], []byte(`,"unexpected":"value"}`)...)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateFile(path); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown field error = %v, want ErrInvalid", err)
	}
}

func TestValidateFileRejectsSymlinkAndTrailingJSON(t *testing.T) {
	path := writeManifest(t, completeManifest())
	link := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.Symlink(path, link); err == nil {
		if _, err := ValidateFile(link); !errors.Is(err, ErrInvalid) {
			t.Fatalf("symlink error = %v, want ErrInvalid", err)
		}
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents = append(contents, []byte("{}")...)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateFile(path); !errors.Is(err, ErrInvalid) {
		t.Fatalf("trailing JSON error = %v, want ErrInvalid", err)
	}
}

func completeManifest() Manifest {
	observedAt := time.Date(2026, time.July, 27, 1, 2, 3, 0, time.UTC).Format(time.RFC3339Nano)
	platforms := make([]Platform, 0, len(requiredPlatforms))
	for _, operatingSystem := range requiredPlatforms {
		platforms = append(platforms, Platform{
			OS: operatingSystem, Status: "passed", GenericRouting: true,
			OSProfileRouting: true, ObservedAt: observedAt,
		})
	}
	scenarios := make([]Scenario, 0, len(requiredScenarios))
	for _, name := range requiredScenarios {
		scenarios = append(scenarios, Scenario{Name: name, Status: "passed", ObservedAt: observedAt})
	}
	return Manifest{
		Version: ManifestVersion, Kind: ManifestKind, Status: "passed",
		GeneratedAt: observedAt, CommitSHA: "0123456789abcdef0123456789abcdef01234567",
		Platforms: platforms,
		Installations: []Installation{
			{ID: "installation-user-1", AccountKind: "user", Status: "passed"},
			{ID: "installation-org-1", AccountKind: "organization", Status: "passed"},
		},
		Scenarios: scenarios,
		SecretScan: SecretScan{
			Status: "passed", Findings: 0,
			Surfaces: append([]string(nil), requiredSecretSurfaces...),
		},
	}
}

func writeManifest(t *testing.T, manifest Manifest) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "manifest.json")
	contents, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func expectInvalid(t *testing.T, manifest Manifest) error {
	t.Helper()
	path := filepath.Join(t.TempDir(), "manifest.json")
	contents, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		return err
	}
	_, err = ValidateFile(path)
	return err
}
