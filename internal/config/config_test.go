package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/genm/tewake/internal/domain"
)

func TestJSONAndYAMLStrictRoundTrip(t *testing.T) {
	t.Parallel()

	fixture := configurationFixture()
	jsonBody, err := EncodeJSON(fixture)
	if err != nil {
		t.Fatalf("encode JSON: %v", err)
	}
	if bytes.Contains(jsonBody, []byte(`"revision":7`)) ||
		!bytes.Contains(jsonBody, []byte(`"revision":"7"`)) ||
		!bytes.Contains(jsonBody, []byte(`"minAvailableMemoryBytes":"4294967296"`)) {
		t.Fatalf("JSON uint64 values are not canonical decimal strings: %s", jsonBody)
	}
	jsonDecoded, err := DecodeJSON(bytes.NewReader(jsonBody))
	if err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if !reflect.DeepEqual(jsonDecoded, fixture) {
		t.Fatalf("JSON round trip = %#v, want %#v", jsonDecoded, fixture)
	}

	yamlBody, err := EncodeYAML(fixture)
	if err != nil {
		t.Fatalf("encode YAML: %v", err)
	}
	if !bytes.Contains(yamlBody, []byte("revision: 7\n")) ||
		!bytes.Contains(yamlBody, []byte("minAvailableMemoryBytes: 4294967296\n")) {
		t.Fatalf("YAML uint64 values are not canonical integers:\n%s", yamlBody)
	}
	yamlDecoded, err := DecodeYAML(bytes.NewReader(yamlBody))
	if err != nil {
		t.Fatalf("decode YAML: %v", err)
	}
	if !reflect.DeepEqual(yamlDecoded, fixture) {
		t.Fatalf("YAML round trip = %#v, want %#v", yamlDecoded, fixture)
	}
}

func TestCanonicalExportSortsIdentityCollectionsWithoutMutatingInput(t *testing.T) {
	t.Parallel()

	first := configurationFixture()
	second := configurationFixture()
	second.Nodes = append([]NodeConfiguration(nil), second.Nodes...)
	second.RunnerProfiles = append([]RunnerProfileConfiguration(nil), second.RunnerProfiles...)
	second.Targets = append([]GitHubTargetConfiguration(nil), second.Targets...)
	reverseNodes(second.Nodes)
	reverseProfiles(second.RunnerProfiles)
	reverseTargets(second.Targets)
	before := second
	before.Nodes = append([]NodeConfiguration(nil), second.Nodes...)
	before.RunnerProfiles = append([]RunnerProfileConfiguration(nil), second.RunnerProfiles...)
	before.Targets = append([]GitHubTargetConfiguration(nil), second.Targets...)

	firstYAML, err := EncodeYAML(first)
	if err != nil {
		t.Fatalf("encode first YAML: %v", err)
	}
	secondYAML, err := EncodeYAML(second)
	if err != nil {
		t.Fatalf("encode second YAML: %v", err)
	}
	if !bytes.Equal(firstYAML, secondYAML) {
		t.Fatalf("canonical YAML differs:\nfirst:\n%s\nsecond:\n%s", firstYAML, secondYAML)
	}
	if !reflect.DeepEqual(second, before) {
		t.Fatal("canonical export mutated the caller's configuration")
	}

	firstJSON, err := EncodeJSON(first)
	if err != nil {
		t.Fatalf("encode first JSON: %v", err)
	}
	secondJSON, err := EncodeJSON(second)
	if err != nil {
		t.Fatalf("encode second JSON: %v", err)
	}
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("canonical JSON differs:\nfirst: %s\nsecond: %s", firstJSON, secondJSON)
	}
}

func TestCanonicalExportEmitsEmptyCollectionsAsArrays(t *testing.T) {
	t.Parallel()

	configuration := Configuration{
		SchemaVersion: SchemaVersion,
		Scheduler:     SchedulerConfiguration{},
	}
	jsonBody, err := EncodeJSON(configuration)
	if err != nil {
		t.Fatalf("encode empty JSON: %v", err)
	}
	for _, collection := range []string{"nodes", "runnerProfiles", "targets"} {
		expected := `"` + collection + `":[]`
		if !bytes.Contains(jsonBody, []byte(expected)) {
			t.Fatalf("empty JSON collection %q is not an array: %s", collection, jsonBody)
		}
	}
	yamlBody, err := EncodeYAML(configuration)
	if err != nil {
		t.Fatalf("encode empty YAML: %v", err)
	}
	for _, collection := range []string{"nodes", "runnerProfiles", "targets"} {
		expected := collection + ": []\n"
		if !bytes.Contains(yamlBody, []byte(expected)) {
			t.Fatalf("empty YAML collection %q is not an array:\n%s", collection, yamlBody)
		}
	}
}

func TestDecodeRejectsUnknownDuplicateTrailingAndSecretFields(t *testing.T) {
	t.Parallel()

	validJSON := `{"schemaVersion":1,"revision":"0","scheduler":{},"nodes":[],"runnerProfiles":[],"targets":[]}`
	validYAML := "schemaVersion: 1\nrevision: 0\nscheduler: {}\nnodes: []\nrunnerProfiles: []\ntargets: []\n"
	secretCanary := "private-key-secret-canary"

	tests := []struct {
		name   string
		format string
		body   string
		want   error
	}{
		{
			name:   "JSON unknown root field",
			format: "json",
			body:   strings.TrimSuffix(validJSON, "}") + `,"unknown":true}`,
			want:   ErrInvalidJSON,
		},
		{
			name:   "JSON case-insensitive field alias",
			format: "json",
			body:   strings.Replace(validJSON, `"schemaVersion"`, `"SchemaVersion"`, 1),
			want:   ErrInvalidJSON,
		},
		{
			name:   "JSON duplicate field",
			format: "json",
			body:   strings.Replace(validJSON, `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1),
			want:   ErrInvalidJSON,
		},
		{
			name:   "JSON trailing document",
			format: "json",
			body:   validJSON + validJSON,
			want:   ErrInvalidJSON,
		},
		{
			name:   "JSON numeric revision is not the API decimal string",
			format: "json",
			body:   strings.Replace(validJSON, `"revision":"0"`, `"revision":0`, 1),
			want:   ErrInvalidJSON,
		},
		{
			name:   "JSON optional null is not nullable",
			format: "json",
			body:   strings.Replace(validJSON, `"scheduler":{}`, `"scheduler":{"maxRunners":null}`, 1),
			want:   ErrInvalidJSON,
		},
		{
			name:   "JSON secret field",
			format: "json",
			body:   strings.TrimSuffix(validJSON, "}") + `,"githubAppPrivateKey":"` + secretCanary + `"}`,
			want:   ErrInvalidJSON,
		},
		{
			name:   "JSON forged verification flags",
			format: "json",
			body: `{"schemaVersion":1,"revision":"0","scheduler":{},"nodes":[],"runnerProfiles":[{
				"id":"generic","label":"tewake","minAvailableMemoryBytes":"0","versionPolicy":"auto_update","runtime":"native"
			}],"targets":[{
				"id":"target","installationId":"installation","scopeKind":"organization","scope":"example-org",
				"scaleSetName":"tewake","runnerProfileId":"generic","visibility":"private","runnerGroupAccessSafe":true
			}]}`,
			want: ErrInvalidJSON,
		},
		{
			name:   "YAML unknown nested field",
			format: "yaml",
			body:   validYAML + "unknown: true\n",
			want:   ErrInvalidYAML,
		},
		{
			name:   "YAML duplicate field",
			format: "yaml",
			body:   strings.Replace(validYAML, "schemaVersion: 1\n", "schemaVersion: 1\nschemaVersion: 1\n", 1),
			want:   ErrInvalidYAML,
		},
		{
			name:   "YAML trailing document",
			format: "yaml",
			body:   validYAML + "---\n" + validYAML,
			want:   ErrInvalidYAML,
		},
		{
			name:   "YAML empty trailing document",
			format: "yaml",
			body:   validYAML + "---\n",
			want:   ErrInvalidYAML,
		},
		{
			name:   "YAML quoted revision is not an integer",
			format: "yaml",
			body:   strings.Replace(validYAML, "revision: 0", `revision: "0"`, 1),
			want:   ErrInvalidYAML,
		},
		{
			name:   "YAML optional null is not nullable",
			format: "yaml",
			body:   strings.Replace(validYAML, "scheduler: {}", "scheduler:\n  maxRunners: null", 1),
			want:   ErrInvalidYAML,
		},
		{
			name:   "YAML string field does not coerce an integer",
			format: "yaml",
			body: `schemaVersion: 1
revision: 0
scheduler: {}
nodes:
  - id: 123
    displayName: Build Node
    maxRunners: 1
runnerProfiles: []
targets: []
`,
			want: ErrInvalidYAML,
		},
		{
			name:   "YAML secret field",
			format: "yaml",
			body:   validYAML + "nodePrivateKey: " + secretCanary + "\n",
			want:   ErrInvalidYAML,
		},
		{
			name:   "YAML forged verification flags",
			format: "yaml",
			body: `schemaVersion: 1
revision: 0
scheduler: {}
nodes: []
runnerProfiles:
  - id: generic
    label: tewake
    minAvailableMemoryBytes: 0
    versionPolicy: auto_update
    runtime: native
targets:
  - id: target
    installationId: installation
    scopeKind: organization
    scope: example-org
    scaleSetName: tewake
    runnerProfileId: generic
    visibility: private
    runnerGroupAccessSafe: true
`,
			want: ErrInvalidYAML,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var err error
			switch test.format {
			case "json":
				_, err = DecodeJSON(strings.NewReader(test.body))
			case "yaml":
				_, err = DecodeYAML(strings.NewReader(test.body))
			default:
				t.Fatalf("unknown test format %q", test.format)
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("decode error = %v, want %v", err, test.want)
			}
			if strings.Contains(fmt.Sprint(err), secretCanary) {
				t.Fatal("decode error leaked a secret value")
			}
		})
	}
}

func TestDecodeRequiresCompleteObjectAndArrayShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format string
		body   string
		want   error
	}{
		{
			name:   "JSON missing collection",
			format: "json",
			body:   `{"schemaVersion":1,"revision":"0","scheduler":{},"nodes":[],"runnerProfiles":[]}`,
			want:   ErrInvalidJSON,
		},
		{
			name:   "JSON null collection",
			format: "json",
			body:   `{"schemaVersion":1,"revision":"0","scheduler":{},"nodes":null,"runnerProfiles":[],"targets":[]}`,
			want:   ErrInvalidJSON,
		},
		{
			name:   "YAML missing collection",
			format: "yaml",
			body:   "schemaVersion: 1\nrevision: 0\nscheduler: {}\nnodes: []\nrunnerProfiles: []\n",
			want:   ErrInvalidYAML,
		},
		{
			name:   "YAML null collection",
			format: "yaml",
			body:   "schemaVersion: 1\nrevision: 0\nscheduler: {}\nnodes: null\nrunnerProfiles: []\ntargets: []\n",
			want:   ErrInvalidYAML,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var err error
			if test.format == "json" {
				_, err = DecodeJSON(strings.NewReader(test.body))
			} else {
				_, err = DecodeYAML(strings.NewReader(test.body))
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("decode error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestValidationRejectsInvalidIdentityRoutingAndReferences(t *testing.T) {
	t.Parallel()

	base := configurationFixture()
	tests := []struct {
		name   string
		mutate func(*Configuration)
		code   string
	}{
		{
			name: "schema version",
			mutate: func(configuration *Configuration) {
				configuration.SchemaVersion = 2
			},
			code: "unsupported_schema_version",
		},
		{
			name: "scheduler max runners",
			mutate: func(configuration *Configuration) {
				zero := 0
				configuration.Scheduler.MaxRunners = &zero
			},
			code: "invalid_max_runners",
		},
		{
			name: "node display name",
			mutate: func(configuration *Configuration) {
				configuration.Nodes[0].DisplayName = " \t"
			},
			code: "required",
		},
		{
			name: "duplicate node ID",
			mutate: func(configuration *Configuration) {
				configuration.Nodes = append(configuration.Nodes, configuration.Nodes[0])
			},
			code: "duplicate_node_id",
		},
		{
			name: "duplicate profile ID",
			mutate: func(configuration *Configuration) {
				duplicate := configuration.RunnerProfiles[0]
				duplicate.Label = "another-label"
				configuration.RunnerProfiles = append(configuration.RunnerProfiles, duplicate)
			},
			code: "duplicate_runner_profile_id",
		},
		{
			name: "duplicate profile label is case insensitive",
			mutate: func(configuration *Configuration) {
				duplicate := configuration.RunnerProfiles[0]
				duplicate.ID = "profile-duplicate-label"
				duplicate.Label = strings.ToUpper(duplicate.Label)
				configuration.RunnerProfiles = append(configuration.RunnerProfiles, duplicate)
			},
			code: "duplicate_runner_profile_label",
		},
		{
			name: "invalid profile domain",
			mutate: func(configuration *Configuration) {
				configuration.RunnerProfiles[0].Runtime = "docker"
			},
			code: "invalid_runner_profile",
		},
		{
			name: "unknown target profile",
			mutate: func(configuration *Configuration) {
				configuration.Targets[0].RunnerProfileID = "missing"
			},
			code: "unknown_runner_profile",
		},
		{
			name: "target scale set differs from profile label",
			mutate: func(configuration *Configuration) {
				configuration.Targets[0].ScaleSetName = "another-label"
			},
			code: "scale_set_profile_mismatch",
		},
		{
			name: "duplicate target ID",
			mutate: func(configuration *Configuration) {
				configuration.Targets = append(configuration.Targets, configuration.Targets[0])
			},
			code: "duplicate_target_id",
		},
		{
			name: "invalid repository scope",
			mutate: func(configuration *Configuration) {
				configuration.Targets[0].ScopeKind = domain.TargetRepository
				configuration.Targets[0].Scope = "repository-without-owner"
			},
			code: "invalid_repository_scope",
		},
		{
			name: "organization and repository route overlap",
			mutate: func(configuration *Configuration) {
				repository := configuration.Targets[0]
				repository.ID = "target-repository"
				repository.ScopeKind = domain.TargetRepository
				repository.Scope = configuration.Targets[0].Scope + "/repository"
				configuration.Targets = append(configuration.Targets, repository)
			},
			code: "overlapping_target_routing",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			configuration := cloneConfiguration(base)
			test.mutate(&configuration)
			err := configuration.Validate()
			var validation *ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("validation error = %v, want *ValidationError", err)
			}
			if validation.Code != test.code {
				t.Fatalf("validation code = %q, want %q (error: %v)", validation.Code, test.code, err)
			}
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("validation error does not wrap ErrInvalidConfiguration: %v", err)
			}
		})
	}
}

func TestTargetConfigurationCannotRepresentProviderVerification(t *testing.T) {
	t.Parallel()

	targetType := reflect.TypeOf(GitHubTargetConfiguration{})
	for _, forbidden := range []string{
		"Visibility",
		"RunnerGroupAccessSafe",
		"ScaleSetID",
		"PrivateKey",
		"Token",
		"Secret",
	} {
		if _, found := targetType.FieldByName(forbidden); found {
			t.Fatalf("external target configuration exposes forbidden field %s", forbidden)
		}
	}

	configuration := configurationFixture()
	encoded, err := json.Marshal(configuration)
	if err != nil {
		t.Fatalf("marshal configuration: %v", err)
	}
	for _, forbidden := range []string{
		"visibility",
		"runnerGroupAccessSafe",
		"scaleSetId",
		"privateKey",
		"token",
		"secret",
	} {
		if bytes.Contains(bytes.ToLower(encoded), bytes.ToLower([]byte(forbidden))) {
			t.Fatalf("encoded configuration contains forbidden field %q: %s", forbidden, encoded)
		}
	}
}

func TestRealisticLargeConfigurationFitsMeasuredTransportBudget(t *testing.T) {
	configuration := Configuration{
		SchemaVersion: SchemaVersion,
		Revision:      9,
		Scheduler:     SchedulerConfiguration{},
		Nodes:         make([]NodeConfiguration, 10_000),
		RunnerProfiles: make(
			[]RunnerProfileConfiguration,
			10_000,
		),
		Targets: make([]GitHubTargetConfiguration, 10_000),
	}
	for index := 0; index < 10_000; index++ {
		id := fmt.Sprintf("%05d", index)
		profileID := domain.RunnerProfileID("profile-" + id)
		label := "tewake-" + id
		configuration.Nodes[index] = NodeConfiguration{
			ID:          domain.NodeID("node-" + id),
			DisplayName: "Build Node " + id,
			MaxRunners:  1 + index%4,
		}
		configuration.RunnerProfiles[index] = RunnerProfileConfiguration{
			ID:                      profileID,
			Label:                   label,
			MinAvailableMemoryBytes: DecimalUint64(4 << 30),
			VersionPolicy:           domain.RunnerVersionAutoUpdate,
			Runtime:                 domain.RuntimeNative,
		}
		configuration.Targets[index] = GitHubTargetConfiguration{
			ID:              domain.TargetID("target-" + id),
			InstallationID:  "installation-" + id,
			ScopeKind:       domain.TargetOrganization,
			Scope:           "example-org-" + id,
			ScaleSetName:    label,
			RunnerProfileID: profileID,
		}
	}

	encoded, err := EncodeJSON(configuration)
	if err != nil {
		t.Fatalf("encode realistic configuration: %v", err)
	}
	t.Logf(
		"realistic 10k nodes + 10k profiles + 10k targets = %d bytes; pre-implementation baseline was 4,478,990 bytes",
		len(encoded),
	)
	if int64(len(encoded)) >= RequestBodyLimitBytes {
		t.Fatalf(
			"realistic configuration size = %d, must be below %d",
			len(encoded),
			RequestBodyLimitBytes,
		)
	}
	decoded, err := DecodeJSON(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode realistic configuration: %v", err)
	}
	if len(decoded.Nodes) != 10_000 ||
		len(decoded.RunnerProfiles) != 10_000 ||
		len(decoded.Targets) != 10_000 {
		t.Fatalf(
			"decoded realistic counts = nodes:%d profiles:%d targets:%d",
			len(decoded.Nodes),
			len(decoded.RunnerProfiles),
			len(decoded.Targets),
		)
	}
}

func TestDecodeRejectsRequestBodyOverTransportBudget(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("x"), int(RequestBodyLimitBytes)+1)
	if _, err := DecodeJSON(bytes.NewReader(payload)); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversize JSON error = %v, want %v", err, ErrPayloadTooLarge)
	}
	if _, err := DecodeYAML(bytes.NewReader(payload)); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversize YAML error = %v, want %v", err, ErrPayloadTooLarge)
	}
}

func TestDecodeClassifiesReaderFailureByMediaTypeWithoutLeakingDetails(t *testing.T) {
	t.Parallel()

	canary := "reader-secret-canary"
	if _, err := DecodeJSON(failingConfigReader{message: canary}); !errors.Is(err, ErrInvalidJSON) {
		t.Fatalf("JSON reader error = %v, want %v", err, ErrInvalidJSON)
	} else if strings.Contains(err.Error(), canary) {
		t.Fatal("JSON reader error leaked the underlying reader detail")
	}
	if _, err := DecodeYAML(failingConfigReader{message: canary}); !errors.Is(err, ErrInvalidYAML) {
		t.Fatalf("YAML reader error = %v, want %v", err, ErrInvalidYAML)
	} else if strings.Contains(err.Error(), canary) {
		t.Fatal("YAML reader error leaked the underlying reader detail")
	}
}

func configurationFixture() Configuration {
	linux := domain.OSLinux
	amd64 := domain.ArchAMD64
	fleetMax := 6
	return Configuration{
		SchemaVersion: SchemaVersion,
		Revision:      7,
		Scheduler: SchedulerConfiguration{
			MaxRunners: &fleetMax,
		},
		Nodes: []NodeConfiguration{
			{ID: "node-a", DisplayName: "Build Desk A", MaxRunners: 2},
			{ID: "node-b", DisplayName: "Build Desk B", MaxRunners: 4},
		},
		RunnerProfiles: []RunnerProfileConfiguration{
			{
				ID:                      "profile-generic",
				Label:                   "tewake",
				MinAvailableMemoryBytes: 0,
				VersionPolicy:           domain.RunnerVersionAutoUpdate,
				Runtime:                 domain.RuntimeNative,
			},
			{
				ID:                      "profile-linux",
				Label:                   "tewake-linux",
				OperatingSystem:         &linux,
				Architecture:            &amd64,
				MinAvailableMemoryBytes: DecimalUint64(4 << 30),
				VersionPolicy:           domain.RunnerVersionAutoUpdate,
				Runtime:                 domain.RuntimeNative,
			},
		},
		Targets: []GitHubTargetConfiguration{
			{
				ID:              "target-generic",
				InstallationID:  "installation-1",
				ScopeKind:       domain.TargetOrganization,
				Scope:           "example-org-generic",
				ScaleSetName:    "tewake",
				RunnerProfileID: "profile-generic",
			},
			{
				ID:              "target-linux",
				InstallationID:  "installation-2",
				ScopeKind:       domain.TargetOrganization,
				Scope:           "example-org-linux",
				ScaleSetName:    "tewake-linux",
				RunnerProfileID: "profile-linux",
			},
		},
	}
}

func cloneConfiguration(configuration Configuration) Configuration {
	clone := configuration
	clone.Nodes = append([]NodeConfiguration(nil), configuration.Nodes...)
	clone.RunnerProfiles = append(
		[]RunnerProfileConfiguration(nil),
		configuration.RunnerProfiles...,
	)
	clone.Targets = append(
		[]GitHubTargetConfiguration(nil),
		configuration.Targets...,
	)
	if configuration.Scheduler.MaxRunners != nil {
		value := *configuration.Scheduler.MaxRunners
		clone.Scheduler.MaxRunners = &value
	}
	for index := range clone.RunnerProfiles {
		if clone.RunnerProfiles[index].OperatingSystem != nil {
			value := *clone.RunnerProfiles[index].OperatingSystem
			clone.RunnerProfiles[index].OperatingSystem = &value
		}
		if clone.RunnerProfiles[index].Architecture != nil {
			value := *clone.RunnerProfiles[index].Architecture
			clone.RunnerProfiles[index].Architecture = &value
		}
	}
	return clone
}

func reverseNodes(values []NodeConfiguration) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func reverseProfiles(values []RunnerProfileConfiguration) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func reverseTargets(values []GitHubTargetConfiguration) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

type failingConfigReader struct {
	message string
}

func (reader failingConfigReader) Read([]byte) (int, error) {
	return 0, errors.New(reader.message)
}
