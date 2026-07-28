package app

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	managementapi "github.com/genm/sparerunner/internal/api"
	"github.com/genm/sparerunner/internal/api/gen"
	"github.com/genm/sparerunner/internal/config"
	"github.com/genm/sparerunner/internal/domain"
	"github.com/genm/sparerunner/internal/enroll"
	"github.com/genm/sparerunner/internal/github"
	"github.com/genm/sparerunner/internal/reconcile"
	"github.com/genm/sparerunner/internal/store"
)

const managementTestRequestID = "req_00112233445566778899aabbccddeeff"

func TestEnrollmentProjectsNodeBeforeFirstAgentSnapshot(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	controllerStore, err := store.OpenController(
		ctx,
		filepath.Join(directory, "controller.db"),
		store.Options{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer controllerStore.Close()
	identity, err := enroll.NewControllerIdentity(time.Now(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := controllerStore.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restart, err := controllerStore.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := reconcile.RestoreRestart(restart, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	var digestKey [32]byte
	for index := range digestKey {
		digestKey[index] = byte(index + 1)
	}
	registry := projectingEnrollmentRegistry{
		Registry: auditedRuntimeEnrollmentRegistry{
			Registry: controllerStore,
			store:    controllerStore,
		},
		reader:     controllerStore,
		controller: projection,
	}
	service, err := enroll.NewService(registry, identity, digestKey, uint64(epoch))
	if err != nil {
		t.Fatal(err)
	}
	code, err := service.CreateJoinCode(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	enrolled, err := service.Enroll(ctx, code, managementTestCSR(t))
	if err != nil {
		t.Fatal(err)
	}
	nodeID := domain.NodeID(enrolled.NodeID)
	admission, err := projection.Admission(nodeID)
	if err != nil {
		t.Fatalf("newly enrolled node was absent before first Agent snapshot: %v", err)
	}
	if admission.Node.Node.DisplayName != enrolled.NodeID ||
		admission.Node.Node.MaxRunners != domain.DefaultMaxRunners {
		t.Fatalf("newly enrolled projection = %#v", admission.Node.Node)
	}

	state := &ControllerState{
		Identity: identity, Store: controllerStore, Service: service,
		Reconciler: projection, Epoch: uint64(epoch),
	}
	backend, err := newManagementBackend(state, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, err := controllerStore.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	document, err := managementConfigurationDocument(current)
	if err != nil {
		t.Fatal(err)
	}
	document.Nodes[0].DisplayName = "Before snapshot"
	payload, err := config.EncodeJSON(document)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := backend.ApplyConfiguration(
		ctx,
		current.Revision,
		"application/json",
		payload,
		managementTestRequestID,
	)
	if err != nil || applied.Revision != "2" {
		t.Fatalf("pre-snapshot configuration apply = (%#v, %v)", applied, err)
	}
	node, revision, err := backend.SetNodeAdministrativeState(
		ctx,
		nodeID,
		domain.NodeDraining,
		2,
		managementTestRequestID,
	)
	if err != nil || revision != "3" ||
		node.AdministrativeState != gen.NodeAdministrativeStateDraining {
		t.Fatalf("pre-snapshot drain = (%#v, %q, %v)", node, revision, err)
	}
}

func TestEnrollmentProjectionSurvivesClientCancellationAfterDurableConsume(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	controllerStore, err := store.OpenController(
		ctx,
		filepath.Join(directory, "controller.db"),
		store.Options{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer controllerStore.Close()
	identity, err := enroll.NewControllerIdentity(time.Now(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := controllerStore.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restart, err := controllerStore.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := reconcile.RestoreRestart(restart, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	enrollmentContext, cancelEnrollment := context.WithCancel(context.Background())
	var digestKey [32]byte
	for index := range digestKey {
		digestKey[index] = byte(index + 1)
	}
	registry := projectingEnrollmentRegistry{
		Registry: cancelAfterEnrollmentConsumeRegistry{
			Registry: auditedRuntimeEnrollmentRegistry{
				Registry: controllerStore,
				store:    controllerStore,
			},
			cancel: cancelEnrollment,
		},
		reader:     controllerStore,
		controller: projection,
	}
	service, err := enroll.NewService(registry, identity, digestKey, uint64(epoch))
	if err != nil {
		t.Fatal(err)
	}
	code, err := service.CreateJoinCode(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	enrolled, err := service.Enroll(
		enrollmentContext,
		code,
		managementTestCSR(t),
	)
	if err != nil {
		t.Fatalf("post-commit cancellation hid enrollment: %v", err)
	}
	if enrollmentContext.Err() == nil {
		t.Fatal("test registry did not cancel the client context")
	}
	admission, err := projection.Admission(domain.NodeID(enrolled.NodeID))
	if err != nil {
		t.Fatalf("cancelled request left durable node unprojected: %v", err)
	}
	if admission.Node.Node.DisplayName != enrolled.NodeID ||
		!projection.ManagementProjectionHealthy() {
		t.Fatalf("cancel-safe enrollment projection = %#v", admission)
	}
	events, err := controllerStore.ReadAuditEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 ||
		events[0].Record.Action != store.AuditActionJoinCodeCreated ||
		events[1].Record.Action != store.AuditActionNodeEnrolled ||
		events[1].Revision != 1 {
		t.Fatalf("cancel-safe enrollment audits = %#v", events)
	}
}

func TestManagementBackendConfigurationApplyCommitsAuditAndLiveProjection(t *testing.T) {
	ctx := context.Background()
	backend, nodeID := newManagementBackendForTest(t)

	current, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	document, err := managementConfigurationDocument(current)
	if err != nil {
		t.Fatal(err)
	}
	document.Nodes[0].DisplayName = "Build node"
	document.Nodes[0].MaxRunners = 2
	payload, err := config.EncodeYAML(document)
	if err != nil {
		t.Fatal(err)
	}

	applied, err := backend.ApplyConfiguration(
		ctx,
		current.Revision,
		"application/yaml",
		payload,
		managementTestRequestID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Revision != "2" ||
		len(applied.Nodes) != 1 ||
		applied.Nodes[0].DisplayName != "Build node" ||
		applied.Nodes[0].MaxRunners != 2 {
		t.Fatalf("applied configuration = %#v", applied)
	}
	admission, err := backend.state.Reconciler.Admission(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if admission.Node.Node.DisplayName != "Build node" ||
		admission.Node.Node.MaxRunners != 2 {
		t.Fatalf("live node projection = %#v", admission.Node.Node)
	}
	restart, err := backend.state.Store.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(restart.NodeTopology) != 1 ||
		restart.NodeTopology[0].DisplayName != "Build node" ||
		restart.NodeTopology[0].MaxRunners != 2 {
		t.Fatalf("restart topology = %#v", restart.NodeTopology)
	}
	events, err := backend.state.Store.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 ||
		events[0].Record.Action != store.AuditActionConfigurationApplied ||
		events[0].Revision != 2 {
		t.Fatalf("configuration audit = %#v", events)
	}

	_, err = backend.ApplyConfiguration(
		ctx,
		current.Revision,
		"application/yaml",
		payload,
		managementTestRequestID,
	)
	var conflict *managementapi.RevisionConflict
	if !errors.As(err, &conflict) || conflict.Current != 2 {
		t.Fatalf("stale configuration error = %#v", err)
	}
	events, err = backend.state.Store.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("stale apply appended audit = %#v", events)
	}

	secretPayload := append(
		append([]byte(nil), payload...),
		[]byte("\ngithubAppPrivateKey: secret-canary\n")...,
	)
	_, err = backend.ApplyConfiguration(
		ctx,
		2,
		"application/yaml",
		secretPayload,
		managementTestRequestID,
	)
	if err == nil || strings.Contains(err.Error(), "secret-canary") {
		t.Fatalf("secret-bearing configuration error = %q", err)
	}
}

func TestManagementBackendProjectsBoundedAuditPages(t *testing.T) {
	ctx := context.Background()
	backend, _ := newManagementBackendForTest(t)
	for index := 0; index < 3; index++ {
		if _, err := backend.state.Store.AppendAuditEvent(ctx, store.AuditRecord{
			Actor:        store.AuditActorAnonymous,
			Action:       store.AuditActionAuthenticationFailed,
			Outcome:      store.AuditOutcomeRejected,
			ResourceKind: store.AuditResourceController,
			ErrorCode:    store.AuditErrorAuthenticationFailed,
			RequestID:    fmt.Sprintf("req_%032x", index+1),
		}); err != nil {
			t.Fatal(err)
		}
	}

	first, err := backend.AuditEvents(ctx, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Events) != 2 ||
		first.Events[0].Id != "audit-1" ||
		first.Events[1].Id != "audit-2" ||
		first.Events[0].ErrorCode == nil ||
		*first.Events[0].ErrorCode != "authentication_failed" ||
		first.NextAfter == nil ||
		*first.NextAfter != 2 ||
		first.ResumeAfter == nil ||
		*first.ResumeAfter != 2 {
		t.Fatalf("first backend audit page = %#v", first)
	}

	last, err := backend.AuditEvents(ctx, *first.NextAfter, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(last.Events) != 1 ||
		last.Events[0].Id != "audit-3" ||
		last.NextAfter != nil ||
		last.ResumeAfter == nil ||
		*last.ResumeAfter != 3 {
		t.Fatalf("last backend audit page = %#v", last)
	}
}

func TestManagementConfigurationRejectsExpandedSuccessResponseBeforeCommit(t *testing.T) {
	ctx := context.Background()
	backend, _ := newManagementBackendForTest(t)
	current, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	document, err := managementConfigurationDocument(current)
	if err != nil {
		t.Fatal(err)
	}
	document.Nodes[0].DisplayName = strings.Repeat(
		"<",
		int(config.RequestBodyLimitBytes/6)+1024,
	)
	payload, err := config.EncodeYAML(document)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(payload)) >= config.RequestBodyLimitBytes {
		t.Fatalf(
			"escaping-heavy fixture input = %d, want below %d",
			len(payload),
			config.RequestBodyLimitBytes,
		)
	}
	_, err = backend.ApplyConfiguration(
		ctx,
		current.Revision,
		"application/yaml",
		payload,
		managementTestRequestID,
	)
	var validation *managementapi.ValidationError
	if !errors.As(err, &validation) ||
		len(validation.Violations) != 1 ||
		validation.Violations[0].Code != "response_too_large" {
		t.Fatalf("expanded response error = %#v", err)
	}
	after, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != current.Revision ||
		after.Nodes[0].DisplayName != current.Nodes[0].DisplayName {
		t.Fatalf("oversize response mutated durable configuration: %#v", after)
	}
}

func TestManagementConfiguredCapacitySaturatesWithoutIntegerWrap(t *testing.T) {
	t.Parallel()

	configuration := store.ManagementConfiguration{
		Nodes: []store.ManagementNodeConfiguration{
			{MaxRunners: math.MaxInt},
			{MaxRunners: 2},
		},
	}
	if got := managementConfiguredCapacity(configuration); got != math.MaxInt {
		t.Fatalf("saturated capacity = %d, want %d", got, math.MaxInt)
	}
	fleetLimit := 7
	configuration.FleetMaxRunners = &fleetLimit
	configuration.Nodes = []store.ManagementNodeConfiguration{
		{MaxRunners: 5},
		{MaxRunners: 5},
	}
	if got := managementConfiguredCapacity(configuration); got != fleetLimit {
		t.Fatalf("fleet-limited capacity = %d, want %d", got, fleetLimit)
	}
}

func TestManagementProjectionDegradationBlocksConfigurationBeforeCommit(t *testing.T) {
	ctx := context.Background()
	backend, _ := newManagementBackendForTest(t)
	current, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	document, err := managementConfigurationDocument(current)
	if err != nil {
		t.Fatal(err)
	}
	document.Nodes[0].DisplayName = "Must not commit"
	payload, err := config.EncodeJSON(document)
	if err != nil {
		t.Fatal(err)
	}

	backend.state.Reconciler.MarkManagementProjectionUnavailable()
	if _, err := backend.ApplyConfiguration(
		ctx,
		current.Revision,
		"application/json",
		payload,
		managementTestRequestID,
	); !errors.Is(err, managementapi.ErrBackendUnavailable) {
		t.Fatalf("degraded projection apply = %v", err)
	}
	after, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != current.Revision ||
		after.Nodes[0].DisplayName != current.Nodes[0].DisplayName {
		t.Fatalf("degraded projection mutated durable configuration: %#v", after)
	}
	setup, err := backend.Setup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(setup.Conditions) != 1 ||
		setup.Conditions[0].Code != "management_projection_unavailable" ||
		setup.Conditions[0].Status != gen.ConditionStatusUnavailable {
		t.Fatalf("degraded projection conditions = %#v", setup.Conditions)
	}
}

func TestManagementHealthyConditionsRemainEmptyArrays(t *testing.T) {
	ctx := context.Background()
	backend, _ := newManagementBackendForTest(t)

	setup, err := backend.Setup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	overview, err := backend.Overview(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if setup.Conditions == nil || len(setup.Conditions) != 0 {
		t.Fatalf("healthy setup conditions = %#v, want empty array", setup.Conditions)
	}
	if overview.Conditions == nil || len(overview.Conditions) != 0 {
		t.Fatalf("healthy overview conditions = %#v, want empty array", overview.Conditions)
	}
}

func TestManagementTargetVerificationFailsClosedAndAcceptsOnlyVerifiedPrivateAuthority(
	t *testing.T,
) {
	ctx := context.Background()
	backend, _ := newManagementBackendForTest(t)
	current, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	document, err := managementConfigurationDocument(current)
	if err != nil {
		t.Fatal(err)
	}
	document.RunnerProfiles = []config.RunnerProfileConfiguration{{
		ID: "profile-default", Label: "sparerunner",
		VersionPolicy: domain.RunnerVersionAutoUpdate,
		Runtime:       domain.RuntimeNative,
	}}
	target := config.GitHubTargetConfiguration{
		ID: "target-private", InstallationID: "installation-41",
		ScopeKind: domain.TargetOrganization, Scope: "example-org",
		ScaleSetName: "sparerunner", RunnerProfileID: "profile-default",
	}
	document.Targets = []config.GitHubTargetConfiguration{target}
	payload, err := config.EncodeJSON(document)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := backend.ApplyConfiguration(
		ctx,
		current.Revision,
		"application/json",
		payload,
		managementTestRequestID,
	); !errors.Is(err, managementapi.ErrBackendUnavailable) {
		t.Fatalf("disconnected target verifier error = %v", err)
	}
	unchanged, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Revision != current.Revision ||
		len(unchanged.RunnerProfiles) != 0 ||
		len(unchanged.GitHubTargets) != 0 {
		t.Fatalf("unverified target mutated configuration: %#v", unchanged)
	}

	backend.targetVerifier = managementTargetVerifierFunc(func(
		context.Context,
		config.GitHubTargetConfiguration,
	) (store.ManagementGitHubTarget, error) {
		return store.ManagementGitHubTarget{}, &managementapi.ValidationError{
			Violations: []managementapi.FieldViolation{{
				Field: "targets[0].scope", Code: "public_target_not_allowed",
				Message: "The target must be private.",
			}},
		}
	})
	_, err = backend.ApplyConfiguration(
		ctx,
		current.Revision,
		"application/json",
		payload,
		managementTestRequestID,
	)
	var validation *managementapi.ValidationError
	if !errors.As(err, &validation) ||
		len(validation.Violations) != 1 ||
		validation.Violations[0].Code != "public_target_not_allowed" {
		t.Fatalf("unsafe target verification error = %#v", err)
	}

	backend.targetVerifier = managementTargetVerifierFunc(func(
		_ context.Context,
		requested config.GitHubTargetConfiguration,
	) (store.ManagementGitHubTarget, error) {
		return store.ManagementGitHubTarget{
			Target: domain.GitHubTarget{
				ID: requested.ID, InstallationID: requested.InstallationID,
				ScopeKind: requested.ScopeKind, Scope: requested.Scope,
				Visibility: domain.TargetPrivate, RunnerGroupAccessSafe: true,
				ScaleSetName:    requested.ScaleSetName,
				RunnerProfileID: requested.RunnerProfileID,
			},
			ScaleSetID: 41,
		}, nil
	})
	applied, err := backend.ApplyConfiguration(
		ctx,
		current.Revision,
		"application/json",
		payload,
		managementTestRequestID,
	)
	if err != nil || applied.Revision != "2" || len(applied.Targets) != 1 {
		t.Fatalf("verified private target apply = (%#v, %v)", applied, err)
	}
}

// TestSetupKeepsTargetCreationAvailableWhileRuntimeIsUnverified pins the
// live-deployment regression: creating the first organization Target left its
// GitHub message session unestablished — the normal state until the runner
// coordinator polls — and the old Setup folded that into githubAppState. The
// console then reported that no verified GitHub installation existed and
// refused every further Target, while the installation was in fact verified.
func TestSetupKeepsTargetCreationAvailableWhileRuntimeIsUnverified(t *testing.T) {
	ctx := context.Background()
	backend, _ := newManagementBackendForTest(t)
	current, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	document, err := managementConfigurationDocument(current)
	if err != nil {
		t.Fatal(err)
	}
	document.RunnerProfiles = []config.RunnerProfileConfiguration{{
		ID: "profile-default", Label: "sparerunner",
		VersionPolicy: domain.RunnerVersionAutoUpdate,
		Runtime:       domain.RuntimeNative,
	}}
	document.Targets = []config.GitHubTargetConfiguration{{
		ID: "target-org", InstallationID: "installation-41",
		ScopeKind: domain.TargetOrganization, Scope: "example-org",
		ScaleSetName: "sparerunner", RunnerProfileID: "profile-default",
	}}
	payload, err := config.EncodeJSON(document)
	if err != nil {
		t.Fatal(err)
	}
	backend.targetVerifier = managementTargetVerifierFunc(func(
		_ context.Context,
		requested config.GitHubTargetConfiguration,
	) (store.ManagementGitHubTarget, error) {
		return store.ManagementGitHubTarget{
			Target: domain.GitHubTarget{
				ID: requested.ID, InstallationID: requested.InstallationID,
				ScopeKind: requested.ScopeKind, Scope: requested.Scope,
				Visibility: domain.TargetPrivate, RunnerGroupAccessSafe: true,
				ScaleSetName:    requested.ScaleSetName,
				RunnerProfileID: requested.RunnerProfileID,
			},
			ScaleSetID: 41,
		}, nil
	})
	if _, err := backend.ApplyConfiguration(
		ctx, current.Revision, "application/json", payload, managementTestRequestID,
	); err != nil {
		t.Fatal(err)
	}

	// A connected App plus an unverified Target runtime is the exact live state:
	// the App state must stay connected so the console keeps offering Target
	// creation. Before the fix this returned degraded.
	credentialStore := &github.MemoryAppCredentialStore{}
	if err := credentialStore.Save(managementTestAppCredential(t)); err != nil {
		t.Fatal(err)
	}
	authority, err := github.NewAuthority(github.AuthorityOptions{CredentialStore: credentialStore})
	if err != nil {
		t.Fatal(err)
	}
	backend.state.GitHubAuthority = authority

	setup, err := backend.Setup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if setup.GithubAppState != gen.SetupGithubAppStateConnected {
		t.Fatalf("connected app with an unverified target runtime = %q", setup.GithubAppState)
	}
	if setup.TargetCount != 1 {
		t.Fatalf("target count = %d", setup.TargetCount)
	}
	// The unverified runtime is still reported, in the place that describes
	// runtime rather than App authority.
	found := false
	for _, condition := range setup.Conditions {
		if condition.Code == "github_target_runtime_unverified" {
			if condition.Status != gen.ConditionStatusDegraded {
				t.Fatalf("runtime condition status = %q", condition.Status)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("unverified target runtime was not reported: %#v", setup.Conditions)
	}
	overview, err := backend.Overview(ctx)
	if err != nil {
		t.Fatal(err)
	}
	overviewFound := false
	for _, condition := range overview.Conditions {
		if condition.Code == "github_target_runtime_unverified" {
			overviewFound = true
		}
	}
	if !overviewFound {
		t.Fatalf("overview omitted the runtime condition: %#v", overview.Conditions)
	}
}

// managementTestAppCredential builds a syntactically valid App credential so a
// test can exercise the connected path without a platform credential store.
func managementTestAppCredential(t *testing.T) github.AppCredential {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	credential, err := github.NewAppCredential(4409279, "Iv1.testclientid", string(encoded))
	if err != nil {
		t.Fatal(err)
	}
	return credential
}

func TestManagementBackendConsumedJoinCodeCannotImplicitlyRevokeNode(t *testing.T) {
	ctx := context.Background()
	backend, _ := newManagementBackendForTest(t)

	tokenID, code, err := backend.CreateJoinCode(
		ctx,
		nil,
		managementTestRequestID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokenID) != 32 || !strings.HasPrefix(code, "spr_") {
		t.Fatalf("join delivery = token %q code %q", tokenID, code)
	}
	csr := managementTestCSR(t)
	enrolled, err := backend.state.Service.Enroll(ctx, code, csr)
	if err != nil {
		t.Fatal(err)
	}
	before, err := backend.state.Store.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.CancelJoinCode(
		ctx,
		tokenID,
		managementTestRequestID,
	); !errors.Is(err, managementapi.ErrResourceNotFound) {
		t.Fatalf("consumed join-code cancellation = %v", err)
	}
	if _, err := backend.state.Store.CurrentCredential(
		ctx,
		enrolled.NodeID,
	); err != nil {
		t.Fatalf("consumed-code cancellation revoked the node: %v", err)
	}
	after, err := backend.state.Store.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("failed cancellation appended audit: before=%d after=%d", len(before), len(after))
	}
	for _, event := range after {
		if strings.Contains(event.Record.ResourceID, code) {
			t.Fatal("join credential leaked into audit resource identity")
		}
	}
}

func TestManagementJoinCodeRejectsInvalidHintsBeforeAuditOrTokenMutation(t *testing.T) {
	ctx := context.Background()
	backend, _ := newManagementBackendForTest(t)
	beforeConfiguration, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	beforeAudits, err := backend.state.Store.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, hint := range []string{
		"https://controller.example.test/untrusted-path",
		"http://controller.example.test:7443",
		"controller.example.test",
	} {
		_, _, err := backend.CreateJoinCode(
			ctx,
			[]string{hint},
			managementTestRequestID,
		)
		var validation *managementapi.ValidationError
		if !errors.As(err, &validation) ||
			len(validation.Violations) != 1 ||
			validation.Violations[0].Field != "endpointHints" ||
			validation.Violations[0].Code != "invalid_endpoint_hint" {
			t.Fatalf("hint %q error = %#v", hint, err)
		}
		if !backend.state.Store.ManagementAuditHealthy() {
			t.Fatalf("invalid hint %q degraded audit authority", hint)
		}
	}

	afterConfiguration, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	afterAudits, err := backend.state.Store.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterConfiguration.Revision != beforeConfiguration.Revision ||
		len(afterAudits) != len(beforeAudits) {
		t.Fatalf(
			"invalid hints mutated state: revision %d -> %d, audits %d -> %d",
			beforeConfiguration.Revision,
			afterConfiguration.Revision,
			len(beforeAudits),
			len(afterAudits),
		)
	}
}

func TestManagementBackendNodeMutationUsesRevisionAndRevokesExplicitly(t *testing.T) {
	ctx := context.Background()
	backend, nodeID := newManagementBackendForTest(t)

	node, revision, err := backend.SetNodeAdministrativeState(
		ctx,
		nodeID,
		domain.NodeDraining,
		1,
		managementTestRequestID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if revision != "2" ||
		node.AdministrativeState != "draining" ||
		node.Reconciled {
		t.Fatalf("drained node = %#v revision=%q", node, revision)
	}
	_, _, err = backend.SetNodeAdministrativeState(
		ctx,
		nodeID,
		domain.NodeActive,
		1,
		managementTestRequestID,
	)
	var conflict *managementapi.RevisionConflict
	if !errors.As(err, &conflict) || conflict.Current != 2 {
		t.Fatalf("stale resume error = %#v", err)
	}
	if _, _, err := backend.SetNodeAdministrativeState(
		ctx,
		nodeID,
		domain.NodeActive,
		2,
		managementTestRequestID,
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := backend.SetNodeAdministrativeState(
		ctx,
		nodeID,
		domain.NodeRevoked,
		3,
		managementTestRequestID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.state.Store.CurrentCredential(ctx, string(nodeID)); !errors.Is(
		err,
		enroll.ErrCredentialRejected,
	) {
		t.Fatalf("explicit revoke credential result = %v", err)
	}
	events, err := backend.state.Store.ReadAuditEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 ||
		events[0].Record.Action != store.AuditActionNodeDrained ||
		events[1].Record.Action != store.AuditActionNodeResumed ||
		events[2].Record.Action != store.AuditActionNodeRevoked {
		t.Fatalf("node mutation audits = %#v", events)
	}
}

func newManagementBackendForTest(
	t *testing.T,
) (*managementBackend, domain.NodeID) {
	t.Helper()
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	controllerStore, err := store.OpenController(
		ctx,
		filepath.Join(directory, "controller.db"),
		store.Options{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := controllerStore.Close(); err != nil {
			t.Error(err)
		}
	})
	identity, err := enroll.NewControllerIdentity(time.Now(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var digestKey [32]byte
	for index := range digestKey {
		digestKey[index] = byte(index + 1)
	}
	service, err := enroll.NewService(controllerStore, identity, digestKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	code, err := service.CreateJoinCode(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	enrolled, err := service.Enroll(ctx, code, managementTestCSR(t))
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := controllerStore.AdvanceEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restart, err := controllerStore.RestartSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := reconcile.RestoreRestart(restart, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	state := &ControllerState{
		Identity:   identity,
		Store:      controllerStore,
		Service:    service,
		Reconciler: projection,
		Epoch:      uint64(epoch),
	}
	backend, err := newManagementBackend(state, nil)
	if err != nil {
		t.Fatal(err)
	}
	return backend, domain.NodeID(enrolled.NodeID)
}

func managementTestCSR(t *testing.T) []byte {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := enroll.CreateNodeCSR(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return csr
}

type managementTargetVerifierFunc func(
	context.Context,
	config.GitHubTargetConfiguration,
) (store.ManagementGitHubTarget, error)

func (verify managementTargetVerifierFunc) VerifyManagementTarget(
	ctx context.Context,
	target config.GitHubTargetConfiguration,
) (store.ManagementGitHubTarget, error) {
	return verify(ctx, target)
}

// TestNodesReportsAdoptedOwnerState proves the management API listing
// surfaces the node-owner-owned availability intent and exclusion set once
// they are adopted from an Agent snapshot, and omits them beforehand rather
// than inventing a default.
func TestNodesReportsAdoptedOwnerState(t *testing.T) {
	ctx := context.Background()
	backend, nodeID := newManagementBackendForTest(t)

	before, _, err := backend.Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 {
		t.Fatalf("nodes before snapshot = %#v, want exactly one", before)
	}
	if before[0].AvailabilityIntent != nil || before[0].ExcludedTargets != nil {
		t.Fatalf("node reported owner state before any snapshot: %#v", before[0])
	}

	if err := backend.state.Store.RecordAgentSnapshot(ctx, store.NodeAgentSnapshot{
		NodeID:             nodeID,
		OS:                 domain.OSLinux,
		Architecture:       domain.ArchAMD64,
		RunnerVersion:      "0.0.0",
		NativeRunnerReady:  true,
		AvailabilityIntent: domain.AvailabilityStopped,
		ExcludedTargets:    []domain.TargetID{"target-b", "target-a"},
		Journal:            store.AgentSnapshot{MaxControllerEpoch: domain.ControllerEpoch(backend.state.Epoch)},
	}); err != nil {
		t.Fatal(err)
	}

	after, _, err := backend.Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 {
		t.Fatalf("nodes after snapshot = %#v, want exactly one", after)
	}
	node := after[0]
	if node.AvailabilityIntent == nil || *node.AvailabilityIntent != gen.Stopped {
		t.Fatalf("node availability intent = %v, want stopped", node.AvailabilityIntent)
	}
	if node.ExcludedTargets == nil || len(*node.ExcludedTargets) != 2 ||
		(*node.ExcludedTargets)[0] != "target-a" || (*node.ExcludedTargets)[1] != "target-b" {
		t.Fatalf("node excluded targets = %v, want sorted [target-a target-b]", node.ExcludedTargets)
	}
}

type cancelAfterEnrollmentConsumeRegistry struct {
	enroll.Registry
	cancel context.CancelFunc
}

func (registry cancelAfterEnrollmentConsumeRegistry) ConsumeEnrollment(
	ctx context.Context,
	token enroll.TokenRecord,
	node enroll.NodeRecord,
) error {
	err := registry.Registry.ConsumeEnrollment(ctx, token, node)
	if err == nil {
		registry.cancel()
	}
	return err
}
