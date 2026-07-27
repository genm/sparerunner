package app

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	managementapi "github.com/genm/tewake/internal/api"
	"github.com/genm/tewake/internal/api/gen"
	"github.com/genm/tewake/internal/buildinfo"
	"github.com/genm/tewake/internal/config"
	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/enroll"
	"github.com/genm/tewake/internal/reconcile"
	"github.com/genm/tewake/internal/runner"
	"github.com/genm/tewake/internal/scheduler"
	"github.com/genm/tewake/internal/store"
)

// ManagementTargetVerifier is the only path that may turn operator-controlled
// target fields into private/safe provider authority. A disconnected Controller
// deliberately leaves this nil and rejects new or identity-changing targets.
// Implementations return a managementapi.ValidationError for an authoritatively
// unsafe/public target and another error for unavailable or uncertain authority.
// Verification is strictly read-only: provider provisioning belongs to a later
// durable intent/reconciler boundary so a concurrent configuration CAS failure
// cannot orphan an external scale set.
type ManagementTargetVerifier interface {
	VerifyManagementTarget(
		context.Context,
		config.GitHubTargetConfiguration,
	) (store.ManagementGitHubTarget, error)
}

type managementBackend struct {
	state          *ControllerState
	targetVerifier ManagementTargetVerifier
	mutationMu     sync.Mutex
}

func newManagementBackend(
	state *ControllerState,
	targetVerifier ManagementTargetVerifier,
) (*managementBackend, error) {
	if state == nil || state.Store == nil || state.Reconciler == nil ||
		state.Epoch == 0 ||
		state.Reconciler.Epoch() != domain.ControllerEpoch(state.Epoch) {
		return nil, managementapi.ErrBackendUnavailable
	}
	return &managementBackend{state: state, targetVerifier: targetVerifier}, nil
}

func (backend *managementBackend) RecordAudit(
	ctx context.Context,
	input managementapi.AuditInput,
) error {
	record, err := managementAuditRecord(input)
	if err != nil {
		return err
	}
	_, err = backend.state.Store.AppendAuditEvent(ctx, record)
	return err
}

func (backend *managementBackend) AuditHealthy() bool {
	return backend != nil && backend.state != nil &&
		backend.state.Store.ManagementAuditHealthy()
}

func (backend *managementBackend) Setup(ctx context.Context) (gen.Setup, error) {
	configuration, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		return gen.Setup{}, err
	}
	appState := gen.SetupGithubAppStateDisconnected
	if len(configuration.GitHubTargets) > 0 {
		appState = gen.SetupGithubAppStateConnected
		for _, target := range configuration.GitHubTargets {
			freshness, freshnessErr := backend.state.Store.ReadGitHubRuntimeFreshness(
				ctx,
				store.GitHubTargetRuntimeBinding{
					TargetID:   target.Target.ID,
					ScaleSetID: target.ScaleSetID,
					ProfileID:  target.Target.RunnerProfileID,
				},
			)
			if freshnessErr != nil ||
				combinedRuntimeFreshness(freshness) != store.RuntimeFreshnessFresh {
				appState = gen.SetupGithubAppStateDegraded
				break
			}
		}
	}
	return gen.Setup{
		ControllerInitialized: true,
		GithubAppState:        appState,
		ManifestFlowSupported: false,
		NodeCount:             len(configuration.Nodes),
		TargetCount:           len(configuration.GitHubTargets),
		Conditions:            backend.conditions(),
	}, nil
}

func (backend *managementBackend) Overview(ctx context.Context) (gen.Overview, error) {
	configuration, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		return gen.Overview{}, err
	}
	snapshot, err := backend.state.Store.Snapshot(ctx)
	if err != nil {
		return gen.Overview{}, err
	}
	capacity := managementConfiguredCapacity(configuration)
	active := 0
	for _, execution := range snapshot.Executions {
		switch execution.State {
		case domain.ExecutionReleased, domain.ExecutionFailed,
			domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
		default:
			active++
		}
	}
	return gen.Overview{
		Version:            buildinfo.String(),
		ControllerEpoch:    strconv.FormatUint(backend.state.Epoch, 10),
		ConfiguredCapacity: capacity,
		ActiveRuns:         active,
		NodeCount:          len(configuration.Nodes),
		TargetCount:        len(configuration.GitHubTargets),
		Conditions:         backend.conditions(),
	}, nil
}

func (backend *managementBackend) Nodes(
	ctx context.Context,
) ([]gen.Node, gen.Revision, error) {
	configuration, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		return nil, "", err
	}
	durable, err := backend.state.Store.Snapshot(ctx)
	if err != nil {
		return nil, "", err
	}
	fleet := backend.state.Reconciler.FleetSnapshot()
	administrative := make(map[domain.NodeID]domain.NodeAdministrativeState, len(durable.Nodes))
	for _, node := range durable.Nodes {
		administrative[node.NodeID] = node.State
	}
	observed := make(map[domain.NodeID]scheduler.NodeSnapshot, len(fleet.Nodes))
	for _, node := range fleet.Nodes {
		observed[node.Node.ID] = node
	}
	statuses := make(map[domain.NodeID]reconcile.NodeStatus, len(fleet.Statuses))
	for _, status := range fleet.Statuses {
		statuses[status.NodeID] = status
	}
	active := make(map[domain.NodeID]int)
	for _, execution := range durable.Executions {
		switch execution.State {
		case domain.ExecutionReleased, domain.ExecutionFailed,
			domain.ExecutionCleanupFailed, domain.ExecutionQuarantined:
		default:
			active[execution.Slot.NodeID]++
		}
	}
	result := make([]gen.Node, 0, len(configuration.Nodes))
	for _, configured := range configuration.Nodes {
		state := administrative[configured.NodeID]
		status := statuses[configured.NodeID]
		node := gen.Node{
			Id:                  string(configured.NodeID),
			DisplayName:         configured.DisplayName,
			AdministrativeState: gen.NodeAdministrativeState(state),
			ObservedState:       managementObservedState(status.Phase),
			MaxRunners:          configured.MaxRunners,
			ActiveRunnerCount:   active[configured.NodeID],
			Reconciled:          false,
			NativeRunnerReady:   false,
		}
		if status.Reason != reconcile.ReasonNone {
			reason := string(status.Reason)
			node.StatusReason = &reason
		}
		if snapshot, found := observed[configured.NodeID]; found {
			operatingSystem := gen.NodeOperatingSystem(snapshot.Node.OS)
			architecture := gen.NodeArchitecture(snapshot.Node.Architecture)
			memory := strconv.FormatUint(snapshot.AvailableMemoryBytes, 10)
			node.OperatingSystem = &operatingSystem
			node.Architecture = &architecture
			node.AvailableMemoryBytes = &memory
			node.Reconciled = snapshot.Reconciled
			node.NativeRunnerReady = snapshot.NativeReady
		}
		result = append(result, node)
	}
	return result, revisionString(configuration.Revision), nil
}

func (backend *managementBackend) Targets(
	ctx context.Context,
) ([]gen.Target, gen.Revision, error) {
	configuration, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		return nil, "", err
	}
	result := make([]gen.Target, 0, len(configuration.GitHubTargets))
	for _, configured := range configuration.GitHubTargets {
		target := gen.Target{
			Id:              string(configured.Target.ID),
			InstallationId:  configured.Target.InstallationID,
			ScopeKind:       gen.TargetScopeKind(configured.Target.ScopeKind),
			Scope:           configured.Target.Scope,
			ScaleSetName:    configured.Target.ScaleSetName,
			RunnerProfileId: string(configured.Target.RunnerProfileID),
			Status:          gen.TargetStatusReconciling,
			Freshness:       gen.Freshness{State: gen.FreshnessStateUnknown},
		}
		runtimeState, runtimeErr := backend.state.Store.ReadGitHubRuntimeFreshness(
			ctx,
			store.GitHubTargetRuntimeBinding{
				TargetID:   configured.Target.ID,
				ScaleSetID: configured.ScaleSetID,
				ProfileID:  configured.Target.RunnerProfileID,
			},
		)
		if runtimeErr == nil {
			target.Freshness = managementFreshness(runtimeState)
			switch target.Freshness.State {
			case gen.FreshnessStateFresh:
				target.Status = gen.TargetStatusReady
			case gen.FreshnessStateStale:
				target.Status = gen.TargetStatusDegraded
			}
		} else {
			// The configured target remains visible with explicit unknown
			// freshness; a read failure never fabricates an empty healthy list.
			target.Status = gen.TargetStatusDegraded
		}
		result = append(result, target)
	}
	return result, revisionString(configuration.Revision), nil
}

func (backend *managementBackend) Runs(ctx context.Context) ([]gen.Run, error) {
	snapshot, err := backend.state.Store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]gen.Run, 0, len(snapshot.Executions))
	for _, execution := range snapshot.Executions {
		result = append(result, gen.Run{
			Id:        string(execution.ID),
			TargetId:  string(execution.TargetID),
			NodeId:    string(execution.Slot.NodeID),
			SlotIndex: execution.Slot.Index,
			State:     gen.RunState(execution.State),
		})
	}
	return result, nil
}

func (backend *managementBackend) AuditEvents(
	ctx context.Context,
	after uint64,
	limit int,
) (managementapi.AuditPage, error) {
	page, err := backend.state.Store.ReadAuditEventsPage(ctx, after, limit)
	if err != nil {
		return managementapi.AuditPage{}, err
	}
	result := make([]gen.AuditEvent, 0, len(page.Events))
	for _, event := range page.Events {
		outcome := gen.AuditEventOutcome(event.Record.Outcome)
		item := gen.AuditEvent{
			Id:           fmt.Sprintf("audit-%d", event.Sequence),
			OccurredAt:   time.Unix(0, event.OccurredAtUnixNano).UTC(),
			Actor:        gen.AuditEventActor(event.Record.Actor),
			Action:       string(event.Record.Action),
			Outcome:      outcome,
			ResourceType: string(event.Record.ResourceKind),
			ResourceId:   event.Record.ResourceID,
			RequestId:    event.Record.RequestID,
		}
		if event.Record.ErrorCode != "" {
			code := string(event.Record.ErrorCode)
			item.ErrorCode = &code
		}
		result = append(result, item)
	}
	return managementapi.AuditPage{
		Events:    result,
		NextAfter: page.NextAfter,
	}, nil
}

func (backend *managementBackend) ReadConfiguration(
	ctx context.Context,
) (gen.Configuration, error) {
	configuration, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		return gen.Configuration{}, err
	}
	return managementConfigurationDTO(configuration)
}

func (backend *managementBackend) ApplyConfiguration(
	ctx context.Context,
	expected uint64,
	mediaType string,
	payload []byte,
	requestID string,
) (gen.Configuration, error) {
	backend.mutationMu.Lock()
	defer backend.mutationMu.Unlock()

	var document config.Configuration
	var err error
	switch mediaType {
	case "application/json":
		document, err = config.DecodeJSON(bytes.NewReader(payload))
	case "application/yaml":
		document, err = config.DecodeYAML(bytes.NewReader(payload))
	default:
		return gen.Configuration{}, &managementapi.ValidationError{
			Violations: []managementapi.FieldViolation{{
				Field: "contentType", Code: "unsupported", Message: "must be JSON or YAML",
			}},
		}
	}
	if err != nil {
		return gen.Configuration{}, managementConfigurationError(err)
	}
	current, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		return gen.Configuration{}, err
	}
	if current.Revision != expected || uint64(document.Revision) != expected {
		return gen.Configuration{}, &managementapi.RevisionConflict{
			Expected: expected,
			Current:  current.Revision,
		}
	}
	if err := validateManagementConfigurationResponseBudget(
		document,
		expected+1,
	); err != nil {
		return gen.Configuration{}, err
	}
	if err := backend.ensureDurableNodeProjection(ctx); err != nil {
		return gen.Configuration{}, err
	}
	desired := desiredManagementConfiguration(document)
	verified, err := backend.resolveManagementAuthorities(ctx, current, document)
	if err != nil {
		return gen.Configuration{}, err
	}
	applied, err := backend.state.Store.ApplyManagementConfiguration(
		ctx,
		expected,
		desired,
		verified,
		store.AuditRecord{
			Actor:        store.AuditActorSingleAdmin,
			Action:       store.AuditActionConfigurationApplied,
			Outcome:      store.AuditOutcomeSucceeded,
			ResourceKind: store.AuditResourceConfiguration,
			RequestID:    requestID,
		},
	)
	if err != nil {
		return gen.Configuration{}, managementStoreError(err)
	}
	if err := backend.state.Reconciler.ApplyNodeConfigurations(
		managementNodeConfigurations(applied.Nodes),
	); err != nil {
		backend.state.Reconciler.MarkManagementProjectionUnavailable()
		return gen.Configuration{}, &managementapi.CommittedMutationError{
			Current: applied.Revision,
		}
	}
	result, err := managementConfigurationDTO(applied)
	if err != nil {
		return gen.Configuration{}, &managementapi.CommittedMutationError{
			Current: applied.Revision,
		}
	}
	return result, nil
}

func (backend *managementBackend) ExportConfiguration(
	ctx context.Context,
) ([]byte, gen.Revision, error) {
	configuration, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		return nil, "", err
	}
	document, err := managementConfigurationDocument(configuration)
	if err != nil {
		return nil, "", err
	}
	encoded, err := config.EncodeYAML(document)
	if err != nil {
		return nil, "", err
	}
	return encoded, revisionString(configuration.Revision), nil
}

func (backend *managementBackend) CreateJoinCode(
	ctx context.Context,
	hints []string,
	requestID string,
) (string, string, error) {
	canonicalHints, err := enroll.CanonicalHints(hints)
	if err != nil {
		return "", "", &managementapi.ValidationError{
			Violations: []managementapi.FieldViolation{{
				Field: "endpointHints", Code: "invalid_endpoint_hint",
				Message: "Endpoint hints must be canonical HTTPS or host:port authorities.",
			}},
		}
	}
	service := backend.state.Service
	service.Registry = auditedJoinCodeRegistry{
		Registry:  service.Registry,
		store:     backend.state.Store,
		requestID: requestID,
	}
	delivery, err := service.CreateJoinCodeDelivery(ctx, canonicalHints)
	if err != nil {
		return "", "", managementStoreError(err)
	}
	return hex.EncodeToString(delivery.TokenID[:]), delivery.Encoded(), nil
}

func (backend *managementBackend) CancelJoinCode(
	ctx context.Context,
	tokenID string,
	requestID string,
) error {
	raw, err := hex.DecodeString(tokenID)
	if err != nil || len(raw) != 16 {
		return managementapi.ErrResourceNotFound
	}
	var id [16]byte
	copy(id[:], raw)
	err = backend.state.Store.CancelTokenWithAudit(ctx, id, store.AuditRecord{
		Actor:        store.AuditActorSingleAdmin,
		Action:       store.AuditActionJoinCodeCancelled,
		Outcome:      store.AuditOutcomeSucceeded,
		ResourceKind: store.AuditResourceJoinCode,
		ResourceID:   tokenID,
		RequestID:    requestID,
	})
	if errors.Is(err, enroll.ErrTokenNotFound) {
		return managementapi.ErrResourceNotFound
	}
	return managementStoreError(err)
}

func (backend *managementBackend) SetNodeAdministrativeState(
	ctx context.Context,
	nodeID domain.NodeID,
	next domain.NodeAdministrativeState,
	expected uint64,
	requestID string,
) (gen.Node, gen.Revision, error) {
	backend.mutationMu.Lock()
	defer backend.mutationMu.Unlock()

	action := store.AuditActionNodeDrained
	switch next {
	case domain.NodeActive:
		action = store.AuditActionNodeResumed
	case domain.NodeDraining:
	case domain.NodeRevoked:
		action = store.AuditActionNodeRevoked
	default:
		return gen.Node{}, "", managementapi.ErrDomainConflict
	}
	if err := backend.ensureDurableNodeProjection(ctx); err != nil {
		return gen.Node{}, "", err
	}
	revision, err := backend.state.Store.SetNodeAdministrativeStateWithAudit(
		ctx,
		nodeID,
		next,
		expected,
		store.AuditRecord{
			Actor:        store.AuditActorSingleAdmin,
			Action:       action,
			Outcome:      store.AuditOutcomeSucceeded,
			ResourceKind: store.AuditResourceNode,
			ResourceID:   string(nodeID),
			RequestID:    requestID,
		},
	)
	if err != nil {
		return gen.Node{}, "", managementStoreError(err)
	}
	if err := backend.state.Reconciler.SetAdministrativeState(nodeID, next, false); err != nil {
		backend.state.Reconciler.MarkManagementProjectionUnavailable()
		return gen.Node{}, "", &managementapi.CommittedMutationError{
			Current: revision,
		}
	}
	nodes, _, err := backend.Nodes(ctx)
	if err != nil {
		return gen.Node{}, "", &managementapi.CommittedMutationError{
			Current: revision,
		}
	}
	for _, node := range nodes {
		if node.Id == string(nodeID) {
			return node, revisionString(revision), nil
		}
	}
	return gen.Node{}, "", &managementapi.CommittedMutationError{
		Current: revision,
	}
}

func (backend *managementBackend) CurrentRevision(
	ctx context.Context,
) (gen.Revision, error) {
	configuration, err := backend.state.Store.ReadManagementConfiguration(ctx)
	if err != nil {
		return "", err
	}
	return revisionString(configuration.Revision), nil
}

func (backend *managementBackend) conditions() []gen.Condition {
	var conditions []gen.Condition
	if !backend.AuditHealthy() {
		conditions = append(conditions, gen.Condition{
			Code:   "audit_persistence_unavailable",
			Status: gen.ConditionStatusUnavailable,
		})
	}
	if backend == nil || backend.state == nil ||
		backend.state.Reconciler == nil ||
		!backend.state.Reconciler.ManagementProjectionHealthy() {
		conditions = append(conditions, gen.Condition{
			Code:   "management_projection_unavailable",
			Status: gen.ConditionStatusUnavailable,
		})
	}
	return conditions
}

func (backend *managementBackend) ensureDurableNodeProjection(
	ctx context.Context,
) error {
	if backend == nil || backend.state == nil ||
		backend.state.Store == nil || backend.state.Reconciler == nil ||
		!backend.state.Reconciler.ManagementProjectionHealthy() {
		return managementapi.ErrBackendUnavailable
	}
	projectionContext, cancel := detachedManagementProjectionContext(ctx)
	defer cancel()
	restart, err := backend.state.Store.RestartSnapshot(projectionContext)
	if err != nil ||
		restart.Controller.ControllerEpoch != backend.state.Reconciler.Epoch() {
		backend.state.Reconciler.MarkManagementProjectionUnavailable()
		return managementapi.ErrBackendUnavailable
	}
	for _, topology := range restart.NodeTopology {
		if err := backend.state.Reconciler.EnsureRestartNode(topology); err != nil {
			backend.state.Reconciler.MarkManagementProjectionUnavailable()
			return managementapi.ErrBackendUnavailable
		}
	}
	return nil
}

func (backend *managementBackend) resolveManagementAuthorities(
	ctx context.Context,
	current store.ManagementConfiguration,
	document config.Configuration,
) (store.ManagementVerifiedAuthorities, error) {
	currentProfiles := make(map[domain.RunnerProfileID]struct{}, len(current.RunnerProfiles))
	for _, profile := range current.RunnerProfiles {
		currentProfiles[profile.Profile.ID] = struct{}{}
	}
	currentTargets := make(
		map[domain.TargetID]store.ManagementGitHubTarget,
		len(current.GitHubTargets),
	)
	for _, target := range current.GitHubTargets {
		currentTargets[target.Target.ID] = target
	}
	var verified store.ManagementVerifiedAuthorities
	for _, profile := range document.RunnerProfiles {
		if _, exists := currentProfiles[profile.ID]; exists {
			continue
		}
		verified.RunnerProfiles = append(
			verified.RunnerProfiles,
			store.ManagementRunnerProfile{
				Profile:       profile.AsDomain(),
				RunnerVersion: runner.OfficialRunnerVersion,
			},
		)
	}
	for _, target := range document.Targets {
		existing, exists := currentTargets[target.ID]
		if exists && sameManagementTargetIdentity(existing.Target, target) {
			continue
		}
		if backend.targetVerifier == nil {
			return store.ManagementVerifiedAuthorities{}, managementapi.ErrBackendUnavailable
		}
		authority, err := backend.targetVerifier.VerifyManagementTarget(ctx, target)
		if err != nil {
			var validation *managementapi.ValidationError
			if errors.As(err, &validation) {
				return store.ManagementVerifiedAuthorities{}, validation
			}
			return store.ManagementVerifiedAuthorities{}, managementapi.ErrBackendUnavailable
		}
		verified.GitHubTargets = append(verified.GitHubTargets, authority)
	}
	return verified, nil
}

type auditedJoinCodeRegistry struct {
	enroll.Registry
	store     *store.ControllerStore
	requestID string
}

func (registry auditedJoinCodeRegistry) CreateToken(
	ctx context.Context,
	token enroll.TokenRecord,
) error {
	tokenID := hex.EncodeToString(token.ID[:])
	return registry.store.CreateTokenWithAudit(ctx, token, store.AuditRecord{
		Actor:        store.AuditActorSingleAdmin,
		Action:       store.AuditActionJoinCodeCreated,
		Outcome:      store.AuditOutcomeSucceeded,
		ResourceKind: store.AuditResourceJoinCode,
		ResourceID:   tokenID,
		RequestID:    registry.requestID,
	})
}

func managementConfigurationDTO(
	configuration store.ManagementConfiguration,
) (gen.Configuration, error) {
	document, err := managementConfigurationDocument(configuration)
	if err != nil {
		return gen.Configuration{}, err
	}
	encoded, err := config.EncodeJSON(document)
	if err != nil {
		return gen.Configuration{}, err
	}
	var result gen.Configuration
	if err := json.Unmarshal(encoded, &result); err != nil {
		return gen.Configuration{}, err
	}
	return result, nil
}

func managementConfiguredCapacity(configuration store.ManagementConfiguration) int {
	limit := math.MaxInt
	if configuration.FleetMaxRunners != nil {
		limit = *configuration.FleetMaxRunners
	}
	capacity := 0
	for _, node := range configuration.Nodes {
		if node.MaxRunners >= limit-capacity {
			return limit
		}
		capacity += node.MaxRunners
	}
	return capacity
}

func validateManagementConfigurationResponseBudget(
	document config.Configuration,
	nextRevision uint64,
) error {
	document.Revision = config.DecimalUint64(nextRevision)
	encoded, err := config.EncodeJSON(document)
	if err != nil {
		return managementConfigurationError(err)
	}
	var response gen.Configuration
	if err := json.Unmarshal(encoded, &response); err != nil {
		return err
	}
	wire, err := json.Marshal(response)
	if err != nil {
		return err
	}
	export, err := config.EncodeYAML(document)
	if err != nil {
		return managementConfigurationError(err)
	}
	// The API encoder appends one newline. Validate the exact success response
	// and the later YAML export before committing, so a client never reports
	// failure after the revision has already advanced.
	if int64(len(wire))+1 > config.RequestBodyLimitBytes ||
		int64(len(export)) > config.RequestBodyLimitBytes {
		return &managementapi.ValidationError{
			Violations: []managementapi.FieldViolation{{
				Field: "configuration",
				Code:  "response_too_large",
				Message: "The canonical configuration response exceeds the " +
					"measured management transport budget.",
			}},
		}
	}
	return nil
}

func managementConfigurationDocument(
	configuration store.ManagementConfiguration,
) (config.Configuration, error) {
	result := config.Configuration{
		SchemaVersion: config.SchemaVersion,
		Revision:      config.DecimalUint64(configuration.Revision),
		Scheduler: config.SchedulerConfiguration{
			MaxRunners: cloneOptionalInt(configuration.FleetMaxRunners),
		},
		Nodes:          make([]config.NodeConfiguration, 0, len(configuration.Nodes)),
		RunnerProfiles: make([]config.RunnerProfileConfiguration, 0, len(configuration.RunnerProfiles)),
		Targets:        make([]config.GitHubTargetConfiguration, 0, len(configuration.GitHubTargets)),
	}
	for _, node := range configuration.Nodes {
		result.Nodes = append(result.Nodes, config.NodeConfiguration{
			ID:          node.NodeID,
			DisplayName: node.DisplayName,
			MaxRunners:  node.MaxRunners,
		})
	}
	for _, profile := range configuration.RunnerProfiles {
		result.RunnerProfiles = append(
			result.RunnerProfiles,
			config.RunnerProfileFromDomain(profile.Profile),
		)
	}
	for _, target := range configuration.GitHubTargets {
		exported, err := config.TargetFromVerifiedDomain(target.Target)
		if err != nil {
			return config.Configuration{}, err
		}
		result.Targets = append(result.Targets, exported)
	}
	if err := result.Validate(); err != nil {
		return config.Configuration{}, err
	}
	return result, nil
}

func desiredManagementConfiguration(
	document config.Configuration,
) store.DesiredManagementConfiguration {
	result := store.DesiredManagementConfiguration{
		FleetMaxRunners: cloneOptionalInt(document.Scheduler.MaxRunners),
		Nodes:           make([]store.ManagementNodeConfiguration, 0, len(document.Nodes)),
		RunnerProfiles:  make([]domain.RunnerProfile, 0, len(document.RunnerProfiles)),
		GitHubTargets:   make([]store.DesiredManagementGitHubTarget, 0, len(document.Targets)),
	}
	for _, node := range document.Nodes {
		result.Nodes = append(result.Nodes, store.ManagementNodeConfiguration{
			NodeID: node.ID, DisplayName: node.DisplayName, MaxRunners: node.MaxRunners,
		})
	}
	for _, profile := range document.RunnerProfiles {
		result.RunnerProfiles = append(result.RunnerProfiles, profile.AsDomain())
	}
	for _, target := range document.Targets {
		result.GitHubTargets = append(result.GitHubTargets, store.DesiredManagementGitHubTarget{
			ID:              target.ID,
			InstallationID:  target.InstallationID,
			ScopeKind:       target.ScopeKind,
			Scope:           target.Scope,
			ScaleSetName:    target.ScaleSetName,
			RunnerProfileID: target.RunnerProfileID,
		})
	}
	return result
}

func managementNodeConfigurations(
	nodes []store.ManagementNodeConfiguration,
) []reconcile.NodeConfiguration {
	result := make([]reconcile.NodeConfiguration, len(nodes))
	for index, node := range nodes {
		result[index] = reconcile.NodeConfiguration{
			NodeID:      node.NodeID,
			DisplayName: node.DisplayName,
			MaxRunners:  node.MaxRunners,
		}
	}
	return result
}

func managementAuditRecord(input managementapi.AuditInput) (store.AuditRecord, error) {
	record := store.AuditRecord{
		Actor:        store.AuditActor(input.Actor),
		Action:       store.AuditAction(input.Action),
		Outcome:      store.AuditOutcome(input.Outcome),
		ResourceKind: store.AuditResourceKind(input.ResourceType),
		ResourceID:   input.ResourceID,
		ErrorCode:    store.AuditErrorCode(input.ErrorCode),
		RequestID:    input.RequestID,
	}
	return record, nil
}

func managementConfigurationError(err error) error {
	var validation *config.ValidationError
	if errors.As(err, &validation) {
		return &managementapi.ValidationError{
			Violations: []managementapi.FieldViolation{{
				Field: validation.Field, Code: validation.Code,
				Message: "The field does not satisfy the configuration contract.",
			}},
		}
	}
	return &managementapi.ValidationError{
		Violations: []managementapi.FieldViolation{{
			Field: "configuration", Code: "invalid_document",
			Message: "The configuration document is invalid.",
		}},
	}
}

func managementStoreError(err error) error {
	if err == nil {
		return nil
	}
	var stale *store.StaleManagementRevisionError
	switch {
	case errors.As(err, &stale):
		return &managementapi.RevisionConflict{
			Expected: stale.Expected,
			Current:  stale.Actual,
		}
	case errors.Is(err, store.ErrManagementConfiguration):
		return &managementapi.ValidationError{
			Violations: []managementapi.FieldViolation{{
				Field: "configuration", Code: "invalid_configuration",
				Message: "The configuration conflicts with durable controller authority.",
			}},
		}
	case errors.Is(err, enroll.ErrNodeNotFound), errors.Is(err, enroll.ErrTokenNotFound):
		return managementapi.ErrResourceNotFound
	default:
		return err
	}
}

func sameManagementTargetIdentity(
	current domain.GitHubTarget,
	desired config.GitHubTargetConfiguration,
) bool {
	return current.ID == desired.ID &&
		current.InstallationID == desired.InstallationID &&
		current.ScopeKind == desired.ScopeKind &&
		current.Scope == desired.Scope &&
		current.ScaleSetName == desired.ScaleSetName
}

func combinedRuntimeFreshness(
	freshness store.GitHubRuntimeFreshness,
) store.RuntimeFreshness {
	if freshness.Release.Freshness == store.RuntimeFreshnessStale ||
		freshness.Session.Freshness == store.RuntimeFreshnessStale {
		return store.RuntimeFreshnessStale
	}
	if freshness.Release.Freshness == store.RuntimeFreshnessFresh &&
		freshness.Session.Freshness == store.RuntimeFreshnessFresh {
		return store.RuntimeFreshnessFresh
	}
	return store.RuntimeFreshnessUnknown
}

func managementFreshness(runtimeState store.GitHubRuntimeFreshness) gen.Freshness {
	state := combinedRuntimeFreshness(runtimeState)
	result := gen.Freshness{State: gen.FreshnessState(state)}
	observedAt := runtimeState.Release.ObservedAtUnixNano
	if runtimeState.Session.LastSuccessAtUnixNano > observedAt {
		observedAt = runtimeState.Session.LastSuccessAtUnixNano
	}
	if observedAt > 0 {
		value := time.Unix(0, observedAt).UTC()
		result.ObservedAt = &value
	}
	failedAt := runtimeState.Release.FailureAtUnixNano
	failureCode := runtimeState.Release.FailureClass
	if runtimeState.Session.FailureAtUnixNano > failedAt {
		failedAt = runtimeState.Session.FailureAtUnixNano
		failureCode = runtimeState.Session.FailureClass
	}
	if failedAt > 0 {
		value := time.Unix(0, failedAt).UTC()
		code := string(failureCode)
		result.FailedAt = &value
		result.FailureCode = &code
	}
	return result
}

func managementObservedState(phase reconcile.NodePhase) gen.NodeObservedState {
	switch phase {
	case reconcile.NodeReady:
		return gen.NodeObservedStateOnline
	case reconcile.NodeOffline:
		return gen.NodeObservedStateOffline
	case reconcile.NodeDegraded:
		return gen.NodeObservedStateStale
	default:
		return gen.NodeObservedStateReconciling
	}
}

func revisionString(revision uint64) gen.Revision {
	return strconv.FormatUint(revision, 10)
}

func cloneOptionalInt(value *int) *int {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
