package app

import (
	"context"
	"encoding/hex"
	"errors"
	"sort"
	"sync"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/reconcile"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

type controllerAgentStore interface {
	CommitAgentCommand(context.Context, store.IssuedAgentCommand) (bool, error)
	ReplayAgentCommand(context.Context, store.IssuedAgentCommand, string) (bool, error)
	CommitAgentReconciliationCommand(context.Context, store.IssuedAgentCommand, string) (bool, error)
	AgentCommandIsReconciliation(context.Context, domain.CommandID) (bool, error)
	RecordAgentSnapshot(context.Context, store.NodeAgentSnapshot) error
	RecordAgentReadiness(context.Context, domain.NodeID, string, bool) error
	RecordAgentDisconnect(context.Context, domain.NodeID, string) error
	RecordAgentExecutionUpdate(context.Context, store.AgentExecutionUpdate) (bool, error)
	ReadManagementConfiguration(context.Context) (store.ManagementConfiguration, error)
	// ManagementConfigurationRevision is the cheap poll primitive EligibleTargets
	// uses to avoid paying for ReadManagementConfiguration's full transaction at
	// heartbeat cadence (1 Hz per node) when nothing has changed.
	ManagementConfigurationRevision(context.Context) (uint64, error)
}

type storeBackedAgentConsumers struct {
	store      controllerAgentStore
	projection controllerAgentProjection
	// eligibility is a pointer so every storeBackedAgentConsumers value copied
	// out of newStoreBackedAgentConsumers (one per AgentConsumers field) shares
	// one cache. A mutex embedded directly in this struct would be silently
	// duplicated by those copies instead of guarding shared state.
	eligibility *eligibilityCache
}

// eligibilityCache holds the last configuration read keyed by its revision.
// EligibleTargets is called once per node per heartbeat interval, so without
// this cache every node would force a full ReadManagementConfiguration SQLite
// transaction at that cadence even when configuration never changes.
type eligibilityCache struct {
	mu       sync.Mutex
	loaded   bool
	revision uint64
	profiles map[domain.RunnerProfileID]domain.RunnerProfile
	targets  []store.ManagementGitHubTarget
}

type controllerAgentProjection interface {
	ApplyIssuedCommand(reconcile.IssuedCommand) error
	ApplyAgentReadiness(domain.NodeID, bool) error
	ApplyExecutionUpdate(transport.ExecutionUpdate) error
	ApplyReconciliationExecutionUpdate(transport.ExecutionUpdate) error
	HandleAgentDisconnect(context.Context, domain.NodeID) error
}

func newStoreBackedAgentConsumers(
	agentStore controllerAgentStore,
	projections ...controllerAgentProjection,
) AgentConsumers {
	consumers := storeBackedAgentConsumers{store: agentStore, eligibility: &eligibilityCache{}}
	if len(projections) > 0 {
		consumers.projection = projections[0]
	}
	return AgentConsumers{
		Commands:         consumers,
		Snapshot:         consumers,
		Readiness:        consumers,
		ExecutionUpdates: consumers,
		Disconnects:      consumers,
		Eligibility:      consumers,
	}
}

// EligibleTargets is read-only display data computed from current
// configuration: which private GitHub Targets have a Runner Profile whose
// optional OS/architecture constraint matches this node. It never consults
// online/reconciled/capacity state, so it answers "could this node ever serve
// this scope" rather than "is a slot free right now."
func (consumers storeBackedAgentConsumers) EligibleTargets(
	ctx context.Context,
	os domain.OperatingSystem,
	architecture domain.Architecture,
) ([]transport.EligibleTarget, error) {
	if consumers.store == nil || consumers.eligibility == nil {
		return nil, ErrAgentCommandConsumerRequired
	}
	profiles, targets, err := consumers.eligibility.read(ctx, consumers.store)
	if err != nil {
		return nil, err
	}
	var eligible []transport.EligibleTarget
	for _, target := range targets {
		profile, known := profiles[target.Target.RunnerProfileID]
		if !known {
			continue
		}
		if profile.OS != nil && *profile.OS != os {
			continue
		}
		if profile.Architecture != nil && *profile.Architecture != architecture {
			continue
		}
		eligible = append(eligible, transport.EligibleTarget{
			TargetID:     target.Target.ID,
			ScopeKind:    target.Target.ScopeKind,
			Scope:        target.Target.Scope,
			ScaleSetName: target.Target.ScaleSetName,
			// The controller does not yet consult ExcludedTargets; adoption
			// lands in a later PR alongside its snapshot persistence.
			Excluded: false,
		})
	}
	// A node's displayed list must not depend on SQLite row order or map
	// iteration order, both of which are unspecified.
	sort.Slice(eligible, func(i, j int) bool {
		return eligible[i].TargetID < eligible[j].TargetID
	})
	return eligible, nil
}

// read returns the profile/target projection for the current configuration
// revision, reusing the cached projection when the cheap revision read proves
// nothing changed since the last full read.
func (cache *eligibilityCache) read(
	ctx context.Context,
	agentStore controllerAgentStore,
) (map[domain.RunnerProfileID]domain.RunnerProfile, []store.ManagementGitHubTarget, error) {
	revision, err := agentStore.ManagementConfigurationRevision(ctx)
	if err != nil {
		return nil, nil, err
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.loaded && cache.revision == revision {
		return cache.profiles, cache.targets, nil
	}
	configuration, err := agentStore.ReadManagementConfiguration(ctx)
	if err != nil {
		return nil, nil, err
	}
	profiles := make(map[domain.RunnerProfileID]domain.RunnerProfile, len(configuration.RunnerProfiles))
	for _, profile := range configuration.RunnerProfiles {
		profiles[profile.Profile.ID] = profile.Profile
	}
	cache.loaded = true
	cache.revision = configuration.Revision
	cache.profiles = profiles
	cache.targets = configuration.GitHubTargets
	return cache.profiles, cache.targets, nil
}

func (consumers storeBackedAgentConsumers) HandleAgentCommand(ctx context.Context, record AgentCommandRecord) error {
	if consumers.store == nil {
		return ErrAgentCommandConsumerRequired
	}
	commandType, err := domainCommandType(record.Kind)
	if err != nil {
		return err
	}
	issued := store.IssuedAgentCommand{
		NodeID: record.NodeID,
		Type:   commandType,
		Command: domain.Command{
			ID:              record.Metadata.CommandID,
			ControllerEpoch: record.Metadata.ControllerEpoch,
			ExecutionID:     record.Metadata.ExecutionID,
			ExpectedState:   record.Metadata.ExpectedState,
			PayloadDigest:   hex.EncodeToString(record.PayloadDigest[:]),
		},
	}
	if record.Reconciliation && record.ReplayOnly {
		return errors.New("Agent command authority modes are mutually exclusive")
	}
	var commitErr error
	if record.Reconciliation {
		if commandType != domain.CommandCancel {
			return errors.New("only Cancel may use reconciliation command authority")
		}
		_, commitErr = consumers.store.CommitAgentReconciliationCommand(
			ctx, issued, record.SnapshotDigest)
	} else if record.ReplayOnly {
		if commandType != domain.CommandPrepare {
			return errors.New("only Prepare may use prior-epoch replay authority")
		}
		_, commitErr = consumers.store.ReplayAgentCommand(
			ctx, issued, record.SnapshotDigest)
	} else {
		if record.SnapshotDigest != "" {
			return errors.New("ordinary Agent command cannot carry reconciliation snapshot authority")
		}
		_, commitErr = consumers.store.CommitAgentCommand(ctx, issued)
	}
	if commitErr != nil {
		return commitErr
	}
	if consumers.projection == nil {
		return nil
	}
	return consumers.projection.ApplyIssuedCommand(reconcile.IssuedCommand{
		NodeID:  issued.NodeID,
		Type:    issued.Type,
		Command: issued.Command,
	})
}

func (consumers storeBackedAgentConsumers) HandleAgentSnapshot(ctx context.Context, snapshot AgentSnapshot) error {
	if consumers.store == nil {
		return ErrAgentSnapshotConsumerRequired
	}
	journal := store.AgentSnapshot{
		MaxControllerEpoch: snapshot.MaxControllerEpoch,
		Commands:           append([]domain.Command(nil), snapshot.Commands...),
	}
	for _, observation := range snapshot.Observations {
		journal.Observations = append(journal.Observations, store.ObservationSnapshot{
			ExecutionID:        observation.ExecutionID,
			State:              observation.State,
			ObservedAtUnixNano: observation.ObservedAtUnixNano,
		})
	}
	for _, tombstone := range snapshot.CleanupTombstones {
		journal.CleanupTombstones = append(journal.CleanupTombstones, store.CleanupTombstoneSnapshot{
			ExecutionID:        tombstone.ExecutionID,
			FailureCode:        tombstone.FailureCode,
			RecordedAtUnixNano: tombstone.RecordedAtUnixNano,
		})
	}
	return consumers.store.RecordAgentSnapshot(ctx, store.NodeAgentSnapshot{
		NodeID:            snapshot.NodeID,
		OS:                domain.OperatingSystem(snapshot.OS),
		Architecture:      domain.Architecture(snapshot.Arch),
		RunnerVersion:     snapshot.RunnerVersion,
		NativeRunnerReady: snapshot.NativeRunnerReady,
		Journal:           journal,
	})
}

func (consumers storeBackedAgentConsumers) HandleAgentReadiness(
	ctx context.Context,
	nodeID domain.NodeID,
	snapshotDigest string,
	ready bool,
) error {
	if consumers.store == nil {
		return ErrAgentReadinessConsumerRequired
	}
	if err := consumers.store.RecordAgentReadiness(
		ctx, nodeID, snapshotDigest, ready); err != nil {
		return err
	}
	if consumers.projection == nil {
		return nil
	}
	return consumers.projection.ApplyAgentReadiness(nodeID, ready)
}

func (consumers storeBackedAgentConsumers) HandleAgentDisconnect(
	ctx context.Context,
	record AgentDisconnectRecord,
) error {
	if consumers.store == nil {
		return ErrAgentReadinessConsumerRequired
	}
	if err := consumers.store.RecordAgentDisconnect(
		ctx,
		record.NodeID,
		record.SnapshotDigest,
	); err != nil {
		return err
	}
	if consumers.projection == nil {
		return nil
	}
	return consumers.projection.HandleAgentDisconnect(ctx, record.NodeID)
}

func (consumers storeBackedAgentConsumers) HandleExecutionUpdate(ctx context.Context, record AgentExecutionUpdateRecord) error {
	if consumers.store == nil {
		return ErrExecutionUpdateConsumerRequired
	}
	_, err := consumers.store.RecordAgentExecutionUpdate(ctx, store.AgentExecutionUpdate{
		NodeID:        record.Update.NodeID,
		MessageID:     record.MessageID,
		CommandID:     record.Update.CommandID,
		ExecutionID:   record.Update.ExecutionID,
		State:         record.Update.State,
		Replayed:      record.Update.Replayed,
		ErrorCode:     record.Update.ErrorCode,
		PayloadDigest: hex.EncodeToString(record.PayloadDigest[:]),
	})
	if err != nil || consumers.projection == nil {
		return err
	}
	reconciliation, err := consumers.store.AgentCommandIsReconciliation(
		ctx, record.Update.CommandID)
	if err != nil {
		return err
	}
	if reconciliation {
		return consumers.projection.ApplyReconciliationExecutionUpdate(
			record.Update)
	}
	return consumers.projection.ApplyExecutionUpdate(record.Update)
}

func domainCommandType(messageType transport.MessageType) (domain.CommandType, error) {
	switch messageType {
	case transport.MessagePrepare:
		return domain.CommandPrepare, nil
	case transport.MessageStart:
		return domain.CommandStart, nil
	case transport.MessageCancel:
		return domain.CommandCancel, nil
	default:
		return "", errors.New("unsupported durable Agent command type")
	}
}
