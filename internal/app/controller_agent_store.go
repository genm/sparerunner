package app

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/store"
	"github.com/genm/tewake/internal/transport"
)

type controllerAgentStore interface {
	CommitAgentCommand(context.Context, store.IssuedAgentCommand) (bool, error)
	RecordAgentSnapshot(context.Context, store.NodeAgentSnapshot) error
	RecordAgentExecutionUpdate(context.Context, store.AgentExecutionUpdate) (bool, error)
}

type storeBackedAgentConsumers struct {
	store controllerAgentStore
}

func newStoreBackedAgentConsumers(agentStore controllerAgentStore) AgentConsumers {
	consumers := storeBackedAgentConsumers{store: agentStore}
	return AgentConsumers{
		Commands:         consumers,
		Snapshot:         consumers,
		ExecutionUpdates: consumers,
	}
}

func (consumers storeBackedAgentConsumers) HandleAgentCommand(ctx context.Context, record AgentCommandRecord) error {
	if consumers.store == nil {
		return ErrAgentCommandConsumerRequired
	}
	commandType, err := domainCommandType(record.Kind)
	if err != nil {
		return err
	}
	_, err = consumers.store.CommitAgentCommand(ctx, store.IssuedAgentCommand{
		NodeID: record.NodeID,
		Type:   commandType,
		Command: domain.Command{
			ID:              record.Metadata.CommandID,
			ControllerEpoch: record.Metadata.ControllerEpoch,
			ExecutionID:     record.Metadata.ExecutionID,
			ExpectedState:   record.Metadata.ExpectedState,
			PayloadDigest:   hex.EncodeToString(record.PayloadDigest[:]),
		},
	})
	return err
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
		NativeRunnerReady: snapshot.NativeRunnerReady,
		Journal:           journal,
	})
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
	return err
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
