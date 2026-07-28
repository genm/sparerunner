package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/genm/sparerunner/internal/runner"
)

// RunnerJournal returns the AgentStore-backed lifecycle journal. Keeping the
// adapter on AgentStore preserves its single SQLite connection and transaction
// boundary; it never opens an independent journal database.
func (s *AgentStore) RunnerJournal() runner.Journal {
	return agentRunnerJournal{store: s}
}

// RunnerJournalRecords returns the complete typed local runtime projection for
// Agent startup reconciliation. JIT bodies and process output are absent by
// schema; callers receive only the same validated records exposed by Journal.
func (s *AgentStore) RunnerJournalRecords(ctx context.Context) ([]runner.VersionedRecord, error) {
	if s == nil || s.baseStore == nil {
		return nil, runner.ErrJournal
	}
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT execution_id FROM runner_journal_records ORDER BY execution_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var executionIDs []string
	for rows.Next() {
		var executionID string
		if err := rows.Scan(&executionID); err != nil {
			return nil, err
		}
		if executionID == "" {
			return nil, runner.ErrJournal
		}
		executionIDs = append(executionIDs, executionID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	records := make([]runner.VersionedRecord, 0, len(executionIDs))
	for _, executionID := range executionIDs {
		record, found, err := loadRunnerJournalRecord(ctx, s.db, executionID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, runner.ErrJournal
		}
		records = append(records, record)
	}
	return records, nil
}

type agentRunnerJournal struct {
	store *AgentStore
}

func (j agentRunnerJournal) Load(ctx context.Context, executionID string) (runner.VersionedRecord, bool, error) {
	if err := j.ready(); err != nil {
		return runner.VersionedRecord{}, false, err
	}
	if executionID == "" {
		return runner.VersionedRecord{}, false, runner.ErrJournal
	}
	return loadRunnerJournalRecord(ctx, j.store.db, executionID)
}

func (j agentRunnerJournal) Create(ctx context.Context, mutationToken string, record runner.Record) (runner.VersionedRecord, bool, error) {
	if err := j.ready(); err != nil {
		return runner.VersionedRecord{}, false, err
	}
	created := runner.VersionedRecord{Record: record, Revision: 1, MutationToken: mutationToken}
	if runner.ValidateVersionedRecord(created) != nil {
		return runner.VersionedRecord{}, false, runner.ErrJournal
	}
	tx, err := j.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return runner.VersionedRecord{}, false, err
	}
	defer tx.Rollback()

	if existing, found, err := loadRunnerJournalRecord(ctx, tx, record.ExecutionID); err != nil || found {
		return existing, false, err
	}
	if err := insertRunnerJournalRecord(ctx, tx, created); err != nil {
		return runner.VersionedRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return runner.VersionedRecord{}, false, err
	}
	return created, true, nil
}

func (j agentRunnerJournal) CompareAndSwap(ctx context.Context, executionID string, expectedRevision uint64, mutationToken string, next runner.Record) (runner.VersionedRecord, bool, error) {
	if err := j.ready(); err != nil {
		return runner.VersionedRecord{}, false, err
	}
	if executionID == "" || next.ExecutionID != executionID || expectedRevision == 0 || expectedRevision >= maxSQLiteInteger {
		return runner.VersionedRecord{}, false, runner.ErrJournal
	}
	updated := runner.VersionedRecord{Record: next, Revision: expectedRevision + 1, MutationToken: mutationToken}
	if runner.ValidateVersionedRecord(updated) != nil {
		return runner.VersionedRecord{}, false, runner.ErrJournal
	}
	tx, err := j.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return runner.VersionedRecord{}, false, err
	}
	defer tx.Rollback()

	current, found, err := loadRunnerJournalRecord(ctx, tx, executionID)
	if err != nil || !found || current.Revision != expectedRevision {
		return current, false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE runner_journal_records
		SET spec_digest = ?, jit_digest = ?, state = ?, root_name = ?, pid = ?, tombstone = ?,
			containment_backend = ?, containment_owner_id = ?, containment_scope = ?, containment_host_epoch = ?, containment_invocation_id = ?, containment_fence_token = ?,
			workspace_backend = ?, workspace_owner_id = ?, revision = ?, mutation_token = ?
		WHERE execution_id = ? AND revision = ?`,
		updated.SpecDigest, updated.JITDigest, updated.State, updated.RootName, updated.PID, updated.Tombstone,
		updated.Containment.Backend, updated.Containment.OwnerID, updated.Containment.Scope, updated.Containment.HostEpoch, updated.Containment.InvocationID, updated.Containment.FenceToken,
		updated.WorkspaceRef.Backend, updated.WorkspaceRef.OwnerID, updated.Revision, updated.MutationToken,
		executionID, expectedRevision)
	if err != nil {
		return runner.VersionedRecord{}, false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return runner.VersionedRecord{}, false, err
	}
	if affected != 1 {
		return runner.VersionedRecord{}, false, fmt.Errorf("runner journal CAS lost its serialized record")
	}
	if err := tx.Commit(); err != nil {
		return runner.VersionedRecord{}, false, err
	}
	return updated, true, nil
}

func (j agentRunnerJournal) ready() error {
	if j.store == nil || j.store.baseStore == nil {
		return runner.ErrJournal
	}
	return j.store.requireReady()
}

type runnerJournalQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadRunnerJournalRecord(ctx context.Context, queryer runnerJournalQueryer, executionID string) (runner.VersionedRecord, bool, error) {
	var record runner.VersionedRecord
	var pid, tombstone, revision int64
	err := queryer.QueryRowContext(ctx, `SELECT execution_id, spec_digest, jit_digest, state, root_name, pid, tombstone,
		containment_backend, containment_owner_id, containment_scope, containment_host_epoch, containment_invocation_id, containment_fence_token,
		workspace_backend, workspace_owner_id, revision, mutation_token
		FROM runner_journal_records WHERE execution_id = ?`, executionID).Scan(
		&record.ExecutionID, &record.SpecDigest, &record.JITDigest, &record.State, &record.RootName, &pid, &tombstone,
		&record.Containment.Backend, &record.Containment.OwnerID, &record.Containment.Scope, &record.Containment.HostEpoch, &record.Containment.InvocationID, &record.Containment.FenceToken,
		&record.WorkspaceRef.Backend, &record.WorkspaceRef.OwnerID, &revision, &record.MutationToken)
	if errors.Is(err, sql.ErrNoRows) {
		return runner.VersionedRecord{}, false, nil
	}
	if err != nil {
		return runner.VersionedRecord{}, false, err
	}
	if pid < 0 || pid > int64(^uint(0)>>1) || tombstone < 0 || tombstone > 1 || revision <= 0 {
		return runner.VersionedRecord{}, false, fmt.Errorf("runner journal persisted value is outside its supported range")
	}
	record.PID = int(pid)
	record.Tombstone = tombstone == 1
	record.Revision = uint64(revision)
	// A malformed record is evidence of corruption, not an absent execution.
	if record.Revision > maxSQLiteInteger || runner.ValidateVersionedRecord(record) != nil {
		return runner.VersionedRecord{}, false, fmt.Errorf("runner journal persisted record is invalid")
	}
	return record, true, nil
}

func insertRunnerJournalRecord(ctx context.Context, tx *sql.Tx, record runner.VersionedRecord) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO runner_journal_records (
		execution_id, spec_digest, jit_digest, state, root_name, pid, tombstone,
		containment_backend, containment_owner_id, containment_scope, containment_host_epoch, containment_invocation_id, containment_fence_token,
		workspace_backend, workspace_owner_id, revision, mutation_token
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ExecutionID, record.SpecDigest, record.JITDigest, record.State, record.RootName, record.PID, record.Tombstone,
		record.Containment.Backend, record.Containment.OwnerID, record.Containment.Scope, record.Containment.HostEpoch, record.Containment.InvocationID, record.Containment.FenceToken,
		record.WorkspaceRef.Backend, record.WorkspaceRef.OwnerID, record.Revision, record.MutationToken)
	return err
}
