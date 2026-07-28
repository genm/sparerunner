package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/genm/sparerunner/internal/domain"
)

// AvailabilityRecord is the durable node-local availability intent together with
// the non-secret provenance shown by desktop clients. ChangedBy names the
// surface that requested the change, never a credential.
type AvailabilityRecord struct {
	Intent            domain.AvailabilityIntent
	ChangedAtUnixNano int64
	ChangedBy         string
	Explicit          bool
}

// DefaultAvailabilityIntent is the value of a node that its owner has never
// touched. Enrollment already expresses the intent to lend the computer, so the
// unset state accepts jobs; only an explicit local decision withholds capacity.
const DefaultAvailabilityIntent = domain.AvailabilityAccepting

// ReadAvailability returns the durable intent. A node with no recorded decision
// reports the default without writing, so reads never create authority.
func (s *AgentStore) ReadAvailability(ctx context.Context) (AvailabilityRecord, error) {
	if err := s.requireReady(); err != nil {
		return AvailabilityRecord{}, err
	}
	row := s.db.QueryRowContext(
		ctx,
		`SELECT intent, changed_at_unix_nano, changed_by FROM node_availability WHERE id = 1`,
	)
	var record AvailabilityRecord
	err := row.Scan(&record.Intent, &record.ChangedAtUnixNano, &record.ChangedBy)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return AvailabilityRecord{Intent: DefaultAvailabilityIntent}, nil
	case err != nil:
		return AvailabilityRecord{}, err
	}
	// A stored value outside the closed set is corruption, not a hint. Fail
	// closed rather than letting an unreadable intent admit jobs.
	if err := record.Intent.Validate("node_availability.intent"); err != nil {
		return AvailabilityRecord{}, err
	}
	record.Explicit = true
	return record, nil
}

// SetAvailability durably records the owner's decision before any caller may
// observe it as effective.
func (s *AgentStore) SetAvailability(
	ctx context.Context,
	intent domain.AvailabilityIntent,
	changedBy string,
) (AvailabilityRecord, error) {
	if err := s.requireReady(); err != nil {
		return AvailabilityRecord{}, err
	}
	if err := intent.Validate("node_availability.intent"); err != nil {
		return AvailabilityRecord{}, err
	}
	if changedBy == "" {
		return AvailabilityRecord{}, errors.New("availability change requires a requesting surface")
	}
	changedAt, err := storeUnixNano(s.now())
	if err != nil {
		return AvailabilityRecord{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return AvailabilityRecord{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO node_availability (id, intent, changed_at_unix_nano, changed_by)
		 VALUES (1, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		     intent = excluded.intent,
		     changed_at_unix_nano = excluded.changed_at_unix_nano,
		     changed_by = excluded.changed_by`,
		string(intent), changedAt, changedBy,
	); err != nil {
		return AvailabilityRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return AvailabilityRecord{}, err
	}
	return AvailabilityRecord{
		Intent:            intent,
		ChangedAtUnixNano: changedAt,
		ChangedBy:         changedBy,
		Explicit:          true,
	}, nil
}

// MaxTargetExclusions bounds the durable owner-editable deny-list. It matches
// the eligible-target list bound the controller already enforces on the wire,
// because both lists are sized by the same configured Target population rather
// than by an independent product quota.
const MaxTargetExclusions = 256

// ErrTargetExclusionsFull is returned when a new exclusion would exceed
// MaxTargetExclusions. It fails closed rather than silently dropping the
// owner's decision.
var ErrTargetExclusionsFull = errors.New("node target exclusion set is full")

// ListExclusions returns the durable exclusion set sorted by target ID. The
// order is stable so wire payloads, digests, and desktop rendering never differ
// between two reads of the same durable state.
func (s *AgentStore) ListExclusions(ctx context.Context) ([]domain.TargetID, error) {
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT target_id FROM node_target_exclusions ORDER BY target_id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var excluded []domain.TargetID
	for rows.Next() {
		var targetID domain.TargetID
		if err := rows.Scan(&targetID); err != nil {
			return nil, err
		}
		// A stored identifier outside the storable shape is corruption, not a
		// hint. Refuse to report a set this node cannot vouch for.
		if err := targetID.ValidateShape("node_target_exclusions.target_id"); err != nil {
			return nil, err
		}
		excluded = append(excluded, targetID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return excluded, nil
}

// AddExclusion durably records the owner's withdrawal of one Target. Re-adding
// an existing exclusion is idempotent and refreshes only its provenance, so a
// repeated click never fails and never consumes another slot in the bound.
func (s *AgentStore) AddExclusion(
	ctx context.Context,
	targetID domain.TargetID,
	changedBy string,
) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if err := targetID.ValidateShape("node_target_exclusions.target_id"); err != nil {
		return err
	}
	if changedBy == "" {
		return errors.New("target exclusion change requires a requesting surface")
	}
	changedAt, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// The bound is enforced inside the same serializable transaction that
	// writes, so two concurrent desktop surfaces cannot both observe room and
	// both commit past it.
	var existing int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM node_target_exclusions WHERE target_id = ?`,
		string(targetID),
	).Scan(&existing); err != nil {
		return err
	}
	if existing == 0 {
		var total int
		if err := tx.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM node_target_exclusions`,
		).Scan(&total); err != nil {
			return err
		}
		if total >= MaxTargetExclusions {
			return ErrTargetExclusionsFull
		}
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO node_target_exclusions (target_id, changed_at_unix_nano, changed_by)
		 VALUES (?, ?, ?)
		 ON CONFLICT(target_id) DO UPDATE SET
		     changed_at_unix_nano = excluded.changed_at_unix_nano,
		     changed_by = excluded.changed_by`,
		string(targetID), changedAt, changedBy,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// RemoveExclusion durably re-allows one Target. Removing an absent entry is
// idempotent: the owner's desired end state is already durable.
func (s *AgentStore) RemoveExclusion(ctx context.Context, targetID domain.TargetID) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if err := targetID.ValidateShape("node_target_exclusions.target_id"); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM node_target_exclusions WHERE target_id = ?`,
		string(targetID),
	); err != nil {
		return err
	}
	return tx.Commit()
}

// ExecutionTarget is the non-secret attribution of one local execution to the
// GitHub scope that produced it. Scope and ScopeKind are display data; TargetID
// is the enforcement key the exec-boundary exclusion check uses.
type ExecutionTarget struct {
	ExecutionID domain.ExecutionID
	TargetID    domain.TargetID
	Scope       string
	ScopeKind   domain.TargetScopeKind
}

func (target ExecutionTarget) validate() error {
	if target.ExecutionID == "" {
		return errors.New("execution target requires an execution identity")
	}
	if err := target.TargetID.ValidateShape("execution_targets.target_id"); err != nil {
		return err
	}
	if target.Scope == "" {
		return errors.New("execution target requires a scope")
	}
	switch target.ScopeKind {
	case domain.TargetRepository, domain.TargetOrganization:
	default:
		return errors.New("execution target scope kind is not a known kind")
	}
	return nil
}

// ExecutionTargets returns attribution for every execution that has it, keyed by
// execution ID. Executions admitted before attribution existed are simply
// absent; desktop surfaces render them without a scope rather than inventing one.
func (s *AgentStore) ExecutionTargets(ctx context.Context) (map[domain.ExecutionID]ExecutionTarget, error) {
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT execution_id, target_id, scope, scope_kind FROM execution_targets ORDER BY execution_id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	targets := make(map[domain.ExecutionID]ExecutionTarget)
	for rows.Next() {
		var target ExecutionTarget
		if err := rows.Scan(&target.ExecutionID, &target.TargetID, &target.Scope, &target.ScopeKind); err != nil {
			return nil, err
		}
		if err := target.validate(); err != nil {
			return nil, err
		}
		targets[target.ExecutionID] = target
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return targets, nil
}
