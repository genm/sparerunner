package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/genm/tewake/internal/domain"
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
