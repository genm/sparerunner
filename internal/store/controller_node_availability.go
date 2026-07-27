package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/genm/tewake/internal/domain"
)

// MaxNodeTargetExclusions bounds one adopted exclusion set. It mirrors the
// transport bound on the same list and exists only to reject a malformed or
// hostile payload before it reaches durable state; it is not a product quota on
// how many Targets a node owner may withhold.
const MaxNodeTargetExclusions = 256

// nodeOwnerAdoption reports which owner-editable values actually changed inside
// one adoption transaction. Audit evidence is written only for a real change, so
// a reconnect that re-reports the same set does not append a redundant event.
type nodeOwnerAdoption struct {
	intentChanged     bool
	exclusionsChanged bool
}

func (adoption nodeOwnerAdoption) audited() bool {
	return adoption.intentChanged || adoption.exclusionsChanged
}

// RecordNodeOwnerState adopts a mid-session node-owner availability change for
// the exact committed full Agent snapshot, mirroring RecordAgentReadiness's
// digest-guarded style: an authority that has moved on cannot have owner state
// grafted onto it.
//
// intent "" and a nil exclusions pointer both mean "no change reported". A
// non-nil pointer is the authoritative full set, including an empty one, and
// replaces the adopted rows wholesale.
func (s *ControllerStore) RecordNodeOwnerState(
	ctx context.Context,
	nodeID domain.NodeID,
	expectedSnapshotDigest string,
	intent domain.AvailabilityIntent,
	exclusions *[]domain.TargetID,
) error {
	if err := s.requireReady(); err != nil {
		return err
	}
	if nodeID == "" || !isLowerSHA256(expectedSnapshotDigest) {
		return errors.New("node owner state authority is invalid")
	}
	if err := validateNodeOwnerState(intent, exclusions); err != nil {
		return err
	}
	if intent == "" && exclusions == nil {
		return nil
	}
	// Adoption can persist audit evidence, and audit evidence is fail-closed
	// authority. Refuse before touching durable owner state rather than
	// silently adopting a change no audit trail can explain.
	if !s.ManagementAuditHealthy() {
		return ErrManagementAuditPersistence
	}
	recordedAt, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := requireKnownEnrolledNode(ctx, tx, nodeID); err != nil {
		return err
	}
	currentEpoch, err := readUintMetadata(ctx, tx, "controller_epoch")
	if err != nil {
		return err
	}
	if err := requireCurrentAgentSnapshotAuthority(
		ctx,
		tx,
		nodeID,
		expectedSnapshotDigest,
		domain.ControllerEpoch(currentEpoch),
	); err != nil {
		return err
	}
	adoption, err := adoptNodeOwnerState(
		ctx, tx, nodeID, intent, exclusions, recordedAt)
	if err != nil {
		return err
	}
	if !adoption.audited() {
		return tx.Commit()
	}
	return s.commitWithManagementAudit(tx, s.beforeManagementMutationCommit)
}

// ReadNodeTargetExclusions returns the controller-adopted exclusion set for one
// node in stable order. The heartbeat-acknowledgement echo reads this durable
// table rather than an in-memory guess, so a desktop surface's display of what
// was actually adopted self-heals across reconnects.
func (s *ControllerStore) ReadNodeTargetExclusions(
	ctx context.Context,
	nodeID domain.NodeID,
) ([]domain.TargetID, error) {
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	if nodeID == "" {
		return nil, errors.New("node target exclusion read requires node ID")
	}
	return readNodeTargetExclusions(ctx, s.db, nodeID)
}

func readNodeTargetExclusions(
	ctx context.Context,
	q queryer,
	nodeID domain.NodeID,
) ([]domain.TargetID, error) {
	rows, err := q.QueryContext(
		ctx,
		`SELECT target_id FROM node_target_exclusions
		 WHERE node_id = ? ORDER BY target_id`,
		nodeID,
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
		if targetID == "" {
			return nil, errors.New("stored node target exclusion is invalid")
		}
		excluded = append(excluded, targetID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return excluded, nil
}

// adoptNodeOwnerState persists the owner-editable values inside the caller's
// transaction. Snapshot adoption and heartbeat adoption share it so the two
// paths cannot drift into different set or audit semantics.
func adoptNodeOwnerState(
	ctx context.Context,
	tx *sql.Tx,
	nodeID domain.NodeID,
	intent domain.AvailabilityIntent,
	exclusions *[]domain.TargetID,
	recordedAt int64,
) (nodeOwnerAdoption, error) {
	var adoption nodeOwnerAdoption
	if intent != "" {
		changed, err := adoptNodeAvailabilityIntent(ctx, tx, nodeID, intent)
		if err != nil {
			return nodeOwnerAdoption{}, err
		}
		adoption.intentChanged = changed
	}
	if exclusions != nil {
		changed, err := adoptNodeTargetExclusions(
			ctx, tx, nodeID, *exclusions, recordedAt)
		if err != nil {
			return nodeOwnerAdoption{}, err
		}
		adoption.exclusionsChanged = changed
	}
	if !adoption.audited() {
		return adoption, nil
	}
	revision, err := readManagementRevision(ctx, tx)
	if err != nil {
		return nodeOwnerAdoption{}, err
	}
	if adoption.intentChanged {
		if err := insertNodeOwnerAudit(
			ctx, tx, recordedAt, revision, nodeID,
			AuditActionNodeAvailabilityChanged,
		); err != nil {
			return nodeOwnerAdoption{}, err
		}
	}
	if adoption.exclusionsChanged {
		if err := insertNodeOwnerAudit(
			ctx, tx, recordedAt, revision, nodeID,
			AuditActionNodeTargetExclusionChanged,
		); err != nil {
			return nodeOwnerAdoption{}, err
		}
	}
	return adoption, nil
}

func insertNodeOwnerAudit(
	ctx context.Context,
	tx *sql.Tx,
	occurredAt int64,
	revision uint64,
	nodeID domain.NodeID,
	action AuditAction,
) error {
	if _, err := insertAuditEvent(ctx, tx, occurredAt, revision, AuditRecord{
		Actor:        AuditActorNode,
		Action:       action,
		Outcome:      AuditOutcomeSucceeded,
		ResourceKind: AuditResourceNode,
		ResourceID:   string(nodeID),
		// Node-reported adoption has no HTTP request of its own; the local
		// control endpoint is the requesting surface and the node is the actor.
		RequestID: "req_unavailable",
	}); err != nil {
		return fmt.Errorf("%w: %v", ErrManagementAuditPersistence, err)
	}
	return nil
}

func adoptNodeAvailabilityIntent(
	ctx context.Context,
	tx *sql.Tx,
	nodeID domain.NodeID,
	intent domain.AvailabilityIntent,
) (bool, error) {
	var current sql.NullString
	if err := tx.QueryRowContext(
		ctx,
		`SELECT availability_intent FROM agent_session_snapshots WHERE node_id = ?`,
		nodeID,
	).Scan(&current); err != nil {
		return false, err
	}
	if current.Valid && domain.AvailabilityIntent(current.String) == intent {
		return false, nil
	}
	result, err := tx.ExecContext(
		ctx,
		`UPDATE agent_session_snapshots SET availability_intent = ? WHERE node_id = ?`,
		string(intent), nodeID,
	)
	if err != nil {
		return false, err
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil || affected != 1 {
		return false, errors.New("node availability intent did not update exactly one session snapshot")
	}
	return true, nil
}

func adoptNodeTargetExclusions(
	ctx context.Context,
	tx *sql.Tx,
	nodeID domain.NodeID,
	exclusions []domain.TargetID,
	recordedAt int64,
) (bool, error) {
	current, err := readNodeTargetExclusions(ctx, tx, nodeID)
	if err != nil {
		return false, err
	}
	next := canonicalTargetIDSet(exclusions)
	if equalTargetIDSets(current, next) {
		return false, nil
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM node_target_exclusions WHERE node_id = ?`,
		nodeID,
	); err != nil {
		return false, err
	}
	for _, targetID := range next {
		if _, err := tx.ExecContext(ctx, `INSERT INTO node_target_exclusions(
			node_id, target_id, recorded_at_unix_nano
		) VALUES (?, ?, ?)`, nodeID, targetID, recordedAt); err != nil {
			return false, err
		}
	}
	return true, nil
}

func canonicalTargetIDSet(targetIDs []domain.TargetID) []domain.TargetID {
	canonical := append([]domain.TargetID(nil), targetIDs...)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i] < canonical[j] })
	return canonical
}

func equalTargetIDSets(left, right []domain.TargetID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validateNodeOwnerState(
	intent domain.AvailabilityIntent,
	exclusions *[]domain.TargetID,
) error {
	if intent != "" {
		if err := intent.Validate("node_owner_state.availability_intent"); err != nil {
			return err
		}
	}
	if exclusions == nil {
		return nil
	}
	return validateNodeTargetExclusions(*exclusions)
}

func validateNodeTargetExclusions(exclusions []domain.TargetID) error {
	if len(exclusions) > MaxNodeTargetExclusions {
		return errors.New("node target exclusion set exceeds its transport bound")
	}
	seen := make(map[domain.TargetID]struct{}, len(exclusions))
	for _, targetID := range exclusions {
		if !canonicalRuntimeIdentifier(string(targetID)) {
			return errors.New("node target exclusion identity is invalid")
		}
		if _, duplicate := seen[targetID]; duplicate {
			return errors.New("node target exclusion set repeats a target ID")
		}
		seen[targetID] = struct{}{}
	}
	return nil
}
