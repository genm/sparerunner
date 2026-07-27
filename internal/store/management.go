package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/enroll"
)

var (
	ErrStaleManagementRevision    = errors.New("management configuration revision is stale")
	ErrManagementConfiguration    = errors.New("management configuration is invalid")
	ErrManagementConfigurationDB  = errors.New("stored management configuration is invalid")
	ErrManagementAuditRecord      = errors.New("management audit record is invalid")
	ErrManagementAuditPersistence = errors.New("management audit persistence failed")
)

// StaleManagementRevisionError preserves both sides of the failed
// compare-and-swap so the API can return an optimistic-conflict response without
// re-reading a potentially newer revision.
type StaleManagementRevisionError struct {
	Expected uint64
	Actual   uint64
}

func (err *StaleManagementRevisionError) Error() string {
	return fmt.Sprintf(
		"%v: expected %d, actual %d",
		ErrStaleManagementRevision,
		err.Expected,
		err.Actual,
	)
}

func (err *StaleManagementRevisionError) Unwrap() error {
	return ErrStaleManagementRevision
}

type ManagementNodeConfiguration struct {
	NodeID      domain.NodeID
	DisplayName string
	MaxRunners  int
}

// ManagementRunnerProfile keeps the non-secret desired profile together with
// the exact official runner version currently authorized by the runtime update
// policy. The latter remains required for both pinned and auto-update modes so a
// restart never invents an executable version.
type ManagementRunnerProfile struct {
	Profile       domain.RunnerProfile
	RunnerVersion string
}

type ManagementGitHubTarget struct {
	Target     domain.GitHubTarget
	ScaleSetID ScaleSetID
}

// DesiredManagementGitHubTarget contains only operator-controlled target
// fields. Provider verification owns private visibility, runner-group safety,
// and ScaleSetID, so external configuration cannot assert those authorities.
type DesiredManagementGitHubTarget struct {
	ID              domain.TargetID
	InstallationID  string
	ScopeKind       domain.TargetScopeKind
	Scope           string
	ScaleSetName    string
	RunnerProfileID domain.RunnerProfileID
}

type DesiredManagementConfiguration struct {
	FleetMaxRunners *int
	Nodes           []ManagementNodeConfiguration
	RunnerProfiles  []domain.RunnerProfile
	GitHubTargets   []DesiredManagementGitHubTarget
}

// ManagementVerifiedAuthorities is supplied by the API's owning verifier only
// for a new or re-verified profile/target. Existing verified values are retained
// by ID when operator-controlled configuration is unchanged.
type ManagementVerifiedAuthorities struct {
	RunnerProfiles []ManagementRunnerProfile
	GitHubTargets  []ManagementGitHubTarget
}

type ManagementConfiguration struct {
	Revision        uint64
	FleetMaxRunners *int
	Nodes           []ManagementNodeConfiguration
	RunnerProfiles  []ManagementRunnerProfile
	GitHubTargets   []ManagementGitHubTarget
}

type AuditActor string
type AuditAction string
type AuditOutcome string
type AuditResourceKind string
type AuditErrorCode string

const (
	AuditActorAnonymous   AuditActor = "anonymous"
	AuditActorJoinCode    AuditActor = "join_code"
	AuditActorNode        AuditActor = "node"
	AuditActorSingleAdmin AuditActor = "single_admin"

	AuditActionAuthenticationSucceeded  AuditAction = "authentication_succeeded"
	AuditActionAuthenticationFailed     AuditAction = "authentication_failed"
	AuditActionBrowserHandoffAuthorized AuditAction = "browser_handoff_authorized"
	AuditActionEnrollmentRejected       AuditAction = "enrollment_rejected"
	AuditActionEnrollmentUnavailable    AuditAction = "enrollment_unavailable"
	AuditActionAgentSessionRejected     AuditAction = "agent_session_rejected"
	AuditActionSessionEnded             AuditAction = "session_ended"
	AuditActionJoinCodeCreated          AuditAction = "join_code_created"
	AuditActionJoinCodeCancelled        AuditAction = "join_code_cancelled"
	AuditActionConfigurationApplied     AuditAction = "configuration_applied"
	AuditActionNodeEnrolled             AuditAction = "node_enrolled"
	AuditActionNodeDrained              AuditAction = "node_drained"
	AuditActionNodeResumed              AuditAction = "node_resumed"
	AuditActionNodeRevoked              AuditAction = "node_revoked"

	AuditOutcomeSucceeded AuditOutcome = "succeeded"
	AuditOutcomeRejected  AuditOutcome = "rejected"
	AuditOutcomeFailed    AuditOutcome = "failed"

	AuditResourceController    AuditResourceKind = "controller"
	AuditResourceConfiguration AuditResourceKind = "configuration"
	AuditResourceJoinCode      AuditResourceKind = "join_code"
	AuditResourceNode          AuditResourceKind = "node"

	AuditErrorAuthenticationFailed   AuditErrorCode = "authentication_failed"
	AuditErrorEnrollmentRejected     AuditErrorCode = "enrollment_rejected"
	AuditErrorEnrollmentRateLimited  AuditErrorCode = "enrollment_rate_limited"
	AuditErrorEnrollmentUnavailable  AuditErrorCode = "enrollment_unavailable"
	AuditErrorNodeCredentialRejected AuditErrorCode = "node_credential_rejected"
	AuditErrorAgentProtocolRejected  AuditErrorCode = "agent_protocol_rejected"
	AuditErrorHostRejected           AuditErrorCode = "host_rejected"
	AuditErrorEventStreamRejected    AuditErrorCode = "event_stream_rejected"
	AuditErrorMutationRejected       AuditErrorCode = "mutation_rejected"
	AuditErrorManagementUnavailable  AuditErrorCode = "management_unavailable"
	AuditErrorConfigurationConflict  AuditErrorCode = "configuration_revision_conflict"
	AuditErrorValidationFailed       AuditErrorCode = "validation_failed"
	AuditErrorJoinCodeNotFound       AuditErrorCode = "join_code_not_found"
	AuditErrorStateConflict          AuditErrorCode = "state_conflict"
	AuditErrorInvalidBody            AuditErrorCode = "invalid_body"
	AuditErrorInvalidPrecondition    AuditErrorCode = "invalid_precondition"
	AuditErrorPreconditionRequired   AuditErrorCode = "precondition_required"
	AuditErrorRequestForbidden       AuditErrorCode = "request_forbidden"
	AuditErrorMisdirectedHost        AuditErrorCode = "misdirected_host"
	AuditErrorMethodNotAllowed       AuditErrorCode = "method_not_allowed"
	AuditErrorPayloadTooLarge        AuditErrorCode = "payload_too_large"
	AuditErrorUnsupportedMediaType   AuditErrorCode = "unsupported_media_type"
	AuditErrorSessionUnavailable     AuditErrorCode = "session_unavailable"
	AuditErrorInvalidSessionRequest  AuditErrorCode = "invalid_session_request"
)

// AuditRecord is intentionally closed over typed, non-secret fields. Raw
// request payloads, headers, credentials, join codes, and arbitrary details do
// not cross the persistence API.
type AuditRecord struct {
	Actor        AuditActor
	Action       AuditAction
	Outcome      AuditOutcome
	ResourceKind AuditResourceKind
	ResourceID   string
	ErrorCode    AuditErrorCode
	RequestID    string
}

type AuditEvent struct {
	Sequence           uint64
	OccurredAtUnixNano int64
	Revision           uint64
	Record             AuditRecord
}

const MaximumAuditPageSize = 500

var ErrInvalidAuditPage = errors.New("management audit page is invalid")

type AuditPage struct {
	Events      []AuditEvent
	NextAfter   *uint64
	ResumeAfter *uint64
}

// ReadManagementConfiguration returns one transactionally consistent,
// secret-free internal authority in stable identity order. HTTP and import
// adapters must project only operator-controlled desired fields outward.
func (s *ControllerStore) ReadManagementConfiguration(
	ctx context.Context,
) (ManagementConfiguration, error) {
	if err := s.requireReady(); err != nil {
		return ManagementConfiguration{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ManagementConfiguration{}, err
	}
	defer tx.Rollback()
	result, err := readManagementConfiguration(ctx, tx)
	if err != nil {
		return ManagementConfiguration{}, err
	}
	if err := tx.Commit(); err != nil {
		return ManagementConfiguration{}, err
	}
	return result, nil
}

// ManagementConfigurationRevision reads only the current revision counter,
// without the full configuration transaction ReadManagementConfiguration
// pays for. Callers that poll at a fixed cadence (for example, the per-node
// eligibility cache) use this to detect "nothing changed" cheaply and only
// pay for the full read when the revision actually advances.
func (s *ControllerStore) ManagementConfigurationRevision(
	ctx context.Context,
) (uint64, error) {
	if err := s.requireReady(); err != nil {
		return 0, err
	}
	return readManagementRevision(ctx, s.db)
}

// ApplyManagementConfiguration compare-and-swaps the complete desired
// configuration, runtime projections, global revision, and success audit in one
// SQLite transaction. Audit failure therefore cannot leave a successful
// mutation without durable evidence.
func (s *ControllerStore) ApplyManagementConfiguration(
	ctx context.Context,
	expectedRevision uint64,
	desired DesiredManagementConfiguration,
	verified ManagementVerifiedAuthorities,
	audit AuditRecord,
) (ManagementConfiguration, error) {
	if err := s.requireReady(); err != nil {
		return ManagementConfiguration{}, err
	}
	if expectedRevision > maxSQLiteInteger {
		return ManagementConfiguration{}, fmt.Errorf(
			"%w: expected revision exceeds SQLite range",
			ErrManagementConfiguration,
		)
	}
	if err := validateDesiredManagementConfiguration(desired); err != nil {
		return ManagementConfiguration{}, err
	}
	if err := validateManagementVerifiedAuthorities(verified); err != nil {
		return ManagementConfiguration{}, err
	}
	if err := validateConfigurationApplyAudit(audit); err != nil {
		return ManagementConfiguration{}, err
	}
	if !s.ManagementAuditHealthy() {
		return ManagementConfiguration{}, ErrManagementAuditPersistence
	}
	occurredAt, err := storeUnixNano(s.now())
	if err != nil {
		return ManagementConfiguration{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return ManagementConfiguration{}, err
	}
	defer tx.Rollback()

	current, err := readManagementConfiguration(ctx, tx)
	if err != nil {
		return ManagementConfiguration{}, err
	}
	if current.Revision != expectedRevision {
		return ManagementConfiguration{}, &StaleManagementRevisionError{
			Expected: expectedRevision,
			Actual:   current.Revision,
		}
	}
	if current.Revision == maxSQLiteInteger {
		return ManagementConfiguration{}, fmt.Errorf(
			"%w: revision is exhausted",
			ErrManagementConfiguration,
		)
	}
	if err := requireExactEnrolledNodes(ctx, tx, desired.Nodes); err != nil {
		return ManagementConfiguration{}, err
	}
	next, err := materializeManagementConfiguration(current, desired, verified)
	if err != nil {
		return ManagementConfiguration{}, err
	}
	if err := validateActiveManagementTransition(ctx, tx, current, next); err != nil {
		return ManagementConfiguration{}, err
	}
	next.Revision = current.Revision + 1
	if err := replaceManagementConfiguration(ctx, tx, current, next); err != nil {
		return ManagementConfiguration{}, err
	}
	result, err := tx.ExecContext(
		ctx,
		`UPDATE management_configuration_state
		 SET revision = ?, fleet_max_runners = ?
		 WHERE singleton = 1 AND revision = ?`,
		next.Revision,
		nullableFleetMaximum(next.FleetMaxRunners),
		current.Revision,
	)
	if err != nil {
		return ManagementConfiguration{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return ManagementConfiguration{}, err
	}
	if affected != 1 {
		return ManagementConfiguration{}, &StaleManagementRevisionError{
			Expected: expectedRevision,
			Actual:   current.Revision,
		}
	}
	if _, err := insertAuditEvent(
		ctx,
		tx,
		occurredAt,
		next.Revision,
		audit,
	); err != nil {
		s.degradeManagementAudit()
		return ManagementConfiguration{}, fmt.Errorf(
			"%w: %v",
			ErrManagementAuditPersistence,
			err,
		)
	}
	if err := s.commitWithManagementAudit(
		tx,
		s.beforeManagementMutationCommit,
	); err != nil {
		return ManagementConfiguration{}, err
	}
	return cloneManagementConfiguration(next), nil
}

// ManagementAuditHealthy is process-local fail-closed authority. A persistence
// or commit failure makes it false for the remainder of this store instance;
// invalid callers and optimistic conflicts do not degrade it.
func (s *ControllerStore) ManagementAuditHealthy() bool {
	return s != nil && s.Ready() == nil && s.auditHealthy.Load()
}

// ManagementAuditChange closes exactly once when management audit authority
// degrades. Long-lived admission loops select on it so an already-running poll
// cannot advertise capacity after audit evidence becomes unavailable.
func (s *ControllerStore) ManagementAuditChange() <-chan struct{} {
	if s == nil || s.auditChange == nil {
		return closedManagementAuditChange
	}
	return s.auditChange
}

func (s *ControllerStore) degradeManagementAudit() {
	if s == nil {
		return
	}
	s.auditGate.Lock()
	defer s.auditGate.Unlock()
	if s.auditHealthy.CompareAndSwap(true, false) {
		close(s.auditChange)
	}
}

func (s *ControllerStore) commitWithManagementAudit(
	tx *sql.Tx,
	beforeCommit func(),
) error {
	// The DB connection is already owned by this transaction. Taking the gate
	// here avoids DB/gate lock inversion while still making degradation and the
	// audited commit globally ordered.
	s.auditGate.RLock()
	if !s.ManagementAuditHealthy() {
		s.auditGate.RUnlock()
		return ErrManagementAuditPersistence
	}
	if beforeCommit != nil {
		beforeCommit()
	}
	err := tx.Commit()
	s.auditGate.RUnlock()
	if err != nil {
		// Do not attempt the write lock while retaining the read lock.
		s.degradeManagementAudit()
	}
	return err
}

var closedManagementAuditChange = func() <-chan struct{} {
	channel := make(chan struct{})
	close(channel)
	return channel
}()

func (s *ControllerStore) AppendAuditEvent(
	ctx context.Context,
	record AuditRecord,
) (AuditEvent, error) {
	if err := s.requireReady(); err != nil {
		return AuditEvent{}, err
	}
	if err := validateAuditRecord(record); err != nil {
		return AuditEvent{}, err
	}
	if !s.ManagementAuditHealthy() {
		return AuditEvent{}, ErrManagementAuditPersistence
	}
	occurredAt, err := storeUnixNano(s.now())
	if err != nil {
		s.degradeManagementAudit()
		return AuditEvent{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		s.degradeManagementAudit()
		return AuditEvent{}, err
	}
	defer tx.Rollback()
	revision, err := readManagementRevision(ctx, tx)
	if err != nil {
		s.degradeManagementAudit()
		return AuditEvent{}, err
	}
	event, err := insertAuditEvent(ctx, tx, occurredAt, revision, record)
	if err != nil {
		s.degradeManagementAudit()
		return AuditEvent{}, fmt.Errorf("%w: %v", ErrManagementAuditPersistence, err)
	}
	if err := s.commitWithManagementAudit(tx, nil); err != nil {
		return AuditEvent{}, err
	}
	return event, nil
}

// CreateTokenWithAudit persists a join-token digest and its non-secret audit
// identity atomically. The raw join code never crosses this boundary.
func (s *ControllerStore) CreateTokenWithAudit(
	ctx context.Context,
	token enroll.TokenRecord,
	audit AuditRecord,
) error {
	tokenID := hex.EncodeToString(token.ID[:])
	if err := validateExactManagementAudit(
		audit,
		AuditActionJoinCodeCreated,
		AuditResourceJoinCode,
		tokenID,
	); err != nil {
		return err
	}
	if !s.ManagementAuditHealthy() {
		return ErrManagementAuditPersistence
	}
	occurredAt, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	if err := s.requireReady(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := createEnrollmentToken(ctx, tx, token); err != nil {
		return err
	}
	revision, err := readManagementRevision(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := insertAuditEvent(ctx, tx, occurredAt, revision, audit); err != nil {
		s.degradeManagementAudit()
		return fmt.Errorf("%w: %v", ErrManagementAuditPersistence, err)
	}
	if err := s.commitWithManagementAudit(
		tx,
		s.beforeManagementMutationCommit,
	); err != nil {
		return err
	}
	return nil
}

// CancelTokenWithAudit removes only an unconsumed token in the same transaction
// as its audit. Revoking an enrolled credential requires the explicit node
// revocation operation.
func (s *ControllerStore) CancelTokenWithAudit(
	ctx context.Context,
	tokenID [16]byte,
	audit AuditRecord,
) error {
	canonicalTokenID := hex.EncodeToString(tokenID[:])
	if err := validateExactManagementAudit(
		audit,
		AuditActionJoinCodeCancelled,
		AuditResourceJoinCode,
		canonicalTokenID,
	); err != nil {
		return err
	}
	if !s.ManagementAuditHealthy() {
		return ErrManagementAuditPersistence
	}
	occurredAt, err := storeUnixNano(s.now())
	if err != nil {
		return err
	}
	if err := s.requireReady(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(
		ctx,
		"DELETE FROM enrollment_tokens WHERE token_id = ?",
		tokenID[:],
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return enroll.ErrTokenNotFound
	}
	revision, err := readManagementRevision(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := insertAuditEvent(ctx, tx, occurredAt, revision, audit); err != nil {
		s.degradeManagementAudit()
		return fmt.Errorf("%w: %v", ErrManagementAuditPersistence, err)
	}
	if err := s.commitWithManagementAudit(
		tx,
		s.beforeManagementMutationCommit,
	); err != nil {
		return err
	}
	return nil
}

// SetNodeAdministrativeStateWithAudit compare-and-swaps the management
// revision, node authority, credential revocation, and audit evidence.
func (s *ControllerStore) SetNodeAdministrativeStateWithAudit(
	ctx context.Context,
	nodeID domain.NodeID,
	next domain.NodeAdministrativeState,
	expectedRevision uint64,
	audit AuditRecord,
) (uint64, error) {
	if err := s.requireReady(); err != nil {
		return 0, err
	}
	if !canonicalNodeID(string(nodeID)) || expectedRevision > maxSQLiteInteger {
		return 0, fmt.Errorf(
			"%w: invalid node management request",
			ErrManagementConfiguration,
		)
	}
	action, err := nodeAdministrativeAuditAction(next)
	if err != nil {
		return 0, err
	}
	if err := validateExactManagementAudit(
		audit,
		action,
		AuditResourceNode,
		string(nodeID),
	); err != nil {
		return 0, err
	}
	if !s.ManagementAuditHealthy() {
		return 0, ErrManagementAuditPersistence
	}
	occurredAt, err := storeUnixNano(s.now())
	if err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	actualRevision, err := readManagementRevision(ctx, tx)
	if err != nil {
		return 0, err
	}
	if actualRevision != expectedRevision {
		return 0, &StaleManagementRevisionError{
			Expected: expectedRevision,
			Actual:   actualRevision,
		}
	}
	if actualRevision == maxSQLiteInteger {
		return 0, fmt.Errorf(
			"%w: revision is exhausted",
			ErrManagementConfiguration,
		)
	}
	var current domain.NodeAdministrativeState
	var revoked int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT a.administrative_state, n.revoked
		 FROM enrolled_nodes n
		 JOIN node_administrative_states a ON a.node_id = n.node_id
		 WHERE n.node_id = ?`,
		nodeID,
	).Scan(&current, &revoked); errors.Is(err, sql.ErrNoRows) {
		return 0, enroll.ErrNodeNotFound
	} else if err != nil {
		return 0, err
	}
	if err := current.Validate("node.administrative_state"); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrManagementConfigurationDB, err)
	}
	if (revoked != 0) != (current == domain.NodeRevoked) {
		return 0, fmt.Errorf(
			"%w: node revocation authority is inconsistent",
			ErrManagementConfigurationDB,
		)
	}

	var revokedCredential *enroll.Credential
	switch next {
	case domain.NodeActive:
		if current != domain.NodeDraining || revoked != 0 {
			return 0, fmt.Errorf(
				"%w: only a draining node can resume",
				ErrManagementConfiguration,
			)
		}
		if err := updateNodeAdministrativeState(
			ctx,
			tx,
			nodeID,
			current,
			next,
		); err != nil {
			return 0, err
		}
	case domain.NodeDraining:
		if current != domain.NodeActive || revoked != 0 {
			return 0, fmt.Errorf(
				"%w: only an active node can drain",
				ErrManagementConfiguration,
			)
		}
		if err := updateNodeAdministrativeState(
			ctx,
			tx,
			nodeID,
			current,
			next,
		); err != nil {
			return 0, err
		}
	case domain.NodeRevoked:
		if revoked != 0 || current == domain.NodeRevoked {
			return 0, fmt.Errorf(
				"%w: node is already revoked",
				ErrManagementConfiguration,
			)
		}
		credential, _, err := s.readCredential(ctx, tx, string(nodeID))
		if err != nil {
			return 0, err
		}
		if credential.Epoch >= maxSQLiteInteger {
			return 0, enroll.ErrCredentialRejected
		}
		result, err := tx.ExecContext(
			ctx,
			`UPDATE enrolled_nodes
			 SET revoked = 1, credential_epoch = ?
			 WHERE node_id = ? AND revoked = 0`,
			credential.Epoch+1,
			nodeID,
		)
		if err != nil {
			return 0, err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return 0, enroll.ErrCredentialRejected
		}
		if _, err := tx.ExecContext(
			ctx,
			"DELETE FROM enrollment_replays WHERE node_id = ?",
			nodeID,
		); err != nil {
			return 0, err
		}
		revokedCredential = &credential
	default:
		return 0, fmt.Errorf(
			"%w: unsupported node administrative state",
			ErrManagementConfiguration,
		)
	}
	nextRevision := actualRevision + 1
	if err := updateManagementRevision(
		ctx,
		tx,
		actualRevision,
		nextRevision,
	); err != nil {
		return 0, err
	}
	if _, err := insertAuditEvent(
		ctx,
		tx,
		occurredAt,
		nextRevision,
		audit,
	); err != nil {
		s.degradeManagementAudit()
		return 0, fmt.Errorf("%w: %v", ErrManagementAuditPersistence, err)
	}
	if err := s.commitWithManagementAudit(
		tx,
		s.beforeManagementMutationCommit,
	); err != nil {
		return 0, err
	}
	if revokedCredential != nil {
		s.notifyRevocation(*revokedCredential)
	}
	return nextRevision, nil
}

// ReadAuditEventsPage returns at most limit append-only events after the
// exclusive sequence cursor. One extra row is read only to prove whether a next
// page exists; the API never needs to materialize the complete audit history.
func (s *ControllerStore) ReadAuditEventsPage(
	ctx context.Context,
	after uint64,
	limit int,
) (AuditPage, error) {
	if err := s.requireReady(); err != nil {
		return AuditPage{}, err
	}
	if after > maxSQLiteInteger || limit < 1 || limit > MaximumAuditPageSize {
		return AuditPage{}, ErrInvalidAuditPage
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		sequence, occurred_at_unix_nano, actor, action, outcome,
		resource_kind, resource_id, error_code, request_id, revision
		FROM management_audit_events
		WHERE sequence > ?
		ORDER BY sequence
		LIMIT ?`,
		after,
		limit+1,
	)
	if err != nil {
		return AuditPage{}, err
	}
	defer rows.Close()

	events := make([]AuditEvent, 0, limit+1)
	for rows.Next() {
		event, err := scanAuditEvent(rows)
		if err != nil {
			return AuditPage{}, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return AuditPage{}, err
	}

	page := AuditPage{Events: events}
	if len(events) > limit {
		page.Events = events[:limit]
		next := page.Events[len(page.Events)-1].Sequence
		page.NextAfter = &next
	}
	if len(page.Events) > 0 {
		resume := page.Events[len(page.Events)-1].Sequence
		page.ResumeAfter = &resume
	} else if after > 0 {
		// Preserve the caller's confirmed append-only position even when no
		// later event exists, so a subsequent invalidation can resume from it.
		resume := after
		page.ResumeAfter = &resume
	}
	return page, nil
}

// ReadAuditEvents returns the append-only audit stream in sequence order. It is
// retained for internal recovery/test consumers; HTTP callers must use the
// bounded page API above.
func (s *ControllerStore) ReadAuditEvents(ctx context.Context) ([]AuditEvent, error) {
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		sequence, occurred_at_unix_nano, actor, action, outcome,
		resource_kind, resource_id, error_code, request_id, revision
		FROM management_audit_events
		ORDER BY sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []AuditEvent
	for rows.Next() {
		event, err := scanAuditEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

type auditEventScanner interface {
	Scan(...any) error
}

func scanAuditEvent(scanner auditEventScanner) (AuditEvent, error) {
	var event AuditEvent
	if err := scanner.Scan(
		&event.Sequence,
		&event.OccurredAtUnixNano,
		&event.Record.Actor,
		&event.Record.Action,
		&event.Record.Outcome,
		&event.Record.ResourceKind,
		&event.Record.ResourceID,
		&event.Record.ErrorCode,
		&event.Record.RequestID,
		&event.Revision,
	); err != nil {
		return AuditEvent{}, err
	}
	if err := validateStoredAuditEvent(event); err != nil {
		return AuditEvent{}, err
	}
	return event, nil
}

func readManagementConfiguration(
	ctx context.Context,
	tx *sql.Tx,
) (ManagementConfiguration, error) {
	revision, fleetMaximum, err := readManagementState(ctx, tx)
	if err != nil {
		return ManagementConfiguration{}, err
	}
	result := ManagementConfiguration{
		Revision:        revision,
		FleetMaxRunners: fleetMaximum,
	}
	rows, err := tx.QueryContext(ctx, `SELECT node_id, display_name, max_runners
		FROM management_node_configurations ORDER BY node_id`)
	if err != nil {
		return ManagementConfiguration{}, err
	}
	for rows.Next() {
		var node ManagementNodeConfiguration
		if err := rows.Scan(&node.NodeID, &node.DisplayName, &node.MaxRunners); err != nil {
			rows.Close()
			return ManagementConfiguration{}, err
		}
		if err := validateManagementNodeConfiguration(node); err != nil {
			rows.Close()
			return ManagementConfiguration{}, fmt.Errorf(
				"%w: node: %v",
				ErrManagementConfigurationDB,
				err,
			)
		}
		result.Nodes = append(result.Nodes, node)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ManagementConfiguration{}, err
	}
	if err := rows.Close(); err != nil {
		return ManagementConfiguration{}, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT
		profile_id, label, operating_system, architecture,
		min_available_memory_bytes, version_policy, runner_version, runtime
		FROM management_runner_profiles ORDER BY profile_id`)
	if err != nil {
		return ManagementConfiguration{}, err
	}
	for rows.Next() {
		var profile ManagementRunnerProfile
		var operatingSystem, architecture sql.NullString
		if err := rows.Scan(
			&profile.Profile.ID,
			&profile.Profile.Label,
			&operatingSystem,
			&architecture,
			&profile.Profile.MinAvailableMemoryBytes,
			&profile.Profile.VersionPolicy,
			&profile.RunnerVersion,
			&profile.Profile.Runtime,
		); err != nil {
			rows.Close()
			return ManagementConfiguration{}, err
		}
		if operatingSystem.Valid {
			value := domain.OperatingSystem(operatingSystem.String)
			profile.Profile.OS = &value
		}
		if architecture.Valid {
			value := domain.Architecture(architecture.String)
			profile.Profile.Architecture = &value
		}
		if err := validateManagementRunnerProfile(profile); err != nil {
			rows.Close()
			return ManagementConfiguration{}, fmt.Errorf(
				"%w: runner profile: %v",
				ErrManagementConfigurationDB,
				err,
			)
		}
		result.RunnerProfiles = append(result.RunnerProfiles, profile)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ManagementConfiguration{}, err
	}
	if err := rows.Close(); err != nil {
		return ManagementConfiguration{}, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT
		target_id, installation_id, scope_kind, scope, visibility,
		runner_group_access_safe, scale_set_name, profile_id, scale_set_id
		FROM management_github_targets ORDER BY target_id`)
	if err != nil {
		return ManagementConfiguration{}, err
	}
	for rows.Next() {
		var target ManagementGitHubTarget
		if err := rows.Scan(
			&target.Target.ID,
			&target.Target.InstallationID,
			&target.Target.ScopeKind,
			&target.Target.Scope,
			&target.Target.Visibility,
			&target.Target.RunnerGroupAccessSafe,
			&target.Target.ScaleSetName,
			&target.Target.RunnerProfileID,
			&target.ScaleSetID,
		); err != nil {
			rows.Close()
			return ManagementConfiguration{}, err
		}
		if err := validateManagementGitHubTarget(target); err != nil {
			rows.Close()
			return ManagementConfiguration{}, fmt.Errorf(
				"%w: GitHub target: %v",
				ErrManagementConfigurationDB,
				err,
			)
		}
		result.GitHubTargets = append(result.GitHubTargets, target)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ManagementConfiguration{}, err
	}
	if err := rows.Close(); err != nil {
		return ManagementConfiguration{}, err
	}
	if err := validateManagementConfiguration(result); err != nil {
		return ManagementConfiguration{}, fmt.Errorf(
			"%w: %v",
			ErrManagementConfigurationDB,
			err,
		)
	}
	if err := requireExactEnrolledNodes(ctx, tx, result.Nodes); err != nil {
		return ManagementConfiguration{}, fmt.Errorf(
			"%w: %v",
			ErrManagementConfigurationDB,
			err,
		)
	}
	return result, nil
}

func readManagementRevision(ctx context.Context, q queryer) (uint64, error) {
	revision, _, err := readManagementState(ctx, q)
	return revision, err
}

func readManagementState(
	ctx context.Context,
	q queryer,
) (uint64, *int, error) {
	var revision uint64
	var fleetMaximum sql.NullInt64
	if err := q.QueryRowContext(
		ctx,
		`SELECT revision, fleet_max_runners
		 FROM management_configuration_state WHERE singleton = 1`,
	).Scan(&revision, &fleetMaximum); err != nil {
		return 0, nil, fmt.Errorf(
			"%w: state: %v",
			ErrManagementConfigurationDB,
			err,
		)
	}
	if revision > maxSQLiteInteger {
		return 0, nil, fmt.Errorf(
			"%w: revision exceeds SQLite range",
			ErrManagementConfigurationDB,
		)
	}
	var result *int
	if fleetMaximum.Valid {
		if fleetMaximum.Int64 < 1 ||
			uint64(fleetMaximum.Int64) > maxSQLiteInteger {
			return 0, nil, fmt.Errorf(
				"%w: fleet maximum is invalid",
				ErrManagementConfigurationDB,
			)
		}
		value := int(fleetMaximum.Int64)
		result = &value
	}
	return revision, result, nil
}

func validateActiveManagementTransition(
	ctx context.Context,
	q queryer,
	current ManagementConfiguration,
	next ManagementConfiguration,
) error {
	nodeLimits := make(map[domain.NodeID]int, len(next.Nodes))
	var summedLimit uint64
	for _, node := range next.Nodes {
		nodeLimits[node.NodeID] = node.MaxRunners
		if summedLimit <= maxSQLiteInteger-uint64(node.MaxRunners) {
			summedLimit += uint64(node.MaxRunners)
		} else {
			summedLimit = maxSQLiteInteger
		}
	}
	var reservations uint64
	rows, err := q.QueryContext(
		ctx,
		"SELECT node_id, slot_index FROM slot_reservations",
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var nodeID domain.NodeID
		var slot int
		if err := rows.Scan(&nodeID, &slot); err != nil {
			rows.Close()
			return err
		}
		limit, exists := nodeLimits[nodeID]
		if !exists || slot < 0 || slot >= limit {
			rows.Close()
			return fmt.Errorf(
				"%w: node %q has a reservation outside its next maximum",
				ErrManagementConfiguration,
				nodeID,
			)
		}
		reservations++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	fleetLimit := summedLimit
	if next.FleetMaxRunners != nil {
		fleetLimit = uint64(*next.FleetMaxRunners)
	}
	if reservations > fleetLimit {
		return fmt.Errorf(
			"%w: active reservations exceed the next fleet maximum",
			ErrManagementConfiguration,
		)
	}

	nextProfiles := make(
		map[domain.RunnerProfileID]ManagementRunnerProfile,
		len(next.RunnerProfiles),
	)
	for _, profile := range next.RunnerProfiles {
		nextProfiles[profile.Profile.ID] = profile
	}
	nextTargets := make(
		map[domain.TargetID]ManagementGitHubTarget,
		len(next.GitHubTargets),
	)
	for _, target := range next.GitHubTargets {
		nextTargets[target.Target.ID] = target
	}
	currentProfiles := make(
		map[domain.RunnerProfileID]ManagementRunnerProfile,
		len(current.RunnerProfiles),
	)
	for _, profile := range current.RunnerProfiles {
		currentProfiles[profile.Profile.ID] = profile
	}
	for _, target := range current.GitHubTargets {
		nextTarget, targetRetained := nextTargets[target.Target.ID]
		currentProfile := currentProfiles[target.Target.RunnerProfileID]
		nextProfile, profileRetained := nextProfiles[target.Target.RunnerProfileID]
		targetChanged := !targetRetained || nextTarget != target
		profileChanged := !profileRetained ||
			!equalManagementRunnerProfile(currentProfile, nextProfile)
		if !targetChanged && !profileChanged {
			continue
		}
		active, err := managementTargetHasActiveAuthority(
			ctx,
			q,
			target.Target.ID,
			target.ScaleSetID,
		)
		if err != nil {
			return err
		}
		if active {
			return fmt.Errorf(
				"%w: target %q has active execution or claim authority",
				ErrManagementConfiguration,
				target.Target.ID,
			)
		}
	}

	policies := make(
		map[domain.RunnerProfileID]RunnerProfileUpdatePolicy,
		len(next.RunnerProfiles),
	)
	rows, err = q.QueryContext(ctx, `SELECT
		profile_id, version_policy, runner_version, revision
		FROM runner_profile_update_policies`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var policy RunnerProfileUpdatePolicy
		if err := rows.Scan(
			&policy.ProfileID,
			&policy.VersionPolicy,
			&policy.RunnerVersion,
			&policy.Revision,
		); err != nil {
			rows.Close()
			return err
		}
		policies[policy.ProfileID] = policy
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = q.QueryContext(ctx, `SELECT
		target_id, scale_set_id, profile_id
		FROM github_target_runtime_bindings`)
	if err != nil {
		return err
	}
	var bindings []GitHubTargetRuntimeBinding
	for rows.Next() {
		var binding GitHubTargetRuntimeBinding
		if err := rows.Scan(
			&binding.TargetID,
			&binding.ScaleSetID,
			&binding.ProfileID,
		); err != nil {
			rows.Close()
			return err
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, binding := range bindings {
		target, targetRetained := nextTargets[binding.TargetID]
		profile, profileRetained := nextProfiles[binding.ProfileID]
		policy, policyExists := policies[binding.ProfileID]
		routeRetained := targetRetained &&
			target.ScaleSetID == binding.ScaleSetID &&
			target.Target.RunnerProfileID == binding.ProfileID
		policyRetained := profileRetained &&
			policyExists &&
			profile.Profile.VersionPolicy == policy.VersionPolicy &&
			profile.RunnerVersion == policy.RunnerVersion
		if routeRetained && policyRetained {
			continue
		}
		active, err := managementTargetHasActiveAuthority(
			ctx,
			q,
			binding.TargetID,
			binding.ScaleSetID,
		)
		if err != nil {
			return err
		}
		if active {
			return fmt.Errorf(
				"%w: runtime target %q has active execution or claim authority",
				ErrManagementConfiguration,
				binding.TargetID,
			)
		}
	}
	return nil
}

func equalManagementRunnerProfile(
	left ManagementRunnerProfile,
	right ManagementRunnerProfile,
) bool {
	return left.RunnerVersion == right.RunnerVersion &&
		equalRunnerProfile(left.Profile, right.Profile)
}

func managementTargetHasActiveAuthority(
	ctx context.Context,
	q queryer,
	targetID domain.TargetID,
	scaleSetID ScaleSetID,
) (bool, error) {
	var activeExecution, activeClaim int
	if err := q.QueryRowContext(
		ctx,
		`SELECT
			EXISTS (
				SELECT 1
				FROM slot_reservations reservation
				JOIN executions execution
					ON execution.id = reservation.execution_id
				WHERE execution.target_id = ?
			),
			EXISTS (
				SELECT 1
				FROM github_job_claims
				WHERE scale_set_id = ?
			)`,
		targetID,
		scaleSetID,
	).Scan(&activeExecution, &activeClaim); err != nil {
		return false, err
	}
	return activeExecution != 0 || activeClaim != 0, nil
}

func replaceManagementConfiguration(
	ctx context.Context,
	tx *sql.Tx,
	current ManagementConfiguration,
	next ManagementConfiguration,
) error {
	runtimeRevisions, err := nextRuntimeProfileRevisions(ctx, tx, current, next)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM management_github_targets"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM github_target_runtime_bindings"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM management_runner_profiles"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM runner_profile_update_policies"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM management_node_configurations"); err != nil {
		return err
	}
	for _, node := range next.Nodes {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO management_node_configurations(
				node_id, display_name, max_runners
			) VALUES (?, ?, ?)`,
			node.NodeID,
			node.DisplayName,
			node.MaxRunners,
		); err != nil {
			return err
		}
	}
	for _, profile := range next.RunnerProfiles {
		var operatingSystem, architecture any
		if profile.Profile.OS != nil {
			operatingSystem = string(*profile.Profile.OS)
		}
		if profile.Profile.Architecture != nil {
			architecture = string(*profile.Profile.Architecture)
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO management_runner_profiles(
				profile_id, label, operating_system, architecture,
				min_available_memory_bytes, version_policy,
				runner_version, runtime
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			profile.Profile.ID,
			profile.Profile.Label,
			operatingSystem,
			architecture,
			profile.Profile.MinAvailableMemoryBytes,
			profile.Profile.VersionPolicy,
			profile.RunnerVersion,
			profile.Profile.Runtime,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO runner_profile_update_policies(
				profile_id, version_policy, runner_version, revision
			) VALUES (?, ?, ?, ?)`,
			profile.Profile.ID,
			profile.Profile.VersionPolicy,
			profile.RunnerVersion,
			runtimeRevisions[profile.Profile.ID],
		); err != nil {
			return err
		}
	}
	for _, target := range next.GitHubTargets {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO management_github_targets(
				target_id, installation_id, scope_kind, scope, visibility,
				runner_group_access_safe, scale_set_name, profile_id,
				scale_set_id
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			target.Target.ID,
			target.Target.InstallationID,
			target.Target.ScopeKind,
			target.Target.Scope,
			target.Target.Visibility,
			target.Target.RunnerGroupAccessSafe,
			target.Target.ScaleSetName,
			target.Target.RunnerProfileID,
			target.ScaleSetID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO github_target_runtime_bindings(
				target_id, scale_set_id, profile_id
			) VALUES (?, ?, ?)`,
			target.Target.ID,
			target.ScaleSetID,
			target.Target.RunnerProfileID,
		); err != nil {
			return err
		}
	}
	return nil
}

func nextRuntimeProfileRevisions(
	ctx context.Context,
	q queryer,
	current ManagementConfiguration,
	next ManagementConfiguration,
) (map[domain.RunnerProfileID]uint64, error) {
	existing := make(map[domain.RunnerProfileID]RunnerProfileUpdatePolicy)
	rows, err := q.QueryContext(ctx, `SELECT
		profile_id, version_policy, runner_version, revision
		FROM runner_profile_update_policies`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var policy RunnerProfileUpdatePolicy
		if err := rows.Scan(
			&policy.ProfileID,
			&policy.VersionPolicy,
			&policy.RunnerVersion,
			&policy.Revision,
		); err != nil {
			rows.Close()
			return nil, err
		}
		if err := validateRunnerProfileUpdatePolicy(policy); err != nil {
			rows.Close()
			return nil, fmt.Errorf(
				"%w: runtime profile projection: %v",
				ErrManagementConfigurationDB,
				err,
			)
		}
		existing[policy.ProfileID] = policy
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	currentProfiles := make(
		map[domain.RunnerProfileID]ManagementRunnerProfile,
		len(current.RunnerProfiles),
	)
	for _, profile := range current.RunnerProfiles {
		currentProfiles[profile.Profile.ID] = profile
	}
	result := make(map[domain.RunnerProfileID]uint64, len(next.RunnerProfiles))
	for _, profile := range next.RunnerProfiles {
		policy, exists := existing[profile.Profile.ID]
		if !exists {
			result[profile.Profile.ID] = 1
			continue
		}
		currentProfile, configured := currentProfiles[profile.Profile.ID]
		if configured &&
			(policy.VersionPolicy != currentProfile.Profile.VersionPolicy ||
				policy.RunnerVersion != currentProfile.RunnerVersion) {
			return nil, fmt.Errorf(
				"%w: runtime profile projection differs from management authority",
				ErrManagementConfigurationDB,
			)
		}
		if policy.VersionPolicy == profile.Profile.VersionPolicy &&
			policy.RunnerVersion == profile.RunnerVersion {
			result[profile.Profile.ID] = policy.Revision
			continue
		}
		if policy.Revision == maxSQLiteInteger {
			return nil, fmt.Errorf(
				"%w: runtime profile revision is exhausted",
				ErrManagementConfiguration,
			)
		}
		result[profile.Profile.ID] = policy.Revision + 1
	}
	return result, nil
}

func insertAuditEvent(
	ctx context.Context,
	tx *sql.Tx,
	occurredAt int64,
	revision uint64,
	record AuditRecord,
) (AuditEvent, error) {
	if err := validateAuditRecord(record); err != nil {
		return AuditEvent{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO management_audit_events(
		occurred_at_unix_nano, actor, action, outcome,
		resource_kind, resource_id, error_code, request_id, revision
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		occurredAt,
		record.Actor,
		record.Action,
		record.Outcome,
		record.ResourceKind,
		record.ResourceID,
		record.ErrorCode,
		record.RequestID,
		revision,
	)
	if err != nil {
		return AuditEvent{}, err
	}
	sequence, err := result.LastInsertId()
	if err != nil || sequence <= 0 {
		return AuditEvent{}, fmt.Errorf("read audit sequence: %v", err)
	}
	return AuditEvent{
		Sequence:           uint64(sequence),
		OccurredAtUnixNano: occurredAt,
		Revision:           revision,
		Record:             record,
	}, nil
}

func requireExactEnrolledNodes(
	ctx context.Context,
	q queryer,
	nodes []ManagementNodeConfiguration,
) error {
	rows, err := q.QueryContext(ctx, "SELECT node_id FROM enrolled_nodes ORDER BY node_id")
	if err != nil {
		return err
	}
	defer rows.Close()
	var enrolled []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return err
		}
		enrolled = append(enrolled, nodeID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	configured := make([]string, len(nodes))
	for index, node := range nodes {
		configured[index] = string(node.NodeID)
	}
	sort.Strings(configured)
	if len(enrolled) != len(configured) {
		return fmt.Errorf(
			"%w: node configuration must match enrolled nodes",
			ErrManagementConfiguration,
		)
	}
	for index := range enrolled {
		if enrolled[index] != configured[index] {
			return fmt.Errorf(
				"%w: node configuration must match enrolled nodes",
				ErrManagementConfiguration,
			)
		}
	}
	return nil
}

func validateDesiredManagementConfiguration(
	desired DesiredManagementConfiguration,
) error {
	if err := validateFleetMaximum(desired.FleetMaxRunners); err != nil {
		return err
	}
	nodeIDs := make(map[domain.NodeID]struct{}, len(desired.Nodes))
	for _, node := range desired.Nodes {
		if err := validateManagementNodeConfiguration(node); err != nil {
			return err
		}
		if _, duplicate := nodeIDs[node.NodeID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate node %q",
				ErrManagementConfiguration,
				node.NodeID,
			)
		}
		nodeIDs[node.NodeID] = struct{}{}
	}
	profileIDs := make(
		map[domain.RunnerProfileID]struct{},
		len(desired.RunnerProfiles),
	)
	for _, profile := range desired.RunnerProfiles {
		if err := validateDesiredManagementRunnerProfile(profile); err != nil {
			return err
		}
		if _, duplicate := profileIDs[profile.ID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate runner profile %q",
				ErrManagementConfiguration,
				profile.ID,
			)
		}
		profileIDs[profile.ID] = struct{}{}
	}
	targetIDs := make(map[domain.TargetID]struct{}, len(desired.GitHubTargets))
	for _, target := range desired.GitHubTargets {
		if err := validateDesiredManagementGitHubTarget(target); err != nil {
			return err
		}
		if _, exists := profileIDs[target.RunnerProfileID]; !exists {
			return fmt.Errorf(
				"%w: target %q references unknown runner profile %q",
				ErrManagementConfiguration,
				target.ID,
				target.RunnerProfileID,
			)
		}
		if _, duplicate := targetIDs[target.ID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate GitHub target %q",
				ErrManagementConfiguration,
				target.ID,
			)
		}
		targetIDs[target.ID] = struct{}{}
	}
	return nil
}

func validateManagementVerifiedAuthorities(
	verified ManagementVerifiedAuthorities,
) error {
	profileIDs := make(
		map[domain.RunnerProfileID]struct{},
		len(verified.RunnerProfiles),
	)
	for _, profile := range verified.RunnerProfiles {
		if err := validateManagementRunnerProfile(profile); err != nil {
			return err
		}
		if _, duplicate := profileIDs[profile.Profile.ID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate verified runner profile %q",
				ErrManagementConfiguration,
				profile.Profile.ID,
			)
		}
		profileIDs[profile.Profile.ID] = struct{}{}
	}
	targetIDs := make(map[domain.TargetID]struct{}, len(verified.GitHubTargets))
	scaleSetIDs := make(map[ScaleSetID]struct{}, len(verified.GitHubTargets))
	for _, target := range verified.GitHubTargets {
		if err := validateManagementGitHubTarget(target); err != nil {
			return err
		}
		if _, duplicate := targetIDs[target.Target.ID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate verified GitHub target %q",
				ErrManagementConfiguration,
				target.Target.ID,
			)
		}
		if _, duplicate := scaleSetIDs[target.ScaleSetID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate verified GitHub scale set %d",
				ErrManagementConfiguration,
				target.ScaleSetID,
			)
		}
		targetIDs[target.Target.ID] = struct{}{}
		scaleSetIDs[target.ScaleSetID] = struct{}{}
	}
	return nil
}

func validateManagementConfiguration(configuration ManagementConfiguration) error {
	if configuration.Revision > maxSQLiteInteger {
		return fmt.Errorf(
			"%w: revision exceeds SQLite range",
			ErrManagementConfiguration,
		)
	}
	if err := validateFleetMaximum(configuration.FleetMaxRunners); err != nil {
		return err
	}
	desired := DesiredManagementConfiguration{
		FleetMaxRunners: configuration.FleetMaxRunners,
		Nodes:           configuration.Nodes,
		RunnerProfiles: make(
			[]domain.RunnerProfile,
			0,
			len(configuration.RunnerProfiles),
		),
		GitHubTargets: make(
			[]DesiredManagementGitHubTarget,
			0,
			len(configuration.GitHubTargets),
		),
	}
	scaleSetIDs := make(map[ScaleSetID]struct{}, len(configuration.GitHubTargets))
	for _, profile := range configuration.RunnerProfiles {
		if err := validateManagementRunnerProfile(profile); err != nil {
			return err
		}
		desired.RunnerProfiles = append(desired.RunnerProfiles, profile.Profile)
	}
	for _, target := range configuration.GitHubTargets {
		if err := validateManagementGitHubTarget(target); err != nil {
			return err
		}
		if _, duplicate := scaleSetIDs[target.ScaleSetID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate GitHub scale set %d",
				ErrManagementConfiguration,
				target.ScaleSetID,
			)
		}
		scaleSetIDs[target.ScaleSetID] = struct{}{}
		desired.GitHubTargets = append(
			desired.GitHubTargets,
			desiredGitHubTarget(target),
		)
	}
	return validateDesiredManagementConfiguration(desired)
}

func materializeManagementConfiguration(
	current ManagementConfiguration,
	desired DesiredManagementConfiguration,
	verified ManagementVerifiedAuthorities,
) (ManagementConfiguration, error) {
	currentProfiles := make(
		map[domain.RunnerProfileID]ManagementRunnerProfile,
		len(current.RunnerProfiles),
	)
	for _, profile := range current.RunnerProfiles {
		currentProfiles[profile.Profile.ID] = profile
	}
	verifiedProfiles := make(
		map[domain.RunnerProfileID]ManagementRunnerProfile,
		len(verified.RunnerProfiles),
	)
	for _, profile := range verified.RunnerProfiles {
		verifiedProfiles[profile.Profile.ID] = profile
	}
	currentTargets := make(
		map[domain.TargetID]ManagementGitHubTarget,
		len(current.GitHubTargets),
	)
	for _, target := range current.GitHubTargets {
		currentTargets[target.Target.ID] = target
	}
	verifiedTargets := make(
		map[domain.TargetID]ManagementGitHubTarget,
		len(verified.GitHubTargets),
	)
	for _, target := range verified.GitHubTargets {
		verifiedTargets[target.Target.ID] = target
	}

	next := ManagementConfiguration{
		FleetMaxRunners: cloneOptionalInt(desired.FleetMaxRunners),
		Nodes:           append([]ManagementNodeConfiguration(nil), desired.Nodes...),
	}
	usedProfiles := make(map[domain.RunnerProfileID]struct{}, len(verifiedProfiles))
	for _, profile := range desired.RunnerProfiles {
		if authority, exists := verifiedProfiles[profile.ID]; exists {
			if !equalRunnerProfile(authority.Profile, profile) {
				return ManagementConfiguration{}, fmt.Errorf(
					"%w: verified runner profile %q does not match desired fields",
					ErrManagementConfiguration,
					profile.ID,
				)
			}
			next.RunnerProfiles = append(next.RunnerProfiles, authority)
			usedProfiles[profile.ID] = struct{}{}
			continue
		}
		existing, exists := currentProfiles[profile.ID]
		if !exists {
			return ManagementConfiguration{}, fmt.Errorf(
				"%w: new runner profile %q requires verified runner version",
				ErrManagementConfiguration,
				profile.ID,
			)
		}
		existing.Profile = profile
		next.RunnerProfiles = append(next.RunnerProfiles, existing)
	}
	for profileID := range verifiedProfiles {
		if _, used := usedProfiles[profileID]; !used {
			return ManagementConfiguration{}, fmt.Errorf(
				"%w: verified runner profile %q is not desired",
				ErrManagementConfiguration,
				profileID,
			)
		}
	}

	usedTargets := make(map[domain.TargetID]struct{}, len(verifiedTargets))
	for _, target := range desired.GitHubTargets {
		if authority, exists := verifiedTargets[target.ID]; exists {
			if desiredGitHubTarget(authority) != target {
				return ManagementConfiguration{}, fmt.Errorf(
					"%w: verified GitHub target %q does not match desired fields",
					ErrManagementConfiguration,
					target.ID,
				)
			}
			next.GitHubTargets = append(next.GitHubTargets, authority)
			usedTargets[target.ID] = struct{}{}
			continue
		}
		existing, exists := currentTargets[target.ID]
		if !exists || !sameVerifiedTargetIdentity(existing.Target, target) {
			return ManagementConfiguration{}, fmt.Errorf(
				"%w: new or changed GitHub target %q requires verified authority",
				ErrManagementConfiguration,
				target.ID,
			)
		}
		existing.Target.RunnerProfileID = target.RunnerProfileID
		next.GitHubTargets = append(next.GitHubTargets, existing)
	}
	for targetID := range verifiedTargets {
		if _, used := usedTargets[targetID]; !used {
			return ManagementConfiguration{}, fmt.Errorf(
				"%w: verified GitHub target %q is not desired",
				ErrManagementConfiguration,
				targetID,
			)
		}
	}

	sort.Slice(next.Nodes, func(i, j int) bool {
		return next.Nodes[i].NodeID < next.Nodes[j].NodeID
	})
	sort.Slice(next.RunnerProfiles, func(i, j int) bool {
		return next.RunnerProfiles[i].Profile.ID < next.RunnerProfiles[j].Profile.ID
	})
	sort.Slice(next.GitHubTargets, func(i, j int) bool {
		return next.GitHubTargets[i].Target.ID < next.GitHubTargets[j].Target.ID
	})
	if err := validateManagementConfiguration(next); err != nil {
		return ManagementConfiguration{}, err
	}
	return next, nil
}

func validateFleetMaximum(value *int) error {
	if value != nil && (*value < 1 || uint64(*value) > maxSQLiteInteger) {
		return fmt.Errorf(
			"%w: invalid fleet maximum",
			ErrManagementConfiguration,
		)
	}
	return nil
}

func validateManagementNodeConfiguration(node ManagementNodeConfiguration) error {
	if !canonicalRuntimeIdentifier(string(node.NodeID)) ||
		node.DisplayName == "" ||
		node.MaxRunners < 1 ||
		uint64(node.MaxRunners) > maxSQLiteInteger {
		return fmt.Errorf(
			"%w: invalid node configuration",
			ErrManagementConfiguration,
		)
	}
	return nil
}

func validateDesiredManagementRunnerProfile(profile domain.RunnerProfile) error {
	if err := profile.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrManagementConfiguration, err)
	}
	if !canonicalRuntimeIdentifier(string(profile.ID)) ||
		profile.MinAvailableMemoryBytes > maxSQLiteInteger {
		return fmt.Errorf(
			"%w: invalid runner profile configuration",
			ErrManagementConfiguration,
		)
	}
	return nil
}

func validateManagementRunnerProfile(profile ManagementRunnerProfile) error {
	if err := validateDesiredManagementRunnerProfile(profile.Profile); err != nil {
		return err
	}
	if !canonicalRunnerVersion(profile.RunnerVersion) {
		return fmt.Errorf(
			"%w: invalid runner profile configuration",
			ErrManagementConfiguration,
		)
	}
	return nil
}

func validateDesiredManagementGitHubTarget(
	target DesiredManagementGitHubTarget,
) error {
	if !canonicalRuntimeIdentifier(string(target.ID)) ||
		target.InstallationID == "" ||
		target.Scope == "" ||
		target.ScaleSetName == "" ||
		!canonicalRuntimeIdentifier(string(target.RunnerProfileID)) {
		return fmt.Errorf(
			"%w: invalid desired GitHub target",
			ErrManagementConfiguration,
		)
	}
	switch target.ScopeKind {
	case domain.TargetRepository, domain.TargetOrganization:
	default:
		return fmt.Errorf(
			"%w: invalid desired GitHub target scope kind",
			ErrManagementConfiguration,
		)
	}
	return nil
}

func validateManagementGitHubTarget(target ManagementGitHubTarget) error {
	if err := target.Target.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrManagementConfiguration, err)
	}
	if !canonicalRuntimeIdentifier(string(target.Target.ID)) {
		return fmt.Errorf(
			"%w: invalid GitHub target ID",
			ErrManagementConfiguration,
		)
	}
	if err := validateScaleSetID(target.ScaleSetID); err != nil {
		return fmt.Errorf("%w: %v", ErrManagementConfiguration, err)
	}
	return nil
}

func desiredGitHubTarget(target ManagementGitHubTarget) DesiredManagementGitHubTarget {
	return DesiredManagementGitHubTarget{
		ID:              target.Target.ID,
		InstallationID:  target.Target.InstallationID,
		ScopeKind:       target.Target.ScopeKind,
		Scope:           target.Target.Scope,
		ScaleSetName:    target.Target.ScaleSetName,
		RunnerProfileID: target.Target.RunnerProfileID,
	}
}

func sameVerifiedTargetIdentity(
	current domain.GitHubTarget,
	desired DesiredManagementGitHubTarget,
) bool {
	return current.ID == desired.ID &&
		current.InstallationID == desired.InstallationID &&
		current.ScopeKind == desired.ScopeKind &&
		current.Scope == desired.Scope &&
		current.ScaleSetName == desired.ScaleSetName
}

func equalRunnerProfile(left, right domain.RunnerProfile) bool {
	return left.ID == right.ID &&
		left.Label == right.Label &&
		equalOperatingSystem(left.OS, right.OS) &&
		equalArchitecture(left.Architecture, right.Architecture) &&
		left.MinAvailableMemoryBytes == right.MinAvailableMemoryBytes &&
		left.VersionPolicy == right.VersionPolicy &&
		left.Runtime == right.Runtime
}

func equalOperatingSystem(
	left, right *domain.OperatingSystem,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func equalArchitecture(left, right *domain.Architecture) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func validateConfigurationApplyAudit(record AuditRecord) error {
	if err := validateAuditRecord(record); err != nil {
		return err
	}
	if record.Actor != AuditActorSingleAdmin ||
		record.Action != AuditActionConfigurationApplied ||
		record.Outcome != AuditOutcomeSucceeded ||
		record.ResourceKind != AuditResourceConfiguration ||
		record.ResourceID != "" {
		return fmt.Errorf(
			"%w: configuration apply requires the single-admin success record",
			ErrManagementAuditRecord,
		)
	}
	return nil
}

func validateExactManagementAudit(
	record AuditRecord,
	action AuditAction,
	resourceKind AuditResourceKind,
	resourceID string,
) error {
	return validateExactAudit(
		record,
		AuditActorSingleAdmin,
		action,
		resourceKind,
		resourceID,
	)
}

func validateExactAudit(
	record AuditRecord,
	actor AuditActor,
	action AuditAction,
	resourceKind AuditResourceKind,
	resourceID string,
) error {
	if err := validateAuditRecord(record); err != nil {
		return err
	}
	if record.Actor != actor ||
		record.Action != action ||
		record.Outcome != AuditOutcomeSucceeded ||
		record.ResourceKind != resourceKind ||
		record.ResourceID != resourceID {
		return fmt.Errorf(
			"%w: mutation success record does not match the operation",
			ErrManagementAuditRecord,
		)
	}
	return nil
}

func nodeAdministrativeAuditAction(
	next domain.NodeAdministrativeState,
) (AuditAction, error) {
	switch next {
	case domain.NodeActive:
		return AuditActionNodeResumed, nil
	case domain.NodeDraining:
		return AuditActionNodeDrained, nil
	case domain.NodeRevoked:
		return AuditActionNodeRevoked, nil
	default:
		return "", fmt.Errorf(
			"%w: unsupported node administrative state",
			ErrManagementConfiguration,
		)
	}
}

func updateNodeAdministrativeState(
	ctx context.Context,
	tx *sql.Tx,
	nodeID domain.NodeID,
	current domain.NodeAdministrativeState,
	next domain.NodeAdministrativeState,
) error {
	result, err := tx.ExecContext(
		ctx,
		`UPDATE node_administrative_states
		 SET administrative_state = ?
		 WHERE node_id = ? AND administrative_state = ?`,
		next,
		nodeID,
		current,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf(
			"%w: node administrative state changed concurrently",
			ErrManagementConfiguration,
		)
	}
	return nil
}

func updateManagementRevision(
	ctx context.Context,
	tx *sql.Tx,
	current uint64,
	next uint64,
) error {
	result, err := tx.ExecContext(
		ctx,
		`UPDATE management_configuration_state
		 SET revision = ?
		 WHERE singleton = 1 AND revision = ?`,
		next,
		current,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf(
			"%w: management revision compare-and-swap failed",
			ErrManagementConfigurationDB,
		)
	}
	return nil
}

func validateAuditRecord(record AuditRecord) error {
	switch record.Actor {
	case AuditActorAnonymous, AuditActorJoinCode, AuditActorNode, AuditActorSingleAdmin:
	default:
		return fmt.Errorf("%w: actor is not allowlisted", ErrManagementAuditRecord)
	}
	switch record.Action {
	case AuditActionAuthenticationSucceeded,
		AuditActionAuthenticationFailed,
		AuditActionBrowserHandoffAuthorized,
		AuditActionEnrollmentRejected,
		AuditActionEnrollmentUnavailable,
		AuditActionAgentSessionRejected,
		AuditActionSessionEnded,
		AuditActionJoinCodeCreated,
		AuditActionJoinCodeCancelled,
		AuditActionConfigurationApplied,
		AuditActionNodeEnrolled,
		AuditActionNodeDrained,
		AuditActionNodeResumed,
		AuditActionNodeRevoked:
	default:
		return fmt.Errorf("%w: action is not allowlisted", ErrManagementAuditRecord)
	}
	switch record.Outcome {
	case AuditOutcomeSucceeded, AuditOutcomeRejected, AuditOutcomeFailed:
	default:
		return fmt.Errorf("%w: outcome is not allowlisted", ErrManagementAuditRecord)
	}
	switch record.ResourceKind {
	case AuditResourceController,
		AuditResourceConfiguration,
		AuditResourceJoinCode,
		AuditResourceNode:
	default:
		return fmt.Errorf(
			"%w: resource kind is not allowlisted",
			ErrManagementAuditRecord,
		)
	}
	switch record.ResourceKind {
	case AuditResourceController, AuditResourceConfiguration:
		if record.ResourceID != "" {
			return fmt.Errorf(
				"%w: singleton resource cannot have an ID",
				ErrManagementAuditRecord,
			)
		}
	default:
		if !canonicalRuntimeIdentifier(record.ResourceID) {
			return fmt.Errorf(
				"%w: resource ID is required",
				ErrManagementAuditRecord,
			)
		}
	}
	if err := validateAuditActionResource(record); err != nil {
		return err
	}
	if err := validateAuditActorOutcome(record); err != nil {
		return err
	}
	if record.Outcome == AuditOutcomeSucceeded {
		if record.ErrorCode != "" {
			return fmt.Errorf(
				"%w: successful event cannot have an error code",
				ErrManagementAuditRecord,
			)
		}
	} else {
		if err := validateAuditErrorCode(record.ErrorCode); err != nil {
			return err
		}
	}
	if !validAuditRequestID(record.RequestID) {
		return fmt.Errorf(
			"%w: request ID is invalid",
			ErrManagementAuditRecord,
		)
	}
	return nil
}

func validateAuditActorOutcome(record AuditRecord) error {
	switch record.Action {
	case AuditActionAuthenticationSucceeded:
		if record.Actor != AuditActorSingleAdmin ||
			record.Outcome != AuditOutcomeSucceeded {
			return fmt.Errorf(
				"%w: authentication success actor or outcome is invalid",
				ErrManagementAuditRecord,
			)
		}
	case AuditActionAuthenticationFailed:
		if record.Actor != AuditActorAnonymous ||
			(record.Outcome != AuditOutcomeRejected &&
				record.Outcome != AuditOutcomeFailed) {
			return fmt.Errorf(
				"%w: authentication failure actor or outcome is invalid",
				ErrManagementAuditRecord,
			)
		}
	case AuditActionEnrollmentRejected:
		if record.Actor != AuditActorAnonymous ||
			record.Outcome != AuditOutcomeRejected {
			return fmt.Errorf(
				"%w: enrollment rejection actor or outcome is invalid",
				ErrManagementAuditRecord,
			)
		}
	case AuditActionEnrollmentUnavailable:
		if record.Actor != AuditActorAnonymous ||
			record.Outcome != AuditOutcomeFailed {
			return fmt.Errorf(
				"%w: enrollment availability actor or outcome is invalid",
				ErrManagementAuditRecord,
			)
		}
	case AuditActionAgentSessionRejected:
		if record.Actor != AuditActorNode ||
			record.Outcome != AuditOutcomeRejected {
			return fmt.Errorf(
				"%w: agent session rejection actor or outcome is invalid",
				ErrManagementAuditRecord,
			)
		}
	case AuditActionNodeEnrolled:
		if record.Actor != AuditActorJoinCode ||
			record.Outcome != AuditOutcomeSucceeded {
			return fmt.Errorf(
				"%w: enrollment actor or outcome is invalid",
				ErrManagementAuditRecord,
			)
		}
	default:
		if record.Actor != AuditActorSingleAdmin {
			return fmt.Errorf(
				"%w: management mutation actor is invalid",
				ErrManagementAuditRecord,
			)
		}
	}
	return nil
}

func validateAuditActionResource(record AuditRecord) error {
	var expected AuditResourceKind
	switch record.Action {
	case AuditActionAuthenticationSucceeded,
		AuditActionAuthenticationFailed,
		AuditActionBrowserHandoffAuthorized,
		AuditActionEnrollmentRejected,
		AuditActionEnrollmentUnavailable,
		AuditActionSessionEnded:
		expected = AuditResourceController
	case AuditActionConfigurationApplied:
		expected = AuditResourceConfiguration
	case AuditActionJoinCodeCreated, AuditActionJoinCodeCancelled:
		expected = AuditResourceJoinCode
	case AuditActionNodeEnrolled,
		AuditActionAgentSessionRejected,
		AuditActionNodeDrained,
		AuditActionNodeResumed,
		AuditActionNodeRevoked:
		expected = AuditResourceNode
	default:
		return fmt.Errorf(
			"%w: action is not allowlisted",
			ErrManagementAuditRecord,
		)
	}
	if record.ResourceKind != expected {
		return fmt.Errorf(
			"%w: action and resource kind do not match",
			ErrManagementAuditRecord,
		)
	}
	return nil
}

func validateAuditErrorCode(code AuditErrorCode) error {
	switch code {
	case AuditErrorAuthenticationFailed,
		AuditErrorEnrollmentRejected,
		AuditErrorEnrollmentRateLimited,
		AuditErrorEnrollmentUnavailable,
		AuditErrorNodeCredentialRejected,
		AuditErrorAgentProtocolRejected,
		AuditErrorHostRejected,
		AuditErrorEventStreamRejected,
		AuditErrorMutationRejected,
		AuditErrorManagementUnavailable,
		AuditErrorConfigurationConflict,
		AuditErrorValidationFailed,
		AuditErrorJoinCodeNotFound,
		AuditErrorStateConflict,
		AuditErrorInvalidBody,
		AuditErrorInvalidPrecondition,
		AuditErrorPreconditionRequired,
		AuditErrorRequestForbidden,
		AuditErrorMisdirectedHost,
		AuditErrorMethodNotAllowed,
		AuditErrorPayloadTooLarge,
		AuditErrorUnsupportedMediaType,
		AuditErrorSessionUnavailable,
		AuditErrorInvalidSessionRequest:
		return nil
	default:
		return fmt.Errorf(
			"%w: error code is not allowlisted",
			ErrManagementAuditRecord,
		)
	}
}

func validAuditRequestID(value string) bool {
	if value == "req_unavailable" {
		return true
	}
	if len(value) != 36 || !strings.HasPrefix(value, "req_") {
		return false
	}
	suffix := strings.TrimPrefix(value, "req_")
	decoded, err := hex.DecodeString(suffix)
	return err == nil &&
		len(decoded) == 16 &&
		hex.EncodeToString(decoded) == suffix
}

func validateStoredAuditEvent(event AuditEvent) error {
	if event.Sequence == 0 ||
		event.Sequence > maxSQLiteInteger ||
		event.OccurredAtUnixNano <= 0 ||
		event.Revision > maxSQLiteInteger {
		return fmt.Errorf(
			"%w: stored event envelope is invalid",
			ErrManagementAuditRecord,
		)
	}
	return validateAuditRecord(event.Record)
}

func cloneManagementConfiguration(
	configuration ManagementConfiguration,
) ManagementConfiguration {
	clone := ManagementConfiguration{
		Revision:        configuration.Revision,
		FleetMaxRunners: cloneOptionalInt(configuration.FleetMaxRunners),
		Nodes: append(
			[]ManagementNodeConfiguration(nil),
			configuration.Nodes...,
		),
		RunnerProfiles: append(
			[]ManagementRunnerProfile(nil),
			configuration.RunnerProfiles...,
		),
		GitHubTargets: append(
			[]ManagementGitHubTarget(nil),
			configuration.GitHubTargets...,
		),
	}
	for index := range clone.RunnerProfiles {
		profile := &clone.RunnerProfiles[index].Profile
		if profile.OS != nil {
			value := *profile.OS
			profile.OS = &value
		}
		if profile.Architecture != nil {
			value := *profile.Architecture
			profile.Architecture = &value
		}
	}
	return clone
}

func cloneOptionalInt(value *int) *int {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func nullableFleetMaximum(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}
