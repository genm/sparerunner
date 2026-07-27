package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/genm/tewake/internal/domain"
	"github.com/genm/tewake/internal/enroll"
	"github.com/genm/tewake/internal/reconcile"
	"github.com/genm/tewake/internal/store"
)

const managementProjectionTimeout = 5 * time.Second

// auditedRuntimeEnrollmentRegistry is the sole production Registry beneath
// Controller enrollment. Code issuance and successful consumption both use
// their store-owned atomic audit transactions; read-only replay remains
// delegated and therefore cannot duplicate an audit event or revision.
type auditedRuntimeEnrollmentRegistry struct {
	enroll.Registry
	store *store.ControllerStore
}

func (registry auditedRuntimeEnrollmentRegistry) CreateToken(
	ctx context.Context,
	token enroll.TokenRecord,
) error {
	if registry.store == nil {
		return errors.New("runtime enrollment audit store is unavailable")
	}
	tokenID := hex.EncodeToString(token.ID[:])
	return registry.store.CreateTokenWithAudit(ctx, token, store.AuditRecord{
		Actor:        store.AuditActorSingleAdmin,
		Action:       store.AuditActionJoinCodeCreated,
		Outcome:      store.AuditOutcomeSucceeded,
		ResourceKind: store.AuditResourceJoinCode,
		ResourceID:   tokenID,
		RequestID:    newEnrollmentAuditRequestID(),
	})
}

func (registry auditedRuntimeEnrollmentRegistry) ConsumeEnrollment(
	ctx context.Context,
	token enroll.TokenRecord,
	node enroll.NodeRecord,
) error {
	if registry.store == nil {
		return errors.New("runtime enrollment audit store is unavailable")
	}
	return registry.store.ConsumeEnrollmentWithAudit(
		ctx,
		token,
		node,
		store.AuditRecord{
			Actor:        store.AuditActorJoinCode,
			Action:       store.AuditActionNodeEnrolled,
			Outcome:      store.AuditOutcomeSucceeded,
			ResourceKind: store.AuditResourceNode,
			ResourceID:   node.NodeID,
			RequestID:    newEnrollmentAuditRequestID(),
		},
	)
}

// projectingEnrollmentRegistry preserves the Registry transaction as the
// enrollment authority, then immediately projects the committed node from a
// fresh store-owned restart snapshot. Replay repeats the projection so an
// interrupted response cannot leave a successful enrollment invisible until
// the Agent's first snapshot.
type projectingEnrollmentRegistry struct {
	enroll.Registry
	reader     reconcile.RestartSnapshotReader
	controller *reconcile.Controller
}

func (registry projectingEnrollmentRegistry) ConsumeEnrollment(
	ctx context.Context,
	token enroll.TokenRecord,
	node enroll.NodeRecord,
) error {
	if err := registry.Registry.ConsumeEnrollment(ctx, token, node); err != nil {
		return err
	}
	return registry.ensureNode(ctx, node.NodeID)
}

func (registry projectingEnrollmentRegistry) ReplayEnrollment(
	ctx context.Context,
	token enroll.TokenRecord,
	publicKeyDigest [32]byte,
) (enroll.NodeRecord, error) {
	node, err := registry.Registry.ReplayEnrollment(ctx, token, publicKeyDigest)
	if err != nil {
		return enroll.NodeRecord{}, err
	}
	if err := registry.ensureNode(ctx, node.NodeID); err != nil {
		return enroll.NodeRecord{}, err
	}
	return node, nil
}

func (registry projectingEnrollmentRegistry) ensureNode(
	ctx context.Context,
	nodeID string,
) error {
	projectionContext, cancel := detachedManagementProjectionContext(ctx)
	defer cancel()
	err := reconcile.EnsureStoreBackedRestartNode(
		projectionContext,
		registry.reader,
		registry.controller,
		domain.NodeID(nodeID),
	)
	if err != nil && registry.controller != nil {
		registry.controller.MarkManagementProjectionUnavailable()
	}
	return err
}

func detachedManagementProjectionContext(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Projection follows a durable commit and therefore cannot inherit client
	// disconnect cancellation. Keep it bounded so a broken local store still
	// fails closed instead of pinning a transport handler.
	return context.WithTimeout(
		context.WithoutCancel(ctx),
		managementProjectionTimeout,
	)
}

func newEnrollmentAuditRequestID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "req_unavailable"
	}
	return "req_" + hex.EncodeToString(value[:])
}
