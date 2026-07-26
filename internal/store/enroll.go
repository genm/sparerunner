package store

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/genm/tewake/internal/enroll"
)

// The controller database records only token HMAC digests and credential
// metadata. Join codes, raw secrets, certificate DER, and private keys are
// intentionally absent from this schema and from every SQL argument here.
func (store *ControllerStore) CreateToken(ctx context.Context, token enroll.TokenRecord) error {
	if err := store.requireReady(); err != nil {
		return err
	}
	if token.Epoch == 0 || token.Epoch > maxSQLiteInteger || allZero(token.ID[:]) || allZero(token.SecretDigest[:]) {
		return enroll.ErrTokenEpochMismatch
	}
	_, err := store.db.ExecContext(ctx, `INSERT INTO enrollment_tokens(token_id, secret_digest, controller_epoch) VALUES (?, ?, ?)`, token.ID[:], token.SecretDigest[:], token.Epoch)
	return err
}

func (store *ControllerStore) ConsumeEnrollment(ctx context.Context, supplied enroll.TokenRecord, node enroll.NodeRecord) error {
	if err := store.requireReady(); err != nil {
		return err
	}
	if supplied.Epoch == 0 || supplied.Epoch > maxSQLiteInteger || allZero(supplied.ID[:]) || allZero(supplied.SecretDigest[:]) || !canonicalNodeID(node.NodeID) || node.Credential.NodeID != node.NodeID || !canonicalSerial(node.Credential.Serial) || node.Credential.Epoch == 0 || allZero(node.PublicKeyDigest[:]) || len(node.CertificateDER) == 0 || len(node.CACertificateDER) == 0 {
		return enroll.ErrCredentialRejected
	}
	before, after, err := enrollmentTimes(node.Credential)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var digest []byte
	var epoch uint64
	err = tx.QueryRowContext(ctx, `SELECT secret_digest, controller_epoch FROM enrollment_tokens WHERE token_id = ?`, supplied.ID[:]).Scan(&digest, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return enroll.ErrTokenNotFound
	}
	if err != nil {
		return err
	}
	if epoch != supplied.Epoch {
		return enroll.ErrTokenEpochMismatch
	}
	if len(digest) != len(supplied.SecretDigest) || subtle.ConstantTimeCompare(digest, supplied.SecretDigest[:]) != 1 {
		return enroll.ErrTokenSecretMismatch
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO enrolled_nodes(node_id, current_serial, credential_epoch, not_before_unix_nano, not_after_unix_nano, revoked) VALUES (?, ?, ?, ?, ?, 0)`, node.NodeID, node.Credential.Serial, node.Credential.Epoch, before, after); err != nil {
		return fmt.Errorf("create enrolled node: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO enrollment_replays(token_id, secret_digest, controller_epoch, public_key_digest, node_id, certificate_der, ca_der) VALUES (?, ?, ?, ?, ?, ?, ?)`, supplied.ID[:], supplied.SecretDigest[:], supplied.Epoch, node.PublicKeyDigest[:], node.NodeID, node.CertificateDER, node.CACertificateDER); err != nil {
		return fmt.Errorf("create enrollment replay: %w", err)
	}
	if result, err := tx.ExecContext(ctx, `DELETE FROM enrollment_tokens WHERE token_id = ?`, supplied.ID[:]); err != nil {
		return err
	} else if affected, _ := result.RowsAffected(); affected != 1 {
		return enroll.ErrTokenNotFound
	}
	return tx.Commit()
}

func (store *ControllerStore) ReplayEnrollment(ctx context.Context, supplied enroll.TokenRecord, csrDigest [32]byte) (enroll.NodeRecord, error) {
	if err := store.requireReady(); err != nil {
		return enroll.NodeRecord{}, err
	}
	if allZero(supplied.ID[:]) || allZero(supplied.SecretDigest[:]) || allZero(csrDigest[:]) {
		return enroll.NodeRecord{}, enroll.ErrTokenNotFound
	}
	var nodeID, serial string
	var epoch uint64
	var before, after int64
	var revoked int
	var storedSecret, storedDigest, certificateDER, caDER []byte
	var storedEpoch uint64
	err := store.db.QueryRowContext(ctx, `SELECT n.node_id, n.current_serial, n.credential_epoch, n.not_before_unix_nano, n.not_after_unix_nano, n.revoked, r.secret_digest, r.controller_epoch, r.public_key_digest, r.certificate_der, r.ca_der FROM enrollment_replays r JOIN enrolled_nodes n ON n.node_id = r.node_id WHERE r.token_id = ?`, supplied.ID[:]).Scan(&nodeID, &serial, &epoch, &before, &after, &revoked, &storedSecret, &storedEpoch, &storedDigest, &certificateDER, &caDER)
	if errors.Is(err, sql.ErrNoRows) {
		return enroll.NodeRecord{}, enroll.ErrTokenNotFound
	}
	if err != nil {
		return enroll.NodeRecord{}, err
	}
	if len(storedSecret) != 32 || len(storedDigest) != 32 || supplied.Epoch == 0 || revoked != 0 || subtle.ConstantTimeCompare(storedSecret, supplied.SecretDigest[:]) != 1 || subtle.ConstantTimeCompare(storedDigest, csrDigest[:]) != 1 {
		return enroll.NodeRecord{}, enroll.ErrTokenNotFound
	}
	return enroll.NodeRecord{NodeID: nodeID, Credential: enroll.Credential{NodeID: nodeID, Serial: serial, Epoch: epoch, NotBefore: time.Unix(0, before), NotAfter: time.Unix(0, after)}, Revoked: revoked != 0, PublicKeyDigest: csrDigest, CertificateDER: certificateDER, CACertificateDER: caDER}, nil
}

func (store *ControllerStore) FinalizeEnrollment(ctx context.Context, credential enroll.Credential) error {
	if err := store.requireReady(); err != nil {
		return err
	}
	if err := store.AuthorizeCredential(ctx, credential, time.Now()); err != nil {
		return err
	}
	_, err := store.db.ExecContext(ctx, `DELETE FROM enrollment_replays WHERE node_id = ?`, credential.NodeID)
	return err
}

func (store *ControllerStore) CancelToken(ctx context.Context, tokenID [16]byte) error {
	if err := store.requireReady(); err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if result, deleteErr := tx.ExecContext(ctx, `DELETE FROM enrollment_tokens WHERE token_id = ?`, tokenID[:]); deleteErr != nil {
		return deleteErr
	} else if affected, _ := result.RowsAffected(); affected == 1 {
		return tx.Commit()
	}
	var nodeID string
	var epoch uint64
	if err := tx.QueryRowContext(ctx, `SELECT n.node_id, n.credential_epoch FROM enrollment_replays r JOIN enrolled_nodes n ON n.node_id=r.node_id WHERE r.token_id=?`, tokenID[:]).Scan(&nodeID, &epoch); errors.Is(err, sql.ErrNoRows) {
		return enroll.ErrTokenNotFound
	} else if err != nil {
		return err
	}
	if epoch >= maxSQLiteInteger {
		return enroll.ErrCredentialRejected
	}
	if _, err := tx.ExecContext(ctx, `UPDATE enrolled_nodes SET revoked=1, credential_epoch=? WHERE node_id=? AND revoked=0`, epoch+1, nodeID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM enrollment_replays WHERE token_id=?`, tokenID[:]); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *ControllerStore) LookupNode(ctx context.Context, nodeID string) (enroll.NodeRecord, error) {
	if err := store.requireReady(); err != nil {
		return enroll.NodeRecord{}, err
	}
	if !canonicalNodeID(nodeID) {
		return enroll.NodeRecord{}, enroll.ErrNodeNotFound
	}
	credential, revoked, err := store.readCredential(ctx, store.db, nodeID)
	if err != nil {
		return enroll.NodeRecord{}, err
	}
	return enroll.NodeRecord{NodeID: nodeID, Credential: credential, Revoked: revoked}, nil
}

func (store *ControllerStore) CurrentCredential(ctx context.Context, nodeID string) (enroll.Credential, error) {
	if err := store.requireReady(); err != nil {
		return enroll.Credential{}, err
	}
	if !canonicalNodeID(nodeID) {
		return enroll.Credential{}, enroll.ErrNodeNotFound
	}
	credential, revoked, err := store.readCredential(ctx, store.db, nodeID)
	if err != nil {
		return enroll.Credential{}, err
	}
	if revoked {
		return enroll.Credential{}, enroll.ErrCredentialRejected
	}
	return credential, nil
}

func (store *ControllerStore) RenewCredential(ctx context.Context, nodeID string, expected, replacement enroll.Credential, now time.Time) error {
	if err := store.requireReady(); err != nil {
		return err
	}
	if !canonicalNodeID(nodeID) || replacement.NodeID != nodeID || !canonicalSerial(expected.Serial) || !canonicalSerial(replacement.Serial) || replacement.Epoch != expected.Epoch || replacement.Serial == expected.Serial {
		return enroll.ErrCredentialRejected
	}
	before, after, err := enrollmentTimes(replacement)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, revoked, err := store.readCredential(ctx, tx, nodeID)
	if err != nil {
		return err
	}
	if revoked || !equalCredential(current, expected) || now.Before(expected.NotBefore) || !now.Before(expected.NotAfter) {
		return enroll.ErrCredentialRejected
	}
	result, err := tx.ExecContext(ctx, `UPDATE enrolled_nodes SET current_serial = ?, credential_epoch = ?, not_before_unix_nano = ?, not_after_unix_nano = ? WHERE node_id = ?`, replacement.Serial, replacement.Epoch, before, after, nodeID)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return enroll.ErrCredentialRejected
	}
	return tx.Commit()
}

func (store *ControllerStore) RevokeNode(ctx context.Context, nodeID string) (enroll.Credential, error) {
	if err := store.requireReady(); err != nil {
		return enroll.Credential{}, err
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return enroll.Credential{}, err
	}
	defer tx.Rollback()
	credential, _, err := store.readCredential(ctx, tx, nodeID)
	if err != nil {
		return enroll.Credential{}, err
	}
	if credential.Epoch >= maxSQLiteInteger {
		return enroll.Credential{}, enroll.ErrCredentialRejected
	}
	previous := credential
	credential.Epoch++
	result, err := tx.ExecContext(ctx, `UPDATE enrolled_nodes SET revoked = 1, credential_epoch = ? WHERE node_id = ? AND revoked = 0`, credential.Epoch, nodeID)
	if err != nil {
		return enroll.Credential{}, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return enroll.Credential{}, enroll.ErrCredentialRejected
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM enrollment_replays WHERE node_id = ?`, nodeID); err != nil {
		return enroll.Credential{}, err
	}
	if err := tx.Commit(); err != nil {
		return enroll.Credential{}, err
	}
	store.notifyRevocation(previous)
	return credential, nil
}

func (store *ControllerStore) SetCredentialRevocationHook(hook func(enroll.Credential)) {
	store.revocationMu.Lock()
	defer store.revocationMu.Unlock()
	store.revocationHook = hook
}
func (store *ControllerStore) notifyRevocation(credential enroll.Credential) {
	store.revocationMu.RLock()
	hook := store.revocationHook
	store.revocationMu.RUnlock()
	if hook != nil {
		hook(credential)
	}
}

func (store *ControllerStore) AuthorizeCredential(ctx context.Context, credential enroll.Credential, now time.Time) error {
	if err := store.requireReady(); err != nil {
		return err
	}
	if !canonicalNodeID(credential.NodeID) || !canonicalSerial(credential.Serial) {
		return enroll.ErrCredentialRejected
	}
	current, revoked, err := store.readCredential(ctx, store.db, credential.NodeID)
	if err != nil || revoked || !equalCredential(current, credential) || now.Before(credential.NotBefore) || !now.Before(credential.NotAfter) {
		return enroll.ErrCredentialRejected
	}
	return nil
}

type credentialQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (store *ControllerStore) readCredential(ctx context.Context, query credentialQueryer, nodeID string) (enroll.Credential, bool, error) {
	var serial string
	var epoch uint64
	var before, after int64
	var revoked int
	err := query.QueryRowContext(ctx, `SELECT current_serial, credential_epoch, not_before_unix_nano, not_after_unix_nano, revoked FROM enrolled_nodes WHERE node_id = ?`, nodeID).Scan(&serial, &epoch, &before, &after, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return enroll.Credential{}, false, enroll.ErrNodeNotFound
	}
	if err != nil {
		return enroll.Credential{}, false, err
	}
	return enroll.Credential{NodeID: nodeID, Serial: serial, Epoch: epoch, NotBefore: time.Unix(0, before), NotAfter: time.Unix(0, after)}, revoked != 0, nil
}

func enrollmentTimes(credential enroll.Credential) (int64, int64, error) {
	if credential.Epoch == 0 || credential.Serial == "" || !credential.NotAfter.After(credential.NotBefore) {
		return 0, 0, enroll.ErrCredentialRejected
	}
	before, err := storeUnixNano(credential.NotBefore)
	if err != nil {
		return 0, 0, err
	}
	after, err := storeUnixNano(credential.NotAfter)
	if err != nil {
		return 0, 0, err
	}
	return before, after, nil
}

func equalCredential(left, right enroll.Credential) bool {
	return left.NodeID == right.NodeID && left.Serial == right.Serial && left.Epoch == right.Epoch && left.NotBefore.Equal(right.NotBefore) && left.NotAfter.Equal(right.NotAfter)
}

func allZero(value []byte) bool {
	var combined byte
	for _, item := range value {
		combined |= item
	}
	return combined == 0
}
func canonicalNodeID(value string) bool {
	return len(value) == 32 && value == strings.ToLower(value) && isLowerHex(value)
}
func canonicalSerial(value string) bool {
	return value != "" && value == strings.ToLower(value) && isLowerHex(value)
}
func isLowerHex(value string) bool {
	for _, item := range value {
		if !(item >= '0' && item <= '9') && !(item >= 'a' && item <= 'f') {
			return false
		}
	}
	return true
}

var _ enroll.Registry = (*ControllerStore)(nil)
