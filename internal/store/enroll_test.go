package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/genm/tewake/internal/enroll"
)

func TestEnrollmentRegistryConsumesTokenAtomicallyAcrossStoreHandles(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "enroll-race.db")
	first, second := openControllerPath(t, path), openControllerPath(t, path)
	defer first.Close()
	defer second.Close()
	token := enrollmentToken(1, 1)
	if err := first.CreateToken(ctx, token); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	results := make(chan error, 2)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index, candidate := range []*ControllerStore{first, second} {
		group.Add(1)
		go func(index int, candidate *ControllerStore) {
			defer group.Done()
			<-start
			nodeID := enrollmentNodeID(index + 1)
			results <- candidate.ConsumeEnrollment(ctx, token, enrollmentNodeRecord(nodeID, "a"+string(rune('0'+index)), now))
		}(index, candidate)
	}
	close(start)
	group.Wait()
	close(results)
	var successful int
	for err := range results {
		if err == nil {
			successful++
		} else if !errors.Is(err, enroll.ErrTokenNotFound) {
			t.Fatalf("unexpected race error: %v", err)
		}
	}
	if successful != 1 {
		t.Fatalf("token race successes = %d", successful)
	}
	if err := first.CancelToken(ctx, token.ID); err != nil {
		t.Fatalf("issued token cancel = %v", err)
	}
}

func TestEnrollmentRegistryRenewRevokeAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "enroll.db")
	store := openControllerPath(t, path)
	now := time.Now().UTC()
	token := enrollmentToken(2, 1)
	nodeID := enrollmentNodeID(9)
	initial := enrollmentCredential(nodeID, "abc", now)
	if err := store.CreateToken(ctx, token); err != nil {
		t.Fatal(err)
	}
	if err := store.ConsumeEnrollment(ctx, token, enrollmentNodeRecord(nodeID, "abc", now)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openControllerPath(t, path)
	defer store.Close()
	if err := store.AuthorizeCredential(ctx, initial, now); err != nil {
		t.Fatal(err)
	}
	renewed := enrollmentCredential(nodeID, "def", now.Add(time.Minute))
	if err := store.RenewCredential(ctx, nodeID, initial, renewed, now); err != nil {
		t.Fatal(err)
	}
	if err := store.AuthorizeCredential(ctx, initial, now); !errors.Is(err, enroll.ErrCredentialRejected) {
		t.Fatalf("superseded = %v", err)
	}
	if _, err := store.RevokeNode(ctx, nodeID); err != nil {
		t.Fatal(err)
	}
	if err := store.AuthorizeCredential(ctx, renewed, now); !errors.Is(err, enroll.ErrCredentialRejected) {
		t.Fatalf("revoked = %v", err)
	}
	if err := store.RenewCredential(ctx, nodeID, renewed, enrollmentCredential(nodeID, "ghi", now), now); !errors.Is(err, enroll.ErrCredentialRejected) {
		t.Fatalf("renew revoked = %v", err)
	}
}

func TestEnrollmentReplaySurvivesRestartOnlyForSameTokenAndPublicKey(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(privateTestDir(t), "replay-restart.db")
	store := openControllerPath(t, path)
	now := time.Now().UTC()
	token := enrollmentToken(8, 1)
	nodeID := enrollmentNodeID(8)
	node := enrollmentNodeRecord(nodeID, "abc", now)
	if err := store.CreateToken(ctx, token); err != nil {
		t.Fatal(err)
	}
	if err := store.ConsumeEnrollment(ctx, token, node); err != nil {
		t.Fatal(err)
	}
	unused := enrollmentToken(9, 1)
	if err := store.CreateToken(ctx, unused); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openControllerPath(t, path)
	defer store.Close()
	newEpochToken := token
	newEpochToken.Epoch = 2
	replayed, err := store.ReplayEnrollment(ctx, newEpochToken, node.PublicKeyDigest)
	if err != nil || replayed.NodeID != nodeID || string(replayed.CertificateDER) != string(node.CertificateDER) {
		t.Fatalf("restart replay = %+v, %v", replayed, err)
	}
	var other [32]byte
	other[0] = 2
	if _, err := store.ReplayEnrollment(ctx, newEpochToken, other); !errors.Is(err, enroll.ErrTokenNotFound) {
		t.Fatalf("different SPKI replay = %v", err)
	}
	unused.Epoch = 2
	if err := store.ConsumeEnrollment(ctx, unused, enrollmentNodeRecord(enrollmentNodeID(7), "def", now)); !errors.Is(err, enroll.ErrTokenEpochMismatch) {
		t.Fatalf("unused old token epoch = %v", err)
	}
}

func TestEnrollmentTokenCreationRejectsAStaleControllerEpoch(t *testing.T) {
	ctx := context.Background()
	store := openControllerPath(t, filepath.Join(privateTestDir(t), "token-epoch.db"))
	defer store.Close()
	if err := store.CreateToken(ctx, enrollmentToken(1, 1)); err != nil {
		t.Fatal(err)
	}
	if epoch, err := store.AdvanceEpoch(ctx); err != nil || epoch != 1 {
		t.Fatalf("first epoch = %d, %v", epoch, err)
	}
	if epoch, err := store.AdvanceEpoch(ctx); err != nil || epoch != 2 {
		t.Fatalf("second epoch = %d, %v", epoch, err)
	}
	if err := store.CreateToken(ctx, enrollmentToken(2, 1)); !errors.Is(err, enroll.ErrTokenEpochMismatch) {
		t.Fatalf("stale token creation = %v", err)
	}
	if err := store.CreateToken(ctx, enrollmentToken(3, 2)); err != nil {
		t.Fatalf("current token creation = %v", err)
	}
}

func enrollmentToken(seed byte, epoch uint64) enroll.TokenRecord {
	var token enroll.TokenRecord
	token.ID[0] = seed
	token.SecretDigest[0] = seed + 1
	token.Epoch = epoch
	return token
}
func enrollmentNodeID(seed int) string {
	return "0000000000000000000000000000000" + string(rune('0'+seed))
}
func enrollmentCredential(nodeID, serial string, now time.Time) enroll.Credential {
	return enroll.Credential{NodeID: nodeID, Serial: serial, Epoch: 1, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour)}
}
func enrollmentNodeRecord(nodeID, serial string, now time.Time) enroll.NodeRecord {
	var digest [32]byte
	digest[0] = 1
	return enroll.NodeRecord{NodeID: nodeID, Credential: enrollmentCredential(nodeID, serial, now), PublicKeyDigest: digest, CertificateDER: []byte{1}, CACertificateDER: []byte{2}}
}
