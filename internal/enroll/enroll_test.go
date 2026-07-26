package enroll

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func testService(t *testing.T) (Service, *MemoryRegistry, time.Time) {
	t.Helper()
	now := time.Now().UTC().Add(-time.Minute)
	identity, err := NewControllerIdentity(now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var digestKey [32]byte
	if _, err := rand.Read(digestKey[:]); err != nil {
		t.Fatal(err)
	}
	registry := NewMemoryRegistry()
	service, err := NewService(registry, identity, digestKey, 7)
	if err != nil {
		t.Fatal(err)
	}
	service.Now = func() time.Time { return now }
	return service, registry, now
}

func nodeCSR(t *testing.T) ([]byte, ed25519.PrivateKey) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := CreateNodeCSR(key)
	if err != nil {
		t.Fatal(err)
	}
	return csr, key
}

func TestJoinCodeCanonicalAndSecretDigest(t *testing.T) {
	var fingerprint [32]byte
	if _, err := rand.Read(fingerprint[:]); err != nil {
		t.Fatal(err)
	}
	code, err := NewJoinCode(fingerprint, []string{"controller.example.test:443"}, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := code.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeJoinCode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.TokenID() != code.TokenID() || decoded.secret != code.secret || decoded.CAFingerprint() != fingerprint {
		t.Fatal("join code changed during canonical round-trip")
	}
	for _, malformed := range []string{encoded + "=", encoded + "A", "twk_"} {
		if _, err := DecodeJoinCode(malformed); err == nil {
			t.Fatalf("accepted noncanonical code %q", malformed[:4])
		}
	}
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	digest := SecretDigest(key, code.tokenID, code.secret)
	if !VerifySecretDigest(digest, SecretDigest(key, code.tokenID, code.secret)) {
		t.Fatal("valid digest rejected")
	}
	var different [32]byte
	if _, err := rand.Read(different[:]); err != nil {
		t.Fatal(err)
	}
	if VerifySecretDigest(digest, SecretDigest(key, code.tokenID, different)) {
		t.Fatal("different secret accepted")
	}
	if hints, err := CanonicalHints([]string{"https://controller.example.test/"}); err != nil || len(hints) != 1 || hints[0] != "https://controller.example.test" {
		t.Fatalf("root endpoint hint = %#v, %v", hints, err)
	}
	if _, err := CanonicalHints([]string{"https://controller.example.test/untrusted-path"}); !errors.Is(err, ErrInvalidJoinCode) {
		t.Fatalf("path-bearing endpoint hint = %v", err)
	}
}

func TestSecretBearingTypesAreRedactedAcrossFormattingAndJSON(t *testing.T) {
	service, _, _ := testService(t)
	code, err := NewJoinCode(service.Identity.CAFingerprint(), nil, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := code.Encode()
	if err != nil {
		t.Fatal(err)
	}
	for _, rendered := range []string{fmt.Sprint(code), fmt.Sprintf("%#v", code), code.LogValue().String(), service.Identity.String(), fmt.Sprintf("%#v", service.Identity), service.Identity.LogValue().String(), service.String(), fmt.Sprintf("%#v", service), service.LogValue().String()} {
		if strings.Contains(rendered, encoded) || strings.Contains(rendered, string(code.secret[:])) {
			t.Fatalf("secret leaked through representation %q", rendered)
		}
	}
	serializedCode, err := json.Marshal(code)
	if err != nil {
		t.Fatal(err)
	}
	serializedIdentity, err := json.Marshal(service.Identity)
	if err != nil {
		t.Fatal(err)
	}
	serializedService, err := json.Marshal(service)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(serializedCode), encoded) || strings.Contains(string(serializedIdentity), string(service.Identity.key)) || strings.Contains(string(serializedService), string(service.digestKey[:])) {
		t.Fatal("secret leaked through JSON")
	}
}

func TestEnrollmentConsumesOnceAndFailsClosed(t *testing.T) {
	service, registry, _ := testService(t)
	code, err := service.CreateJoinCode(context.Background(), []string{"controller.example.test:443"})
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := nodeCSR(t)
	result, err := service.Enroll(context.Background(), code, csr)
	if err != nil {
		t.Fatal(err)
	}
	if result.NodeID == "" || result.Credential.NodeID != result.NodeID || result.Credential.Epoch != 1 {
		t.Fatal("missing node identity")
	}
	replayed, err := service.Enroll(context.Background(), code, csr)
	if err != nil || replayed.NodeID != result.NodeID || replayed.Credential != result.Credential {
		t.Fatalf("idempotent replay = %+v, %v", replayed, err)
	}
	restarted := service
	restarted.Epoch++
	replayed, err = restarted.Enroll(context.Background(), code, csr)
	if err != nil || replayed.NodeID != result.NodeID || replayed.Credential != result.Credential {
		t.Fatalf("restart replay = %+v, %v", replayed, err)
	}
	if err := registry.AuthorizeCredential(context.Background(), result.Credential, service.Now()); err != nil {
		t.Fatalf("fresh credential rejected: %v", err)
	}

	cancelCode, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeJoinCode(cancelCode)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Cancel(context.Background(), decoded.TokenID()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Enroll(context.Background(), cancelCode, csr); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("cancelled token = %v", err)
	}

	pendingCode, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := service.Enroll(context.Background(), pendingCode, csr)
	if err != nil {
		t.Fatal(err)
	}
	decodedPending, err := DecodeJoinCode(pendingCode)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Cancel(context.Background(), decodedPending.TokenID()); err != nil {
		t.Fatal(err)
	}
	if err := registry.AuthorizeCredential(context.Background(), pending.Credential, service.Now()); !errors.Is(err, ErrCredentialRejected) {
		t.Fatalf("cancelled issued credential = %v", err)
	}
	if _, err := service.Enroll(context.Background(), pendingCode, csr); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("cancelled issued replay = %v", err)
	}

	other := service
	other.Epoch++
	if _, err := other.Enroll(context.Background(), serviceMustCode(t, service), csr); !errors.Is(err, ErrTokenEpochMismatch) {
		t.Fatalf("epoch mismatch = %v", err)
	}
}

func serviceMustCode(t *testing.T, service Service) string {
	t.Helper()
	code, err := service.CreateJoinCode(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return code
}

func TestEnrollmentRaceHasExactlyOneWinner(t *testing.T) {
	service, _, _ := testService(t)
	code := serviceMustCode(t, service)
	var winners int
	var mutex sync.Mutex
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			csr, _ := nodeCSR(t)
			if _, err := service.Enroll(context.Background(), code, csr); err == nil {
				mutex.Lock()
				winners++
				mutex.Unlock()
			}
		}()
	}
	group.Wait()
	if winners != 1 {
		t.Fatalf("token race winners = %d, want 1", winners)
	}
}

func TestCSRSubjectAndSANTamperingCannotControlNodeIdentity(t *testing.T) {
	service, _, now := testService(t)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "attacker"}, DNSNames: []string{"attacker.example.test"}}, key)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := NewNodeID(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate, _, err := service.Identity.IssueNodeCertificate(csr, nodeID, service.Epoch, now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if certificate.Subject.CommonName == "attacker" || len(certificate.DNSNames) != 0 || len(certificate.URIs) != 1 {
		t.Fatal("CSR subject or SAN leaked into certificate")
	}
	if got, _, epoch, err := NodeCredentialIdentity(certificate); err != nil || got != nodeID || epoch != service.Epoch {
		t.Fatalf("controller identity = %q, %d, %v", got, epoch, err)
	}
	broken := append([]byte(nil), csr...)
	broken[len(broken)-1] ^= 0x01
	if _, _, err := service.Identity.IssueNodeCertificate(broken, nodeID, service.Epoch, now, rand.Reader); err == nil {
		t.Fatal("tampered CSR accepted")
	}
}

func TestRenewalPreservesNodeAndSupersedesOldCredential(t *testing.T) {
	service, registry, now := testService(t)
	code := serviceMustCode(t, service)
	csr, _ := nodeCSR(t)
	initial, err := service.Enroll(context.Background(), code, csr)
	if err != nil {
		t.Fatal(err)
	}
	// A controller restart changes the token epoch, not the node credential epoch.
	restarted := service
	restarted.Epoch++
	renewed, err := restarted.Renew(context.Background(), initial.Credential, csr)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.NodeID != initial.NodeID || renewed.Credential.Serial == initial.Credential.Serial {
		t.Fatal("renewal did not preserve identity and replace serial")
	}
	if err := registry.AuthorizeCredential(context.Background(), initial.Credential, now); !errors.Is(err, ErrCredentialRejected) {
		t.Fatalf("superseded credential = %v", err)
	}
	if err := registry.AuthorizeCredential(context.Background(), renewed.Credential, now); err != nil {
		t.Fatalf("renewed credential = %v", err)
	}
	if _, err := registry.RevokeNode(context.Background(), initial.NodeID); err != nil {
		t.Fatal(err)
	}
	if err := registry.AuthorizeCredential(context.Background(), renewed.Credential, now); !errors.Is(err, ErrCredentialRejected) {
		t.Fatalf("revoked credential = %v", err)
	}
	if err := registry.AuthorizeCredential(context.Background(), renewed.Credential, renewed.Credential.NotAfter); !errors.Is(err, ErrCredentialRejected) {
		t.Fatalf("expired credential = %v", err)
	}
}

func TestPrivateIdentityPersistenceAndRenewalJitter(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("platform credential store adapter is owned by twk008/twk009")
	}
	service, _, now := testService(t)
	identityDirectory := t.TempDir()
	if err := os.Chmod(identityDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	path := identityDirectory + "/controller-identity.pem"
	if err := service.Identity.Save(path); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("controller identity permissions = %v, %v", info.Mode().Perm(), err)
	}
	loaded, err := LoadControllerIdentity(path)
	if err != nil || loaded.CAFingerprint() != service.Identity.CAFingerprint() {
		t.Fatalf("identity persistence = %v", err)
	}
	keyDirectory := t.TempDir()
	if err := os.Chmod(keyDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	keyPath := keyDirectory + "/node-key.pem"
	key, err := GenerateAndPersistNodeKey(keyPath, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(keyPath); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("node key permissions = %v, %v", info.Mode().Perm(), err)
	}
	if loadedKey, err := LoadNodePrivateKey(keyPath); err != nil || !loadedKey.Equal(key) {
		t.Fatalf("node key persistence = %v", err)
	}
	certificate, err := x509.ParseCertificate(service.Identity.Certificate.Raw)
	if err != nil {
		t.Fatal(err)
	}
	if due, err := RenewalDue(certificate, now, bytesReader{0}); err != nil || due {
		t.Fatalf("renewal before jitter window = %v, %v", due, err)
	}
}

func TestPrivatePersistenceNeverClobbersOrFollowsUnsafePaths(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("platform credential store adapter is owned by twk008/twk009")
	}
	service, _, _ := testService(t)
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := directory + "/identity.pem"
	if err := service.Identity.Save(path); err != nil {
		t.Fatal(err)
	}
	if err := service.Identity.Save(path); err == nil {
		t.Fatal("identity save clobbered existing file")
	}
	if err := os.Chmod(path, 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadControllerIdentity(path); err == nil {
		t.Fatal("shared identity mode accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/tmp/not-tewake", path); err != nil {
		t.Fatal(err)
	}
	if err := service.Identity.Save(path); err == nil {
		t.Fatal("symlink destination accepted")
	}
	unsafeAncestor := t.TempDir() + "/shared"
	if err := os.Mkdir(unsafeAncestor, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafeAncestor, 0o777); err != nil {
		t.Fatal(err)
	}
	privateChild := unsafeAncestor + "/private"
	if err := os.Mkdir(privateChild, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := service.Identity.Save(privateChild + "/identity.pem"); err == nil {
		t.Fatal("writable non-sticky ancestor accepted")
	}
}

type bytesReader struct{ value byte }

func (reader bytesReader) Read(buffer []byte) (int, error) {
	for index := range buffer {
		buffer[index] = reader.value
	}
	return len(buffer), nil
}
