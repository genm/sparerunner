package enroll

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const (
	CAValidity        = 10 * 365 * 24 * time.Hour
	LeafValidity      = 365 * 24 * time.Hour
	ControllerDNSName = "tewake-controller"
	canonicalNodeHost = "tewake.local"
)

var credentialEpochOID = []int{1, 3, 6, 1, 4, 1, 57264, 1, 1}

type ControllerIdentity struct {
	CA          *x509.Certificate
	caKey       ed25519.PrivateKey
	Certificate *x509.Certificate
	key         ed25519.PrivateKey
}

func (identity ControllerIdentity) String() string       { return "controller-identity[redacted]" }
func (identity ControllerIdentity) GoString() string     { return identity.String() }
func (identity ControllerIdentity) LogValue() slog.Value { return slog.StringValue(identity.String()) }

func NewControllerIdentity(now time.Time, reader io.Reader) (ControllerIdentity, error) {
	if reader == nil {
		reader = rand.Reader
	}
	caPublic, caPrivate, err := ed25519.GenerateKey(reader)
	if err != nil {
		return ControllerIdentity{}, err
	}
	caTemplate, err := certificateTemplate(now, CAValidity, reader)
	if err != nil {
		return ControllerIdentity{}, err
	}
	caTemplate.IsCA = true
	caTemplate.BasicConstraintsValid = true
	caTemplate.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature
	caTemplate.Subject = pkix.Name{CommonName: "Tewake Controller CA"}
	caDER, err := x509.CreateCertificate(reader, caTemplate, caTemplate, caPublic, caPrivate)
	if err != nil {
		return ControllerIdentity{}, err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return ControllerIdentity{}, err
	}
	public, private, err := ed25519.GenerateKey(reader)
	if err != nil {
		return ControllerIdentity{}, err
	}
	leafTemplate, err := certificateTemplate(now, LeafValidity, reader)
	if err != nil {
		return ControllerIdentity{}, err
	}
	leafTemplate.Subject = pkix.Name{CommonName: ControllerDNSName}
	leafTemplate.DNSNames = []string{ControllerDNSName}
	leafTemplate.KeyUsage = x509.KeyUsageDigitalSignature
	leafTemplate.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	leafDER, err := x509.CreateCertificate(reader, leafTemplate, ca, public, caPrivate)
	if err != nil {
		return ControllerIdentity{}, err
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		return ControllerIdentity{}, err
	}
	return ControllerIdentity{CA: ca, caKey: caPrivate, Certificate: leaf, key: private}, nil
}

func (identity ControllerIdentity) TLSCertificate() (tlsCertificate tls.Certificate, err error) {
	if err := identity.Validate(time.Now()); err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{identity.Certificate.Raw, identity.CA.Raw}, PrivateKey: identity.key, Leaf: identity.Certificate}, nil
}

// CAFingerprint is the trust anchor transported by a join code, not a leaf that
// will rotate annually.
func (identity ControllerIdentity) CAFingerprint() [sha256.Size]byte {
	if identity.CA == nil {
		return [sha256.Size]byte{}
	}
	return sha256.Sum256(identity.CA.Raw)
}

func NewNodeID(reader io.Reader) (string, error) {
	if reader == nil {
		reader = rand.Reader
	}
	var value [16]byte
	if _, err := io.ReadFull(reader, value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func CanonicalNodeURI(nodeID string) (*url.URL, error) {
	if len(nodeID) != 32 {
		return nil, errors.New("invalid node ID")
	}
	if _, err := hex.DecodeString(nodeID); err != nil {
		return nil, errors.New("invalid node ID")
	}
	return &url.URL{Scheme: "spiffe", Host: canonicalNodeHost, Path: "/node/" + nodeID}, nil
}

// IssueNodeCertificate deliberately ignores CSR subject and SAN fields. Only a
// verified public key is read from the CSR; controller-owned identity is written
// into the certificate.
func (identity ControllerIdentity) IssueNodeCertificate(csrDER []byte, nodeID string, epoch uint64, now time.Time, reader io.Reader) (*x509.Certificate, []byte, error) {
	if epoch == 0 || identity.CA == nil || len(identity.caKey) != ed25519.PrivateKeySize || !identity.CA.NotAfter.After(now) {
		return nil, nil, errors.New("invalid certificate authority or credential epoch")
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		return nil, nil, errors.New("invalid certificate request")
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, nil, errors.New("invalid certificate request")
	}
	uri, err := CanonicalNodeURI(nodeID)
	if err != nil {
		return nil, nil, err
	}
	validity := LeafValidity
	if remaining := identity.CA.NotAfter.Sub(now); remaining < validity {
		validity = remaining
	}
	template, err := certificateTemplate(now, validity, reader)
	if err != nil {
		return nil, nil, err
	}
	template.Subject = pkix.Name{CommonName: "tewake-node-" + nodeID}
	template.URIs = []*url.URL{uri}
	template.KeyUsage = x509.KeyUsageDigitalSignature
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	epochBytes, err := asn1.Marshal(new(big.Int).SetUint64(epoch))
	if err != nil {
		return nil, nil, err
	}
	template.ExtraExtensions = []pkix.Extension{{Id: credentialEpochOID, Value: epochBytes}}
	der, err := x509.CreateCertificate(readerOrDefault(reader), template, identity.CA, csr.PublicKey, identity.caKey)
	if err != nil {
		return nil, nil, err
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	return certificate, der, nil
}

func certificateTemplate(now time.Time, validity time.Duration, reader io.Reader) (*x509.Certificate, error) {
	serial, err := positiveSerial(readerOrDefault(reader))
	if err != nil {
		return nil, err
	}
	return &x509.Certificate{SerialNumber: serial, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(validity), BasicConstraintsValid: true}, nil
}

func positiveSerial(reader io.Reader) (*big.Int, error) {
	// rand.Int's range starts at zero; shift it so every issued serial is
	// strictly positive while retaining 127 bits of unpredictable entropy.
	upperBound := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
	serial, err := rand.Int(reader, upperBound)
	if err != nil {
		return nil, err
	}
	return serial.Add(serial, big.NewInt(1)), nil
}

func readerOrDefault(reader io.Reader) io.Reader {
	if reader == nil {
		return rand.Reader
	}
	return reader
}

func NodeCredentialIdentity(certificate *x509.Certificate) (nodeID, serial string, epoch uint64, err error) {
	if certificate == nil || certificate.SerialNumber == nil || certificate.SerialNumber.Sign() <= 0 || len(certificate.URIs) != 1 {
		return "", "", 0, errors.New("invalid node certificate")
	}
	uri := certificate.URIs[0]
	if uri.Scheme != "spiffe" || uri.Host != canonicalNodeHost || len(uri.Path) != len("/node/")+32 {
		return "", "", 0, errors.New("invalid node certificate")
	}
	nodeID = uri.Path[len("/node/"):]
	canonical, canonicalErr := CanonicalNodeURI(nodeID)
	if canonicalErr != nil || canonical.String() != uri.String() {
		return "", "", 0, errors.New("invalid node certificate")
	}
	var found bool
	for _, extension := range certificate.Extensions {
		if extension.Id.Equal(credentialEpochOID) {
			if found {
				return "", "", 0, errors.New("duplicate node credential epoch")
			}
			found = true
			var parsed *big.Int
			rest, parseErr := asn1.Unmarshal(extension.Value, &parsed)
			if parseErr != nil || len(rest) != 0 || parsed == nil || parsed.Sign() <= 0 || !parsed.IsUint64() {
				return "", "", 0, errors.New("invalid node credential epoch")
			}
			epoch = parsed.Uint64()
		}
	}
	if !found {
		return "", "", 0, errors.New("missing node credential epoch")
	}
	return nodeID, certificate.SerialNumber.Text(16), epoch, nil
}

func (identity ControllerIdentity) Validate(now time.Time) error {
	if identity.CA == nil || identity.Certificate == nil || len(identity.caKey) != ed25519.PrivateKeySize || len(identity.key) != ed25519.PrivateKeySize {
		return errors.New("incomplete controller identity")
	}
	caPublic, ok := identity.CA.PublicKey.(ed25519.PublicKey)
	if !ok || !caPublic.Equal(identity.caKey.Public()) || !identity.CA.IsCA || identity.CA.CheckSignatureFrom(identity.CA) != nil {
		return errors.New("invalid controller certificate authority")
	}
	leafPublic, ok := identity.Certificate.PublicKey.(ed25519.PublicKey)
	if !ok || !leafPublic.Equal(identity.key.Public()) || !identity.CA.NotAfter.After(now) || !identity.Certificate.NotAfter.After(now) {
		return errors.New("invalid controller leaf identity")
	}
	roots := x509.NewCertPool()
	roots.AddCert(identity.CA)
	if _, err := identity.Certificate.Verify(x509.VerifyOptions{DNSName: ControllerDNSName, Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, CurrentTime: now}); err != nil {
		return fmt.Errorf("invalid controller leaf chain: %w", err)
	}
	return nil
}

func (identity ControllerIdentity) Save(path string) error {
	if err := identity.Validate(time.Now()); err != nil {
		return err
	}
	caKey, err := x509.MarshalPKCS8PrivateKey(identity.caKey)
	if err != nil {
		return err
	}
	key, err := x509.MarshalPKCS8PrivateKey(identity.key)
	if err != nil {
		return err
	}
	contents := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: identity.CA.Raw}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: caKey})...)
	contents = append(contents, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: identity.Certificate.Raw})...)
	contents = append(contents, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})...)
	return atomicPrivateFile(path, contents)
}

func LoadControllerIdentity(path string) (ControllerIdentity, error) {
	if err := requirePrivateRegularFile(path); err != nil {
		return ControllerIdentity{}, err
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return ControllerIdentity{}, err
	}
	var certificates []*x509.Certificate
	var keys []ed25519.PrivateKey
	for len(contents) > 0 {
		block, rest := pem.Decode(contents)
		if block == nil {
			return ControllerIdentity{}, errors.New("invalid controller identity file")
		}
		contents = rest
		switch block.Type {
		case "CERTIFICATE":
			certificate, parseErr := x509.ParseCertificate(block.Bytes)
			if parseErr != nil {
				return ControllerIdentity{}, parseErr
			}
			certificates = append(certificates, certificate)
		case "PRIVATE KEY":
			key, parseErr := x509.ParsePKCS8PrivateKey(block.Bytes)
			if parseErr != nil {
				return ControllerIdentity{}, parseErr
			}
			edKey, ok := key.(ed25519.PrivateKey)
			if !ok {
				return ControllerIdentity{}, errors.New("controller identity key is not Ed25519")
			}
			keys = append(keys, edKey)
		default:
			return ControllerIdentity{}, errors.New("invalid controller identity file")
		}
	}
	if len(certificates) != 2 || len(keys) != 2 {
		return ControllerIdentity{}, errors.New("incomplete controller identity")
	}
	identity := ControllerIdentity{CA: certificates[0], caKey: keys[0], Certificate: certificates[1], key: keys[1]}
	if err := identity.Validate(time.Now()); err != nil {
		return ControllerIdentity{}, err
	}
	return identity, nil
}

func GenerateAndPersistNodeKey(path string, reader io.Reader) (ed25519.PrivateKey, error) {
	_, key, err := ed25519.GenerateKey(readerOrDefault(reader))
	if err != nil {
		return nil, err
	}
	return key, SaveNodePrivateKey(path, key)
}

func SaveNodePrivateKey(path string, key ed25519.PrivateKey) error {
	if len(key) != ed25519.PrivateKeySize {
		return errors.New("invalid node private key")
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	return atomicPrivateFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}))
}

func LoadNodePrivateKey(path string) (ed25519.PrivateKey, error) {
	if err := requirePrivateRegularFile(path); err != nil {
		return nil, err
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(contents)
	if block == nil || len(rest) != 0 || block.Type != "PRIVATE KEY" {
		return nil, errors.New("invalid node private key file")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	edKey, ok := key.(ed25519.PrivateKey)
	if !ok || len(edKey) != ed25519.PrivateKeySize {
		return nil, errors.New("node key is not Ed25519")
	}
	return edKey, nil
}

func atomicPrivateFile(path string, contents []byte) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	if err := requirePrivateDirectory(parent); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return errors.New("private material already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(parent, ".tewake-private-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	// Link creation is an atomic no-clobber publish operation. Unlike Rename, it
	// can never replace an identity/key that a concurrent initializer created.
	if err := os.Link(temporaryPath, path); err != nil {
		return fmt.Errorf("atomically persist private material: %w", err)
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	return nil
}

func requirePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("private material parent is unsafe")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return errors.New("private material parent is not private")
	}
	return nil
}

func requirePrivateRegularFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("private material path is unsafe")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		return errors.New("private material is not private")
	}
	return nil
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	} // ACL/DPAPI protection is owned by twk009.
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
