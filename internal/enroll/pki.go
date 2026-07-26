package enroll

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
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
	CAKey       ed25519.PrivateKey
	Certificate *x509.Certificate
	Key         ed25519.PrivateKey
}

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
	return ControllerIdentity{CA: ca, CAKey: caPrivate, Certificate: leaf, Key: private}, nil
}

func (identity ControllerIdentity) TLSCertificate() (tlsCertificate tls.Certificate, err error) {
	if identity.Certificate == nil || identity.CA == nil || len(identity.Key) != ed25519.PrivateKeySize {
		return tls.Certificate{}, errors.New("incomplete controller identity")
	}
	return tls.Certificate{Certificate: [][]byte{identity.Certificate.Raw, identity.CA.Raw}, PrivateKey: identity.Key, Leaf: identity.Certificate}, nil
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
	if identity.CA == nil || len(identity.CAKey) != ed25519.PrivateKeySize {
		return nil, nil, errors.New("incomplete controller identity")
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
	template, err := certificateTemplate(now, LeafValidity, reader)
	if err != nil {
		return nil, nil, err
	}
	template.Subject = pkix.Name{CommonName: "tewake-node-" + nodeID}
	template.URIs = []*url.URL{uri}
	template.KeyUsage = x509.KeyUsageDigitalSignature
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	var epochBytes [8]byte
	binary.BigEndian.PutUint64(epochBytes[:], epoch)
	template.ExtraExtensions = []pkix.Extension{{Id: credentialEpochOID, Value: epochBytes[:]}}
	der, err := x509.CreateCertificate(readerOrDefault(reader), template, identity.CA, csr.PublicKey, identity.CAKey)
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
	upperBound := new(big.Int).Lsh(big.NewInt(1), 127)
	return rand.Int(reader, upperBound)
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
	for _, extension := range certificate.Extensions {
		if extension.Id.Equal(credentialEpochOID) {
			if len(extension.Value) != 8 {
				return "", "", 0, errors.New("invalid node credential epoch")
			}
			return nodeID, certificate.SerialNumber.Text(16), binary.BigEndian.Uint64(extension.Value), nil
		}
	}
	return "", "", 0, errors.New("missing node credential epoch")
}

func (identity ControllerIdentity) Save(path string) error {
	if identity.CA == nil || identity.Certificate == nil || len(identity.CAKey) != ed25519.PrivateKeySize || len(identity.Key) != ed25519.PrivateKeySize {
		return errors.New("incomplete controller identity")
	}
	caKey, err := x509.MarshalPKCS8PrivateKey(identity.CAKey)
	if err != nil {
		return err
	}
	key, err := x509.MarshalPKCS8PrivateKey(identity.Key)
	if err != nil {
		return err
	}
	contents := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: identity.CA.Raw}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: caKey})...)
	contents = append(contents, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: identity.Certificate.Raw})...)
	contents = append(contents, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})...)
	return atomicPrivateFile(path, contents)
}

func LoadControllerIdentity(path string) (ControllerIdentity, error) {
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
	if len(certificates) != 2 || len(keys) != 2 || !certificates[0].IsCA {
		return ControllerIdentity{}, errors.New("incomplete controller identity")
	}
	return ControllerIdentity{CA: certificates[0], CAKey: keys[0], Certificate: certificates[1], Key: keys[1]}, nil
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
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".tewake-private-*")
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
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("atomically persist private material: %w", err)
	}
	return nil
}
