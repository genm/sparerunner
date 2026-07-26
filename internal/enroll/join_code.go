// Package enroll implements one-time controller enrollment without persisting
// private node key material.
package enroll

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

const (
	JoinCodePrefix    = "twk_"
	JoinProtocolV1    = byte(1)
	joinCodeHeaderLen = 3 + 1 + sha256.Size + 2 + 16 + 32
)

var (
	ErrInvalidJoinCode = errors.New("invalid join code")
	ErrJoinCodeVersion = errors.New("unsupported join code version")
)

// JoinCode contains only data that must be available to the joining node. It
// deliberately has no String or JSON representation so callers cannot
// accidentally include the one-time secret in diagnostics.
type JoinCode struct {
	version       byte
	caFingerprint [sha256.Size]byte
	endpointHints []string
	tokenID       [16]byte
	secret        [32]byte
}

func (code JoinCode) CAFingerprint() [sha256.Size]byte { return code.caFingerprint }
func (code JoinCode) TokenID() [16]byte                { return code.tokenID }
func (code JoinCode) EndpointHints() []string          { return append([]string(nil), code.endpointHints...) }
func (code JoinCode) String() string                   { return "twk_[redacted]" }
func (code JoinCode) GoString() string                 { return code.String() }
func (code JoinCode) LogValue() slog.Value             { return slog.StringValue(code.String()) }

func NewJoinCode(fingerprint [sha256.Size]byte, hints []string, reader io.Reader) (JoinCode, error) {
	if reader == nil {
		reader = rand.Reader
	}
	if zeroBytes(fingerprint[:]) {
		return JoinCode{}, ErrInvalidJoinCode
	}
	canonical, err := CanonicalHints(hints)
	if err != nil {
		return JoinCode{}, err
	}
	code := JoinCode{version: JoinProtocolV1, caFingerprint: fingerprint, endpointHints: canonical}
	if _, err := io.ReadFull(reader, code.tokenID[:]); err != nil {
		return JoinCode{}, err
	}
	if _, err := io.ReadFull(reader, code.secret[:]); err != nil {
		return JoinCode{}, err
	}
	return code, nil
}

func (code JoinCode) Encode() (string, error) {
	if code.version != JoinProtocolV1 {
		return "", ErrJoinCodeVersion
	}
	if zeroBytes(code.caFingerprint[:]) || zeroBytes(code.tokenID[:]) || zeroBytes(code.secret[:]) {
		return "", ErrInvalidJoinCode
	}
	if err := canonicalHints(code.endpointHints); err != nil {
		return "", err
	}
	length := joinCodeHeaderLen
	for _, hint := range code.endpointHints {
		length += len(hint)
	}
	payload := make([]byte, 0, length)
	payload = append(payload, 'T', 'W', 'K', code.version)
	payload = append(payload, code.caFingerprint[:]...)
	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(code.endpointHints)))
	payload = append(payload, count[:]...)
	for _, hint := range code.endpointHints {
		binary.BigEndian.PutUint16(count[:], uint16(len(hint)))
		payload = append(payload, count[:]...)
		payload = append(payload, hint...)
	}
	payload = append(payload, code.tokenID[:]...)
	payload = append(payload, code.secret[:]...)
	return JoinCodePrefix + base64.RawURLEncoding.EncodeToString(payload), nil
}

// DecodeJoinCode accepts exactly one canonical URL-safe base64 representation.
// This rejects padded, standard-base64, reordered-hint, duplicate, and trailing
// representations before a network connection can be attempted.
func DecodeJoinCode(encoded string) (JoinCode, error) {
	if !strings.HasPrefix(encoded, JoinCodePrefix) || len(encoded) == len(JoinCodePrefix) {
		return JoinCode{}, ErrInvalidJoinCode
	}
	body := strings.TrimPrefix(encoded, JoinCodePrefix)
	if strings.ContainsAny(body, "=+/") {
		return JoinCode{}, ErrInvalidJoinCode
	}
	payload, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil || base64.RawURLEncoding.EncodeToString(payload) != body || len(payload) < joinCodeHeaderLen {
		return JoinCode{}, ErrInvalidJoinCode
	}
	if string(payload[:3]) != "TWK" {
		return JoinCode{}, ErrInvalidJoinCode
	}
	if payload[3] != JoinProtocolV1 {
		return JoinCode{}, ErrJoinCodeVersion
	}
	var code JoinCode
	code.version = payload[3]
	copy(code.caFingerprint[:], payload[4:4+sha256.Size])
	offset := 4 + sha256.Size
	count := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2
	code.endpointHints = make([]string, 0, count)
	for range count {
		if offset+2 > len(payload) {
			return JoinCode{}, ErrInvalidJoinCode
		}
		length := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if length == 0 || offset+length > len(payload) {
			return JoinCode{}, ErrInvalidJoinCode
		}
		hint := string(payload[offset : offset+length])
		if !validHint(hint) {
			return JoinCode{}, ErrInvalidJoinCode
		}
		code.endpointHints = append(code.endpointHints, hint)
		offset += length
	}
	if offset+len(code.tokenID)+len(code.secret) != len(payload) {
		return JoinCode{}, ErrInvalidJoinCode
	}
	copy(code.tokenID[:], payload[offset:offset+len(code.tokenID)])
	offset += len(code.tokenID)
	copy(code.secret[:], payload[offset:])
	if zeroBytes(code.caFingerprint[:]) || zeroBytes(code.tokenID[:]) || zeroBytes(code.secret[:]) || canonicalHints(code.endpointHints) != nil {
		return JoinCode{}, ErrInvalidJoinCode
	}
	return code, nil
}

func canonicalHints(hints []string) error {
	if len(hints) > int(^uint16(0)) {
		return ErrInvalidJoinCode
	}
	for index, hint := range hints {
		if !validHint(hint) || (index > 0 && hints[index-1] >= hint) {
			return ErrInvalidJoinCode
		}
	}
	return nil
}

func validHint(hint string) bool {
	if hint == "" || len(hint) > int(^uint16(0)) || strings.TrimSpace(hint) != hint || strings.IndexByte(hint, 0) != -1 {
		return false
	}
	canonical, err := normalizeHint(hint)
	return err == nil && canonical == hint
}

// CanonicalHints is useful to controller adapters before a code is created.
func CanonicalHints(hints []string) ([]string, error) {
	copyHints := make([]string, 0, len(hints))
	for _, hint := range hints {
		canonical, err := normalizeHint(hint)
		if err != nil {
			return nil, ErrInvalidJoinCode
		}
		copyHints = append(copyHints, canonical)
	}
	sort.Strings(copyHints)
	if err := canonicalHints(copyHints); err != nil {
		return nil, err
	}
	return copyHints, nil
}

func normalizeHint(hint string) (string, error) {
	if hint == "" || strings.TrimSpace(hint) != hint || strings.IndexByte(hint, 0) >= 0 {
		return "", ErrInvalidJoinCode
	}
	if strings.HasPrefix(hint, "https://") {
		parsed, err := url.Parse(hint)
		if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return "", ErrInvalidJoinCode
		}
		host, err := normalizeHostPort(parsed.Host, true)
		if err != nil {
			return "", err
		}
		parsed.Host = host
		parsed.Scheme = "https"
		return parsed.String(), nil
	}
	return normalizeHostPort(hint, false)
}

func normalizeHostPort(value string, optionalPort bool) (string, error) {
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		if optionalPort && !strings.Contains(value, ":") {
			host = value
			port = ""
		} else {
			return "", ErrInvalidJoinCode
		}
	}
	if host == "" {
		return "", ErrInvalidJoinCode
	}
	if port != "" {
		parsedPort, parseErr := strconv.ParseUint(port, 10, 16)
		if parseErr != nil || parsedPort == 0 {
			return "", ErrInvalidJoinCode
		}
		port = strconv.FormatUint(parsedPort, 10)
	}
	if address, parseErr := netip.ParseAddr(host); parseErr == nil {
		host = address.String()
	} else {
		host = strings.ToLower(host)
		if strings.ContainsAny(host, " /?#@") {
			return "", ErrInvalidJoinCode
		}
	}
	if port == "" {
		return host, nil
	}
	return net.JoinHostPort(host, port), nil
}

func zeroBytes(value []byte) bool {
	var combined byte
	for _, item := range value {
		combined |= item
	}
	return combined == 0
}

// SecretDigest returns the only secret-derived value allowed across the Registry
// boundary. key is separate controller-held state and must be exactly 32 bytes.
func SecretDigest(key [32]byte, tokenID [16]byte, secret [32]byte) [sha256.Size]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("tewake/enrollment/v1\x00"))
	_, _ = mac.Write(tokenID[:])
	_, _ = mac.Write(secret[:])
	var digest [sha256.Size]byte
	copy(digest[:], mac.Sum(nil))
	return digest
}

func VerifySecretDigest(expected, actual [sha256.Size]byte) bool {
	return hmac.Equal(expected[:], actual[:])
}
