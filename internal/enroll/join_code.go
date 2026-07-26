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
	"sort"
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
	Version       byte
	CAFingerprint [sha256.Size]byte
	EndpointHints []string
	TokenID       [16]byte
	Secret        [32]byte
}

func NewJoinCode(fingerprint [sha256.Size]byte, hints []string, reader io.Reader) (JoinCode, error) {
	if reader == nil {
		reader = rand.Reader
	}
	code := JoinCode{Version: JoinProtocolV1, CAFingerprint: fingerprint, EndpointHints: append([]string(nil), hints...)}
	if err := canonicalHints(code.EndpointHints); err != nil {
		return JoinCode{}, err
	}
	if _, err := io.ReadFull(reader, code.TokenID[:]); err != nil {
		return JoinCode{}, err
	}
	if _, err := io.ReadFull(reader, code.Secret[:]); err != nil {
		return JoinCode{}, err
	}
	return code, nil
}

func (code JoinCode) Encode() (string, error) {
	if code.Version != JoinProtocolV1 {
		return "", ErrJoinCodeVersion
	}
	if err := canonicalHints(code.EndpointHints); err != nil {
		return "", err
	}
	length := joinCodeHeaderLen
	for _, hint := range code.EndpointHints {
		length += len(hint)
	}
	payload := make([]byte, 0, length)
	payload = append(payload, 'T', 'W', 'K', code.Version)
	payload = append(payload, code.CAFingerprint[:]...)
	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(code.EndpointHints)))
	payload = append(payload, count[:]...)
	for _, hint := range code.EndpointHints {
		binary.BigEndian.PutUint16(count[:], uint16(len(hint)))
		payload = append(payload, count[:]...)
		payload = append(payload, hint...)
	}
	payload = append(payload, code.TokenID[:]...)
	payload = append(payload, code.Secret[:]...)
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
	code.Version = payload[3]
	copy(code.CAFingerprint[:], payload[4:4+sha256.Size])
	offset := 4 + sha256.Size
	count := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2
	code.EndpointHints = make([]string, 0, count)
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
		code.EndpointHints = append(code.EndpointHints, hint)
		offset += length
	}
	if offset+len(code.TokenID)+len(code.Secret) != len(payload) {
		return JoinCode{}, ErrInvalidJoinCode
	}
	copy(code.TokenID[:], payload[offset:offset+len(code.TokenID)])
	offset += len(code.TokenID)
	copy(code.Secret[:], payload[offset:])
	if err := canonicalHints(code.EndpointHints); err != nil {
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
	return hint != "" && len(hint) <= int(^uint16(0)) && strings.TrimSpace(hint) == hint && strings.IndexByte(hint, 0) == -1
}

// CanonicalHints is useful to controller adapters before a code is created.
func CanonicalHints(hints []string) ([]string, error) {
	copyHints := append([]string(nil), hints...)
	sort.Strings(copyHints)
	if err := canonicalHints(copyHints); err != nil {
		return nil, err
	}
	return copyHints, nil
}

// SecretDigest returns the only secret-derived value allowed across the Registry
// boundary. key is separate controller-held state and must be exactly 32 bytes.
func SecretDigest(key [32]byte, secret [32]byte) [sha256.Size]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(secret[:])
	var digest [sha256.Size]byte
	copy(digest[:], mac.Sum(nil))
	return digest
}

func VerifySecretDigest(expected, actual [sha256.Size]byte) bool {
	return hmac.Equal(expected[:], actual[:])
}
