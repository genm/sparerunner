package transport

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/genm/tewake/internal/enroll"
)

// EnrollmentBodyLimit is derived from the transport's established 1 MiB GitHub
// adapter payload bound: a request may carry that payload encoded as raw Base64
// plus the fixed enrollment object metadata. It is a resource boundary, not a
// node or product quota.
var EnrollmentBodyLimit = int64(base64.RawStdEncoding.EncodedLen(int(GitHubAdapterResponseLimit))) + 4096

// EnrollmentHandler accepts the one unauthenticated operation on the controller
// listener. Every rejection is deliberately secret-agnostic.
func EnrollmentHandler(service enroll.Service) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/enroll" || request.Header.Get("Content-Type") != "application/json" {
			http.Error(writer, "invalid enrollment request", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(io.LimitReader(request.Body, EnrollmentBodyLimit+1))
		if err != nil || int64(len(body)) > EnrollmentBodyLimit {
			http.Error(writer, "invalid enrollment request", http.StatusBadRequest)
			return
		}
		values, err := strictObject(body, []string{"joinCode", "csr"})
		if err != nil {
			http.Error(writer, "invalid enrollment request", http.StatusBadRequest)
			return
		}
		var encodedCode, encodedCSR string
		if json.Unmarshal(values["joinCode"], &encodedCode) != nil || json.Unmarshal(values["csr"], &encodedCSR) != nil {
			http.Error(writer, "invalid enrollment request", http.StatusBadRequest)
			return
		}
		csr, err := decodeRawBase64(encodedCSR)
		if err != nil {
			http.Error(writer, "invalid enrollment request", http.StatusBadRequest)
			return
		}
		result, err := service.Enroll(request.Context(), encodedCode, csr)
		if err != nil {
			http.Error(writer, "enrollment rejected", http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(struct {
			NodeID      string `json:"nodeId"`
			Certificate string `json:"certificate"`
			CA          string `json:"ca"`
		}{NodeID: result.NodeID, Certificate: base64.RawStdEncoding.EncodeToString(result.CertificateDER), CA: base64.RawStdEncoding.EncodeToString(result.CACertificateDER)})
	})
}

func strictObject(payload []byte, expected []string) (map[string]json.RawMessage, error) {
	if err := rejectDuplicateJSONKeys(payload); err != nil {
		return nil, err
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(payload, &values); err != nil || len(values) != len(expected) {
		return nil, errors.New("invalid JSON object")
	}
	for _, key := range expected {
		if _, found := values[key]; !found {
			return nil, errors.New("missing JSON member")
		}
	}
	return values, nil
}

func decodeRawBase64(value string) ([]byte, error) {
	decoded, err := base64.RawStdEncoding.DecodeString(value)
	if err != nil || len(decoded) == 0 || base64.RawStdEncoding.EncodeToString(decoded) != value {
		return nil, errors.New("invalid Base64")
	}
	return decoded, nil
}
