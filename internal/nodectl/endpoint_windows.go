//go:build windows

package nodectl

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"path/filepath"
	"strings"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

// EndpointName keeps the state-directory vocabulary identical across platforms
// for diagnostics, even though Windows carries the contract over a named pipe
// rather than a file inside that directory.
const EndpointName = "control.sock"

// endpointPipePrefix is the flat kernel namespace every named pipe lives in.
const endpointPipePrefix = `\\.\pipe\SpareRunner-control-`

// endpointClientAccess is FILE_GENERIC_READ|FILE_GENERIC_WRITE. A node owner
// needs exactly that to exchange one request and one response; granting GA
// instead would also hand out WRITE_DAC, letting an authorized owner rewrite the
// boundary that authorized them.
const endpointClientAccess = "GRGW"

// EndpointPath names this installation's endpoint. Windows pipes are not
// filesystem objects, so the state directory cannot contain the endpoint; it
// keys it instead. The digest keeps two installations on one computer, and two
// concurrent tests, from colliding on a single fixed name, and case-insensitive
// normalization keeps the agent and every client deriving the same name from the
// same directory.
func EndpointPath(stateDirectory string) (string, error) {
	if stateDirectory == "" {
		return "", ErrEndpointUnavailable
	}
	absolute, err := filepath.Abs(stateDirectory)
	if err != nil {
		return "", ErrEndpointUnavailable
	}
	digest := sha256.Sum256([]byte(strings.ToLower(filepath.Clean(absolute))))
	return endpointPipePrefix + hex.EncodeToString(digest[:16]), nil
}

// Listen creates the same-host endpoint. The pipe is never a network endpoint:
// go-winio rejects remote clients, and creation fails rather than joining an
// existing pipe, so a squatted name cannot become this agent's control surface.
// The explicit protected DACL is the access boundary; per-connection SID
// authorization in Serve is what refuses and audits a peer that reaches it.
func Listen(stateDirectory string, owners []string) (net.Listener, error) {
	name, err := EndpointPath(stateDirectory)
	if err != nil {
		return nil, err
	}
	descriptor, err := endpointSecurityDescriptor(owners)
	if err != nil {
		return nil, err
	}
	listener, err := winio.ListenPipe(name, &winio.PipeConfig{
		SecurityDescriptor: descriptor,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEndpointUnavailable, err)
	}
	return listener, nil
}

// endpointSecurityDescriptor builds a protected DACL owned by the serving
// account. The serving account gets full control because it owns the endpoint;
// every named node owner gets read/write only. An unparseable owner fails closed
// rather than being skipped, because silently dropping a principal would produce
// a working endpoint that the operator believes is wider than it is.
func endpointSecurityDescriptor(owners []string) (string, error) {
	self, err := ServiceAccountSID()
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "O:%sD:P(A;;GA;;;%s)", self, self)
	granted := map[string]struct{}{strings.ToUpper(self): {}}
	for _, owner := range owners {
		normalized, err := normalizeSID(owner)
		if err != nil {
			return "", err
		}
		if _, duplicate := granted[strings.ToUpper(normalized)]; duplicate {
			continue
		}
		granted[strings.ToUpper(normalized)] = struct{}{}
		fmt.Fprintf(&builder, "(A;;%s;;;%s)", endpointClientAccess, normalized)
	}
	return builder.String(), nil
}

// normalizeSID resolves the operator's string through the OS so a typo cannot
// reach the DACL, and so the stored form matches what PeerIdentity reports.
func normalizeSID(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("%w: empty owner security identifier", ErrEndpointUnavailable)
	}
	sid, err := windows.StringToSid(trimmed)
	if err != nil || sid == nil || !sid.IsValid() {
		return "", fmt.Errorf(
			"%w: %q is not a valid security identifier", ErrEndpointUnavailable, trimmed,
		)
	}
	return sid.String(), nil
}

// ServiceAccountSID reports the identity of the process serving the endpoint. It
// is the one principal that is always authorized, matching the Unix endpoint
// where the service account always reaches its own socket.
func ServiceAccountSID() (string, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil {
		return "", fmt.Errorf("%w: cannot read the serving account identity: %v", ErrEndpointUnavailable, err)
	}
	return user.User.Sid.String(), nil
}

func dial(name string) (net.Conn, error) {
	timeout := RequestTimeout
	connection, err := winio.DialPipe(name, &timeout)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEndpointUnavailable, err)
	}
	return connection, nil
}
