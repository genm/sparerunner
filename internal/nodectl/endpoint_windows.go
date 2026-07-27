//go:build windows

package nodectl

import (
	"net"
	"path/filepath"
)

// EndpointName keeps the state-directory vocabulary identical across platforms
// for diagnostics, even though Windows must carry the contract over a named
// pipe with an explicit DACL rather than a filesystem socket.
const EndpointName = "control.sock"

func EndpointPath(stateDirectory string) (string, error) {
	if stateDirectory == "" {
		return "", ErrEndpointUnavailable
	}
	absolute, err := filepath.Abs(stateDirectory)
	if err != nil {
		return "", ErrEndpointUnavailable
	}
	return filepath.Join(absolute, EndpointName), nil
}

// Listen fails closed on Windows until the named-pipe endpoint with an explicit
// DACL and client-token authorization exists. A socket without kernel-verified
// peer identity would be a local privilege boundary in name only, so no
// degraded fallback is offered.
func Listen(string) (net.Listener, error) {
	return nil, ErrEndpointUnsupported
}

func dial(string) (net.Conn, error) {
	return nil, ErrEndpointUnsupported
}
