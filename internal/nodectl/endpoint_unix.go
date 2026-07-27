//go:build unix

package nodectl

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
)

// EndpointName is the socket file inside the Agent state directory.
const EndpointName = "control.sock"

// maxUnixSocketPath is the platform limit for sun_path. macOS allows 104 bytes
// including the terminator, which is the tighter of the supported platforms.
// Exceeding it fails closed with an actionable error rather than a truncated
// path that would bind somewhere else.
const maxUnixSocketPath = 103

func EndpointPath(stateDirectory string) (string, error) {
	if stateDirectory == "" {
		return "", ErrEndpointUnavailable
	}
	absolute, err := filepath.Abs(stateDirectory)
	if err != nil {
		return "", ErrEndpointUnavailable
	}
	path := filepath.Join(absolute, EndpointName)
	if len(path) > maxUnixSocketPath {
		return "", fmt.Errorf(
			"%w: control socket path %q exceeds the %d byte platform limit; use a shorter agent state directory",
			ErrEndpointUnavailable, path, maxUnixSocketPath,
		)
	}
	return path, nil
}

// Listen creates the same-host endpoint. It is never bound to a network
// address, and the socket is private to the service account: authorization is
// enforced per connection, but the filesystem permission keeps an unrelated
// local user from reaching the endpoint at all.
func Listen(stateDirectory string) (net.Listener, error) {
	path, err := EndpointPath(stateDirectory)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEndpointUnavailable, err)
	}
	// A stale socket from a killed service must not block startup, but only a
	// socket may be removed here: refusing anything else keeps a misconfigured
	// path from deleting real data.
	if info, statErr := os.Lstat(path); statErr == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf(
				"%w: %q exists and is not a control socket", ErrEndpointUnavailable, path,
			)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrEndpointUnavailable, err)
		}
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEndpointUnavailable, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		return nil, fmt.Errorf("%w: %v", ErrEndpointUnavailable, err)
	}
	return listener, nil
}

func dial(path string) (net.Conn, error) {
	connection, err := net.Dial("unix", path)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEndpointUnavailable, err)
	}
	return connection, nil
}
