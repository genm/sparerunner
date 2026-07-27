//go:build darwin

package nodectl

import (
	"net"

	"golang.org/x/sys/unix"
)

// PeerIdentity reads the connecting process identity from the kernel. macOS
// LOCAL_PEERCRED reports the peer's effective user without a PID, so the PID
// stays unknown rather than being guessed from the client.
func PeerIdentity(connection net.Conn) (Peer, error) {
	unknown := Peer{UID: -1, PID: -1}
	unixConn, ok := connection.(*net.UnixConn)
	if !ok {
		return unknown, ErrUnauthorizedPeer
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return unknown, ErrUnauthorizedPeer
	}
	var credential *unix.Xucred
	var credentialErr error
	if err := raw.Control(func(fd uintptr) {
		credential, credentialErr = unix.GetsockoptXucred(
			int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED,
		)
	}); err != nil || credentialErr != nil || credential == nil {
		return unknown, ErrUnauthorizedPeer
	}
	return Peer{UID: int(credential.Uid), PID: -1}, nil
}
