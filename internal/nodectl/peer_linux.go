//go:build linux

package nodectl

import (
	"net"

	"golang.org/x/sys/unix"
)

// PeerIdentity reads the connecting process identity from the kernel. The
// client cannot influence it, which is what makes it an authorization input.
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
	var credential *unix.Ucred
	var credentialErr error
	if err := raw.Control(func(fd uintptr) {
		credential, credentialErr = unix.GetsockoptUcred(
			int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED,
		)
	}); err != nil || credentialErr != nil || credential == nil {
		return unknown, ErrUnauthorizedPeer
	}
	return Peer{UID: int(credential.Uid), PID: int(credential.Pid)}, nil
}
