//go:build !linux && !darwin

package nodectl

import "net"

// PeerIdentity fails closed on platforms without a verified peer-credential
// implementation. An endpoint that cannot name its caller must not authorize
// one.
func PeerIdentity(net.Conn) (Peer, error) {
	return Peer{UID: -1, PID: -1}, ErrUnauthorizedPeer
}
