//go:build windows

package nodectl

import (
	"net"

	"golang.org/x/sys/windows"
)

// PeerIdentity reads the connecting process from the pipe itself. The client
// cannot influence it, which is what makes it an authorization input. Windows
// has no Unix user ID, so UID stays -1 and the SID carries the identity.
func PeerIdentity(connection net.Conn) (Peer, error) {
	unknown := Peer{UID: -1, PID: -1}
	pipe, ok := connection.(interface{ Fd() uintptr })
	if !ok {
		return unknown, ErrUnauthorizedPeer
	}
	var processID uint32
	err := windows.GetNamedPipeClientProcessId(windows.Handle(pipe.Fd()), &processID)
	if err != nil || processID == 0 {
		return unknown, ErrUnauthorizedPeer
	}
	// ponytail: the client PID names the token, exactly as the enrollment
	// bootstrap pipe already does, so the repository has one Windows peer-identity
	// idiom instead of two. The known ceiling is PID reuse between connect and
	// this read; the protected pipe DACL, not this lookup, is the access boundary.
	// ImpersonateNamedPipeClient is the upgrade path if that ever stops holding.
	sid, err := processUserSID(processID)
	if err != nil {
		return Peer{UID: -1, PID: int(processID)}, ErrUnauthorizedPeer
	}
	return Peer{UID: -1, PID: int(processID), SID: sid}, nil
}

func processUserSID(processID uint32) (string, error) {
	process, err := windows.OpenProcess(
		windows.PROCESS_QUERY_LIMITED_INFORMATION, false, processID,
	)
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(process)
	var token windows.Token
	if err := windows.OpenProcessToken(process, windows.TOKEN_QUERY, &token); err != nil {
		return "", err
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil {
		return "", err
	}
	if user == nil || user.User.Sid == nil || !user.User.Sid.IsValid() {
		return "", ErrUnauthorizedPeer
	}
	return user.User.Sid.String(), nil
}
