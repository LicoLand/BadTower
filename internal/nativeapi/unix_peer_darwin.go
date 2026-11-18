//go:build darwin

package nativeapi

import (
	"errors"
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func verifyUnixPeerUID(conn *net.UnixConn, expectedUID uint32) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var peerUID uint32
	var sockErr error
	if err := rawConn.Control(func(fd uintptr) {
		cred, err := unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if err != nil {
			sockErr = err
			return
		}
		peerUID = cred.Uid
	}); err != nil {
		return err
	}
	if sockErr != nil {
		return fmt.Errorf("verify unix peer credentials: %w", sockErr)
	}
	if peerUID != expectedUID {
		return errors.New("unix socket peer uid mismatch")
	}
	return nil
}
