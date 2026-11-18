//go:build !darwin && !linux

package nativeapi

import (
	"errors"
	"net"
)

func verifyUnixPeerUID(*net.UnixConn, uint32) error {
	return errors.New("unix socket peer credential verification is unsupported on this platform")
}
