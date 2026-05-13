//go:build windows

package main

import "syscall"

// Conn wraps a Windows socket handle.
type Conn struct{ sock syscall.Handle }

// Send writes all bytes synchronously to the connection.
func (c *Conn) Send(data []byte) error {
	for off := 0; off < len(data); {
		buf := syscall.WSABuf{Len: uint32(len(data[off:])), Buf: &data[off]}
		var sent uint32
		if err := syscall.WSASend(c.sock, &buf, 1, &sent, 0, nil, nil); err != nil {
			return err
		}
		off += int(sent)
	}
	return nil
}

// Recv reads data synchronously from the connection into buf.
func (c *Conn) Recv(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	wsabuf := syscall.WSABuf{Len: uint32(len(buf)), Buf: &buf[0]}
	var received, flags uint32
	err := syscall.WSARecv(c.sock, &wsabuf, 1, &received, &flags, nil, nil)
	return int(received), err
}

// Disconnect closes the connection.
func (c *Conn) Disconnect() {
	syscall.Closesocket(c.sock)
}
