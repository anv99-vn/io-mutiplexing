//go:build linux || darwin

package main

import "syscall"

// Conn wraps a POSIX socket fd.
type Conn struct{ fd int }

// Send writes all bytes to the connection.
func (c *Conn) Send(data []byte) error {
	for off := 0; off < len(data); {
		n, err := syscall.Write(c.fd, data[off:])
		if err != nil {
			return err
		}
		off += n
	}
	return nil
}

// Recv reads data from the connection into buf.
func (c *Conn) Recv(buf []byte) (int, error) {
	return syscall.Read(c.fd, buf)
}

// Disconnect closes the connection.
func (c *Conn) Disconnect() {
	syscall.Close(c.fd)
}
