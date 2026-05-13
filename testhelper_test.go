package main

import (
	"net"
	"testing"
	"time"
)

// waitListening polls addr until a TCP connection succeeds or 5s elapses.
func waitListening(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server never started listening on %s", addr)
}

// dialSend dials addr, sends msg, reads up to maxRead bytes, closes, returns read bytes.
func dialSend(t *testing.T, addr, msg string, maxRead int) []byte {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, maxRead)
	n, _ := c.Read(buf)
	return buf[:n]
}
