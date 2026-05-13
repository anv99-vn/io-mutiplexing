package main

import (
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTCPCallbackOrder verifies Connect fires before Data, Disconnect fires after Data,
// each exactly once per connection.
func TestTCPCallbackOrder(t *testing.T) {
	const addr = "127.0.0.1:18001"

	var (
		mu    sync.Mutex
		order []string
	)
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}

	go func() {
		NewEngine().
			OnConnect(func(_ *Conn) { record("connect") }).
			OnData(func(conn *Conn, data []byte) {
				record("data")
				conn.Send(data) // echo back so client unblocks
			}).
			OnDisconnect(func(_ *Conn) { record("disconnect") }).
			Listen(":" + strings.Split(addr, ":")[1])
	}()
	waitListening(t, addr)

	dialSend(t, addr, "ping", 4)

	// Disconnect fires synchronously in handleConn after Data returns,
	// but give the goroutine a moment to complete.
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	want := []string{"connect", "data", "disconnect"}
	if len(order) != len(want) {
		t.Fatalf("callback order = %v, want %v", order, want)
	}
	for i, w := range want {
		if order[i] != w {
			t.Errorf("order[%d] = %q, want %q", i, order[i], w)
		}
	}
}

// TestTCPEcho verifies the server echoes raw bytes correctly.
func TestTCPEcho(t *testing.T) {
	const addr = "127.0.0.1:18002"

	go func() {
		NewEngine().
			OnData(func(conn *Conn, data []byte) {
				conn.Send(data)
			}).
			Listen(":" + strings.Split(addr, ":")[1])
	}()
	waitListening(t, addr)

	for _, msg := range []string{"hello", "world", "foo bar baz"} {
		got := dialSend(t, addr, msg, len(msg))
		if string(got) != msg {
			t.Errorf("echo %q: got %q", msg, got)
		}
	}
}

// TestTCPOnlyData verifies the server works when only OnData is registered (no Connect/Disconnect).
func TestTCPOnlyData(t *testing.T) {
	const addr = "127.0.0.1:18003"

	go func() {
		NewEngine().
			OnData(func(conn *Conn, data []byte) {
				conn.Send([]byte("ok"))
			}).
			Listen(":" + strings.Split(addr, ":")[1])
	}()
	waitListening(t, addr)

	got := dialSend(t, addr, "anything", 2)
	if string(got) != "ok" {
		t.Errorf("got %q, want %q", got, "ok")
	}
}

// TestTCPConcurrent verifies multiple simultaneous connections are all handled.
func TestTCPConcurrent(t *testing.T) {
	const (
		addr    = "127.0.0.1:18004"
		clients = 8
	)

	var handled atomic.Int32

	go func() {
		NewEngine().
			OnData(func(conn *Conn, data []byte) {
				handled.Add(1)
				conn.Send([]byte("ok"))
			}).
			Listen(":" + strings.Split(addr, ":")[1])
	}()
	waitListening(t, addr)

	var wg sync.WaitGroup
	errs := make(chan error, clients)

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(3 * time.Second))
			c.Write([]byte("ping"))
			buf := make([]byte, 2)
			c.Read(buf)
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("dial error: %v", err)
	}

	if got := handled.Load(); got != clients {
		t.Errorf("handled %d connections, want %d", got, clients)
	}
}

// TestTCPDisconnectOnEmptyRead verifies Disconnect fires when client closes without sending.
func TestTCPDisconnectOnEmptyRead(t *testing.T) {
	const addr = "127.0.0.1:18005"

	var disconnects atomic.Int32
	var datas atomic.Int32

	go func() {
		NewEngine().
			OnData(func(_ *Conn, _ []byte) { datas.Add(1) }).
			OnDisconnect(func(_ *Conn) { disconnects.Add(1) }).
			Listen(":" + strings.Split(addr, ":")[1])
	}()
	waitListening(t, addr)

	// Connect and immediately close without sending.
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()

	// Give server time to detect the closed connection and fire Disconnect.
	time.Sleep(100 * time.Millisecond)

	if datas.Load() != 0 {
		t.Errorf("Data fired %d times on empty read, want 0", datas.Load())
	}
	if disconnects.Load() != 1 {
		t.Errorf("Disconnect fired %d times, want 1", disconnects.Load())
	}
}
