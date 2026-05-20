package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2/hpack"
)

func startHTTPServer(t *testing.T, addr string) {
	t.Helper()
	srv := NewHTTPServer().
		HTTP1(defaultHTTP1Handler).
		HTTP2(defaultHTTP2Handler)
	go func() { _ = srv.Listen(addr) }()
	waitListening(t, "127.0.0.1"+addr)
}

func TestHTTPServer_HTTP1(t *testing.T) {
	const addr = ":17988"
	startHTTPServer(t, addr)

	c, err := net.Dial("tcp", "127.0.0.1"+addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = c.Write([]byte("GET /h1 HTTP/1.1\r\nHost: x\r\n\r\n"))
	body, _ := io.ReadAll(c)
	if !strings.Contains(string(body), "HTTP/1.1 200 OK") {
		t.Fatalf("missing status line: %s", body)
	}
	if !strings.Contains(string(body), "Path: /h1") {
		t.Fatalf("body missing path: %s", body)
	}
	if !strings.Contains(string(body), "Protocol: HTTP/1.x") {
		t.Fatalf("body missing protocol: %s", body)
	}
}

func TestHTTPServer_H2C_PriorKnowledge(t *testing.T) {
	const addr = ":17987"
	startHTTPServer(t, addr)

	c, err := net.Dial("tcp", "127.0.0.1"+addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))

	// Client-side connection preface: magic + empty SETTINGS.
	preface := []byte(http2Preface)
	preface = append(preface, emptySettingsFrame()...)
	// HEADERS frame for stream 1 with END_HEADERS|END_STREAM.
	preface = append(preface, encodeHeadersFrameRaw(t, 1, []hpack.HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":scheme", Value: "http"},
		{Name: ":path", Value: "/h2c"},
		{Name: ":authority", Value: "localhost"},
	})...)
	if _, err := c.Write(preface); err != nil {
		t.Fatal(err)
	}

	frames := readFramesUntilDataEnd(t, c, 1)
	verifyH2Response(t, frames, "/h2c")
}

func TestHTTPServer_H2C_Upgrade(t *testing.T) {
	const addr = ":17986"
	startHTTPServer(t, addr)

	c, err := net.Dial("tcp", "127.0.0.1"+addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))

	req := "GET /upgrade HTTP/1.1\r\n" +
		"Host: localhost\r\n" +
		"Connection: Upgrade, HTTP2-Settings\r\n" +
		"Upgrade: h2c\r\n" +
		"HTTP2-Settings: AAMAAABkAAQAoAAAAAIAAAAA\r\n" +
		"\r\n"
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	// The event-loop backends used by Listen are one-shot per accepted
	// connection: a single recv -> dispatch -> close. The server therefore
	// emits the entire upgrade response (101 + SETTINGS + HEADERS + DATA for
	// stream 1) in one go and closes the socket. The client does not get a
	// chance to send the HTTP/2 connection preface; we only assert that the
	// upgrade response and stream-1 response frames are delivered.
	respBuf := drainAll(t, c, 3*time.Second)

	idx := bytes.Index(respBuf, []byte("\r\n\r\n"))
	if idx < 0 {
		t.Fatalf("no 101 terminator in response: %q", respBuf)
	}
	statusLine := string(respBuf[:bytes.Index(respBuf, []byte("\r\n"))])
	if !strings.HasPrefix(statusLine, "HTTP/1.1 101") {
		t.Fatalf("bad upgrade status line: %q", statusLine)
	}
	framesBuf := respBuf[idx+4:]
	_, frames := scanForDataEnd(framesBuf, 1)
	verifyH2Response(t, frames, "/upgrade")
}

// encodeHeadersFrameRaw is a local copy used by integration tests to avoid
// coupling to the http2_test.go helper (which is in the same package but
// emphasizes clarity per test).
func encodeHeadersFrameRaw(t *testing.T, streamID uint32, fields []hpack.HeaderField) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	for _, f := range fields {
		if err := enc.WriteField(f); err != nil {
			t.Fatal(err)
		}
	}
	payload := buf.Bytes()
	frame := make([]byte, 9+len(payload))
	frame[0] = byte(len(payload) >> 16)
	frame[1] = byte(len(payload) >> 8)
	frame[2] = byte(len(payload))
	frame[3] = frameHeaders
	frame[4] = flagEndHeaders | flagEndStream
	binary.BigEndian.PutUint32(frame[5:9], streamID)
	copy(frame[9:], payload)
	return frame
}

// readFramesUntilDataEnd reads frames from c until it sees a DATA frame on
// streamID with END_STREAM, then returns all frames collected.
func readFramesUntilDataEnd(t *testing.T, c net.Conn, streamID uint32) []parsedFrame {
	t.Helper()
	var acc []byte
	buf := make([]byte, 4096)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, err := c.Read(buf)
		if n > 0 {
			acc = append(acc, buf[:n]...)
			if done, frames := scanForDataEnd(acc, streamID); done {
				return frames
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
		}
	}
	t.Fatalf("never received DATA END_STREAM on stream %d; got %d bytes", streamID, len(acc))
	return nil
}

func scanForDataEnd(buf []byte, streamID uint32) (bool, []parsedFrame) {
	var out []parsedFrame
	for len(buf) >= 9 {
		length := int(buf[0])<<16 | int(buf[1])<<8 | int(buf[2])
		if 9+length > len(buf) {
			return false, out
		}
		typ := buf[3]
		flags := buf[4]
		sid := binary.BigEndian.Uint32(buf[5:9]) & 0x7fffffff
		payload := append([]byte(nil), buf[9:9+length]...)
		out = append(out, parsedFrame{typ: typ, flags: flags, streamID: sid, payload: payload})
		buf = buf[9+length:]
		if typ == frameData && sid == streamID && flags&flagEndStream != 0 {
			return true, out
		}
	}
	return false, out
}

func drainAll(t *testing.T, c net.Conn, total time.Duration) []byte {
	t.Helper()
	var acc []byte
	buf := make([]byte, 4096)
	deadline := time.Now().Add(total)
	for time.Now().Before(deadline) {
		c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		n, err := c.Read(buf)
		if n > 0 {
			acc = append(acc, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil && n == 0 {
			// likely deadline; loop once more if outer deadline not passed
		}
	}
	return acc
}

func readUntil(t *testing.T, c net.Conn, needle []byte, limit int) bool {
	t.Helper()
	var acc []byte
	buf := make([]byte, 512)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(acc) < limit {
		c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, _ := c.Read(buf)
		if n > 0 {
			acc = append(acc, buf[:n]...)
			if bytes.Contains(acc, needle) {
				return true
			}
		}
	}
	return false
}

func verifyH2Response(t *testing.T, frames []parsedFrame, wantPath string) {
	t.Helper()
	var sawHeaders bool
	var dataParts []byte
	for _, f := range frames {
		if f.typ == frameHeaders && f.streamID == 1 {
			sawHeaders = true
			dec := hpack.NewDecoder(4096, nil)
			fields, err := dec.DecodeFull(f.payload)
			if err != nil {
				t.Fatalf("hpack decode: %v", err)
			}
			var status string
			for _, h := range fields {
				if h.Name == ":status" {
					status = h.Value
				}
			}
			if status != "200" {
				t.Fatalf("status=%q want 200", status)
			}
		}
		if f.typ == frameData && f.streamID == 1 {
			dataParts = append(dataParts, f.payload...)
		}
	}
	if !sawHeaders {
		t.Fatal("never saw HEADERS frame on stream 1")
	}
	body := string(dataParts)
	if !strings.Contains(body, wantPath) {
		t.Fatalf("body missing path %q: %s", wantPath, body)
	}
	if !strings.Contains(body, "Protocol: HTTP/2") {
		t.Fatalf("body missing protocol marker: %s", body)
	}
}
