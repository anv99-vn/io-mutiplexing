package main

import (
	"bytes"
	"encoding/binary"
	"testing"

	"golang.org/x/net/http2/hpack"
)

// captureWriter records every Write call into a buffer so the test can inspect
// frames the server sent.
type captureWriter struct{ bytes.Buffer }

func (w *captureWriter) Write(p []byte) (int, error) { return w.Buffer.Write(p) }

func TestHasHTTP2Preface(t *testing.T) {
	if !hasHTTP2Preface([]byte(http2Preface)) {
		t.Fatal("expected preface match")
	}
	if hasHTTP2Preface([]byte("GET / HTTP/1.1\r\n")) {
		t.Fatal("HTTP/1 must not match")
	}
	if hasHTTP2Preface([]byte("PRI * HTTP/2")) {
		t.Fatal("partial preface must not match")
	}
}

func TestDetectH2CUpgrade(t *testing.T) {
	req := "GET / HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"Connection: Upgrade, HTTP2-Settings\r\n" +
		"Upgrade: h2c\r\n" +
		"HTTP2-Settings: AAMAAABkAAQAoAAAAAIAAAAA\r\n" +
		"\r\n"
	if !detectH2CUpgrade([]byte(req)) {
		t.Fatal("expected upgrade detected")
	}
	plain := "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	if detectH2CUpgrade([]byte(plain)) {
		t.Fatal("plain HTTP/1.1 must not be detected as upgrade")
	}
}

func TestParseHTTP1Request(t *testing.T) {
	raw := "POST /api HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n"
	req, ok := parseHTTP1Request([]byte(raw))
	if !ok {
		t.Fatal("parse failed")
	}
	if req.Method != "POST" || req.Path != "/api" || req.Version != "HTTP/1.1" {
		t.Fatalf("bad request line: %+v", req)
	}
	if v := headerValue(req.Headers, "content-length"); v != "5" {
		t.Fatalf("Content-Length got %q", v)
	}
	if v := headerValue(req.Headers, "Host"); v != "x" {
		t.Fatalf("Host got %q", v)
	}
}

func TestParsePositiveInt(t *testing.T) {
	cases := []struct {
		in     string
		want   int
		errStr bool
	}{
		{"0", 0, false},
		{"42", 42, false},
		{"", 0, true},
		{"abc", 0, true},
		{"-5", 0, true},
	}
	for _, c := range cases {
		got, err := parsePositiveInt(c.in)
		if (err != nil) != c.errStr {
			t.Fatalf("%q err=%v wantErr=%v", c.in, err, c.errStr)
		}
		if err == nil && got != c.want {
			t.Fatalf("%q got %d want %d", c.in, got, c.want)
		}
	}
}

// encodeHeadersFrame builds a single HEADERS frame with END_HEADERS+END_STREAM
// carrying the given header fields hpack-encoded with a fresh encoder.
func encodeHeadersFrame(t *testing.T, streamID uint32, fields []hpack.HeaderField) []byte {
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

// emptySettingsFrame returns a 9-byte SETTINGS frame with empty payload (the
// client-side preface trailing data).
func emptySettingsFrame() []byte {
	return []byte{0, 0, 0, frameSettings, 0, 0, 0, 0, 0}
}

func TestHttp2RoundTrip(t *testing.T) {
	w := &captureWriter{}
	var gotMethod, gotPath string
	h := func(s *Http2Stream, _ []hpack.HeaderField, _ []byte) {
		gotMethod = s.Method()
		gotPath = s.Path()
		if err := s.SimpleResponse(200, "text/plain", []byte("hi")); err != nil {
			t.Fatal(err)
		}
	}
	conn := newHttp2Conn(w, h)

	// Client preface + empty SETTINGS + HEADERS(stream 1, END_STREAM).
	stream := []byte(http2Preface)
	stream = append(stream, emptySettingsFrame()...)
	stream = append(stream, encodeHeadersFrame(t, 1, []hpack.HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":scheme", Value: "http"},
		{Name: ":path", Value: "/hello"},
		{Name: ":authority", Value: "localhost"},
	})...)

	if err := conn.feed(stream); err != nil {
		t.Fatalf("feed: %v", err)
	}
	if gotMethod != "GET" || gotPath != "/hello" {
		t.Fatalf("handler saw method=%q path=%q", gotMethod, gotPath)
	}

	// Parse what the server wrote: expect SETTINGS ack (from client SETTINGS)
	// then HEADERS + DATA frames for stream 1. Order: depending on order of
	// frames we sent (we never called sendInitialSettings here so server only
	// emits the SETTINGS ack + the response frames).
	frames := parseFrames(t, w.Bytes())
	var sawHeaders, sawData bool
	for _, f := range frames {
		if f.typ == frameHeaders && f.streamID == 1 {
			sawHeaders = true
		}
		if f.typ == frameData && f.streamID == 1 && string(f.payload) == "hi" {
			sawData = true
		}
	}
	if !sawHeaders {
		t.Error("missing HEADERS response frame")
	}
	if !sawData {
		t.Error("missing DATA response frame with body 'hi'")
	}
}

type parsedFrame struct {
	typ      byte
	flags    byte
	streamID uint32
	payload  []byte
}

func parseFrames(t *testing.T, buf []byte) []parsedFrame {
	t.Helper()
	var out []parsedFrame
	for len(buf) >= 9 {
		length := int(buf[0])<<16 | int(buf[1])<<8 | int(buf[2])
		if 9+length > len(buf) {
			t.Fatalf("truncated frame in capture, want %d have %d", 9+length, len(buf))
		}
		out = append(out, parsedFrame{
			typ:      buf[3],
			flags:    buf[4],
			streamID: binary.BigEndian.Uint32(buf[5:9]) & 0x7fffffff,
			payload:  append([]byte(nil), buf[9:9+length]...),
		})
		buf = buf[9+length:]
	}
	return out
}

func TestHttp2BadPreface(t *testing.T) {
	w := &captureWriter{}
	conn := newHttp2Conn(w, nil)
	if err := conn.feed([]byte("GET / HTTP/1.1\r\n\r\n              ")); err == nil {
		t.Fatal("expected error on bad preface")
	}
}
