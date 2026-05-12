package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseRequest(t *testing.T) {
	cases := []struct {
		name       string
		buf        string
		wantMethod string
		wantPath   string
		wantOK     bool
	}{
		{"get", "GET /hello HTTP/1.1\r\n\r\n", "GET", "/hello", true},
		{"post", "POST /api/x HTTP/1.1\r\nContent-Length: 0\r\n\r\n", "POST", "/api/x", true},
		{"empty", "", "", "", false},
		{"no_crlf", "GET /hello HTTP/1.1", "", "", false},
		{"missing_parts", "GET\r\n", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, p, ok := parseRequest([]byte(c.buf))
			if m != c.wantMethod || p != c.wantPath || ok != c.wantOK {
				t.Errorf("got (%q, %q, %v), want (%q, %q, %v)",
					m, p, ok, c.wantMethod, c.wantPath, c.wantOK)
			}
		})
	}
}

func TestBuildResponse(t *testing.T) {
	resp := buildResponse("GET", "/test")
	if !bytes.HasPrefix(resp, []byte("HTTP/1.1 200 OK\r\n")) {
		t.Errorf("missing status line: %s", resp)
	}
	if !bytes.Contains(resp, []byte("Content-Length:")) {
		t.Errorf("missing content-length")
	}
	if !strings.Contains(string(resp), "Method: GET") {
		t.Errorf("missing method line")
	}
	if !strings.Contains(string(resp), "Path: /test") {
		t.Errorf("missing path line")
	}
}

func TestParseAddr(t *testing.T) {
	cases := []struct {
		in       string
		wantIP   [4]byte
		wantPort int
		wantErr  bool
	}{
		{":8080", [4]byte{0, 0, 0, 0}, 8080, false},
		{"0.0.0.0:80", [4]byte{0, 0, 0, 0}, 80, false},
		{"127.0.0.1:9999", [4]byte{127, 0, 0, 1}, 9999, false},
		{"1.2.3.4:1234", [4]byte{1, 2, 3, 4}, 1234, false},
		{"bad", [4]byte{}, 0, true},
		{":notnum", [4]byte{}, 0, true},
		{"1.2.3:80", [4]byte{}, 0, true},
		{"1.2.3.4.5:80", [4]byte{}, 0, true},
		{"300.0.0.0:80", [4]byte{}, 0, true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			ip, p, err := parseAddr(c.in)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if err != nil {
				return
			}
			if ip != c.wantIP || p != c.wantPort {
				t.Errorf("got (%v, %d), want (%v, %d)", ip, p, c.wantIP, c.wantPort)
			}
		})
	}
}
