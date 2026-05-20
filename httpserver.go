package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
)

// HTTPRequest is the protocol-agnostic representation of an inbound request
// passed to an HTTP/1 handler. (HTTP/2 handlers receive *Http2Stream directly.)
type HTTPRequest struct {
	Method  string
	Path    string
	Version string // "HTTP/1.0" or "HTTP/1.1"
	Headers []HTTPHeader
	Body    []byte
}

// HTTPHeader is a single name/value header pair preserving wire order.
type HTTPHeader struct{ Name, Value string }

// HTTP1Handler is invoked for each parsed HTTP/1.x request. The handler writes
// the response by calling write; the connection is closed after the handler
// returns.
type HTTP1Handler func(write func([]byte) error, req *HTTPRequest)

// HTTPServer accepts connections and dispatches each to the appropriate
// protocol handler. On plain TCP it serves HTTP/1.1 plus h2c (both
// prior-knowledge preface and Upgrade: h2c). On TLS it negotiates HTTP/2 vs
// HTTP/1.1 via ALPN.
type HTTPServer struct {
	h1 HTTP1Handler
	h2 Http2Handler
}

// NewHTTPServer returns an HTTPServer with no handlers installed.
func NewHTTPServer() *HTTPServer { return &HTTPServer{} }

// HTTP1 registers the HTTP/1.x handler.
func (s *HTTPServer) HTTP1(h HTTP1Handler) *HTTPServer { s.h1 = h; return s }

// HTTP2 registers the HTTP/2 handler used for both h2c and ALPN-negotiated h2.
func (s *HTTPServer) HTTP2(h Http2Handler) *HTTPServer { s.h2 = h; return s }

// Listen serves plain TCP on addr. HTTP/1.1 and h2c are both supported.
//
// State is keyed by Conn.Key (the underlying socket handle/fd), not by *Conn
// pointer, because backends like Windows IOCP create a fresh *Conn wrapper per
// callback even when the underlying socket is the same.
func (s *HTTPServer) Listen(addr string) error {
	states := &sync.Map{} // Conn.Key() -> *httpConnState
	engine := NewEngine().
		OnConnect(func(c *Conn) {
			states.Store(c.Key(), &httpConnState{})
		}).
		OnDisconnect(func(c *Conn) {
			states.Delete(c.Key())
		}).
		OnData(func(c *Conn, data []byte) {
			v, ok := states.Load(c.Key())
			if !ok {
				// Backends that do not invoke OnConnect (or do so on a
				// different goroutine) may deliver Data first; lazily create.
				st := &httpConnState{}
				actual, _ := states.LoadOrStore(c.Key(), st)
				v = actual
			}
			s.dispatchPlain(c, v.(*httpConnState), data)
		})
	return engine.Listen(addr)
}

// ListenTLS serves on addr using TLS with ALPN negotiation between "h2" and
// "http/1.1". Each accepted connection runs on its own goroutine; the event
// loop used by Listen is bypassed because TLS handshakes are blocking.
func (s *HTTPServer) ListenTLS(addr, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("load cert: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h2", "http/1.1"},
		MinVersion:   tls.VersionTLS12,
	}
	return s.ListenTLSConfig(addr, cfg)
}

// ListenTLSConfig is like ListenTLS but accepts a fully built *tls.Config.
// The caller is responsible for setting NextProtos appropriately
// ("h2"/"http/1.1") if HTTP/2 ALPN is desired. The config is used as-is.
func (s *HTTPServer) ListenTLSConfig(addr string, cfg *tls.Config) error {
	if cfg == nil {
		return errors.New("nil tls.Config")
	}
	if len(cfg.NextProtos) == 0 {
		cfg.NextProtos = []string{"h2", "http/1.1"}
	}
	ln, err := tls.Listen("tcp", addr, cfg)
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleTLSConn(c)
	}
}

func (s *HTTPServer) handleTLSConn(c net.Conn) {
	defer c.Close()
	tc, ok := c.(*tls.Conn)
	if !ok {
		return
	}
	if err := tc.Handshake(); err != nil {
		return
	}
	proto := tc.ConnectionState().NegotiatedProtocol
	if proto == "h2" {
		s.serveH2OverConn(c)
	} else {
		s.serveH1OverConn(c)
	}
}

// httpConnState tracks the per-connection protocol state on the event-loop path.
type httpConnState struct {
	mode    httpMode
	rxBuf   []byte
	h2      *http2Conn
	closing bool
}

type httpMode uint8

const (
	modeUnknown httpMode = iota
	modeHTTP1
	modeHTTP2
)

// dispatchPlain inspects bytes from a plain TCP connection and routes them to
// the right protocol handler. Once a mode is decided it never changes.
func (s *HTTPServer) dispatchPlain(c *Conn, st *httpConnState, data []byte) {
	if st.closing {
		return
	}
	switch st.mode {
	case modeHTTP2:
		if err := st.h2.feed(data); err != nil {
			s.closeConn(c, st)
		}
		return
	case modeHTTP1:
		st.rxBuf = append(st.rxBuf, data...)
		s.serveHTTP1Plain(c, st)
		return
	}

	st.rxBuf = append(st.rxBuf, data...)

	if hasHTTP2Preface(st.rxBuf) {
		st.mode = modeHTTP2
		w := plainConnWriter{c: c}
		st.h2 = newHttp2Conn(w, s.h2)
		buf := st.rxBuf
		st.rxBuf = nil
		if err := st.h2.feed(buf); err != nil {
			s.closeConn(c, st)
		}
		return
	}

	headerEnd := bytes.Index(st.rxBuf, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		// not enough bytes yet to decide
		if len(st.rxBuf) > 64*1024 {
			s.closeConn(c, st)
		}
		return
	}

	if detectH2CUpgrade(st.rxBuf[:headerEnd+4]) {
		s.beginH2CUpgrade(c, st, headerEnd+4)
		return
	}

	st.mode = modeHTTP1
	s.serveHTTP1Plain(c, st)
}

// beginH2CUpgrade reads the original HTTP/1.1 request, sends the 101 Switching
// Protocols response and the server's SETTINGS preface, then synthesizes a
// stream-1 record from the upgraded request.
func (s *HTTPServer) beginH2CUpgrade(c *Conn, st *httpConnState, hdrEnd int) {
	req, ok := parseHTTP1Request(st.rxBuf[:hdrEnd])
	if !ok {
		s.closeConn(c, st)
		return
	}
	if err := c.Send(h2cUpgradeResponse()); err != nil {
		s.closeConn(c, st)
		return
	}
	w := plainConnWriter{c: c}
	st.h2 = newHttp2Conn(w, s.h2)
	st.mode = modeHTTP2
	if err := st.h2.sendInitialSettings(); err != nil {
		s.closeConn(c, st)
		return
	}
	authority := headerValue(req.Headers, "Host")
	st.h2.synthesizeUpgradedStream(req.Method, req.Path, authority, nil)
	leftover := st.rxBuf[hdrEnd:]
	st.rxBuf = nil
	if len(leftover) > 0 {
		if err := st.h2.feed(leftover); err != nil {
			s.closeConn(c, st)
		}
	}
}

// serveHTTP1Plain drives the HTTP/1 handler for buffered bytes on a plain
// connection. The connection is closed after the handler runs (Connection:
// close semantics — keep-alive is not implemented here).
func (s *HTTPServer) serveHTTP1Plain(c *Conn, st *httpConnState) {
	headerEnd := bytes.Index(st.rxBuf, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		return
	}
	req, ok := parseHTTP1Request(st.rxBuf[:headerEnd])
	if !ok {
		s.closeConn(c, st)
		return
	}
	bodyStart := headerEnd + 4
	if cl := headerValue(req.Headers, "Content-Length"); cl != "" {
		n, err := parsePositiveInt(cl)
		if err != nil {
			s.closeConn(c, st)
			return
		}
		if len(st.rxBuf) < bodyStart+n {
			return
		}
		req.Body = st.rxBuf[bodyStart : bodyStart+n]
	}
	if s.h1 != nil {
		s.h1(func(b []byte) error { return c.Send(b) }, req)
	}
	s.closeConn(c, st)
}

func (s *HTTPServer) closeConn(c *Conn, st *httpConnState) {
	if st.closing {
		return
	}
	st.closing = true
	c.Disconnect()
}

// plainConnWriter adapts a raw *Conn to the http2Writer interface.
type plainConnWriter struct{ c *Conn }

func (w plainConnWriter) Write(p []byte) (int, error) {
	if err := w.c.Send(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// netConnWriter adapts a net.Conn to the http2Writer interface.
type netConnWriter struct{ c net.Conn }

func (w netConnWriter) Write(p []byte) (int, error) { return w.c.Write(p) }

// serveH2OverConn drives the HTTP/2 frame loop over a TLS net.Conn.
func (s *HTTPServer) serveH2OverConn(c net.Conn) {
	h2 := newHttp2Conn(netConnWriter{c: c}, s.h2)
	if err := h2.sendInitialSettings(); err != nil {
		return
	}
	buf := make([]byte, 16*1024)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			if ferr := h2.feed(buf[:n]); ferr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// serveH1OverConn drives the HTTP/1.x handler over a TLS net.Conn.
func (s *HTTPServer) serveH1OverConn(c net.Conn) {
	buf := make([]byte, 0, 8192)
	tmp := make([]byte, 4096)
	for {
		n, err := c.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			headerEnd := bytes.Index(buf, []byte("\r\n\r\n"))
			if headerEnd >= 0 {
				req, ok := parseHTTP1Request(buf[:headerEnd])
				if !ok {
					return
				}
				bodyStart := headerEnd + 4
				if cl := headerValue(req.Headers, "Content-Length"); cl != "" {
					want, perr := parsePositiveInt(cl)
					if perr != nil {
						return
					}
					if len(buf) < bodyStart+want {
						continue
					}
					req.Body = buf[bodyStart : bodyStart+want]
				}
				if s.h1 != nil {
					s.h1(func(b []byte) error { _, werr := c.Write(b); return werr }, req)
				}
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// parseHTTP1Request parses a raw HTTP/1.x request head (no body) into method,
// path, version, and headers. Input must end just before the CRLF CRLF
// terminator.
func parseHTTP1Request(raw []byte) (*HTTPRequest, bool) {
	lineEnd := bytes.Index(raw, []byte("\r\n"))
	if lineEnd < 0 {
		return nil, false
	}
	parts := bytes.SplitN(raw[:lineEnd], []byte(" "), 3)
	if len(parts) < 3 {
		return nil, false
	}
	req := &HTTPRequest{
		Method:  string(parts[0]),
		Path:    string(parts[1]),
		Version: string(parts[2]),
	}
	rest := raw[lineEnd+2:]
	for len(rest) > 0 {
		end := bytes.Index(rest, []byte("\r\n"))
		if end < 0 {
			end = len(rest)
		}
		line := rest[:end]
		if len(line) == 0 {
			break
		}
		colon := bytes.IndexByte(line, ':')
		if colon < 0 {
			return nil, false
		}
		name := strings.TrimSpace(string(line[:colon]))
		value := strings.TrimSpace(string(line[colon+1:]))
		req.Headers = append(req.Headers, HTTPHeader{Name: name, Value: value})
		if end == len(rest) {
			break
		}
		rest = rest[end+2:]
	}
	return req, true
}

// headerValue returns the value of the first header matching name (case-insensitive).
func headerValue(h []HTTPHeader, name string) string {
	for _, kv := range h {
		if strings.EqualFold(kv.Name, name) {
			return kv.Value
		}
	}
	return ""
}

// parsePositiveInt parses a non-negative ASCII decimal integer.
func parsePositiveInt(s string) (int, error) {
	if s == "" {
		return 0, errors.New("empty number")
	}
	n := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch < '0' || ch > '9' {
			return 0, errors.New("non-digit")
		}
		n = n*10 + int(ch-'0')
		if n > 1<<30 {
			return 0, errors.New("too large")
		}
	}
	return n, nil
}

