package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"golang.org/x/net/http2/hpack"
)

// HTTP/2 connection preface (RFC 7540 §3.5).
const http2Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

// Frame types (RFC 7540 §6).
const (
	frameData         = 0x0
	frameHeaders      = 0x1
	framePriority     = 0x2
	frameRSTStream    = 0x3
	frameSettings     = 0x4
	framePushPromise  = 0x5
	framePing         = 0x6
	frameGoAway       = 0x7
	frameWindowUpdate = 0x8
	frameContinuation = 0x9
)

// Frame flags.
const (
	flagEndStream  = 0x1
	flagAck        = 0x1
	flagEndHeaders = 0x4
	flagPadded     = 0x8
	flagPriority   = 0x20
)

const defaultMaxFrameSize = 16384

// Http2Handler is invoked when a request stream completes (END_STREAM received).
// Headers contain :method/:path/:scheme/:authority pseudo-headers and regular
// headers, in the order sent by the client. Body holds the buffered request
// body (may be empty). The handler must respond via the Http2Stream methods.
type Http2Handler func(stream *Http2Stream, headers []hpack.HeaderField, body []byte)

// http2Writer abstracts the transport sink (a raw Conn for h2c, a net.Conn for h2 TLS).
type http2Writer interface {
	Write([]byte) (int, error)
}

// http2Conn holds per-connection HTTP/2 state.
type http2Conn struct {
	w           http2Writer
	dec         *hpack.Decoder
	enc         *hpack.Encoder
	encBuf      bytes.Buffer
	rxBuf       []byte
	prefaceSeen bool
	handler     Http2Handler
	streams     map[uint32]*Http2Stream

	// in-progress HEADERS block (assembled across CONTINUATION frames)
	headersStream uint32
	headersBlock  []byte
	headersEnd    bool // latched END_STREAM flag for the in-progress block
}

func newHttp2Conn(w http2Writer, h Http2Handler) *http2Conn {
	c := &http2Conn{
		w:       w,
		handler: h,
		streams: make(map[uint32]*Http2Stream),
	}
	c.dec = hpack.NewDecoder(4096, nil)
	c.enc = hpack.NewEncoder(&c.encBuf)
	return c
}

// sendInitialSettings sends the server's connection preface (an empty SETTINGS frame).
// Server defaults are used (no overrides).
func (c *http2Conn) sendInitialSettings() error {
	return c.writeFrame(frameSettings, 0, 0, nil)
}

// writeFrame emits a single frame: 9-byte header + payload.
func (c *http2Conn) writeFrame(typ byte, flags byte, streamID uint32, payload []byte) error {
	n := len(payload)
	if n > 1<<24-1 {
		return errors.New("frame payload too large")
	}
	buf := make([]byte, 9+n)
	buf[0] = byte(n >> 16)
	buf[1] = byte(n >> 8)
	buf[2] = byte(n)
	buf[3] = typ
	buf[4] = flags
	binary.BigEndian.PutUint32(buf[5:9], streamID&0x7fffffff)
	copy(buf[9:], payload)
	_, err := c.w.Write(buf)
	return err
}

// feed pushes received bytes into the parser. Returns a non-nil error on fatal
// protocol violation; the caller should send GOAWAY and close the connection.
func (c *http2Conn) feed(data []byte) error {
	c.rxBuf = append(c.rxBuf, data...)
	if !c.prefaceSeen {
		if len(c.rxBuf) < len(http2Preface) {
			return nil
		}
		if string(c.rxBuf[:len(http2Preface)]) != http2Preface {
			return errors.New("bad HTTP/2 preface")
		}
		c.rxBuf = c.rxBuf[len(http2Preface):]
		c.prefaceSeen = true
	}
	for {
		if len(c.rxBuf) < 9 {
			return nil
		}
		length := int(c.rxBuf[0])<<16 | int(c.rxBuf[1])<<8 | int(c.rxBuf[2])
		if 9+length > len(c.rxBuf) {
			return nil
		}
		typ := c.rxBuf[3]
		flags := c.rxBuf[4]
		streamID := binary.BigEndian.Uint32(c.rxBuf[5:9]) & 0x7fffffff
		payload := c.rxBuf[9 : 9+length]
		if err := c.handleFrame(typ, flags, streamID, payload); err != nil {
			return err
		}
		c.rxBuf = c.rxBuf[9+length:]
	}
}

func (c *http2Conn) handleFrame(typ, flags byte, streamID uint32, payload []byte) error {
	switch typ {
	case frameSettings:
		if flags&flagAck != 0 {
			return nil
		}
		if len(payload)%6 != 0 {
			return errors.New("malformed SETTINGS frame")
		}
		// We accept the client's settings (no enforcement of MAX_FRAME_SIZE etc.).
		return c.writeFrame(frameSettings, flagAck, 0, nil)
	case framePing:
		if flags&flagAck != 0 {
			return nil
		}
		if len(payload) != 8 {
			return errors.New("malformed PING frame")
		}
		return c.writeFrame(framePing, flagAck, 0, payload)
	case frameWindowUpdate:
		// Flow control is not enforced server-side; ignore.
		return nil
	case frameRSTStream:
		delete(c.streams, streamID)
		return nil
	case framePriority:
		return nil
	case frameGoAway:
		return io.EOF
	case frameHeaders:
		return c.onHeaders(flags, streamID, payload)
	case frameContinuation:
		return c.onContinuation(flags, streamID, payload)
	case frameData:
		return c.onData(flags, streamID, payload)
	default:
		// RFC 7540 §4.1: unknown frame types must be ignored.
		return nil
	}
}

func (c *http2Conn) onHeaders(flags byte, streamID uint32, payload []byte) error {
	if streamID == 0 {
		return errors.New("HEADERS frame on stream 0")
	}
	off := 0
	padLen := 0
	if flags&flagPadded != 0 {
		if len(payload) < 1 {
			return errors.New("PADDED HEADERS frame too short")
		}
		padLen = int(payload[0])
		off = 1
	}
	if flags&flagPriority != 0 {
		if len(payload) < off+5 {
			return errors.New("PRIORITY HEADERS frame too short")
		}
		off += 5
	}
	end := len(payload) - padLen
	if end < off {
		return errors.New("invalid HEADERS padding")
	}
	c.headersStream = streamID
	c.headersBlock = append(c.headersBlock[:0], payload[off:end]...)
	c.headersEnd = flags&flagEndStream != 0
	if flags&flagEndHeaders != 0 {
		return c.finishHeaders()
	}
	return nil
}

func (c *http2Conn) onContinuation(flags byte, streamID uint32, payload []byte) error {
	if streamID == 0 || streamID != c.headersStream {
		return errors.New("unexpected CONTINUATION frame")
	}
	c.headersBlock = append(c.headersBlock, payload...)
	if flags&flagEndHeaders != 0 {
		return c.finishHeaders()
	}
	return nil
}

func (c *http2Conn) finishHeaders() error {
	fields, err := c.dec.DecodeFull(c.headersBlock)
	if err != nil {
		return fmt.Errorf("hpack decode: %w", err)
	}
	st := &Http2Stream{conn: c, id: c.headersStream, headers: fields}
	c.streams[c.headersStream] = st
	endStream := c.headersEnd
	c.headersStream = 0
	c.headersBlock = c.headersBlock[:0]
	if endStream {
		c.invoke(st, nil)
	}
	return nil
}

func (c *http2Conn) onData(flags byte, streamID uint32, payload []byte) error {
	st, ok := c.streams[streamID]
	if !ok {
		return nil
	}
	off := 0
	padLen := 0
	if flags&flagPadded != 0 {
		if len(payload) < 1 {
			return errors.New("PADDED DATA frame too short")
		}
		padLen = int(payload[0])
		off = 1
	}
	end := len(payload) - padLen
	if end < off {
		return errors.New("invalid DATA padding")
	}
	st.body = append(st.body, payload[off:end]...)
	if flags&flagEndStream != 0 {
		c.invoke(st, st.body)
	}
	return nil
}

func (c *http2Conn) invoke(st *Http2Stream, body []byte) {
	if c.handler != nil {
		c.handler(st, st.headers, body)
	}
	delete(c.streams, st.id)
}

// Http2Stream represents a single HTTP/2 request/response stream.
type Http2Stream struct {
	conn    *http2Conn
	id      uint32
	headers []hpack.HeaderField
	body    []byte
	sentHdr bool
}

// ID returns the stream identifier.
func (s *Http2Stream) ID() uint32 { return s.id }

// Headers returns the request header fields, including pseudo-headers.
func (s *Http2Stream) Headers() []hpack.HeaderField { return s.headers }

// Method returns the :method pseudo-header value.
func (s *Http2Stream) Method() string { return s.pseudo(":method") }

// Path returns the :path pseudo-header value.
func (s *Http2Stream) Path() string { return s.pseudo(":path") }

// Scheme returns the :scheme pseudo-header value.
func (s *Http2Stream) Scheme() string { return s.pseudo(":scheme") }

// Authority returns the :authority pseudo-header value.
func (s *Http2Stream) Authority() string { return s.pseudo(":authority") }

func (s *Http2Stream) pseudo(name string) string {
	for _, h := range s.headers {
		if h.Name == name {
			return h.Value
		}
	}
	return ""
}

// WriteHeaders sends a HEADERS frame with the given status code and additional
// header fields. END_HEADERS is always set. If endStream is true, END_STREAM is
// also set and no DATA frame should follow.
func (s *Http2Stream) WriteHeaders(status int, extra []hpack.HeaderField, endStream bool) error {
	if s.sentHdr {
		return errors.New("headers already sent")
	}
	s.sentHdr = true
	s.conn.encBuf.Reset()
	if err := s.conn.enc.WriteField(hpack.HeaderField{Name: ":status", Value: strconv.Itoa(status)}); err != nil {
		return err
	}
	for _, h := range extra {
		if err := s.conn.enc.WriteField(h); err != nil {
			return err
		}
	}
	flags := byte(flagEndHeaders)
	if endStream {
		flags |= flagEndStream
	}
	return s.conn.writeFrame(frameHeaders, flags, s.id, s.conn.encBuf.Bytes())
}

// WriteData sends one or more DATA frames carrying body bytes. Set endStream
// on the final call. Payloads larger than the default 16 KiB frame size are
// split across multiple frames.
func (s *Http2Stream) WriteData(data []byte, endStream bool) error {
	for len(data) > defaultMaxFrameSize {
		if err := s.conn.writeFrame(frameData, 0, s.id, data[:defaultMaxFrameSize]); err != nil {
			return err
		}
		data = data[defaultMaxFrameSize:]
	}
	flags := byte(0)
	if endStream {
		flags |= flagEndStream
	}
	return s.conn.writeFrame(frameData, flags, s.id, data)
}

// SimpleResponse sends a status+content-type+body response and closes the stream.
func (s *Http2Stream) SimpleResponse(status int, contentType string, body []byte) error {
	extra := []hpack.HeaderField{
		{Name: "content-type", Value: contentType},
		{Name: "content-length", Value: strconv.Itoa(len(body))},
	}
	if err := s.WriteHeaders(status, extra, len(body) == 0); err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	return s.WriteData(body, true)
}

// hasHTTP2Preface reports whether buf starts with the HTTP/2 connection preface.
func hasHTTP2Preface(buf []byte) bool {
	return bytes.HasPrefix(buf, []byte(http2Preface))
}

// detectH2CUpgrade returns true if buf contains a complete HTTP/1.1 request
// whose headers carry an "Upgrade: h2c" + "HTTP2-Settings" handshake.
func detectH2CUpgrade(buf []byte) bool {
	end := bytes.Index(buf, []byte("\r\n\r\n"))
	if end < 0 {
		return false
	}
	head := strings.ToLower(string(buf[:end]))
	if !strings.Contains(head, "\r\nupgrade: h2c") {
		return false
	}
	if !strings.Contains(head, "\r\nconnection:") {
		return false
	}
	return strings.Contains(head, "\r\nhttp2-settings:")
}

// h2cUpgradeResponse returns the 101 Switching Protocols response sent in
// reply to a successful "Upgrade: h2c" handshake.
func h2cUpgradeResponse() []byte {
	return []byte(
		"HTTP/1.1 101 Switching Protocols\r\n" +
			"Connection: Upgrade\r\n" +
			"Upgrade: h2c\r\n" +
			"\r\n",
	)
}

// synthesizeUpgradedStream installs a stream-1 record built from the original
// HTTP/1.1 request that triggered the h2c upgrade, and invokes the handler.
// Per RFC 7540 §3.2 the upgraded request is implicitly half-closed (remote);
// no further client DATA on stream 1 is expected.
func (c *http2Conn) synthesizeUpgradedStream(method, path, authority string, body []byte) {
	headers := []hpack.HeaderField{
		{Name: ":method", Value: method},
		{Name: ":scheme", Value: "http"},
		{Name: ":path", Value: path},
		{Name: ":authority", Value: authority},
	}
	st := &Http2Stream{conn: c, id: 1, headers: headers, body: body}
	c.streams[1] = st
	c.invoke(st, body)
}
