package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// parseRequest: bóc tách dòng request line đầu tiên của HTTP/1.x.
// Định dạng: "METHOD SP PATH SP HTTP/1.1\r\n..."
// Bước 1: tìm "\r\n" -> biết hết dòng đầu.
// Bước 2: split theo dấu cách thành 3 phần: method, path, version.
// Bước 3: trả method + path (bỏ qua version, body, header — server tối giản).
func parseRequest(buf []byte) (method, path string, ok bool) {
	end := bytes.Index(buf, []byte("\r\n"))
	if end < 0 {
		return "", "", false
	}
	parts := bytes.SplitN(buf[:end], []byte(" "), 3)
	if len(parts) < 3 {
		return "", "", false
	}
	return string(parts[0]), string(parts[1]), true
}

// buildResponse: dựng response HTTP/1.1 thuần text.
// Header bắt buộc: Content-Length (để client biết khi nào hết body),
// Connection: close (server đóng socket sau mỗi request, không keep-alive).
func buildResponse(method, path string) []byte {
	body := fmt.Sprintf("Hello from io-multiplexing server\nMethod: %s\nPath: %s\n", method, path)
	return []byte(fmt.Sprintf(
		"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		len(body), body,
	))
}

// newHTTPHandler returns a PacketHandler that serves basic HTTP/1.1 responses.
func newHTTPHandler() PacketHandler {
	return func(conn *Conn, data []byte) {
		method, path, ok := parseRequest(data)
		if !ok {
			conn.Disconnect()
			return
		}
		conn.Send(buildResponse(method, path))
		conn.Disconnect()
	}
}

// parseAddr: chuyển chuỗi "host:port" thành ([4]byte IP, int port).
// Hỗ trợ: ":8080" (bind 0.0.0.0), "127.0.0.1:8080", "1.2.3.4:8080".
// Cần [4]byte vì syscall.SockaddrInet4 yêu cầu field Addr [4]byte.
func parseAddr(addr string) ([4]byte, int, error) {
	var ip [4]byte
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return ip, 0, fmt.Errorf("invalid addr %q", addr)
	}
	port, err := strconv.Atoi(addr[idx+1:])
	if err != nil {
		return ip, 0, fmt.Errorf("invalid port: %w", err)
	}
	host := addr[:idx]
	// host rỗng hoặc 0.0.0.0 -> giữ [0,0,0,0] = INADDR_ANY = bind mọi interface.
	if host == "" || host == "0.0.0.0" {
		return ip, port, nil
	}
	octs := strings.Split(host, ".")
	if len(octs) != 4 {
		return ip, 0, fmt.Errorf("invalid IPv4 %q", host)
	}
	for i, o := range octs {
		v, err := strconv.Atoi(o)
		if err != nil || v < 0 || v > 255 {
			return ip, 0, fmt.Errorf("invalid octet %q", o)
		}
		ip[i] = byte(v)
	}
	return ip, port, nil
}
