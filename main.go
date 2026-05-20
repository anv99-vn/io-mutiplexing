package main

import (
	"fmt"
	"log"
	"os"

	"golang.org/x/net/http2/hpack"
)

// Usage:
//
//	io-multiplexing-server [addr]                          # plain TCP, HTTP/1.1 + h2c
//	io-multiplexing-server tls [addr] [certFile] [keyFile] # TLS with ALPN (h2 + http/1.1)
func main() {
	args := os.Args[1:]
	addr := ":8080"
	tlsMode := false
	certFile, keyFile := "", ""

	if len(args) > 0 && args[0] == "tls" {
		tlsMode = true
		if len(args) >= 4 {
			addr = args[1]
			certFile = args[2]
			keyFile = args[3]
		} else if len(args) == 3 {
			certFile = args[1]
			keyFile = args[2]
		} else {
			log.Fatal("tls mode requires: tls [addr] <certFile> <keyFile>")
		}
	} else if len(args) > 0 {
		addr = args[0]
	}

	srv := NewHTTPServer().
		HTTP1(defaultHTTP1Handler).
		HTTP2(defaultHTTP2Handler)

	if tlsMode {
		log.Printf("io-multiplexing HTTPS server (h2 + http/1.1) listening on %s", addr)
		log.Fatal(srv.ListenTLS(addr, certFile, keyFile))
	}
	log.Printf("io-multiplexing HTTP server (h1 + h2c) listening on %s", addr)
	log.Fatal(srv.Listen(addr))
}

func defaultHTTP1Handler(write func([]byte) error, req *HTTPRequest) {
	body := fmt.Sprintf("Hello from io-multiplexing server\nProtocol: HTTP/1.x\nMethod: %s\nPath: %s\n", req.Method, req.Path)
	resp := fmt.Sprintf(
		"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		len(body), body,
	)
	_ = write([]byte(resp))
}

func defaultHTTP2Handler(stream *Http2Stream, _ []hpack.HeaderField, _ []byte) {
	body := fmt.Sprintf("Hello from io-multiplexing server\nProtocol: HTTP/2\nMethod: %s\nPath: %s\n", stream.Method(), stream.Path())
	_ = stream.SimpleResponse(200, "text/plain", []byte(body))
}
