package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestServerIntegration: spawn Run() in goroutine, send HTTP request, verify response.
// Same test code on all 3 OS — Run() is build-tag selected (epoll/kqueue/IOCP).
func TestServerIntegration(t *testing.T) {
	const addr = ":17999"
	const url = "http://127.0.0.1" + addr + "/integration"

	go func() {
		_ = NewEngine().OnData(newHTTPHandler()).Listen(addr)
	}()

	waitListening(t, "127.0.0.1"+addr)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Path: /integration") {
		t.Fatalf("body missing path: %s", body)
	}
	if !strings.Contains(string(body), "Method: GET") {
		t.Fatalf("body missing method: %s", body)
	}
}
