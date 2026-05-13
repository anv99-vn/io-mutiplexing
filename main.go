package main

import (
	"log"
	"os"
)

// main: điểm vào chương trình.
// Bước 1: đọc địa chỉ lắng nghe từ tham số dòng lệnh (mặc định :8080).
// Bước 2: NewServer() được build-tag chọn theo OS/biến thể:
//
//	server_linux.go         -> epollServer
//	server_linux_iouring.go -> ioUringServer (build tag `iouring`)
//	server_darwin.go        -> kqueueServer
//	server_windows.go       -> iocpServer
//
// Tất cả đều thoả interface Server -> main chỉ cần biết Server.Run.
func main() {
	addr := ":8080"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	log.Printf("io-multiplexing HTTP server listening on %s", addr)
	srv := NewServer()
	if err := srv.Run(addr); err != nil {
		log.Fatal(err)
	}
}
