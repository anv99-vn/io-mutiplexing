package main

import (
	"log"
	"os"
)

// main: điểm vào chương trình.
// Bước 1: đọc địa chỉ lắng nghe từ tham số dòng lệnh (mặc định :8080).
// Bước 2: gọi Run() — hàm Run được build-tag chọn theo OS:
//
//	server_linux.go   -> Run dùng epoll
//	server_darwin.go  -> Run dùng kqueue
//	server_windows.go -> Run dùng WSAPoll
func main() {
	addr := ":8080"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	log.Printf("io-multiplexing HTTP server listening on %s", addr)
	if err := Run(addr); err != nil {
		log.Fatal(err)
	}
}
