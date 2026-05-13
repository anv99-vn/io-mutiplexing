package main

// Server: interface chung cho mọi backend I/O multiplexing
// (epoll, kqueue, IOCP, io_uring). Mỗi OS/biến thể cung cấp
// một implementation riêng qua build tag, và hàm NewServer()
// trả về instance phù hợp cho platform hiện tại.
type Server interface {
	// Run khởi động server, lắng nghe trên addr (vd ":8080") và
	// chạy event loop. Gọi h mỗi khi nhận được dữ liệu từ client.
	// Chặn cho tới khi gặp lỗi không thể phục hồi.
	Run(addr string, h PacketHandler) error
}
