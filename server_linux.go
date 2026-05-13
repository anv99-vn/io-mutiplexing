//go:build linux && !iouring

// Linux (default): I/O multiplexing dùng epoll.
// Build io_uring variant: `go build -tags iouring`.
//
// Luồng tổng quát:
//  1. Tạo socket TCP non-blocking + REUSEADDR.
//  2. Bind + Listen.
//  3. Tạo epoll instance (epfd).
//  4. EPOLL_CTL_ADD listener vào epfd với EPOLLIN.
//  5. Vòng lặp epoll_wait -> kernel trả danh sách fd ready.
//     - Nếu fd == listener: accept loop (vì non-blocking, accept đến EAGAIN).
//     - Nếu fd == client: gỡ khỏi epoll, gọi handler, đóng.
package main

import (
	"syscall"
)

// epollServer: implementation Server dùng epoll.
type epollServer struct{ h PacketHandler }

// NewServer: factory trả về backend epoll cho Linux (non-iouring build).
func NewServer() Server { return &epollServer{} }

func (s *epollServer) Run(addr string, h PacketHandler) error {
	s.h = h

	host, port, err := parseAddr(addr)
	if err != nil {
		return err
	}

	// Bước 1: tạo socket TCP/IPv4.
	// SOCK_NONBLOCK: gọi accept/read sẽ trả EAGAIN thay vì block.
	// SOCK_CLOEXEC: tự đóng fd khi exec() -> tránh leak vào child process.
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}

	// SO_REUSEADDR: cho phép bind lại nhanh sau restart (tránh "address already in use"
	// do TIME_WAIT của kernel).
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return err
	}

	// Bước 2: bind địa chỉ + listen với backlog 128 (số conn chờ accept tối đa).
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Port: port, Addr: host}); err != nil {
		return err
	}
	if err := syscall.Listen(fd, 128); err != nil {
		return err
	}

	// Bước 3: tạo epoll instance. epfd dùng để đăng ký fd cần theo dõi.
	epfd, err := syscall.EpollCreate1(0)
	if err != nil {
		return err
	}

	// Bước 4: đăng ký listener vào epoll với EPOLLIN
	// (kernel báo khi có conn mới có thể accept).
	if err := epollAdd(epfd, fd); err != nil {
		return err
	}

	// Buffer chứa tối đa 64 event mỗi lần epoll_wait trả về.
	events := make([]syscall.EpollEvent, 64)
	for {
		// Bước 5: chờ event. timeout=-1 = block vô hạn tới khi có fd ready.
		// Kernel chỉ trả những fd thực sự ready (readiness model).
		n, err := syscall.EpollWait(epfd, events, -1)
		if err != nil {
			// EINTR: bị signal ngắt -> lặp lại, không phải lỗi thật.
			if err == syscall.EINTR {
				continue
			}
			return err
		}

		// Duyệt từng fd ready.
		for i := 0; i < n; i++ {
			efd := int(events[i].Fd)

			// Nếu là listener -> accept conn mới.
			if efd == fd {
				// Vòng accept tới khi EAGAIN (do socket non-blocking).
				// Cần vòng vì có thể nhiều conn pending nhưng epoll chỉ báo 1 event.
				for {
					cfd, _, err := syscall.Accept4(fd, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
					if err != nil {
						break // EAGAIN -> hết conn pending
					}
					// Đăng ký client fd vào epoll để chờ data.
					if err := epollAdd(epfd, cfd); err != nil {
						syscall.Close(cfd)
					}
				}
				continue
			}

			// Gỡ client khỏi epoll trước khi xử lý — handler sẽ close fd.
			syscall.EpollCtl(epfd, syscall.EPOLL_CTL_DEL, efd, nil)
			handleConn(efd, s.h)
		}
	}
}

// epollAdd: helper đăng ký fd vào epoll với event EPOLLIN (sẵn sàng đọc).
// Field Fd của EpollEvent là id để nhận dạng khi event trả về.
func epollAdd(epfd, fd int) error {
	return syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd:     int32(fd),
	})
}

// handleConn: đọc data từ fd, gọi handler, handler chịu trách nhiệm gửi + đóng.
func handleConn(fd int, h PacketHandler) {
	buf := make([]byte, 4096)
	n, err := syscall.Read(fd, buf)
	if err != nil || n <= 0 {
		syscall.Close(fd)
		return
	}
	h(&Conn{fd: fd}, buf[:n])
}
