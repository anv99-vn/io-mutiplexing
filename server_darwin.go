//go:build darwin

// macOS: I/O multiplexing dùng kqueue.
//
// Khác epoll chính ở chỗ:
//   - kqueue thống nhất nhiều loại event (file, socket, signal, timer, proc, vnode...)
//     qua "filter". Ở đây chỉ dùng EVFILT_READ cho socket.
//   - Thay đổi đăng ký (add/remove) và lấy event đều dùng cùng 1 syscall: kevent.
//     -> Có thể batch nhiều thay đổi trong 1 call.
//   - Không có flag SOCK_NONBLOCK ngay trong socket() -> phải SetNonblock riêng.
package main

import (
	"syscall"
)

func Run(addr string) error {
	host, port, err := parseAddr(addr)
	if err != nil {
		return err
	}

	// Bước 1: tạo socket TCP/IPv4 (chưa non-blocking).
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return err
	}

	// Bước 2: bật non-blocking thủ công (macOS không có SOCK_NONBLOCK flag trong Socket()).
	if err := syscall.SetNonblock(fd, true); err != nil {
		return err
	}

	// Bước 3: bind + listen.
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Port: port, Addr: host}); err != nil {
		return err
	}
	if err := syscall.Listen(fd, 128); err != nil {
		return err
	}

	// Bước 4: tạo kqueue instance.
	kq, err := syscall.Kqueue()
	if err != nil {
		return err
	}

	// Bước 5: đăng ký listener fd với filter READ
	// -> kernel báo khi có conn chờ accept.
	if err := kqueueAdd(kq, fd); err != nil {
		return err
	}

	events := make([]syscall.Kevent_t, 64)
	for {
		// Bước 6: chờ event. Param 2 (changes) = nil -> chỉ lấy event,
		// không thay đổi đăng ký lần này.
		n, err := syscall.Kevent(kq, nil, events, nil)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return err
		}

		for i := 0; i < n; i++ {
			// Ident chứa fd tương ứng (kqueue dùng key 64-bit chung cho mọi loại event).
			efd := int(events[i].Ident)

			if efd == fd {
				// Listener ready -> vòng accept tới khi EAGAIN.
				for {
					cfd, _, err := syscall.Accept(fd)
					if err != nil {
						break
					}
					syscall.SetNonblock(cfd, true)
					if err := kqueueAdd(kq, cfd); err != nil {
						syscall.Close(cfd)
					}
				}
				continue
			}

			// Client fd ready -> đọc, parse, ghi, đóng.
			// Đóng fd sẽ tự gỡ khỏi kqueue (không cần kevent DEL).
			handleConnDarwin(efd)
		}
	}
}

// kqueueAdd: thêm fd vào kqueue, chờ event "đọc được" (EVFILT_READ).
// EV_ADD: thêm mới. EV_ENABLE: bật.
// kevent với param changes có entry, events=nil, nevents=0 -> chỉ đăng ký.
func kqueueAdd(kq, fd int) error {
	ev := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE,
	}
	_, err := syscall.Kevent(kq, []syscall.Kevent_t{ev}, nil, nil)
	return err
}

// handleConnDarwin: giống Linux — read, parse, write, close.
func handleConnDarwin(fd int) {
	buf := make([]byte, 4096)
	n, err := syscall.Read(fd, buf)
	if err != nil || n <= 0 {
		syscall.Close(fd)
		return
	}
	method, path, ok := parseRequest(buf[:n])
	if !ok {
		syscall.Close(fd)
		return
	}
	resp := buildResponse(method, path)
	for off := 0; off < len(resp); {
		w, err := syscall.Write(fd, resp[off:])
		if err != nil || w <= 0 {
			break
		}
		off += w
	}
	syscall.Close(fd)
}
