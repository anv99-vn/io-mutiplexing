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
	"time"
)

// kqueueServer: implementation Server dùng kqueue cho macOS/BSD.
type kqueueServer struct{ h EventHandler }

// NewServer: factory trả về backend kqueue.
func NewServer() Server { return &kqueueServer{} }

func (s *kqueueServer) Run(addr string, h EventHandler) error {
	s.h = h

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

	// deadlines: fd -> thời điểm bị đóng nếu vẫn idle.
	// Sweep mỗi sweepTick xoá fd quá hạn -> chống FD leak khi client treo.
	deadlines := make(map[int]time.Time)
	nextSweep := time.Now().Add(sweepTick)

	events := make([]syscall.Kevent_t, 64)
	for {
		// Bước 6: chờ event với timeout = thời gian tới sweep tiếp theo.
		// Kqueue trả về sớm nếu có event, hoặc sau timeout để sweep deadline.
		remaining := time.Until(nextSweep)
		if remaining < 0 {
			remaining = 0
		}
		ts := syscall.NsecToTimespec(remaining.Nanoseconds())
		n, err := syscall.Kevent(kq, nil, events, &ts)
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
						continue
					}
					// Set deadline: client phải gửi data trong idleTimeout.
					deadlines[cfd] = time.Now().Add(idleTimeout)
				}
				continue
			}

			// Client fd ready: bỏ deadline, xử lý.
			// Đóng fd tự gỡ khỏi kqueue trên darwin (không cần kevent DEL).
			delete(deadlines, efd)
			handleConn(efd, s.h)
		}

		// Sweep deadline: đóng fd idle quá hạn.
		if now := time.Now(); !now.Before(nextSweep) {
			for cfd, dl := range deadlines {
				if now.After(dl) {
					syscall.Close(cfd) // đóng fd tự gỡ khỏi kqueue
					delete(deadlines, cfd)
				}
			}
			nextSweep = now.Add(sweepTick)
		}
	}
}

// kqueueAdd: thêm fd vào kqueue, chờ event "đọc được" (EVFILT_READ).
// EV_ADD: thêm mới. EV_ENABLE: bật.
func kqueueAdd(kq, fd int) error {
	ev := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE,
	}
	_, err := syscall.Kevent(kq, []syscall.Kevent_t{ev}, nil, nil)
	return err
}

// handleConn: gọi Connect → đọc data → Data → Disconnect → đóng fd.
// Server luôn đóng fd sau Disconnect — handler không cần gọi conn.Disconnect().
// Nếu handler đã đóng sớm, syscall.Close trả EBADF và được bỏ qua.
func handleConn(fd int, h EventHandler) {
	conn := &Conn{fd: fd}
	if h.Connect != nil {
		h.Connect(conn)
	}
	buf := make([]byte, 4096)
	n, err := syscall.Read(fd, buf)
	if err != nil || n <= 0 {
		if h.Disconnect != nil {
			h.Disconnect(conn)
		}
		syscall.Close(fd)
		return
	}
	h.Data(conn, buf[:n])
	if h.Disconnect != nil {
		h.Disconnect(conn)
	}
	syscall.Close(fd) // EBADF ignored if handler already closed
}
