//go:build windows

// Windows: I/O multiplexing dùng IOCP (completion model).
//
// Khác readiness (epoll/kqueue/WSAPoll):
//   - Code POST trước 1 thao tác (Accept/Recv/Send) cùng buffer + OVERLAPPED.
//   - Kernel làm việc bất đồng bộ, khi xong "post completion" vào completion port.
//   - 1 (hoặc N) worker thread gọi GetQueuedCompletionStatus -> lấy completion.
//
// Luồng:
//   1. WSAStartup (khởi tạo Winsock).
//   2. WSASocket(listener) với WSA_FLAG_OVERLAPPED.
//   3. Bind + Listen.
//   4. CreateIoCompletionPort -> iocp handle, gắn listener vào port.
//   5. Pre-post nhiều AcceptEx (mỗi cái có sẵn socket trống để nhận conn mới).
//   6. Worker loop: GetQueuedCompletionStatus -> dispatch theo loại op.
//      accept done -> setsockopt SO_UPDATE_ACCEPT_CONTEXT, gắn client vào port,
//                     post WSARecv, replenish 1 AcceptEx mới.
//      recv done   -> parse request, build response, post WSASend.
//      send done   -> nếu còn byte chưa gửi -> post WSASend tiếp.
//                     gửi xong -> closesocket.
//
// Mỗi op kèm 1 struct ioOp với field overlapped tại offset 0. Kernel trả về
// con trỏ OVERLAPPED khi completion -> cast ngược về *ioOp qua unsafe.
//
// Pin op trong map: Go GC không biết kernel đang giữ con trỏ -> phải tự giữ
// reference (map) tới khi completion về, mới cho GC thu hồi.
package main

import (
	"fmt"
	"log"
	"sync"
	"syscall"
	"unsafe"
)

// AcceptEx ở mswsock.dll. WSASocket ở ws2_32.dll.
// syscall std không export WSASocket -> load lazy.
var (
	mswsock        = syscall.NewLazyDLL("mswsock.dll")
	procAcceptEx   = mswsock.NewProc("AcceptEx")
	ws2_32         = syscall.NewLazyDLL("ws2_32.dll")
	procWSASocketW = ws2_32.NewProc("WSASocketW")
)

const (
	wsaFlagOverlapped     = 0x01
	soUpdateAcceptContext = 0x700B
	winInfinite           = 0xFFFFFFFF
	// AcceptEx yêu cầu mỗi địa chỉ chiếm sizeof(SOCKADDR_IN)+16 = 32 byte.
	acceptAddrLen = 16 + 16
)

type opKind int

const (
	opAccept opKind = iota
	opRecv
	opSend
)

// ioOp: state cho 1 phép I/O async.
// QUAN TRỌNG: field overlapped PHẢI ở offset 0 -> kernel trả pointer ra đầu struct.
type ioOp struct {
	overlapped syscall.Overlapped
	kind       opKind
	sock       syscall.Handle // op=accept: socket mới (pre-created). op=recv/send: client.
	listener   syscall.Handle // op=accept: ref tới listener để replenish + setsockopt.
	buf        [4096]byte
	wsabuf     syscall.WSABuf
	acceptBuf  [acceptAddrLen * 2]byte // AcceptEx ghi local+remote addr vào đây
	sendData   []byte                  // op=send: payload đầy đủ
	sendOff    int                     // op=send: số byte đã gửi
}

var (
	iocp   syscall.Handle
	pins   = map[uintptr]*ioOp{}
	pinsMu sync.Mutex
)

// pinOp: giữ tham chiếu Go tới ioOp để GC không free trong khi kernel còn dùng.
func pinOp(o *ioOp) {
	pinsMu.Lock()
	pins[uintptr(unsafe.Pointer(o))] = o
	pinsMu.Unlock()
}

func unpinOp(o *ioOp) {
	pinsMu.Lock()
	delete(pins, uintptr(unsafe.Pointer(o)))
	pinsMu.Unlock()
}

// newOverlappedSocket: tạo socket WSA_FLAG_OVERLAPPED — bắt buộc để dùng IOCP.
func newOverlappedSocket() (syscall.Handle, error) {
	r, _, e := procWSASocketW.Call(
		uintptr(syscall.AF_INET),
		uintptr(syscall.SOCK_STREAM),
		uintptr(syscall.IPPROTO_TCP),
		0, 0,
		wsaFlagOverlapped,
	)
	if r == ^uintptr(0) { // INVALID_SOCKET
		return 0, e
	}
	return syscall.Handle(r), nil
}

// postAccept: tạo socket trống + gọi AcceptEx. Kernel sẽ điền socket khi có conn.
func postAccept(listener syscall.Handle) error {
	s, err := newOverlappedSocket()
	if err != nil {
		return err
	}
	o := &ioOp{kind: opAccept, sock: s, listener: listener}
	pinOp(o)

	var bytesRecv uint32
	r, _, e := procAcceptEx.Call(
		uintptr(listener),
		uintptr(s),
		uintptr(unsafe.Pointer(&o.acceptBuf[0])),
		0,                // dwReceiveDataLength = 0 -> không nhận data lúc accept
		acceptAddrLen,    // local addr len
		acceptAddrLen,    // remote addr len
		uintptr(unsafe.Pointer(&bytesRecv)),
		uintptr(unsafe.Pointer(&o.overlapped)),
	)
	// AcceptEx trả FALSE + WSA_IO_PENDING khi pending bình thường.
	if r == 0 {
		en, _ := e.(syscall.Errno)
		if en != syscall.ERROR_IO_PENDING {
			unpinOp(o)
			syscall.Closesocket(s)
			return fmt.Errorf("AcceptEx: %v", e)
		}
	}
	return nil
}

// postRecv: post 1 WSARecv async lên client socket.
func postRecv(client syscall.Handle) error {
	o := &ioOp{kind: opRecv, sock: client}
	o.wsabuf = syscall.WSABuf{Len: uint32(len(o.buf)), Buf: &o.buf[0]}
	pinOp(o)

	var bytes, flags uint32
	err := syscall.WSARecv(client, &o.wsabuf, 1, &bytes, &flags, &o.overlapped, nil)
	if err != nil && err != syscall.ERROR_IO_PENDING {
		unpinOp(o)
		return err
	}
	return nil
}

// postSend: post 1 WSASend async với toàn bộ payload (có thể chỉ gửi 1 phần,
// completion sẽ báo bao nhiêu byte đã gửi; phần còn lại repost ở worker).
func postSend(client syscall.Handle, data []byte) error {
	o := &ioOp{kind: opSend, sock: client, sendData: data, sendOff: 0}
	o.wsabuf = syscall.WSABuf{Len: uint32(len(data)), Buf: &data[0]}
	pinOp(o)

	var bytes uint32
	err := syscall.WSASend(client, &o.wsabuf, 1, &bytes, 0, &o.overlapped, nil)
	if err != nil && err != syscall.ERROR_IO_PENDING {
		unpinOp(o)
		return err
	}
	return nil
}

// iocpServer: implementation Server dùng IOCP của Windows.
type iocpServer struct{}

// NewServer: factory trả về backend IOCP.
func NewServer() Server { return &iocpServer{} }

func (s *iocpServer) Run(addr string) error {
	host, port, err := parseAddr(addr)
	if err != nil {
		return err
	}

	// Bước 1: khởi tạo Winsock 2.2.
	var wsaData syscall.WSAData
	if err := syscall.WSAStartup(0x0202, &wsaData); err != nil {
		return err
	}

	// Bước 2: socket overlapped làm listener.
	listener, err := newOverlappedSocket()
	if err != nil {
		return err
	}
	if err := syscall.SetsockoptInt(listener, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return err
	}

	// Bước 3: bind + listen.
	if err := syscall.Bind(listener, &syscall.SockaddrInet4{Port: port, Addr: host}); err != nil {
		return err
	}
	if err := syscall.Listen(listener, 128); err != nil {
		return err
	}

	// Bước 4: tạo IOCP và gắn listener vào.
	// param 2 = 0 -> tạo port mới. param 4 = 0 -> dùng số thread mặc định (= số CPU).
	iocp, err = syscall.CreateIoCompletionPort(listener, 0, 0, 0)
	if err != nil {
		return err
	}

	// Bước 5: pre-post nhiều AcceptEx để nhận song song nhiều conn.
	for i := 0; i < 8; i++ {
		if err := postAccept(listener); err != nil {
			return err
		}
	}

	// Bước 6: worker loop trên completion port.
	for {
		var nbytes uint32
		var key uint32
		var po *syscall.Overlapped
		cerr := syscall.GetQueuedCompletionStatus(iocp, &nbytes, &key, &po, winInfinite)
		if po == nil {
			// Port hỏng hoàn toàn -> log + tiếp tục (không có op để xử lý).
			log.Printf("GQCS po=nil err=%v", cerr)
			continue
		}
		// Cast OVERLAPPED ngược về *ioOp (overlapped ở offset 0 của struct).
		o := (*ioOp)(unsafe.Pointer(po))

		switch o.kind {
		case opAccept:
			if cerr != nil {
				syscall.Closesocket(o.sock)
				lh := o.listener
				unpinOp(o)
				postAccept(lh)
				continue
			}
			// SO_UPDATE_ACCEPT_CONTEXT: bắt buộc sau AcceptEx,
			// để client kế thừa thuộc tính listener (getpeername v.v. mới hoạt động).
			lh := o.listener
			syscall.Setsockopt(o.sock, syscall.SOL_SOCKET, soUpdateAcceptContext,
				(*byte)(unsafe.Pointer(&lh)), int32(unsafe.Sizeof(lh)))
			// Gắn client socket vào cùng completion port.
			if _, err := syscall.CreateIoCompletionPort(o.sock, iocp, 0, 0); err != nil {
				syscall.Closesocket(o.sock)
				unpinOp(o)
				postAccept(lh)
				continue
			}
			client := o.sock
			unpinOp(o)
			// Post recv chờ request từ client.
			if err := postRecv(client); err != nil {
				syscall.Closesocket(client)
			}
			// Replenish 1 slot accept để duy trì backlog.
			postAccept(lh)

		case opRecv:
			if cerr != nil || nbytes == 0 {
				// Lỗi hoặc peer đóng -> dọn.
				syscall.Closesocket(o.sock)
				unpinOp(o)
				continue
			}
			method, path, ok := parseRequest(o.buf[:nbytes])
			client := o.sock
			unpinOp(o)
			if !ok {
				syscall.Closesocket(client)
				continue
			}
			resp := buildResponse(method, path)
			// Post send response (toàn bộ trong 1 op, kernel có thể chia làm nhiều completion).
			if err := postSend(client, resp); err != nil {
				syscall.Closesocket(client)
			}

		case opSend:
			if cerr != nil {
				syscall.Closesocket(o.sock)
				unpinOp(o)
				continue
			}
			o.sendOff += int(nbytes)
			if o.sendOff >= len(o.sendData) {
				// Gửi xong hết -> đóng conn (Connection: close).
				syscall.Closesocket(o.sock)
				unpinOp(o)
				continue
			}
			// Còn dữ liệu chưa gửi -> repost trên cùng op.
			rest := o.sendData[o.sendOff:]
			o.wsabuf = syscall.WSABuf{Len: uint32(len(rest)), Buf: &rest[0]}
			o.overlapped = syscall.Overlapped{} // reset trước khi reuse
			var bytes uint32
			err := syscall.WSASend(o.sock, &o.wsabuf, 1, &bytes, 0, &o.overlapped, nil)
			if err != nil && err != syscall.ERROR_IO_PENDING {
				syscall.Closesocket(o.sock)
				unpinOp(o)
			}
		}
	}
}
