//go:build linux && iouring

// Linux io_uring: completion model (giống IOCP của Windows, khác epoll readiness).
// Build: go build -tags iouring
// Yêu cầu kernel >= 5.4 (cần IORING_FEAT_SINGLE_MMAP + IORING_OP_ACCEPT/RECV/SEND).
//
// Luồng:
//  1. io_uring_setup(entries) -> ring fd + params (offset của SQ/CQ trong mmap).
//  2. mmap 2 vùng: SQ+CQ ring (chung 1 mmap nếu FEAT_SINGLE_MMAP) và SQE array.
//  3. Tạo listener TCP/IPv4.
//  4. Submit IORING_OP_ACCEPT trên listener.
//  5. io_uring_enter(GET_EVENTS) -> kernel làm việc + trả CQE.
//  6. Loop: đọc CQE -> dispatch theo opKind:
//       accept done -> submit RECV cho client mới + submit ACCEPT mới cho listener.
//       recv done   -> parse request, submit SEND response.
//       send done   -> nếu chưa hết byte, repost SEND; xong -> close.
//
// Ring đặt trong shared memory mmap với kernel:
//   sq_tail (user ghi) / sq_head (kernel đọc)
//   cq_tail (kernel ghi) / cq_head (user đọc)
// Sync qua atomic load/store + io_uring_enter syscall.

package main

import (
	"fmt"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	sysIoUringSetup = 425
	sysIoUringEnter = 426

	opcodeAccept = 13
	opcodeRecv   = 27
	opcodeSend   = 26

	enterGetEvents = 1
	featSingleMmap = 1

	offSqRing = 0
	offCqRing = 0x8000000
	offSqes   = 0x10000000

	sizeofSqe    = 64
	sizeofCqe    = 16
	sizeofUint32 = 4
)

type sqRingOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	Resv1       uint32
	Resv2       uint64
}

type cqRingOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	Cqes        uint32
	Flags       uint32
	Resv1       uint32
	Resv2       uint64
}

type ioUringParams struct {
	SqEntries    uint32
	CqEntries    uint32
	Flags        uint32
	SqThreadCPU  uint32
	SqThreadIdle uint32
	Features     uint32
	WqFd         uint32
	Resv         [3]uint32
	SqOff        sqRingOffsets
	CqOff        cqRingOffsets
}

// sqe: 64 byte, match struct io_uring_sqe trong include/uapi/linux/io_uring.h.
type sqe struct {
	Opcode      uint8
	Flags       uint8
	Ioprio      uint16
	Fd          int32
	Off         uint64
	Addr        uint64
	Len         uint32
	OpFlags     uint32
	UserData    uint64
	BufIndex    uint16
	Personality uint16
	SpliceFdIn  int32
	Pad2        [2]uint64
}

// cqe: 16 byte.
type cqe struct {
	UserData uint64
	Res      int32
	Flags    uint32
}

type opKind uint8

const (
	opAccept opKind = iota + 1
	opRecv
	opSend
)

// ioOp: state cho 1 op async. UserData của SQE chứa id (uint64) tra ra ioOp qua opMap.
// Dùng id thay vì pointer-as-UserData để tránh uintptr↔Pointer round-trip (go vet cấm).
type ioOp struct {
	id       uint64
	kind     opKind
	fd       int32 // listener cho accept; client cho recv/send
	buf      [4096]byte
	sendData []byte
	sendOff  int
}

// opMap + opSeq: registry duy nhất giữ tham chiếu Go tới ioOp khi kernel đang xử lý.
// Worker loop chạy 1 goroutine -> không cần lock.
var (
	opSeq uint64
	opMap = map[uint64]*ioOp{}
)

func registerOp(o *ioOp) {
	opSeq++
	o.id = opSeq
	opMap[o.id] = o
}

func unregisterOp(o *ioOp) {
	delete(opMap, o.id)
}

type ring struct {
	fd     int
	params ioUringParams

	sqRing  []byte
	cqRing  []byte
	sqesMem []byte

	sqHead  *uint32
	sqTail  *uint32
	sqMask  uint32
	sqArray []uint32

	cqHead *uint32
	cqTail *uint32
	cqMask uint32
	cqes   []cqe

	sqes []sqe
}

// newRing: setup io_uring + mmap 3 vùng (SQ ring, CQ ring, SQE array).
func newRing(entries uint32) (*ring, error) {
	var p ioUringParams
	r1, _, errno := syscall.Syscall(sysIoUringSetup, uintptr(entries),
		uintptr(unsafe.Pointer(&p)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup: %w", error(errno))
	}

	r := &ring{fd: int(r1), params: p}

	sqSize := p.SqOff.Array + p.SqEntries*sizeofUint32
	cqSize := p.CqOff.Cqes + p.CqEntries*sizeofCqe

	// Nếu kernel báo FEAT_SINGLE_MMAP -> chỉ cần 1 mmap cho cả 2 ring (kích thước = max).
	if p.Features&featSingleMmap != 0 {
		if cqSize > sqSize {
			sqSize = cqSize
		}
		cqSize = sqSize
	}

	sqMem, err := syscall.Mmap(r.fd, offSqRing, int(sqSize),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		return nil, fmt.Errorf("mmap SQ: %w", err)
	}
	r.sqRing = sqMem

	if p.Features&featSingleMmap != 0 {
		r.cqRing = sqMem
	} else {
		cqMem, err := syscall.Mmap(r.fd, offCqRing, int(cqSize),
			syscall.PROT_READ|syscall.PROT_WRITE,
			syscall.MAP_SHARED|syscall.MAP_POPULATE)
		if err != nil {
			return nil, fmt.Errorf("mmap CQ: %w", err)
		}
		r.cqRing = cqMem
	}

	sqesSize := p.SqEntries * sizeofSqe
	sqesMem, err := syscall.Mmap(r.fd, offSqes, int(sqesSize),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		return nil, fmt.Errorf("mmap SQEs: %w", err)
	}
	r.sqesMem = sqesMem

	// Trỏ các con trỏ head/tail/mask vào trong vùng mmap (qua offset kernel báo).
	// Dùng unsafe.Add để giữ pointer hợp lệ với vet (không tạo uintptr trung gian).
	sqBase := unsafe.Pointer(&sqMem[0])
	r.sqHead = (*uint32)(unsafe.Add(sqBase, p.SqOff.Head))
	r.sqTail = (*uint32)(unsafe.Add(sqBase, p.SqOff.Tail))
	r.sqMask = *(*uint32)(unsafe.Add(sqBase, p.SqOff.RingMask))
	r.sqArray = unsafe.Slice(
		(*uint32)(unsafe.Add(sqBase, p.SqOff.Array)),
		p.SqEntries)

	cqBase := unsafe.Pointer(&r.cqRing[0])
	r.cqHead = (*uint32)(unsafe.Add(cqBase, p.CqOff.Head))
	r.cqTail = (*uint32)(unsafe.Add(cqBase, p.CqOff.Tail))
	r.cqMask = *(*uint32)(unsafe.Add(cqBase, p.CqOff.RingMask))
	r.cqes = unsafe.Slice(
		(*cqe)(unsafe.Add(cqBase, p.CqOff.Cqes)),
		p.CqEntries)

	r.sqes = unsafe.Slice((*sqe)(unsafe.Pointer(&sqesMem[0])), p.SqEntries)
	return r, nil
}

// getSqe: lấy SQE slot tiếp theo, zero rồi điền sq_array để kernel biết vị trí.
// Sau khi điền, gọi advanceTail() để publish.
func (r *ring) getSqe() *sqe {
	tail := atomic.LoadUint32(r.sqTail)
	idx := tail & r.sqMask
	s := &r.sqes[idx]
	*s = sqe{}
	r.sqArray[idx] = idx
	return s
}

func (r *ring) advanceTail() {
	atomic.StoreUint32(r.sqTail, atomic.LoadUint32(r.sqTail)+1)
}

// enter: kernel pull SQE mới + push CQE vào, có thể block tới khi >= minComplete CQE sẵn sàng.
func (r *ring) enter(toSubmit, minComplete uint32, flags uint32) error {
	_, _, errno := syscall.Syscall6(sysIoUringEnter,
		uintptr(r.fd), uintptr(toSubmit), uintptr(minComplete),
		uintptr(flags), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func (r *ring) submitAccept(listener int32, o *ioOp) {
	s := r.getSqe()
	s.Opcode = opcodeAccept
	s.Fd = listener
	s.UserData = o.id
	r.advanceTail()
}

func (r *ring) submitRecv(client int32, o *ioOp) {
	s := r.getSqe()
	s.Opcode = opcodeRecv
	s.Fd = client
	s.Addr = uint64(uintptr(unsafe.Pointer(&o.buf[0])))
	s.Len = uint32(len(o.buf))
	s.UserData = o.id
	r.advanceTail()
}

func (r *ring) submitSend(client int32, data []byte, o *ioOp) {
	s := r.getSqe()
	s.Opcode = opcodeSend
	s.Fd = client
	s.Addr = uint64(uintptr(unsafe.Pointer(&data[0])))
	s.Len = uint32(len(data))
	s.UserData = o.id
	r.advanceTail()
}

// ioUringServer: implementation Server dùng io_uring (Linux >= 5.4).
type ioUringServer struct{ h PacketHandler }

// NewServer: factory trả về backend io_uring (build tag `iouring`).
func NewServer() Server { return &ioUringServer{} }

func (s *ioUringServer) Run(addr string, h PacketHandler) error {
	s.h = h
	host, port, err := parseAddr(addr)
	if err != nil {
		return err
	}

	// Bước 1-3: tạo listener TCP/IPv4.
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return err
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Port: port, Addr: host}); err != nil {
		return err
	}
	if err := syscall.Listen(fd, 128); err != nil {
		return err
	}

	// Bước 1-2: setup io_uring ring.
	r, err := newRing(64)
	if err != nil {
		return err
	}

	// Bước 4: submit accept đầu tiên.
	o := &ioOp{kind: opAccept, fd: int32(fd)}
	registerOp(o)
	r.submitAccept(int32(fd), o)

	// Bước 5-6: worker loop.
	for {
		if err := r.enter(r.params.SqEntries, 1, enterGetEvents); err != nil {
			if errno, ok := err.(syscall.Errno); ok && errno == syscall.EINTR {
				continue
			}
			return err
		}

		// Drain hết CQE hiện có.
		for {
			head := atomic.LoadUint32(r.cqHead)
			tail := atomic.LoadUint32(r.cqTail)
			if head == tail {
				break
			}
			c := r.cqes[head&r.cqMask]
			o := opMap[c.UserData]
			if o == nil {
				atomic.StoreUint32(r.cqHead, head+1)
				continue
			}

			switch o.kind {
			case opAccept:
				listener := o.fd
				unregisterOp(o)

				if c.Res >= 0 {
					client := c.Res
					clientOp := &ioOp{kind: opRecv, fd: client}
					registerOp(clientOp)
					r.submitRecv(client, clientOp)
				}
				// Luôn submit accept mới để duy trì backlog.
				newAccept := &ioOp{kind: opAccept, fd: listener}
				registerOp(newAccept)
				r.submitAccept(listener, newAccept)

			case opRecv:
				client := o.fd
				if c.Res <= 0 {
					syscall.Close(int(client))
					unregisterOp(o)
					break
				}
				data := make([]byte, c.Res)
				copy(data, o.buf[:c.Res])
				unregisterOp(o)
				// handler owns send + close via Conn.
				s.h(&Conn{fd: int(client)}, data)

			case opSend:
				client := o.fd
				if c.Res > 0 {
					o.sendOff += int(c.Res)
				}
				if c.Res < 0 || o.sendOff >= len(o.sendData) {
					syscall.Close(int(client))
					unregisterOp(o)
				} else {
					// Còn byte chưa gửi: repost cùng op với phần còn lại.
					rest := o.sendData[o.sendOff:]
					r.submitSend(client, rest, o)
				}
			}

			atomic.StoreUint32(r.cqHead, head+1)
		}
	}
}
