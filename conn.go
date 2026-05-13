package main

// PacketHandler is called when data arrives on a connection.
type PacketHandler func(conn *Conn, data []byte)

// EventHandler groups callbacks for the TCP connection lifecycle.
// Nil fields are no-ops.
type EventHandler struct {
	Connect    func(*Conn)   // fired after accept
	Data       PacketHandler // fired when data arrives
	Disconnect func(*Conn)   // fired before connection closes
}

// TCPServer is the interface for a non-blocking TCP server.
type TCPServer interface {
	// OnConnect registers a callback fired when a new connection is accepted.
	OnConnect(fn func(*Conn)) TCPServer
	// OnData registers a callback fired when data arrives.
	OnData(fn PacketHandler) TCPServer
	// OnDisconnect registers a callback fired before a connection closes.
	OnDisconnect(fn func(*Conn)) TCPServer
	// Listen binds to addr and starts the event loop. Blocks until fatal error.
	Listen(addr string) error
}

// Engine implements TCPServer using the platform-selected backend.
type Engine struct {
	connect    func(*Conn)
	data       PacketHandler
	disconnect func(*Conn)
}

// NewEngine returns a new Engine that implements TCPServer.
func NewEngine() TCPServer { return &Engine{} }

func (e *Engine) OnConnect(fn func(*Conn)) TCPServer {
	e.connect = fn
	return e
}

func (e *Engine) OnData(fn PacketHandler) TCPServer {
	e.data = fn
	return e
}

func (e *Engine) OnDisconnect(fn func(*Conn)) TCPServer {
	e.disconnect = fn
	return e
}

func (e *Engine) Listen(addr string) error {
	h := EventHandler{
		Connect:    e.connect,
		Data:       e.data,
		Disconnect: e.disconnect,
	}
	if h.Data == nil {
		h.Data = func(*Conn, []byte) {}
	}
	return NewServer().Run(addr, h)
}
