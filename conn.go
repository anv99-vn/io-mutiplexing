package main

// PacketHandler is called when data arrives on a connection.
// The handler owns the connection: call conn.Send and conn.Disconnect as needed.
type PacketHandler func(conn *Conn, data []byte)

// Engine wraps the platform Server with a simple callback API.
type Engine struct {
	handler PacketHandler
}

// NewEngine returns a new Engine.
func NewEngine() *Engine { return &Engine{} }

// OnPacket registers the handler called for each incoming packet.
// Returns the Engine for chaining.
func (e *Engine) OnPacket(h PacketHandler) *Engine {
	e.handler = h
	return e
}

// Listen starts the server on addr (e.g. ":8080"). Blocks until fatal error.
func (e *Engine) Listen(addr string) error {
	h := e.handler
	if h == nil {
		h = func(*Conn, []byte) {}
	}
	return NewServer().Run(addr, h)
}
