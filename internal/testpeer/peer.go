// Package testpeer provides a scripted raw WebSocket peer for NT4 client tests.
// It deliberately does not import the production codec or implement NT state.
package testpeer

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Frame is one complete WebSocket data frame, copied before delivery.
type Frame struct {
	Type int
	Data []byte
}

// Peer accepts one connection at a time and lets a test script read/write raw
// frames. Frames is bounded; if the test stops draining it the socket read
// blocks, allowing deterministic writer pressure without an unbounded buffer.
type Peer struct {
	Server         *httptest.Server
	Frames         chan Frame
	Connections    chan *websocket.Conn
	UpgradeGate    <-chan struct{} // set before dialing to hold the HTTP upgrade
	UpgradeEntered chan struct{}   // optional buffered notification before the gate
	PingObserved   chan struct{}   // optional buffered notification, with replies disabled
	Disconnected   chan struct{}   // buffered notification when a peer socket exits
	Protocol       string          // empty means no subprotocol selected

	mu        sync.Mutex
	conns     map[*websocket.Conn]struct{}
	wg        sync.WaitGroup
	closed    chan struct{}
	closeOnce sync.Once
}

func New(protocol string) *Peer {
	p := &Peer{Protocol: protocol, Frames: make(chan Frame, 256), Connections: make(chan *websocket.Conn, 8), UpgradeEntered: make(chan struct{}, 8), PingObserved: make(chan struct{}, 32), Disconnected: make(chan struct{}, 8), conns: make(map[*websocket.Conn]struct{}), closed: make(chan struct{})}
	p.Server = httptest.NewServer(http.HandlerFunc(p.serve))
	return p
}

func (p *Peer) serve(w http.ResponseWriter, r *http.Request) {
	if gate := p.UpgradeGate; gate != nil {
		select {
		case p.UpgradeEntered <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		case <-p.closed:
			return
		}
	}
	select {
	case <-p.closed:
		return
	default:
	}
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	if p.Protocol != "" {
		up.Subprotocols = []string{p.Protocol}
	}
	c, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	p.mu.Lock()
	select {
	case <-p.closed:
		p.mu.Unlock()
		_ = c.Close()
		return
	default:
	}
	p.conns[c] = struct{}{}
	p.wg.Add(1)
	p.mu.Unlock()
	c.SetPingHandler(func(string) error {
		select {
		case p.PingObserved <- struct{}{}:
		default:
		}
		return nil // deliberately withhold pong, unless script chooses to send one
	})
	defer func() {
		c.Close()
		p.mu.Lock()
		delete(p.conns, c)
		p.mu.Unlock()
		select {
		case p.Disconnected <- struct{}{}:
		default:
		}
		p.wg.Done()
	}()
	select {
	case p.Connections <- c:
	case <-r.Context().Done():
		return
	case <-p.closed:
		return
	}
	for {
		kind, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		frame := Frame{kind, append([]byte(nil), data...)}
		select {
		case p.Frames <- frame:
		case <-r.Context().Done():
			return
		case <-p.closed:
			return
		}
	}
}

func (p *Peer) Address() (string, int) {
	u, _ := url.Parse(p.Server.URL)
	host, port, _ := net.SplitHostPort(u.Host)
	a, _ := net.LookupPort("tcp", port)
	return host, a
}

// Next waits for one client data message; it does not consume control pings.
func (p *Peer) Next(ctx context.Context) (Frame, error) {
	select {
	case f := <-p.Frames:
		return f, nil
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	}
}

func (p *Peer) Connect(ctx context.Context) (*websocket.Conn, error) {
	select {
	case c := <-p.Connections:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Write sends a single raw server message with a finite deadline. The caller
// owns serialization when multiple scripts share the same connection.
func Write(c *websocket.Conn, kind int, data []byte) error {
	if c == nil {
		return errors.New("nil connection")
	}
	if err := c.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	return c.WriteMessage(kind, data)
}

func (p *Peer) Close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		close(p.closed)
		for c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
		p.Server.CloseClientConnections()
		p.Server.Close()
		p.wg.Wait()
	})
}
