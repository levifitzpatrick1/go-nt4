package nt4

import (
	"context"
	"encoding/json"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/levifitzpatrick1/go-nt4/internal/wire"
)

const protocol41 = "v4.1.networktables.first.wpi.edu"
const protocol40 = "networktables.first.wpi.edu"

type sessionEvent struct {
	epoch           uint64
	kind            string
	controls        []controlEvent
	frame           wire.Frame
	at              time.Time
	err             error
	protocol        string
	conn            *websocket.Conn
	size            int
	echo            int64
	sent            time.Time
	valueOwner      uint32
	valueSequence   uint64
	propertyName    string
	propertyWrite   bool
	propertyVersion uint64
}
type writeJob struct {
	kind            int
	data            []byte
	size            int
	probe           bool
	publisher       uint32
	registration    bool
	valueOwner      uint32
	valueSequence   uint64
	propertyName    string
	propertyWrite   bool
	propertyVersion uint64
}
type session struct {
	epoch       uint64
	conn        *websocket.Conn
	protocol    string
	jobs        chan writeJob
	stop        context.CancelFunc
	ctx         context.Context
	done        sync.WaitGroup
	closeDone   chan struct{} // signals completion of the connection-close worker
	writerBytes int
	writerItems int
	started     atomic.Int64 // taken by writer, completion not yet processed by owner
	lastPong    time.Time
	lastPing    time.Time
	lastRTT     time.Time
	probeQueued bool
}

func endpoint(o ClientOptions) (string, error) {
	if o.EndpointURL != "" {
		u, e := url.Parse(o.EndpointURL)
		if e != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.Path != "" && u.Path != "/" {
			return "", ErrInvalidOptions
		}
		return u.String(), nil
	}
	host := o.ServerAddress
	if strings.Contains(host, ":") {
		if strings.HasPrefix(host, "[") {
			if _, _, e := net.SplitHostPort(host); e == nil {
				return "", ErrInvalidOptions
			}
			host = strings.Trim(host, "[]")
		}
		if net.ParseIP(host) == nil {
			return "", ErrInvalidOptions
		}
	}
	if strings.ContainsAny(host, "/?#% ") || host == "" {
		return "", ErrInvalidOptions
	}
	return "ws://" + net.JoinHostPort(host, strconv.Itoa(o.Port)) + "/nt/" + url.PathEscape(o.ClientName), nil
}
func (c *Client) dial(ctx context.Context, epoch uint64) {
	defer c.workers.Done()
	d := websocket.Dialer{Subprotocols: []string{protocol41, protocol40}, HandshakeTimeout: c.opts.DialTimeout, ReadBufferSize: 1400, WriteBufferSize: 1400}
	var mu sync.Mutex
	var raw net.Conn
	d.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		nc, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err == nil {
			mu.Lock()
			raw = nc
			mu.Unlock()
		}
		return nc, err
	}
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		if raw != nil {
			raw.Close()
		}
		mu.Unlock()
	})
	conn, resp, err := d.DialContext(ctx, c.endpoint, nil)
	stop()
	if ctx.Err() != nil {
		if conn != nil {
			conn.Close()
			conn = nil
		}
		err = ctx.Err()
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err == nil && conn.Subprotocol() != protocol41 && conn.Subprotocol() != protocol40 {
		err = ErrProtocol
		conn.Close()
		conn = nil
	}
	ev := sessionEvent{epoch: epoch, kind: "dial", conn: conn, err: err}
	if conn != nil {
		ev.protocol = conn.Subprotocol()
	}
	select {
	case c.dials <- ev:
	case <-ctx.Done():
		if conn != nil {
			conn.Close()
		}
	}
}
func (c *Client) post(ev sessionEvent) bool {
	c.inMu.Lock()
	if ev.size > c.opts.InboundMaxBytes-c.inBytes || len(c.inbound) == cap(c.inbound) {
		c.inRejected++
		c.inMu.Unlock()
		return false
	}
	c.inBytes += ev.size
	select {
	case c.inbound <- ev:
		c.inMu.Unlock()
		return true
	default:
		c.inBytes -= ev.size
		c.inRejected++
		c.inMu.Unlock()
		return false
	}
}
func (c *Client) readSession(s *session) {
	defer c.sessionWorkers.Done()
	defer s.done.Done()
	s.conn.SetReadLimit(int64(max(c.opts.MaxTextBytes, c.opts.MaxBinaryBytes)))
	s.conn.SetPongHandler(func(string) error { _ = s.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); return c.pong(s) })
	for {
		s.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
		kind, b, err := s.conn.ReadMessage()
		now := time.Now()
		if err != nil {
			c.failSession(s, err)
			return
		}
		if kind == websocket.TextMessage {
			controls, e := parseControls(b, c.opts)
			if e != nil {
				c.failSession(s, e)
				return
			}
			for _, v := range controls {
				if !c.post(sessionEvent{epoch: s.epoch, kind: "control", controls: []controlEvent{v}, at: now, size: len(b)/max(1, len(controls)) + 1}) {
					c.failSession(s, ErrQueueFull)
					return
				}
			}
		} else if kind == websocket.BinaryMessage {
			if len(b) > c.opts.MaxBinaryBytes {
				c.failSession(s, ErrProtocol)
				return
			}
			err = wire.WalkFrames(b, func(f wire.Frame) error {
				if !c.post(sessionEvent{epoch: s.epoch, kind: "frame", frame: f, at: now, size: ValueSize(f.Value) + 32}) {
					return ErrQueueFull
				}
				return nil
			})
			if err != nil {
				c.failSession(s, err)
				return
			}
		}
	}
}
func (c *Client) failSession(s *session, err error) {
	ev := sessionEvent{epoch: s.epoch, kind: "failure", err: err, size: 1}
	if c.post(ev) {
		return
	}
	select {
	case c.failures <- ev:
	case <-s.ctx.Done():
	}
}
func (c *Client) pong(s *session) error {
	if !c.post(sessionEvent{epoch: s.epoch, kind: "pong", at: time.Now(), size: 1}) {
		return ErrQueueFull
	}
	return nil
}
func (c *Client) writeSession(s *session) {
	defer c.sessionWorkers.Done()
	defer s.done.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case job, ok := <-s.jobs:
			if !ok {
				return
			}
			if job.probe {
				now := time.Now()
				echo, ok := probeEcho(c.origin, now)
				if !ok {
					c.failSession(s, ErrProtocol)
					return
				}
				ack := make(chan bool, 1)
				if !c.postProbe(s, echo, now, ack) {
					return
				}
				select {
				case ok := <-ack:
					if !ok {
						continue
					}
				case <-s.ctx.Done():
					return
				}
				job.data, _ = wire.EncodeTimeSyncRequest(echo)
			}
			if job.kind == websocket.PingMessage {
				s.started.Add(1) // only a socket write attempt may be uncertain
				if err := s.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(c.opts.WriteTimeout)); err != nil {
					c.failSession(s, err)
					return
				}
				select {
				case c.written <- sessionEvent{epoch: s.epoch, kind: "written", size: job.size}:
				case <-s.ctx.Done():
					return
				}
				continue
			}
			if err := s.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
				c.failSession(s, err)
				return
			}
			s.started.Add(1) // a probe canceled before its ack was never attempted
			if err := s.conn.WriteMessage(job.kind, job.data); err != nil {
				c.failSession(s, err)
				return
			}
			select {
			case c.written <- sessionEvent{epoch: s.epoch, kind: "written", size: job.size, valueOwner: job.valueOwner, valueSequence: job.valueSequence, propertyName: job.propertyName, propertyWrite: job.propertyWrite, propertyVersion: job.propertyVersion}:
			case <-s.ctx.Done():
				return
			}
		}
	}
}
func (c *Client) postProbe(s *session, echo int64, now time.Time, ack chan bool) bool {
	select {
	case c.probes <- probeRequest{sessionEvent: sessionEvent{epoch: s.epoch, echo: echo, sent: now}, ack: ack}:
	case <-s.ctx.Done():
		return false
	}
	return true
}

type probeRequest struct {
	sessionEvent
	ack chan bool
}

func encodeControl(method string, params any) []byte {
	b, _ := json.Marshal([]any{map[string]any{"method": method, "params": params}})
	return b
}
func jobControl(method string, params any) writeJob {
	b := encodeControl(method, params)
	return writeJob{kind: websocket.TextMessage, data: b, size: len(b)}
}
func (s *session) enqueue(o ClientOptions, j writeJob) bool {
	if s.writerItems >= o.WriterCapacity || j.size > o.WriterMaxBytes-s.writerBytes {
		return false
	}
	s.jobs <- j
	s.writerBytes += j.size
	s.writerItems++
	return true
}
