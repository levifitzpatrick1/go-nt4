package nt4

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/levifitzpatrick1/go-nt4/internal/testpeer"
	"github.com/levifitzpatrick1/go-nt4/internal/wire"
)

func peerClient(t *testing.T, protocol string) (*testpeer.Peer, *Client) {
	t.Helper()
	p := testpeer.New(protocol)
	t.Cleanup(p.Close)
	host, port := p.Address()
	c, err := NewClient(ClientOptions{
		ServerAddress:     host,
		Port:              port,
		ClientName:        "a/b?#%🚀",
		KeepaliveInterval: 80 * time.Millisecond,
		ReadTimeout:       750 * time.Millisecond,
		RetryMin:          20 * time.Millisecond,
		RetryMax:          100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return p, c
}

func deadline(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 3*time.Second)
}

func peerNext(t *testing.T, p *testpeer.Peer) testpeer.Frame {
	t.Helper()
	ctx, cancel := deadline(t)
	defer cancel()
	f, err := p.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func peerConn(t *testing.T, p *testpeer.Peer) *websocket.Conn {
	t.Helper()
	ctx, cancel := deadline(t)
	defer cancel()
	v, err := p.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func sendControl(t *testing.T, conn *websocket.Conn, body string) {
	t.Helper()
	if err := testpeer.Write(conn, websocket.TextMessage, []byte(body)); err != nil {
		t.Fatal(err)
	}
}

func sendValue(t *testing.T, conn *websocket.Conn, id int32, ts int64, v any) {
	t.Helper()
	b, err := wire.EncodeValue(id, ts, wire.DataTypeInt, v)
	if err != nil {
		t.Fatal(err)
	}
	if err = testpeer.Write(conn, websocket.BinaryMessage, b); err != nil {
		t.Fatal(err)
	}
}

func waitState(t *testing.T, c *Client, state LifecycleState) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		if c.Status().State == state {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state %v, wanted %v", c.Status(), state)
}

func readyPeer(t *testing.T, peer *testpeer.Peer, conn *websocket.Conn) {
	t.Helper()
	for {
		probe := peerNext(t, peer)
		if probe.Type != websocket.BinaryMessage {
			continue
		}
		var f wire.Frame
		if err := wire.WalkFrames(probe.Data, func(v wire.Frame) error { f = v; return nil }); err != nil {
			t.Fatal(err)
		}
		if f.TopicID != -1 {
			continue
		}
		b, err := wire.EncodeValue(-1, 10000000, wire.DataTypeInt, f.Value)
		if err != nil {
			t.Fatal(err)
		}
		if err = testpeer.Write(conn, websocket.BinaryMessage, b); err != nil {
			t.Fatal(err)
		}
		return
	}
}

func nextData(t *testing.T, peer *testpeer.Peer) wire.Frame {
	t.Helper()
	for {
		f := peerNext(t, peer)
		if f.Type != websocket.BinaryMessage {
			continue
		}
		var result wire.Frame
		if err := wire.WalkFrames(f.Data, func(v wire.Frame) error { result = v; return nil }); err != nil {
			t.Fatal(err)
		}
		if result.TopicID != -1 {
			return result
		}
	}
}

func hostCounters() (rss int64, switches uint64) {
	data, err := os.ReadFile("/proc/self/statm")
	if err == nil {
		var pages, resident uint64
		if _, err = fmt.Sscanf(string(data), "%d %d", &pages, &resident); err == nil {
			rss = int64(resident) * int64(os.Getpagesize())
		}
	}
	tasks, _ := filepath.Glob("/proc/self/task/*/status")
	for _, task := range tasks {
		b, err := os.ReadFile(task)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "voluntary_ctxt_switches:") || strings.HasPrefix(line, "nonvoluntary_ctxt_switches:") {
				var n uint64
				fmt.Sscanf(strings.TrimSpace(strings.SplitN(line, ":", 2)[1]), "%d", &n)
				switches += n
			}
		}
	}
	return
}
