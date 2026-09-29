package testpeer

import (
	"context"
	"github.com/gorilla/websocket"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"
)

func peerURL(p *Peer) string { u, _ := url.Parse(p.Server.URL); u.Scheme = "ws"; return u.String() }
func awaitClose(t *testing.T, p *Peer) {
	t.Helper()
	done := make(chan struct{})
	go func() { p.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("peer shutdown hung")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.conns) != 0 {
		t.Fatalf("surviving connections: %d", len(p.conns))
	}
}
func TestCloseWithFullFrameObservations(t *testing.T) {
	p := New("")
	defer p.Close()
	c, _, err := websocket.DefaultDialer.Dial(peerURL(p), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := p.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cap(p.Frames); i++ {
		if err := Write(c, websocket.BinaryMessage, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(2 * time.Second)
	for len(p.Frames) != cap(p.Frames) {
		select {
		case <-deadline:
			t.Fatal("frame buffer not filled")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := Write(c, websocket.BinaryMessage, []byte("blocked")); err != nil {
		t.Fatal(err)
	}
	awaitClose(t, p)
	if len(p.Frames) != cap(p.Frames) {
		t.Fatalf("silently dropped observed frames: %d", len(p.Frames))
	}
	for i := 0; i < cap(p.Frames); i++ {
		f := <-p.Frames
		if len(f.Data) != 1 || f.Data[0] != byte(i) {
			t.Fatalf("observation order %d: %v", i, f)
		}
	}
}
func TestCloseDuringConcurrentGatedUpgrades(t *testing.T) {
	p := New("")
	gate := make(chan struct{})
	p.UpgradeGate = gate
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dialer := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
			c, _, _ := dialer.Dial(peerURL(p), http.Header{})
			if c != nil {
				c.Close()
			}
		}()
	}
	select {
	case <-p.UpgradeEntered:
	case <-time.After(time.Second):
		t.Fatal("no upgrade entered")
	}
	awaitClose(t, p)
	close(gate)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("dials survived close")
	}
	if len(p.Connections) != 0 {
		t.Fatal("late connection registered")
	}
}
