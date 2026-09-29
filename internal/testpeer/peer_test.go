package testpeer

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestCloseUnblocksGatedUpgrade(t *testing.T) {
	gate := make(chan struct{})
	p := New("v4.1.networktables.first.wpi.edu")
	p.UpgradeGate = gate
	u, err := url.Parse(p.Server.URL)
	if err != nil {
		t.Fatal(err)
	}
	u.Scheme = "ws"
	u.Path = "/nt/test"
	result := make(chan error, 1)
	go func() {
		c, _, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), nil)
		if c != nil {
			_ = c.Close()
		}
		result <- err
	}()
	select {
	case <-p.UpgradeEntered:
	case <-time.After(time.Second):
		t.Fatal("upgrade did not block")
	}
	finished := make(chan struct{})
	go func() { p.Close(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("peer.Close blocked on gated upgrade")
	}
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("dial survived peer.Close")
	}
	p.Close()
}
