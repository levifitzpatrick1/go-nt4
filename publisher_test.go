package nt4

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/levifitzpatrick1/go-nt4/internal/testpeer"
	"github.com/levifitzpatrick1/go-nt4/internal/wire"
)

func TestHandlesAndProperties(t *testing.T) {
	c, _ := NewClient(ClientOptions{})
	defer c.Close()
	if err := ((*Publisher)(nil)).Set(1); !errors.Is(err, ErrInvalidHandle) {
		t.Fatal(err)
	}
	if _, err := c.Publish("$forbidden", "int", nil, PublisherOptions{}); err == nil {
		t.Fatal("hidden publish accepted")
	}
	props := map[string]any{"inner": map[string]any{"x": "old"}}
	p, err := c.Publish("topic", "int", props, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	props["inner"].(map[string]any)["x"] = "changed"
	if err := p.Set(int64(42)); err != nil {
		t.Fatal(err)
	}
	topic, _ := c.Topic("topic")
	if topic.Properties != nil && topic.Properties["inner"] != nil {
		t.Fatal("requested properties exposed as observed")
	}
	if err := p.Set("bad"); !errors.Is(err, ErrInvalidValue) {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Set(int64(1)); err == nil {
		t.Fatal("closed publisher accepted")
	}
}

func TestOfflineAtomicBudgets(t *testing.T) {
	c, err := NewClient(ClientOptions{RetainedMaxBytes: 64, TotalOfflineCapacity: 2, TotalOfflineMaxBytes: 30})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("a", "string", nil, PublisherOptions{OfflineQueueCapacity: 2, OfflineQueueMaxBytes: 30})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Set("a"); err != nil {
		t.Fatal(err)
	}
	if err = p.Set("b"); err != nil {
		t.Fatal(err)
	}
	before, _ := c.Latest("a")
	if err = p.SetDefault("c"); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	after, _ := c.Latest("a")
	if after.Value != before.Value {
		t.Fatal("cache changed after rejection")
	}
	snap, _ := c.Topic("a")
	if snap.Properties["retained"] != nil {
		t.Fatal("property changed after rejection")
	}
	if err = p.DiscardPending(); err != nil {
		t.Fatal(err)
	}
	if err = p.SetDefault("c"); err != nil {
		t.Fatal(err)
	}
	after, _ = c.Latest("a")
	if after.Timestamp != 0 {
		t.Fatal(after)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	if c.Status().RetainedBytes != 0 || c.Status().OfflineItems != 0 {
		t.Fatal(c.Status())
	}
}

func TestSharedCacheAndUncached(t *testing.T) {
	c, _ := NewClient(ClientOptions{})
	defer c.Close()
	a, err := c.Publish("x", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Publish("x", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetDefault(int64(1)); err != nil {
		t.Fatal(err)
	}
	if s, ok := c.Latest("x"); !ok || s.Timestamp != 0 {
		t.Fatal(s, ok)
	}
	if err := b.Set(int64(2)); err != nil {
		t.Fatal(err)
	}
	if s, ok := c.Latest("x"); !ok || s.Value != int64(2) || s.Timestamp != 1 {
		t.Fatal(s, ok)
	}
	if err := c.SetProperties("x", map[string]any{"cached": false}); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Latest("x"); ok {
		t.Fatal("cached:false retained")
	}
	if err := b.Set(int64(3)); !errors.Is(err, ErrIncompatibleOptions) {
		t.Fatal(err)
	}
	_ = a.Close()
	_ = b.Close()
	if _, ok := c.Topic("x"); ok {
		t.Fatal("topic survived last publisher")
	}
}

func TestOfflineByteRejectionUnchanged(t *testing.T) {
	c, _ := NewClient(ClientOptions{RetainedMaxBytes: 20})
	defer c.Close()
	p, err := c.Publish("x", "string", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Set("ok"); err != nil {
		t.Fatal(err)
	}
	if err := p.Set("a value too large for retained budget"); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	v, ok := c.Latest("x")
	if !ok || v.Value != "ok" {
		t.Fatal(v, ok)
	}
}

func TestControlAdmissionAtomicAtStalledWriter(t *testing.T) {
	c, err := NewClient(ClientOptions{CommandCapacity: 1, CommandMaxBytes: 512, WriterCapacity: 1, WriterMaxBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.call(func(e *engine) error {
		e.session = &session{jobs: make(chan writeJob, 1), writerBytes: c.opts.WriterMaxBytes}
		e.session.jobs <- writeJob{size: c.opts.WriterMaxBytes}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.Publish("one", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Publish("two", "int", nil, PublisherOptions{}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("second publish %v", err)
	}
	if _, ok := c.Topic("two"); ok {
		t.Fatal("rejected registration became desired state")
	}
	if err = p.Close(); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("unpublish silently evicted: %v", err)
	}
	if err = p.Set(int64(1)); err != nil {
		t.Fatalf("rejected close invalidated handle: %v", err)
	}
	// Detach the synthetic session; actual socket close is tested by peer cases.
	if err = c.call(func(e *engine) error { e.session = nil; e.replay = nil; e.replayBytes = 0; return nil }); err != nil {
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReceivedStrongDoesNotMakeWeakPublisherStrong(t *testing.T) {
	peer, c := peerClient(t, protocol40)
	p, err := c.Publish("weak", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.SetDefault(int64(1)); err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)
	if f := peerNext(t, peer); f.Type != websocket.TextMessage {
		t.Fatal(f)
	}
	ctx, cancel := deadline(t)
	defer cancel()
	if err = c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if f := nextData(t, peer); f.Timestamp != 0 {
		t.Fatal(f)
	}
	sendControl(t, conn, `[{"method":"announce","params":{"name":"weak","id":0,"type":"int","properties":{}}}]`)
	sendValue(t, conn, 0, 500, int64(9))
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		v, ok := c.Latest("weak")
		if ok && v.Value == int64(9) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if v, ok := c.Latest("weak"); !ok || v.Value != int64(9) {
		t.Fatalf("not canonical: %+v %v", v, ok)
	}
	conn.Close()
	waitState(t, c, StateBackoff)
	if v, ok := c.Latest("weak"); !ok || v.Value != int64(9) || v.Timestamp != 0 || !v.Stale {
		t.Fatalf("weak-only disconnect: %+v %v", v, ok)
	}
}

func TestSocketConcurrentSetCloseNoResurrection(t *testing.T) {
	peer, c := peerClient(t, protocol40)
	p, err := c.Publish("close-race", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)
	if f := peerNext(t, peer); f.Type != websocket.TextMessage {
		t.Fatal(f)
	}
	ctx, cancel := deadline(t)
	defer cancel()
	if err = c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if err := p.TrySet(int64(j)); err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrQueueFull) {
					t.Error(err)
					return
				}
			}
		}()
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	found := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		f := peerNext(t, peer)
		if f.Type == websocket.TextMessage && strings.Contains(string(f.Data), `"unpublish"`) {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("unpublish absent")
	}
	for i := 0; i < 3; i++ {
		probeCtx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
		f, err := peer.Next(probeCtx)
		stop()
		if err != nil {
			break
		}
		if f.Type == websocket.TextMessage && strings.Contains(string(f.Data), `"publish"`) {
			t.Fatal("resurrected publisher")
		}
		if f.Type == websocket.BinaryMessage {
			_ = wire.WalkFrames(f.Data, func(v wire.Frame) error {
				if v.TopicID == int32(p.uid) {
					t.Errorf("value after unpublish: %+v", v)
				}
				return nil
			})
		}
	}
}

func TestCanonicalHistoryAndDiscard(t *testing.T) {
	peer, c := peerClient(t, protocol40)
	a, err := c.Publish("shared", "int", nil, PublisherOptions{OfflineQueueCapacity: 3, OfflineQueueMaxBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Publish("shared", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []int64{1, 2, 3} {
		if err = a.Set(v); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Set(4); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected overflow, got %v", err)
	}
	if got, _ := c.Latest("shared"); got.Value != int64(3) {
		t.Fatal(got)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)
	for i := 0; i < 2; i++ {
		if f := peerNext(t, peer); f.Type != websocket.TextMessage {
			t.Fatalf("missing publish: %+v", f)
		}
	}
	ctx, cancel := deadline(t)
	defer cancel()
	if err = c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for _, want := range []int64{3, 1, 2} {
		if f := nextData(t, peer); f.Value != want || f.Timestamp <= 1 {
			t.Fatalf("want %d got %+v", want, f)
		}
	}
	until := time.Now().Add(time.Second)
	for c.Status().OfflineItems != 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if c.Status().OfflineItems != 0 {
		t.Fatal(c.Status())
	}
	if err = b.SetDefault(int64(8)); err != nil {
		t.Fatal(err)
	}
	if f := nextData(t, peer); f.Value != int64(8) || f.Timestamp != 0 {
		t.Fatalf("weak: %+v", f)
	}
}

func TestRegistrationCursorTinyWriter(t *testing.T) {
	peer := testpeer.New(protocol40)
	defer peer.Close()
	host, port := peer.Address()
	c, err := NewClient(ClientOptions{ServerAddress: host, Port: port, WriterCapacity: 1, WriterMaxBytes: 512, MaxPublishers: 10000, MaxTopics: 10000, KeepaliveInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	const count = 10000
	for i := 0; i < count; i++ {
		if _, err = c.Publish(fmt.Sprintf("p%d", i), "int", nil, PublisherOptions{}); err != nil {
			t.Fatal(i, err)
		}
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	peerConn(t, peer)
	if f := peerNext(t, peer); f.Type != websocket.BinaryMessage {
		t.Fatal(f)
	}
	for i := 0; i < count; {
		f := peerNext(t, peer)
		if f.Type == websocket.BinaryMessage {
			continue
		} // keepalive probe
		if f.Type != websocket.TextMessage {
			t.Fatalf("registration %d: %+v", i, f)
		}
		i++
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = c.WaitConnected(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOfflineAcquisitionBeforeServerOriginRetained(t *testing.T) {
	peer, c := peerClient(t, protocol40)
	p, err := c.Publish("acquisition", "int", nil, PublisherOptions{OfflineQueueCapacity: 1, OfflineQueueMaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.SetAt(time.Now().Add(-time.Hour), int64(7)); err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)
	if f := peerNext(t, peer); f.Type != websocket.TextMessage {
		t.Fatal(f)
	}
	ctx, cancel := deadline(t)
	defer cancel()
	if err = c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-p.Errors():
		if !errors.Is(err, ErrTimestampUnrepresentable) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("missing mapping diagnostic")
	}
	if s := c.Status(); s.OfflineItems != 1 {
		t.Fatal(s)
	}
	if err = p.DiscardPending(); err != nil {
		t.Fatal(err)
	}
	if s := c.Status(); s.OfflineItems != 0 {
		t.Fatal(s)
	}
}

func TestRepairLocalPublishRoutesExplicitPropertyAck(t *testing.T) {
	peer, c := peerClient(t, protocol40)
	sub, err := c.Subscribe([]string{"local-ack"}, SubscriptionOptions{BufferCapacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	pub, err := c.Publish("local-ack", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)
	for i := 0; i < 2; i++ {
		if frame := peerNext(t, peer); frame.Type != websocket.TextMessage {
			t.Fatalf("control %d: %+v", i, frame)
		}
	}
	sendControl(t, conn, `[{"method":"announce","params":{"name":"local-ack","id":4,"type":"int","properties":{}}},{"method":"properties","params":{"name":"local-ack","ack":true,"update":{"review":true}}}]`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, kind := range []EventKind{Announced, PropertiesChanged} {
		select {
		case ev := <-sub.Events():
			if ev.Kind != kind {
				t.Fatalf("event: %+v", ev)
			}
			if kind == PropertiesChanged {
				if !ev.Ack || ev.Topic.Properties["review"] != true {
					t.Fatalf("explicit ACK: %+v", ev)
				}
				if _, ok := ev.Topic.Properties["ack"]; ok {
					t.Fatal("ACK leaked into property map")
				}
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	_ = pub
}

func TestRepairClosingUnmappableSampleReleasesRegistration(t *testing.T) {
	c, err := NewClient(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("/old", "int", nil, PublisherOptions{OfflineQueueCapacity: 2, OfflineQueueMaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetAt(time.Now().Add(-24*time.Hour), int64(7)); err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		e.epoch = 1
		e.clock.reset(1)
		e.session = &session{epoch: 1, jobs: make(chan writeJob, 8)}
		p.registeredEpoch = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		e.clock.hasBest = true
		e.clock.localReceive = time.Now()
		e.clock.serverAtReceive = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		if e.pubs[p.uid] != nil || e.offlineItems != 0 || e.offlineBytes != 0 {
			t.Errorf("closing publisher or pending sample retained")
		}
		if tpc := e.topics[p.name]; tpc != nil && tpc.latest != nil {
			t.Errorf("impossible sample still cached")
		}
		if len(e.session.jobs) != 1 {
			t.Errorf("expected only unpublish, got %d jobs", len(e.session.jobs))
		}
		e.session = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, ok := <-p.Errors(); !ok || !errors.Is(got, ErrTimestampUnrepresentable) {
		t.Fatalf("missing mapping error: %v, open=%v", got, ok)
	}
	if _, ok := <-p.Errors(); ok {
		t.Fatal("error channel not closed after disposition")
	}
}

func TestRepairDeferredPublisherErrorChannelClosedOnClientClose(t *testing.T) {
	c, err := NewClient(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.Publish("/pending", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		e.epoch = 1
		e.session = &session{epoch: 1, jobs: make(chan writeJob, 8)}
		p.registeredEpoch = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error { e.session = nil; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-p.Errors(); ok {
		t.Fatal("client close left deferred publisher error channel open")
	}
}

func TestRepairOfflineLatestCoalescingCountAndHistory(t *testing.T) {
	c, err := NewClient(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("coalescing", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []int64{1, 2, 3} {
		if err = p.Set(v); err != nil {
			t.Fatal(err)
		}
	}
	if s, ok := c.Latest("coalescing"); !ok || s.Value != int64(3) {
		t.Fatalf("latest: %+v %v", s, ok)
	}
	status := c.Status()
	if status.OfflineItems != 0 || status.Dropped != 2 {
		t.Fatalf("latest-only coalescing: %+v", status)
	}
	weak, err := c.Publish("coalescing", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = weak.SetDefault(int64(4)); err != nil {
		t.Fatal(err)
	}
	if c.Status().Dropped != 2 {
		t.Fatalf("nonwinning weak must not evict strong: %+v", c.Status())
	}
}

func TestRepairHistoryRetainedUntilMatchingCompletion(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 2, WriterMaxBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("history", "int", map[string]any{"retained": true}, PublisherOptions{OfflineQueueCapacity: 2, OfflineQueueMaxBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	c.call(func(e *engine) error {
		e.epoch = 1
		e.session = &session{jobs: make(chan writeJob, 2)}
		p.registeredEpoch = 1
		return nil
	})
	if err = p.SetDefault(int64(8)); err != nil {
		t.Fatal(err)
	}
	c.call(func(e *engine) error {
		if len(p.pending) != 1 || e.status.OfflineItems != 1 || p.pending[0].queuedEpoch != 1 {
			t.Fatalf("accepted history retired before completion: %+v %+v", p.pending, e.status)
		}
		j := <-e.session.jobs
		e.session.started.Add(1)
		c.written <- sessionEvent{epoch: 1, size: j.size, valueOwner: j.valueOwner, valueSequence: j.valueSequence}
		return nil
	})
	deadline := time.Now().Add(time.Second)
	for {
		done := false
		c.call(func(e *engine) error {
			done = len(p.pending) == 0 && e.status.OfflineItems == 0
			if done {
				e.session = nil
			}
			return nil
		})
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("matching completion did not release history")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRepairDescriptorBudgetReclaimAndAtomicity(t *testing.T) {
	c, err := NewClient(ClientOptions{RetainedMaxBytes: 256, CommandMaxBytes: 4096, WriterMaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("/a", "string", map[string]any{"label": strings.Repeat("a", 180)}, PublisherOptions{})
	if err != nil {
		t.Fatal("first valid descriptor:", err)
	}
	if _, err := c.Publish("/b", "string", map[string]any{"label": strings.Repeat("b", 180)}, PublisherOptions{}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("aggregate admission: %v", err)
	}
	if _, ok := c.Topic("/b"); ok {
		t.Fatal("rejected topic retained")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish("/b", "string", map[string]any{"label": strings.Repeat("b", 180)}, PublisherOptions{}); err != nil {
		t.Fatal("descriptor not released:", err)
	}
}

func TestRepairRequestedPropertyBudgetAndLatestShareLimit(t *testing.T) {
	c, err := NewClient(ClientOptions{RetainedMaxBytes: 100, CommandMaxBytes: 4096, WriterMaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("/shared", "string", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Set(strings.Repeat("v", 60)); err != nil {
		t.Fatal(err)
	}
	if err := c.SetProperties("/shared", map[string]any{"label": strings.Repeat("x", 60)}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("properties evaded aggregate cache budget: %v", err)
	}
	if v, ok := c.Latest("/shared"); !ok || v.Value != strings.Repeat("v", 60) {
		t.Fatalf("rejection mutated cache: %v %v", v, ok)
	}
	if err := c.SetProperties("/shared", map[string]any{"label": "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := p.Set(strings.Repeat("v", 80)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("latest evaded descriptor budget: %v", err)
	}
	if v, ok := c.Latest("/shared"); !ok || v.Value != strings.Repeat("v", 60) {
		t.Fatalf("rejection mutated cache: %v %v", v, ok)
	}
}

func TestRepairRegisteredCloseBlockedWriterFinalBarrier(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 1, WriterMaxBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("final", "int", map[string]any{"retained": true}, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	err = c.call(func(e *engine) error {
		e.epoch = 1
		s := &session{jobs: make(chan writeJob, 1), writerBytes: 512, writerItems: 1}
		s.jobs <- writeJob{size: 512}
		e.session = s
		p.registeredEpoch = 1
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.SetDefault(int64(42)); err != nil {
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	err = c.call(func(e *engine) error {
		s := e.session
		<-s.jobs
		s.writerBytes = 0
		s.writerItems = 0
		c.flush(e)
		if len(s.jobs) != 1 {
			t.Fatal("final value not scheduled")
		}
		j := <-s.jobs
		order = append(order, "value")
		s.writerBytes -= j.size
		s.writerItems--
		c.flush(e)
		if len(s.jobs) != 1 {
			t.Fatal("unpublish not scheduled")
		}
		j = <-s.jobs
		order = append(order, string(j.data))
		s.writerBytes -= j.size
		s.writerItems--
		e.session = nil
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || !strings.Contains(order[1], `"unpublish"`) {
		t.Fatalf("order %v", order)
	}
}

func TestRepairRemotePropertyIntentByteAdmissionAtomic(t *testing.T) {
	c, err := NewClient(ClientOptions{CommandMaxBytes: 180, RetainedMaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, name := range []string{"/one", "/two"} {
		if err := c.call(func(e *engine) error {
			e.topics[name] = &topicRecord{name: name, requested: map[string]any{}, observed: map[string]any{"retained": true}, members: map[uint32]bool{}}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.SetProperties("/one", map[string]any{"key": strings.Repeat("a", 19)}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetProperties("/two", map[string]any{"key": strings.Repeat("b", 19)}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected global byte rejection: %v", err)
	}
	if err := c.call(func(e *engine) error {
		if len(e.topics["/two"].propertyPatch) != 0 || e.topics["/two"].requested["key"] != nil {
			t.Fatal("rejected intent mutated state")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRepairRemotePropertyVersionsAndReconnect(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.call(func(e *engine) error {
		e.topics["/remote"] = &topicRecord{name: "/remote", observed: map[string]any{"retained": true}, requested: map[string]any{}, members: map[uint32]bool{}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetProperties("/remote", map[string]any{"old": true}); err != nil {
		t.Fatal(err)
	}
	var first writeJob
	if err := c.call(func(e *engine) error {
		e.epoch = 1
		e.session = &session{epoch: 1, jobs: make(chan writeJob, 8)}
		c.flush(e)
		first = <-e.session.jobs
		if !first.propertyWrite || first.propertyVersion != 1 {
			t.Fatalf("first: %+v", first)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetProperties("/remote", map[string]any{"old": nil, "new": "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		e.session.writerItems = 1
		e.session.writerBytes = first.size
		e.session.started.Add(1)
		c.written <- sessionEvent{epoch: 1, size: first.size, propertyName: first.propertyName, propertyWrite: first.propertyWrite, propertyVersion: first.propertyVersion}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		var ready bool
		if err := c.call(func(e *engine) error { ready = len(e.session.jobs) == 1; return nil }); err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new property version not scheduled")
		}
		time.Sleep(time.Millisecond)
	}
	if err := c.call(func(e *engine) error {
		j := <-e.session.jobs
		if !strings.Contains(string(j.data), `"new":"ok"`) || !strings.Contains(string(j.data), `"old":null`) {
			t.Fatalf("lost deletion/update: %s", j.data)
		}
		e.session = nil
		e.topics["/remote"].propertyInFlight = false
		e.epoch = 2
		e.session = &session{epoch: 2, jobs: make(chan writeJob, 8)}
		c.flush(e)
		if len(e.session.jobs) != 1 {
			t.Fatalf("reconnect lost intent: %d", len(e.session.jobs))
		}
		again := <-e.session.jobs
		if !strings.Contains(string(again.data), `"old":null`) || !strings.Contains(string(again.data), `"new":"ok"`) {
			t.Fatalf("reconnect: %s", again.data)
		}
		e.session = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRepairTwoPublisherWeakStrongRebootOrders(t *testing.T) {
	for _, strongFirst := range []bool{true, false} {
		name := "weak-first"
		if strongFirst {
			name = "strong-first"
		}
		t.Run(name, func(t *testing.T) {
			peer, c := peerClient(t, protocol40)
			a, err := c.Publish("shared-reboot", "int", nil, PublisherOptions{})
			if err != nil {
				t.Fatal(err)
			}
			b, err := c.Publish("shared-reboot", "int", nil, PublisherOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if strongFirst {
				err = a.Set(int64(9))
				if err == nil {
					err = b.SetDefault(int64(2))
				}
			} else {
				err = a.SetDefault(int64(2))
				if err == nil {
					err = b.Set(int64(9))
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			got, ok := c.Latest("shared-reboot")
			if !ok || got.Value != int64(9) || got.Timestamp != 1 {
				t.Fatalf("offline canonical: %+v %v", got, ok)
			}
			if c.Status().OfflineItems != 0 {
				t.Fatalf("weak persisted as latest-only history: %+v", c.Status())
			}
			if err = c.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			for epoch := 0; epoch < 2; epoch++ {
				conn := peerConn(t, peer)
				readyPeer(t, peer, conn)
				for i := 0; i < 2; i++ {
					if f := peerNext(t, peer); f.Type != websocket.TextMessage {
						t.Fatalf("epoch %d publish %d: %+v", epoch, i, f)
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				if err = c.WaitReady(ctx); err != nil {
					cancel()
					t.Fatal(err)
				}
				cancel()
				frame := nextData(t, peer)
				if frame.Value != int64(9) || frame.Timestamp <= 1 {
					t.Fatalf("epoch %d canonical frame: %+v", epoch, frame)
				}
				if epoch == 0 {
					conn.Close()
					until := time.Now().Add(3 * time.Second)
					for c.Status().State != StateBackoff && c.Status().State != StateDialing && time.Now().Before(until) {
						time.Sleep(time.Millisecond)
					}
				}
			}
		})
	}
}

func TestRepairPropertyBarrierAndReconnect(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 1, WriterMaxBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("/barrier", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = p
	if err := c.call(func(e *engine) error {
		e.epoch = 1
		e.session = &session{jobs: make(chan writeJob, 1), writerBytes: 2048, writerItems: 1}
		e.session.jobs <- writeJob{size: 2048}
		e.registering = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetProperties("/barrier", map[string]any{"retained": true}); err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		<-e.session.jobs
		e.session.writerBytes, e.session.writerItems = 0, 0
		c.flush(e)
		if len(e.session.jobs) != 1 || !strings.Contains(string((<-e.session.jobs).data), `"publish"`) {
			return fmt.Errorf("publish must lead")
		}
		e.session = nil
		e.replay = nil
		e.replayBytes = 0
		e.registering = false
		e.epoch = 2
		e.session = &session{jobs: make(chan writeJob, 1)}
		e.registering = true
		e.registrationPub = 0
		c.flush(e)
		if len(e.session.jobs) != 1 || !strings.Contains(string((<-e.session.jobs).data), `"retained":true`) {
			return fmt.Errorf("accepted property missing from reconnect registration")
		}
		e.session = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRepairQueuedRegistrationCloseOrdersUnpublish(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 1, WriterMaxBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var jobs []writeJob
	c.call(func(e *engine) error {
		e.epoch = 1
		e.session = &session{jobs: make(chan writeJob, 1), writerBytes: c.opts.WriterMaxBytes, writerItems: 1}
		e.session.jobs <- writeJob{size: c.opts.WriterMaxBytes}
		return nil
	})
	p, err := c.Publish("queued", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetDefault(int64(5)); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	c.call(func(e *engine) error {
		jobs = append(jobs, e.replay...)
		e.session = nil
		e.replay = nil
		e.replayBytes = 0
		return nil
	})
	pub := -1
	unpub := -1
	for i, j := range jobs {
		if strings.Contains(string(j.data), `"publish"`) {
			pub = i
		}
		if strings.Contains(string(j.data), `"unpublish"`) {
			unpub = i
		}
	}
	if pub < 0 || unpub >= 0 {
		t.Fatalf("premature unpublish or lost registration: %+v", jobs)
	}
}

func TestRepairWriterDiagnosticsAndUncertain(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 2, WriterMaxBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.call(func(e *engine) error {
		e.epoch = 1
		s := &session{jobs: make(chan writeJob, 2), writerBytes: 100, writerItems: 2}
		s.jobs <- writeJob{size: 50}
		s.jobs <- writeJob{size: 50}
		e.session = s
		c.flush(e)
		if e.status.WriterItems != 2 || e.status.WriterBytes != 100 {
			t.Fatalf("writer status %+v", e.status)
		}
		if s.started.Load() != 0 {
			t.Fatal("queued jobs counted as attempted")
		}
		e.session = nil
		return nil
	})
}

func TestRepairLatestWriterVersionAndUnattemptedLoss(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 1, WriterMaxBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("latest-version", "int", map[string]any{"retained": true}, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.call(func(e *engine) error {
		e.epoch = 1
		e.session = &session{epoch: 1, jobs: make(chan writeJob, 1), writerItems: 1, writerBytes: 512}
		e.session.jobs <- writeJob{size: 512}
		p.registeredEpoch = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, v := range []int64{1, 2, 3} {
		if err = p.SetDefault(v); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.call(func(e *engine) error {
		if e.status.Dropped != 2 || len(e.session.jobs) != 1 {
			t.Fatalf("blocked coalescing: %+v", e.status)
		}
		<-e.session.jobs
		e.session.writerItems = 0
		e.session.writerBytes = 0
		c.flush(e)
		j := <-e.session.jobs
		var val wire.Frame
		if err := wire.WalkFrames(j.data, func(f wire.Frame) error { val = f; return nil }); err != nil {
			return err
		}
		if val.Value != int64(3) {
			t.Fatalf("latest scheduled: %+v", val)
		}
		e.session.jobs <- j
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = p.SetDefault(int64(4)); err != nil {
		t.Fatal(err)
	}
	if err = c.call(func(e *engine) error {
		j := <-e.session.jobs
		e.session.started.Add(1)
		c.written <- sessionEvent{epoch: 1, size: j.size, valueOwner: j.valueOwner, valueSequence: j.valueSequence}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	for {
		done := false
		if err = c.call(func(e *engine) error {
			if len(e.session.jobs) == 1 {
				j := <-e.session.jobs
				var val wire.Frame
				if err := wire.WalkFrames(j.data, func(f wire.Frame) error { val = f; return nil }); err != nil {
					return err
				}
				if val.Value != int64(4) {
					t.Fatalf("newer value lost after older completion: %+v", val)
				}
				if e.session.started.Load() != 0 {
					t.Fatal("completed attempt still counted")
				}
				e.session.jobs <- j
				done = true
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		if time.Now().After(until) {
			t.Fatal("dirty value not scheduled")
		}
		time.Sleep(time.Millisecond)
	}
	if err = c.call(func(e *engine) error {
		s := e.session
		e.session = nil
		e.status.Uncertain += uint64(s.started.Load())
		p.sent = 0
		p.registeredEpoch = 0
		e.epoch = 2
		e.session = &session{epoch: 2, jobs: make(chan writeJob, 1)}
		p.registeredEpoch = 2
		c.flush(e)
		if e.status.Uncertain != 0 {
			t.Fatalf("untaken job uncertain: %+v", e.status)
		}
		if len(e.session.jobs) != 1 {
			t.Fatal("latest not replayed")
		}
		e.session = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReviewEmptyTopicPropertyCompletion(t *testing.T) {
	peer, c := peerClient(t, protocol40)
	if _, err := c.Publish("", "int", nil, PublisherOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	peerConn(t, peer)
	if f := peerNext(t, peer); f.Type != websocket.BinaryMessage {
		t.Fatalf("probe: %+v", f)
	}
	if f := peerNext(t, peer); f.Type != websocket.TextMessage {
		t.Fatalf("publish: %+v", f)
	}
	if err := c.SetProperties("", map[string]any{"retained": true}); err != nil {
		t.Fatal(err)
	}
	if f := peerNext(t, peer); f.Type != websocket.TextMessage {
		t.Fatalf("properties: %+v", f)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		var inflight bool
		var bytes int
		if err := c.call(func(e *engine) error {
			inflight = e.topics[""].propertyInFlight
			bytes = e.propertyBytes
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !inflight && bytes == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("successful property write never retires empty-name intent: inflight=%v bytes=%d", inflight, bytes)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReviewOfflinePublisherPropertiesRespectAggregateByteBudget(t *testing.T) {
	c, err := NewClient(ClientOptions{RetainedMaxBytes: 256, CommandMaxBytes: 4096, WriterMaxBytes: 4096, MaxPublishers: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	props := map[string]any{"label": strings.Repeat("x", 180)}
	accepted := 0
	for i := 0; i < 12; i++ {
		_, err := c.Publish(strings.Repeat("n", i+1), "string", props, PublisherOptions{})
		if err == nil {
			accepted++
			continue
		}
		if !errors.Is(err, ErrQueueFull) {
			t.Fatalf("unexpected admission error %v", err)
		}
		break
	}
	if accepted > 1 {
		t.Fatalf("accepted %d descriptors (~%d property bytes), over aggregate 256-byte retained budget", accepted, accepted*180)
	}
}

func TestReviewCloseBeforeReplayDoesNotSendUnregisteredUnpublish(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 1, WriterMaxBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("review-order", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		e.session = &session{jobs: make(chan writeJob, 1), writerBytes: c.opts.WriterMaxBytes}
		e.session.jobs <- writeJob{size: c.opts.WriterMaxBytes}
		e.registering = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	var first string
	if err := c.call(func(e *engine) error {
		if len(e.replay) != 0 {
			first = string(e.replay[0].data)
		}
		e.session = nil
		e.replay = nil
		e.replayBytes = 0
		e.registering = false
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(first, `"unpublish"`) {
		t.Fatalf("unpublish scheduled before publisher registered: %s", first)
	}
}

func TestReviewPublisherClosePreservesAcceptedFinalValue(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 1, WriterMaxBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("review-last", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		e.session = &session{jobs: make(chan writeJob, 1), writerBytes: c.opts.WriterMaxBytes}
		e.session.jobs <- writeJob{size: c.opts.WriterMaxBytes}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.SetDefault(int64(7)); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	var jobs []writeJob
	if err := c.call(func(e *engine) error {
		jobs = append(jobs, e.replay...)
		e.session = nil
		e.replay = nil
		e.replayBytes = 0
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	seenValue := false
	for _, job := range jobs {
		if job.kind == 2 && !job.probe {
			seenValue = true
		}
		if strings.Contains(string(job.data), `"unpublish"`) && !seenValue {
			t.Fatalf("unpublish before accepted final value; queued jobs: %+v", jobs)
		}
	}
}

func TestReviewOfflineRemotePropertyIntentIsNotReplayed(t *testing.T) {
	c, err := NewClient(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.call(func(e *engine) error {
		e.topics["/review/remote"] = &topicRecord{name: "/review/remote", observedType: "int", observed: map[string]any{"retained": true}, requested: map[string]any{}, members: map[uint32]bool{}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetProperties("/review/remote", map[string]any{"label": "accepted"}); err != nil {
		t.Fatal(err)
	}
	var sent []string
	if err := c.call(func(e *engine) error {
		e.epoch++
		e.session = &session{jobs: make(chan writeJob, 8)}
		e.registering = true
		c.flush(e)
		for len(e.session.jobs) > 0 {
			sent = append(sent, string((<-e.session.jobs).data))
		}
		e.session = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, msg := range sent {
		if strings.Contains(msg, `"setproperties"`) && strings.Contains(msg, `"label"`) {
			return
		}
	}
	t.Fatalf("accepted remote property intent omitted on reconnect: %q", sent)
}

func TestReviewSecondPublisherCannotDisableCachedWithPendingHistory(t *testing.T) {
	c, err := NewClient(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first, err := c.Publish("/shared-history", "int", nil, PublisherOptions{OfflineQueueCapacity: 2, OfflineQueueMaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err = first.Set(int64(9)); err != nil {
		t.Fatal(err)
	}
	before := c.Status()
	second, err := c.Publish("/shared-history", "int", map[string]any{"cached": false}, PublisherOptions{})
	if !errors.Is(err, ErrIncompatibleOptions) {
		t.Fatalf("accepted cached:false with another publisher's pending history: handle=%v err=%v", second, err)
	}
	if second != nil {
		t.Fatal("rejected publisher returned a handle")
	}
	after := c.Status()
	if after.OfflineItems != before.OfflineItems || after.OfflineBytes != before.OfflineBytes {
		t.Fatalf("changed accepted pending history: before=%+v after=%+v", before, after)
	}
	sample, ok := c.Latest("/shared-history")
	if !ok || sample.Value != int64(9) {
		t.Fatalf("lost canonical latest after rejected property change: %+v %v", sample, ok)
	}
	if err := first.Set(int64(10)); err != nil {
		t.Fatalf("first handle after rejection: %v", err)
	}
	if err := first.DiscardPending(); err != nil {
		t.Fatal(err)
	}
	second, err = c.Publish("/shared-history", "int", map[string]any{"cached": false}, PublisherOptions{})
	if err != nil || second == nil {
		t.Fatalf("transition after explicit discard: %v %v", second, err)
	}
	if _, ok := c.Latest("/shared-history"); ok {
		t.Fatal("uncached transition kept canonical latest")
	}
	if _, err = c.Publish("/shared-history", "int", nil, PublisherOptions{OfflineQueueCapacity: 1, OfflineQueueMaxBytes: 64}); !errors.Is(err, ErrIncompatibleOptions) {
		t.Fatalf("history handle on shared uncached topic: %v", err)
	}
}

func TestReviewOfflineWeakDoesNotReplaceStrongCanonicalCache(t *testing.T) {
	c, err := NewClient(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	strong, err := c.Publish("shared", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	weak, err := c.Publish("shared", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := strong.Set(int64(9)); err != nil {
		t.Fatal(err)
	}
	if err := weak.SetDefault(int64(2)); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Latest("shared")
	if !ok || got.Timestamp != 1 || got.Value != int64(9) {
		t.Fatalf("weak write displaced stronger cached value: %+v, ok=%v", got, ok)
	}
}

func TestReviewPropertiesCannotPrecedeRegistration(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 2, WriterMaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("/review/props", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = p
	var order []string
	err = c.call(func(e *engine) error {
		e.epoch = 1
		e.session = &session{jobs: make(chan writeJob, 2), writerBytes: 1024, writerItems: 1}
		e.session.jobs <- writeJob{size: 1024}
		e.registering = true
		e.registrationPub = 0
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetProperties("/review/props", map[string]any{"retained": true}); err != nil {
		t.Fatal(err)
	}
	err = c.call(func(e *engine) error {
		<-e.session.jobs
		e.session.writerBytes = 0
		e.session.writerItems = 0
		c.flush(e)
		for len(e.session.jobs) > 0 {
			j := <-e.session.jobs
			order = append(order, string(j.data))
		}
		e.session = nil
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(order) < 2 || !strings.Contains(order[0], `"publish"`) {
		t.Fatalf("property before registration: %q", order)
	}
}
