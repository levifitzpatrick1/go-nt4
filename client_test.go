package nt4

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func TestOptionsDefaultsAndInvalid(t *testing.T) {
	c, e := NormalizeClientOptions(ClientOptions{})
	if e != nil || c.CommandMaxBytes <= 0 || c.RetryMin <= 0 {
		t.Fatalf("%+v %v", c, e)
	}
	for _, v := range []ClientOptions{
		{CommandCapacity: -1},
		{Port: 65536},
		{RetryMin: time.Hour, RetryMax: time.Second},
		{MaxJSONDepth: -1},
		{EndpointURL: "file:///tmp/socket"},
	} {
		if _, e := NormalizeClientOptions(v); !errors.Is(e, ErrInvalidOptions) {
			t.Errorf("%+v: %v", v, e)
		}
	}
	if e := ValidatePublisherOptions(PublisherOptions{OfflineQueueCapacity: 1, OfflineQueueMaxBytes: 1024}, c); e != nil {
		t.Fatal(e)
	}
	for _, v := range []PublisherOptions{
		{OfflineQueueCapacity: 1},
		{OfflineQueueMaxBytes: 1},
		{OfflineQueueCapacity: c.TotalOfflineCapacity + 1, OfflineQueueMaxBytes: 1},
	} {
		if e := ValidatePublisherOptions(v, c); !errors.Is(e, ErrInvalidOptions) {
			t.Errorf("%+v: %v", v, e)
		}
	}
	s, e := NormalizeSubscriptionOptions(SubscriptionOptions{})
	if e != nil || s.BufferCapacity <= 0 || s.BufferMaxBytes <= 0 {
		t.Fatal(s, e)
	}
	if _, e := NormalizeSubscriptionOptions(SubscriptionOptions{Mode: DeliveryAll}); !errors.Is(e, ErrIncompatibleOptions) {
		t.Fatal(e)
	}
	if _, e := NormalizeSubscriptionOptions(SubscriptionOptions{Periodic: -1}); !errors.Is(e, ErrInvalidOptions) {
		t.Fatal(e)
	}
	if e := ValidatePatterns([]string{""}, true, c.MaxNameBytes); e != nil {
		t.Fatal(e)
	}
	if e := ValidateTopicName("slashless", c.MaxNameBytes); e != nil {
		t.Fatal(e)
	}
	if e := ValidateTopicName("$clients", c.MaxNameBytes); !errors.Is(e, ErrInvalidOptions) {
		t.Fatal(e)
	}
}

func TestStartAndCancellation(t *testing.T) {
	c, _ := NewClient(ClientOptions{CommandCapacity: 1})
	defer c.Close()
	if c.Status().State != StateIdle {
		t.Fatal(c.Status())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := c.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestOfflineOwnerConcurrent(t *testing.T) {
	c, err := NewClient(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.Publish("camera", "string[]", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1500; j++ {
				if err := p.Set([]string{"abc"}); err != nil {
					t.Error(err)
					return
				}
				s, ok := c.Latest("camera")
				if !ok || s.Timestamp != 1 {
					t.Errorf("lost cached value")
					return
				}
			}
		}()
	}
	wg.Wait()
	s, _ := c.Latest("camera")
	s.Value.([]string)[0] = "modified"
	s, _ = c.Latest("camera")
	if s.Value.([]string)[0] != "abc" {
		t.Fatal("value alias")
	}
	if got := c.Status().RetainedBytes; got == 0 || got > 100 {
		t.Fatalf("retained accumulated: %d", got)
	}
	var closes sync.WaitGroup
	for i := 0; i < 5; i++ {
		closes.Add(1)
		go func() {
			defer closes.Done()
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	closes.Wait()
	if c.Status().State != StateClosed {
		t.Fatal(c.Status())
	}
}

func TestCommandByteAccountingConcurrent(t *testing.T) {
	c, err := NewClient(ClientOptions{CommandCapacity: 16, CommandMaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if err := c.call(func(e *engine) error { return nil }); err != nil {
					t.Error(err)
				}
				if n := c.Status().CommandBytes; n > 1024 {
					t.Errorf("command byte underflow: %d", n)
				}
			}
		}()
	}
	wg.Wait()
	if got := c.Status().CommandBytes; got != 0 {
		t.Fatalf("undrained command reservations: %d", got)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestForeignAndForgedHandlesCannotMutateRegistrations(t *testing.T) {
	a, err := NewClient(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := NewClient(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ap, err := a.Publish("a", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bp, err := b.Publish("b", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Equal numeric UIDs must not turn a forged handle into registry membership.
	fake := *ap
	fake.client = b
	if err := fake.Set(int64(7)); !errors.Is(err, ErrForeignHandle) {
		t.Fatalf("foreign set: %v", err)
	}
	if err := fake.Close(); !errors.Is(err, ErrForeignHandle) {
		t.Fatalf("foreign close: %v", err)
	}
	if err := bp.Set(int64(3)); err != nil {
		t.Fatal(err)
	}
	if s, ok := b.Latest("b"); !ok || s.Value != int64(3) {
		t.Fatalf("local publisher corrupted: %+v %v", s, ok)
	}
	as, err := a.Subscribe([]string{"a"}, SubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bs, err := b.Subscribe([]string{"b"}, SubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	forged := *as
	forged.client = b
	if err := forged.Close(); !errors.Is(err, ErrForeignHandle) {
		t.Fatalf("foreign unsubscribe: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("local subscription removed: %v", err)
	}
	if err := ap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ap.Set(int64(9)); !errors.Is(err, ErrClosed) {
		t.Fatalf("stale set: %v", err)
	}
}

func TestUIDAndClosedHandles(t *testing.T) {
	c, _ := NewClient(ClientOptions{})
	defer c.Close()
	if err := c.call(func(e *engine) error { e.nextPub = math.MaxInt32; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish("a", "int", nil, PublisherOptions{}); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error { e.nextSub = math.MaxInt32; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subscribe([]string{"a"}, SubscriptionOptions{}); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
}

func TestRepairTerminalCloseJoinsConnectionCloser(t *testing.T) {
	peer, c := peerClient(t, protocol40)
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = peerConn(t, peer)
	_ = peerNext(t, peer)
	var s *session
	if err := c.call(func(e *engine) error { s = e.session; return nil }); err != nil {
		t.Fatal(err)
	}
	if s == nil {
		t.Fatal("session not active")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.closeDone:
	default:
		t.Fatal("Close returned before connection-close worker")
	}
}

func TestRepairBackoffCapAndSnapshotEpoch(t *testing.T) {
	c, err := NewClient(ClientOptions{RetryMin: time.Millisecond, RetryMax: 4 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.call(func(e *engine) error {
		e.epoch = 3
		e.topics["foo"] = &topicRecord{name: "foo", epoch: 3, observed: map[string]any{"retained": true}}
		for i := 0; i < 10; i++ {
			c.loss(e, ErrProtocol)
			if e.backoff > c.opts.RetryMax {
				t.Fatalf("backoff %v", e.backoff)
			}
		}
		if e.backoff != c.opts.RetryMax {
			t.Fatalf("backoff %v", e.backoff)
		}
		return nil
	})
	s, ok := c.Topic("foo")
	if !ok || s.Epoch != 3 {
		t.Fatalf("snapshot %+v %v", s, ok)
	}
}

func TestReviewCommandRejectionCountStableAcrossOwnerUpdates(t *testing.T) {
	c, err := NewClient(ClientOptions{CommandMaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("/rejections", "raw", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.TrySet(make([]byte, 64)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("oversized command: %v", err)
	}
	first := c.Status().Rejected
	if first != 1 {
		t.Fatalf("one rejection counted %d times", first)
	}
	for i := 0; i < 3; i++ {
		if _, ok := c.Topic("/rejections"); !ok {
			t.Fatal("topic missing")
		}
		if got := c.Status().Rejected; got != first {
			t.Fatalf("rejection count changed after read %d: %d -> %d", i, first, got)
		}
	}
}

func TestRepairInboundDiagnosticsAtLimit(t *testing.T) {
	c, err := NewClient(ClientOptions{InboundCapacity: 1, InboundMaxBytes: 128, MaxBinaryBytes: 64, MaxTextBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); _ = c.call(func(*engine) error { close(entered); <-release; return nil }) }()
	<-entered
	if !c.post(sessionEvent{size: 64}) {
		close(release)
		t.Fatal("first item rejected")
	}
	s := c.Status()
	if s.InboundItems != 1 || s.InboundBytes != 64 {
		close(release)
		t.Fatalf("inbound %+v", s)
	}
	if c.post(sessionEvent{size: 65}) {
		close(release)
		t.Fatal("byte overflow accepted")
	}
	if c.post(sessionEvent{size: 1}) {
		close(release)
		t.Fatal("item overflow accepted")
	}
	s = c.Status()
	if s.InboundItems != 1 || s.InboundBytes != 64 || s.Rejected != 2 {
		close(release)
		t.Fatalf("overflow changed counters %+v", s)
	}
	close(release)
	<-done
}
