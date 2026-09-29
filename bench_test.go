package nt4

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func BenchmarkCachedRead(b *testing.B) {
	c, err := NewClient(ClientOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("read", "int", nil, PublisherOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := p.Set(int64(7)); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if s, ok := c.Latest("read"); !ok || s.Value != int64(7) {
			b.Fatal(s, ok)
		}
	}
}

func BenchmarkFanout(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			e := &engine{subs: make(map[uint32]*Subscription), topics: make(map[string]*topicRecord)}
			topic := &topicRecord{name: "fanout", observedType: "int"}
			e.topics[topic.name] = topic
			for i := range n {
				s := &Subscription{uid: uint32(i + 1), active: true, patterns: []string{"fanout"}, opts: SubscriptionOptions{Mode: DeliveryAll, All: true, BufferCapacity: 2, BufferMaxBytes: 512}, events: make(chan Event, 2), errors: make(chan error, 1)}
				e.subs[s.uid] = s
			}
			e.rebuildRoutes()
			ev := Event{Kind: ValueReceived, Topic: topic.snapshot(), Sample: Sample{Value: int64(42)}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e.route(ev)
				for _, s := range topic.routes {
					select {
					case <-s.events:
					default:
						b.Fatal("missing fanout event")
					}
				}
			}
		})
	}
}

func BenchmarkOfflineAdmission(b *testing.B) {
	for _, tc := range []struct {
		name, kind string
		value      any
	}{
		{"scalar", "int", int64(3)},
		{"raw1KiB", "raw", make([]byte, 1024)},
		{"raw64KiB", "raw", make([]byte, 64<<10)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c, err := NewClient(ClientOptions{CommandMaxBytes: 256 << 10, RetainedMaxBytes: 256 << 10})
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			p, err := c.Publish("bench", tc.kind, nil, PublisherOptions{})
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(ValueSize(tc.value)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := p.TrySet(tc.value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReplayRegistration(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c, err := NewClient(ClientOptions{MaxPublishers: 128, MaxTopics: 128})
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 100; j++ {
			p, err := c.Publish(fmt.Sprintf("replay/%d", j), "int", nil, PublisherOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if err := p.Set(int64(j)); err != nil {
				b.Fatal(err)
			}
		}
		if err := c.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func FuzzControlBatch(f *testing.F) {
	for _, b := range [][]byte{
		[]byte(`[42,{"method":"announce","params":{"name":"a","id":0,"type":"int","properties":{}}}]`),
		[]byte(`[{"method":"announce","params":{"name":"bad","id":4294967296,"type":"int","properties":{}}}]`),
		[]byte(`[{"method":"properties","params":{"name":"a","update":{"nested":[1,null,{"x":true}]}}}]`),
		[]byte(`[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]`),
	} {
		f.Add(b)
	}
	o, err := NormalizeClientOptions(ClientOptions{})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > o.MaxTextBytes {
			t.Skip()
		}
		events, err := parseControls(data, o)
		if err != nil {
			return
		}
		for _, ev := range events {
			if ev.id < 0 || ev.method == "announce" && (ev.typ == "" || ev.props == nil) {
				t.Fatalf("invalid envelope accepted: %+v", ev)
			}
		}
	})
}

func FuzzOfflineTransitions(f *testing.F) {
	f.Add([]byte{0, 1, 1, 2, 3, 4, 1, 5})
	f.Add([]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
	f.Fuzz(func(t *testing.T, actions []byte) {
		if len(actions) > 128 {
			t.Skip()
		}
		c, err := NewClient(ClientOptions{CommandCapacity: 2, CommandMaxBytes: 2048, RetainedMaxBytes: 4096, MaxPublishers: 16})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		var p *Publisher
		var closed bool
		for _, action := range actions {
			switch action % 6 {
			case 0:
				if !closed && p == nil {
					p, err = c.Publish("fuzz", "int", nil, PublisherOptions{})
					if err != nil {
						t.Fatal(err)
					}
				}
			case 1:
				if p != nil {
					err = p.Set(int64(action))
					if closed && !errors.Is(err, ErrClosed) {
						t.Fatalf("set after close: %v", err)
					}
					if !closed && err != nil && !errors.Is(err, ErrClosed) {
						t.Fatal(err)
					}
				}
			case 2:
				if p != nil {
					_ = p.SetDefault(int64(action))
				}
			case 3:
				if !closed && p != nil {
					_ = c.SetProperties("fuzz", map[string]any{"retained": true})
				}
			case 4:
				if !closed && p != nil {
					_ = p.Close()
					p = nil
				}
			case 5:
				if !closed {
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					closed = true
				}
			}
			if closed && c.Status().State != StateClosed {
				t.Fatalf("not terminal: %+v", c.Status())
			}
		}
		if closed && c.Start(context.Background()) != ErrClosed {
			t.Fatal("restarted closed client")
		}
	})
}
