package nt4

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/levifitzpatrick1/go-nt4/internal/testpeer"
	"github.com/levifitzpatrick1/go-nt4/internal/wire"
)

func TestEndpointIdentityAndValidation(t *testing.T) {
	o, _ := NormalizeClientOptions(ClientOptions{ServerAddress: "::1", ClientName: "a/b?#%🚀"})
	path, err := endpoint(o)
	if err != nil || !strings.Contains(path, "[::1]:5810/nt/a%2Fb%3F%23%25%F0%9F%9A%80") {
		t.Fatalf("%q: %v", path, err)
	}
	for _, bad := range []string{"ws://host/nt/x#fragment", "ws://host/nt/x?q=1", "ftp://host/"} {
		if _, err := NewClient(ClientOptions{EndpointURL: bad}); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}

func TestControlNeighborsAndIDBoundaries(t *testing.T) {
	o, err := NormalizeClientOptions(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	batch := []byte(`[42,{"method":"announce","params":{"name":"zero","id":0,"type":"int","properties":{}}},{"method":"announce","params":{"name":"overflow","id":4294967296,"type":"int","properties":{}}},{"method":"announce","params":{"name":"fraction","id":1.5,"type":"int","properties":{}}},{"method":"future","params":{}},{"method":"announce","params":{"name":"max","id":2147483647,"type":"custom","properties":{"nested":{"v":[true,null]}}}}]`)
	got, err := parseControls(batch, o)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].name != "zero" || got[0].id != 0 || got[1].name != "max" || got[1].id != 2147483647 || got[1].typ != "custom" {
		t.Fatalf("batch isolation: %+v", got)
	}
	if _, err := parseControls([]byte(`[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]`), o); err == nil {
		t.Fatal("excess depth accepted")
	}
}

func TestPeerValuesReplayAndEpoch(t *testing.T) {
	p, c := peerClient(t, protocol40)
	sub, err := c.Subscribe([]string{"topic"}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 16})
	if err != nil {
		t.Fatal(err)
	}
	pub, err := c.Publish("mine", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = pub.Set(int64(21)); err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, p)
	first := peerNext(t, p)
	if first.Type != websocket.BinaryMessage {
		t.Fatalf("RTT not first: %+v", first)
	}
	second := peerNext(t, p)
	if second.Type != websocket.TextMessage {
		t.Fatalf("publish missing: %+v", second)
	}
	third := peerNext(t, p)
	if third.Type != websocket.TextMessage {
		t.Fatalf("subscribe missing: %+v", third)
	}
	ctx, cancel := deadline(t)
	defer cancel()
	if err = c.WaitConnected(ctx); err != nil {
		t.Fatal(err)
	}
	sendControl(t, conn, `[{"method":"announce","params":{"name":"topic","id":0,"type":"int","properties":{}}},{"method":"announce","params":{"name":2,"id":3,"type":"int","properties":{}}}]`)
	sendValue(t, conn, 0, 10, int64(1))
	sendValue(t, conn, 0, 9, int64(2))
	sendValue(t, conn, 0, 10, int64(3))
	for i := 0; i < 4; i++ {
		select {
		case ev := <-sub.Events():
			if i == 0 && ev.Kind != Announced || i > 0 && ev.Kind != ValueReceived {
				t.Fatalf("event %d: %+v", i, ev)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	latest, ok := c.Latest("topic")
	if !ok || latest.Value != int64(3) {
		t.Fatalf("latest %+v %v", latest, ok)
	}
	var probe wire.Frame
	_ = wire.WalkFrames(first.Data, func(f wire.Frame) error { probe = f; return nil })
	echo := probe.Value.(int64)
	b, _ := wire.EncodeValue(-1, 8000, wire.DataTypeInt, echo)
	if err = testpeer.Write(conn, websocket.BinaryMessage, b); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		f := peerNext(t, p)
		if f.Type == websocket.BinaryMessage {
			var got wire.Frame
			_ = wire.WalkFrames(f.Data, func(v wire.Frame) error { got = v; return nil })
			if got.TopicID == -1 {
				continue
			}
			if got.TopicID != int32(pub.uid) || got.Timestamp <= 1 || got.Value != int64(21) {
				t.Fatalf("replay %+v", got)
			}
			break
		}
	}
	conn.Close()
	until := time.Now().Add(3 * time.Second)
	for c.Status().Epoch < 2 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if c.Status().Epoch < 2 {
		t.Fatal("no reconnect")
	}
}

func TestPeerHeldUpgradeCloseAndMissingProtocol(t *testing.T) {
	p, c := peerClient(t, protocol40)
	gate := make(chan struct{})
	p.UpgradeGate = gate
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := deadline(t)
	defer cancel()
	select {
	case <-p.UpgradeEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("close blocked during upgrade")
	}
	close(gate)
	if c.Status().State != StateClosed {
		t.Fatal(c.Status())
	}
	q, d := peerClient(t, "")
	if err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.Disconnected:
	case <-ctx.Done():
		t.Fatal("missing protocol not disconnected")
	}
	for !errors.Is(d.Status().LastError, ErrProtocol) && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(d.Status().LastError, ErrProtocol) {
		t.Fatal(d.Status(), ctx.Err())
	}
}

func TestPeerNoPongWithTrafficAnd40NoPing(t *testing.T) {
	p, c := peerClient(t, protocol41)
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, p)
	peerNext(t, p)
	waitState(t, c, StateOnlineUnsynchronized)
	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				b, _ := json.Marshal([]any{map[string]any{"method": "unannounce", "params": map[string]any{"name": "unused", "id": 0}}})
				_ = testpeer.Write(conn, websocket.TextMessage, b)
			}
		}
	}()
	defer close(stop)
	ctx, cancel := deadline(t)
	defer cancel()
	select {
	case <-p.PingObserved:
	case <-ctx.Done():
		t.Fatal("no ping")
	}
	select {
	case <-p.Disconnected:
	case <-ctx.Done():
		t.Fatal("RTT/text traffic hid missing pong")
	}
	p2, c2 := peerClient(t, protocol40)
	if err := c2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	peerConn(t, p2)
	peerNext(t, p2)
	select {
	case <-p2.PingObserved:
		t.Fatal("4.0 pinged")
	case <-time.After(180 * time.Millisecond):
	}
}

func TestPeerMalformedSuffixPreservesPriorAndReconnect(t *testing.T) {
	p, c := peerClient(t, protocol40)
	sub, err := c.Subscribe([]string{"v"}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 16})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, p)
	peerNext(t, p)
	peerNext(t, p)
	sendControl(t, conn, `[{"method":"announce","params":{"name":"v","id":0,"type":"int","properties":{}}}]`)
	first, _ := wire.EncodeValue(0, 5, wire.DataTypeInt, int64(7))
	frame := append(first, 0xc1)
	if err = testpeer.Write(conn, websocket.BinaryMessage, frame); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := deadline(t)
	defer cancel()
	for {
		select {
		case v := <-sub.Events():
			if v.Kind == ValueReceived {
				if v.Sample.Value != int64(7) {
					t.Fatal(v)
				}
				goto valid
			}
		case <-ctx.Done():
			t.Fatal("lost valid prefix")
		}
	}
valid:
	second := peerConn(t, p)
	if second == nil {
		t.Fatal("no reconnect")
	}
	peerNext(t, p)
	peerNext(t, p)
	sendControl(t, second, `[{"method":"announce","params":{"name":"v","id":3,"type":"int","properties":{}}}]`)
	sendValue(t, second, 3, 1, int64(9))
	for {
		select {
		case ev := <-sub.Events():
			if ev.Kind == ValueReceived && ev.Epoch == c.Status().Epoch {
				if ev.Sample.Value != int64(9) {
					t.Fatal(ev)
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("no fresh epoch")
		}
	}
}

func TestPeerTinyWriterReplayAndImmediateRTT(t *testing.T) {
	p := testpeer.New(protocol40)
	defer p.Close()
	host, port := p.Address()
	c, err := NewClient(ClientOptions{ServerAddress: host, Port: port, WriterCapacity: 1, WriterMaxBytes: 300, RetryMin: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 20; i++ {
		if _, err := c.Subscribe([]string{strings.Repeat("s", i+1)}, SubscriptionOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, p)
	first := peerNext(t, p)
	var probe wire.Frame
	if err = wire.WalkFrames(first.Data, func(f wire.Frame) error { probe = f; return nil }); err != nil {
		t.Fatal(err)
	}
	reply, _ := wire.EncodeValue(-1, 100, wire.DataTypeInt, probe.Value)
	if err = testpeer.Write(conn, websocket.BinaryMessage, reply); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := deadline(t)
	defer cancel()
	if err = c.WaitConnected(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		f := peerNext(t, p)
		if f.Type != websocket.TextMessage || !strings.Contains(string(f.Data), "subscribe") {
			t.Fatalf("missing replay %d: %+v", i, f)
		}
	}
}

func TestRepairInitialProbePrecedesPressureReplay(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 1, WriterMaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Publish("/probe", "int", nil, PublisherOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		defer func() { e.session = nil }()
		e.epoch = 1
		e.session = &session{jobs: make(chan writeJob, 1), writerItems: 1, writerBytes: 1024}
		e.session.jobs <- writeJob{size: 1024}
		e.registering = true
		e.replay = append(e.replay, writeJob{kind: websocket.BinaryMessage, probe: true, size: 32})
		e.replayBytes = 32
		c.flush(e)
		if e.registrationPub != 0 || len(e.replay) != 1 {
			return ErrProtocol
		}
		<-e.session.jobs
		e.session.writerBytes, e.session.writerItems = 0, 0
		c.flush(e)
		if len(e.session.jobs) != 1 || !(<-e.session.jobs).probe {
			return ErrProtocol
		}
		e.session.writerBytes, e.session.writerItems = 0, 0
		c.flush(e)
		if len(e.session.jobs) != 1 || !strings.Contains(string((<-e.session.jobs).data), `"publish"`) {
			return ErrProtocol
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRepairOSSocketWriteDeadlineWithChurn(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux TCP socket pressure fixture")
	}
	peer := testpeer.New(protocol40)
	defer peer.Close()
	host, port := peer.Address()
	c, err := NewClient(ClientOptions{ServerAddress: host, Port: port, ClientName: "socket-pressure", WriterCapacity: 4, WriterMaxBytes: 256 << 10, MaxBinaryBytes: 128 << 10, KeepaliveInterval: 10 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 350 * time.Millisecond, RetryMin: 20 * time.Millisecond, RetryMax: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	pub, err := c.Publish("/socket-pressure", "raw", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)
	payload := make([]byte, 96<<10)
	var churn int
	end := time.Now().Add(9 * time.Second)
	for time.Now().Before(end) {
		payload[0]++
		if err := pub.TrySet(payload); err != nil && !errors.Is(err, ErrQueueFull) {
			t.Fatal(err)
		}
		if churn%16 == 0 {
			s, err := c.Subscribe([]string{"/socket-pressure"}, SubscriptionOptions{BufferCapacity: 2, BufferMaxBytes: 256})
			if err != nil && !errors.Is(err, ErrQueueFull) {
				t.Fatal(err)
			}
			if s != nil {
				if err := s.Close(); err != nil && !errors.Is(err, ErrQueueFull) {
					t.Fatal(err)
				}
			}
		}
		churn++
		status := c.Status()
		if status.WriterItems > 4 || status.WriterBytes > 256<<10 || status.CommandBytes > uint64(c.opts.CommandMaxBytes) || status.OfflineBytes > uint64(c.opts.TotalOfflineMaxBytes) {
			t.Fatalf("unbounded pressure: %+v", status)
		}
		if len(peer.Frames) == cap(peer.Frames) && status.LastLoss != nil && strings.Contains(status.LastLoss.Error(), "timeout") {
			break
		}
		if churn%32 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	if len(peer.Frames) != cap(peer.Frames) || c.Status().LastLoss == nil || !strings.Contains(c.Status().LastLoss.Error(), "timeout") {
		t.Fatalf("no confirmed OS write deadline: frames=%d/%d churn=%d status=%+v", len(peer.Frames), cap(peer.Frames), churn, c.Status())
	}
	fresh := peerConn(t, peer)
	found := false
	for i := 0; i < cap(peer.Frames)+32; i++ {
		f := peerNext(t, peer)
		if f.Type != websocket.BinaryMessage {
			continue
		}
		var probe wire.Frame
		if err := wire.WalkFrames(f.Data, func(v wire.Frame) error { probe = v; return nil }); err != nil {
			t.Fatal(err)
		}
		if probe.TopicID != -1 {
			continue
		}
		b, err := wire.EncodeValue(-1, 10000000, wire.DataTypeInt, probe.Value)
		if err != nil {
			t.Fatal(err)
		}
		if err := testpeer.Write(fresh, websocket.BinaryMessage, b); err != nil {
			t.Fatal(err)
		}
		found = true
		break
	}
	if !found {
		t.Fatal("reconnect probe stalled behind old socket")
	}
	until := time.Now().Add(2 * time.Second)
	for !c.Status().Ready && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if !c.Status().Ready || c.Status().Epoch < 2 {
		t.Fatalf("reconnect did not recover: %+v", c.Status())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRepairStalledSocketBoundedAndJoined(t *testing.T) {
	before := runtime.NumGoroutine()
	peer := testpeer.New(protocol40)
	defer peer.Close()
	host, port := peer.Address()
	c, err := NewClient(ClientOptions{ServerAddress: host, Port: port, ClientName: "stalled-socket", WriterCapacity: 4, WriterMaxBytes: 128 << 10, MaxBinaryBytes: 128 << 10, KeepaliveInterval: time.Second, ReadTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	pub, err := c.Publish("/stalled", "raw", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)
	payload := make([]byte, 64<<10)
	end := time.Now().Add(8 * time.Second)
	for i := 0; i < 20000 && len(peer.Frames) < cap(peer.Frames) && time.Now().Before(end); i++ {
		payload[0] = byte(i)
		if i%16 == 0 {
			time.Sleep(time.Millisecond)
		}
		if err = pub.Set(payload); err != nil && !errors.Is(err, ErrQueueFull) {
			t.Fatal(err)
		}
		s := c.Status()
		if s.WriterItems > 4 || s.WriterBytes > 128<<10 || s.CommandBytes > uint64(c.opts.CommandMaxBytes) {
			t.Fatalf("unbounded socket pressure: %+v", s)
		}
	}
	if len(peer.Frames) != cap(peer.Frames) {
		t.Fatalf("peer never stopped draining socket: frames %d/%d", len(peer.Frames), cap(peer.Frames))
	}
	for i := 0; i < 2000; i++ {
		payload[0] = byte(i)
		if err = pub.Set(payload); err != nil && !errors.Is(err, ErrQueueFull) {
			t.Fatal(err)
		}
		if s := c.Status(); s.WriterItems > 4 || s.WriterBytes > 128<<10 {
			t.Fatalf("writer exceeded capacity: %+v", s)
		}
	}
	conn.Close()
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	peer.Close()
	if n := runtime.NumGoroutine(); n > before+8 {
		t.Fatalf("workers after terminal close: %d before %d", n, before)
	}
}

func TestRepairSustainedReconnectAndSubscriptionChurn(t *testing.T) {
	before := runtime.NumGoroutine()
	peer, c := peerClient(t, protocol40)
	pub, err := c.Publish("churn", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for cycle := 0; cycle < 16; cycle++ {
		conn := peerConn(t, peer)
		for {
			f := peerNext(t, peer)
			if f.Type != websocket.BinaryMessage {
				continue
			}
			var probe wire.Frame
			if err := wire.WalkFrames(f.Data, func(v wire.Frame) error { probe = v; return nil }); err != nil {
				t.Fatal(err)
			}
			if probe.TopicID != -1 {
				continue
			}
			b, err := wire.EncodeValue(-1, 10000000, wire.DataTypeInt, probe.Value)
			if err != nil {
				t.Fatal(err)
			}
			if err = testpeer.Write(conn, websocket.BinaryMessage, b); err != nil {
				t.Fatal(err)
			}
			break
		}
		for i := 0; i < 30; i++ {
			sub, err := c.Subscribe([]string{"churn"}, SubscriptionOptions{BufferCapacity: 2, BufferMaxBytes: 256})
			if err != nil {
				t.Fatal(err)
			}
			if err = pub.Set(int64(i + cycle*30)); err != nil {
				t.Fatal(err)
			}
			if err = sub.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if c.Status().OfflineItems != 0 {
			t.Fatalf("unexpected offline history: %+v", c.Status())
		}
		conn.Close()
		deadline := time.Now().Add(2 * time.Second)
		for c.Status().Epoch <= uint64(cycle+1) && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	peer.Close()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+8 && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+8 {
		t.Fatalf("workers remain: before=%d after=%d", before, n)
	}
}

func TestHostContentionLatencyProfile(t *testing.T) {
	peer, c := peerClient(t, protocol40)
	sub, err := c.Subscribe([]string{"host/inbound"}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 16, BufferMaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	pub, err := c.Publish("host/outbound", "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)
	for i := 0; i < 2; i++ {
		peerNext(t, peer)
	}
	sendControl(t, conn, `[{"method":"announce","params":{"name":"host/inbound","id":7,"type":"int","properties":{}}}]`)
	select {
	case <-sub.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("announce missing")
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const samples = 300
	latencies := make([]time.Duration, 0, samples)
	start := time.Now()
	publishing := make(chan error, 1)
	go func() {
		for i := 0; i < samples; i++ {
			if err := pub.Set(int64(i)); err != nil {
				publishing <- err
				return
			}
		}
		publishing <- nil
	}()
	for i := 0; i < samples; i++ {
		sent := time.Now()
		sendValue(t, conn, 7, int64(i+1), int64(i))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		select {
		case ev := <-sub.Events():
			if ev.Kind != ValueReceived || ev.Sample.Value != int64(i) {
				cancel()
				t.Fatalf("sample %d: %+v", i, ev)
			}
			latencies = append(latencies, time.Since(sent))
		case <-ctx.Done():
			cancel()
			t.Fatal("bounded consumer stalled")
		}
		cancel()
	}
	if err := <-publishing; err != nil {
		t.Fatal(err)
	}
	duration := time.Since(start)
	runtime.ReadMemStats(&after)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	rss := int64(-1)
	if data, err := os.ReadFile("/proc/self/statm"); err == nil {
		var pages, resident uint64
		if _, err = fmt.Sscanf(string(data), "%d %d", &pages, &resident); err == nil {
			rss = int64(resident) * int64(os.Getpagesize())
		}
	}
	t.Logf("host samples=%d throughput=%.0f/s p50=%s p99=%s allocs/sample=%.1f bytes/sample=%.0f rss_bytes=%d sys_bytes=%d dropped=%d", samples, float64(samples)/duration.Seconds(), latencies[samples/2], latencies[samples*99/100], float64(after.Mallocs-before.Mallocs)/samples, float64(after.TotalAlloc-before.TotalAlloc)/samples, rss, after.Sys, c.Status().Dropped)
}

func TestRepairContinuousHostResourceProfile(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux /proc measurement")
	}
	peer := testpeer.New(protocol40)
	defer peer.Close()
	host, port := peer.Address()
	c, err := NewClient(ClientOptions{ServerAddress: host, Port: port, ClientName: "host-profile", ReadTimeout: 10 * time.Second, KeepaliveInterval: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sub, err := c.Subscribe([]string{"profile"}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 16, BufferMaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)
	peerNext(t, peer)
	sendControl(t, conn, `[{"method":"announce","params":{"name":"profile","id":1,"type":"int","properties":{}}}]`)
	select {
	case <-sub.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("announce missing")
	}
	beforeRSS, beforeSwitches := hostCounters()
	const count = 200
	for i := 0; i < count; i++ {
		sendValue(t, conn, 1, int64(i+1), int64(i))
		select {
		case ev := <-sub.Events():
			if ev.Kind != ValueReceived || ev.Sample.Value != int64(i) {
				t.Fatalf("bad sample %d: %+v", i, ev)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for event")
		}
	}
	afterRSS, afterSwitches := hostCounters()
	t.Logf("continuous profile: count=%d rss_before=%d rss_after=%d switches_before=%d switches_after=%d", count, beforeRSS, afterRSS, beforeSwitches, afterSwitches)
}
