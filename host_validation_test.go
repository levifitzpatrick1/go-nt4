package nt4

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/levifitzpatrick1/go-nt4/internal/testpeer"
	"github.com/levifitzpatrick1/go-nt4/internal/wire"
)

// Explicitly selected by scripts/validation/run-host.sh; never part of the quick suite.
func TestHostValidationCombined(t *testing.T) {
	if os.Getenv("GONT4_HOST_VALIDATE") != "1" {
		t.Skip("opt-in host validation; use scripts/validation/run-host.sh")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("Linux /proc and TCP required")
	}
	duration := hostDuration(t, "GONT4_HOST_DURATION", 300*time.Second)
	idle := hostDuration(t, "GONT4_HOST_IDLE_DURATION", 30*time.Second)
	dir := os.Getenv("GONT4_VALIDATION_OUTPUT_DIR")
	if dir == "" {
		t.Fatal("GONT4_VALIDATION_OUTPUT_DIR required")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(dir, "timeseries.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	peer := testpeer.New(protocol40)
	defer peer.Close()
	host, port := peer.Address()
	opts := ClientOptions{ServerAddress: host, Port: port, ClientName: "host-validation", CommandCapacity: 64, CommandMaxBytes: 1 << 18, InboundCapacity: 64, InboundMaxBytes: 1 << 18, WriterCapacity: 4, WriterMaxBytes: 256 << 10, MaxTextBytes: 64 << 10, MaxBinaryBytes: 128 << 10, TotalOfflineCapacity: 16, TotalOfflineMaxBytes: 256 << 10, RetainedMaxBytes: 1 << 20, MaxSubscriptions: 32, KeepaliveInterval: duration + idle + time.Minute, ReadTimeout: duration + idle + time.Minute, WriteTimeout: 350 * time.Millisecond, RetryMin: 20 * time.Millisecond, RetryMax: 40 * time.Millisecond}
	c, err := NewClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	beforeWorkers := runtime.NumGoroutine()
	defer c.Close()
	pub, err := c.Publish("/host-pressure", "raw", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := c.Subscribe([]string{"host-inbound"}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 32, BufferMaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)
	var epoch uint64
	announce := func(conn *websocket.Conn) {
		sendControl(t, conn, `[{"method":"announce","params":{"name":"host-inbound","id":7,"type":"int","properties":{}}}]`)
		select {
		case ev := <-sub.Events():
			if ev.Kind != Announced {
				t.Fatalf("announce: %+v", ev)
			}
			epoch = ev.Epoch
		case <-time.After(3 * time.Second):
			t.Fatal("announce missing")
		}
	}
	announce(conn)
	// Whole process (including peer/test) idle: not library-only or exact wakeups.
	_, sw0 := hostCounters()
	idleCPU0 := hostCPUSeconds()
	time.Sleep(idle)
	_, sw1 := hostCounters()
	idleCPU1 := hostCPUSeconds()
	var mem0, mem1 runtime.MemStats
	runtime.ReadMemStats(&mem0)
	start := time.Now()
	end := start.Add(duration)
	nextReport := start.Add(5 * time.Second)
	payload := make([]byte, 96<<10)
	hist := make(map[int]uint64)
	var offered, delivered, rejected, churn, cycles, timeouts, gaps uint64
	var high Status
	peakG := runtime.NumGoroutine()
	minG := peakG
	var peakRSS int64
	seq := int64(0)
	report := func(record bool) {
		s := c.Status()
		rss, _ := hostCounters()
		if rss > peakRSS {
			peakRSS = rss
		}
		g := runtime.NumGoroutine()
		if g > peakG {
			peakG = g
		}
		if g < minG {
			minG = g
		}
		high.CommandItems = max(high.CommandItems, s.CommandItems)
		high.CommandBytes = max(high.CommandBytes, s.CommandBytes)
		high.InboundItems = max(high.InboundItems, s.InboundItems)
		high.InboundBytes = max(high.InboundBytes, s.InboundBytes)
		high.WriterItems = max(high.WriterItems, s.WriterItems)
		high.WriterBytes = max(high.WriterBytes, s.WriterBytes)
		high.OfflineItems = max(high.OfflineItems, s.OfflineItems)
		high.OfflineBytes = max(high.OfflineBytes, s.OfflineBytes)
		high.RetainedBytes = max(high.RetainedBytes, s.RetainedBytes)
		if s.CommandItems > uint64(opts.CommandCapacity) || s.CommandBytes > uint64(opts.CommandMaxBytes) || s.InboundItems > uint64(opts.InboundCapacity) || s.InboundBytes > uint64(opts.InboundMaxBytes) || s.WriterItems > uint64(opts.WriterCapacity) || s.WriterBytes > uint64(opts.WriterMaxBytes) || s.OfflineItems > uint64(opts.TotalOfflineCapacity) || s.OfflineBytes > uint64(opts.TotalOfflineMaxBytes) || s.RetainedBytes > uint64(opts.RetainedMaxBytes) {
			t.Fatalf("capacity exceeded: %+v", s)
		}
		if !record {
			return
		}
		if err := enc.Encode(map[string]any{"seconds": time.Since(start).Seconds(), "epoch": s.Epoch, "state": s.State, "offered": offered, "delivered": delivered, "rejected": rejected, "churn": churn, "cycles": cycles, "timeouts": timeouts, "gaps": gaps, "dropped": s.Dropped, "uncertain": s.Uncertain, "writer_items": s.WriterItems, "writer_bytes": s.WriterBytes, "command_items": s.CommandItems, "command_bytes": s.CommandBytes, "inbound_items": s.InboundItems, "inbound_bytes": s.InboundBytes, "offline_items": s.OfflineItems, "offline_bytes": s.OfflineBytes, "retained_bytes": s.RetainedBytes, "goroutines": g, "rss_bytes": rss}); err != nil {
			t.Fatal(err)
		}
	}
	for time.Now().Before(end) {
		// A single sustained run: each epoch must pressure the real TCP write until its deadline.
		old := c.Status().Epoch
		pressureEnd := time.Now().Add(9 * time.Second)
		confirmed := false
		for time.Now().Before(pressureEnd) {
			if time.Now().After(nextReport) {
				report(true)
				nextReport = time.Now().Add(5 * time.Second)
			}
			if s := c.Status(); (s.Epoch > old || s.State == StateBackoff) && s.LastLoss != nil && strings.Contains(s.LastLoss.Error(), "timeout") {
				confirmed = true
				break
			}
			if time.Now().After(end) {
				break
			}
			// Typed inbound stream while the outbound OS socket is under pressure.
			seq++
			sent := time.Now()
			b, e := wire.EncodeValue(7, seq, wire.DataTypeInt, seq)
			if e != nil {
				t.Fatal(e)
			}
			if e = testpeer.Write(conn, websocket.BinaryMessage, b); e != nil {
				if s := c.Status(); s.LastLoss == nil || !strings.Contains(s.LastLoss.Error(), "timeout") {
					t.Fatalf("inbound write before confirmed socket loss: %v status=%+v", e, s)
				}
				confirmed = true
				break
			}
			offered++
			select {
			case ev := <-sub.Events():
				if ev.Kind == LocalInvalidated && ev.Epoch == epoch {
					confirmed = true
					break
				}
				if ev.Kind != ValueReceived || ev.Epoch != epoch || ev.Sample.Value != seq {
					t.Fatalf("stream/epoch gap without loss: seq=%d event=%+v", seq, ev)
				}
				delivered++
				d := time.Since(sent)
				hist[int(math.Log1p(float64(d.Nanoseconds()))/math.Log(1.05))]++
			case <-time.After(2 * time.Second):
				t.Fatal("inbound stalled under outgoing pressure")
			}
			if confirmed {
				break
			}
			for j := 0; j < 2; j++ {
				payload[0]++
				err := pub.TrySet(payload)
				if errors.Is(err, ErrQueueFull) {
					rejected++
				} else if err != nil {
					t.Fatal(err)
				}
			}
			if churn%3 == 0 {
				s, e := c.Subscribe([]string{"host-inbound"}, SubscriptionOptions{BufferCapacity: 2, BufferMaxBytes: 256})
				if e == nil {
					if e = s.Close(); e != nil && !errors.Is(e, ErrQueueFull) {
						t.Fatal(e)
					}
				} else if !errors.Is(e, ErrQueueFull) {
					t.Fatal(e)
				}
			}
			churn++
			report(false)
			time.Sleep(2 * time.Millisecond)
		}
		if !confirmed {
			if time.Now().After(end) {
				break
			}
			t.Fatalf("no actual socket write timeout in epoch %d; frames=%d/%d status=%+v", old, len(peer.Frames), cap(peer.Frames), c.Status())
		}
		if s := c.Status(); s.LastLoss == nil || !strings.Contains(s.LastLoss.Error(), "timeout") || (s.Epoch == old && s.State != StateBackoff) {
			t.Fatalf("not an OS write deadline: %+v", s)
		}
		// The kernel may apply TCP backpressure before the scripted peer's frame
		// channel fills; only the actual write tcp i/o timeout proves the stall.
		timeouts++
		gaps++
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		fresh, connectErr := peer.Connect(ctx)
		cancel()
		if connectErr != nil {
			t.Fatalf("reconnect peer: %v status=%+v frames=%d connections=%d", connectErr, c.Status(), len(peer.Frames), len(peer.Connections))
		}
		found := false
		for j := 0; j < cap(peer.Frames)+64; j++ {
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
			b, e := wire.EncodeValue(-1, 10000000, wire.DataTypeInt, probe.Value)
			if e != nil {
				t.Fatal(e)
			}
			if e = testpeer.Write(fresh, websocket.BinaryMessage, b); e != nil {
				t.Fatal(e)
			}
			found = true
			break
		}
		if !found {
			t.Fatal("reconnect probe blocked by stale frames")
		}
		until := time.Now().Add(3 * time.Second)
		for (!c.Status().Ready || c.Status().Epoch <= old) && time.Now().Before(until) {
			time.Sleep(time.Millisecond)
		}
		if !c.Status().Ready || c.Status().Epoch <= old {
			t.Fatalf("no recovery: %+v", c.Status())
		}
		// Disconnect invalidations may precede the new announcement; never interpret them as remote removal.
		sendControl(t, fresh, `[{"method":"announce","params":{"name":"host-inbound","id":7,"type":"int","properties":{}}}]`)
		until = time.Now().Add(3 * time.Second)
		seen := false
		for !seen && time.Now().Before(until) {
			select {
			case ev := <-sub.Events():
				if ev.Kind == ServerUnannounced {
					t.Fatal("disconnect misreported as remote removal")
				}
				if ev.Kind == Announced && ev.Epoch == c.Status().Epoch {
					epoch = ev.Epoch
					seen = true
				}
			case <-time.After(50 * time.Millisecond):
			}
		}
		if !seen {
			t.Fatal("new epoch announcement missing")
		}
		conn = fresh
		cycles++
		report(true)
	}
	report(true)
	runtime.ReadMemStats(&mem1)
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case e := <-closed:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not join workers")
	}
	peer.Close()
	time.Sleep(50 * time.Millisecond)
	afterWorkers := runtime.NumGoroutine()
	if afterWorkers > beforeWorkers+3 {
		t.Fatalf("library workers leaked: before=%d after=%d", beforeWorkers, afterWorkers)
	}
	if cycles < 3 || timeouts < 3 || delivered < 100 || churn < 100 {
		t.Fatalf("insufficient combined workload: cycles=%d timeouts=%d delivered=%d churn=%d", cycles, timeouts, delivered, churn)
	}
	keys := make([]int, 0, len(hist))
	for k := range hist {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	quant := func(p float64) float64 {
		target := uint64(math.Ceil(float64(delivered) * p))
		var n uint64
		for _, k := range keys {
			n += hist[k]
			if n >= target {
				return (math.Pow(1.05, float64(k+1)) - 1) / 1e6
			}
		}
		return 0
	}
	result := map[string]any{"duration_seconds": time.Since(start).Seconds(), "idle_seconds": idle.Seconds(), "offered": offered, "delivered": delivered, "offered_per_second": float64(offered) / duration.Seconds(), "delivered_per_second": float64(delivered) / duration.Seconds(), "latency_p50_ms_upper": quant(.5), "latency_p99_ms_upper": quant(.99), "latency_histogram_log105_ns": hist, "histogram_upper_relative_error": "<5% plus 1 ns", "socket_write_timeouts": timeouts, "reconnect_cycles": cycles, "epoch_gaps": gaps, "subscription_churn": churn, "admission_rejected": rejected, "status_dropped": c.Status().Dropped, "status_uncertain": c.Status().Uncertain, "queue_high_water": high, "peak_sampled_rss_bytes": peakRSS, "vm_hwm_bytes": hostHWM(), "goroutines_before": beforeWorkers, "goroutines_min": minG, "goroutines_max": peakG, "goroutines_after": afterWorkers, "allocations": mem1.Mallocs - mem0.Mallocs, "allocated_bytes": mem1.TotalAlloc - mem0.TotalAlloc, "idle_process_cpu_seconds": idleCPU1 - idleCPU0, "idle_process_context_switches_proxy": sw1 - sw0, "idle_exact_wakeups": "unavailable: /proc/self/sched does not expose nr_wakeups; perf permissions not assumed"}
	b, e := json.MarshalIndent(result, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "summary.json"), append(b, '\n'), 0644); e != nil {
		t.Fatal(e)
	}
	t.Logf("host validation summary: %s", b)
}
func hostDuration(t *testing.T, key string, def time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, e := time.ParseDuration(v)
	if e != nil || d <= 0 {
		t.Fatalf("%s: invalid duration %q", key, v)
	}
	return d
}
func hostHWM() int64 {
	b, e := os.ReadFile("/proc/self/status")
	if e != nil {
		return 0
	}
	for _, s := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(s, "VmHWM:") {
			fields := strings.Fields(s)
			if len(fields) > 1 {
				v, _ := strconv.ParseInt(fields[1], 10, 64)
				return v * 1024
			}
		}
	}
	return 0
}
func hostCPUSeconds() float64 {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return 0
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
