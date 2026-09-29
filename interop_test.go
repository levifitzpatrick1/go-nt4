//go:build interop

package nt4

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/levifitzpatrick1/go-nt4/internal/wire"
)

type nativeFixture struct {
	t       *testing.T
	cmd     *exec.Cmd
	base    string
	port    int
	stopped bool
}

func startNative(t *testing.T) *nativeFixture { return startNativeAt(t, 0) }
func startNativeAt(t *testing.T, port int) *nativeFixture {
	t.Helper()
	py := os.Getenv("NT4_INTEROP_PYTHON")
	if py == "" {
		t.Fatal("run scripts/interop/run.sh (requires pinned external virtualenv)")
	}
	if port == 0 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port = l.Addr().(*net.TCPAddr).Port
		l.Close()
	}
	script, err := filepath.Abs("scripts/interop/native_server.py")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(py, script, strconv.Itoa(port))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	f := &nativeFixture{t: t, cmd: cmd, port: port}
	t.Cleanup(f.stop)
	line := make(chan string, 1)
	go func() {
		scan := bufio.NewScanner(stdout)
		if scan.Scan() {
			line <- scan.Text()
		} else {
			line <- ""
		}
	}()
	var httpPort string
	select {
	case httpPort = <-line:
	case <-time.After(8 * time.Second):
		t.Fatalf("native startup timeout: %s", stderr.String())
	}
	if httpPort == "" {
		t.Fatalf("native failed: %s", stderr.String())
	}
	f.base = "http://127.0.0.1:" + httpPort
	eventually(t, 5*time.Second, func() bool {
		r, err := http.Get(f.base + "/ready")
		if err != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == 200
	})
	return f
}

func (f *nativeFixture) stop() {
	if f.stopped {
		return
	}
	f.stopped = true
	_ = f.cmd.Process.Kill()
	_ = f.cmd.Wait()
}

func (f *nativeFixture) request(path string, payload map[string]any) map[string]any {
	f.t.Helper()
	b, _ := json.Marshal(payload)
	client := &http.Client{Timeout: 2 * time.Second}
	r, err := client.Post(f.base+path, "application/json", bytes.NewReader(b))
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(r.Body).Decode(&out); err != nil {
		f.t.Fatal(err)
	}
	if r.StatusCode != 200 {
		f.t.Fatalf("native %s: %v", path, out)
	}
	return out
}

func eventually(t *testing.T, d time.Duration, pred func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if pred() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("native interoperability observation timed out")
}

func (f *nativeFixture) client(t *testing.T) *Client {
	t.Helper()
	c, err := NewClient(ClientOptions{ServerAddress: "127.0.0.1", Port: f.port, ClientName: "interop", RetryMin: 50 * time.Millisecond, RetryMax: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("native sync: %v status=%+v", err, c.Status())
	}
	return c
}

func TestInteropGoValuesObservedByNative(t *testing.T) {
	cases := []struct {
		typ      string
		val      any
		expected any
	}{
		{"boolean", true, true}, {"double", float64(2.25), float64(2.25)}, {"int", int64(123), float64(123)},
		{"float", float32(1.5), float64(1.5)}, {"string", "hello", "hello"}, {"json", "{\"x\":1}", "{\"x\":1}"},
		{"raw", []byte{0, 2, 255}, []any{float64(0), float64(2), float64(255)}},
		{"boolean[]", []bool{true, false}, []any{true, false}}, {"double[]", []float64{1.25, 2}, []any{1.25, float64(2)}},
		{"int[]", []int64{1, -3}, []any{float64(1), float64(-3)}}, {"float[]", []float32{1.5, 2.5}, []any{1.5, 2.5}}, {"string[]", []string{"a", "b"}, []any{"a", "b"}},
		{"custom/type", []byte{1, 3}, []any{float64(1), float64(3)}},
	}
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			f := startNative(t)
			name := "/interop/" + tc.typ
			f.request("/watch", map[string]any{"name": name, "type": tc.typ})
			c := f.client(t)
			p, err := c.Publish(name, tc.typ, nil, PublisherOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.Set(tc.val); err != nil {
				t.Fatal(err)
			}
			eventually(t, 5*time.Second, func() bool {
				obs := f.request("/inspect", map[string]any{"name": name})
				a, _ := json.Marshal(obs["value"])
				b, _ := json.Marshal(tc.expected)
				return obs["type"] == tc.typ && bytes.Equal(a, b)
			})
		})
	}
}

func TestInteropNativeValuesAndProperties(t *testing.T) {
	f := startNative(t)
	c := f.client(t)
	name := "/interop/native"
	s, err := c.Subscribe([]string{name}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f.request("/publish", map[string]any{"name": name, "type": "string[]"})
	f.request("/set", map[string]any{"name": name, "type": "string[]", "value": []string{"mjpeg:http://camera", "backup"}})
	eventually(t, 5*time.Second, func() bool {
		v, ok := c.Latest(name)
		return ok && fmt.Sprint(v.Value) == "[mjpeg:http://camera backup]"
	})
	f.request("/property", map[string]any{"name": name, "update": map[string]any{"nested": map[string]any{"x": float64(1)}}})
	eventually(t, 5*time.Second, func() bool { v, ok := c.Topic(name); return ok && v.Properties["nested"] != nil })
	f.request("/property", map[string]any{"name": name, "update": map[string]any{"nested": nil}})
	eventually(t, 5*time.Second, func() bool { v, ok := c.Topic(name); return ok && v.Properties["nested"] == nil })
	f.request("/delete", map[string]any{"name": name})
	eventually(t, 5*time.Second, func() bool { v, ok := c.Topic(name); return !ok || !v.HasID })
}

func TestInteropNativeRawPropertyAckShape(t *testing.T) {
	f := startNative(t)
	uri := fmt.Sprintf("ws://127.0.0.1:%d/nt/ack-shape", f.port)
	conn, _, err := (&websocket.Dialer{Subprotocols: []string{protocol41}, HandshakeTimeout: 3 * time.Second}).Dial(uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if conn.Subprotocol() != protocol41 {
		t.Fatalf("native protocol=%q", conn.Subprotocol())
	}
	probe, err := wire.EncodeTimeSyncRequest(12345)
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.WriteMessage(websocket.BinaryMessage, probe); err != nil {
		t.Fatal(err)
	}
	name := "/interop/native/raw-ack"
	subscribe := jobControl("subscribe", wire.Subscribe([]string{name}, 2, map[string]any{}).Params)
	if err = conn.WriteMessage(websocket.TextMessage, subscribe.data); err != nil {
		t.Fatal(err)
	}
	publish := jobControl("publish", map[string]any{"name": name, "pubuid": 1, "type": "int", "properties": map[string]any{}})
	if err = conn.WriteMessage(websocket.TextMessage, publish.data); err != nil {
		t.Fatal(err)
	}
	if err = conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	announce := false
	for !announce {
		kind, b, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if kind != websocket.TextMessage {
			continue
		}
		var msgs []struct {
			Method string `json:"method"`
			Params struct {
				Name   string `json:"name"`
				PubUID *int   `json:"pubuid"`
			} `json:"params"`
		}
		if err = json.Unmarshal(b, &msgs); err != nil {
			t.Fatal(err)
		}
		for _, msg := range msgs {
			if msg.Method == "announce" && msg.Params.Name == name {
				if msg.Params.PubUID == nil || *msg.Params.PubUID != 1 {
					t.Fatalf("publish announcement UID: %s", b)
				}
				announce = true
			}
		}
	}
	j := jobControl("setproperties", map[string]any{"name": name, "update": map[string]any{"review": true}})
	if err = conn.WriteMessage(websocket.TextMessage, j.data); err != nil {
		t.Fatal(err)
	}
	for {
		kind, b, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if kind != websocket.TextMessage {
			continue
		}
		var msgs []struct {
			Method string `json:"method"`
			Params struct {
				Name   string         `json:"name"`
				Ack    *bool          `json:"ack"`
				Update map[string]any `json:"update"`
			} `json:"params"`
		}
		if err = json.Unmarshal(b, &msgs); err != nil {
			t.Fatal(err)
		}
		for _, msg := range msgs {
			if msg.Method == "properties" && msg.Params.Name == name && msg.Params.Update["review"] == true {
				if msg.Params.Ack == nil {
					t.Logf("pinned ntcore 2026.2.2 omits ack on this property update: %s", b)
				} else if !*msg.Params.Ack {
					t.Fatalf("native sent explicit ack:false: %s", b)
				}
				return
			}
		}
	}
}

func TestInteropNativePropertyEnvelopeAndPublisherMetadata(t *testing.T) {
	f := startNative(t)
	c := f.client(t)
	name := "/interop/metadata/ack"
	sub, err := c.Subscribe([]string{name}, SubscriptionOptions{BufferCapacity: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	meta, err := c.Subscribe([]string{"$pub$" + name}, SubscriptionOptions{BufferCapacity: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()
	p, err := c.Publish(name, "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, func() bool { topic, ok := c.Topic(name); return ok && topic.HasID })
	eventually(t, 5*time.Second, func() bool {
		v, ok := c.Latest("$pub$" + name)
		if !ok {
			return false
		}
		b, ok := v.Value.([]byte)
		return ok && len(b) > 0
	})
	if err = c.SetProperties(name, map[string]any{"review": true}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-sub.Events():
			if ev.Kind == PropertiesChanged && ev.Topic.Properties["review"] == true {
				if _, ok := ev.Topic.Properties["ack"]; ok {
					t.Fatalf("ack leaked into properties: %+v", ev)
				}
				_ = p
				return
			}
		case <-deadline:
			t.Fatalf("native property event not observed; status=%+v topic=%+v", c.Status(), func() TopicSnapshot { s, _ := c.Topic(name); return s }())
		}
	}
}

func TestInteropPropertiesAckAndConflict(t *testing.T) {
	f := startNative(t)
	c := f.client(t)
	name := "/interop/properties"
	s, err := c.Subscribe([]string{name}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 32})
	if err != nil {
		t.Fatal(err)
	}
	f.request("/publish", map[string]any{"name": name, "type": "double"})
	eventually(t, 5*time.Second, func() bool { v, ok := c.Topic(name); return ok && v.HasID })
	if _, err := c.Publish(name, "string", nil, PublisherOptions{}); !errors.Is(err, ErrTypeConflict) {
		t.Fatalf("conflict=%v", err)
	}
	if err := c.SetProperties(name, map[string]any{"nested": map[string]any{"answer": float64(42)}}); err != nil {
		t.Fatal(err)
	}
	seenUpdate := false
	eventually(t, 5*time.Second, func() bool {
		for {
			select {
			case e := <-s.Events():
				if e.Kind == PropertiesChanged && e.Topic.Properties["nested"] != nil {
					seenUpdate = true
				}
			default:
				goto done
			}
		}
	done:
		obs := f.request("/inspect", map[string]any{"name": name})
		return seenUpdate && obs["properties"].(map[string]any)["nested"] != nil
	})
	if err := c.SetProperties(name, map[string]any{"nested": nil}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, func() bool {
		obs := f.request("/inspect", map[string]any{"name": name})
		return obs["properties"].(map[string]any)["nested"] == nil
	})
}

func TestInteropReconnectFreshNativeProcess(t *testing.T) {
	f := startNative(t)
	c := f.client(t)
	name := "/interop/reboot"
	p, err := c.Publish(name, "int", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.request("/watch", map[string]any{"name": name, "type": "int"})
	if err := p.Set(int64(77)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, func() bool { return f.request("/inspect", map[string]any{"name": name})["value"] == float64(77) })
	oldEpoch := c.Status().Epoch
	port := f.port
	f.stop()
	eventually(t, 5*time.Second, func() bool { return c.Status().State == StateBackoff || c.Status().Epoch > oldEpoch })
	fresh := startNativeAt(t, port)
	fresh.request("/watch", map[string]any{"name": name, "type": "int"})
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	eventually(t, 6*time.Second, func() bool {
		return c.Status().Epoch > oldEpoch && fresh.request("/inspect", map[string]any{"name": name})["value"] == float64(77)
	})
}

func TestInteropCameraFMSAndOverlappingFilters(t *testing.T) {
	f := startNative(t)
	c := f.client(t)
	fms := "/FMSInfo/FMSControlData"
	cam := "/CameraPublisher/front/streams"
	exact, err := c.Subscribe([]string{fms}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 32})
	if err != nil {
		t.Fatal(err)
	}
	cameras, err := c.Subscribe([]string{"/CameraPublisher/"}, SubscriptionOptions{Prefix: true, All: true, Mode: DeliveryAll, BufferCapacity: 32})
	if err != nil {
		t.Fatal(err)
	}
	topics, err := c.Subscribe([]string{"/CameraPublisher/"}, SubscriptionOptions{Prefix: true, TopicsOnly: true, BufferCapacity: 32})
	if err != nil {
		t.Fatal(err)
	}
	f.request("/publish", map[string]any{"name": fms, "type": "int"})
	f.request("/publish", map[string]any{"name": cam, "type": "string[]"})
	f.request("/set", map[string]any{"name": fms, "type": "int", "value": 32})
	f.request("/set", map[string]any{"name": cam, "type": "string[]", "value": []string{"mjpeg:http://camera/stream"}})
	eventually(t, 5*time.Second, func() bool { v, ok := c.Latest(cam); return ok && len(v.Value.([]string)) == 1 })
	eventually(t, 5*time.Second, func() bool { v, ok := c.Latest(fms); return ok && v.Value == int64(32) })
	sawFMS, sawCamera, sawTopic := false, false, false
	eventually(t, 5*time.Second, func() bool {
		for _, part := range []struct {
			s     *Subscription
			found *bool
			kind  EventKind
		}{{exact, &sawFMS, ValueReceived}, {cameras, &sawCamera, ValueReceived}, {topics, &sawTopic, Announced}} {
			for {
				select {
				case ev := <-part.s.Events():
					if ev.Kind == part.kind {
						*part.found = true
					}
					if part.s == topics && ev.Kind == ValueReceived {
						t.Fatal("topics-only received value")
					}
				default:
					goto next
				}
			}
		next:
		}
		return sawFMS && sawCamera && sawTopic
	})
	f.request("/set", map[string]any{"name": cam, "type": "string[]", "value": []string{"mjpeg:http://camera/new"}})
	eventually(t, 5*time.Second, func() bool { v, ok := c.Latest(cam); return ok && v.Value.([]string)[0] == "mjpeg:http://camera/new" })
}

func TestInteropMultiplePublishersWeakAndUncached(t *testing.T) {
	for _, tc := range []struct {
		name  string
		props map[string]any
		weak  bool
	}{
		{"/interop/shared", nil, false}, {"/interop/weak", nil, true}, {"/interop/uncached", map[string]any{"cached": false}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := startNative(t)
			c := f.client(t)
			name := tc.name
			f.request("/watch", map[string]any{"name": name, "type": "int"})
			p, e := c.Publish(name, "int", tc.props, PublisherOptions{})
			if e != nil {
				t.Fatal(e)
			}
			if tc.weak {
				e = p.SetDefault(int64(12))
			} else {
				e = p.Set(int64(12))
			}
			if e != nil {
				t.Fatal(e)
			}
			var last map[string]any
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				last = f.request("/inspect", map[string]any{"name": name})
				if (tc.props != nil && last["queued"] == float64(12)) || (tc.props == nil && last["value"] == float64(12)) {
					break
				}
				if tc.props != nil {
					if err := p.Set(int64(12)); err != nil {
						t.Fatal(err)
					}
				}
				time.Sleep(30 * time.Millisecond)
			}
			if (tc.props != nil && last["queued"] != float64(12)) || (tc.props == nil && last["value"] != float64(12)) {
				t.Fatalf("native observation: %v status=%+v publisher=%v", last, c.Status(), p.Err())
			}
			if tc.props == nil {
				stamp, ok := last["time"].(float64)
				if !ok || (tc.weak && stamp != 0) || (!tc.weak && stamp <= 0) {
					t.Fatalf("native weak/strong timestamp: %v", last)
				}
			}
			if tc.weak {
				eventually(t, 5*time.Second, func() bool {
					v := f.request("/inspect", map[string]any{"name": name})
					return v["properties"].(map[string]any)["retained"] == true
				})
			}
			if tc.props != nil {
				if _, ok := c.Latest(name); ok {
					t.Fatal("cached:false retained local latest")
				}
			}
			if name == "/interop/shared" {
				p2, e := c.Publish(name, "int", nil, PublisherOptions{})
				if e != nil {
					t.Fatal(e)
				}
				if e = p2.Set(int64(99)); e != nil {
					t.Fatal(e)
				}
				eventually(t, 5*time.Second, func() bool { return f.request("/inspect", map[string]any{"name": name})["value"] == float64(99) })
				if e = p.Close(); e != nil {
					t.Fatal(e)
				}
				if e = p2.Set(int64(101)); e != nil {
					t.Fatal(e)
				}
				eventually(t, 5*time.Second, func() bool { return f.request("/inspect", map[string]any{"name": name})["value"] == float64(101) })
			}
		})
	}
}

func TestInteropOfflineLatestAndHistory(t *testing.T) {
	f := startNative(t)
	c, e := NewClient(ClientOptions{Port: f.port, RetryMin: 50 * time.Millisecond, RetryMax: 200 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	latest, e := c.Publish("/interop/offline/latest", "int", nil, PublisherOptions{})
	if e != nil {
		t.Fatal(e)
	}
	history, e := c.Publish("/interop/offline/history", "int", nil, PublisherOptions{OfflineQueueCapacity: 3, OfflineQueueMaxBytes: 100})
	if e != nil {
		t.Fatal(e)
	}
	for i := int64(1); i <= 3; i++ {
		if e = latest.Set(i); e != nil {
			t.Fatal(e)
		}
		if e = history.Set(i); e != nil {
			t.Fatal(e)
		}
	}
	if c.Status().OfflineItems != 3 {
		t.Fatalf("history items=%d", c.Status().OfflineItems)
	}
	for _, name := range []string{"/interop/offline/latest", "/interop/offline/history"} {
		f.request("/watch", map[string]any{"name": name, "type": "int"})
	}
	if e = c.Start(context.Background()); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	if e = c.WaitReady(ctx); e != nil {
		t.Fatal(e)
	}
	eventually(t, 5*time.Second, func() bool {
		return f.request("/inspect", map[string]any{"name": "/interop/offline/latest"})["value"] == float64(3)
	})
	eventually(t, 5*time.Second, func() bool {
		obs := f.request("/inspect", map[string]any{"name": "/interop/offline/history"})
		v, ok := c.Latest("/interop/offline/history")
		return ok && v.Value == int64(3) && len(obs["received"].([]any)) > 0
	})
	eventually(t, 5*time.Second, func() bool { return c.Status().OfflineItems == 0 })
}

func TestInteropNativeTwoPublisherRebootOrders(t *testing.T) {
	for _, strongFirst := range []bool{true, false} {
		label := "weak-first"
		if strongFirst {
			label = "strong-first"
		}
		t.Run(label, func(t *testing.T) {
			f := startNative(t)
			name := "/interop/two/reboot"
			f.request("/watch", map[string]any{"name": name, "type": "int"})
			c := f.client(t)
			a, err := c.Publish(name, "int", nil, PublisherOptions{})
			if err != nil {
				t.Fatal(err)
			}
			b, err := c.Publish(name, "int", nil, PublisherOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if strongFirst {
				err = a.Set(int64(9))
				if err == nil {
					eventually(t, 5*time.Second, func() bool { return f.request("/inspect", map[string]any{"name": name})["value"] == float64(9) })
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
			var observed map[string]any
			until := time.Now().Add(5 * time.Second)
			for time.Now().Before(until) {
				observed = f.request("/inspect", map[string]any{"name": name})
				if observed["value"] == float64(9) {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if observed["value"] != float64(9) {
				t.Fatalf("native before reboot: %v, status=%+v", observed, c.Status())
			}
			eventually(t, 5*time.Second, func() bool { v, ok := c.Latest(name); return ok && v.Value == int64(9) })
			epoch := c.Status().Epoch
			port := f.port
			f.stop()
			eventually(t, 5*time.Second, func() bool { return c.Status().State == StateBackoff || c.Status().Epoch > epoch })
			fresh := startNativeAt(t, port)
			fresh.request("/watch", map[string]any{"name": name, "type": "int"})
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancel()
			if err = c.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			eventually(t, 6*time.Second, func() bool {
				return c.Status().Epoch > epoch && fresh.request("/inspect", map[string]any{"name": name})["value"] == float64(9)
			})
		})
	}
}

func TestInteropNative40Negotiation(t *testing.T) {
	f := startNative(t)
	addr := fmt.Sprintf("ws://127.0.0.1:%d/nt/native40", f.port)
	conn, _, err := (&websocket.Dialer{Subprotocols: []string{protocol40}, HandshakeTimeout: 3 * time.Second}).Dial(addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if conn.Subprotocol() != protocol40 {
		t.Fatalf("negotiated %q not 4.0", conn.Subprotocol())
	}
	defer conn.Close()
	probe, err := wire.EncodeTimeSyncRequest(12345)
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.WriteMessage(websocket.BinaryMessage, probe); err != nil {
		t.Fatal(err)
	}
	if err = conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	kind, body, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if kind != websocket.BinaryMessage {
		t.Fatalf("native 4.0 response kind=%d", kind)
	}
	found := false
	if err = wire.WalkFrames(body, func(v wire.Frame) error {
		if v.TopicID == -1 && v.Value == int64(12345) && v.Timestamp > 0 {
			found = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("native 4.0 missing RTT echo: %x", body)
	}
}

func TestInteropNativeAllTypesToGo(t *testing.T) {
	f := startNative(t)
	c := f.client(t)
	cases := []struct {
		typ  string
		sent any
		want any
	}{
		{"boolean", true, true}, {"double", 2.25, float64(2.25)}, {"int", 123, int64(123)},
		{"float", 1.5, float32(1.5)}, {"string", "hello", "hello"}, {"json", "{\"x\":1}", "{\"x\":1}"},
		{"raw", []int{0, 2, 255}, []byte{0, 2, 255}},
		{"boolean[]", []bool{true, false}, []bool{true, false}},
		{"double[]", []float64{1.25, 2}, []float64{1.25, 2}},
		{"int[]", []int64{1, -3}, []int64{1, -3}},
		{"float[]", []float32{1.5, 2.5}, []float32{1.5, 2.5}},
		{"string[]", []string{"a", "b"}, []string{"a", "b"}},
		{"custom/type", []int{1, 3}, []byte{1, 3}},
	}
	for i, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			name := fmt.Sprintf("/interop/native/all/%d", i)
			sub, err := c.Subscribe([]string{name}, SubscriptionOptions{BufferCapacity: 8})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			f.request("/publish", map[string]any{"name": name, "type": tc.typ})
			eventually(t, 5*time.Second, func() bool { s, ok := c.Topic(name); return ok && s.HasID && s.Type == tc.typ })
			eventually(t, 5*time.Second, func() bool {
				f.request("/set", map[string]any{"name": name, "type": tc.typ, "value": tc.sent})
				s, ok := c.Latest(name)
				return ok && reflect.DeepEqual(s.Value, tc.want)
			})
		})
	}
}
