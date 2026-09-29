package nt4

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestConvenienceLifecycle(t *testing.T) {
	var connected atomic.Bool
	var disconnected atomic.Bool

	c, err := NewClientBuilder().
		Server("127.0.0.1").
		Identity("test-lifecycle").
		OnConnect(func() { connected.Store(true) }).
		OnDisconnect(func() { disconnected.Store(true) }).
		Logger(NewSilentLogger()).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if c.IsConnected() {
		t.Error("expected IsConnected to be false before connect")
	}

	if err := c.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	c.Disconnect()

	if c.IsConnected() {
		t.Error("expected IsConnected to be false after disconnect")
	}

	// Double disconnect should be safe
	c.Disconnect()

	// Test WaitForConnection with canceled/timeout context
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := c.WaitForConnection(timeoutCtx); err == nil {
		t.Error("expected error on disconnected client WaitForConnection")
	}

	// Test ConnectWithRetry with canceled context
	canceledCtx, cancel2 := context.WithCancel(context.Background())
	cancel2()
	c2, _ := NewClientBuilder().Server("127.0.0.1").Build()
	defer c2.Disconnect()
	if err := c2.ConnectWithRetry(canceledCtx); err == nil {
		t.Error("expected error on canceled context ConnectWithRetry")
	}
}

func TestConveniencePublishers(t *testing.T) {
	c, err := NewClientBuilder().Server("127.0.0.1").Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	defer c.Disconnect()

	// Typed publishers
	pBool, err := c.PublishBoolean("/test/bool", true)
	if err != nil {
		t.Fatal(err)
	}
	pDouble, err := c.PublishDouble("/test/double", 3.14)
	if err != nil {
		t.Fatal(err)
	}
	pInt, err := c.PublishInt("/test/int", 42)
	if err != nil {
		t.Fatal(err)
	}
	pFloat, err := c.PublishFloat("/test/float", 1.23)
	if err != nil {
		t.Fatal(err)
	}
	pStr, err := c.PublishString("/test/str", "hello")
	if err != nil {
		t.Fatal(err)
	}
	pDoubleArr, err := c.PublishDoubleArray("/test/darr", []float64{1.1, 2.2})
	if err != nil {
		t.Fatal(err)
	}
	pStrArr, err := c.PublishStringArray("/test/sarr", []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	pBoolArr, err := c.PublishBooleanArray("/test/barr", []bool{true, false})
	if err != nil {
		t.Fatal(err)
	}
	pIntArr, err := c.PublishIntArray("/test/iarr", []int64{10, 20})
	if err != nil {
		t.Fatal(err)
	}
	pFloatArr, err := c.PublishFloatArray("/test/farr", []float32{1.5, 2.5})
	if err != nil {
		t.Fatal(err)
	}
	pRaw, err := c.PublishRaw("/test/raw", []byte{0xDE, 0xAD})
	if err != nil {
		t.Fatal(err)
	}

	// Repeated publish to the same topic reuses convenience publisher
	pDouble2, err := c.PublishDouble("/test/double", 6.28)
	if err != nil {
		t.Fatal(err)
	}
	if pDouble != pDouble2 {
		t.Error("expected reused publisher handle for same topic")
	}

	// SetValue with *Publisher
	if err := c.SetValue(pDouble, 9.99); err != nil {
		t.Errorf("SetValue(*Publisher) failed: %v", err)
	}

	// SetValue with topic name string
	if err := c.SetValue("/test/double", 10.0); err != nil {
		t.Errorf("SetValue(string) failed: %v", err)
	}

	// SetValue with *Topic
	top := &Topic{Name: "/test/double", Type: TypeDouble, Publisher: pDouble}
	if err := c.SetValue(top, 11.0); err != nil {
		t.Errorf("SetValue(*Topic) failed: %v", err)
	}

	// PublishTopic and Unpublish
	customPub, err := c.PublishTopic("/test/custom", TypeString, map[string]any{"retained": true})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Unpublish(customPub); err != nil {
		t.Errorf("Unpublish failed: %v", err)
	}

	_ = pBool
	_ = pInt
	_ = pFloat
	_ = pStr
	_ = pDoubleArr
	_ = pStrArr
	_ = pBoolArr
	_ = pIntArr
	_ = pFloatArr
	_ = pRaw
}

func TestConvenienceGettersAndRetrieve(t *testing.T) {
	c, err := NewClientBuilder().Server("127.0.0.1").Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	defer c.Disconnect()

	// Initial publish stores into client's local cache
	if _, err := c.PublishDouble("/get/double", 42.5); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishBoolean("/get/bool", true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishInt("/get/int", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishFloat("/get/float", 3.14); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishString("/get/string", "robot"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishDoubleArray("/get/darr", []float64{1.0, 2.0}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishStringArray("/get/sarr", []string{"x", "y"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishBooleanArray("/get/barr", []bool{true}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishIntArray("/get/iarr", []int64{99}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishRaw("/get/raw", []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}

	timeout := 50 * time.Millisecond

	// Test getters with cached values
	if v, ok := c.GetDouble("/get/double", timeout); !ok || v != 42.5 {
		t.Errorf("GetDouble = (%v, %v), want (42.5, true)", v, ok)
	}
	if v, ok := c.GetBoolean("/get/bool", timeout); !ok || !v {
		t.Errorf("GetBoolean = (%v, %v), want (true, true)", v, ok)
	}
	if v, ok := c.GetInt("/get/int", timeout); !ok || v != 100 {
		t.Errorf("GetInt = (%v, %v), want (100, true)", v, ok)
	}
	if v, ok := c.GetFloat("/get/float", timeout); !ok || v < 3.13 || v > 3.15 {
		t.Errorf("GetFloat = (%v, %v), want (~3.14, true)", v, ok)
	}
	if v, ok := c.GetString("/get/string", timeout); !ok || v != "robot" {
		t.Errorf("GetString = (%v, %v), want (robot, true)", v, ok)
	}
	if v, ok := c.GetDoubleArray("/get/darr", timeout); !ok || len(v) != 2 {
		t.Errorf("GetDoubleArray = (%v, %v), want 2 items", v, ok)
	}
	if v, ok := c.GetStringArray("/get/sarr", timeout); !ok || len(v) != 2 {
		t.Errorf("GetStringArray = (%v, %v), want 2 items", v, ok)
	}
	if v, ok := c.GetBooleanArray("/get/barr", timeout); !ok || len(v) != 1 {
		t.Errorf("GetBooleanArray = (%v, %v), want 1 item", v, ok)
	}
	if v, ok := c.GetIntArray("/get/iarr", timeout); !ok || len(v) != 1 {
		t.Errorf("GetIntArray = (%v, %v), want 1 item", v, ok)
	}
	if v, ok := c.GetRaw("/get/raw", timeout); !ok || len(v) != 3 {
		t.Errorf("GetRaw = (%v, %v), want 3 bytes", v, ok)
	}

	// Non-existent topic returns timeout false
	if _, ok := c.GetDouble("/nonexistent/value", 10*time.Millisecond); ok {
		t.Error("expected timeout on non-existent topic")
	}
}

func TestSubscriptionCallbackAndUpdates(t *testing.T) {
	peer, client := peerClient(t, protocol41)
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	readyPeer(t, peer, conn)

	waitState(t, client, StateOnlineReady)

	sub, err := client.SubscribeWithPrefix("/sensor/", nil)
	if err != nil {
		t.Fatalf("SubscribeWithPrefix failed: %v", err)
	}
	defer sub.Close()

	var receivedValue atomic.Value
	receivedCh := make(chan struct{}, 1)

	sub.SetCallback(func(topic *Topic, timestamp int64, value any) {
		receivedValue.Store(value)
		select {
		case receivedCh <- struct{}{}:
		default:
		}
	})

	if sub.GetCallback() == nil {
		t.Error("expected non-nil GetCallback")
	}

	// Announce and send value from peer
	sendControl(t, conn, `[{"method":"announce","params":{"name":"/sensor/temp","id":10,"type":"int","properties":{}}}]`)
	sendValue(t, conn, 10, 10000000, int64(99))

	select {
	case <-receivedCh:
		if v, ok := receivedValue.Load().(int64); !ok || v != 99 {
			t.Errorf("callback received %v, want 99", receivedValue.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for callback")
	}

	// Also verify Updates() channel receives
	select {
	case up := <-sub.Updates():
		if up.Topic.Name != "/sensor/temp" || up.Value != int64(99) {
			t.Errorf("unexpected update: %+v", up)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update channel")
	}

	// Test Unsubscribe helper
	if err := client.Unsubscribe(sub); err != nil {
		t.Errorf("Unsubscribe failed: %v", err)
	}
}

func TestConvenienceTopicSnapshots(t *testing.T) {
	c, err := NewClientBuilder().Server("127.0.0.1").Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	defer c.Disconnect()

	if _, err := c.PublishDouble("/robot/speed", 1.0); err != nil {
		t.Fatal(err)
	}

	top := c.GetTopic("/robot/speed")
	if top == nil {
		t.Fatal("GetTopic returned nil")
	}
	if top.Name != "/robot/speed" || top.Type != TypeDouble {
		t.Errorf("unexpected topic: %+v", top)
	}

	top.UpdateProperties(map[string]any{"persistent": true})
	if top.Properties["persistent"] != true {
		t.Errorf("UpdateProperties failed: %+v", top.Properties)
	}

	topics := c.GetTopics()
	if len(topics) != 1 {
		t.Errorf("GetTopics length = %d, want 1", len(topics))
	}

	snaps := c.Topics()
	if len(snaps) != 1 {
		t.Errorf("Topics length = %d, want 1", len(snaps))
	}

	// Diagnostic getters
	_ = c.GetServerTimeOffset()
	_ = c.GetLastRTT()
}

func TestSubscribeWithOptions(t *testing.T) {
	c, err := NewClientBuilder().Server("127.0.0.1").Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	defer c.Disconnect()

	sub, err := c.SubscribeWithOptions([]string{"/test/"}, SubscribeOptions{
		Prefix:     true,
		Periodic:   100 * time.Millisecond,
		TopicsOnly: false,
	})
	if err != nil {
		t.Fatalf("SubscribeWithOptions failed: %v", err)
	}
	defer sub.Close()
}
