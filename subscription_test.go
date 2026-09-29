package nt4

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/levifitzpatrick1/go-nt4/internal/testpeer"
)

func TestSubscriptionByteAndMetadataCutoff(t *testing.T) {
	c, _ := NewClient(ClientOptions{})
	defer c.Close()
	all, _ := c.Subscribe([]string{"/camera"}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 8, BufferMaxBytes: 240})
	latest, _ := c.Subscribe([]string{"/camera"}, SubscriptionOptions{BufferCapacity: 8, BufferMaxBytes: 240})
	deliver := func(ev Event) {
		t.Helper()
		if err := c.call(func(e *engine) error {
			if e.topics["/camera"] == nil {
				e.topics["/camera"] = &topicRecord{name: "/camera"}
				e.rebuildRoutes()
			}
			e.route(ev)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	metadata := Event{Kind: Announced, Topic: TopicSnapshot{Name: "/camera", Properties: map[string]any{"nested": map[string]any{"key": "owned"}}}}
	deliver(metadata)
	deliver(Event{Kind: ValueReceived, Topic: TopicSnapshot{Name: "/camera"}, Sample: Sample{Value: strings.Repeat("x", 500)}})
	for _, s := range []*Subscription{all, latest} {
		select {
		case err := <-s.Err():
			if !errors.Is(err, ErrSubscriptionOverflow) && s == all {
				t.Fatal(err)
			}
		default:
			if s == all {
				t.Fatal("missing independent overflow")
			}
		}
		ev := <-s.Events()
		if ev.Kind != Announced {
			t.Fatal("metadata lost", ev.Kind)
		}
		if err := s.Close(); s == all && !errors.Is(err, ErrClosed) || s == latest && err != nil {
			t.Fatal(err)
		}
	}
	if all.Dropped() != 0 || latest.Dropped() != 1 {
		t.Fatal("incorrect drop accounting", all.Dropped(), latest.Dropped())
	}
}

func TestWaitForValueCancellationDisposes(t *testing.T) {
	c, _ := NewClient(ClientOptions{})
	defer c.Close()
	for i := 0; i < 120; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, err := c.WaitForValue(ctx, "/missing")
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}
	if err := c.call(func(e *engine) error {
		if len(e.subs) != 0 {
			t.Fatalf("temporary subscriptions leaked: %d", len(e.subs))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOfflineRouteOverflow(t *testing.T) {
	c, _ := NewClient(ClientOptions{})
	defer c.Close()
	all, _ := c.Subscribe([]string{""}, SubscriptionOptions{Prefix: true, All: true, Mode: DeliveryAll, BufferCapacity: 1, BufferMaxBytes: 1024})
	latest, _ := c.Subscribe([]string{"/"}, SubscriptionOptions{Prefix: true, BufferCapacity: 1, BufferMaxBytes: 1024})
	topics, _ := c.Subscribe([]string{"/"}, SubscriptionOptions{Prefix: true, TopicsOnly: true, BufferCapacity: 1, BufferMaxBytes: 1024})
	emit := func(kind EventKind, name string) {
		t.Helper()
		if err := c.call(func(e *engine) error {
			if e.topics[name] == nil {
				e.topics[name] = &topicRecord{name: name}
				e.rebuildRoutes()
			}
			e.route(Event{Kind: kind, Topic: TopicSnapshot{Name: name}, Sample: Sample{Value: "sample"}, ReceivedAt: time.Now()})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	emit(Announced, "/a")
	emit(ValueReceived, "/a")
	if !errors.Is(<-all.Err(), ErrSubscriptionOverflow) {
		t.Fatal("missing independent overflow")
	}
	if (<-all.Events()).Kind != Announced {
		t.Fatal("accepted metadata lost")
	}
	if _, ok := <-all.Events(); ok {
		t.Fatal("all events not closed")
	}
	if got := <-topics.Events(); got.Kind != Announced {
		t.Fatal(got)
	}
	select {
	case <-topics.Events():
		t.Fatal("topics-only received a value")
	default:
	}
	if got := <-latest.Events(); got.Kind != Announced {
		t.Fatal("value evicted metadata")
	}
	if !errors.Is(<-latest.Err(), ErrSubscriptionOverflow) {
		t.Fatal("metadata overflow must fail, not silently continue")
	}
	hidden, _ := c.Subscribe([]string{"$"}, SubscriptionOptions{Prefix: true, BufferCapacity: 2})
	emit(Announced, "$private")
	select {
	case <-hidden.Events():
	default:
		t.Fatal("explicit hidden prefix missed")
	}
	select {
	case _, ok := <-latest.Events():
		if ok {
			t.Fatal("hidden leaked")
		}
	default:
	}
}

func TestConcurrentEventDrain(t *testing.T) {
	c, _ := NewClient(ClientOptions{})
	defer c.Close()
	s, _ := c.Subscribe([]string{"a"}, SubscriptionOptions{BufferCapacity: 4, BufferMaxBytes: 360})
	if err := c.call(func(e *engine) error { e.topics["a"] = &topicRecord{name: "a"}; e.rebuildRoutes(); return nil }); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range s.Events() {
		}
	}()
	for i := 0; i < 2000; i++ {
		if err := c.call(func(e *engine) error {
			e.route(Event{Kind: ValueReceived, Topic: TopicSnapshot{Name: "a"}, Sample: Sample{Value: "v"}})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestConsumerDiscoveryTransitionsAndAuthoritativeRemoval(t *testing.T) {
	peer, c := peerClient(t, protocol40)
	cam, err := c.Subscribe([]string{"/CameraPublisher/"}, SubscriptionOptions{Prefix: true, All: true, Mode: DeliveryAll, BufferCapacity: 16})
	if err != nil {
		t.Fatal(err)
	}
	fms, err := c.Subscribe([]string{"/FMSInfo/FMSControlData"}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 16})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := peerConn(t, peer)
	for i := 0; i < 3; i++ {
		peerNext(t, peer)
	}
	sendControl(t, conn, `[{"method":"announce","params":{"name":"/CameraPublisher/cam/streams","id":1,"type":"string[]","properties":{}}},{"method":"announce","params":{"name":"/FMSInfo/FMSControlData","id":2,"type":"int","properties":{}}}]`)
	camera, _ := hex.DecodeString("9401011491a46d6a7067")
	replacement, _ := hex.DecodeString("9401021491a36e6577")
	transitions, _ := hex.DecodeString("940201020194020202009402030201")
	for _, b := range [][]byte{camera, replacement, transitions} {
		if err := testpeer.Write(conn, websocket.BinaryMessage, b); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	receive := func(sub *Subscription, kind EventKind) Event {
		t.Helper()
		for {
			select {
			case ev := <-sub.Events():
				if ev.Kind == kind {
					return ev
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
				return Event{}
			}
		}
	}
	first := receive(cam, ValueReceived)
	second := receive(cam, ValueReceived)
	if !reflect.DeepEqual(first.Sample.Value, []string{"mjpg"}) || !reflect.DeepEqual(second.Sample.Value, []string{"new"}) || second.Sample.Timestamp != 2 {
		t.Fatalf("camera stream types/times: %+v %+v", first, second)
	}
	for i, want := range []int64{1, 0, 1} {
		ev := receive(fms, ValueReceived)
		if ev.Sample.Value != want || ev.Sample.Timestamp != int64(i+1) {
			t.Fatalf("transition %d: %+v", i, ev)
		}
	}
	if s, ok := c.Latest("/FMSInfo/FMSControlData"); !ok || s.Value != int64(1) || s.Timestamp != 3 {
		t.Fatalf("latest: %+v %v", s, ok)
	}
	sendControl(t, conn, `[{"method":"unannounce","params":{"name":"/CameraPublisher/cam/streams","id":1}}]`)
	if ev := receive(cam, ServerUnannounced); ev.Topic.Name != "/CameraPublisher/cam/streams" {
		t.Fatal(ev)
	}
}

func TestConsumerCompileOffline(t *testing.T) {
	c, err := NewClient(ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, err := c.Subscribe([]string{"/CameraPublisher/"}, SubscriptionOptions{Prefix: true, All: true, Mode: DeliveryAll, BufferCapacity: 4, BufferMaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := c.Publish("status", "string", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.Set("idle"); err != nil {
		t.Fatal(err)
	}
	_, _ = c.Latest("status")
	_, _ = c.Topic("status")
	_ = c.Status()
	_ = c.StateChanges()
	_ = s.Events()
	_ = s.Err()
	_ = s.Dropped()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = c.Start(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRepairSubscriptionPatternByteBudgetAndOverflowReclaim(t *testing.T) {
	c, err := NewClient(ClientOptions{RetainedMaxBytes: 80, MaxSubscriptions: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	pattern := strings.Repeat("p", 55)
	s, err := c.Subscribe([]string{pattern}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 1, BufferMaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subscribe([]string{pattern}, SubscriptionOptions{}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("aggregate pattern admission: %v", err)
	}
	if err := c.call(func(e *engine) error {
		e.topics[pattern] = &topicRecord{name: pattern}
		e.rebuildRoutes()
		ev := Event{Kind: ValueReceived, Topic: TopicSnapshot{Name: pattern}, Sample: Sample{Value: int64(1)}}
		e.route(ev)
		e.route(ev)
		if len(e.subIDs) != 0 || e.descriptorBytes != 0 {
			t.Errorf("overflow left live descriptors: %d UIDs, %d bytes", len(e.subIDs), e.descriptorBytes)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(<-s.Err(), ErrSubscriptionOverflow) {
		t.Fatal("no terminal overflow")
	}
	s2, err := c.Subscribe([]string{pattern}, SubscriptionOptions{})
	if err != nil {
		t.Fatal("overflow descriptor not reclaimed:", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		if e.descriptorBytes != 0 || len(e.subIDs) != 0 {
			t.Errorf("close leaked descriptors: %d, %d", e.descriptorBytes, len(e.subIDs))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRepairSubscribeCloseUnderReplayPressure(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 1, WriterMaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	a, err := c.Subscribe([]string{"/a"}, SubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Subscribe([]string{"/b"}, SubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		e.epoch = 1
		e.session = &session{jobs: make(chan writeJob, 1)}
		e.registering = true
		c.flush(e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		defer func() { e.session = nil }()
		if len(e.replay) != 1 || !strings.Contains(string(e.replay[0].data), `"unsubscribe"`) {
			return fmt.Errorf("missing reliable close barrier")
		}
		if !strings.Contains(string((<-e.session.jobs).data), `"subscribe"`) {
			return fmt.Errorf("subscribe not first")
		}
		e.session.writerBytes, e.session.writerItems = 0, 0
		c.flush(e)
		if !strings.Contains(string((<-e.session.jobs).data), `"subscribe"`) {
			return fmt.Errorf("second subscribe must precede unsubscribe")
		}
		e.session.writerBytes, e.session.writerItems = 0, 0
		c.flush(e)
		if len(e.session.jobs) != 1 || !strings.Contains(string((<-e.session.jobs).data), `"unsubscribe"`) {
			return fmt.Errorf("orphaned subscription")
		}
		e.session = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRepairRemoteMetadataReplacementDeletionAndReclamation(t *testing.T) {
	c, err := NewClient(ClientOptions{RetainedMaxBytes: 256, MaxTopics: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	big := map[string]any{"description": strings.Repeat("x", 180)}
	if err := c.call(func(e *engine) error {
		announce := func(name string, id int32, props map[string]any) {
			c.control(e, controlEvent{method: "announce", name: name, id: id, typ: "string", props: props}, time.Now())
		}
		update := func(name string, props map[string]any) {
			c.control(e, controlEvent{method: "properties", name: name, props: props}, time.Now())
		}
		announce("/a", 1, big)
		charged := e.observedBytes
		if charged != descriptorJSONBytes(big) {
			t.Fatalf("charge %d", charged)
		}
		announce("/b", 2, big)
		if e.topics["/b"] != nil || e.observedBytes != charged {
			t.Fatalf("rejected announce changed state: %d", e.observedBytes)
		}
		update("/a", map[string]any{"other": strings.Repeat("y", 180)})
		if e.observedBytes != charged || len(e.topics["/a"].observed) != 1 {
			t.Fatal("rejected update changed state")
		}
		update("/a", map[string]any{"description": nil})
		if e.observedBytes != 0 {
			t.Fatalf("deletion retained %d bytes", e.observedBytes)
		}
		announce("/b", 2, big)
		if e.topics["/b"] == nil || e.observedBytes != charged {
			t.Fatal("reclaimed bytes unavailable")
		}
		announce("/b", 2, map[string]any{})
		if e.observedBytes != 0 {
			t.Fatal("replacement did not release charge")
		}
		announce("/a", 1, big)
		c.control(e, controlEvent{method: "unannounce", name: "/a", id: 1}, time.Now())
		if e.observedBytes != 0 || e.topics["/a"] != nil {
			t.Fatal("unannounce leaked charge")
		}
		local := &topicRecord{name: "/local", members: map[uint32]bool{1: true}, requested: map[string]any{}}
		e.topics[local.name] = local
		e.pubs[1] = &Publisher{typ: "string", errors: make(chan error, 1)}
		announce("/local", 4, big)
		c.control(e, controlEvent{method: "unannounce", name: "/local", id: 4}, time.Now())
		if e.observedBytes != 0 || e.topics["/local"] == nil || len(local.observed) != 0 {
			t.Fatal("local topic unannounce retained remote metadata")
		}
		delete(e.topics, "/local")
		delete(e.pubs, 1)
		announce("/retained", 3, map[string]any{"retained": true, "description": strings.Repeat("x", 100)})
		kept := e.observedBytes
		c.loss(e, ErrProtocol)
		if e.observedBytes != kept || e.topics["/retained"] == nil {
			t.Fatal("loss dropped retained metadata charge")
		}
		for i := 0; i < 10; i++ {
			announce(fmt.Sprintf("/extra/%d", i), int32(10+i), big)
		}
		if e.observedBytes != kept {
			t.Fatal("retained metadata exceeded budget after loss")
		}
		e.removeTopic(e.topics["/retained"])
		if e.observedBytes != 0 {
			t.Fatal("removed topic leaked charge")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRepairRemoteDescriptorReplacementAndReclamation(t *testing.T) {
	c, err := NewClient(ClientOptions{RetainedMaxBytes: 128, MaxNameBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.call(func(e *engine) error {
		announce := func(name string, id int32, typ string, props map[string]any) {
			c.control(e, controlEvent{method: "announce", name: name, id: id, typ: typ, props: props}, time.Now())
		}
		name := "/" + strings.Repeat("n", 53)
		announce(name, 1, "string", map[string]any{"retained": true})
		rec := e.topics[name]
		if rec == nil || rec.observedDescriptorBytes != len(name)+len("string") {
			t.Fatal("missing descriptor charge")
		}
		before := e.observedDescriptorBytes
		announce(name, 2, strings.Repeat("t", 120), map[string]any{"retained": false})
		if e.observedDescriptorBytes != before || !rec.hasID || rec.id != 1 || rec.observedType != "string" || rec.observed["retained"] != true || e.ids[1] != rec || e.ids[2] != nil {
			t.Fatal("rejection mutated remote registration")
		}
		announce(name, 2, "int", map[string]any{"retained": true})
		if e.ids[1] != nil || e.ids[2] != rec || e.observedDescriptorBytes != before-len("string")+len("int") {
			t.Fatal("accepted replacement did not adjust charge/ID")
		}
		before = e.observedDescriptorBytes
		c.loss(e, ErrProtocol)
		if e.topics[name] != rec || e.observedDescriptorBytes != before || rec.hasID {
			t.Fatal("retained remote descriptor lost across disconnect")
		}
		extra := "/" + strings.Repeat("z", 75)
		announce(extra, 3, "string", map[string]any{})
		if e.topics[extra] != nil || e.observedDescriptorBytes != before {
			t.Fatal("retained descriptor failed to constrain new admission")
		}
		announce(name, 4, "int", map[string]any{"retained": true})
		c.control(e, controlEvent{method: "unannounce", name: name, id: 4}, time.Now())
		if e.topics[name] != nil || e.observedDescriptorBytes != 0 {
			t.Fatal("unannounce leaked remote descriptor")
		}
		announce(extra, 3, "string", map[string]any{})
		if e.topics[extra] == nil {
			t.Fatal("released bytes unavailable")
		}
		c.loss(e, ErrProtocol)
		if e.observedDescriptorBytes != 0 || e.topics[extra] != nil {
			t.Fatal("non-retained disconnect leaked descriptor")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRepairRemoteDescriptorSharesLocalBudget(t *testing.T) {
	c, err := NewClient(ClientOptions{RetainedMaxBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := c.Publish("/local", "string", nil, PublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		name := "/" + strings.Repeat("x", 115)
		c.control(e, controlEvent{method: "announce", name: name, id: 1, typ: "string", props: map[string]any{}}, time.Now())
		if e.topics[name] != nil {
			t.Fatal("remote descriptor ignored local budget")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.call(func(e *engine) error {
		name := "/" + strings.Repeat("x", 115)
		c.control(e, controlEvent{method: "announce", name: name, id: 1, typ: "string", props: map[string]any{}}, time.Now())
		if e.topics[name] == nil {
			t.Fatal("local descriptor reclamation unavailable")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReviewOverflowReleasesLiveSubscriptionUIDIndex(t *testing.T) {
	c, err := NewClient(ClientOptions{MaxSubscriptions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 12; i++ {
		s, err := c.Subscribe([]string{"/review"}, SubscriptionOptions{All: true, Mode: DeliveryAll, BufferCapacity: 1, BufferMaxBytes: 1024})
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		err = c.call(func(e *engine) error {
			if e.topics["/review"] == nil {
				e.topics["/review"] = &topicRecord{name: "/review"}
				e.rebuildRoutes()
			}
			ev := Event{Kind: ValueReceived, Topic: TopicSnapshot{Name: "/review"}, Sample: Sample{Value: int64(1)}}
			e.route(ev)
			e.route(ev)
			if len(e.subIDs) != 0 {
				t.Errorf("iteration %d: %d stale live subscription UIDs after terminal overflow", i, len(e.subIDs))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := <-s.Err(); !errors.Is(got, ErrSubscriptionOverflow) {
			t.Fatalf("iteration %d: terminal error %v", i, got)
		}
	}
}

func TestReviewSubscriptionCloseDuringRegistration(t *testing.T) {
	c, err := NewClient(ClientOptions{WriterCapacity: 1, WriterMaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, err := c.Subscribe([]string{"/review/sub"}, SubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Subscribe([]string{"/review/other"}, SubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = c.call(func(e *engine) error {
		e.epoch = 1
		e.session = &session{jobs: make(chan writeJob, 1)}
		e.registering = true
		c.flush(e)
		if e.registrationSub != s.uid || !e.registering {
			t.Fatalf("registration cursor %d active=%v", e.registrationSub, e.registering)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var found bool
	err = c.call(func(e *engine) error {
		for _, j := range e.replay {
			found = found || strings.Contains(string(j.data), `"unsubscribe"`)
		}
		for len(e.session.jobs) > 0 {
			j := <-e.session.jobs
			found = found || strings.Contains(string(j.data), `"unsubscribe"`)
		}
		e.session = nil
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("accepted close leaves already enqueued subscribe active on peer")
	}
}

func TestReviewRemoteAnnouncementsHaveAggregateByteAdmission(t *testing.T) {
	c, err := NewClient(ClientOptions{RetainedMaxBytes: 256, MaxTopics: 100, MaxTextBytes: 4096, MaxBinaryBytes: 4096, InboundMaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.call(func(e *engine) error {
		for i := 0; i < 12; i++ {
			c.control(e, controlEvent{method: "announce", name: fmt.Sprintf("/remote/%d", i), id: int32(i), typ: "string", props: map[string]any{"description": strings.Repeat("x", 180)}}, time.Now())
		}
		if len(e.topics) > 1 {
			t.Errorf("accepted %d remote observed property trees, over aggregate 256-byte retained budget", len(e.topics))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReviewRemoteTopicNamesRespectRetainedByteBudget(t *testing.T) {
	c, err := NewClient(ClientOptions{RetainedMaxBytes: 256, MaxTopics: 100, MaxNameBytes: 128, MaxTextBytes: 4096, MaxBinaryBytes: 4096, InboundMaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.call(func(e *engine) error {
		for i := 0; i < 12; i++ {
			c.control(e, controlEvent{method: "announce", name: fmt.Sprintf("/remote/%02d/%s", i, strings.Repeat("n", 90)), id: int32(i), typ: "string", props: map[string]any{}}, time.Now())
		}
		total := 0
		for _, topic := range e.topics {
			total += len(topic.name) + len(topic.observedType)
		}
		if total > c.opts.RetainedMaxBytes {
			t.Errorf("accepted %d topic-name/type bytes (retained cap %d); status=%+v", total, c.opts.RetainedMaxBytes, e.status)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
