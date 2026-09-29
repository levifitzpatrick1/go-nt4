package nt4

import (
	"context"
	"strings"
	"sync"

	"github.com/levifitzpatrick1/go-nt4/internal/wire"
)

// Subscription is an opaque identity. Copying it does not create a second registration.
type Subscription struct {
	client       *Client
	uid          uint32
	patternBytes int // aggregate descriptor admission; released at terminal disposal
	patterns     []string
	opts         SubscriptionOptions
	events       chan Event
	errors       chan error
	active       bool   // owner only
	dropped      uint64 // owner only
	bytes        int    // owner only, conservative between consumer receives
	sizes        []int  // FIFO sizes of outstanding sends; owner reconciles with channel length
	kinds        []EventKind
	disp         *subDispatcher
}

type subDispatcher struct {
	mu       sync.RWMutex
	callback func(topic *Topic, timestamp int64, value any)
	updates  chan TopicUpdate
	once     sync.Once
	done     chan struct{}
}

func (c *Client) Subscribe(patterns []string, o SubscriptionOptions) (*Subscription, error) {
	if c == nil {
		return nil, ErrInvalidHandle
	}
	var err error
	if o, err = NormalizeSubscriptionOptions(o); err != nil {
		return nil, err
	}
	if err = ValidatePatterns(patterns, o.Prefix, c.opts.MaxNameBytes); err != nil {
		return nil, err
	}
	patterns = append([]string(nil), patterns...)
	n := 0
	for _, p := range patterns {
		n += len(p)
	}
	var s *Subscription
	err = c.submit(context.Background(), n+32, true, func(e *engine) error {
		if len(e.subs)+len(e.pendingUnsubs) >= c.opts.MaxSubscriptions || n > c.opts.RetainedMaxBytes-e.retained-e.descriptorBytes-e.observedBytes-e.observedDescriptorBytes-e.propertyBytes {
			e.status.Rejected++
			return ErrQueueFull
		}
		if e.nextSub >= 1<<31-1 {
			return ErrQueueFull
		}
		if e.session != nil && !e.registering {
			opts := map[string]any{"prefix": o.Prefix, "all": o.All, "topicsonly": o.TopicsOnly}
			if o.Periodic > 0 {
				opts["periodic"] = o.Periodic.Seconds()
			}
			if err := e.admitControl(c.opts, jobControl("subscribe", wire.Subscribe(patterns, int32(e.nextSub+1), opts).Params)); err != nil {
				return err
			}
		}
		id, err := nextUID(&e.nextSub)
		if err != nil {
			return err
		}
		s = &Subscription{client: c, uid: id, patterns: patterns, opts: o, patternBytes: n, events: make(chan Event, o.BufferCapacity), errors: make(chan error, 1), active: true, disp: &subDispatcher{}}
		e.subs[id] = s
		e.subIDs = append(e.subIDs, id)
		e.descriptorBytes += n
		e.rebuildRoutes()
		return nil
	})
	return s, err
}
func (s *Subscription) check() error {
	if s == nil || s.client == nil || s.uid == 0 {
		return ErrInvalidHandle
	}
	return nil
}
func (s *Subscription) Events() <-chan Event {
	if s == nil {
		return nil
	}
	return s.events
}
func (s *Subscription) Err() <-chan error {
	if s == nil {
		return nil
	}
	return s.errors
}

func (s *Subscription) ensureDispatcher() *subDispatcher {
	if s.disp == nil {
		s.disp = &subDispatcher{}
	}
	s.disp.once.Do(func() {
		s.disp.updates = make(chan TopicUpdate, s.opts.BufferCapacity)
		s.disp.done = make(chan struct{})
		go s.dispatchLoop()
	})
	return s.disp
}

func (s *Subscription) dispatchLoop() {
	disp := s.disp
	defer close(disp.done)
	defer close(disp.updates)
	for ev := range s.events {
		if ev.Kind != ValueReceived {
			continue
		}
		top := &Topic{
			Name:       ev.Topic.Name,
			Type:       ev.Topic.Type,
			ID:         ev.Topic.ID,
			Properties: ev.Topic.Properties,
		}
		up := TopicUpdate{
			Topic:     top,
			Timestamp: ev.Sample.Timestamp,
			Value:     ev.Sample.Value,
		}

		disp.mu.RLock()
		cb := disp.callback
		disp.mu.RUnlock()

		if cb != nil {
			cb(top, ev.Sample.Timestamp, ev.Sample.Value)
		}

		select {
		case disp.updates <- up:
		default:
		}
	}
}

// SetCallback registers a callback to be called whenever a new value arrives for this subscription.
func (s *Subscription) SetCallback(callback func(topic *Topic, timestamp int64, value any)) {
	if s == nil {
		return
	}
	disp := s.ensureDispatcher()
	disp.mu.Lock()
	disp.callback = callback
	disp.mu.Unlock()
}

// GetCallback returns the currently registered callback, if any.
func (s *Subscription) GetCallback() func(topic *Topic, timestamp int64, value any) {
	if s == nil || s.disp == nil {
		return nil
	}
	s.disp.mu.RLock()
	defer s.disp.mu.RUnlock()
	return s.disp.callback
}

// Updates returns a channel delivering TopicUpdate values.
func (s *Subscription) Updates() <-chan TopicUpdate {
	if s == nil {
		return nil
	}
	disp := s.ensureDispatcher()
	return disp.updates
}
func (s *Subscription) Dropped() uint64 {
	if s == nil || s.client == nil {
		return 0
	}
	var n uint64
	_ = s.client.call(func(e *engine) error { n = s.dropped; return nil })
	return n
}
func (s *Subscription) Close() error {
	if err := s.check(); err != nil {
		return err
	}
	return s.client.call(func(e *engine) error {
		if e.subs[s.uid] != s {
			if !s.active {
				return ErrClosed
			}
			return ErrForeignHandle
		}
		if !s.active {
			return ErrClosed
		}
		// A registration already placed in the writer must be undone even if
		// replay is still generating other subscriptions. The replay backlog
		// drains only after registration, preserving subscribe-before-close.
		if e.session != nil && (!e.registering || e.registrationSub >= s.uid) {
			if err := e.admitControl(s.client.opts, jobControl("unsubscribe", wire.Unsubscribe(int32(s.uid)).Params)); err != nil {
				return err
			}
		}
		delete(e.subs, s.uid)
		e.subIDs = removeUID(e.subIDs, s.uid)
		e.descriptorBytes -= s.patternBytes
		e.rebuildRoutes()
		s.terminal(false, nil, e)
		return nil
	})
}

// Only the owner sends or evicts. Receives can race the owner: len is used
// solely to retire *already received* FIFO sizes. A late receive can leave
// conservative over-accounting, never a byte-budget underestimate.
func (s *Subscription) reconcile() {
	for len(s.sizes) > len(s.events) {
		s.bytes -= s.sizes[0]
		s.sizes[0] = 0
		s.sizes = s.sizes[1:]
		s.kinds = s.kinds[1:]
	}
}
func (s *Subscription) terminal(discard bool, reason error, e *engine) {
	if !s.active {
		return
	}
	s.active = false
	if discard {
		for {
			select {
			case _, ok := <-s.events:
				if !ok {
					return
				}
				s.dropped++
				e.status.Dropped++
			default:
				goto drained
			}
		}
	}
drained:
	s.reconcile()
	if reason != nil {
		s.errors <- reason
		e.status.LastError = reason
	}
	close(s.errors)
	close(s.events)
	s.sizes = nil
	s.kinds = nil
	s.bytes = 0
}
func eventSize(ev Event) int {
	return len(ev.Topic.Name) + len(ev.Topic.Type) + jsonSize(ev.Topic.Properties) + ValueSize(ev.Sample.Value) + 64
}
func (s *Subscription) matches(name string) bool {
	for _, p := range s.patterns {
		if strings.HasPrefix(name, "$") && !strings.HasPrefix(p, "$") {
			continue
		}
		if s.opts.Prefix {
			if strings.HasPrefix(name, p) {
				return true
			}
		} else if p == name {
			return true
		}
	}
	return false
}
func (e *engine) failSubscription(s *Subscription) {
	if !s.active {
		return
	}
	delete(e.subs, s.uid)
	e.subIDs = removeUID(e.subIDs, s.uid)
	e.descriptorBytes -= s.patternBytes
	e.rebuildRoutes()
	// Keep the disposal intent until writer admission succeeds. No new server
	// values can revive a failed handle, even while the writer is saturated.
	if e.session != nil {
		e.pendingUnsubs = append(e.pendingUnsubs, s.uid)
	}
	s.terminal(false, ErrSubscriptionOverflow, e)
}
func (s *Subscription) deliver(ev Event, e *engine) {
	if !s.active || s.opts.TopicsOnly && ev.Kind == ValueReceived {
		return
	}
	s.reconcile()
	size := eventSize(ev)
	if size > s.opts.BufferMaxBytes {
		if s.opts.Mode == DeliveryAll || ev.Kind != ValueReceived {
			e.failSubscription(s)
		} else {
			s.dropped++
			e.status.Dropped++
		}
		return
	}
drainLoop:
	for len(s.events) >= cap(s.events) || size > s.opts.BufferMaxBytes-s.bytes {
		if s.opts.Mode == DeliveryAll {
			e.failSubscription(s)
			return
		}
		// Metadata is not replaceable. If any queued metadata prevents replacing
		// values, fail explicitly rather than silently corrupting a topic stream.
		if len(s.events) == 0 {
			s.reconcile()
			if size <= s.opts.BufferMaxBytes-s.bytes {
				break drainLoop
			}
			e.failSubscription(s)
			return
		}
		for _, kind := range s.kinds {
			if kind != ValueReceived {
				e.failSubscription(s)
				return
			}
		}
		select {
		case old := <-s.events:
			// A consumer may have raced our receive; reconcile all preceding dequeues
			// while accounting for the one item just removed by us.
			for len(s.sizes) > len(s.events)+1 {
				s.bytes -= s.sizes[0]
				s.sizes[0] = 0
				s.sizes = s.sizes[1:]
				s.kinds = s.kinds[1:]
			}
			if len(s.sizes) > 0 {
				s.bytes -= s.sizes[0]
				s.sizes[0] = 0
				s.sizes = s.sizes[1:]
				s.kinds = s.kinds[1:]
			}
			if old.Kind != ValueReceived {
				e.failSubscription(s)
				return
			}
			s.dropped++
			e.status.Dropped++
		default:
			s.reconcile()
			if len(s.events) == 0 && size <= s.opts.BufferMaxBytes-s.bytes {
				break drainLoop
			}
		}
	}
	select {
	case s.events <- ev.Clone():
		s.bytes += size
		s.sizes = append(s.sizes, size)
		s.kinds = append(s.kinds, ev.Kind)
	default:
		e.failSubscription(s)
	}
}
