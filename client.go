package nt4

import (
	"context"
	"encoding/json"
	"github.com/levifitzpatrick1/go-nt4/internal/wire"
	"math"
	"sync"
	"time"
)

type command struct {
	size  int
	run   func(*engine) error
	reply chan error
}
type engine struct {
	topics                               map[string]*topicRecord
	ids                                  map[int32]*topicRecord
	pubs                                 map[uint32]*Publisher
	subs                                 map[uint32]*Subscription
	pubIDs, subIDs                       []uint32 // live, monotonic UIDs for bounded replay cursor traversal
	nextPub, nextSub                     uint64
	retained, offlineItems, offlineBytes int
	descriptorBytes                      int // live publisher descriptors, requested properties and subscription patterns
	observedBytes                        int // authoritative remote property maps, including retained topics
	observedDescriptorBytes              int // remote names and types, including retained topics
	propertyBytes                        int
	status                               Status
	started                              bool
	session                              *session
	epoch                                uint64
	clock                                sessionClock
	retry                                time.Time
	backoff                              time.Duration
	replay                               []writeJob // bounded live control backlog
	replayBytes                          int
	registrationPub, registrationSub     uint32 // registry replay cursors; no serialized copy
	registering                          bool
	pendingUnsubs                        []uint32
}
type topicRecord struct {
	name, observedType, requestedType string
	observed, requested               map[string]any
	propertyPatch                     map[string]any // accepted, not yet confirmed written; nil means deletion
	propertyVersion, propertyQueued   uint64
	propertyInFlight                  bool
	requestedBytes                    int
	observedBytes                     int
	observedDescriptorBytes           int
	members                           map[uint32]bool
	latest                            *Sample
	latestSize                        int
	localStrong                       bool
	latestSequence                    uint64
	latestOwner                       uint32
	replayedEpoch                     uint64
	replayedSequence                  uint64
	replayedOwner                     uint32
	acquisition                       time.Time
	hasID                             bool
	id                                int32
	stale                             bool
	epoch                             uint64
	routes                            []*Subscription
}
type Client struct {
	mu             sync.Mutex
	opts           ClientOptions
	commands       chan command
	wake           chan struct{}
	done           chan struct{}
	closing        bool
	changesClosed  bool
	status         Status
	changes        chan Status
	endpoint       string
	ctx            context.Context
	cancel         context.CancelFunc
	workers        sync.WaitGroup
	sessionWorkers sync.WaitGroup
	origin         time.Time
	dials          chan sessionEvent
	failures       chan sessionEvent
	written        chan sessionEvent
	probes         chan probeRequest
	inbound        chan sessionEvent
	inMu           sync.Mutex
	inBytes        int
	inRejected     uint64 // guarded by inMu
	localRejected  uint64 // guarded by mu
	lastState      LifecycleState
	convMu         sync.Mutex
	convPubs       map[string]*Publisher
}

func NewClient(o ClientOptions) (*Client, error) {
	var err error
	o, err = NormalizeClientOptions(o)
	if err != nil {
		return nil, err
	}
	address, err := endpoint(o)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{opts: o, endpoint: address, ctx: ctx, cancel: cancel, origin: time.Now(), dials: make(chan sessionEvent, 1), failures: make(chan sessionEvent, 1), written: make(chan sessionEvent, o.WriterCapacity), probes: make(chan probeRequest), inbound: make(chan sessionEvent, o.InboundCapacity), commands: make(chan command, o.CommandCapacity), wake: make(chan struct{}), done: make(chan struct{}), changes: make(chan Status, 1)}
	e := &engine{topics: make(map[string]*topicRecord), ids: make(map[int32]*topicRecord), pubs: make(map[uint32]*Publisher), subs: make(map[uint32]*Subscription), status: Status{State: StateIdle}, clock: newSessionClock(c.origin)}
	go c.loop(e)
	return c, nil
}
func (c *Client) loop(e *engine) {
	tick := time.NewTicker(min(c.opts.KeepaliveInterval/2, 100*time.Millisecond))
	defer tick.Stop()
	for {
		var cmd command
		var ok bool
		select {
		case cmd, ok = <-c.commands:
			if !ok {
				c.stopSession(e)
				c.workers.Wait()
				c.sessionWorkers.Wait()
				close(c.done)
				return
			}
		case ev := <-c.dials:
			c.onDial(e, ev)
			continue
		case ev := <-c.failures:
			if ev.epoch == e.epoch {
				c.loss(e, ev.err)
			}
			continue
		case ev := <-c.inbound:
			c.inMu.Lock()
			c.inBytes -= ev.size
			e.status.InboundItems = uint64(len(c.inbound))
			e.status.InboundBytes = uint64(c.inBytes)
			c.inMu.Unlock()
			if ev.epoch == e.epoch && e.session != nil {
				before := e.status.LastError
				c.onInbound(e, ev)
				if before != e.status.LastError {
					c.update(e)
				} else {
					c.mu.Lock()
					queuedBytes := c.status.CommandBytes
					c.status = e.status
					c.status.Rejected += c.localRejected
					c.status.CommandBytes = queuedBytes
					c.status.CommandItems = uint64(len(c.commands))
					c.mu.Unlock()
				}
			}
			continue
		case p := <-c.probes:
			if e.session != nil && p.epoch == e.epoch {
				e.session.probeQueued = false
			}
			p.ack <- e.session != nil && e.clock.recordProbe(probeSent{p.epoch, p.echo, p.sent})
			continue
		case ev := <-c.written:
			if e.session != nil && ev.epoch == e.epoch {
				e.session.started.Add(-1)
				if ev.propertyWrite {
					if t := e.topics[ev.propertyName]; t != nil && t.propertyInFlight && t.propertyQueued == ev.propertyVersion {
						t.propertyInFlight = false
						if t.propertyVersion == ev.propertyVersion {
							e.propertyBytes -= propertyPatchSize(t.propertyPatch)
							t.propertyPatch = nil
						}
					}
				}
				e.session.writerBytes -= ev.size
				e.session.writerItems--
				if p := e.pubs[ev.valueOwner]; p != nil {
					for i := range p.pending {
						if p.pending[i].sequence == ev.valueSequence && p.pending[i].queuedEpoch == ev.epoch {
							p.pending[i].completed = true
							break
						}
					}
					for len(p.pending) > 0 && p.pending[0].completed {
						item := p.pending[0]
						e.offlineItems--
						e.offlineBytes -= item.bytes
						p.pendingBytes -= item.bytes
						p.pending[0] = pendingSample{}
						p.pending = p.pending[1:]
					}
					e.status.OfflineItems = uint64(e.offlineItems)
					e.status.OfflineBytes = uint64(e.offlineBytes)
				}
				e.status.WriterItems = uint64(e.session.writerItems)
				e.status.WriterBytes = uint64(e.session.writerBytes)
				c.flush(e)
				c.update(e)
			}
			continue
		case <-tick.C:
			c.tick(e)
			continue
		}
		err := cmd.run(e)
		c.flush(e)
		c.mu.Lock()
		if c.closing && e.status.State != StateClosed {
			e.status.State = StateClosing
		}
		remaining := c.status.CommandBytes - uint64(cmd.size)
		c.status = e.status
		c.status.Rejected += c.localRejected
		c.status.CommandItems = uint64(len(c.commands))
		c.status.CommandBytes = remaining
		close(c.wake)
		c.wake = make(chan struct{})
		c.mu.Unlock()
		cmd.reply <- err
	}
}
func (c *Client) submit(ctx context.Context, size int, wait bool, f func(*engine) error) error {
	if c == nil {
		return ErrInvalidHandle
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if size < 1 {
		size = 1
	}
	for {
		c.mu.Lock()
		if c.closing {
			c.mu.Unlock()
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return err
		}
		if size > c.opts.CommandMaxBytes {
			c.localRejected++
			c.status.Rejected++
			c.mu.Unlock()
			return ErrQueueFull
		}
		if len(c.commands) < cap(c.commands) && uint64(size) <= uint64(c.opts.CommandMaxBytes)-c.status.CommandBytes {
			r := make(chan error, 1)
			c.status.CommandBytes += uint64(size)
			c.commands <- command{size, f, r}
			c.mu.Unlock()
			return <-r
		}
		if !wait {
			c.localRejected++
			c.status.Rejected++
			c.mu.Unlock()
			return ErrQueueFull
		}
		wake := c.wake
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}
func (c *Client) call(f func(*engine) error) error { return c.submit(context.Background(), 1, true, f) }
func (c *Client) notify(e *engine) {
	c.mu.Lock()
	s := e.status
	oldState := c.lastState
	c.lastState = s.State
	select {
	case c.changes <- s:
	default:
		select {
		case <-c.changes:
			e.status.StateChangesDropped++
		default:
		}
		c.changes <- s
	}
	c.mu.Unlock()

	if oldState != s.State {
		isOnline := s.State == StateOnlineReady || s.State == StateOnlineUnsynchronized
		wasOnline := oldState == StateOnlineReady || oldState == StateOnlineUnsynchronized
		if isOnline && !wasOnline {
			if c.opts.Logger != nil {
				c.opts.Logger.Info("connected to server", "address", c.opts.ServerAddress, "port", c.opts.Port)
			}
			if c.opts.OnConnect != nil {
				go c.opts.OnConnect()
			}
		} else if !isOnline && wasOnline {
			if c.opts.Logger != nil {
				c.opts.Logger.Warn("disconnected from server", "address", c.opts.ServerAddress)
			}
			if c.opts.OnDisconnect != nil {
				go c.opts.OnDisconnect()
			}
		}
	}
}
func (c *Client) StateChanges() <-chan Status {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.changes
}
func (c *Client) Start(ctx context.Context) error {
	return c.submit(ctx, 1, true, func(e *engine) error {
		if e.started {
			return ErrAlreadyStarted
		}
		e.started = true
		c.beginDial(e)
		c.notify(e)
		return nil
	})
}
func (c *Client) Status() Status {
	if c == nil {
		return Status{State: StateClosed}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.status
	c.inMu.Lock()
	s.InboundItems = uint64(len(c.inbound))
	s.InboundBytes = uint64(c.inBytes)
	s.Rejected += c.inRejected
	c.inMu.Unlock()
	return s
}
func (c *Client) WaitConnected(ctx context.Context) error { return c.wait(ctx, false) }
func (c *Client) WaitReady(ctx context.Context) error     { return c.wait(ctx, true) }
func (c *Client) wait(ctx context.Context, ready bool) error {
	if c == nil {
		return ErrInvalidHandle
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		c.mu.Lock()
		s := c.status
		w := c.wake
		c.mu.Unlock()
		if s.State == StateClosed || s.State == StateClosing {
			return ErrClosed
		}
		if (!ready && (s.State == StateOnlineUnsynchronized || s.State == StateOnlineReady)) || (ready && s.Ready) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w:
		}
	}
}
func (c *Client) Close() error {
	if c == nil {
		return ErrInvalidHandle
	}
	c.mu.Lock()
	if !c.closing {
		c.closing = true
		c.cancel()
		c.status.State = StateClosing
		close(c.wake)
		c.wake = make(chan struct{})
		r := make(chan error, 1)
		for len(c.commands) == cap(c.commands) {
			w := c.wake
			c.mu.Unlock()
			<-w
			c.mu.Lock()
		}
		c.commands <- command{size: 0, reply: r, run: func(e *engine) error {
			e.status.State = StateClosed
			for _, s := range e.subs {
				s.terminal(true, nil, e)
			}
			for _, p := range e.pubs {
				close(p.errors) // registered handles, including deferred closes, are owner-owned

				p.active = false
				p.pending = nil
			}
			e.topics = make(map[string]*topicRecord)
			e.retained = 0
			e.descriptorBytes = 0
			e.observedBytes = 0
			e.observedDescriptorBytes = 0
			e.offlineItems = 0
			e.offlineBytes = 0
			e.status.OfflineItems = 0
			e.status.OfflineBytes = 0
			e.status.RetainedBytes = 0
			c.notify(e)
			return nil
		}}
		close(c.commands)
	}
	c.mu.Unlock()
	<-c.done
	c.mu.Lock()
	if !c.changesClosed {
		close(c.changes)
		c.changesClosed = true
	}
	c.mu.Unlock()
	return nil
}
func (c *Client) Latest(name string) (Sample, bool) {
	var s Sample
	ok := false
	err := c.call(func(e *engine) error {
		if t := e.topics[name]; t != nil && t.latest != nil {
			s = t.latest.Clone()
			ok = true
		}
		return nil
	})
	return s, ok && err == nil
}

// WaitForValue observes a current value without consuming any existing subscription.
// It owns and disposes its bounded temporary subscription on every exit path.
func (c *Client) WaitForValue(ctx context.Context, name string) (Sample, error) {
	if c == nil {
		return Sample{}, ErrInvalidHandle
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ValidateTopicName(name, c.opts.MaxNameBytes); err != nil {
		return Sample{}, err
	}
	if err := ctx.Err(); err != nil {
		return Sample{}, err
	}
	sub, err := c.Subscribe([]string{name}, SubscriptionOptions{BufferCapacity: 4, BufferMaxBytes: c.opts.MaxBinaryBytes + 2*c.opts.MaxNameBytes + 1024})
	if err != nil {
		return Sample{}, err
	}
	defer sub.Close()
	if v, ok := c.Latest(name); ok && !v.Stale {
		return v, nil
	}
	for {
		select {
		case <-ctx.Done():
			return Sample{}, ctx.Err()
		case err, ok := <-sub.Err():
			if ok {
				return Sample{}, err
			}
			return Sample{}, ErrClosed
		case ev, ok := <-sub.Events():
			if !ok {
				return Sample{}, ErrClosed
			}
			if ev.Kind == ValueReceived {
				return ev.Sample, nil
			}
		}
	}
}
func (c *Client) Topic(name string) (TopicSnapshot, bool) {
	var s TopicSnapshot
	ok := false
	err := c.call(func(e *engine) error {
		if t := e.topics[name]; t != nil {
			s = t.snapshot()
			ok = true
		}
		return nil
	})
	return s, ok && err == nil
}
func (t *topicRecord) snapshot() TopicSnapshot {
	typ := t.observedType
	if typ == "" {
		typ = t.requestedType
	}
	props := t.observed
	if props == nil {
		props = t.requested
	}
	return TopicSnapshot{Name: t.name, Type: typ, ID: t.id, HasID: t.hasID, Properties: cloneJSONTrustedMap(props), Stale: t.stale, Epoch: t.epoch}
}
func (c *Client) limits() JSONLimits {
	return JSONLimits{c.opts.MaxTextBytes, c.opts.MaxJSONDepth, c.opts.MaxJSONContainerItems, c.opts.MaxNameBytes}
}

// Descriptor maps have already passed OwnJSON validation. Charge their actual
// encoded size, rather than the JSON parser's worst-case input amplification.
func descriptorJSONBytes(m map[string]any) int {
	if len(m) == 0 {
		return 0
	}
	b, _ := json.Marshal(m)
	return len(b)
}
func propertyPatchSize(m map[string]any) int {
	if len(m) == 0 {
		return 0
	}
	return jsonSize(m)
}
func jsonSize(m map[string]any) int {
	_, n, _ := OwnJSON(m, JSONLimits{1 << 30, 64, 1 << 20, 1 << 20})
	return n
}
func cached(t *topicRecord) bool {
	return t.requested["cached"] != false && t.observed["cached"] != false
}
func (e *engine) reserveLatest(t *topicRecord, size, max int) bool {
	return e.retained+e.descriptorBytes+e.observedBytes+e.observedDescriptorBytes+e.propertyBytes-t.latestSize+size <= max
}
func (e *engine) setLatest(t *topicRecord, s Sample, size int) {
	e.retained += size - t.latestSize
	t.latestSize = size
	t.latest = &s
	e.status.RetainedBytes = uint64(e.retained)
}
func (e *engine) clearLatest(t *topicRecord) {
	e.retained -= t.latestSize
	t.latestSize = 0
	t.latest = nil
	e.status.RetainedBytes = uint64(e.retained)
}

// route is the transport seam: the next phase fences inbound epochs before
// calling this on the owner. No application code executes on this path.
func (e *engine) route(ev Event) {
	if t := e.topics[ev.Topic.Name]; t != nil {
		for _, s := range t.routes {
			s.deliver(ev, e)
		}
	}
}
func (e *engine) rebuildRoutes() {
	for _, t := range e.topics {
		t.routes = make([]*Subscription, 0, len(e.subs))
		for _, s := range e.subs {
			if s.matches(t.name) {
				t.routes = append(t.routes, s)
			}
		}
	}
}
func (e *engine) removeTopic(t *topicRecord) {
	e.clearLatest(t)
	e.descriptorBytes -= t.requestedBytes
	e.observedBytes -= t.observedBytes
	e.observedDescriptorBytes -= t.observedDescriptorBytes
	e.propertyBytes -= propertyPatchSize(t.propertyPatch)
	delete(e.topics, t.name)
}
func nextUID(n *uint64) (uint32, error) {
	if *n >= math.MaxInt32 {
		return 0, ErrQueueFull
	}
	*n++
	return uint32(*n), nil
}
func mergeProps(dst, update map[string]any) {
	for k, v := range update {
		if v == nil {
			delete(dst, k)
		} else {
			dst[k] = v
		}
	}
}
func (c *Client) SetProperties(name string, update map[string]any) error {
	if c == nil {
		return ErrInvalidHandle
	}
	if err := ValidateTopicName(name, c.opts.MaxNameBytes); err != nil {
		return err
	}
	u, n, err := OwnJSON(update, c.limits())
	if err != nil {
		return err
	}
	return c.submit(context.Background(), n+len(name), true, func(e *engine) error {
		t := e.topics[name]
		if t == nil {
			return ErrInvalidHandle
		}
		if len(u) == 0 {
			return nil
		}
		if u["cached"] == false {
			for id := range t.members {
				if len(e.pubs[id].pending) > 0 {
					return ErrIncompatibleOptions
				}
			}
		}
		next := cloneJSONTrustedMap(t.requested)
		mergeProps(next, u)
		nextBytes := descriptorJSONBytes(next)
		if e.retained+e.descriptorBytes+e.observedBytes+e.observedDescriptorBytes+e.propertyBytes+nextBytes-t.requestedBytes-t.latestSize > c.opts.RetainedMaxBytes {
			return ErrQueueFull
		}
		patch := cloneJSONTrustedMap(t.propertyPatch)
		if patch == nil {
			patch = make(map[string]any)
		}
		for k, v := range u {
			patch[k] = v
		}
		// Pending property intent has its own finite byte budget, including
		// deletions (which cannot be represented by requested state alone).
		j := jobControl("setproperties", wire.SetProperties(name, patch).Params)
		patchBytes := jsonSize(patch)
		if j.size > c.opts.CommandMaxBytes || j.size > c.opts.WriterMaxBytes || patchBytes > c.opts.CommandMaxBytes-e.propertyBytes+propertyPatchSize(t.propertyPatch) || e.retained+e.descriptorBytes+e.observedBytes+e.observedDescriptorBytes+e.propertyBytes+nextBytes-t.requestedBytes+patchBytes-propertyPatchSize(t.propertyPatch)-t.latestSize > c.opts.RetainedMaxBytes {
			e.status.Rejected++
			return ErrQueueFull
		}
		if e.session != nil && !t.propertyInFlight {
			j.propertyName, j.propertyVersion, j.propertyWrite = name, t.propertyVersion+1, true
			if err := e.admitControl(c.opts, j); err != nil {
				return err
			}
			t.propertyInFlight = true
			t.propertyQueued = j.propertyVersion
		}
		e.propertyBytes += patchBytes - propertyPatchSize(t.propertyPatch)
		t.propertyPatch = patch
		t.propertyVersion++
		e.descriptorBytes += nextBytes - t.requestedBytes
		t.requestedBytes = nextBytes
		t.requested = next
		if !cached(t) {
			e.clearLatest(t)
		}
		return nil
	})
}
