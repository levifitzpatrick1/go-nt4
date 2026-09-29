package nt4

import (
	"context"
	"github.com/levifitzpatrick1/go-nt4/internal/wire"
	"time"
)

type pendingSample struct {
	sample      Sample
	bytes       int
	acquisition time.Time
	sequence    uint64
	queuedEpoch uint64
	completed   bool
}
type Publisher struct {
	client          *Client
	uid             uint32
	descriptorBytes int // name/type plus the immutable per-handle descriptor
	name, typ       string
	opts            PublisherOptions
	active          bool
	closing         bool   // admitted close waits for the final value to enter the writer
	registeredEpoch uint64 // publish ordered into the current writer
	pending         []pendingSample
	pendingBytes    int
	lastError       error
	sequence        uint64
	sent            uint64
	errors          chan error
}

func (c *Client) Publish(name, typeName string, props map[string]any, o PublisherOptions) (*Publisher, error) {
	if c == nil {
		return nil, ErrInvalidHandle
	}
	if err := ValidateTopicName(name, c.opts.MaxNameBytes); err != nil {
		return nil, err
	}
	if _, err := TypeID(typeName); err != nil {
		return nil, err
	}
	if err := ValidatePublisherOptions(o, c.opts); err != nil {
		return nil, err
	}
	owned, n, err := OwnJSON(props, c.limits())
	if err != nil {
		return nil, err
	}
	if owned["cached"] == false && o.OfflineQueueCapacity > 0 {
		return nil, ErrIncompatibleOptions
	}
	var p *Publisher
	err = c.submit(context.Background(), n+len(name)+len(typeName), true, func(e *engine) error {
		if len(e.pubs) >= c.opts.MaxPublishers {
			return ErrQueueFull
		}
		t := e.topics[name]
		if t == nil && len(e.topics) >= c.opts.MaxTopics {
			return ErrQueueFull
		}
		if t != nil && t.requestedType != "" && t.requestedType != typeName {
			return ErrTypeConflict
		}
		if t != nil && t.observedType != "" && t.observedType != typeName {
			return ErrTypeConflict
		}
		requested := map[string]any{}
		if t != nil {
			requested = cloneJSONTrustedMap(t.requested)
		}
		mergeProps(requested, owned)
		// The topic is canonical across all publishers. A new handle cannot
		// disable caching while another handle owns accepted history, nor
		// enable offline history on an already uncached requested topic.
		if requested["cached"] == false {
			if o.OfflineQueueCapacity > 0 {
				return ErrIncompatibleOptions
			}
			if t != nil {
				for id := range t.members {
					if len(e.pubs[id].pending) > 0 {
						return ErrIncompatibleOptions
					}
				}
			}
		}
		requestedBytes := descriptorJSONBytes(requested)
		descriptorBytes := len(name) + len(typeName)
		oldBytes := 0
		if t != nil {
			oldBytes = t.requestedBytes
		}
		if e.retained+e.descriptorBytes+e.observedBytes+e.observedDescriptorBytes+e.propertyBytes+descriptorBytes+requestedBytes-oldBytes > c.opts.RetainedMaxBytes {
			e.status.Rejected++
			return ErrQueueFull
		}
		if e.nextPub >= 1<<31-1 {
			return ErrQueueFull
		}
		if e.session != nil && !e.registering {
			j := jobControl("publish", wire.Publish(name, int32(e.nextPub+1), typeName, requested).Params)
			j.publisher, j.registration = uint32(e.nextPub+1), true
			if err := e.admitControl(c.opts, j); err != nil {
				return err
			}
		}
		id, err := nextUID(&e.nextPub)
		if err != nil {
			return err
		}
		if t == nil {
			t = &topicRecord{name: name, requestedType: typeName, requested: map[string]any{}, observed: map[string]any{}, members: map[uint32]bool{}}
			for _, s := range e.subs {
				if s.matches(name) {
					t.routes = append(t.routes, s)
				}
			}
			e.topics[name] = t
		}
		t.requested = requested
		e.descriptorBytes += descriptorBytes + requestedBytes - t.requestedBytes
		t.requestedBytes = requestedBytes
		if !cached(t) {
			e.clearLatest(t)
		}
		p = &Publisher{client: c, uid: id, name: name, typ: typeName, opts: o, active: true, descriptorBytes: descriptorBytes, errors: make(chan error, 1)}
		e.pubs[id] = p
		e.pubIDs = append(e.pubIDs, id)
		t.members[id] = true
		return nil
	})
	return p, err
}
func (p *Publisher) check() error {
	if p == nil || p.client == nil || p.uid == 0 {
		return ErrInvalidHandle
	}
	return nil
}
func (p *Publisher) Set(v any) error { return p.SetContext(context.Background(), v) }
func (p *Publisher) SetContext(ctx context.Context, v any) error {
	return p.set(ctx, v, false, false, time.Time{}, false)
}
func (p *Publisher) TrySet(v any) error {
	return p.set(context.Background(), v, false, false, time.Time{}, true)
}
func (p *Publisher) SetDefault(v any) error {
	return p.set(context.Background(), v, true, false, time.Time{}, false)
}
func (p *Publisher) SetAt(at time.Time, v any) error {
	if at.IsZero() {
		return ErrTimestampUnrepresentable
	}
	return p.set(context.Background(), v, false, true, at, false)
}
func (p *Publisher) set(ctx context.Context, v any, weak, at bool, when time.Time, try bool) error {
	if err := p.check(); err != nil {
		return err
	}
	c := p.client
	owned, size, err := OwnValue(p.typ, v, c.opts.MaxBinaryBytes)
	if err != nil {
		return err
	}
	return c.submit(ctx, size+32, !try, func(e *engine) error {
		if !p.active {
			return ErrClosed
		}
		if e.pubs[p.uid] != p {
			return ErrForeignHandle
		}
		t := e.topics[p.name]
		if t == nil {
			return ErrInvalidHandle
		}
		if t.observedType != "" && t.observedType != p.typ {
			p.report(ErrTypeConflict)
			return ErrTypeConflict
		}
		var defaultProps map[string]any
		if weak {
			if _, explicit := t.requested["retained"]; !explicit {
				defaultProps = cloneJSONTrustedMap(t.requested)
				defaultProps["retained"] = true
				if e.retained+e.descriptorBytes+e.observedBytes+e.observedDescriptorBytes+e.propertyBytes+descriptorJSONBytes(defaultProps)-t.requestedBytes > c.opts.RetainedMaxBytes {
					return ErrQueueFull
				}
			}
		}
		isCached := cached(t)
		if !isCached && e.session == nil {
			return ErrIncompatibleOptions // uncached data cannot be retained across a disconnect
		}
		now := time.Now()
		timestamp := int64(1)
		if weak {
			timestamp = 0
		} else if e.session != nil && e.clock.hasBest {
			stamp := when
			if !at {
				stamp = now
			}
			var ok bool
			timestamp, ok = e.clock.mapAcquisition(e.epoch, stamp)
			if !ok {
				return ErrTimestampUnrepresentable
			}
		}
		wins := isCached && (t.latest == nil || t.latest.Stale || timestamp >= t.latest.Timestamp)
		if wins && !e.reserveLatest(t, size, c.opts.RetainedMaxBytes) {
			e.status.Rejected++
			return ErrQueueFull
		}
		// An offline nonwinning sample is not retained in latest-only mode.
		// Explicit history retains it; connected writes still enter the writer.
		queue := p.opts.OfflineQueueCapacity > 0 || e.session != nil && (!isCached || !wins)
		if queue {
			capacity, maxBytes := p.opts.OfflineQueueCapacity, p.opts.OfflineQueueMaxBytes
			if p.opts.OfflineQueueCapacity == 0 {
				capacity, maxBytes = 1, c.opts.WriterMaxBytes
			}
			if len(p.pending) >= capacity || size > maxBytes-p.pendingBytes || e.offlineItems >= c.opts.TotalOfflineCapacity || size > c.opts.TotalOfflineMaxBytes-e.offlineBytes {
				e.status.Rejected++
				return ErrQueueFull
			}
		}
		if defaultProps != nil && e.session != nil {
			j := jobControl("setproperties", wire.SetProperties(p.name, map[string]any{"retained": true}).Params)
			if err := e.admitControl(c.opts, j); err != nil {
				return err
			}
		}
		if defaultProps != nil {
			n := descriptorJSONBytes(defaultProps)
			e.descriptorBytes += n - t.requestedBytes
			t.requestedBytes = n
			t.requested = defaultProps
		}
		if !weak {
			t.localStrong = true
		}
		s := Sample{Value: owned, Timestamp: timestamp, Epoch: e.epoch, ReceivedAt: now}
		p.sequence++
		if wins {
			// Only an unscheduled latest-only local sample is coalesced.
			// Explicit offline history has its own ordered storage; jobs already
			// placed in the writer are not silently counted as discarded.
			if previous := e.pubs[t.latestOwner]; previous != nil && previous.sent < t.latestSequence && previous.opts.OfflineQueueCapacity == 0 {
				e.status.Dropped++
			}
			t.latestSequence = p.sequence
			t.latestOwner = p.uid
			e.setLatest(t, s, size)
			t.acquisition = when // zero for ordinary strong/default sets
		}
		if queue {
			p.pending = append(p.pending, pendingSample{sample: s, bytes: size, acquisition: when, sequence: p.sequence})
			p.pendingBytes += size
			e.offlineItems++
			e.offlineBytes += size
			e.status.OfflineItems = uint64(e.offlineItems)
			e.status.OfflineBytes = uint64(e.offlineBytes)
		}
		_ = at // acquisition instant is retained in pendingSample, not restamped
		return nil
	})
}
func (p *Publisher) DiscardPending() error {
	if err := p.check(); err != nil {
		return err
	}
	return p.client.call(func(e *engine) error {
		if e.pubs[p.uid] != p {
			return ErrForeignHandle
		}
		if !p.active {
			return ErrClosed
		}
		if len(p.pending) > 0 {
			if t := e.topics[p.name]; t != nil && t.latestOwner == p.uid && t.latestSequence == p.pending[len(p.pending)-1].sequence {
				e.clearLatest(t)
				t.latestOwner, t.latestSequence = 0, 0
			}
		}
		e.offlineItems -= len(p.pending)
		e.offlineBytes -= p.pendingBytes
		p.pending = nil
		p.pendingBytes = 0
		e.status.OfflineItems = uint64(e.offlineItems)
		e.status.OfflineBytes = uint64(e.offlineBytes)
		return nil
	})
}

// Errors is an independent coalescing asynchronous notification channel.
// The owner calls report on conflicting remote announcements and mapping failures.
func (p *Publisher) Errors() <-chan error {
	if p == nil {
		return nil
	}
	return p.errors
}
func (p *Publisher) report(err error) {
	p.lastError = err
	select {
	case p.errors <- err:
	default:
		select {
		case <-p.errors:
		default:
		}
		p.errors <- err
	}
}
func (p *Publisher) Err() error {
	if err := p.check(); err != nil {
		return err
	}
	var result error
	err := p.client.call(func(e *engine) error {
		if e.pubs[p.uid] != p {
			return ErrForeignHandle
		}
		result = p.lastError
		return nil
	})
	if err != nil {
		return err
	}
	return result
}

// hasQueuedPublish distinguishes a live registration reserved in the control
// backlog from one that replay has not yet generated.
func (e *engine) hasQueuedPublish(uid uint32) bool {
	for _, j := range e.replay {
		if j.registration && j.publisher == uid {
			return true
		}
	}
	return false
}

func (e *engine) finishPublisher(p *Publisher) {
	e.descriptorBytes -= p.descriptorBytes
	e.offlineItems -= len(p.pending)
	e.offlineBytes -= p.pendingBytes
	p.pending = nil
	p.pendingBytes = 0
	delete(e.pubs, p.uid)
	e.pubIDs = removeUID(e.pubIDs, p.uid)
	if t := e.topics[p.name]; t != nil {
		delete(t.members, p.uid)
		if len(t.members) == 0 && !t.hasID {
			e.removeTopic(t)
		}
	}
	if p.closing {
		close(p.errors) // owner is the only sender; no future report after removal
	}
	p.closing = false
	e.status.OfflineItems = uint64(e.offlineItems)
	e.status.OfflineBytes = uint64(e.offlineBytes)
}

func (p *Publisher) Close() error {
	if err := p.check(); err != nil {
		return err
	}
	return p.client.call(func(e *engine) error {
		if !p.active {
			return ErrClosed
		}
		if e.pubs[p.uid] != p {
			return ErrForeignHandle
		}
		// Reserve reliable control capacity before changing the handle. A
		// saturated control backlog must reject close atomically.
		registered := e.session != nil && (p.registeredEpoch != 0 && p.registeredEpoch == e.epoch || e.hasQueuedPublish(p.uid))
		if registered {
			j := jobControl("unpublish", wire.Unpublish(int32(p.uid)).Params)
			if len(e.replay) >= p.client.opts.CommandCapacity || j.size > p.client.opts.CommandMaxBytes-e.replayBytes {
				e.status.Rejected++
				return ErrQueueFull
			}
		}
		// A close before replay reaches this UID needs no unpublish. A
		// registered publisher must retain its accepted final sample until
		// it has entered the ordered writer ahead of unpublish.
		p.active = false
		p.closing = true
		if e.session == nil || (p.registeredEpoch == 0 || p.registeredEpoch != e.epoch) && !e.hasQueuedPublish(p.uid) {
			e.finishPublisher(p)
		}
		return nil
	})
}
