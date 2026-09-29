package nt4

import (
	"context"
	"github.com/gorilla/websocket"
	"github.com/levifitzpatrick1/go-nt4/internal/wire"
	"sort"
	"time"
)

func (c *Client) beginDial(e *engine) {
	if c.ctx.Err() != nil {
		return
	}
	e.epoch++
	for _, t := range e.topics {
		t.epoch = e.epoch
	}
	e.status.Epoch = e.epoch
	e.status.State = StateDialing
	e.status.Ready = false
	e.status.ClockValid = false
	e.status.RTT = 0
	e.status.ClockOffset = 0
	e.clock.reset(e.epoch)
	ctx, cancel := context.WithTimeout(c.ctx, c.opts.DialTimeout)
	epoch := e.epoch
	c.workers.Add(1)
	go func() { defer cancel(); c.dial(ctx, epoch) }()
	c.update(e)
}
func (c *Client) onDial(e *engine, ev sessionEvent) {
	if ev.epoch != e.epoch || c.ctx.Err() != nil || e.status.State != StateDialing {
		if ev.conn != nil {
			ev.conn.Close()
		}
		return
	}
	if ev.err != nil {
		c.loss(e, ev.err)
		return
	}
	ctx, cancel := context.WithCancel(c.ctx)
	s := &session{epoch: ev.epoch, conn: ev.conn, protocol: ev.protocol, jobs: make(chan writeJob, c.opts.WriterCapacity), stop: cancel, ctx: ctx, lastPong: time.Now(), lastPing: time.Now(), lastRTT: time.Now(), closeDone: make(chan struct{})}
	e.session = s
	e.status.Protocol = ev.protocol
	// Probe first. Replay is maintained as an owner-owned cursor; tiny writer queues do not drop registrations.
	s.probeQueued = true
	e.replay = append(e.replay, writeJob{kind: websocket.BinaryMessage, probe: true, size: 32})
	e.replayBytes += 32
	e.registering = true
	e.registrationPub, e.registrationSub = 0, 0
	s.done.Add(3)
	c.sessionWorkers.Add(3)
	go c.readSession(s)
	go c.writeSession(s)
	go func() {
		defer s.done.Done()
		defer c.sessionWorkers.Done()
		defer close(s.closeDone)
		<-ctx.Done()
		s.conn.Close()
	}()
	c.flush(e)
}

// admitControl reserves a bounded reliable control slot before desired state changes.
func (e *engine) admitControl(o ClientOptions, j writeJob) error {
	if j.size > o.WriterMaxBytes || len(e.replay) >= o.CommandCapacity || j.size > o.CommandMaxBytes-e.replayBytes {
		e.status.Rejected++
		return ErrQueueFull
	}
	e.replay = append(e.replay, j)
	e.replayBytes += j.size
	return nil
}

// removeUID keeps registration cursors bounded by live registrations, not
// lifetime UID churn; UIDs are allocated monotonically and never reused.
func removeUID(ids []uint32, uid uint32) []uint32 {
	i := sort.Search(len(ids), func(i int) bool { return ids[i] >= uid })
	if i < len(ids) && ids[i] == uid {
		copy(ids[i:], ids[i+1:])
		ids[len(ids)-1] = 0
		return ids[:len(ids)-1]
	}
	return ids
}

func (c *Client) flush(e *engine) {
	s := e.session
	if s == nil {
		return
	}
	defer func() { e.status.WriterItems = uint64(s.writerItems); e.status.WriterBytes = uint64(s.writerBytes) }()
	for !e.registering && len(e.pendingUnsubs) > 0 {
		j := jobControl("unsubscribe", wire.Unsubscribe(int32(e.pendingUnsubs[0])).Params)
		if len(e.replay) >= c.opts.CommandCapacity || j.size > c.opts.CommandMaxBytes-e.replayBytes {
			break
		}
		if e.admitControl(c.opts, j) != nil {
			break
		}
		e.pendingUnsubs[0] = 0
		e.pendingUnsubs = e.pendingUnsubs[1:]
	}
	// While generating registrations, only the initial probe may pass the
	// registration cursor. Live controls accepted meanwhile stay behind it.
	for len(e.replay) > 0 && (!e.registering || e.replay[0].probe) {
		if e.replay[0].size > c.opts.WriterMaxBytes {
			c.loss(e, ErrQueueFull)
			return
		}
		if !s.enqueue(c.opts, e.replay[0]) {
			break
		}
		if p := e.pubs[e.replay[0].publisher]; p != nil && e.replay[0].registration {
			p.registeredEpoch = e.epoch
		}
		e.replayBytes -= e.replay[0].size
		e.replay[0] = writeJob{}
		e.replay = e.replay[1:]
	}
	// The initial probe is a strict ordering barrier even when the writer is
	// blocked; never generate registrations past an unscheduled probe.
	if e.registering && len(e.replay) > 0 && e.replay[0].probe {
		return
	}
	// Generate replay from live registries, one job at a time. Removed handles
	// cannot be resurrected by a stale serialized replay snapshot.
	for e.registering {
		var next uint32
		if i := sort.Search(len(e.pubIDs), func(i int) bool { return e.pubIDs[i] > e.registrationPub }); i < len(e.pubIDs) {
			next = e.pubIDs[i]
		}
		if next != 0 {
			p := e.pubs[next]
			if p.closing {
				e.registrationPub = next
				continue
			}
			t := e.topics[p.name]
			j := jobControl("publish", wire.Publish(p.name, int32(next), p.typ, t.requested).Params)
			if j.size > c.opts.WriterMaxBytes {
				c.loss(e, ErrQueueFull)
				return
			}
			if !s.enqueue(c.opts, j) {
				break
			}
			p.registeredEpoch = e.epoch
			e.registrationPub = next
			continue
		}
		if i := sort.Search(len(e.subIDs), func(i int) bool { return e.subIDs[i] > e.registrationSub }); i < len(e.subIDs) {
			next = e.subIDs[i]
		}
		if next != 0 {
			sub := e.subs[next]
			opts := map[string]any{"prefix": sub.opts.Prefix, "all": sub.opts.All, "topicsonly": sub.opts.TopicsOnly}
			if sub.opts.Periodic > 0 {
				opts["periodic"] = sub.opts.Periodic.Seconds()
			}
			j := jobControl("subscribe", wire.Subscribe(sub.patterns, int32(next), opts).Params)
			if j.size > c.opts.WriterMaxBytes {
				c.loss(e, ErrQueueFull)
				return
			}
			if !s.enqueue(c.opts, j) {
				break
			}
			e.registrationSub = next
			continue
		}
		e.registering = false
		if len(e.replay) > 0 || len(e.pendingUnsubs) > 0 {
			c.flush(e)
			return
		}
	}
	if len(e.replay) == 0 && !e.registering {
		// Restore property intent after registration and before any cached value.
		// A control stays pending until its matching successful writer completion.
		names := make([]string, 0, len(e.topics))
		for name := range e.topics {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			t := e.topics[name]
			if len(t.propertyPatch) == 0 || t.propertyInFlight {
				continue
			}
			j := jobControl("setproperties", wire.SetProperties(name, t.propertyPatch).Params)
			j.propertyName, j.propertyVersion, j.propertyWrite = name, t.propertyVersion, true
			if j.size > c.opts.WriterMaxBytes {
				c.loss(e, ErrQueueFull)
				return
			}
			if !s.enqueue(c.opts, j) {
				return
			}
			t.propertyInFlight, t.propertyQueued = true, t.propertyVersion
		}
		if e.status.State == StateDialing {
			e.status.State = StateOnlineUnsynchronized
			c.update(e)
		}
		c.flushValues(e)
	}
}
func (c *Client) flushValues(e *engine) {
	s := e.session
	if s == nil {
		return
	}
	ids := make([]int, 0, len(e.pubs))
	for id := range e.pubs {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, id := range ids {
		p := e.pubs[uint32(id)]
		if !p.active && !p.closing || p.registeredEpoch != e.epoch {
			continue
		}
		t := e.topics[p.name]
		if t == nil || t.observedType != "" && t.observedType != p.typ {
			continue
		}
		// Only one member sends the canonical latest. A remote update can be
		// that latest; received strength never changes localStrong.
		first := true
		for member := range t.members {
			if member < p.uid {
				first = false
				break
			}
		}
		if first && t.replayedEpoch != e.epoch && t.latest != nil && cached(t) {
			at := t.acquisition
			if t.latestOwner == 0 {
				at = time.Time{}
			} // remote value has no local acquisition instant
			if !c.sendSample(e, p, *t.latest, at, t.latestOwner, t.latestSequence) {
				return
			}
			t.replayedEpoch = e.epoch
			t.replayedOwner, t.replayedSequence = t.latestOwner, t.latestSequence
			if owner := e.pubs[t.latestOwner]; owner != nil && len(owner.pending) == 0 {
				owner.sent = t.latestSequence
			}
		}
		if t.replayedEpoch != e.epoch && t.latest != nil && cached(t) {
			continue
		}
		for i := range p.pending {
			item := &p.pending[i]
			if item.queuedEpoch == e.epoch || item.completed {
				continue
			}
			// Canonical replay already placed this sample in the writer.
			if t.replayedOwner == p.uid && t.replayedSequence == item.sequence && t.replayedEpoch == e.epoch {
				item.queuedEpoch = e.epoch
				p.sent = item.sequence
				continue
			}
			if !c.sendSample(e, p, item.sample, item.acquisition, p.uid, item.sequence) {
				return
			}
			item.queuedEpoch = e.epoch
			p.sent = item.sequence
		}
		if len(p.pending) == 0 && p.sequence > p.sent && t.latestOwner == p.uid && t.latest != nil && cached(t) {
			if !c.sendSample(e, p, *t.latest, t.acquisition, p.uid, p.sequence) {
				return
			}
			p.sent = p.sequence
		}
		if p.closing {
			j := jobControl("unpublish", wire.Unpublish(int32(p.uid)).Params)
			if !s.enqueue(c.opts, j) {
				return
			}
			e.finishPublisher(p)
		}
	}
}

// sendSample does not consume a pending item unless it has entered the writer.
// Active unmappable SetAt samples remain bounded for explicit discard. A closing
// publisher cannot wait for a timestamp that will never become representable.
func (e *engine) discardClosingSample(p *Publisher) bool {
	if !p.closing {
		return false
	}
	if t := e.topics[p.name]; t != nil && t.latestOwner == p.uid {
		e.clearLatest(t)
		t.latestOwner, t.latestSequence = 0, 0
	}
	e.offlineItems -= len(p.pending)
	e.offlineBytes -= p.pendingBytes
	p.pending = nil
	p.pendingBytes = 0
	p.sent = p.sequence
	e.status.OfflineItems, e.status.OfflineBytes = uint64(e.offlineItems), uint64(e.offlineBytes)
	return true
}
func (c *Client) sendSample(e *engine, p *Publisher, sample Sample, at time.Time, owner uint32, sequence uint64) bool {
	ts := int64(0)
	if sample.Timestamp != 0 {
		if !e.clock.hasBest {
			return false
		}
		if at.IsZero() {
			at = time.Now()
		} // ordinary strong or remote canonical replay
		var ok bool
		ts, ok = e.clock.mapAcquisition(e.epoch, at)
		if !ok {
			p.report(ErrTimestampUnrepresentable)
			return e.discardClosingSample(p)
		}
	}
	typ, _ := TypeID(p.typ)
	data, err := wire.EncodeValue(int32(p.uid), ts, typ, sample.Value)
	if err != nil {
		p.report(err)
		return e.discardClosingSample(p)
	}
	j := writeJob{kind: websocket.BinaryMessage, data: data, size: len(data), valueOwner: owner, valueSequence: sequence}
	if j.size > c.opts.WriterMaxBytes {
		p.report(ErrQueueFull)
		return e.discardClosingSample(p)
	}
	return e.session.enqueue(c.opts, j)
}
func (c *Client) stopSession(e *engine) {
	if e.session != nil {
		s := e.session
		s.stop()
		s.conn.Close()
		s.done.Wait()
		e.session = nil
	}
	e.replay = nil
	e.replayBytes = 0
	e.registering = false
	e.pendingUnsubs = nil
	for _, t := range e.topics {
		t.propertyInFlight = false
	}
	e.status.WriterItems, e.status.WriterBytes = 0, 0
}
func (c *Client) loss(e *engine, err error) {
	if e.session != nil {
		// Only jobs taken by the writer may have reached the socket. Jobs
		// still queued in memory were never attempted.
		e.status.Uncertain += uint64(e.session.started.Load())
		e.session.stop()
		e.session.conn.Close()
		e.session = nil
		e.status.WriterItems, e.status.WriterBytes = 0, 0
	}
	e.replay = nil
	e.replayBytes = 0
	e.registering = false
	e.pendingUnsubs = nil
	for _, t := range e.topics {
		t.propertyInFlight = false
	}
	for _, p := range e.pubs {
		if p.closing {
			e.finishPublisher(p)
			continue
		}
		p.registeredEpoch = 0
		p.sent = 0
		for i := range p.pending {
			p.pending[i].queuedEpoch = 0
		}
		if t := e.topics[p.name]; t != nil && !cached(t) {
			e.offlineItems -= len(p.pending)
			e.offlineBytes -= p.pendingBytes
			p.pending = nil
			p.pendingBytes = 0
		}
	}
	e.status.OfflineItems, e.status.OfflineBytes = uint64(e.offlineItems), uint64(e.offlineBytes)
	e.status.LastError = err
	e.status.LastLoss = err
	e.status.Ready = false
	e.status.ClockValid = false
	e.status.Protocol = ""
	e.clock.reset(e.epoch)
	e.ids = make(map[int32]*topicRecord)
	for _, t := range e.topics {
		was := t.hasID
		t.hasID = false
		t.epoch = e.epoch
		t.stale = true
		if len(t.members) == 0 && t.observed["retained"] != true && t.observed["persistent"] != true {
			if was {
				e.route(Event{Kind: LocalInvalidated, Topic: t.snapshot(), Epoch: e.epoch, ReceivedAt: time.Now()})
			}
			e.removeTopic(t)
			continue
		}
		if len(t.members) > 0 && t.latest != nil {
			if t.localStrong {
				t.latest.Timestamp = 1
			} else {
				t.latest.Timestamp = 0
			}
			t.latest.Stale = true
		} else if len(t.members) == 0 && t.latest != nil {
			t.latest.Stale = true
		}
		if was {
			e.route(Event{Kind: LocalInvalidated, Topic: t.snapshot(), Epoch: e.epoch, ReceivedAt: time.Now()})
		}
	}
	e.status.State = StateBackoff
	if e.backoff == 0 {
		e.backoff = c.opts.RetryMin
	} else if e.backoff >= c.opts.RetryMax/2 {
		e.backoff = c.opts.RetryMax
	} else {
		e.backoff *= 2
	}
	e.retry = time.Now().Add(e.backoff)
	c.update(e)
}
func (c *Client) tick(e *engine) {
	if c.ctx.Err() != nil || !e.started {
		return
	}
	now := time.Now()
	if e.status.State == StateBackoff && !now.Before(e.retry) {
		c.beginDial(e)
		return
	}
	s := e.session
	if s == nil {
		return
	}
	e.clock.expireProbes(now, c.opts.ReadTimeout)
	if s.protocol == protocol41 {
		if now.Sub(s.lastPong) > c.opts.ReadTimeout {
			c.loss(e, ErrProtocol)
			return
		}
		if now.Sub(s.lastPing) >= c.opts.KeepaliveInterval {
			if s.enqueue(c.opts, writeJob{kind: websocket.PingMessage, size: 1}) {
				s.lastPing = now
			}
		}
	} else if (!e.clock.hasResponse && now.Sub(s.lastRTT) > c.opts.ReadTimeout) || e.clock.hasResponse && now.Sub(e.clock.lastResponse) > c.opts.ReadTimeout {
		c.loss(e, ErrProtocol)
		return
	}
	if now.Sub(s.lastRTT) >= c.opts.KeepaliveInterval && !s.probeQueued && len(e.clock.probes) < maxClockProbes {
		s.probeQueued = true
		s.lastRTT = now
		if len(e.replay) < c.opts.CommandCapacity && e.replayBytes+32 <= c.opts.CommandMaxBytes {
			e.replay = append(e.replay, writeJob{kind: websocket.BinaryMessage, probe: true, size: 32})
			e.replayBytes += 32
		} else {
			s.probeQueued = false
		}
		c.flush(e)
	}
}
func (c *Client) update(e *engine) {
	c.mu.Lock()
	queuedBytes := c.status.CommandBytes
	c.status = e.status
	c.status.Rejected += c.localRejected
	c.status.CommandBytes = queuedBytes
	c.status.CommandItems = uint64(len(c.commands))
	close(c.wake)
	c.wake = make(chan struct{})
	c.mu.Unlock()
	c.notify(e)
}
func (c *Client) onInbound(e *engine, ev sessionEvent) {
	if ev.kind == "failure" {
		c.loss(e, ev.err)
		return
	}
	if ev.kind == "pong" {
		e.session.lastPong = ev.at
		return
	}
	if ev.kind == "control" {
		for _, v := range ev.controls {
			c.control(e, v, ev.at)
		}
		return
	}
	f := ev.frame
	if f.TopicID == -1 {
		echo, ok := f.Value.(int64)
		if f.TypeID != wire.DataTypeInt || !ok {
			return
		}
		if e.clock.receiveProbe(e.epoch, f.Timestamp, echo, ev.at) {
			e.status.Ready = true
			e.status.ClockValid = true
			e.status.RTT = e.clock.bestRTT
			if local, ok := elapsedMicro(e.clock.localReceive, c.origin); ok {
				offset := e.clock.serverAtReceive - local
				if offset >= -int64((1<<63-1)/1000) && offset <= int64((1<<63-1)/1000) {
					e.status.ClockOffset = time.Duration(offset) * time.Microsecond
				}
			}
			e.status.State = StateOnlineReady
			e.backoff = 0
			c.update(e)
			c.flush(e)
		}
		return
	}
	if t := e.ids[f.TopicID]; t != nil {
		typ, err := TypeID(t.observedType)
		if err != nil || typ != f.TypeID {
			return
		}
		sample := Sample{Value: f.Value, Timestamp: f.Timestamp, Epoch: e.epoch, ReceivedAt: ev.at}
		if cached(t) && (t.latest == nil || t.latest.Stale || f.Timestamp >= t.latest.Timestamp) {
			size := ValueSize(f.Value)
			if e.reserveLatest(t, size, c.opts.RetainedMaxBytes) {
				e.setLatest(t, sample, size)
				t.latestOwner = 0
				t.acquisition = time.Time{}
			}
		}
		e.route(Event{Kind: ValueReceived, Topic: t.snapshot(), Sample: sample, Epoch: e.epoch, ReceivedAt: ev.at})
		return
	}
}
func (c *Client) control(e *engine, v controlEvent, now time.Time) {
	t := e.topics[v.name]
	switch v.method {
	case "announce":
		n := descriptorJSONBytes(v.props)
		// An announce owns remote name/type storage even with empty properties.
		// Check the complete replacement before touching IDs, caches or routes.
		descriptor := len(v.name) + len(v.typ)
		old, oldDescriptor := 0, 0
		if t != nil {
			old, oldDescriptor = t.observedBytes, t.observedDescriptorBytes
		}
		if n+descriptor > c.opts.RetainedMaxBytes-e.retained-e.descriptorBytes-e.propertyBytes-e.observedBytes-e.observedDescriptorBytes+old+oldDescriptor {
			e.status.Rejected++
			return
		}
		if t == nil {
			if len(e.topics) >= c.opts.MaxTopics {
				return
			}
			t = &topicRecord{name: v.name, members: map[uint32]bool{}, requested: map[string]any{}}
			e.topics[v.name] = t
			for _, s := range e.subs {
				if s.matches(v.name) {
					t.routes = append(t.routes, s)
				}
			}
		}
		if prior := e.ids[v.id]; prior != nil && prior != t {
			prior.hasID = false
			e.clearLatest(prior)
		}
		if t.hasID && t.id != v.id {
			delete(e.ids, t.id)
			e.clearLatest(t)
		}
		if t.observedType != "" && t.observedType != v.typ {
			e.clearLatest(t)
		}
		t.id = v.id
		t.epoch = e.epoch
		e.ids[v.id] = t
		t.hasID = true
		t.stale = false
		t.observedType = v.typ
		e.observedBytes += n - t.observedBytes
		t.observedBytes = n
		e.observedDescriptorBytes += descriptor - t.observedDescriptorBytes
		t.observedDescriptorBytes = descriptor
		t.observed = v.props
		for id := range t.members {
			if e.pubs[id].typ != v.typ {
				e.pubs[id].report(ErrTypeConflict)
			}
		}
		e.route(Event{Kind: Announced, Topic: t.snapshot(), Epoch: e.epoch, ReceivedAt: now})
		if c.opts.OnTopicAnnounce != nil {
			snap := t.snapshot()
			go c.opts.OnTopicAnnounce(&Topic{
				Name:       snap.Name,
				Type:       snap.Type,
				ID:         snap.ID,
				Properties: snap.Properties,
			})
		}
	case "properties":
		if t == nil || !t.hasID {
			return
		}
		next := cloneJSONTrustedMap(t.observed)
		mergeProps(next, v.props)
		n := descriptorJSONBytes(next)
		if n > c.opts.RetainedMaxBytes-e.retained-e.descriptorBytes-e.propertyBytes-e.observedBytes-e.observedDescriptorBytes+t.observedBytes {
			e.status.Rejected++
			return
		}
		e.observedBytes += n - t.observedBytes
		t.observedBytes = n
		t.observed = next
		if !cached(t) {
			e.clearLatest(t)
		}
		e.route(Event{Kind: PropertiesChanged, Topic: t.snapshot(), Epoch: e.epoch, ReceivedAt: now, Ack: v.ack})
	case "unannounce":
		if t == nil || !t.hasID || t.id != v.id {
			return
		}
		snap := t.snapshot()
		e.route(Event{Kind: ServerUnannounced, Topic: snap, Epoch: e.epoch, ReceivedAt: now})
		if c.opts.OnTopicUnannounce != nil {
			go c.opts.OnTopicUnannounce(&Topic{
				Name:       snap.Name,
				Type:       snap.Type,
				ID:         snap.ID,
				Properties: snap.Properties,
			})
		}
		delete(e.ids, v.id)
		t.hasID = false
		t.observedType = ""
		if len(t.members) != 0 {
			e.observedBytes -= t.observedBytes
			t.observedBytes = 0
			e.observedDescriptorBytes -= t.observedDescriptorBytes
			t.observedDescriptorBytes = 0
			t.observed = nil
		}
		e.clearLatest(t)
		t.latestOwner, t.latestSequence = 0, 0
		for id := range t.members {
			p := e.pubs[id]
			p.sent = p.sequence
			e.offlineItems -= len(p.pending)
			e.offlineBytes -= p.pendingBytes
			p.pending = nil
			p.pendingBytes = 0
		}
		e.status.OfflineItems, e.status.OfflineBytes = uint64(e.offlineItems), uint64(e.offlineBytes)
		if len(t.members) == 0 {
			e.removeTopic(t)
		}
	}
}
