package nt4

import (
	"math"
	"time"
)

// probeSent is posted by the writer to the SAME ordered inbound FIFO as reader
// events, before writing the already-stamped RTT frame. The owner alone calls
// recordProbe; the writer must never mutate sessionClock.
type probeSent struct {
	epoch uint64
	echo  int64
	sent  time.Time
}

const maxClockProbes = 4

type clockProbe struct {
	echo int64
	sent time.Time
}

// sessionClock is owned exclusively by the client owner. origin is immutable
// for the entire client lifetime; reset changes only epoch-local state.
// All instants passed to this API must retain the same monotonic time base.
type sessionClock struct {
	origin          time.Time
	epoch           uint64
	probes          []clockProbe // never more than maxClockProbes
	lastEcho        int64
	hasEcho         bool
	hasBest         bool
	bestRTT         time.Duration
	serverAtReceive int64
	localReceive    time.Time
	lastResponse    time.Time
	hasResponse     bool
}

func newSessionClock(origin time.Time) sessionClock {
	return sessionClock{origin: origin}
}

func (c *sessionClock) reset(epoch uint64) {
	c.epoch = epoch
	c.probes = nil
	c.lastEcho = 0
	c.hasEcho = false
	c.hasBest = false
	c.bestRTT = 0
	c.serverAtReceive = 0
	c.localReceive = time.Time{}
	c.lastResponse = time.Time{}
	c.hasResponse = false
}

// elapsedMicro returns a signed, floored microsecond duration. Saturated
// time.Sub results are rejected: they cannot represent the actual interval.
func elapsedMicro(a, b time.Time) (int64, bool) {
	d := a.Sub(b)
	if d == time.Duration(math.MaxInt64) || d == time.Duration(math.MinInt64) {
		return 0, false
	}
	n := int64(d)
	q := n / 1000
	if n < 0 && n%1000 != 0 {
		q--
	}
	return q, true
}

// probeEcho is stamped by the writer immediately before the socket write.
// A repeated microsecond echo cannot be sent in this epoch; retry on a later
// tick. Do not stamp while a job waits in the writer queue.
func probeEcho(origin, now time.Time) (int64, bool) {
	echo, ok := elapsedMicro(now, origin)
	return echo, ok && echo >= 0
}

// recordProbe is called only by the owner on the FIFO probeSent event. The
// writer posts the event before its socket write; a reply therefore cannot
// overtake registration. The caller must not write a probe rejected here.
func (c *sessionClock) recordProbe(p probeSent) bool {
	if p.epoch != c.epoch || len(c.probes) >= maxClockProbes {
		return false
	}
	echo, ok := elapsedMicro(p.sent, c.origin)
	if !ok || echo < 0 || echo != p.echo || (c.hasEcho && echo <= c.lastEcho) {
		return false
	}
	c.probes = append(c.probes, clockProbe{echo, p.sent})
	c.lastEcho, c.hasEcho = echo, true
	return true
}

// expireProbes reclaims bounded slots after the configured reply timeout.
// Expired echoes remain invalid even if a late reply arrives.
func (c *sessionClock) expireProbes(now time.Time, timeout time.Duration) int {
	if timeout <= 0 {
		return 0
	}
	kept := c.probes[:0]
	for _, p := range c.probes {
		age := now.Sub(p.sent)
		if age < timeout || now.Before(p.sent) {
			kept = append(kept, p)
		}
	}
	removed := len(c.probes) - len(kept)
	c.probes = kept
	return removed
}

// receiveProbe accepts only an outstanding echo for the active epoch. The
// reader captures received before dispatch. Each valid reply refreshes 4.0
// liveness, including replies whose RTT is worse than the best estimate.
// 4.1 pong liveness is independent and must not consult lastResponse.
func (c *sessionClock) receiveProbe(epoch uint64, server, echo int64, received time.Time) bool {
	if epoch != c.epoch || server < 0 {
		return false
	}
	idx := -1
	for i, p := range c.probes {
		if p.echo == echo {
			idx = i
			break
		}
	}
	if idx < 0 || received.Before(c.probes[idx].sent) {
		return false
	}
	rtt := received.Sub(c.probes[idx].sent)
	if rtt < 0 || rtt == time.Duration(math.MaxInt64) || rtt == time.Duration(math.MinInt64) {
		return false
	}
	half := int64(rtt / time.Microsecond / 2)
	if server > math.MaxInt64-half {
		return false
	}
	// Do not allow a saturated origin difference to become a false mapping.
	if _, ok := elapsedMicro(received, c.origin); !ok {
		return false
	}
	c.probes = append(c.probes[:idx], c.probes[idx+1:]...)
	if !c.hasResponse || received.After(c.lastResponse) {
		c.lastResponse = received
	}
	c.hasResponse = true
	if !c.hasBest || rtt < c.bestRTT {
		c.hasBest = true
		c.bestRTT = rtt
		c.serverAtReceive = server + half
		c.localReceive = received
	}
	return true
}

// mapAcquisition converts an owned acquisition instant, never replacing it
// with 'now'. It accepts instants before the LOCAL origin if their mapped
// server uptime is nonnegative. false maps to ErrTimestampUnrepresentable at
// the public boundary (or to not-ready if hasBest is false).
func (c *sessionClock) mapAcquisition(epoch uint64, at time.Time) (int64, bool) {
	if epoch != c.epoch || !c.hasBest {
		return 0, false
	}
	delta, ok := elapsedMicro(at, c.localReceive)
	if !ok || (delta > 0 && c.serverAtReceive > math.MaxInt64-delta) || (delta < 0 && c.serverAtReceive < -delta) {
		return 0, false
	}
	return c.serverAtReceive + delta, true
}
