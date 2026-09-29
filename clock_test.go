package nt4

import (
	"math"
	"testing"
	"time"
)

func clockFixture() (sessionClock, time.Time) {
	origin := time.Now()
	c := newSessionClock(origin)
	c.reset(7)
	return c, origin
}

func sendProbe(t *testing.T, c *sessionClock, at time.Time) int64 {
	t.Helper()
	echo, ok := probeEcho(c.origin, at)
	if !ok || !c.recordProbe(probeSent{c.epoch, echo, at}) {
		t.Fatalf("probe rejected at %v", at)
	}
	return echo
}

func TestClockZeroAndBest(t *testing.T) {
	c, base := clockFixture()
	sent := base.Add(time.Millisecond)
	echo := sendProbe(t, &c, sent)
	if !c.receiveProbe(7, 0, echo, sent) || !c.hasBest || c.bestRTT != 0 || c.serverAtReceive != 0 {
		t.Fatalf("zero RTT/server: %+v", c)
	}
	if ts, ok := c.mapAcquisition(7, sent); !ok || ts != 0 {
		t.Fatalf("zero map %d %v", ts, ok)
	}
	if c.receiveProbe(7, 1, echo, sent) {
		t.Fatal("duplicate")
	}
	echo = sendProbe(t, &c, base.Add(2*time.Millisecond))
	received := base.Add(6 * time.Millisecond)
	if !c.receiveProbe(7, 100, echo, received) {
		t.Fatal("worse reply rejected")
	}
	if c.bestRTT != 0 || c.serverAtReceive != 0 || !c.lastResponse.Equal(received) {
		t.Fatalf("worse overwrote best or failed liveness: %+v", c)
	}
	c.reset(8)
	if c.hasBest || c.hasResponse || len(c.probes) != 0 {
		t.Fatal("epoch leaked clock")
	}
	if _, ok := c.mapAcquisition(8, sent); ok {
		t.Fatal("unsynchronized map")
	}
}

func TestClockSignedAcquisition(t *testing.T) {
	c, base := clockFixture()
	sent := base.Add(2 * time.Second)
	echo := sendProbe(t, &c, sent)
	recv := sent.Add(4 * time.Millisecond)
	if !c.receiveProbe(7, 5_000_000, echo, recv) {
		t.Fatal("reply")
	}
	if c.serverAtReceive != 5_002_000 {
		t.Fatalf("plus half RTT: %d", c.serverAtReceive)
	}
	for _, tc := range []struct {
		at    time.Time
		want  int64
		valid bool
	}{
		{base.Add(-time.Second), 1_998_000, true}, // before local origin, after server origin
		{base.Add(-4 * time.Second), 0, false},
		{recv.Add(3 * time.Millisecond), 5_005_000, true},
	} {
		got, ok := c.mapAcquisition(7, tc.at)
		if ok != tc.valid || (ok && got != tc.want) {
			t.Fatalf("mapping %v: %d %v, want %d %v", tc.at, got, ok, tc.want, tc.valid)
		}
	}
	// Add carries the monotonic reading even when the wall field changes.
	shiftedWall := recv.Add(3 * time.Millisecond)
	if got, ok := c.mapAcquisition(7, shiftedWall); !ok || got != 5_005_000 {
		t.Fatal("monotonic conversion")
	}
	if _, ok := c.mapAcquisition(6, recv); ok {
		t.Fatal("old epoch")
	}
}

func TestClockAdmissionExpiryAndEpoch(t *testing.T) {
	c, base := clockFixture()
	if c.recordProbe(probeSent{6, 1, base.Add(time.Microsecond)}) || c.recordProbe(probeSent{7, -1, base}) {
		t.Fatal("bad probe accepted")
	}
	if c.recordProbe(probeSent{7, 3, base.Add(4 * time.Microsecond)}) {
		t.Fatal("mismatched echo")
	}
	for i := 1; i <= maxClockProbes; i++ {
		sendProbe(t, &c, base.Add(time.Duration(i)*time.Millisecond))
	}
	if c.recordProbe(probeSent{7, 5000, base.Add(5 * time.Millisecond)}) {
		t.Fatal("unbounded probes")
	}
	if c.receiveProbe(6, 1, 1000, base.Add(6*time.Millisecond)) || c.receiveProbe(7, 1, 999, base.Add(6*time.Millisecond)) {
		t.Fatal("old/unsolicited reply")
	}
	if c.receiveProbe(7, -1, 1000, base.Add(6*time.Millisecond)) || c.receiveProbe(7, 1, 1000, base) {
		t.Fatal("negative/early reply")
	}
	if n := c.expireProbes(base.Add(2500*time.Microsecond), time.Millisecond); n != 1 {
		t.Fatalf("expired %d", n)
	}
	if c.receiveProbe(7, 1, 1000, base.Add(3*time.Millisecond)) || c.recordProbe(probeSent{7, 1000, base.Add(time.Millisecond)}) {
		t.Fatal("stale echo")
	}
	sendProbe(t, &c, base.Add(5*time.Millisecond))
	c.reset(8)
	if c.receiveProbe(8, 2, 5000, base.Add(6*time.Millisecond)) || c.receiveProbe(7, 2, 5000, base.Add(6*time.Millisecond)) {
		t.Fatal("cross epoch reply")
	}
	sendProbe(t, &c, base.Add(5*time.Millisecond)) // echo may recur only in a new epoch
}

func TestClockArithmeticBounds(t *testing.T) {
	c, base := clockFixture()
	echo := sendProbe(t, &c, base.Add(time.Millisecond))
	if c.receiveProbe(7, math.MaxInt64, echo, base.Add(3*time.Millisecond)) {
		t.Fatal("server + RTT/2 overflow")
	}
	if !c.receiveProbe(7, math.MaxInt64-1000, echo, base.Add(3*time.Millisecond)) {
		t.Fatal("valid near-limit reply")
	}
	if _, ok := c.mapAcquisition(7, base.Add(4*time.Millisecond)); ok {
		t.Fatal("mapping overflow")
	}
	if _, ok := c.mapAcquisition(7, base.Add(-time.Duration(math.MaxInt64))); ok {
		t.Fatal("saturated elapsed")
	}
	c.reset(9)
	echo = sendProbe(t, &c, base.Add(time.Millisecond))
	if !c.receiveProbe(9, 0, echo, base.Add(time.Millisecond)) {
		t.Fatal("zero origin")
	}
	if _, ok := c.mapAcquisition(9, base.Add(time.Millisecond-time.Nanosecond)); ok {
		t.Fatal("pre-server submicro instant")
	}
	if _, ok := probeEcho(base, base.Add(time.Duration(math.MaxInt64))); ok {
		t.Fatal("saturated stamp")
	}
}
