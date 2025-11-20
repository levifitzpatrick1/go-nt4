package nt4

import (
	"testing"
	"time"
)

func TestSubscriptionSetAndGetCallback(t *testing.T) {
	sub := &Subscription{
		UID:    1,
		Topics: []string{"/test"},
	}

	called := false
	callback := func(topic *Topic, timestamp int64, value any) {
		called = true
	}

	sub.SetCallback(callback)

	cb := sub.GetCallback()
	if cb == nil {
		t.Fatal("Expected callback to be set")
	}

	// Test callback execution
	cb(nil, 0, nil)
	if !called {
		t.Error("Expected callback to be called")
	}
}

func TestSubscriptionGetCallbackNil(t *testing.T) {
	sub := &Subscription{
		UID:    1,
		Topics: []string{"/test"},
	}

	cb := sub.GetCallback()
	if cb != nil {
		t.Error("Expected callback to be nil")
	}
}

func TestSubscriptionUpdatesChannel(t *testing.T) {
	sub := &Subscription{
		UID:    1,
		Topics: []string{"/test"},
	}
	sub.InitUpdates(10)

	// Test that Updates() returns a receive-only channel
	updatesChan := sub.Updates()
	if updatesChan == nil {
		t.Fatal("Expected updates channel to be initialized")
	}

	// Send an update
	topic := &Topic{Name: "/test"}
	update := TopicUpdate{
		Topic:     topic,
		Timestamp: 12345,
		Value:     42.0,
	}

	sent := sub.SendUpdate(update)
	if !sent {
		t.Error("Expected update to be sent")
	}

	// Receive the update
	select {
	case received := <-updatesChan:
		if received.Topic.Name != topic.Name {
			t.Errorf("Expected topic name %s, got %s", topic.Name, received.Topic.Name)
		}
		if received.Timestamp != 12345 {
			t.Errorf("Expected timestamp 12345, got %d", received.Timestamp)
		}
		if received.Value != 42.0 {
			t.Errorf("Expected value 42.0, got %v", received.Value)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for update")
	}
}

func TestSubscriptionSendUpdateFull(t *testing.T) {
	sub := &Subscription{
		UID:    1,
		Topics: []string{"/test"},
	}
	sub.InitUpdates(1) // Buffer size of 1

	topic := &Topic{Name: "/test"}
	update1 := TopicUpdate{Topic: topic, Timestamp: 1, Value: 1}
	update2 := TopicUpdate{Topic: topic, Timestamp: 2, Value: 2}

	// First send should succeed
	if !sub.SendUpdate(update1) {
		t.Error("Expected first update to be sent")
	}

	// Second send should fail (channel full)
	if sub.SendUpdate(update2) {
		t.Error("Expected second update to fail (channel full)")
	}
}

func TestSubscriptionCloseUpdates(t *testing.T) {
	sub := &Subscription{
		UID:    1,
		Topics: []string{"/test"},
	}
	sub.InitUpdates(10)

	sub.CloseUpdates()

	// Reading from closed channel should return zero value and false
	updatesChan := sub.Updates()
	_, ok := <-updatesChan
	if ok {
		t.Error("Expected channel to be closed")
	}
}
