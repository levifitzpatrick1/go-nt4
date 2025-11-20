package nt4

import "sync"

// Active subscription to one or more topics.
type Subscription struct {
	// Client assigned subscription ID.
	UID int32

	// List of topic paths subscribed to.
	Topics []string

	// Subscription config.
	Options SubscribeOptions

	callback func(topic *Topic, timestamp int64, value any)
	updates  chan TopicUpdate
	mu       sync.RWMutex
}

// Set callback for topic updates. Non-blocking.
func (sub *Subscription) SetCallback(callback func(topic *Topic, timestamp int64, value any)) {
	sub.mu.Lock()
	sub.callback = callback
	sub.mu.Unlock()
}

// Get the current callback function.
func (sub *Subscription) GetCallback() func(topic *Topic, timestamp int64, value any) {
	sub.mu.RLock()
	defer sub.mu.RUnlock()
	return sub.callback
}

// Get the updates channel for this subscription.
// Updates are sent to both the channel and any registered callback.
//
// Example:
//
//	for update := range subscription.Updates() {
//	    fmt.Printf("Topic: %s, Value: %v\n", update.Topic.Name, update.Value)
//	}
func (sub *Subscription) Updates() <-chan TopicUpdate {
	return sub.updates
}

// Creates the update channel with specified buffer size.
// Called internally when creating a subscription.
func (sub *Subscription) InitUpdates(bufferSize int) {
	sub.updates = make(chan TopicUpdate, bufferSize)
}

// Send an update to the channel.
// Returns true if sent, false if channel is full. Called internally.
func (sub *Subscription) SendUpdate(update TopicUpdate) bool {
	select {
	case sub.updates <- update:
		return true
	default:
		return false
	}
}

// Close the update channel. Called internally when unsubscribing.
func (sub *Subscription) CloseUpdates() {
	close(sub.updates)
}
