package nt4

import (
	"context"
	"time"
)

// Connect starts the client background connection and reconnection loop
// using context.Background().
func (c *Client) Connect() error {
	if c == nil {
		return ErrInvalidHandle
	}
	return c.Start(context.Background())
}

// Disconnect gracefully terminates all connections and worker goroutines.
func (c *Client) Disconnect() {
	if c != nil {
		c.Close()
	}
}

// IsConnected reports whether the client currently has an active connection to the server.
func (c *Client) IsConnected() bool {
	if c == nil {
		return false
	}
	st := c.Status().State
	return st == StateOnlineReady || st == StateOnlineUnsynchronized
}

// WaitForConnection blocks until the client is connected or the context expires.
func (c *Client) WaitForConnection(ctx context.Context) error {
	if c == nil {
		return ErrInvalidHandle
	}
	return c.WaitConnected(ctx)
}

// ConnectWithRetry starts the client and waits for the initial connection to be established.
func (c *Client) ConnectWithRetry(ctx context.Context) error {
	if c == nil {
		return ErrInvalidHandle
	}
	if err := c.Start(ctx); err != nil && err != ErrAlreadyStarted {
		return err
	}
	return c.WaitConnected(ctx)
}

func (c *Client) getOrCreateConveniencePublisher(name, typeStr string) (*Publisher, error) {
	c.convMu.Lock()
	defer c.convMu.Unlock()
	if c.convPubs == nil {
		c.convPubs = make(map[string]*Publisher)
	}
	if p, ok := c.convPubs[name]; ok && p.active {
		return p, nil
	}
	p, err := c.Publish(name, typeStr, nil, PublisherOptions{})
	if err != nil {
		return nil, err
	}
	c.convPubs[name] = p
	return p, nil
}

// PublishBoolean publishes or updates a boolean topic.
func (c *Client) PublishBoolean(name string, value bool) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeBoolean)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishDouble publishes or updates a 64-bit float topic.
func (c *Client) PublishDouble(name string, value float64) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeDouble)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishInt publishes or updates a 64-bit integer topic.
func (c *Client) PublishInt(name string, value int64) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeInt)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishFloat publishes or updates a 32-bit float topic.
func (c *Client) PublishFloat(name string, value float32) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeFloat)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishString publishes or updates a string topic.
func (c *Client) PublishString(name string, value string) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeString)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishDoubleArray publishes or updates a []float64 topic.
func (c *Client) PublishDoubleArray(name string, value []float64) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeDoubleArray)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishStringArray publishes or updates a []string topic.
func (c *Client) PublishStringArray(name string, value []string) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeStringArray)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishBooleanArray publishes or updates a []bool topic.
func (c *Client) PublishBooleanArray(name string, value []bool) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeBooleanArray)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishIntArray publishes or updates a []int64 topic.
func (c *Client) PublishIntArray(name string, value []int64) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeIntArray)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishFloatArray publishes or updates a []float32 topic.
func (c *Client) PublishFloatArray(name string, value []float32) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeFloatArray)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishRaw publishes or updates a raw binary []byte topic.
func (c *Client) PublishRaw(name string, value []byte) (*Publisher, error) {
	p, err := c.getOrCreateConveniencePublisher(name, TypeRaw)
	if err != nil {
		return nil, err
	}
	return p, p.Set(value)
}

// PublishTopic publishes a topic with default options.
func (c *Client) PublishTopic(name, typeStr string, props map[string]any) (*Publisher, error) {
	return c.Publish(name, typeStr, props, PublisherOptions{})
}

// Unpublish closes and removes a publisher handle.
func (c *Client) Unpublish(p *Publisher) error {
	if p == nil {
		return ErrInvalidHandle
	}
	return p.Close()
}

// SetValue updates the value of a topic or publisher handle.
// Target can be a *Publisher, *Topic, or topic name string.
func (c *Client) SetValue(target any, value any) error {
	if c == nil {
		return ErrInvalidHandle
	}
	switch t := target.(type) {
	case *Publisher:
		if t == nil {
			return ErrInvalidHandle
		}
		return t.Set(value)
	case *Topic:
		if t == nil {
			return ErrInvalidHandle
		}
		if t.Publisher != nil {
			return t.Publisher.Set(value)
		}
		pub, err := c.getOrCreateConveniencePublisher(t.Name, t.Type)
		if err != nil {
			return err
		}
		return pub.Set(value)
	case string:
		c.convMu.Lock()
		p, ok := c.convPubs[t]
		c.convMu.Unlock()
		if !ok || p == nil {
			return ErrInvalidHandle
		}
		return p.Set(value)
	default:
		return ErrInvalidHandle
	}
}

// SubscribeAndRetrieve temporarily subscribes to a topic, waits for its value, and returns it.
func (c *Client) SubscribeAndRetrieve(topic string, timeout time.Duration) any {
	if c == nil {
		return nil
	}
	if sample, ok := c.Latest(topic); ok && !sample.Stale {
		return sample.Value
	}
	sub, err := c.Subscribe([]string{topic}, SubscriptionOptions{BufferCapacity: 16})
	if err != nil {
		return nil
	}
	defer sub.Close()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return nil
		case ev, ok := <-sub.Events():
			if !ok {
				return nil
			}
			if ev.Kind == ValueReceived {
				return ev.Sample.Value
			}
		}
	}
}

// GetBoolean retrieves a boolean value from a topic within the timeout.
func (c *Client) GetBoolean(name string, timeout time.Duration) (bool, bool) {
	v := c.SubscribeAndRetrieve(name, timeout)
	if b, ok := v.(bool); ok {
		return b, true
	}
	return false, false
}

// GetDouble retrieves a 64-bit float value from a topic within the timeout.
func (c *Client) GetDouble(name string, timeout time.Duration) (float64, bool) {
	v := c.SubscribeAndRetrieve(name, timeout)
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	default:
		return 0, false
	}
}

// GetInt retrieves a 64-bit integer value from a topic within the timeout.
func (c *Client) GetInt(name string, timeout time.Duration) (int64, bool) {
	v := c.SubscribeAndRetrieve(name, timeout)
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	default:
		return 0, false
	}
}

// GetFloat retrieves a 32-bit float value from a topic within the timeout.
func (c *Client) GetFloat(name string, timeout time.Duration) (float32, bool) {
	v := c.SubscribeAndRetrieve(name, timeout)
	switch n := v.(type) {
	case float32:
		return n, true
	case float64:
		return float32(n), true
	default:
		return 0, false
	}
}

// GetString retrieves a string value from a topic within the timeout.
func (c *Client) GetString(name string, timeout time.Duration) (string, bool) {
	v := c.SubscribeAndRetrieve(name, timeout)
	if s, ok := v.(string); ok {
		return s, true
	}
	return "", false
}

// GetDoubleArray retrieves a []float64 slice from a topic within the timeout.
func (c *Client) GetDoubleArray(name string, timeout time.Duration) ([]float64, bool) {
	v := c.SubscribeAndRetrieve(name, timeout)
	if arr, ok := v.([]float64); ok {
		return arr, true
	}
	return nil, false
}

// GetStringArray retrieves a []string slice from a topic within the timeout.
func (c *Client) GetStringArray(name string, timeout time.Duration) ([]string, bool) {
	v := c.SubscribeAndRetrieve(name, timeout)
	if arr, ok := v.([]string); ok {
		return arr, true
	}
	return nil, false
}

// GetBooleanArray retrieves a []bool slice from a topic within the timeout.
func (c *Client) GetBooleanArray(name string, timeout time.Duration) ([]bool, bool) {
	v := c.SubscribeAndRetrieve(name, timeout)
	if arr, ok := v.([]bool); ok {
		return arr, true
	}
	return nil, false
}

// GetIntArray retrieves a []int64 slice from a topic within the timeout.
func (c *Client) GetIntArray(name string, timeout time.Duration) ([]int64, bool) {
	v := c.SubscribeAndRetrieve(name, timeout)
	if arr, ok := v.([]int64); ok {
		return arr, true
	}
	return nil, false
}

// GetRaw retrieves a []byte slice from a topic within the timeout.
func (c *Client) GetRaw(name string, timeout time.Duration) ([]byte, bool) {
	v := c.SubscribeAndRetrieve(name, timeout)
	if b, ok := v.([]byte); ok {
		return b, true
	}
	return nil, false
}

// SubscribeWithPrefix subscribes to all topics under a prefix and delivers updates to a callback.
func (c *Client) SubscribeWithPrefix(prefix string, callback func(topic *Topic, timestamp int64, value any)) (*Subscription, error) {
	sub, err := c.Subscribe([]string{prefix}, SubscriptionOptions{Prefix: true})
	if err != nil {
		return nil, err
	}
	if callback != nil {
		sub.SetCallback(callback)
	}
	return sub, nil
}

// SubscribeWithOptions subscribes using the v0.1.1 SubscribeOptions configuration struct.
func (c *Client) SubscribeWithOptions(patterns []string, o SubscribeOptions) (*Subscription, error) {
	mode := DeliveryLatest
	if o.All {
		mode = DeliveryAll
	}
	return c.Subscribe(patterns, SubscriptionOptions{
		Prefix:     o.Prefix,
		All:        o.All,
		TopicsOnly: o.TopicsOnly,
		Periodic:   o.Periodic,
		Mode:       mode,
	})
}

// Unsubscribe closes the given subscription handle.
func (c *Client) Unsubscribe(s *Subscription) error {
	if s == nil {
		return ErrInvalidHandle
	}
	return s.Close()
}

// GetTopic returns a *Topic representation of a topic by name, or nil if not found.
func (c *Client) GetTopic(name string) *Topic {
	if c == nil {
		return nil
	}
	snap, ok := c.Topic(name)
	if !ok {
		return nil
	}
	return &Topic{
		Name:       snap.Name,
		Type:       snap.Type,
		ID:         snap.ID,
		Properties: snap.Properties,
	}
}

// GetTopics returns a slice of all currently known topics as *Topic objects.
func (c *Client) GetTopics() []*Topic {
	if c == nil {
		return nil
	}
	var out []*Topic
	_ = c.call(func(e *engine) error {
		out = make([]*Topic, 0, len(e.topics))
		for _, t := range e.topics {
			snap := t.snapshot()
			out = append(out, &Topic{
				Name:       snap.Name,
				Type:       snap.Type,
				ID:         snap.ID,
				Properties: snap.Properties,
			})
		}
		return nil
	})
	return out
}

// Topics returns snapshots of all currently known topics.
func (c *Client) Topics() []TopicSnapshot {
	if c == nil {
		return nil
	}
	var out []TopicSnapshot
	_ = c.call(func(e *engine) error {
		out = make([]TopicSnapshot, 0, len(e.topics))
		for _, t := range e.topics {
			out = append(out, t.snapshot())
		}
		return nil
	})
	return out
}

// GetServerTimeOffset returns the estimated server clock offset in microseconds.
func (c *Client) GetServerTimeOffset() int64 {
	if c == nil {
		return 0
	}
	return c.Status().ClockOffset.Microseconds()
}

// GetLastRTT returns the last measured round-trip time in microseconds.
func (c *Client) GetLastRTT() int64 {
	if c == nil {
		return 0
	}
	return c.Status().RTT.Microseconds()
}
