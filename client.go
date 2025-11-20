package nt4

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"
)

// Client is the main NT4 client that manages the WebSocket connection,
// topics, subscriptions, and time synchronization with the server.
type Client struct {
	options ClientOptions

	conn      *websocket.Conn
	connMu    sync.Mutex
	connected atomic.Bool

	topics     map[string]*Topic
	topicsByID map[int32]*Topic
	topicsMu   sync.RWMutex

	subscriptions   map[int32]*Subscription
	subscriptionsMu sync.RWMutex

	publishers   map[int32]*Topic
	publishersMu sync.RWMutex

	nextPubUID atomic.Int32
	nextSubUID atomic.Int32

	serverTimeOffset atomic.Int64
	lastRTT          atomic.Int64

	done      chan struct{}
	doneMu    sync.Mutex
	writeChan chan any

	logger Logger
}

// NewClient creates a new NT4 client with the specified options.
// Use DefaultClientOptions() for sensible defaults.
//
// Example:
//
//	opts := nt4.DefaultClientOptions("10.20.64.2")
//	client := nt4.NewClient(opts)
func NewClient(options ClientOptions) *Client {
	if options.Port == 0 {
		options.Port = DefaultPort
	}
	if options.Identity == "" {
		options.Identity = "Go-NT4-Client"
	}
	if options.ReconnectInterval == 0 {
		options.ReconnectInterval = time.Second
	}
	if options.Logger == nil {
		options.Logger = NewDefaultLogger(LogLevelInfo)
	}

	c := &Client{
		options:       options,
		topics:        make(map[string]*Topic),
		topicsByID:    make(map[int32]*Topic),
		subscriptions: make(map[int32]*Subscription),
		publishers:    make(map[int32]*Topic),
		done:          make(chan struct{}),
		writeChan:     make(chan any, 100),
		logger:        options.Logger,
	}

	c.nextPubUID.Store(1)
	c.nextSubUID.Store(1)

	return c
}

// Connect establishes a WebSocket connection to the NT4 server.
// Returns an error if the connection fails.
//
// For automatic retry logic, use ConnectWithRetry() instead.
func (c *Client) Connect() error {
	if c.connected.Load() {
		return ErrAlreadyConnected
	}

	u := url.URL{
		Scheme: "ws",
		Host:   fmt.Sprintf("%s:%d", c.options.ServerAddress, c.options.Port),
		Path:   "/nt/" + c.options.Identity,
	}

	c.logger.Info("connecting to server", "url", u.String())

	dialer := websocket.Dialer{
		Subprotocols:     []string{"networktables.first.wpi.edu"},
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		c.logger.Error("connection failed", "error", err)
		return newConnectionError(c.options.ServerAddress, c.options.Port, err)
	}

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	c.connected.Store(true)

	c.doneMu.Lock()
	c.done = make(chan struct{})
	c.doneMu.Unlock()

	go c.readLoop()
	go c.writeLoop()
	go c.timeSyncLoop()

	c.logger.Info("connected to server")

	if c.options.OnConnect != nil {
		c.options.OnConnect()
	}

	return nil
}

// Disconnect closes the connection to the NT4 server.
// Safe to call multiple times.
func (c *Client) Disconnect() {
	if !c.connected.Load() {
		return
	}

	c.logger.Info("disconnecting from server")

	c.connected.Store(false)

	c.doneMu.Lock()
	if c.done != nil {
		close(c.done)
		c.done = nil
	}
	c.doneMu.Unlock()

	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()

	c.logger.Info("disconnected from server")

	if c.options.OnDisconnect != nil {
		c.options.OnDisconnect()
	}
}

// IsConnected returns true if the client is currently connected to the server.
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// Publish announces a new topic to the server and returns the Topic handle.
// Use SetValue() to publish values to the topic.
//
// Important: To receive the server-assigned topic ID in the announce message,
// you must have an active subscription. Otherwise, the topic will have ID 0.
//
// Example:
//
//	topic := client.Publish("/robot/speed", nt4.TypeDouble, nil)
//	client.SetValue(topic, 42.5)
func (c *Client) Publish(name string, typeStr string, properties map[string]any) *Topic {
	pubUID := c.nextPubUID.Add(1) - 1

	topic := &Topic{
		Name:       name,
		Type:       typeStr,
		TypeID:     TypeStringToID(typeStr),
		PubUID:     pubUID,
		Properties: properties,
	}

	c.publishersMu.Lock()
	c.publishers[pubUID] = topic
	c.publishersMu.Unlock()

	msg := newPublishMessage(name, pubUID, typeStr, properties)
	c.sendJSON(msg)

	c.logger.Debug("published topic", "name", name, "type", typeStr, "pubUID", pubUID)

	return topic
}

// Unpublish removes a published topic from the server.
func (c *Client) Unpublish(topic *Topic) {
	c.publishersMu.Lock()
	delete(c.publishers, topic.PubUID)
	c.publishersMu.Unlock()

	msg := newUnpublishMessage(topic.PubUID)
	c.sendJSON(msg)

	c.logger.Debug("unpublished topic", "name", topic.Name, "pubUID", topic.PubUID)
}

// SetValue publishes a value to a topic.
// The topic must have been created with Publish() or received via subscription.
func (c *Client) SetValue(topic *Topic, value any) {
	timestamp := c.getServerTimestamp()

	// For topics we published, use our PubUID, not the server-assigned ID
	// For topics published by others, use the server-assigned ID
	topicID := topic.PubUID
	if topicID == 0 {
		// This is a topic published by someone else, use server ID
		topicID = topic.ID
		if topicID == 0 {
			// Topic not announced yet
			c.logger.Warn("cannot set value on topic without ID", "topic", topic.Name)
			return
		}
	}

	binary := binaryMessage{
		TopicID:   topicID,
		Timestamp: timestamp,
		TypeID:    topic.TypeID,
		Value:     value,
	}

	c.sendBinary(binary)
}

// Subscribe creates a subscription to one or more topics.
// Returns a Subscription that can be used to receive updates via the Updates() channel.
//
// The topics parameter can contain exact topic names or patterns (if Prefix option is set).
// Set SubscribeOptions.Prefix to true to match topic prefixes.
//
// Example:
//
//	// Subscribe to a single topic
//	sub := client.Subscribe([]string{"/robot/speed"}, nil)
//
//	// Subscribe to all topics with a prefix
//	opts := &nt4.SubscribeOptions{Prefix: true, All: true}
//	sub := client.Subscribe([]string{"/robot"}, opts)
//
//	// Receive updates
//	for update := range sub.Updates() {
//	    fmt.Printf("%s = %v\n", update.Topic.Name, update.Value)
//	}
func (c *Client) Subscribe(topics []string, options *SubscribeOptions) *Subscription {
	subUID := c.nextSubUID.Add(1) - 1

	opts := SubscribeOptions{}
	if options != nil {
		opts = *options
	}

	sub := &Subscription{
		UID:     subUID,
		Topics:  topics,
		Options: opts,
	}
	sub.InitUpdates(100)

	c.subscriptionsMu.Lock()
	c.subscriptions[subUID] = sub
	c.subscriptionsMu.Unlock()

	msg := newSubscribeMessage(topics, subUID, options)
	c.sendJSON(msg)

	c.logger.Debug("created subscription", "topics", topics, "subUID", subUID, "prefix", opts.Prefix)

	return sub
}

// Unsubscribe removes an active subscription.
func (c *Client) Unsubscribe(sub *Subscription) {
	c.subscriptionsMu.Lock()
	delete(c.subscriptions, sub.UID)
	c.subscriptionsMu.Unlock()

	sub.CloseUpdates()

	msg := newUnsubscribeMessage(sub.UID)
	c.sendJSON(msg)

	c.logger.Debug("removed subscription", "subUID", sub.UID)
}

// SetProperties updates properties for a topic.
func (c *Client) SetProperties(name string, properties map[string]any) {
	msg := newSetPropertiesMessage(name, properties)
	c.sendJSON(msg)

	c.logger.Debug("set properties", "topic", name, "properties", properties)
}

// GetTopic retrieves a topic by name, or nil if not found.
func (c *Client) GetTopic(name string) *Topic {
	c.topicsMu.RLock()
	defer c.topicsMu.RUnlock()
	return c.topics[name]
}

// GetTopics returns all currently known topics.
func (c *Client) GetTopics() []*Topic {
	c.topicsMu.RLock()
	defer c.topicsMu.RUnlock()

	topics := make([]*Topic, 0, len(c.topics))
	for _, t := range c.topics {
		topics = append(topics, t)
	}
	return topics
}

// GetServerTimeOffset returns the current server time offset in microseconds.
// This offset is used to convert local timestamps to server timestamps.
func (c *Client) GetServerTimeOffset() int64 {
	return c.serverTimeOffset.Load()
}

// GetLastRTT returns the last measured round-trip time in microseconds.
func (c *Client) GetLastRTT() int64 {
	return c.lastRTT.Load()
}

// readLoop continuously reads messages from the WebSocket connection.
func (c *Client) readLoop() {
	defer func() {
		if c.connected.Load() {
			c.connected.Store(false)
			c.logger.Info("read loop terminated, disconnected")
			if c.options.OnDisconnect != nil {
				c.options.OnDisconnect()
			}
		}
	}()

	for {
		select {
		case <-c.done:
			return
		default:
		}

		c.connMu.Lock()
		conn := c.conn
		c.connMu.Unlock()

		if conn == nil {
			return
		}

		messageType, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				c.logger.Error("read error", "error", err)
			}
			return
		}

		switch messageType {
		case websocket.TextMessage:
			c.handleTextMessage(data)
		case websocket.BinaryMessage:
			c.handleBinaryMessage(data)
		}
	}
}

// writeLoop continuously sends messages to the WebSocket connection.
func (c *Client) writeLoop() {
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.writeChan:
			if !c.connected.Load() {
				continue
			}

			c.connMu.Lock()
			conn := c.conn
			c.connMu.Unlock()

			if conn == nil {
				continue
			}

			switch m := msg.(type) {
			case []jsonMessage:
				data, err := json.Marshal(m)
				if err != nil {
					c.logger.Error("JSON marshal error", "error", err)
					continue
				}
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					c.logger.Error("write error", "error", err)
				} else {
					c.logger.Debug("sent JSON message", "methods", len(m))
				}
			case []byte:
				if err := conn.WriteMessage(websocket.BinaryMessage, m); err != nil {
					c.logger.Error("write error", "error", err)
				} else {
					c.logger.Debug("sent binary message", "bytes", len(m))
				}
			}
		}
	}
}

// handleTextMessage processes incoming JSON text messages.
func (c *Client) handleTextMessage(data []byte) {
	var msgs []map[string]any
	if err := json.Unmarshal(data, &msgs); err != nil {
		c.logger.Error("JSON unmarshal error", "error", err)
		return
	}

	for _, msg := range msgs {
		method, ok := msg["method"].(string)
		if !ok {
			c.logger.Warn("message missing method field")
			continue
		}

		params, ok := msg["params"].(map[string]any)
		if !ok {
			c.logger.Warn("message missing params field", "method", method)
			continue
		}

		switch method {
		case methodAnnounce:
			c.handleAnnounce(params)
		case methodUnannounce:
			c.handleUnannounce(params)
		case methodProperties:
			c.handleProperties(params)
		default:
			c.logger.Debug("unknown method", "method", method)
		}
	}
}

// handleAnnounce processes topic announcement messages.
func (c *Client) handleAnnounce(params map[string]any) {
	name, _ := params["name"].(string)
	id := int32(params["id"].(float64))
	typeStr, _ := params["type"].(string)

	var pubUID int32
	if p, ok := params["pubuid"].(float64); ok {
		pubUID = int32(p)
	}

	properties, _ := params["properties"].(map[string]any)

	// Check if this is an announcement for a topic we published
	var topic *Topic
	if pubUID != 0 {
		c.publishersMu.RLock()
		publishedTopic, exists := c.publishers[pubUID]
		c.publishersMu.RUnlock()

		if exists {
			// Update our published topic with the server-assigned ID
			publishedTopic.ID = id
			publishedTopic.Properties = properties
			topic = publishedTopic
		}
	}

	// If not our published topic, create new topic
	if topic == nil {
		topic = &Topic{
			ID:         id,
			Name:       name,
			Type:       typeStr,
			TypeID:     TypeStringToID(typeStr),
			PubUID:     pubUID,
			Properties: properties,
		}
	}

	c.topicsMu.Lock()
	c.topics[name] = topic
	c.topicsByID[id] = topic
	c.topicsMu.Unlock()

	c.logger.Debug("topic announced", "name", name, "id", id, "type", typeStr)

	if c.options.OnTopicAnnounce != nil {
		c.options.OnTopicAnnounce(topic)
	}
}

// handleUnannounce processes topic unannouncement messages.
func (c *Client) handleUnannounce(params map[string]any) {
	name, _ := params["name"].(string)
	id := int32(params["id"].(float64))

	c.topicsMu.Lock()
	topic := c.topics[name]
	delete(c.topics, name)
	delete(c.topicsByID, id)
	c.topicsMu.Unlock()

	c.logger.Debug("topic unannounced", "name", name, "id", id)

	if topic != nil && c.options.OnTopicUnannounce != nil {
		c.options.OnTopicUnannounce(topic)
	}
}

// handleProperties processes property update messages.
func (c *Client) handleProperties(params map[string]any) {
	name, _ := params["name"].(string)

	c.topicsMu.RLock()
	topic := c.topics[name]
	c.topicsMu.RUnlock()

	if topic == nil {
		return
	}

	propsToUpdate := make(map[string]any)
	for k, v := range params {
		if k != "name" && k != "ack" {
			propsToUpdate[k] = v
		}
	}
	topic.UpdateProperties(propsToUpdate)

	c.logger.Debug("properties updated", "topic", name, "properties", propsToUpdate)
}

// handleBinaryMessage processes incoming binary messages (value updates and time sync).
func (c *Client) handleBinaryMessage(data []byte) {
	reader := bytes.NewReader(data)
	decoder := msgpack.NewDecoder(reader)

	for reader.Len() > 0 {
		var msg []any
		if err := decoder.Decode(&msg); err != nil {
			c.logger.Warn("msgpack decode error", "error", err)
			break
		}

		if len(msg) < 4 {
			c.logger.Warn("binary message too short", "length", len(msg))
			continue
		}

		topicID := toInt32(msg[0])
		timestamp := toInt64(msg[1])

		if topicID == -1 {
			c.handleTimeSync(timestamp, msg)
			continue
		}

		value := msg[3]

		c.topicsMu.RLock()
		topic := c.topicsByID[topicID]
		c.topicsMu.RUnlock()

		if topic == nil {
			c.logger.Debug("received value for unknown topic", "topicID", topicID)
			continue
		}

		c.logger.Debug("received value", "topic", topic.Name, "value", value)

		c.subscriptionsMu.RLock()
		for _, sub := range c.subscriptions {
			if c.subscriptionMatches(sub, topic) {
				update := TopicUpdate{
					Topic:     topic,
					Timestamp: timestamp,
					Value:     value,
				}

				sub.SendUpdate(update)

				callback := sub.GetCallback()
				if callback != nil {
					go callback(topic, timestamp, value)
				}
			}
		}
		c.subscriptionsMu.RUnlock()
	}
}

// subscriptionMatches checks if a subscription matches a topic.
func (c *Client) subscriptionMatches(sub *Subscription, topic *Topic) bool {
	for _, pattern := range sub.Topics {
		if sub.Options.Prefix {
			if len(topic.Name) >= len(pattern) && topic.Name[:len(pattern)] == pattern {
				return true
			}
		} else {
			if topic.Name == pattern {
				return true
			}
		}
	}
	return false
}

// handleTimeSync processes time synchronization responses from the server.
func (c *Client) handleTimeSync(clientTime int64, msg []any) {
	if len(msg) < 4 {
		return
	}

	// msg format: [topicID, clientTimestamp, typeID, serverTimestamp]
	// msg[0] = -1 (topic ID)
	// msg[1] = clientTimestamp (the time we sent)
	// msg[2] = typeID (should be 2 for int)
	// msg[3] = serverTimestamp (server's current time)
	serverTime := toInt64(msg[3])

	if serverTime != 0 {
		now := time.Now().UnixMicro()
		rtt := now - clientTime
		c.lastRTT.Store(rtt)

		offset := serverTime - clientTime - rtt/2
		c.serverTimeOffset.Store(offset)

		c.logger.Debug("time sync", "rtt_us", rtt, "offset_us", offset)
	}
}

// timeSyncLoop periodically sends time sync requests to the server.
func (c *Client) timeSyncLoop() {
	// Wait a moment before starting time sync to let connection stabilize
	time.Sleep(100 * time.Millisecond)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// Initial sync
	c.sendTimeSyncRequest()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if c.connected.Load() {
				c.sendTimeSyncRequest()
			}
		}
	}
}

// sendTimeSyncRequest sends a time sync request to the server.
func (c *Client) sendTimeSyncRequest() {
	now := time.Now().UnixMicro()
	// Format: [topicID, serverTimestamp, typeID, clientTimestamp]
	// For time sync requests: [-1, 0, 2 (int type), clientTime]
	msg := []any{int32(-1), int64(0), int32(2), now}

	data, err := msgpack.Marshal(msg)
	if err != nil {
		c.logger.Error("time sync encode error", "error", err)
		return
	}

	select {
	case c.writeChan <- data:
	default:
		c.logger.Warn("write channel full, dropping time sync request")
	}
}

// getServerTimestamp returns the current time adjusted to server time in microseconds.
func (c *Client) getServerTimestamp() int64 {
	return time.Now().UnixMicro() + c.serverTimeOffset.Load()
}

// sendJSON queues a JSON message for sending.
func (c *Client) sendJSON(msg jsonMessage) {
	messages := []jsonMessage{msg}
	select {
	case c.writeChan <- messages:
	default:
		c.logger.Warn("write channel full, dropping JSON message", "method", msg.Method)
	}
}

// sendBinary queues a binary message for sending.
func (c *Client) sendBinary(msg binaryMessage) {
	data := []any{msg.TopicID, msg.Timestamp, msg.TypeID, msg.Value}

	encoded, err := msgpack.Marshal(data)
	if err != nil {
		c.logger.Error("msgpack encode error", "error", err)
		return
	}

	c.logger.Debug("sending binary", "topicID", msg.TopicID, "typeID", msg.TypeID, "bytes", len(encoded))

	select {
	case c.writeChan <- encoded:
	default:
		c.logger.Warn("write channel full, dropping binary message", "topicID", msg.TopicID)
	}
}

// Helper functions for type conversions.

func toInt32(v any) int32 {
	switch n := v.(type) {
	case int:
		return int32(n)
	case int8:
		return int32(n)
	case int16:
		return int32(n)
	case int32:
		return n
	case int64:
		return int32(n)
	case uint:
		return int32(n)
	case uint8:
		return int32(n)
	case uint16:
		return int32(n)
	case uint32:
		return int32(n)
	case uint64:
		return int32(n)
	case float32:
		return int32(n)
	case float64:
		return int32(n)
	default:
		return 0
	}
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case uint:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n)
	case float32:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// Typed convenience methods.

// SubscribeAndRetrieve subscribes to a topic, waits for the first update, then unsubscribes.
// Returns nil if the timeout expires before receiving a value.
func (c *Client) SubscribeAndRetrieve(topic string, timeout time.Duration) any {
	sub := c.Subscribe([]string{topic}, nil)
	defer c.Unsubscribe(sub)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case update := <-sub.Updates():
		return update.Value
	case <-ctx.Done():
		return nil
	}
}

// SubscribeWithPrefix creates a subscription with prefix matching and callback.
func (c *Client) SubscribeWithPrefix(prefix string, callback func(topic *Topic, timestamp int64, value any)) *Subscription {
	opts := &SubscribeOptions{
		Prefix: true,
	}

	sub := c.Subscribe([]string{prefix}, opts)
	sub.SetCallback(callback)
	return sub
}

// PublishBoolean publishes a boolean topic with an initial value.
func (c *Client) PublishBoolean(name string, value bool) *Topic {
	topic := c.Publish(name, TypeBoolean, nil)
	c.SetValue(topic, value)
	return topic
}

// PublishDouble publishes a double topic with an initial value.
func (c *Client) PublishDouble(name string, value float64) *Topic {
	topic := c.Publish(name, TypeDouble, nil)
	c.SetValue(topic, value)
	return topic
}

// PublishInt publishes an int topic with an initial value.
func (c *Client) PublishInt(name string, value int64) *Topic {
	topic := c.Publish(name, TypeInt, nil)
	c.SetValue(topic, value)
	return topic
}

// PublishString publishes a string topic with an initial value.
func (c *Client) PublishString(name string, value string) *Topic {
	topic := c.Publish(name, TypeString, nil)
	c.SetValue(topic, value)
	return topic
}

// PublishDoubleArray publishes a double array topic with an initial value.
func (c *Client) PublishDoubleArray(name string, value []float64) *Topic {
	topic := c.Publish(name, TypeDoubleArray, nil)
	c.SetValue(topic, value)
	return topic
}

// PublishStringArray publishes a string array topic with an initial value.
func (c *Client) PublishStringArray(name string, value []string) *Topic {
	topic := c.Publish(name, TypeStringArray, nil)
	c.SetValue(topic, value)
	return topic
}

// PublishBooleanArray publishes a boolean array topic with an initial value.
func (c *Client) PublishBooleanArray(name string, value []bool) *Topic {
	topic := c.Publish(name, TypeBooleanArray, nil)
	c.SetValue(topic, value)
	return topic
}

// PublishIntArray publishes an int array topic with an initial value.
func (c *Client) PublishIntArray(name string, value []int64) *Topic {
	topic := c.Publish(name, TypeIntArray, nil)
	c.SetValue(topic, value)
	return topic
}

// PublishRaw publishes a raw binary topic with an initial value.
func (c *Client) PublishRaw(name string, value []byte) *Topic {
	topic := c.Publish(name, TypeRaw, nil)
	c.SetValue(topic, value)
	return topic
}

// GetBoolean retrieves a boolean value from a topic.
// Returns (value, true) if successful, (false, false) if timeout or type mismatch.
func (c *Client) GetBoolean(name string, timeout time.Duration) (bool, bool) {
	val := c.SubscribeAndRetrieve(name, timeout)
	if val == nil {
		return false, false
	}

	if b, ok := val.(bool); ok {
		return b, true
	}
	return false, false
}

// GetDouble retrieves a double value from a topic.
// Returns (value, true) if successful, (0, false) if timeout or type mismatch.
func (c *Client) GetDouble(name string, timeout time.Duration) (float64, bool) {
	val := c.SubscribeAndRetrieve(name, timeout)
	if val == nil {
		return 0, false
	}

	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	}
	return 0, false
}

// GetString retrieves a string value from a topic.
// Returns (value, true) if successful, ("", false) if timeout or type mismatch.
func (c *Client) GetString(name string, timeout time.Duration) (string, bool) {
	val := c.SubscribeAndRetrieve(name, timeout)
	if val == nil {
		return "", false
	}

	if s, ok := val.(string); ok {
		return s, true
	}
	return "", false
}

// GetInt retrieves an int value from a topic.
// Returns (value, true) if successful, (0, false) if timeout or type mismatch.
func (c *Client) GetInt(name string, timeout time.Duration) (int64, bool) {
	val := c.SubscribeAndRetrieve(name, timeout)
	if val == nil {
		return 0, false
	}

	switch v := val.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	}
	return 0, false
}

// WaitForConnection blocks until the client is connected or the context is cancelled.
func (c *Client) WaitForConnection(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if c.IsConnected() {
				return nil
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// ConnectWithRetry attempts to connect to the server with automatic retries.
// It will keep retrying until successful or the context is cancelled.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	if err := client.ConnectWithRetry(ctx); err != nil {
//	    log.Fatal("Failed to connect:", err)
//	}
func (c *Client) ConnectWithRetry(ctx context.Context) error {
	for {
		err := c.Connect()
		if err == nil {
			return nil
		}

		c.logger.Warn("connection attempt failed, retrying", "error", err, "retry_in", c.options.ReconnectInterval)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.options.ReconnectInterval):
			// Try again
		}
	}
}
