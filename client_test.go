package nt4

import (
	"context"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	opts := ClientOptions{
		ServerAddress: "127.0.0.1",
		Port:          5810,
		Identity:      "test-client",
	}

	client := NewClient(opts)

	if client == nil {
		t.Fatal("Expected client to be created")
	}

	if client.options.ServerAddress != "127.0.0.1" {
		t.Errorf("Expected server address 127.0.0.1, got %s", client.options.ServerAddress)
	}
	if client.options.Port != 5810 {
		t.Errorf("Expected port 5810, got %d", client.options.Port)
	}
	if client.options.Identity != "test-client" {
		t.Errorf("Expected identity test-client, got %s", client.options.Identity)
	}
}

func TestNewClientDefaults(t *testing.T) {
	opts := ClientOptions{
		ServerAddress: "127.0.0.1",
	}

	client := NewClient(opts)

	if client.options.Port != DefaultPort {
		t.Errorf("Expected default port %d, got %d", DefaultPort, client.options.Port)
	}
	if client.options.Identity != "Go-NT4-Client" {
		t.Errorf("Expected default identity Go-NT4-Client, got %s", client.options.Identity)
	}
	if client.options.ReconnectInterval != time.Second {
		t.Errorf("Expected default reconnect interval 1s, got %v", client.options.ReconnectInterval)
	}
	if client.logger == nil {
		t.Error("Expected default logger to be set")
	}
}

func TestDefaultClientOptions(t *testing.T) {
	opts := DefaultClientOptions("10.20.64.2")

	if opts.ServerAddress != "10.20.64.2" {
		t.Errorf("Expected server address 10.20.64.2, got %s", opts.ServerAddress)
	}
	if opts.Port != DefaultPort {
		t.Errorf("Expected port %d, got %d", DefaultPort, opts.Port)
	}
	if opts.Identity != "Go-NT4-Client" {
		t.Errorf("Expected identity Go-NT4-Client, got %s", opts.Identity)
	}
	if opts.ReconnectInterval != time.Second {
		t.Errorf("Expected reconnect interval 1s, got %v", opts.ReconnectInterval)
	}
	if opts.Logger == nil {
		t.Error("Expected logger to be set")
	}
}

func TestClientIsConnected(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	client := NewClient(opts)

	if client.IsConnected() {
		t.Error("Expected client to not be connected initially")
	}
}

func TestClientPublish(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	topic := client.Publish("/test/value", TypeDouble, map[string]any{"persistent": true})

	if topic == nil {
		t.Fatal("Expected topic to be created")
	}
	if topic.Name != "/test/value" {
		t.Errorf("Expected topic name /test/value, got %s", topic.Name)
	}
	if topic.Type != TypeDouble {
		t.Errorf("Expected type %s, got %s", TypeDouble, topic.Type)
	}
	if topic.TypeID != DataTypeDouble {
		t.Errorf("Expected type ID %d, got %d", DataTypeDouble, topic.TypeID)
	}
	if topic.PubUID == 0 {
		t.Error("Expected PubUID to be assigned")
	}
	if topic.Properties["persistent"] != true {
		t.Error("Expected persistent property to be true")
	}
}

func TestClientPublishMultiple(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	topic1 := client.Publish("/test/1", TypeDouble, nil)
	topic2 := client.Publish("/test/2", TypeString, nil)

	if topic1.PubUID == topic2.PubUID {
		t.Error("Expected different PubUIDs for different topics")
	}
	if topic1.PubUID >= topic2.PubUID {
		t.Error("Expected PubUIDs to increment")
	}
}

func TestClientSubscribe(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	sub := client.Subscribe([]string{"/test/value"}, nil)

	if sub == nil {
		t.Fatal("Expected subscription to be created")
	}
	if len(sub.Topics) != 1 {
		t.Errorf("Expected 1 topic, got %d", len(sub.Topics))
	}
	if sub.Topics[0] != "/test/value" {
		t.Errorf("Expected topic /test/value, got %s", sub.Topics[0])
	}
	if sub.UID == 0 {
		t.Error("Expected UID to be assigned")
	}
	if sub.Updates() == nil {
		t.Error("Expected updates channel to be initialized")
	}
}

func TestClientSubscribeWithOptions(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	subOpts := &SubscribeOptions{
		Prefix: true,
		All:    true,
	}

	sub := client.Subscribe([]string{"/robot"}, subOpts)

	if !sub.Options.Prefix {
		t.Error("Expected Prefix option to be true")
	}
	if !sub.Options.All {
		t.Error("Expected All option to be true")
	}
}

func TestClientGetTopic(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	// Should return nil for non-existent topic
	topic := client.GetTopic("/nonexistent")
	if topic != nil {
		t.Error("Expected nil for non-existent topic")
	}

	// Add a topic manually
	testTopic := &Topic{
		ID:     1,
		Name:   "/test",
		Type:   TypeDouble,
		TypeID: DataTypeDouble,
	}
	client.topicsMu.Lock()
	client.topics["/test"] = testTopic
	client.topicsMu.Unlock()

	// Should return the topic
	topic = client.GetTopic("/test")
	if topic == nil {
		t.Fatal("Expected topic to be found")
	}
	if topic.Name != "/test" {
		t.Errorf("Expected topic name /test, got %s", topic.Name)
	}
}

func TestClientGetTopics(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	// Initially empty
	topics := client.GetTopics()
	if len(topics) != 0 {
		t.Errorf("Expected 0 topics, got %d", len(topics))
	}

	// Add topics
	client.topicsMu.Lock()
	client.topics["/test1"] = &Topic{Name: "/test1"}
	client.topics["/test2"] = &Topic{Name: "/test2"}
	client.topicsMu.Unlock()

	topics = client.GetTopics()
	if len(topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(topics))
	}
}

func TestClientServerTimeOffset(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	// Initially should be 0
	offset := client.GetServerTimeOffset()
	if offset != 0 {
		t.Errorf("Expected initial offset 0, got %d", offset)
	}

	// Set offset
	testOffset := int64(123456)
	client.serverTimeOffset.Store(testOffset)

	offset = client.GetServerTimeOffset()
	if offset != testOffset {
		t.Errorf("Expected offset %d, got %d", testOffset, offset)
	}
}

func TestClientLastRTT(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	// Initially should be 0
	rtt := client.GetLastRTT()
	if rtt != 0 {
		t.Errorf("Expected initial RTT 0, got %d", rtt)
	}

	// Set RTT
	testRTT := int64(5000)
	client.lastRTT.Store(testRTT)

	rtt = client.GetLastRTT()
	if rtt != testRTT {
		t.Errorf("Expected RTT %d, got %d", testRTT, rtt)
	}
}

func TestClientPublishTypedMethods(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	tests := []struct {
		name     string
		publish  func() *Topic
		expected string
	}{
		{"Boolean", func() *Topic { return client.PublishBoolean("/test/bool", true) }, TypeBoolean},
		{"Double", func() *Topic { return client.PublishDouble("/test/double", 1.5) }, TypeDouble},
		{"Int", func() *Topic { return client.PublishInt("/test/int", 42) }, TypeInt},
		{"String", func() *Topic { return client.PublishString("/test/str", "hello") }, TypeString},
		{"DoubleArray", func() *Topic { return client.PublishDoubleArray("/test/darr", []float64{1.0, 2.0}) }, TypeDoubleArray},
		{"StringArray", func() *Topic { return client.PublishStringArray("/test/sarr", []string{"a", "b"}) }, TypeStringArray},
		{"BooleanArray", func() *Topic { return client.PublishBooleanArray("/test/barr", []bool{true, false}) }, TypeBooleanArray},
		{"IntArray", func() *Topic { return client.PublishIntArray("/test/iarr", []int64{1, 2}) }, TypeIntArray},
		{"Raw", func() *Topic { return client.PublishRaw("/test/raw", []byte{0x01, 0x02}) }, TypeRaw},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := tt.publish()
			if topic.Type != tt.expected {
				t.Errorf("Expected type %s, got %s", tt.expected, topic.Type)
			}
		})
	}
}

func TestClientWaitForConnection(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Should timeout since we're not connected
	err := client.WaitForConnection(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("Expected DeadlineExceeded error, got %v", err)
	}
}

func TestClientSubscriptionMatches(t *testing.T) {
	opts := DefaultClientOptions("127.0.0.1")
	opts.Logger = NewSilentLogger()
	client := NewClient(opts)

	tests := []struct {
		name     string
		sub      *Subscription
		topic    *Topic
		expected bool
	}{
		{
			name:     "Exact match",
			sub:      &Subscription{Topics: []string{"/robot/speed"}, Options: SubscribeOptions{}},
			topic:    &Topic{Name: "/robot/speed"},
			expected: true,
		},
		{
			name:     "No match",
			sub:      &Subscription{Topics: []string{"/robot/speed"}, Options: SubscribeOptions{}},
			topic:    &Topic{Name: "/robot/position"},
			expected: false,
		},
		{
			name:     "Prefix match",
			sub:      &Subscription{Topics: []string{"/robot"}, Options: SubscribeOptions{Prefix: true}},
			topic:    &Topic{Name: "/robot/speed"},
			expected: true,
		},
		{
			name:     "Prefix match root",
			sub:      &Subscription{Topics: []string{""}, Options: SubscribeOptions{Prefix: true}},
			topic:    &Topic{Name: "/anything"},
			expected: true,
		},
		{
			name:     "Prefix no match",
			sub:      &Subscription{Topics: []string{"/robot"}, Options: SubscribeOptions{Prefix: true}},
			topic:    &Topic{Name: "/sensor/temp"},
			expected: false,
		},
		{
			name:     "Multiple patterns - one matches",
			sub:      &Subscription{Topics: []string{"/robot", "/sensor"}, Options: SubscribeOptions{Prefix: true}},
			topic:    &Topic{Name: "/sensor/temp"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.subscriptionMatches(tt.sub, tt.topic)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestToInt32(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected int32
	}{
		{"int", 42, 42},
		{"int8", int8(10), 10},
		{"int16", int16(100), 100},
		{"int32", int32(1000), 1000},
		{"int64", int64(10000), 10000},
		{"uint", uint(42), 42},
		{"uint8", uint8(10), 10},
		{"uint16", uint16(100), 100},
		{"uint32", uint32(1000), 1000},
		{"uint64", uint64(10000), 10000},
		{"float32", float32(3.14), 3},
		{"float64", float64(2.71), 2},
		{"string", "invalid", 0},
		{"nil", nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toInt32(tt.input)
			if result != tt.expected {
				t.Errorf("toInt32(%v) = %d; want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected int64
	}{
		{"int", 42, 42},
		{"int8", int8(10), 10},
		{"int16", int16(100), 100},
		{"int32", int32(1000), 1000},
		{"int64", int64(10000), 10000},
		{"uint", uint(42), 42},
		{"uint8", uint8(10), 10},
		{"uint16", uint16(100), 100},
		{"uint32", uint32(1000), 1000},
		{"uint64", uint64(10000), 10000},
		{"float32", float32(3.14), 3},
		{"float64", float64(2.71), 2},
		{"string", "invalid", 0},
		{"nil", nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toInt64(tt.input)
			if result != tt.expected {
				t.Errorf("toInt64(%v) = %d; want %d", tt.input, result, tt.expected)
			}
		})
	}
}
