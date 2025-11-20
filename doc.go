// Package nt4 provides a Go client for the NetworkTables 4 (NT4) protocol.
//
// NT4 is a WebSocket-based pub/sub protocol used in FIRST Robotics Competition (FRC)
// for real-time data exchange between robots and control systems. This implementation
// uses MessagePack for binary serialization and supports all NT4 message types.
//
// # Quick Start
//
// Create a client and connect to an NT4 server:
//
//	opts := nt4.DefaultClientOptions("10.20.64.2")
//	client := nt4.NewClient(opts)
//	if err := client.Connect(); err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Disconnect()
//
// # Publishing Topics
//
// Publish a topic and send values:
//
//	topic := client.Publish("/robot/speed", nt4.TypeDouble, nil)
//	client.SetValue(topic, 42.5)
//
// Or use convenience methods:
//
//	client.PublishDouble("/robot/speed", 42.5)
//
// # Subscribing to Topics
//
// Subscribe to topics and receive updates:
//
//	sub := client.Subscribe([]string{"/robot/speed"}, nil)
//	for update := range sub.Updates() {
//	    fmt.Printf("%s = %v\n", update.Topic.Name, update.Value)
//	}
//
// Subscribe with a callback:
//
//	opts := &nt4.SubscribeOptions{Prefix: true}
//	sub := client.Subscribe([]string{"/robot"}, opts)
//	sub.SetCallback(func(topic *Topic, timestamp int64, value any) {
//	    fmt.Printf("%s = %v\n", topic.Name, value)
//	})
//
// # Time Synchronization
//
// The client automatically synchronizes time with the server every 3 seconds.
// Use GetServerTimeOffset() and GetLastRTT() to monitor synchronization.
//
// # Important Notes
//
// - The client does NOT automatically reconnect. Use ConnectWithRetry() for retry logic.
// - To receive topic announcements, you MUST have an active subscription.
// - Published topics receive server-assigned IDs only when subscribed.
// - Default port is 5810.
// - Timestamps are in microseconds (UnixMicro).
//
// # Type System
//
// NT4 supports the following types:
//   - Primitives: boolean, int, float, double, string
//   - Arrays: boolean[], int[], float[], double[], string[]
//   - Binary formats: raw, msgpack, protobuf, json
//
// Use the Type constants (TypeBoolean, TypeDouble, etc.) when publishing topics.
package nt4
