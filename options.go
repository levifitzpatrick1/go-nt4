package nt4

import "time"

// Config for subscribers.
type SubscribeOptions struct {
	// Time between updates in seconds. (0 for fastest)
	Periodic float64

	// Send all values or only latest.
	All bool

	// Send topic announcements without values.
	TopicsOnly bool

	// If true, treats items in Topics as prefixes.
	Prefix bool
}

// NT4 client config.
type ClientOptions struct {
	// Address of NT4 server (10.TE.AM.2 for rio, 127.0.0.1 for sim).
	ServerAddress string

	// NT4 Port.
	// Default: 5810 (DefaultPort)
	Port int

	// Client identifier.
	// Default: "Go-NT4-Client"
	Identity string

	// Called when the client connects to the server.
	// Optional callback.
	OnConnect func()

	// Called when the client disconnects from the server.
	// Optional callback.
	OnDisconnect func()

	// OnTopicAnnounce is called when a topic is announced by the server.
	// To receive announcements, you must have an active subscription.
	// Optional callback.
	OnTopicAnnounce func(topic *Topic)

	// OnTopicUnannounce is called when a topic is unannounced by the server.
	// Optional callback.
	OnTopicUnannounce func(topic *Topic)

	// Wait between reconnection attempts when using ConnectWithRetry.
	// Default: 1 second
	ReconnectInterval time.Duration

	// Logger instance.
	// Default: NewDefaultLogger(LogLevelInfo)
	// Use NewSilentLogger() to disable logging.
	Logger Logger
}
