package nt4

import (
	"context"
	"fmt"
	"time"
)

// TeamNumberToAddress converts an FRC team number to a roboRIO server IP address.
// For example: 2064 becomes "10.20.64.2", and 254 becomes "10.2.54.2".
func TeamNumberToAddress(teamNumber int) string {
	octet2 := teamNumber / 100
	octet3 := teamNumber % 100
	return fmt.Sprintf("10.%d.%d.2", octet2, octet3)
}

// DefaultClientOptions returns ClientOptions initialized with sensible defaults
// for the given server address.
func DefaultClientOptions(serverAddress string) ClientOptions {
	return ClientOptions{
		ServerAddress: serverAddress,
		Port:          DefaultPort,
		ClientName:    "go-nt4",
	}
}

// TypeStringToID maps an NT4 type string to a numeric ID.
func TypeStringToID(typeStr string) int {
	id, err := TypeID(typeStr)
	if err != nil {
		return DataTypeBinary
	}
	return id
}

// TypeIDToString maps an NT4 numeric ID to its standard type string.
func TypeIDToString(typeID int) string {
	switch typeID {
	case DataTypeBoolean:
		return TypeBoolean
	case DataTypeDouble:
		return TypeDouble
	case DataTypeInt:
		return TypeInt
	case DataTypeFloat:
		return TypeFloat
	case DataTypeString:
		return TypeString
	case DataTypeBooleanArray:
		return TypeBooleanArray
	case DataTypeDoubleArray:
		return TypeDoubleArray
	case DataTypeIntArray:
		return TypeIntArray
	case DataTypeFloatArray:
		return TypeFloatArray
	case DataTypeStringArray:
		return TypeStringArray
	default:
		return TypeRaw
	}
}

// ClientBuilder provides a fluent interface for configuring and creating a Client.
type ClientBuilder struct {
	opts ClientOptions
}

// NewClientBuilder creates a new ClientBuilder with default settings (127.0.0.1:5810).
func NewClientBuilder() *ClientBuilder {
	return &ClientBuilder{
		opts: ClientOptions{
			ServerAddress: "127.0.0.1",
			Port:          DefaultPort,
			ClientName:    "go-nt4",
		},
	}
}

// Server sets the server host or IP address.
func (b *ClientBuilder) Server(address string) *ClientBuilder {
	b.opts.ServerAddress = address
	return b
}

// Team sets the server address to the standard FRC roboRIO IP for the given team number.
// For example, b.Team(2064) sets the address to "10.20.64.2".
func (b *ClientBuilder) Team(teamNumber int) *ClientBuilder {
	b.opts.ServerAddress = TeamNumberToAddress(teamNumber)
	return b
}

// Port sets the server port (default: 5810).
func (b *ClientBuilder) Port(port int) *ClientBuilder {
	b.opts.Port = port
	return b
}

// Name sets the client identification name sent to the server.
func (b *ClientBuilder) Name(name string) *ClientBuilder {
	b.opts.ClientName = name
	b.opts.Identity = name
	return b
}

// Identity sets the client identifier (alias for Name).
func (b *ClientBuilder) Identity(id string) *ClientBuilder {
	b.opts.Identity = id
	b.opts.ClientName = id
	return b
}

// Logger sets the logger instance.
func (b *ClientBuilder) Logger(l Logger) *ClientBuilder {
	b.opts.Logger = l
	return b
}

// OnConnect sets a callback invoked when the client connects to the server.
func (b *ClientBuilder) OnConnect(fn func()) *ClientBuilder {
	b.opts.OnConnect = fn
	return b
}

// OnDisconnect sets a callback invoked when the client disconnects from the server.
func (b *ClientBuilder) OnDisconnect(fn func()) *ClientBuilder {
	b.opts.OnDisconnect = fn
	return b
}

// OnTopicAnnounce sets a callback invoked when a topic is announced.
func (b *ClientBuilder) OnTopicAnnounce(fn func(topic *Topic)) *ClientBuilder {
	b.opts.OnTopicAnnounce = fn
	return b
}

// OnTopicUnannounce sets a callback invoked when a topic is unannounced.
func (b *ClientBuilder) OnTopicUnannounce(fn func(topic *Topic)) *ClientBuilder {
	b.opts.OnTopicUnannounce = fn
	return b
}

// ReconnectInterval sets the delay between reconnection attempts.
func (b *ClientBuilder) ReconnectInterval(d time.Duration) *ClientBuilder {
	b.opts.ReconnectInterval = d
	b.opts.RetryMin = d
	return b
}

// EndpointURL overrides the server address and port with an explicit ws:// or wss:// URL.
func (b *ClientBuilder) EndpointURL(url string) *ClientBuilder {
	b.opts.EndpointURL = url
	return b
}

// DialTimeout sets the connection timeout for dialing the server.
func (b *ClientBuilder) DialTimeout(d time.Duration) *ClientBuilder {
	b.opts.DialTimeout = d
	return b
}

// WriteTimeout sets the write timeout for outbound WebSocket frames.
func (b *ClientBuilder) WriteTimeout(d time.Duration) *ClientBuilder {
	b.opts.WriteTimeout = d
	return b
}

// ReadTimeout sets the read timeout for inbound WebSocket frames.
func (b *ClientBuilder) ReadTimeout(d time.Duration) *ClientBuilder {
	b.opts.ReadTimeout = d
	return b
}

// Retry sets the minimum and maximum reconnect backoff intervals.
func (b *ClientBuilder) Retry(min, max time.Duration) *ClientBuilder {
	b.opts.RetryMin = min
	b.opts.RetryMax = max
	return b
}

// Keepalive sets the ping keepalive interval.
func (b *ClientBuilder) Keepalive(interval time.Duration) *ClientBuilder {
	b.opts.KeepaliveInterval = interval
	return b
}

// Options sets or overrides the underlying ClientOptions directly.
func (b *ClientBuilder) Options(opts ClientOptions) *ClientBuilder {
	b.opts = opts
	return b
}

// Build validates options and creates a new NT4 Client.
func (b *ClientBuilder) Build() (*Client, error) {
	return NewClient(b.opts)
}

// BuildAndStart creates and starts the Client with the provided context.
// If starting fails, the client is closed automatically before returning the error.
func (b *ClientBuilder) BuildAndStart(ctx context.Context) (*Client, error) {
	c, err := b.Build()
	if err != nil {
		return nil, err
	}
	if err := c.Start(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
