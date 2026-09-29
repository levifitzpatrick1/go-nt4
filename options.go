package nt4

import (
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

// DefaultPort is the standard NetworkTables 4 server port.
const DefaultPort = 5810

// Zero-valued limits select finite defaults. Negative limits and durations are invalid.
type ClientOptions struct {
	ServerAddress                                                                 string
	Port                                                                          int
	ClientName                                                                    string
	Identity                                                                      string
	EndpointURL                                                                   string
	OnConnect                                                                     func()
	OnDisconnect                                                                  func()
	OnTopicAnnounce                                                               func(topic *Topic)
	OnTopicUnannounce                                                             func(topic *Topic)
	ReconnectInterval                                                             time.Duration
	Logger                                                                        Logger
	CommandCapacity, CommandMaxBytes                                              int
	InboundCapacity, InboundMaxBytes                                              int
	WriterCapacity, WriterMaxBytes                                                int
	TotalOfflineCapacity, TotalOfflineMaxBytes                                    int
	RetainedMaxBytes                                                              int
	MaxTopics, MaxPublishers, MaxSubscriptions                                    int
	MaxTextBytes, MaxBinaryBytes                                                  int
	MaxJSONDepth, MaxJSONContainerItems, MaxNameBytes                             int
	DialTimeout, WriteTimeout, ReadTimeout, RetryMin, RetryMax, KeepaliveInterval time.Duration
}

type PublisherOptions struct{ OfflineQueueCapacity, OfflineQueueMaxBytes int }

type DeliveryMode uint8

const (
	DeliveryLatest DeliveryMode = iota
	DeliveryAll
)

type SubscriptionOptions struct {
	Prefix, All, TopicsOnly        bool
	Periodic                       time.Duration
	Mode                           DeliveryMode
	BufferCapacity, BufferMaxBytes int
}

func positive(p *int, def int) error {
	if *p < 0 {
		return ErrInvalidOptions
	}
	if *p == 0 {
		*p = def
	}
	return nil
}
func duration(p *time.Duration, def time.Duration) error {
	if *p < 0 {
		return ErrInvalidOptions
	}
	if *p == 0 {
		*p = def
	}
	return nil
}

// NormalizeClientOptions validates and fills finite resource and time limits.
func NormalizeClientOptions(o ClientOptions) (ClientOptions, error) {
	for _, pair := range []struct {
		p   *int
		def int
	}{
		{&o.CommandCapacity, 256}, {&o.CommandMaxBytes, 1 << 20}, {&o.InboundCapacity, 256}, {&o.InboundMaxBytes, 1 << 20},
		{&o.WriterCapacity, 256}, {&o.WriterMaxBytes, 1 << 20}, {&o.TotalOfflineCapacity, 1024}, {&o.TotalOfflineMaxBytes, 4 << 20},
		{&o.RetainedMaxBytes, 4 << 20}, {&o.MaxTopics, 4096}, {&o.MaxPublishers, 4096}, {&o.MaxSubscriptions, 1024},
		{&o.MaxTextBytes, 1 << 20}, {&o.MaxBinaryBytes, 1 << 20}, {&o.MaxJSONDepth, 32}, {&o.MaxJSONContainerItems, 4096}, {&o.MaxNameBytes, 4096},
	} {
		if err := positive(pair.p, pair.def); err != nil {
			return o, err
		}
	}
	for _, pair := range []struct {
		p   *time.Duration
		def time.Duration
	}{
		{&o.DialTimeout, 5 * time.Second}, {&o.WriteTimeout, 5 * time.Second}, {&o.ReadTimeout, 30 * time.Second},
		{&o.RetryMin, 250 * time.Millisecond}, {&o.RetryMax, 10 * time.Second}, {&o.KeepaliveInterval, 5 * time.Second},
	} {
		if err := duration(pair.p, pair.def); err != nil {
			return o, err
		}
	}
	if o.RetryMax < o.RetryMin || o.Port < 0 || o.Port > 65535 || o.MaxTextBytes > o.InboundMaxBytes || o.MaxBinaryBytes > o.InboundMaxBytes {
		return o, fmt.Errorf("%w: inconsistent limits", ErrInvalidOptions)
	}
	if o.Port == 0 {
		o.Port = DefaultPort
	}
	if o.ReconnectInterval > 0 && o.RetryMin == 0 {
		o.RetryMin = o.ReconnectInterval
	}
	if o.ClientName == "" && o.Identity != "" {
		o.ClientName = o.Identity
	}
	if o.Identity == "" && o.ClientName != "" {
		o.Identity = o.ClientName
	}
	if o.ClientName == "" {
		o.ClientName = "go-nt4"
		o.Identity = "go-nt4"
	}
	if o.ServerAddress == "" {
		o.ServerAddress = "127.0.0.1"
	}
	if !utf8.ValidString(o.ServerAddress) || !utf8.ValidString(o.ClientName) || !utf8.ValidString(o.EndpointURL) || len(o.ClientName) > o.MaxNameBytes {
		return o, ErrInvalidOptions
	}
	if o.EndpointURL != "" {
		u, e := url.Parse(o.EndpointURL)
		if e != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil {
			return o, ErrInvalidOptions
		}
	}
	return o, nil
}

// ValidatePublisherOptions checks both local and total offline-history bounds.
func ValidatePublisherOptions(o PublisherOptions, c ClientOptions) error {
	if o.OfflineQueueCapacity < 0 || o.OfflineQueueMaxBytes < 0 {
		return ErrInvalidOptions
	}
	if o.OfflineQueueCapacity == 0 {
		if o.OfflineQueueMaxBytes != 0 {
			return ErrInvalidOptions
		}
		return nil
	}
	if o.OfflineQueueMaxBytes == 0 || o.OfflineQueueCapacity > c.TotalOfflineCapacity || o.OfflineQueueMaxBytes > c.TotalOfflineMaxBytes {
		return ErrInvalidOptions
	}
	return nil
}

// NormalizeSubscriptionOptions fills bounded delivery defaults and checks server All for lossless local delivery.
func NormalizeSubscriptionOptions(o SubscriptionOptions) (SubscriptionOptions, error) {
	if o.Periodic < 0 || o.Mode > DeliveryAll {
		return o, ErrInvalidOptions
	}
	if o.Mode == DeliveryAll && !o.All {
		return o, ErrIncompatibleOptions
	}
	if err := positive(&o.BufferCapacity, 128); err != nil {
		return o, err
	}
	if err := positive(&o.BufferMaxBytes, 1<<20); err != nil {
		return o, err
	}
	return o, nil
}

// ValidatePatterns accepts slashless and empty prefix patterns; exact empty names are invalid.
func ValidatePatterns(patterns []string, prefix bool, maxNameBytes int) error {
	if len(patterns) == 0 || maxNameBytes <= 0 {
		return ErrInvalidOptions
	}
	for _, p := range patterns {
		if len(p) > maxNameBytes || !utf8.ValidString(p) || (!prefix && p == "") {
			return ErrInvalidOptions
		}
	}
	return nil
}

// ValidateTopicName rejects only invalid UTF-8, oversize names and the reserved local '$' namespace.
func ValidateTopicName(name string, maxBytes int) error {
	if len(name) > maxBytes || maxBytes <= 0 || !utf8.ValidString(name) || strings.HasPrefix(name, "$") {
		return ErrInvalidOptions
	}
	return nil
}
