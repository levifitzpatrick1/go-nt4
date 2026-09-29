package nt4

import (
	"sync"
	"time"
)

// Topic type string constants.
const (
	TypeBoolean      = "boolean"
	TypeDouble       = "double"
	TypeInt          = "int"
	TypeFloat        = "float"
	TypeString       = "string"
	TypeBooleanArray = "boolean[]"
	TypeDoubleArray  = "double[]"
	TypeIntArray     = "int[]"
	TypeFloatArray   = "float[]"
	TypeStringArray  = "string[]"
	TypeRaw          = "raw"
	TypeMsgpack      = "msgpack"
	TypeProtobuf     = "protobuf"
	TypeJSON         = "json"
)

// Data type binary ID constants.
const (
	DataTypeBoolean      = 0
	DataTypeDouble       = 1
	DataTypeInt          = 2
	DataTypeFloat        = 3
	DataTypeString       = 4
	DataTypeBinary       = 5
	DataTypeBooleanArray = 16
	DataTypeDoubleArray  = 17
	DataTypeIntArray     = 18
	DataTypeFloatArray   = 19
	DataTypeStringArray  = 20
)

type LifecycleState uint8


const (
	StateIdle LifecycleState = iota
	StateDialing
	StateOnlineUnsynchronized
	StateOnlineReady
	StateBackoff
	StateClosing
	StateClosed
)

type EventKind uint8

const (
	Announced EventKind = iota
	PropertiesChanged
	ValueReceived
	ServerUnannounced
	LocalInvalidated
)

// Snapshots handed to callers must be copied from the owner's private records.
type Sample struct {
	Value      any
	Timestamp  int64
	Epoch      uint64
	ReceivedAt time.Time
	Stale      bool
}

func (s Sample) Clone() Sample { s.Value = CloneValue(s.Value); return s }

type TopicSnapshot struct {
	Name, Type string
	ID         int32
	HasID      bool
	Properties map[string]any
	Epoch      uint64
	Stale      bool
}

func (t TopicSnapshot) Clone() TopicSnapshot {
	t.Properties = cloneJSONTrustedMap(t.Properties)
	return t
}

type Event struct {
	Kind       EventKind
	Topic      TopicSnapshot
	Sample     Sample
	Epoch      uint64
	ReceivedAt time.Time
	Ack        bool
}

func (e Event) Clone() Event { e.Topic = e.Topic.Clone(); e.Sample = e.Sample.Clone(); return e }

// Status is a point-in-time diagnostic, not a delivery acknowledgement.
type Status struct {
	State                                             LifecycleState
	Epoch                                             uint64
	Protocol                                          string
	Ready                                             bool
	ClockValid                                        bool
	RTT, ClockOffset                                  time.Duration
	LastLoss, LastError                               error
	CommandItems, CommandBytes                        uint64
	InboundItems, InboundBytes                        uint64
	WriterItems, WriterBytes                          uint64
	OfflineItems, OfflineBytes                        uint64
	RetainedBytes                                     uint64
	Dropped, Rejected, Uncertain, StateChangesDropped uint64
}

// Topic represents a NetworkTables topic.
type Topic struct {
	ID         int32
	Name       string
	Type       string
	TypeID     int
	Properties map[string]any
	PubUID     int32
	Publisher  *Publisher
	mu         sync.RWMutex
}

// UpdateProperties merges new properties into the topic's property map.
func (t *Topic) UpdateProperties(props map[string]any) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Properties == nil {
		t.Properties = make(map[string]any, len(props))
	}
	for k, v := range props {
		t.Properties[k] = v
	}
}

// TopicUpdate represents an update to a subscribed topic.
type TopicUpdate struct {
	Topic     *Topic
	Timestamp int64
	Value     any
}

// SubscribeOptions provides an intuitive configuration for subscriptions.
type SubscribeOptions struct {
	Periodic   time.Duration
	All        bool
	TopicsOnly bool
	Prefix     bool
}

