package nt4

// TopicUpdate represents an update to a subscribed topic.
type TopicUpdate struct {
	// Topic is the Network Table topic that was updated.
	Topic *Topic

	// Server timestamp microseconds.
	Timestamp int64

	// The value to be set. The value should align to the Topic type.
	Value any
}
