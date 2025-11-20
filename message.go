package nt4

// NT4 protocol methods.
const (
	methodPublish       = "publish"
	methodUnpublish     = "unpublish"
	methodSetProperties = "setproperties"
	methodSubscribe     = "subscribe"
	methodUnsubscribe   = "unsubscribe"
	methodAnnounce      = "announce"
	methodUnannounce    = "unannounce"
	methodProperties    = "properties"
)

// Text-based NT4 protocol message.
type jsonMessage struct {
	Method string `json:"method"`
	Params any    `json:"params"`
}

// Parameters for a publish message.
type publishParams struct {
	Name       string         `json:"name"`
	PubUID     int32          `json:"pubuid"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties,omitempty"`
}

// Parameters for an unpublish message.
type unpublishParams struct {
	PubUID int32 `json:"pubuid"`
}

// Parameters for a setproperties message.
type setPropertiesParams struct {
	Name   string         `json:"name"`
	Update map[string]any `json:"update"`
}

// Parameters for a subscribe message.
type subscribeParams struct {
	Topics  []string       `json:"topics"`
	SubUID  int32          `json:"subuid"`
	Options map[string]any `json:"options,omitempty"`
}

// Parameters for an unsubscribe message.
type unsubscribeParams struct {
	SubUID int32 `json:"subuid"`
}

// Binary NT4 value message.
type binaryMessage struct {
	TopicID   int32
	Timestamp int64
	TypeID    int
	Value     any
}

func newPublishMessage(name string, pubUID int32, typeStr string, props map[string]any) jsonMessage {
	return jsonMessage{
		Method: methodPublish,
		Params: publishParams{
			Name:       name,
			PubUID:     pubUID,
			Type:       typeStr,
			Properties: props,
		},
	}
}

func newUnpublishMessage(pubUID int32) jsonMessage {
	return jsonMessage{
		Method: methodUnpublish,
		Params: unpublishParams{
			PubUID: pubUID,
		},
	}
}

func newSubscribeMessage(topics []string, subUID int32, options *SubscribeOptions) jsonMessage {
	opts := make(map[string]any)

	if options != nil {
		if options.Periodic != 0 {
			opts["periodic"] = options.Periodic
		}
		if options.All {
			opts["all"] = true
		}
		if options.TopicsOnly {
			opts["topicsonly"] = true
		}
		if options.Prefix {
			opts["prefix"] = true
		}
	}

	params := subscribeParams{
		Topics: topics,
		SubUID: subUID,
	}

	if len(opts) > 0 {
		params.Options = opts
	}

	return jsonMessage{
		Method: methodSubscribe,
		Params: params,
	}
}

func newUnsubscribeMessage(subUID int32) jsonMessage {
	return jsonMessage{
		Method: methodUnsubscribe,
		Params: unsubscribeParams{
			SubUID: subUID,
		},
	}
}

func newSetPropertiesMessage(name string, update map[string]any) jsonMessage {
	return jsonMessage{
		Method: methodSetProperties,
		Params: setPropertiesParams{
			Name:   name,
			Update: update,
		},
	}
}
