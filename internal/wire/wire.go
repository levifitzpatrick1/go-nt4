// Package wire implements the NT4 protocol wire format: the JSON control
// messages exchanged as WebSocket text frames and the MessagePack value
// frames exchanged as binary frames.
//
// It is deliberately free of any client state so that the encoding can be
// tested in isolation from connection handling.
package wire

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"unicode/utf8"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// NT4 protocol methods.
const (
	MethodPublish       = "publish"
	MethodUnpublish     = "unpublish"
	MethodSetProperties = "setproperties"
	MethodSubscribe     = "subscribe"
	MethodUnsubscribe   = "unsubscribe"
	MethodAnnounce      = "announce"
	MethodUnannounce    = "unannounce"
	MethodProperties    = "properties"
)

// TimeSyncTopicID is the reserved topic ID used for time synchronization frames.
const TimeSyncTopicID = -1

// NT4 binary type IDs.
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

// timeSyncTypeID is the NT4 integer type ID, used for time sync frames.
const timeSyncTypeID = DataTypeInt

// Message is a text-based NT4 protocol message.
type Message struct {
	Method string `json:"method"`
	Params any    `json:"params"`
}

type PublishParams struct {
	Name       string         `json:"name"`
	PubUID     int32          `json:"pubuid"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
}

type UnpublishParams struct {
	PubUID int32 `json:"pubuid"`
}

type SetPropertiesParams struct {
	Name   string         `json:"name"`
	Update map[string]any `json:"update"`
}

type SubscribeParams struct {
	Topics  []string       `json:"topics"`
	SubUID  int32          `json:"subuid"`
	Options map[string]any `json:"options"`
}

type UnsubscribeParams struct {
	SubUID int32 `json:"subuid"`
}

func Publish(name string, pubUID int32, typeStr string, props map[string]any) Message {
	if props == nil {
		props = map[string]any{}
	}
	return Message{Method: MethodPublish, Params: PublishParams{Name: name, PubUID: pubUID, Type: typeStr, Properties: props}}
}

func Unpublish(pubUID int32) Message {
	return Message{Method: MethodUnpublish, Params: UnpublishParams{PubUID: pubUID}}
}

func Subscribe(topics []string, subUID int32, options map[string]any) Message {
	if options == nil {
		options = map[string]any{}
	}
	return Message{Method: MethodSubscribe, Params: SubscribeParams{Topics: topics, SubUID: subUID, Options: options}}
}

func Unsubscribe(subUID int32) Message {
	return Message{Method: MethodUnsubscribe, Params: UnsubscribeParams{SubUID: subUID}}
}

func SetProperties(name string, update map[string]any) Message {
	if update == nil {
		update = map[string]any{}
	}
	return Message{Method: MethodSetProperties, Params: SetPropertiesParams{Name: name, Update: update}}
}

// EncodeValue encodes a single validated NT4 value frame: [topicID, timestamp, typeID, value].
func EncodeValue(topicID int32, timestamp int64, typeID int, value any) ([]byte, error) {
	if topicID < TimeSyncTopicID {
		return nil, fmt.Errorf("invalid topic id %d", topicID)
	}
	if timestamp < 0 {
		return nil, fmt.Errorf("negative timestamp %d", timestamp)
	}
	v, err := NormalizeValue(typeID, value)
	if err != nil {
		return nil, err
	}
	if typeID < 0 || typeID > math.MaxInt32 {
		return nil, fmt.Errorf("invalid type id %d", typeID)
	}
	b, err := msgpack.Marshal([]any{topicID, timestamp, int32(typeID), v})
	if err == nil && len(b) > maxWireBytes {
		return nil, fmt.Errorf("frame exceeds byte limit")
	}
	return b, err
}

func EncodeTimeSyncRequest(clientTime int64) ([]byte, error) {
	return msgpack.Marshal([]any{int32(TimeSyncTopicID), int64(0), int32(timeSyncTypeID), clientTime})
}

// DecodeFrames decodes and validates concatenated MessagePack frames. Frames
// decoded before a malformed frame are returned with the error.
// Frame is a validated binary NT tuple. Value is owned by the decoder.
type Frame struct {
	TopicID   int32
	Timestamp int64
	TypeID    int
	Value     any
}

// WalkFrames visits complete tuples in wire order. A malformed suffix returns
// an error after visits for all preceding complete tuples; visit errors stop
// traversal immediately. Callers must bound their input WebSocket message.
func WalkFrames(data []byte, visit func(Frame) error) error {
	reader := bytes.NewReader(data)
	decoder := msgpack.NewDecoder(reader)
	for reader.Len() > 0 {
		if reader.Len() > maxWireBytes {
			return fmt.Errorf("binary message exceeds byte limit")
		}
		frame, err := decodeFrame(decoder)
		if err != nil {
			return err
		}
		if err := visit(frame); err != nil {
			return err
		}
	}
	return nil
}

// DecodeFrames retains the original tuple ABI and returns complete frames
// preceding a malformed suffix. New code should use WalkFrames.
func DecodeFrames(data []byte) ([][]any, error) {
	var frames [][]any
	err := WalkFrames(data, func(f Frame) error {
		frames = append(frames, []any{f.TopicID, f.Timestamp, f.TypeID, f.Value})
		return nil
	})
	return frames, err
}

const (
	maxWireBytes  = 1 << 20
	maxArrayItems = 1 << 16
)

func decodeFrame(decoder *msgpack.Decoder) (Frame, error) {
	ln, err := decoder.DecodeArrayLen()
	if err != nil || ln != 4 {
		return Frame{}, fmt.Errorf("invalid NT frame length %d: %w", ln, err)
	}
	id64, err := decodeInteger(decoder)
	if err != nil || id64 < TimeSyncTopicID || id64 > math.MaxInt32 {
		return Frame{}, fmt.Errorf("invalid topic id: %v", err)
	}
	ts, err := decodeInteger(decoder)
	if err != nil || ts < 0 {
		return Frame{}, fmt.Errorf("invalid timestamp: %v", err)
	}
	type64, err := decodeInteger(decoder)
	if err != nil || type64 < 0 || type64 > math.MaxInt32 {
		return Frame{}, fmt.Errorf("invalid type id: %v", err)
	}
	v, err := decodeValue(decoder, int(type64))
	if err != nil {
		return Frame{}, err
	}
	return Frame{int32(id64), ts, int(type64), v}, nil
}

func decodeInteger(d *msgpack.Decoder) (int64, error) {
	c, err := d.PeekCode()
	if err != nil {
		return 0, err
	}
	if !(msgpcode.IsFixedNum(c) || c >= msgpcode.Uint8 && c <= msgpcode.Int64) {
		return 0, fmt.Errorf("not an integer: %x", c)
	}
	if c == msgpcode.Uint64 {
		// DecodeInt64 otherwise wraps uint64 values above MaxInt64.
		n, err := d.DecodeUint64()
		if err != nil || n > math.MaxInt64 {
			return 0, fmt.Errorf("integer overflow: %v", err)
		}
		return int64(n), nil
	}
	return d.DecodeInt64()
}

func decodeBool(d *msgpack.Decoder) (bool, error) {
	c, err := d.PeekCode()
	if err != nil {
		return false, err
	}
	if c != msgpcode.True && c != msgpcode.False {
		return false, fmt.Errorf("expected boolean, got %x", c)
	}
	return d.DecodeBool()
}

func requireCode(d *msgpack.Decoder, family string) error {
	c, err := d.PeekCode()
	if err != nil {
		return err
	}
	valid := false
	switch family {
	case "double":
		valid = c == msgpcode.Double
	case "float":
		valid = c == msgpcode.Float
	case "string":
		valid = msgpcode.IsFixedString(c) || c == msgpcode.Str8 || c == msgpcode.Str16 || c == msgpcode.Str32
	case "bin":
		valid = c == msgpcode.Bin8 || c == msgpcode.Bin16 || c == msgpcode.Bin32
	}
	if !valid {
		return fmt.Errorf("expected %s family, got %x", family, c)
	}
	return nil
}

func decodeValue(decoder *msgpack.Decoder, typeID int) (any, error) {
	switch typeID {
	case DataTypeBoolean:
		return decodeBool(decoder)
	case DataTypeDouble:
		if err := requireCode(decoder, "double"); err != nil {
			return nil, err
		}
		return decoder.DecodeFloat64()
	case DataTypeInt:
		return decodeInteger(decoder)
	case DataTypeFloat:
		if err := requireCode(decoder, "float"); err != nil {
			return nil, err
		}
		return decoder.DecodeFloat32()
	case DataTypeString:
		b, err := decodeLimitedBytes(decoder, "string")
		if err != nil {
			return nil, err
		}
		if !utf8.Valid(b) {
			return nil, errors.New("invalid UTF-8 string")
		}
		return string(b), nil
	case DataTypeBinary:
		return decodeLimitedBytes(decoder, "bin")
	case DataTypeBooleanArray:
		ln, err := limitedArrayLen(decoder)
		if err != nil {
			return nil, err
		}
		out := make([]bool, ln)
		for i := range out {
			out[i], err = decodeBool(decoder)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	case DataTypeDoubleArray:
		ln, err := limitedArrayLen(decoder)
		if err != nil {
			return nil, err
		}
		out := make([]float64, ln)
		for i := range out {
			if err = requireCode(decoder, "double"); err != nil {
				return nil, err
			}
			out[i], err = decoder.DecodeFloat64()
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	case DataTypeIntArray:
		ln, err := limitedArrayLen(decoder)
		if err != nil {
			return nil, err
		}
		out := make([]int64, ln)
		for i := range out {
			out[i], err = decodeInteger(decoder)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	case DataTypeFloatArray:
		ln, err := limitedArrayLen(decoder)
		if err != nil {
			return nil, err
		}
		out := make([]float32, ln)
		for i := range out {
			if err = requireCode(decoder, "float"); err != nil {
				return nil, err
			}
			out[i], err = decoder.DecodeFloat32()
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	case DataTypeStringArray:
		ln, err := limitedArrayLen(decoder)
		if err != nil {
			return nil, err
		}
		out := make([]string, ln)
		for i := range out {
			b, err := decodeLimitedBytes(decoder, "string")
			if err != nil {
				return nil, err
			}
			if !utf8.Valid(b) {
				return nil, errors.New("invalid UTF-8 string")
			}
			out[i] = string(b)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown NT type id %d", typeID)
	}
}

func limitedArrayLen(decoder *msgpack.Decoder) (int, error) {
	ln, err := decoder.DecodeArrayLen()
	if err != nil {
		return 0, err
	}
	if ln < 0 || ln > maxArrayItems {
		return 0, fmt.Errorf("array length %d exceeds limit", ln)
	}
	return ln, nil
}

func decodeLimitedBytes(decoder *msgpack.Decoder, family string) ([]byte, error) {
	if err := requireCode(decoder, family); err != nil {
		return nil, err
	}
	ln, err := decoder.DecodeBytesLen()
	if err != nil {
		return nil, err
	}
	if ln < 0 {
		return nil, errors.New("nil bytes not valid")
	}
	if ln > maxWireBytes {
		return nil, fmt.Errorf("byte length %d exceeds limit", ln)
	}
	b := make([]byte, ln)
	if err := decoder.ReadFull(b); err != nil {
		return nil, err
	}
	return b, nil
}

func checkedInt(v any) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case int16:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case uint:
		if uint64(n) > math.MaxInt64 {
			return 0, errors.New("overflow")
		}
		return int64(n), nil
	case uint8:
		return int64(n), nil
	case uint16:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case uint64:
		if n > math.MaxInt64 {
			return 0, errors.New("overflow")
		}
		return int64(n), nil
	default:
		return 0, fmt.Errorf("not integer %T", v)
	}
}

// NormalizeValue validates an NT value for typeID and returns an owned,
// canonical Go representation suitable for encoding or delivery.
func NormalizeValue(typeID int, value any) (any, error) {
	switch typeID {
	case DataTypeBoolean:
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("boolean value is %T", value)
		}
		return v, nil
	case DataTypeDouble:
		switch v := value.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		default:
			return nil, fmt.Errorf("double value is %T", value)
		}
	case DataTypeInt:
		v, err := checkedInt(value)
		if err != nil {
			return nil, fmt.Errorf("int value is %T", value)
		}
		return v, nil
	case DataTypeFloat:
		switch v := value.(type) {
		case float32:
			return v, nil
		case float64:
			return float32(v), nil
		default:
			return nil, fmt.Errorf("float value is %T", value)
		}
	case DataTypeString:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("string value is %T", value)
		}
		if !utf8.ValidString(v) || len(v) > maxWireBytes {
			return nil, errors.New("invalid or oversized UTF-8 string")
		}
		return v, nil
	case DataTypeBinary:
		v, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("binary value is %T", value)
		}
		if v == nil || len(v) > maxWireBytes {
			return nil, errors.New("nil or oversized binary value")
		}
		return append([]byte(nil), v...), nil
	case DataTypeBooleanArray:
		return normalizeSlice[bool](value, "boolean[]")
	case DataTypeDoubleArray:
		return normalizeFloat64Slice(value)
	case DataTypeIntArray:
		return normalizeInt64Slice(value)
	case DataTypeFloatArray:
		return normalizeFloat32Slice(value)
	case DataTypeStringArray:
		out, err := normalizeSlice[string](value, "string[]")
		if err != nil {
			return nil, err
		}
		for _, s := range out {
			if !utf8.ValidString(s) || len(s) > maxWireBytes {
				return nil, errors.New("invalid or oversized UTF-8 array element")
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown NT type id %d", typeID)
	}
}

func normalizeSlice[T any](value any, name string) ([]T, error) {
	if value == nil || reflect.ValueOf(value).Kind() != reflect.Slice || reflect.ValueOf(value).IsNil() {
		return nil, fmt.Errorf("nil or non-slice %s value %T", name, value)
	}
	if v, ok := value.([]T); ok {
		if len(v) > maxArrayItems {
			return nil, errors.New("array too large")
		}
		return append([]T(nil), v...), nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s value is %T", name, value)
	}
	if len(items) > maxArrayItems {
		return nil, errors.New("array too large")
	}
	out := make([]T, len(items))
	for i, item := range items {
		x, ok := item.(T)
		if !ok {
			return nil, fmt.Errorf("%s element %d is %T", name, i, item)
		}
		out[i] = x
	}
	return out, nil
}

func normalizeFloat64Slice(value any) ([]float64, error) {
	if value == nil || reflect.ValueOf(value).Kind() != reflect.Slice || reflect.ValueOf(value).IsNil() {
		return nil, fmt.Errorf("nil or non-slice double[] value %T", value)
	}
	switch v := value.(type) {
	case []float64:
		if len(v) > maxArrayItems {
			return nil, errors.New("array too large")
		}
		return append([]float64(nil), v...), nil
	case []any:
		if len(v) > maxArrayItems {
			return nil, errors.New("array too large")
		}
		out := make([]float64, len(v))
		for i, item := range v {
			switch n := item.(type) {
			case float64:
				out[i] = n
			case float32:
				out[i] = float64(n)
			default:
				return nil, fmt.Errorf("double[] element %d is %T", i, item)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("double[] value is %T", value)
	}
}

func normalizeFloat32Slice(value any) ([]float32, error) {
	if value == nil || reflect.ValueOf(value).Kind() != reflect.Slice || reflect.ValueOf(value).IsNil() {
		return nil, fmt.Errorf("nil or non-slice float[] value %T", value)
	}
	switch v := value.(type) {
	case []float32:
		if len(v) > maxArrayItems {
			return nil, errors.New("array too large")
		}
		return append([]float32(nil), v...), nil
	case []any:
		if len(v) > maxArrayItems {
			return nil, errors.New("array too large")
		}
		out := make([]float32, len(v))
		for i, item := range v {
			switch n := item.(type) {
			case float32:
				out[i] = n
			case float64:
				out[i] = float32(n)
			default:
				return nil, fmt.Errorf("float[] element %d is %T", i, item)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("float[] value is %T", value)
	}
}

func normalizeInt64Slice(value any) ([]int64, error) {
	if value == nil || reflect.ValueOf(value).Kind() != reflect.Slice || reflect.ValueOf(value).IsNil() {
		return nil, fmt.Errorf("nil or non-slice int[] value %T", value)
	}
	switch v := value.(type) {
	case []int64:
		if len(v) > maxArrayItems {
			return nil, errors.New("array too large")
		}
		return append([]int64(nil), v...), nil
	case []int:
		if len(v) > maxArrayItems {
			return nil, errors.New("array too large")
		}
		out := make([]int64, len(v))
		for i, n := range v {
			out[i] = int64(n)
		}
		return out, nil
	case []any:
		if len(v) > maxArrayItems {
			return nil, errors.New("array too large")
		}
		out := make([]int64, len(v))
		for i, item := range v {
			n, err := checkedInt(item)
			if err != nil {
				return nil, fmt.Errorf("int[] element %d is %T", i, item)
			}
			out[i] = n
		}
		return out, nil
	default:
		return nil, fmt.Errorf("int[] value is %T", value)
	}
}
