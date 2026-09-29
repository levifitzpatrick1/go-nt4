package wire

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestPublish(t *testing.T) {
	props := map[string]any{"persistent": true}
	msg := Publish("/test/topic", 123, "double", props)

	if msg.Method != MethodPublish {
		t.Errorf("Expected method %s, got %s", MethodPublish, msg.Method)
	}

	params, ok := msg.Params.(PublishParams)
	if !ok {
		t.Fatal("Expected params to be PublishParams")
	}

	if params.Name != "/test/topic" {
		t.Errorf("Expected name /test/topic, got %s", params.Name)
	}
	if params.PubUID != 123 {
		t.Errorf("Expected PubUID 123, got %d", params.PubUID)
	}
	if params.Type != "double" {
		t.Errorf("Expected type double, got %s", params.Type)
	}
	if params.Properties["persistent"] != true {
		t.Error("Expected persistent property to be true")
	}
}

func TestUnpublish(t *testing.T) {
	msg := Unpublish(456)

	if msg.Method != MethodUnpublish {
		t.Errorf("Expected method %s, got %s", MethodUnpublish, msg.Method)
	}

	params, ok := msg.Params.(UnpublishParams)
	if !ok {
		t.Fatal("Expected params to be UnpublishParams")
	}

	if params.PubUID != 456 {
		t.Errorf("Expected PubUID 456, got %d", params.PubUID)
	}
}

func TestSubscribe(t *testing.T) {
	topics := []string{"/robot/speed", "/robot/position"}
	options := map[string]any{
		"periodic": 0.5,
		"all":      true,
		"prefix":   true,
	}

	msg := Subscribe(topics, 789, options)

	if msg.Method != MethodSubscribe {
		t.Errorf("Expected method %s, got %s", MethodSubscribe, msg.Method)
	}

	params, ok := msg.Params.(SubscribeParams)
	if !ok {
		t.Fatal("Expected params to be SubscribeParams")
	}

	if len(params.Topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(params.Topics))
	}
	if params.SubUID != 789 {
		t.Errorf("Expected SubUID 789, got %d", params.SubUID)
	}

	if params.Options["periodic"] != 0.5 {
		t.Errorf("Expected periodic 0.5, got %v", params.Options["periodic"])
	}
	if params.Options["all"] != true {
		t.Error("Expected all to be true")
	}
	if params.Options["prefix"] != true {
		t.Error("Expected prefix to be true")
	}
}

func TestSubscribeNoOptions(t *testing.T) {
	msg := Subscribe([]string{"/test"}, 1, nil)

	params, ok := msg.Params.(SubscribeParams)
	if !ok {
		t.Fatal("Expected params to be SubscribeParams")
	}

	if params.Options == nil || len(params.Options) != 0 {
		t.Error("Expected empty Options object when no options provided")
	}
}

func TestUnsubscribe(t *testing.T) {
	msg := Unsubscribe(999)

	if msg.Method != MethodUnsubscribe {
		t.Errorf("Expected method %s, got %s", MethodUnsubscribe, msg.Method)
	}

	params, ok := msg.Params.(UnsubscribeParams)
	if !ok {
		t.Fatal("Expected params to be UnsubscribeParams")
	}

	if params.SubUID != 999 {
		t.Errorf("Expected SubUID 999, got %d", params.SubUID)
	}
}

func TestSetProperties(t *testing.T) {
	update := map[string]any{
		"persistent": true,
		"cached":     false,
	}

	msg := SetProperties("/test/topic", update)

	if msg.Method != MethodSetProperties {
		t.Errorf("Expected method %s, got %s", MethodSetProperties, msg.Method)
	}

	params, ok := msg.Params.(SetPropertiesParams)
	if !ok {
		t.Fatal("Expected params to be SetPropertiesParams")
	}

	if params.Name != "/test/topic" {
		t.Errorf("Expected name /test/topic, got %s", params.Name)
	}
	if params.Update["persistent"] != true {
		t.Error("Expected persistent to be true")
	}
	if params.Update["cached"] != false {
		t.Error("Expected cached to be false")
	}
}

func TestMessageJSONSerialization(t *testing.T) {
	msg := Publish("/test", 1, "double", nil)

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal message: %v", err)
	}

	if decoded["method"] != MethodPublish {
		t.Errorf("Expected method %s, got %v", MethodPublish, decoded["method"])
	}

	params, ok := decoded["params"].(map[string]any)
	if !ok {
		t.Fatal("Expected params to be a map")
	}

	if params["name"] != "/test" {
		t.Errorf("Expected name /test, got %v", params["name"])
	}
}

func TestEncodeValueRoundTrip(t *testing.T) {
	data, err := EncodeValue(7, 1234, 1, 42.5)
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	frames, err := DecodeFrames(data)
	if err != nil {
		t.Fatalf("DecodeFrames failed: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("Expected 1 frame, got %d", len(frames))
	}
	if len(frames[0]) != 4 {
		t.Fatalf("Expected 4 fields, got %d", len(frames[0]))
	}
	if frames[0][3] != 42.5 {
		t.Errorf("Expected value 42.5, got %v", frames[0][3])
	}
}

func TestDecodeFramesMultiple(t *testing.T) {
	a, _ := EncodeValue(1, 10, 1, 1.0)
	b, _ := EncodeValue(2, 20, 1, 2.0)

	frames, err := DecodeFrames(append(a, b...))
	if err != nil {
		t.Fatalf("DecodeFrames failed: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("Expected 2 frames, got %d", len(frames))
	}
}

func TestDecodeFramesReturnsPartialOnError(t *testing.T) {
	good, _ := EncodeValue(1, 10, 1, 1.0)
	data := append(good, 0xc1) // 0xc1 is never a valid msgpack byte

	frames, err := DecodeFrames(data)
	if err == nil {
		t.Fatal("Expected an error for malformed trailing data")
	}
	if len(frames) != 1 {
		t.Fatalf("Expected the 1 intact frame to be returned, got %d", len(frames))
	}
}

func TestEncodeTimeSyncRequest(t *testing.T) {
	data, err := EncodeTimeSyncRequest(999)
	if err != nil {
		t.Fatalf("EncodeTimeSyncRequest failed: %v", err)
	}

	frames, err := DecodeFrames(data)
	if err != nil {
		t.Fatalf("DecodeFrames failed: %v", err)
	}
	if len(frames) != 1 || len(frames[0]) != 4 {
		t.Fatalf("Unexpected frame shape: %v", frames)
	}
	if got := fmt.Sprint(frames[0][0]); got != fmt.Sprint(TimeSyncTopicID) {
		t.Errorf("Expected topic ID %d, got %v (%T)", TimeSyncTopicID, frames[0][0], frames[0][0])
	}
	if got := fmt.Sprint(frames[0][3]); got != "999" {
		t.Errorf("Expected client time 999, got %v", frames[0][3])
	}
}
