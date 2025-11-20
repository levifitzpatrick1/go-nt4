package nt4

import (
	"encoding/json"
	"testing"
)

func TestNewPublishMessage(t *testing.T) {
	props := map[string]any{"persistent": true}
	msg := newPublishMessage("/test/topic", 123, TypeDouble, props)

	if msg.Method != methodPublish {
		t.Errorf("Expected method %s, got %s", methodPublish, msg.Method)
	}

	params, ok := msg.Params.(publishParams)
	if !ok {
		t.Fatal("Expected params to be publishParams")
	}

	if params.Name != "/test/topic" {
		t.Errorf("Expected name /test/topic, got %s", params.Name)
	}
	if params.PubUID != 123 {
		t.Errorf("Expected PubUID 123, got %d", params.PubUID)
	}
	if params.Type != TypeDouble {
		t.Errorf("Expected type %s, got %s", TypeDouble, params.Type)
	}
	if params.Properties["persistent"] != true {
		t.Error("Expected persistent property to be true")
	}
}

func TestNewUnpublishMessage(t *testing.T) {
	msg := newUnpublishMessage(456)

	if msg.Method != methodUnpublish {
		t.Errorf("Expected method %s, got %s", methodUnpublish, msg.Method)
	}

	params, ok := msg.Params.(unpublishParams)
	if !ok {
		t.Fatal("Expected params to be unpublishParams")
	}

	if params.PubUID != 456 {
		t.Errorf("Expected PubUID 456, got %d", params.PubUID)
	}
}

func TestNewSubscribeMessage(t *testing.T) {
	topics := []string{"/robot/speed", "/robot/position"}
	opts := &SubscribeOptions{
		Periodic:   0.5,
		All:        true,
		TopicsOnly: false,
		Prefix:     true,
	}

	msg := newSubscribeMessage(topics, 789, opts)

	if msg.Method != methodSubscribe {
		t.Errorf("Expected method %s, got %s", methodSubscribe, msg.Method)
	}

	params, ok := msg.Params.(subscribeParams)
	if !ok {
		t.Fatal("Expected params to be subscribeParams")
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

func TestNewSubscribeMessageNoOptions(t *testing.T) {
	msg := newSubscribeMessage([]string{"/test"}, 1, nil)

	params, ok := msg.Params.(subscribeParams)
	if !ok {
		t.Fatal("Expected params to be subscribeParams")
	}

	if params.Options != nil {
		t.Error("Expected Options to be nil when no options provided")
	}
}

func TestNewUnsubscribeMessage(t *testing.T) {
	msg := newUnsubscribeMessage(999)

	if msg.Method != methodUnsubscribe {
		t.Errorf("Expected method %s, got %s", methodUnsubscribe, msg.Method)
	}

	params, ok := msg.Params.(unsubscribeParams)
	if !ok {
		t.Fatal("Expected params to be unsubscribeParams")
	}

	if params.SubUID != 999 {
		t.Errorf("Expected SubUID 999, got %d", params.SubUID)
	}
}

func TestNewSetPropertiesMessage(t *testing.T) {
	update := map[string]any{
		"persistent": true,
		"cached":     false,
	}

	msg := newSetPropertiesMessage("/test/topic", update)

	if msg.Method != methodSetProperties {
		t.Errorf("Expected method %s, got %s", methodSetProperties, msg.Method)
	}

	params, ok := msg.Params.(setPropertiesParams)
	if !ok {
		t.Fatal("Expected params to be setPropertiesParams")
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
	msg := newPublishMessage("/test", 1, TypeDouble, nil)

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal message: %v", err)
	}

	if decoded["method"] != methodPublish {
		t.Errorf("Expected method %s, got %v", methodPublish, decoded["method"])
	}

	params, ok := decoded["params"].(map[string]any)
	if !ok {
		t.Fatal("Expected params to be a map")
	}

	if params["name"] != "/test" {
		t.Errorf("Expected name /test, got %v", params["name"])
	}
}
