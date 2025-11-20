package nt4

import (
	"sync"
	"testing"
)

func TestTopicUpdateProperties(t *testing.T) {
	topic := &Topic{
		ID:         1,
		Name:       "/test/topic",
		Type:       TypeDouble,
		TypeID:     DataTypeDouble,
		Properties: map[string]any{"persistent": true},
	}

	// Update with new properties
	topic.UpdateProperties(map[string]any{
		"retained": true,
		"cached":   false,
	})

	if topic.Properties["persistent"] != true {
		t.Error("Expected persistent property to remain")
	}
	if topic.Properties["retained"] != true {
		t.Error("Expected retained property to be added")
	}
	if topic.Properties["cached"] != false {
		t.Error("Expected cached property to be added")
	}
}

func TestTopicUpdatePropertiesNil(t *testing.T) {
	topic := &Topic{
		ID:     1,
		Name:   "/test/topic",
		Type:   TypeDouble,
		TypeID: DataTypeDouble,
	}

	// Update when Properties is nil
	topic.UpdateProperties(map[string]any{
		"persistent": true,
	})

	if topic.Properties == nil {
		t.Fatal("Expected Properties map to be initialized")
	}
	if topic.Properties["persistent"] != true {
		t.Error("Expected persistent property to be set")
	}
}

func TestTopicUpdatePropertiesOverwrite(t *testing.T) {
	topic := &Topic{
		ID:         1,
		Name:       "/test/topic",
		Type:       TypeDouble,
		TypeID:     DataTypeDouble,
		Properties: map[string]any{"value": 1},
	}

	topic.UpdateProperties(map[string]any{
		"value": 2,
	})

	if topic.Properties["value"] != 2 {
		t.Errorf("Expected value to be overwritten to 2, got %v", topic.Properties["value"])
	}
}

func TestTopicUpdatePropertiesConcurrent(t *testing.T) {
	topic := &Topic{
		ID:         1,
		Name:       "/test/topic",
		Type:       TypeDouble,
		TypeID:     DataTypeDouble,
		Properties: make(map[string]any),
	}

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent updates
	for i := range iterations {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			topic.UpdateProperties(map[string]any{
				"key": val,
			})
		}(i)
	}

	wg.Wait()

	// Should have completed without panicking
	if topic.Properties == nil {
		t.Fatal("Expected Properties to be initialized")
	}
}
