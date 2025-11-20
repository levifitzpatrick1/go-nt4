package nt4

import (
	"maps"
	"sync"
)

// A NT4 topic.
type Topic struct {
	// Server assigned topic ID.
	ID int32

	// Topic path ("/robot/speed").
	Name string

	// NT4 type string (e.g., "double", "string", "boolean[]").
	// Should be pulled from NT4 Type String constants.
	Type string

	// Type ID for binary messages.
	// Should be pulled from NT4 Type Binary constants.
	TypeID int

	// Topic metadata (e.g., "persistent", "retained").
	Properties map[string]any

	// Publisher UID.
	PubUID int32

	mu sync.RWMutex
}

// Thread safe merge of new properties into current.
func (t *Topic) UpdateProperties(props map[string]any) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Properties == nil {
		t.Properties = make(map[string]any)
	}
	maps.Copy(t.Properties, props)
}
