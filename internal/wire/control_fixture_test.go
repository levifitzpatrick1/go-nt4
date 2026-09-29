package wire

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Required parameter objects must be present even when empty. This is a
// JSON-shape assertion, not a marshal-then-unmarshal round-trip oracle.
func TestControlGolden(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		want string
	}{
		{"publish-empty-properties", Publish("/a", 1, "double", nil), `{"method":"publish","params":{"name":"/a","pubuid":1,"type":"double","properties":{}}}`},
		{"subscribe-empty-options", Subscribe([]string{"/a"}, 2, nil), `{"method":"subscribe","params":{"topics":["/a"],"subuid":2,"options":{}}}`},
		{"unpublish", Unpublish(1), `{"method":"unpublish","params":{"pubuid":1}}`},
		{"unsubscribe", Unsubscribe(2), `{"method":"unsubscribe","params":{"subuid":2}}`},
		{"setproperties-nested-null", SetProperties("/a", map[string]any{"nested": map[string]any{"a": []any{true, nil}}, "old": nil}), `{"method":"setproperties","params":{"name":"/a","update":{"nested":{"a":[true,null]},"old":null}}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal([]Message{tc.msg})
			if err != nil {
				t.Fatal(err)
			}
			var got, want any
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte("["+tc.want+"]"), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %s, want [%s]", b, tc.want)
			}
		})
	}
}
