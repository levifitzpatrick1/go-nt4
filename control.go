package nt4

import (
	"bytes"
	"encoding/json"
	"math"
	"unicode/utf8"
)

type controlEvent struct {
	method, name, typ string
	id                int32
	props             map[string]any
	ack               bool
}

// parseControls isolates invalid array members, while rejecting invalid envelopes.
func parseControls(data []byte, o ClientOptions) ([]controlEvent, error) {
	if !utf8.Valid(data) || len(data) > o.MaxTextBytes || !validJSONDepth(data, o.MaxJSONDepth) {
		return nil, ErrProtocol
	}
	var elements []json.RawMessage
	if err := json.Unmarshal(data, &elements); err != nil || elements == nil || len(elements) > o.MaxJSONContainerItems {
		return nil, ErrProtocol
	}
	out := make([]controlEvent, 0, len(elements))
	for _, raw := range elements {
		var envelope struct {
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal(raw, &envelope) != nil {
			continue
		}
		if envelope.Method != "announce" && envelope.Method != "unannounce" && envelope.Method != "properties" {
			continue
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(envelope.Params, &fields) != nil || fields == nil {
			continue
		}
		var ev controlEvent
		ev.method = envelope.Method
		if json.Unmarshal(fields["name"], &ev.name) != nil || len(ev.name) > o.MaxNameBytes {
			continue
		}
		if ev.method != "properties" {
			var n json.Number
			if json.Unmarshal(fields["id"], &n) != nil {
				continue
			}
			i, err := n.Int64()
			if err != nil || i < 0 || i > math.MaxInt32 {
				continue
			}
			ev.id = int32(i)
		}
		if ev.method == "announce" {
			if json.Unmarshal(fields["type"], &ev.typ) != nil || ev.typ == "" || len(ev.typ) > o.MaxNameBytes {
				continue
			}
			if !parseProperties(fields["properties"], o, &ev.props) {
				continue
			}
		}
		if ev.method == "properties" {
			if !parseProperties(fields["update"], o, &ev.props) {
				continue
			}
			if b, ok := fields["ack"]; ok && json.Unmarshal(b, &ev.ack) != nil {
				continue
			}
		}
		out = append(out, ev)
	}
	return out, nil
}
func parseProperties(raw json.RawMessage, o ClientOptions, out *map[string]any) bool {
	if len(raw) == 0 || raw[0] != '{' {
		return false
	}
	var props map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&props) != nil {
		return false
	}
	owned, _, err := OwnJSON(props, JSONLimits{o.MaxTextBytes, o.MaxJSONDepth, o.MaxJSONContainerItems, o.MaxNameBytes})
	if err != nil {
		return false
	}
	*out = owned
	return true
}
func validJSONDepth(b []byte, max int) bool {
	depth := 0
	quoted := false
	escape := false
	for _, c := range b {
		if quoted {
			if escape {
				escape = false
			} else if c == '\\' {
				escape = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		switch c {
		case '"':
			quoted = true
		case '{', '[':
			depth++
			if depth > max {
				return false
			}
		case '}', ']':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0 && !quoted
}
