package nt4

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"unicode/utf8"

	"github.com/levifitzpatrick1/go-nt4/internal/wire"
)

// TypeID preserves unknown textual types as binary, rather than guessing their encoding.
func TypeID(name string) (int, error) {
	if name == "" || !utf8.ValidString(name) {
		return 0, ErrInvalidType
	}
	switch name {
	case TypeBoolean:
		return wire.DataTypeBoolean, nil
	case TypeDouble:
		return wire.DataTypeDouble, nil
	case TypeInt:
		return wire.DataTypeInt, nil
	case TypeFloat:
		return wire.DataTypeFloat, nil
	case TypeString, TypeJSON:
		return wire.DataTypeString, nil
	case TypeBooleanArray:
		return wire.DataTypeBooleanArray, nil
	case TypeDoubleArray:
		return wire.DataTypeDoubleArray, nil
	case TypeIntArray:
		return wire.DataTypeIntArray, nil
	case TypeFloatArray:
		return wire.DataTypeFloatArray, nil
	case TypeStringArray:
		return wire.DataTypeStringArray, nil
	default:
		return wire.DataTypeBinary, nil
	}
}

// OwnValue validates, copies and measures a value before admission. The size is
// its upper-bound payload estimate (array headers and lengths included).
func OwnValue(typeName string, input any, maxBytes int) (any, int, error) {
	id, err := TypeID(typeName)
	if err != nil {
		return nil, 0, err
	}
	if maxBytes <= 0 {
		return nil, 0, ErrInvalidOptions
	}
	// Reject large inputs before the wire normalizer allocates a copy.
	if n, ok := valueSize(id, input, maxBytes); !ok || n > maxBytes {
		return nil, 0, ErrInvalidValue
	}
	v, err := wire.NormalizeValue(id, input)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %v", ErrInvalidValue, err)
	}
	// Preserve valid empty arrays as non-nil after wire normalization.
	switch id {
	case wire.DataTypeBinary:
		if v.([]byte) == nil {
			v = []byte{}
		}
	case wire.DataTypeBooleanArray:
		if v.([]bool) == nil {
			v = []bool{}
		}
	case wire.DataTypeDoubleArray:
		if v.([]float64) == nil {
			v = []float64{}
		}
	case wire.DataTypeIntArray:
		if v.([]int64) == nil {
			v = []int64{}
		}
	case wire.DataTypeFloatArray:
		if v.([]float32) == nil {
			v = []float32{}
		}
	case wire.DataTypeStringArray:
		if v.([]string) == nil {
			v = []string{}
		}
	}
	if id == wire.DataTypeString && !utf8.ValidString(v.(string)) {
		return nil, 0, ErrInvalidValue
	}
	if id == wire.DataTypeStringArray {
		for _, s := range v.([]string) {
			if !utf8.ValidString(s) {
				return nil, 0, ErrInvalidValue
			}
		}
	}
	return v, ValueSize(v), nil
}

// CloneValue owns the mutable NT families; use only with already validated values.
func CloneValue(v any) any {
	switch x := v.(type) {
	case []byte:
		return append([]byte{}, x...)
	case []bool:
		return append([]bool{}, x...)
	case []float32:
		return append([]float32{}, x...)
	case []float64:
		return append([]float64{}, x...)
	case []int64:
		return append([]int64{}, x...)
	case []string:
		return append([]string{}, x...)
	default:
		return v
	}
}

// ValueSize counts a validated value's estimated serialized payload bytes.
func ValueSize(v any) int {
	switch x := v.(type) {
	case bool:
		return 1
	case float32:
		return 5
	case float64, int64:
		return 9
	case string:
		return len(x) + 5
	case []byte:
		return len(x) + 5
	case []bool:
		return len(x) + 5
	case []float32:
		return len(x)*5 + 5
	case []float64:
		return len(x)*9 + 5
	case []int64:
		return len(x)*9 + 5
	case []string:
		n := 5
		for _, s := range x {
			n += len(s) + 5
		}
		return n
	default:
		return 0
	}
}
func valueSize(id int, v any, limit int) (int, bool) {
	switch id {
	case wire.DataTypeBoolean:
		_, ok := v.(bool)
		return 1, ok
	case wire.DataTypeDouble:
		switch v.(type) {
		case float32, float64:
			return 9, true
		}
		return 0, false
	case wire.DataTypeInt:
		switch x := v.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32:
			return 9, true
		case uint64:
			return 9, x <= math.MaxInt64
		}
		return 0, false
	case wire.DataTypeFloat:
		switch v.(type) {
		case float32, float64:
			return 5, true
		}
		return 0, false
	case wire.DataTypeString:
		x, ok := v.(string)
		return len(x) + 5, ok
	case wire.DataTypeBinary:
		x, ok := v.([]byte)
		return len(x) + 5, ok && x != nil
	}
	// NormalizeValue supports typed arrays and []any; bound both before cloning.
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || rv.Kind() != reflect.Slice || rv.IsNil() {
		return 0, false
	}
	count := rv.Len()
	unit := 9
	if id == wire.DataTypeBooleanArray || id == wire.DataTypeStringArray {
		unit = 1
	}
	if id == wire.DataTypeFloatArray {
		unit = 5
	}
	if limit < 5 || count > (limit-5)/unit {
		return limit + 1, false
	}
	n := 5 + count*unit
	if id == wire.DataTypeStringArray {
		n = 5
		for i := 0; i < count; i++ {
			s, ok := rv.Index(i).Interface().(string)
			if !ok || len(s) > limit-n-5 {
				return limit + 1, false
			}
			n += len(s) + 5
		}
	}
	return n, true
}

// JSONLimits are finite admission limits, independent of transport frame limits.
type JSONLimits struct{ MaxBytes, MaxDepth, MaxContainerItems, MaxNameBytes int }

func (l JSONLimits) valid() bool {
	return l.MaxBytes > 0 && l.MaxDepth > 0 && l.MaxContainerItems > 0 && l.MaxNameBytes > 0
}

type jsonVisit struct {
	kind uint8
	ptr  uintptr
}
type jsonWalker struct {
	limits       JSONLimits
	bytes, items int
	active       map[jsonVisit]bool
}

func (w *jsonWalker) add(n int) error {
	if n < 0 || n > w.limits.MaxBytes-w.bytes {
		return ErrInvalidValue
	}
	w.bytes += n
	return nil
}
func (w *jsonWalker) item(n int) error {
	if n > w.limits.MaxContainerItems-w.items {
		return ErrInvalidValue
	}
	w.items += n
	return nil
}

// OwnJSON validates JSON before encoding and returns a deep-owned tree. Null
// property values remain nil for protocol deletion semantics.
func OwnJSON(input map[string]any, limits JSONLimits) (map[string]any, int, error) {
	if !limits.valid() {
		return nil, 0, ErrInvalidOptions
	}
	if input == nil {
		input = map[string]any{}
	}
	w := jsonWalker{limits: limits, active: make(map[jsonVisit]bool)}
	v, err := w.walk(input, 1)
	if err != nil {
		return nil, 0, err
	}
	return v.(map[string]any), w.bytes, nil
}
func (w *jsonWalker) walk(v any, depth int) (any, error) {
	if depth > w.limits.MaxDepth {
		return nil, ErrInvalidValue
	}
	switch x := v.(type) {
	case nil:
		return nil, w.add(4)
	case bool:
		return x, w.add(5)
	case string:
		if !utf8.ValidString(x) {
			return nil, ErrInvalidValue
		}
		if len(x) > (w.limits.MaxBytes-w.bytes-2)/6 {
			return nil, ErrInvalidValue
		}
		if err := w.add(len(x)*6 + 2); err != nil {
			return nil, err
		}
		return x, nil
	case json.Number:
		s := string(x)
		if !json.Valid([]byte(s)) || len(s) == 0 || (s[0] != '-' && (s[0] < '0' || s[0] > '9')) {
			return nil, ErrInvalidValue
		}
		return x, w.add(len(s))
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil, ErrInvalidValue
		}
		return x, w.add(32)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, ErrInvalidValue
		}
		return x, w.add(32)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return x, w.add(32)
	case map[string]any:
		if err := w.item(len(x)); err != nil {
			return nil, err
		}
		if err := w.add(2); err != nil {
			return nil, err
		}
		key := jsonVisit{1, reflect.ValueOf(x).Pointer()}
		if w.active[key] {
			return nil, ErrInvalidValue
		}
		w.active[key] = true
		defer delete(w.active, key)
		out := make(map[string]any, len(x))
		for k, val := range x {
			if !utf8.ValidString(k) || len(k) > w.limits.MaxNameBytes {
				return nil, ErrInvalidValue
			}
			if len(k) > (w.limits.MaxBytes-w.bytes-3)/6 {
				return nil, ErrInvalidValue
			}
			if err := w.add(len(k)*6 + 3); err != nil {
				return nil, err
			}
			child, err := w.walk(val, depth+1)
			if err != nil {
				return nil, err
			}
			out[k] = child
		}
		return out, nil
	case []any:
		if err := w.item(len(x)); err != nil {
			return nil, err
		}
		if err := w.add(2 + len(x)); err != nil {
			return nil, err
		}
		key := jsonVisit{2, reflect.ValueOf(x).Pointer()}
		if w.active[key] {
			return nil, ErrInvalidValue
		}
		w.active[key] = true
		defer delete(w.active, key)
		out := make([]any, len(x))
		for i, val := range x {
			child, err := w.walk(val, depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = child
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: unsupported JSON value %T", ErrInvalidValue, v)
	}
}
func cloneJSONTrustedMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		switch x := v.(type) {
		case map[string]any:
			out[k] = cloneJSONTrustedMap(x)
		case []any:
			out[k] = cloneJSONTrustedSlice(x)
		default:
			out[k] = v
		}
	}
	return out
}
func cloneJSONTrustedSlice(s []any) []any {
	out := make([]any, len(s))
	for i, v := range s {
		switch x := v.(type) {
		case map[string]any:
			out[i] = cloneJSONTrustedMap(x)
		case []any:
			out[i] = cloneJSONTrustedSlice(x)
		default:
			out[i] = v
		}
	}
	return out
}

// JSONSize reports the validated tree's conservative size for queue accounting.
func JSONSize(m map[string]any, limits JSONLimits) (int, error) {
	_, n, e := OwnJSON(m, limits)
	return n, e
}
