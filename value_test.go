package nt4

import (
	"errors"
	"math"
	"testing"
)

func TestTypeTableAndOwnedValue(t *testing.T) {
	cases := []struct {
		name  string
		value any
	}{{"boolean", true}, {"double", math.Inf(1)}, {"int", int64(9)}, {"float", float32(1)}, {"string", "hi"}, {"json", "{}"}, {"raw", []byte{1}}, {"future", []byte{1}}, {"boolean[]", []bool{}}, {"double[]", []float64{1}}, {"int[]", []int64{1}}, {"float[]", []float32{1}}, {"string[]", []string{"x"}}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, n, e := OwnValue(c.name, c.value, 1024); e != nil || n <= 0 {
				t.Fatalf("%d %v", n, e)
			}
		})
	}
	empty, _, err := OwnValue("string[]", []string{}, 100)
	if err != nil || empty.([]string) == nil {
		t.Fatalf("empty array: %v", err)
	}
	b := []byte{1}
	owned, _, e := OwnValue("raw", b, 100)
	if e != nil {
		t.Fatal(e)
	}
	b[0] = 2
	if owned.([]byte)[0] != 1 {
		t.Fatal("input alias")
	}
	sample := Sample{Value: owned}
	copy := sample.Clone()
	copy.Value.([]byte)[0] = 3
	if sample.Value.([]byte)[0] != 1 {
		t.Fatal("output alias")
	}
	for _, tc := range []struct {
		kind  string
		value any
	}{{"double", "wrong"}, {"int", uint64(math.MaxUint64)}, {"string", string([]byte{0xff})}, {"string[]", []string{string([]byte{0xff})}}, {"raw", []byte(nil)}, {"int[]", []int64(nil)}} {
		if _, _, e := OwnValue(tc.kind, tc.value, 100); !errors.Is(e, ErrInvalidValue) {
			t.Errorf("%s %T: %v", tc.kind, tc.value, e)
		}
	}
	if _, _, e := OwnValue("raw", make([]byte, 1000), 10); !errors.Is(e, ErrInvalidValue) {
		t.Fatal(e)
	}
}
func TestJSONOwnershipAndRejection(t *testing.T) {
	limits := JSONLimits{MaxBytes: 1000, MaxDepth: 5, MaxContainerItems: 10, MaxNameBytes: 100}
	leaf := map[string]any{"extension": []any{map[string]any{"x": true}, nil}}
	out, n, e := OwnJSON(leaf, limits)
	if e != nil || n == 0 {
		t.Fatal(e)
	}
	leaf["extension"].([]any)[0].(map[string]any)["x"] = false
	if out["extension"].([]any)[0].(map[string]any)["x"] != true {
		t.Fatal("input alias")
	}
	snapshot := TopicSnapshot{Properties: out}
	copied := snapshot.Clone()
	copied.Properties["extension"].([]any)[0].(map[string]any)["x"] = false
	if out["extension"].([]any)[0].(map[string]any)["x"] != true {
		t.Fatal("snapshot alias")
	}
	cycle := map[string]any{}
	cycle["x"] = cycle
	for _, bad := range []map[string]any{cycle, {"bad": math.NaN()}, {"bad": make(chan int)}, {"bad": string([]byte{0xff})}, {"bad": []any{[]any{[]any{[]any{[]any{true}}}}}}} {
		if _, _, e := OwnJSON(bad, limits); !errors.Is(e, ErrInvalidValue) {
			t.Fatalf("expected invalid JSON: %v", e)
		}
	}
	if _, _, e := OwnJSON(map[string]any{"a": string(make([]byte, 1000))}, limits); !errors.Is(e, ErrInvalidValue) {
		t.Fatal(e)
	}
}
