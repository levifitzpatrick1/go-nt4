package wire

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

type fixtures struct {
	Frames []struct{ Name, Hex string }  `json:"frames"`
	Text   []struct{ Name, JSON string } `json:"text"`
}

func loadFixtures(t *testing.T) fixtures {
	t.Helper()
	data, err := os.ReadFile("../../testdata/nt41-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var f fixtures
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestIndependentFrameFixtureDecoding(t *testing.T) {
	for _, f := range loadFixtures(t).Frames {
		t.Run(f.Name, func(t *testing.T) {
			data, err := hex.DecodeString(f.Hex)
			if err != nil {
				t.Fatal(err)
			}
			frames, err := DecodeFrames(data)
			invalid := len(f.Name) > 7 && f.Name[:7] == "invalid"
			if invalid {
				if err == nil && len(frames) != 0 {
					t.Fatalf("accepted invalid NT tuple: %v", frames)
				}
				return
			}
			if err != nil || len(frames) != 1 || len(frames[0]) != 4 {
				t.Fatalf("decode %x: %v, %v", data, frames, err)
			}
		})
	}
}

func TestIndependentConcatenatedFrames(t *testing.T) {
	fs := loadFixtures(t).Frames
	var buf []byte
	for _, f := range fs[:11] {
		b, _ := hex.DecodeString(f.Hex)
		buf = append(buf, b...)
	}
	frames, err := DecodeFrames(buf)
	if err != nil || len(frames) != 11 {
		t.Fatalf("concatenated decode: %d frames, %v", len(frames), err)
	}
}

func TestIndependentArrayEncoding(t *testing.T) {
	fs := loadFixtures(t).Frames
	tests := []struct {
		i, kind int
		value   any
	}{
		{6, 16, []bool{true, false}}, {7, 17, []float64{1.5}},
		{8, 18, []int64{42}}, {9, 19, []float32{1.5}}, {10, 20, []string{"x"}},
	}
	for _, tc := range tests {
		t.Run(fs[tc.i].Name, func(t *testing.T) {
			want, _ := hex.DecodeString(fs[tc.i].Hex)
			got, err := EncodeValue(1, 0, tc.kind, tc.value)
			if err != nil {
				t.Fatal(err)
			}
			var actual, expected []any
			if err := msgpack.Unmarshal(got, &actual); err != nil {
				t.Fatal(err)
			}
			if err := msgpack.Unmarshal(want, &expected); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(actual) != fmt.Sprint(expected) {
				t.Fatalf("got %x (%v), want %x (%v)", got, actual, want, expected)
			}
			if tc.kind == 19 && !bytes.Contains(got, []byte{0xca, 0x3f, 0xc0, 0, 0}) {
				t.Fatalf("float32 array widened: %x", got)
			}
		})
	}
}

func TestInvalidNilArrayIsRejected(t *testing.T) {
	// MessagePack nil is not a valid NT array or binary value. An empty array
	// (or bin) is fine; a nil Go slice must normalize or be rejected locally.
	for _, tc := range []struct {
		kind  int
		value any
	}{
		{5, []byte(nil)}, {16, []bool(nil)}, {17, []float64(nil)},
		{18, []int64(nil)}, {19, []float32(nil)}, {20, []string(nil)},
	} {
		t.Run(fmt.Sprint(tc.kind), func(t *testing.T) {
			encoded, err := EncodeValue(1, 0, tc.kind, tc.value)
			if err != nil {
				return
			}
			var items []any
			if err := msgpack.Unmarshal(encoded, &items); err != nil {
				t.Fatal(err)
			}
			if len(items) != 4 || items[3] == nil {
				t.Fatalf("nil value encoded as MessagePack nil: %x", encoded)
			}
		})
	}
}

func TestIndependentScalarEncoding(t *testing.T) {
	// Fixtures are literal hand-encoded MessagePack, not encoder round-trips.
	fs := loadFixtures(t).Frames
	tests := []struct {
		i      int
		id     int32
		ts     int64
		typeID int
		value  any
	}{
		{0, 0, 0, 0, true}, {1, 3, 100, 1, 1.5}, {2, 1, 0, 2, int64(42)},
		{3, 1, 0, 3, float32(1.5)}, {4, 1, 0, 4, "x"}, {5, 1, 0, 5, []byte{0, 255}},
	}
	for _, tc := range tests {
		t.Run(fs[tc.i].Name, func(t *testing.T) {
			want, _ := hex.DecodeString(fs[tc.i].Hex)
			got, err := EncodeValue(tc.id, tc.ts, tc.typeID, tc.value)
			if err != nil {
				t.Fatal(err)
			}
			var actual, expected []any
			if err := msgpack.Unmarshal(got, &actual); err != nil {
				t.Fatal(err)
			}
			if err := msgpack.Unmarshal(want, &expected); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(actual) != fmt.Sprint(expected) {
				t.Fatalf("got %x (%v), want %x (%v)", got, actual, want, expected)
			}
			// float32 must not be widened to float64; bin must not become an array.
			if tc.typeID == 3 && !bytes.Contains(got, []byte{0xca, 0x3f, 0xc0, 0, 0}) {
				t.Fatalf("float32 wire family lost: %x", got)
			}
		})
	}
}
