package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"testing"
)

// Literal MessagePack codes, independently constructed from the protocol's type table.
func TestStrictHandwrittenFrames(t *testing.T) {
	good := []struct {
		hex   string
		id    int32
		ts    int64
		kind  int
		value any
	}{
		{"94000000c3", 0, 0, 0, true},
		{"94ff000201", -1, 0, 2, int64(1)},
		{"94ce7fffffff0003ca3fc00000", math.MaxInt32, 0, 3, float32(1.5)},
		{"94010004a178", 1, 0, 4, "x"},
		{"94010005c401ff", 1, 0, 5, []byte{255}},
		{"9401001491a178", 1, 0, 20, []string{"x"}},
	}
	var stream []byte
	for _, tc := range good {
		b, _ := hex.DecodeString(tc.hex)
		stream = append(stream, b...)
	}
	i := 0
	err := WalkFrames(stream, func(f Frame) error {
		tc := good[i]
		i++
		if f.TopicID != tc.id || f.Timestamp != tc.ts || f.TypeID != tc.kind {
			t.Fatalf("frame %d: %+v", i, f)
		}
		if tc.kind == 5 && !bytes.Equal(f.Value.([]byte), tc.value.([]byte)) {
			t.Fatal("binary mismatch")
		}
		return nil
	})
	if err != nil || i != len(good) {
		t.Fatalf("walk %d: %v", i, err)
	}
	stop := errors.New("stop")
	i = 0
	if err := WalkFrames(stream, func(Frame) error { i++; return stop }); !errors.Is(err, stop) || i != 1 {
		t.Fatalf("visitor stop: %v %d", err, i)
	}
}

func TestStrictMaliciousSuffix(t *testing.T) {
	bad := []string{
		"94010001ca3f800000",           // float32 where double required
		"94010003cb3ff0000000000000",   // float64 where float32 required
		"94010004c40178",               // bin where string required
		"94010005a178",                 // string where bin required
		"94010002cf8000000000000000",   // uint64 overflow
		"94cf80000000000000000000c3",   // ID overflow
		"9401cf800000000000000000c3",   // timestamp overflow
		"94fe0000c3",                   // ID below -1
		"9401ff00c3",                   // negative timestamp
		"94010004d9ff",                 // truncated string header/payload
		"94010004dbffffffff",           // malicious string length
		"94010005c6ffffffff",           // malicious bin length
		"94010010ddffffffff",           // malicious array length
		"94010004a1ff",                 // invalid UTF8
		"9401001491a1ff",               // invalid UTF8 array
		"9401001191ca3f800000",         // float array wrong family
		"9401001291cf8000000000000000", // int array overflow
		"94010010c0",                   // nil array
		"94010005c0",                   // nil binary
		"9401007fc3",                   // unknown numeric type
		"95",                           // malformed array length
		"c1",                           // invalid marker
	}
	prefix, _ := hex.DecodeString("94000000c3")
	for _, h := range bad {
		t.Run(h, func(t *testing.T) {
			suffix, e := hex.DecodeString(h)
			if e != nil {
				t.Fatal(e)
			}
			count := 0
			err := WalkFrames(append(bytes.Clone(prefix), suffix...), func(f Frame) error {
				count++
				if f.TopicID != 0 {
					t.Fatal(f)
				}
				return nil
			})
			if err == nil || count != 1 {
				t.Fatalf("error %v, visited %d", err, count)
			}
			frames, err := DecodeFrames(append(bytes.Clone(prefix), suffix...))
			if err == nil || len(frames) != 1 {
				t.Fatalf("ABI partial %v %v", frames, err)
			}
		})
	}
}

func TestStrictEncodeValidation(t *testing.T) {
	for _, v := range []any{"\xff", string(bytes.Repeat([]byte{'x'}, maxWireBytes+1))} {
		if _, err := EncodeValue(0, 0, DataTypeString, v); err == nil {
			t.Fatal("accepted bad string")
		}
	}
	if _, err := EncodeValue(-2, 0, DataTypeBoolean, true); err == nil {
		t.Fatal("accepted invalid ID")
	}
	if _, err := EncodeValue(0, -1, DataTypeBoolean, true); err == nil {
		t.Fatal("accepted invalid timestamp")
	}
	if _, err := EncodeValue(0, 0, DataTypeStringArray, []string{"\xff"}); err == nil {
		t.Fatal("accepted invalid array text")
	}
	if _, err := EncodeValue(0, 0, DataTypeBinary, make([]byte, maxWireBytes+1)); err == nil {
		t.Fatal("accepted huge binary")
	}
	if _, err := EncodeValue(0, 0, DataTypeInt, uint64(math.MaxInt64)+1); err == nil {
		t.Fatal("accepted overflow")
	}
}

func BenchmarkWalkFrames(b *testing.B) {
	data, _ := hex.DecodeString("94016401cb3ff8000000000000")
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		if err := WalkFrames(data, func(Frame) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}
