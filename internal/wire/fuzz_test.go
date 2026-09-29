package wire

import (
	"encoding/hex"
	"math"
	"testing"
)

func FuzzDecodeFrames(f *testing.F) {
	for _, h := range []string{
		"94000000c3",
		"94016401cb3ff8000000000000",
		"94010005c40200ff",
		"95000000c3c2",
		"94ffffffff000200",
		"c1",
		"94010005c6ffffffff",
		"94010010ddffffffff",
		"94010002cf8000000000000000",
		"94010004a1ff",
		"94010003cb3ff0000000000000",
	} {
		b, _ := hex.DecodeString(h)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeFrames(data)
		_ = WalkFrames(data, func(Frame) error { return nil })
	})
}

func FuzzEncodeValue(f *testing.F) {
	f.Add(1, int64(0), DataTypeBoolean, "true")
	f.Add(1, int64(0), DataTypeDouble, "1.5")
	f.Add(1, int64(0), DataTypeInt, "42")
	f.Add(1, int64(0), DataTypeString, "x")
	f.Fuzz(func(t *testing.T, id int, ts int64, typeID int, s string) {
		if id < math.MinInt32 || id > math.MaxInt32 {
			return
		}
		values := []any{true, float64(1.5), int64(42), float32(1.5), s, []byte(s), []bool{true}, []float64{1.5}, []int64{42}, []float32{1.5}, []string{s}, []byte(nil)}
		for _, v := range values {
			_, _ = EncodeValue(int32(id), ts, typeID, v)
		}
	})
}
