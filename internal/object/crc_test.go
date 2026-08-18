package object

import (
	"bytes"
	"hash/crc32"
	"testing"
)

func TestCombineCRC32CMatchesASingleHash(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)
	cases := []struct {
		a, b []byte
	}{
		{[]byte("hello"), []byte(" world")},
		{[]byte{}, []byte("x")},
		{[]byte("x"), []byte{}},
		{bytes.Repeat([]byte("a"), 1), bytes.Repeat([]byte("b"), 7)},
		{bytes.Repeat([]byte("a"), 1000), []byte("z")},
		{bytes.Repeat([]byte{0}, 65537), []byte("tail")},
		{randBytes(17), randBytes(4099)},
	}
	for _, tt := range cases {
		a := crc32.Checksum(tt.a, table)
		b := crc32.Checksum(tt.b, table)
		got := CombineCRC32C(a, b, int64(len(tt.b)))
		want := crc32.Checksum(append(append([]byte{}, tt.a...), tt.b...), table)
		if got != want {
			t.Errorf("CombineCRC32C(%d+%d bytes) = %08x, want %08x",
				len(tt.a), len(tt.b), got, want)
		}
	}

	// Three parts, combined left to right, the way CompleteUpload walks them.
	p1, p2, p3 := randBytes(100), randBytes(1), randBytes(999)
	c1 := crc32.Checksum(p1, table)
	c2 := crc32.Checksum(p2, table)
	c3 := crc32.Checksum(p3, table)
	got := CombineCRC32C(CombineCRC32C(c1, c2, int64(len(p2))), c3, int64(len(p3)))
	want := crc32.Checksum(append(append(append([]byte{}, p1...), p2...), p3...), table)
	if got != want {
		t.Errorf("three-way combine = %08x, want %08x", got, want)
	}
}
