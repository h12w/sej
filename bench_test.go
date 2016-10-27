package sej

import (
	"strings"
	"testing"
)

func BenchmarkWrite(b *testing.B) {
	path := newTestPath(b)
	w := newTestWriter(b, path, 500000000)
	defer w.Close()
	msg := []byte(strings.Repeat("x", 128))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(msg); err != nil {
			b.Fatal(err)
		}
	}
}
