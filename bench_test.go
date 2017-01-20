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
		if err := w.Append(Message{Value: msg}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriterStartup(b *testing.B) {
	path := newTestPath(b)
	w := newTestWriter(b, path, 1024*1024*1024)
	for i := 0; i < 48*1024*1024; i++ {
		w.Append(Message{Value: []byte("a")})
	}
	w.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := newTestWriter(b, path, 1024*1024*1024)
		w.Close()
	}
}
