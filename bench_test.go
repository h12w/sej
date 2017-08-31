package sej

import (
	"bytes"
	"strconv"
	"testing"
)

func BenchmarkAppend(b *testing.B) {
	path := newTestPath(b)
	w := newTestWriter(b, path, 1024*1024*1024)

	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte("key-" + strconv.Itoa(i))
	}
	value := bytes.Repeat([]byte{'a'}, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(Message{Key: keys[i], Value: value}); err != nil {
			b.Fatal(err)
		}
	}
	w.Flush()
	b.StopTimer()
	closeTestWriter(b, w)
}

func BenchmarkWriterStartup(b *testing.B) {
	path := newTestPath(b)
	w := newTestWriter(b, path, 1024*1024*1024)
	for i := 0; i < 1024*1024; i++ {
		w.Append(Message{Value: []byte("a")})
	}
	w.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := newTestWriter(b, path, 1024*1024*1024)
		w.Close()
	}
}
