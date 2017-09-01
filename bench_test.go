package sej

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func BenchmarkAppend(b *testing.B) {
	path := newTestPath(b)
	w, err := NewWriter(path)
	if err != nil {
		b.Fatal(err)
	}

	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte("key-" + fmt.Sprintf("%09x", i))
	}
	value := bytes.Repeat([]byte{'a'}, 100)
	now := time.Now()
	msg := Message{Value: value, Timestamp: now}
	timeAppend(b, w, keys, &msg)
	// timeAppendParallel(b, w, keys, &msg)
	w.Close()
}

func timeAppend(b *testing.B, w *Writer, keys [][]byte, msg *Message) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.Key = keys[i]
		if err := w.Append(msg); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func timeAppendParallel(b *testing.B, w *Writer, keys [][]byte, msg *Message) {
	b.SetParallelism(2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			msg.Key = keys[i]
			if err := w.Append(msg); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

/*
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
		if err := w.Append(&Message{Key: keys[i], Value: value}); err != nil {
			b.Fatal(err)
		}
	}
	w.Flush()
	b.StopTimer()
	closeTestWriter(b, w)
}
*/

func BenchmarkWriterStartup(b *testing.B) {
	path := newTestPath(b)
	w := newTestWriter(b, path, 1024*1024*1024)
	for i := 0; i < 1024*1024; i++ {
		w.Append(&Message{Value: []byte("a")})
	}
	w.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := newTestWriter(b, path, 1024*1024*1024)
		w.Close()
	}
}
