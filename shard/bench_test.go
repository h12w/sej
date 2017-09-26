package shard

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"h12.me/sej"
)

func BenchmarkAppend(b *testing.B) {
	path := newTestPath(b)
	w, err := NewWriter(Path{path, "blue", 8}, shardFNV)
	if err != nil {
		b.Fatal(err)
	}

	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte("key-" + fmt.Sprintf("%09x", i))
	}
	value := bytes.Repeat([]byte{'a'}, 100)
	now := time.Now()
	msg := sej.Message{Value: value, Timestamp: now}
	// timeAppend(b, w, keys, &msg)
	timeAppendParallel(b, w, keys, &msg)
	w.Close()
}

func timeAppend(b *testing.B, w *Writer, keys [][]byte, msg *sej.Message) {
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

func timeAppendParallel(b *testing.B, w *Writer, keys [][]byte, msg *sej.Message) {
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

func shardCRC(msg *sej.Message) uint16 {
	const mask16 = 1<<16 - 1
	s := crc32.ChecksumIEEE(msg.Key)
	return uint16((s >> 16) ^ (s & mask16))
}

func shardFNV(msg *sej.Message) uint16 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
		mask16   = 1<<16 - 1
	)
	var s uint32 = offset32
	for _, c := range msg.Key {
		s ^= uint32(c)
		s *= prime32
	}
	return uint16((s >> 16) ^ (s & mask16))
}
