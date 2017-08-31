package shard

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"strconv"
	"testing"

	"h12.me/sej"
)

func BenchmarkAppend(b *testing.B) {
	path := newTestPath(b)
	w, err := NewWriter(path, 8, shardFNV)
	if err != nil {
		b.Fatal(err)
	}
	fmt.Println(w.ShardCount())
	defer w.Close()

	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte("key-" + strconv.Itoa(i))
	}
	value := bytes.Repeat([]byte{'a'}, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(&sej.Message{Key: keys[i], Value: value}); err != nil {
			b.Fatal(err)
		}
	}
	w.Flush()
	b.StopTimer()
}

func shardCRC(msg *sej.Message) uint32 {
	return crc32.ChecksumIEEE(msg.Key)
}

func shardFNV(msg *sej.Message) uint32 {
	h := fnv.New32a()
	h.Write(msg.Key)
	return h.Sum32()
}
