package sej

import (
	"fmt"
	"path"
	"runtime"
	"sort"
	"testing"
	"time"
)

func TestWatch(t *testing.T) {
	dir := newTestPath(t)

	shardChan := make(chan string)
	go func() {
		if err := WatchRootDir(dir, time.Millisecond, func(dir string) {
			go func() {
				shardChan <- dir
			}()
		}); err != nil {
			t.Fatal(err)
		}
	}()
	runtime.Gosched()

	{
		w, err := NewWriter(dir)
		if err != nil {
			t.Fatal(err)
		}
		w.Append(&Message{Key: []byte("a")})
		w.Close()
	}

	{
		w, err := NewWriter(path.Join(dir, "d1"))
		if err != nil {
			t.Fatal(err)
		}
		w.Append(&Message{Key: []byte("a")})
		w.Close()
	}
	{
		w, err := NewWriter(path.Join(dir, "d2"))
		if err != nil {
			t.Fatal(err)
		}
		w.Append(&Message{Key: []byte("a")})
		w.Append(&Message{Key: []byte("b")})
		w.Close()
	}
	time.Sleep(time.Millisecond)

	var shards []string
	for i := 0; i < 10; i++ {
		select {
		case shard := <-shardChan:
			shards = append(shards, shard)
		default:
		}
	}
	sort.Strings(shards)
	expected := "[" +
		dir + ` ` +
		dir + `/d1 ` +
		dir + `/d2` +
		"]"
	actual := fmt.Sprint(shards)
	if expected != actual {
		t.Fatalf("expect\n%s\ngot\n%s", expected, actual)
	}
}
