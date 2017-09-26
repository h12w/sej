package shard

import (
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"

	"h12.me/sej"
)

func TestWatch(t *testing.T) {
	dir := newTestPath(t)
	prefix := "blue"

	WatchInterval = time.Millisecond
	shardChan := make(chan string)
	go func() {
		if err := Watch(dir, func(dir string) {
			go func() {
				shardChan <- dir
			}()
		}); err != nil {
			t.Fatal(err)
		}
	}()
	runtime.Gosched()

	{
		w, err := NewWriter(Path{dir, prefix, 0}, nil)
		if err != nil {
			t.Fatal(err)
		}
		w.Append(&sej.Message{Key: []byte("a")})
		w.Close()
	}
	{
		w, err := NewWriter(Path{dir, prefix, 1}, shardFNV)
		if err != nil {
			t.Fatal(err)
		}
		w.Append(&sej.Message{Key: []byte("a")})
		w.Append(&sej.Message{Key: []byte("b")})
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
		dir + `/blue ` +
		dir + `/blue.1.000 ` +
		dir + `/blue.1.001` +
		"]"
	actual := fmt.Sprint(shards)
	if expected != actual {
		t.Fatalf("expect\n%s\ngot\n%s", expected, actual)
	}
}

type ByDir []shard

func (a ByDir) Len() int           { return len(a) }
func (a ByDir) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDir) Less(i, j int) bool { return a[i].Dir() < a[j].Dir() }
