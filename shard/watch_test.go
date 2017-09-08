package shard

import (
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"
)

func TestWatch(t *testing.T) {
	dir := newTestPath(t)

	WatchInterval = time.Millisecond
	shardChan := make(chan Shard)
	go func() {
		if err := Watch(dir, func(shard *Shard) {
			go func() {
				shardChan <- *shard
			}()
		}); err != nil {
			t.Fatal(err)
		}
	}()
	runtime.Gosched()

	{
		w, err := NewWriter(dir, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		w.Close()
	}
	{
		w, err := NewWriter(dir, 1, shardFNV)
		if err != nil {
			t.Fatal(err)
		}
		w.Close()
	}
	time.Sleep(time.Millisecond)

	var shards []Shard
	for i := 0; i < 10; i++ {
		select {
		case shard := <-shardChan:
			shards = append(shards, shard)
		default:
		}
	}
	sort.Sort(ByDir(shards))
	expected := "[" +
		`{` + dir + ` 0 0} ` +
		`{` + dir + ` 1 0} ` +
		`{` + dir + ` 1 1}` +
		"]"
	actual := fmt.Sprint(shards)
	if expected != actual {
		t.Fatalf("expect\n%s\ngot\n%s", expected, actual)
	}
}

type ByDir []Shard

func (a ByDir) Len() int           { return len(a) }
func (a ByDir) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDir) Less(i, j int) bool { return a[i].Dir() < a[j].Dir() }
