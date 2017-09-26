package shard

import (
	"reflect"
	"testing"
)

func TestShardDir(t *testing.T) {
	for _, testcase := range []struct {
		shard shard
		dir   string
	}{
		{
			shard: shard{
				Path: Path{
					Root:     "",
					Prefix:   "",
					ShardBit: 0,
				},
				Index: 0,
			},
			dir: "",
		},
		{
			shard: shard{
				Path: Path{
					Root:     "/r",
					Prefix:   "",
					ShardBit: 0,
				},
				Index: 0,
			},
			dir: "/r",
		},
		{
			shard: shard{
				Path: Path{
					Root:     "/r",
					Prefix:   "p",
					ShardBit: 0,
				},
				Index: 0,
			},
			dir: "/r/p",
		},
		{
			shard: shard{
				Path: Path{
					Root:     "/r",
					Prefix:   "p",
					ShardBit: 1,
				},
				Index: 0x0a,
			},
			dir: "/r/p.1.00a",
		},
		{
			shard: shard{
				Path: Path{
					Root:     "/r",
					Prefix:   "p",
					ShardBit: 10,
				},
				Index: 0x1ff,
			},
			dir: "/r/p.a.1ff",
		},
		{
			shard: shard{
				Path: Path{
					Root:     "/r",
					Prefix:   "",
					ShardBit: 1,
				},
				Index: 0x0a,
			},
			dir: "/r/1.00a",
		},
	} {
		if dir := (testcase.shard.Dir()); dir != testcase.dir {
			t.Fatalf("expect %s got %s", testcase.dir, dir)
		}
		shard, err := parseShardDir(testcase.shard.Path.Root, testcase.dir)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(shard, testcase.shard) {
			t.Fatalf("expect %v got %v", testcase.shard, shard)
		}
	}
}
