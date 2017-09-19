package shard

import (
	"reflect"
	"testing"
)

func TestShardDir(t *testing.T) {
	root := "/r"
	for _, testcase := range []struct {
		shard Shard
		dir   string
	}{
		{
			shard: Shard{
				Prefix: "",
				Bit:    0,
				Index:  0,
			},
			dir: "/r",
		},
		{
			shard: Shard{
				Prefix: "p",
				Bit:    0,
				Index:  0,
			},
			dir: "/r/p",
		},
		{
			shard: Shard{
				Prefix: "",
				Bit:    1,
				Index:  0x0a,
			},
			dir: "/r/1.00a",
		},
		{
			shard: Shard{
				Prefix: "p",
				Bit:    1,
				Index:  0x0a,
			},
			dir: "/r/p.1.00a",
		},
		{
			shard: Shard{
				Prefix: "p",
				Bit:    10,
				Index:  0x1ff,
			},
			dir: "/r/p.a.1ff",
		},
	} {
		if dir := (testcase.shard.Dir(root)); dir != testcase.dir {
			t.Fatalf("expect %s got %s", testcase.dir, dir)
		}
		shard, err := parseShardDir(root, testcase.dir)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(shard, testcase.shard) {
			t.Fatalf("expect %v got %v", testcase.shard, shard)
		}
	}
}
