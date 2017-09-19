package shard

import "testing"

func TestShardDir(t *testing.T) {
	root := "r"
	for _, testcase := range []struct {
		shardBit uint8
		index    int
		dir      string
	}{
		{
			shardBit: 0,
			index:    0,
			dir:      "r/blue",
		},
		{
			shardBit: 1,
			index:    0x0a,
			dir:      "r/blue.1.00a",
		},
		{
			shardBit: 10,
			index:    0x1ff,
			dir:      "r/blue.a.1ff",
		},
	} {
		if dir := (Shard{Prefix: "blue", Bit: testcase.shardBit, Index: testcase.index}.Dir(root)); dir != testcase.dir {
			t.Fatalf("expect %s got %s", testcase.dir, dir)

		}
	}
}
