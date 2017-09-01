package shard

import "testing"

func TestShardDir(t *testing.T) {
	root := "r"
	for _, testcase := range []struct {
		shardBit uint
		index    int
		dir      string
	}{
		{
			shardBit: 1,
			index:    0x0a,
			dir:      "r/shd/1/00a",
		},
		{
			shardBit: 10,
			index:    0x1ff,
			dir:      "r/shd/a/1ff",
		},
	} {
		if dir := shardDir(root, testcase.shardBit, testcase.index); dir != testcase.dir {
			t.Fatalf("expect %s got %s", testcase.dir, dir)

		}
	}
}
