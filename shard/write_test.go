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
			shardBit: 0,
			index:    0,
			dir:      "r",
		},
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
		if dir := (Shard{RootDir: root, Bit: testcase.shardBit, Index: testcase.index}.Dir()); dir != testcase.dir {
			t.Fatalf("expect %s got %s", testcase.dir, dir)

		}
	}
}
