package shard

import "testing"

func TestShardDir(t *testing.T) {
	root := "r"
	for _, testcase := range []struct {
		mask  uint16
		index int
		dir   string
	}{
		{
			mask:  0xff,
			index: 0x0a,
			dir:   "r/shd/0ff/00a",
		},
		{
			mask:  0xfff,
			index: 0x1ff,
			dir:   "r/shd/fff/1ff",
		},
	} {
		if dir := shardDir(root, testcase.mask, testcase.index); dir != testcase.dir {
			t.Fatalf("expect %s got %s", testcase.dir, dir)

		}
	}
}
