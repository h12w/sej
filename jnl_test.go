package sej

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestOpenOrCreateDir(t *testing.T) {
	dir := testDirPrefix + strconv.Itoa(rand.Int())
	d, err := openOrCreateDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := d.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !stat.IsDir() {
		t.Fatal("expect dir")
	}
}
