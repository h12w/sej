package sej

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestOpenOrCreateDir(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dir := strconv.Itoa(rand.Int())
	defer func() {
		os.RemoveAll(dir)
	}()
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
