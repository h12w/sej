package shard

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
)

 
func newTestPath(t testing.TB) string {
	path := testFilePrefix + strconv.Itoa(rand.Int())
	if err := os.Mkdir(path, 0755); err != nil {
		t.Fatal(err)
	}
	return path
}
