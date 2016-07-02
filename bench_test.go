package sej

import (
	"io/ioutil"
	"strings"
	"testing"
)

func BenchmarkWrite(b *testing.B) {
	path, err := ioutil.TempDir(".", testPrefix)
	if err != nil {
		b.Fatal(err)
	}
	w, err := NewWriter(path, 500000000)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	msg := []byte(strings.Repeat("x", 128))
	// cnt := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(msg); err != nil {
			b.Fatal(err)
		}
		// cnt++
		// if cnt == 10 {
		// 	cnt = 0
		// 	if err := w.Flush(); err != nil {
		// 		b.Fatal(err)
		// 	}
		// }
	}
}
