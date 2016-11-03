package sej

import (
	"os"
	"testing"
)

func TestReadTruncated(t *testing.T) {
	for cut := 20; cut >= 1; cut-- {
		path := newTestPath(t)
		w := newTestWriter(t, path)
		writeTestMessages(t, w, "a")
		closeTestWriter(t, w)

		file := path + "/0000000000000000.jnl"
		truncateFile(t, file, cut)

		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := ReadMessage(f); err == nil {
			t.Fatal("expect error but got nil")
		}

		if offset := fileOffset(t, f); offset != 0 {
			t.Fatalf("cut=%d: expect offset 0 after failed reading, but got %d", cut, offset)
		}
	}
}
