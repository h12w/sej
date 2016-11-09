package sej

import (
	"io"
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
		var msg Message
		n, err := msg.ReadFrom(f)
		if err == nil {
			t.Fatal("expect error but got nil")
		}

		fileOffset, err := f.Seek(-n, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}

		if offset := fileOffset; offset != 0 {
			t.Fatalf("cut=%d: expect offset 0 after failed reading, but got %d", cut, offset)
		}
	}
}
