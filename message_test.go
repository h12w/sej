package sej

import (
	"bytes"
	"io"
	"os"
	"reflect"
	"testing"
	"time"
)

const (
	metaSize = 26 // message size excluding the value (assuming key is zero size)
)

func TestMarshalUnmarshal(t *testing.T) {
	msg := Message{
		Offset:    42,
		Timestamp: time.Now(),
		Type:      43,
		Key:       []byte("a"),
		Value:     []byte("b"),
	}
	var buf bytes.Buffer
	n1, err := msg.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var result Message
	n2, err := result.ReadFrom(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if n1 != n2 {
		t.Fatal("size mismatch")
	}
	if !reflect.DeepEqual(result, msg) {
		t.Fatalf("expect %v got %v", msg, result)
	}
}

func TestReadTruncatedMessage(t *testing.T) {
	for cut := 20; cut >= 1; cut-- {
		path := newTestPath(t)
		w := newTestWriter(t, path)
		writeTestMessages(t, w, "a")
		closeTestWriter(t, w)

		file := JournalDirPath(path) + "/0000000000000000.jnl"
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

		// test bytes read n
		fileOffset, err := f.Seek(-n, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if fileOffset != 0 {
			t.Fatalf("cut=%d: expect offset 0 after failed reading, but got %d", cut, fileOffset)
		}
	}
}
