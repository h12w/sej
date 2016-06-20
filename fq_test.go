package fq

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

func TestAppendRead(t *testing.T) {
	path := newTestPath(t)
	defer os.RemoveAll(path)
	var messages []string
	for i := 0; i < 1000; i++ {
		messages = append(messages, strconv.Itoa(i))
	}

	{
		w, err := NewWriter(path)
		if err != nil {
			t.Fatal(err)
			return
		}
		for i := 0; i < 500; i++ {
			msg := messages[i]
			offset, err := w.Append([]byte(msg))
			if err != nil {
				t.Fatal(err)
				return
			}
			if offset != uint64(i+1) {
				t.Fatalf("expect offset %d, got %d", i+1, offset)
				return
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
			return
		}
	}

	{
		w, err := NewWriter(path)
		if err != nil {
			t.Fatal(err)
			return
		}
		for i := 500; i < 1000; i++ {
			msg := messages[i]
			offset, err := w.Append([]byte(msg))
			if err != nil {
				t.Fatal(err)
				return
			}
			if offset != uint64(i+1) {
				t.Fatalf("expect offset %d, got %d", i+1, offset)
				return
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
			return
		}
	}

	for readFrom := 0; readFrom < 5; readFrom++ {
		r, err := NewReader(path, uint64(readFrom))
		if err != nil {
			t.Fatal(err)
			return
		}
		defer r.Close()
		for i := readFrom; i < len(messages); i++ {
			msg, offset, err := r.Read()
			if err != nil {
				t.Fatal(err)
				return
			}
			if int(offset) != i+1 {
				t.Fatalf("expect offset %d, got %d", i+1, offset)
				return
			}
			if string(msg) != messages[i] {
				t.Fatalf("expect %s, got %s", messages[i], string(msg))
				return
			}
		}
	}
}

func newTestPath(t *testing.T) string {
	path, err := ioutil.TempDir(".", "test-")
	if err != nil {
		t.Fatal(err)
	}
	return path
}
