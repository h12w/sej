package sej

import (
	"os"
	"testing"
)

func TestReadOffset(t *testing.T) {
	messages := []string{"a", "b", "c", "d", "e"}
	for _, segmentSize := range []int{metaSize + 1, (metaSize + 1) * 2, 9999} {
		func() {
			path := newTestPath(t)
			defer os.RemoveAll(path)
			w := newTestWriter(t, path, segmentSize)
			writeTestMessages(t, w, messages...)
			closeTestWriter(t, w)

			for i, expectedMsg := range messages {
				func() {
					r, err := NewReader(path, uint64(i))
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					msg, err := r.Read()
					if err != nil {
						t.Fatal(err)
					}
					actualMsg := string(msg)
					if actualMsg != expectedMsg {
						t.Fatalf("expect msg %s, got %s", expectedMsg, actualMsg)
					}
					nextOffset := uint64(i + 1)
					if r.Offset() != nextOffset {
						t.Fatalf("expect offset %s, got %s", nextOffset, r.Offset())
					}
				}()
			}
		}()
	}
}

func TestReadBeforeWrite(t *testing.T) {
	done := make(chan bool)
	path := newTestPath(t)
	defer func() {
		<-done
		os.RemoveAll(path)
	}()
	r, err := NewReader(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	go func() {
		w := newTestWriter(t, path, 9999)
		writeTestMessages(t, w, "a")
		closeTestWriter(t, w)
		done <- true
	}()
	msg, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	actualMsg, expectedMsg := string(msg), "a"
	if actualMsg != expectedMsg {
		t.Fatalf("expect msg %s, got %s", expectedMsg, actualMsg)
	}
}

func writeTestMessages(t *testing.T, w *Writer, messages ...string) {
	start := w.Offset()
	for i, msg := range messages {
		if err := w.Append([]byte(msg)); err != nil {
			t.Fatal(err)
		}
		offset := start + uint64(i) + 1
		if w.Offset() != offset {
			t.Fatalf("offset: expect %d but got %d", offset, w.Offset())
		}
	}
}

func newTestWriter(t *testing.T, path string, maxFileSize int) *Writer {
	w, err := NewWriter(path, maxFileSize)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func closeTestWriter(t *testing.T, w *Writer) {
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func flushTestWriter(t *testing.T, w *Writer) {
	if err := w.Flush(w.Offset()); err != nil {
		t.Fatal(err)
	}
}
