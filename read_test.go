package sej

import "testing"

func TestReadThrough(t *testing.T) {
	messages := []string{"a", "b", "c", "d", "e", "f", "g"}

	path := newTestPath(t)
	w := newTestWriter(t, path, (metaSize+1)*2)
	writeTestMessages(t, w, messages...)
	closeTestWriter(t, w)

	r, err := NewReader(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i, expectedMsg := range messages {
		// create a new reader starting from i
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
			t.Fatalf("expect offset %d, got %d", nextOffset, r.Offset())
		}
	}
}

func TestReadFromOffset(t *testing.T) {
	messages := []string{"a", "b", "c", "d", "e"}
	for _, segmentSize := range []int{metaSize + 1, (metaSize + 1) * 2, 9999} {
		func() {
			path := newTestPath(t)
			w := newTestWriter(t, path, segmentSize)
			writeTestMessages(t, w, messages...)
			closeTestWriter(t, w)

			for i, expectedMsg := range messages {
				func() {
					// create a new reader starting from i
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
						t.Fatalf("expect offset %d, got %d", nextOffset, r.Offset())
					}
				}()
			}
		}()
	}
}

func TestReadFirst(t *testing.T) {
	messages := []string{"a", "b", "c", "d", "e"}
	for _, segmentSize := range []int{metaSize + 1, (metaSize + 1) * 2, 9999} {
		func() {
			path := newTestPath(t)
			r, err := NewReader(path, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()

			done := make(chan bool)
			defer func() { <-done }()
			go func() {
				w := newTestWriter(t, path, segmentSize)
				writeTestMessages(t, w, messages...)
				closeTestWriter(t, w)
				done <- true
			}()
			for i := range messages {
				msg, err := r.Read()
				if err != nil {
					t.Fatal(err)
				}
				actualMsg, expectedMsg := string(msg), messages[i]
				if actualMsg != expectedMsg {
					t.Fatalf("expect msg %s, got %s", expectedMsg, actualMsg)
				}
			}
		}()
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
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}
