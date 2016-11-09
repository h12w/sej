package sej

import "testing"

func TestReadThroughSegmentBoundary(t *testing.T) {
	messages := []string{"a", "b", "c"}

	path := newTestPath(t)
	w := newTestWriter(t, path, metaSize+1)
	writeTestMessages(t, w, messages...)
	closeTestWriter(t, w)

	r, err := NewReader(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	verifyReadMessages(t, path, messages...)
}

func TestReadFromOffset(t *testing.T) {
	messages := []string{"a", "b", "c", "d", "e"}
	for _, segmentSize := range []int{metaSize + 1, (metaSize + 1) * 2, 1000} {
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
					actualMsg := string(msg.Value)
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

func TestReadBeforeWrite(t *testing.T) {
	messages := []string{"a", "b", "c", "d", "e"}
	for _, segmentSize := range []int{metaSize + 1, (metaSize + 1) * 2, 1000} {
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
				actualMsg, expectedMsg := string(msg.Value), messages[i]
				if actualMsg != expectedMsg {
					t.Fatalf("expect msg %s, got %s", expectedMsg, actualMsg)
				}
			}
		}()
	}
}
