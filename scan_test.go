package sej

import (
	"testing"
	"time"
)

func TestScanThroughSegmentBoundary(t *testing.T) {
	tt := Test{t}
	messages := []string{"a", "b", "c"}

	path := newTestPath(t)
	w := newTestWriter(t, path, metaSize+1)
	writeTestMessages(t, w, messages...)
	closeTestWriter(t, w)

	r, err := NewScanner(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	tt.VerifyMessageValues(path, messages...)
}

func TestScanFromOffset(t *testing.T) {
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
					r, err := NewScanner(path, uint64(i))
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					r.Scan()
					if r.Err() != nil {
						t.Fatal(r.Err())
					}
					actualMsg := string(r.Message().Value)
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

func TestScanBeforeWrite(t *testing.T) {
	messages := []string{"a", "b", "c", "d", "e"}
	for _, segmentSize := range []int{metaSize + 1, (metaSize + 1) * 2, 1000} {
		func() {
			path := newTestPath(t)
			r, err := NewScanner(path, 0)
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
				if !r.Scan() {
					t.Fatal("Scan should return true")
				}
				if r.Err() != nil {
					t.Fatal(r.Err())
				}
				actualMsg, expectedMsg := string(r.Message().Value), messages[i]
				if actualMsg != expectedMsg {
					t.Fatalf("expect msg %s, got %s", expectedMsg, actualMsg)
				}
			}
		}()
	}
}

func TestScanTimeoutAndAgain(t *testing.T) {
	path := newTestPath(t)
	w := newTestWriter(t, path, 1000)

	s, err := NewScanner(path, 0)
	s.Timeout = time.Nanosecond
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if s.Scan() == true {
		t.Fatal("Scan should return false")
	}
	if s.Err() != ErrTimeout {
		t.Fatal("expect timeout error")
	}

	writeTestMessages(t, w, "a")
	flushTestWriter(t, w)

	if s.Scan() == false {
		t.Fatal("Scan should return true")
	}
	if s.Err() != nil {
		t.Fatal(s.Err())
	}

	actualMsg, expectedMsg := string(s.Message().Value), "a"
	if actualMsg != expectedMsg {
		t.Fatalf("expect msg %s, got %s", expectedMsg, actualMsg)
	}
}
