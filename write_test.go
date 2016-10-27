package sej

import (
	"os"
	"reflect"
	"testing"
)

func TestWriteFlush(t *testing.T) {
	path := newTestPath(t)
	messages := []string{"a", "bc"}
	w := newTestWriter(t, path)
	defer closeTestWriter(t, w)

	writeTestMessages(t, w, messages...)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	verifyReadMessages(t, path, messages...)
}

func TestWriteSegment(t *testing.T) {
	for _, testcase := range []struct {
		messages  []string
		maxSize   int
		fileSizes []int
	}{
		{
			messages:  []string{"a", "ab"},
			maxSize:   0,
			fileSizes: []int{metaSize + 1, metaSize + 2, 0},
		},
		{
			messages:  []string{"a"},
			maxSize:   (metaSize + 1),
			fileSizes: []int{metaSize + 1, 0},
		},
		{
			messages:  []string{"a", "bc"},
			maxSize:   (metaSize + 1) + (metaSize + 2),
			fileSizes: []int{(metaSize + 1) + (metaSize + 2), 0},
		},
	} {
		func() {
			path := newTestPath(t)
			w := newTestWriter(t, path, testcase.maxSize)
			writeTestMessages(t, w, testcase.messages...)
			closeTestWriter(t, w)

			journalFiles, err := openJournalDir(path)
			if err != nil {
				t.Fatal(err)
			}
			sizes := journalFiles.sizes(t)
			if !reflect.DeepEqual(sizes, testcase.fileSizes) {
				t.Fatalf("expect journal files with size %v but got %d", testcase.fileSizes, sizes)
			}

			verifyReadMessages(t, path, testcase.messages...)
		}()
	}
}

func TestWriteReopen(t *testing.T) {
	messages := []string{"a", "bc", "def"}
	// test cases for multiple and single segments
	for _, segmentSize := range []int{0, 50} {
		func() {
			path := newTestPath(t)
			for _, msg := range messages {
				w := newTestWriter(t, path, segmentSize)
				writeTestMessages(t, w, msg)
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			}
			verifyReadMessages(t, path, messages...)
		}()
	}
}

func TestWriteDetectCorruption(t *testing.T) {
	path := newTestPath(t)
	w := newTestWriter(t, path)
	writeTestMessages(t, w, "a", "b", "c")
	closeTestWriter(t, w)

	// corrupt the last message
	{
		f, err := os.OpenFile(path+"/0000000000000000.jnl", os.O_RDWR, 0644)
		if err != nil {
			t.Fatal(err)
		}
		stat, err := f.Stat()
		if err != nil {
			f.Close()
			t.Fatal(err)
		}
		if err := f.Truncate(stat.Size() - 1); err != nil {
			f.Close()
			t.Fatal(err)
		}
		f.Close()
	}

	w, err := NewWriter(path, defaultSegmentSize)
	if err != ErrCorrupted {
		defer w.Close()
		t.Fatalf("expect corruption error but got %v", err)
	}
}
