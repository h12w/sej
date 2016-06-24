package fq

import (
	"os"
	"reflect"
	"testing"
)

func TestWriteFlush(t *testing.T) {
	path := newTestPath(t)
	defer os.RemoveAll(path)
	messages := []string{"a", "bc"}

	w := newTestWriter(t, path, 9999)
	writeTestMessages(t, w, messages...)
	if err := w.Flush(w.Offset()); err != nil {
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
			messages:  []string{"a"},
			maxSize:   (metaSize + 1) + 1,
			fileSizes: []int{metaSize + 1},
		},
		{
			messages:  []string{"a", "bc"},
			maxSize:   (metaSize + 1) + (metaSize + 2),
			fileSizes: []int{(metaSize + 1) + (metaSize + 2), 0},
		},
		{
			messages:  []string{"a", "bc"},
			maxSize:   (metaSize + 1) + (metaSize + 2) + 1,
			fileSizes: []int{(metaSize + 1) + (metaSize + 2)},
		},
	} {
		func() {
			path := newTestPath(t)
			defer os.RemoveAll(path)
			w := newTestWriter(t, path, testcase.maxSize)
			writeTestMessages(t, w, testcase.messages...)
			closeTestWriter(t, w)

			journalFiles, err := getJournalFiles(path)
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
	for _, segmentSize := range []int{0, 9999} {
		func() {
			path := newTestPath(t)
			defer os.RemoveAll(path)
			{
				// test reopening an empty file
				w := newTestWriter(t, path, segmentSize)
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			}
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
