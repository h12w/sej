package sej

import (
	"reflect"
	"testing"
)

func TestWriteFlush(t *testing.T) {
	tt := Test{t}
	path := newTestPath(t)
	messages := []string{"a", "bc"}
	w := newTestWriter(t, path)
	defer closeTestWriter(t, w)

	writeTestMessages(t, w, messages...)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	tt.VerifyMessageValues(path, messages...)
}

func TestWriteSegment(t *testing.T) {
	tt := Test{t}
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

			journalFiles, err := OpenJournalDir(JournalDirPath(path))
			if err != nil {
				t.Fatal(err)
			}
			sizes := journalFiles.sizes(t)
			if !reflect.DeepEqual(sizes, testcase.fileSizes) {
				t.Fatalf("expect journal files with size %v but got %d", testcase.fileSizes, sizes)
			}

			tt.VerifyMessageValues(path, testcase.messages...)
		}()
	}
}

func TestWriteReopen(t *testing.T) {
	tt := Test{t}
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
			tt.VerifyMessageValues(path, messages...)
		}()
	}
}

func TestWriteDetectCorruption(t *testing.T) {
	path := newTestPath(t)
	w := newTestWriter(t, path)
	writeTestMessages(t, w, "a", "b", "c")
	closeTestWriter(t, w)

	file := JournalDirPath(path) + "/0000000000000000.jnl"
	// corrupt the last message
	truncateFile(t, file, 1)

	// 1st time
	w, err := NewWriter(path)
	if err == nil {
		w.Close()
	}
	if _, ok := err.(*CorruptionError); !ok {
		t.Fatalf("expect corruption error but got %v", err)
	}

	// 2nd time
	w, err = NewWriter(path)
	if err == nil {
		w.Close()
	}
	if err != nil {
		t.Fatalf("expect corruption fixed but got: %v", err)
	}

}
