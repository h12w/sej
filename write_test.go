package sej

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestWriteFlush(t *testing.T) {
	path := newTestPath(t)
	messages := []string{"a", "bc"}

	w := newTestWriter(t, path, 9999)
	defer closeTestWriter(t, w)
	writeTestMessages(t, w, messages...)
	if err := w.flush(); err != nil {
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
	for _, segmentSize := range []int{0, 9999} {
		func() {
			path := newTestPath(t)
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

func TestWriteDetectCorruption(t *testing.T) {
	path := newTestPath(t)
	w := newTestWriter(t, path, 9999)
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

	w, err := NewWriter(path, 9999)
	if err != ErrCorrupted {
		defer w.Close()
		t.Fatalf("expect corruption error but got %v", err)
	}
}

func readMessages(t *testing.T, path string, start uint64, n int) (messages []string) {
	r, err := NewReader(path, start)
	if err != nil {
		t.Fatal(err)
		return nil
	}
	defer r.Close()
	for i := 0; i < n; i++ {
		msg, err := r.Read()
		if err != nil {
			t.Fatal(err)
		}
		offset := start + uint64(i) + 1
		if r.Offset() != offset {
			t.Fatalf("offset: expect %d but read %d", offset, r.Offset())
		}
		messages = append(messages, string(msg))
	}
	return messages
}

func verifyReadMessages(t *testing.T, path string, messages ...string) {
	gotMessages := readMessages(t, path, 0, len(messages))
	for i, expected := range messages {
		actual := gotMessages[i]
		if actual != expected {
			t.Fatalf("expect %s but got %s", expected, actual)
		}
	}
}

func (f *journalFile) size(t *testing.T) int {
	info, err := os.Stat(f.fileName)
	if err != nil {
		t.Fatal(err)
	}
	return int(info.Size())
}

func (fs *journalDir) sizes(t *testing.T) []int {
	sizes := make([]int, len(fs.files))
	for i := range fs.files {
		sizes[i] = fs.files[i].size(t)
	}
	return sizes
}

func newTestPath(t *testing.T) string {
	path, err := ioutil.TempDir(".", testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	return path
}
