package sej

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func writeTestMessages(t testing.TB, w *Writer, messages ...string) {
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

func newTestWriter(t testing.TB, dir string, segmentSize ...int) *Writer {
	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segmentSize) == 1 {
		w.SegmentSize = segmentSize[0]
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
		messages = append(messages, string(msg.Value))
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

func (f *JournalFile) size(t *testing.T) int {
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

func newTestPath(t testing.TB) string {
	path, err := ioutil.TempDir(".", testFilePrefix)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func truncateFile(t testing.TB, file string, offset int) {
	f, err := os.OpenFile(file, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(stat.Size() - int64(offset)); err != nil {
		t.Fatal(err)
	}
}
