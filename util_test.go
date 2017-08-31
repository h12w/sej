package sej

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func writeTestMessages(t testing.TB, w *Writer, messages ...string) {
	start := w.Offset()
	for i, msg := range messages {
		if err := w.Append(&Message{Value: []byte(msg)}); err != nil {
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

func closeTestWriter(t testing.TB, w *Writer) {
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
	r, err := NewScanner(path, start)
	if err != nil {
		t.Fatal(err)
		return nil
	}
	defer r.Close()
	for i := 0; i < n; i++ {
		r.Scan()
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		offset := start + uint64(i) + 1
		if r.Offset() != offset {
			t.Fatalf("offset: expect %d but read %d", offset, r.Offset())
		}
		messages = append(messages, string(r.Message().Value))
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
	info, err := os.Stat(f.FileName)
	if err != nil {
		t.Fatal(err)
	}
	return int(info.Size())
}

func (fs *JournalDir) sizes(t *testing.T) []int {
	sizes := make([]int, len(fs.Files))
	for i := range fs.Files {
		sizes[i] = fs.Files[i].size(t)
	}
	return sizes
}

func newTestPath(t testing.TB) string {
	path := testFilePrefix + strconv.Itoa(rand.Int())
	if err := os.Mkdir(path, 0755); err != nil {
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
