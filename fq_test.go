package fq

import (
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
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

func TestWriteReopen(t *testing.T) {
	path := newTestPath(t)
	defer os.RemoveAll(path)
	messages := []string{"a", "bc"}

	{
		// create empty file only
		w := newTestWriter(t, path, 9999)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		w := newTestWriter(t, path, 9999)
		writeTestMessages(t, w, messages[0])
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		w := newTestWriter(t, path, 9999)
		writeTestMessages(t, w, messages[1])
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
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

// TODO
func TestReadFirst(t *testing.T) {
	// path := newTestPath(t)
	// defer os.RemoveAll(path)
	//
	// w := newTestWriter(t, path, 9999)
	// writeTestMessages(t, w, "a")
	// flushTestWriter(t, w)
}

func TestAppendRead(t *testing.T) {
	maxFileSize := 5500
	path := newTestPath(t)
	defer os.RemoveAll(path)
	var messages []string
	for i := 0; i < 500; i++ {
		messages = append(messages, strconv.Itoa(i))
	}

	{
		w, err := NewWriter(path, maxFileSize)
		if err != nil {
			t.Fatal(err)
			return
		}
		for i := 0; i < 250; i++ {
			msg := messages[i]
			if err := w.Append([]byte(msg)); err != nil {
				t.Fatal(err)
				return
			}
			if w.Offset() != uint64(i+1) {
				t.Fatalf("expect offset %d, got %d", i+1, w.Offset())
				return
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
			return
		}
	}

	r, err := NewReader(path, 0)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer r.Close()
	for i := 0; i < 250; i++ {
		msg, err := r.Read()
		if err != nil {
			t.Fatal(err)
			return
		}
		if int(r.Offset()) != i+1 {
			t.Fatalf("expect offset %d, got %d", i+1, r.Offset())
			return
		}
		if string(msg) != messages[i] {
			t.Fatalf("expect %s, got %s", messages[i], string(msg))
			return
		}
	}

	{
		w, err := NewWriter(path, maxFileSize)
		if err != nil {
			t.Fatal(err)
			return
		}
		for i := 250; i < 500; i++ {
			msg := messages[i]
			if err := w.Append([]byte(msg)); err != nil {
				t.Fatal(err)
				return
			}
			if w.Offset() != uint64(i+1) {
				t.Fatalf("expect offset %d, got %d", i+1, w.Offset())
				return
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
			return
		}
	}

	for i := 250; i < 500; i++ {
		msg, err := r.Read()
		if err != nil {
			t.Fatalf("%d: %v", i, err)
			return
		}
		if int(r.Offset()) != i+1 {
			t.Fatalf("expect offset %d, got %d", i+1, r.Offset())
			return
		}
		if string(msg) != messages[i] {
			t.Fatalf("expect %s, got %s", messages[i], string(msg))
			return
		}
	}
}

func newTestPath(t *testing.T) string {
	path, err := ioutil.TempDir(".", "test-")
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func testMessages(n int) (messages []string) {
	for i := 0; i < n; i++ {
		messages = append(messages, "msg"+strconv.Itoa(i))
	}
	return messages
}

func readMessages(t *testing.T, path string, start uint64, n int) (messages []string) {
	r, err := NewReader(path, start)
	if err != nil {
		t.Fatal(err)
		return nil
	}
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

func (f *journalFile) size(t *testing.T) int {
	info, err := os.Stat(f.fileName)
	if err != nil {
		t.Fatal(err)
	}
	return int(info.Size())
}

func (fs journalFiles) sizes(t *testing.T) []int {
	sizes := make([]int, len(fs))
	for i := range fs {
		sizes[i] = fs[i].size(t)
	}
	return sizes
}
