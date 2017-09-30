package sej

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Test is a collection of test utility methods
type Test struct {
	testing.TB
}

// testDirPrefix is the prefix of test directories
const testDirPrefix = "sej-test-"

// Main should be called to clear test directories
func (Test) Main(m *testing.M) {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	ret := m.Run()
	removeTestFiles()
	os.Exit(ret)
}
func removeTestFiles() {
	files, _ := ioutil.ReadDir(".")
	for _, file := range files {
		if file.IsDir() && strings.HasPrefix(path.Base(file.Name()), testDirPrefix) {
			if err := os.RemoveAll(file.Name()); err != nil {
				fmt.Println(err)
			}
		}
	}
}

// NewDir creates a new test directory
// the path will be deleted automatically after the tests
func (t Test) NewDir() string {
	dir := testDirPrefix + strconv.Itoa(rand.Int())
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	return dir
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

func readMessages(t testing.TB, path string, start uint64, n int) (messages []Message) {
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
		messages = append(messages, *r.Message())
	}
	return messages
}

func readMessageValues(t testing.TB, path string, start uint64, n int) (messages []string) {
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

func (t Test) VerifyMessages(path string, messages []Message) {
	gotMessages := readMessages(t, path, 0, len(messages))
	for i, expected := range messages {
		actual := gotMessages[i]
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("expect %v but got %v", expected, actual)
		}
	}
}

func (t Test) VerifyMessageValues(path string, messages ...string) {
	gotMessages := readMessageValues(t, path, 0, len(messages))
	for i, expected := range messages {
		actual := gotMessages[i]
		if actual != expected {
			t.Fatalf("expect %s but got %s", expected, actual)
		}
	}
}

func (f *JournalFile) size(t testing.TB) int {
	info, err := os.Stat(f.FileName)
	if err != nil {
		t.Fatal(err)
	}
	return int(info.Size())
}

func (fs *JournalDir) sizes(t testing.TB) []int {
	sizes := make([]int, len(fs.Files))
	for i := range fs.Files {
		sizes[i] = fs.Files[i].size(t)
	}
	return sizes
}

func newTestPath(t testing.TB) string {
	path := testDirPrefix + strconv.Itoa(rand.Int())
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
