// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package reader

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"testing/iotest"
)

func TestSeek(t *testing.T) {
	a := make([]byte, 255)
	for i := range a {
		a[i] = byte(i)
	}
	buf1 := make([]byte, 1)
	r := NewReaderSize(bytes.NewReader(a), 10)
	{
		const pos = 5
		n, err := r.Seek(5, os.SEEK_CUR)
		if err != nil {
			t.Fatal(err)
		}
		if n != pos {
			t.Fatalf("expect %d, got %d", pos, n)
		}
		if _, err := io.ReadFull(r, buf1); err != nil {
			t.Fatal(err)
		}
		if buf1[0] != pos {
			t.Fatalf("expect %d, got %d", pos, buf1[0])
		}
	}
	{
		const pos = 7
		n, err := r.Seek(1, os.SEEK_CUR)
		if err != nil {
			t.Fatal(err)
		}
		if n != pos {
			t.Fatalf("expect %d, got %d", pos, n)
		}
		if _, err := io.ReadFull(r, buf1); err != nil {
			t.Fatal(err)
		}
		if buf1[0] != pos {
			t.Fatalf("expect %d, got %d", pos, buf1[0])
		}
	}
	{
		const pos = 7
		n, err := r.Seek(-1, os.SEEK_CUR)
		if err != nil {
			t.Fatal(err)
		}
		if n != pos {
			t.Fatalf("expect %d, got %d", pos, n)
		}
		if _, err := io.ReadFull(r, buf1); err != nil {
			t.Fatal(err)
		}
		if buf1[0] != pos {
			t.Fatalf("expect %d, got %d", pos, buf1[0])
		}
	}
	{
		const pos = 18
		n, err := r.Seek(10, os.SEEK_CUR)
		if err != nil {
			t.Fatal(err)
		}
		if n != pos {
			t.Fatalf("expect %d, got %d", pos, n)
		}
		if _, err := io.ReadFull(r, buf1); err != nil {
			t.Fatal(err)
		}
		if buf1[0] != pos {
			t.Fatalf("expect %d, got %d", pos, buf1[0])
		}
	}
	{
		const pos = 19
		n, err := r.Seek(-1000, os.SEEK_CUR)
		if err == nil {
			t.Fatal("expect error")
		}
		if n != pos {
			t.Fatalf("expect %d, got %d", pos, n)
		}
		if _, err := io.ReadFull(r, buf1); err != nil {
			t.Fatal(err)
		}
		if buf1[0] != pos {
			t.Fatalf("expect %d, got %d", pos, buf1[0])
		}
	}
}

type nopSeeker struct {
	io.Reader
}

func (nopSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case os.SEEK_SET:
		return offset, nil
	}
	return 0, nil
}

// Reads from a reader and rot13s the result.
type rot13Reader struct {
	r io.Reader
}

func newRot13Reader(r io.Reader) *rot13Reader {
	r13 := new(rot13Reader)
	r13.r = r
	return r13
}

func (r13 *rot13Reader) Read(p []byte) (int, error) {
	n, err := r13.r.Read(p)
	for i := 0; i < n; i++ {
		c := p[i] | 0x20 // lowercase byte
		if 'a' <= c && c <= 'm' {
			p[i] += 13
		} else if 'n' <= c && c <= 'z' {
			p[i] -= 13
		}
	}
	return n, err
}

type readMaker struct {
	name string
	fn   func(io.Reader) io.Reader
}

var readMakers = []readMaker{
	{"full", func(r io.Reader) io.Reader { return r }},
	{"byte", iotest.OneByteReader},
	{"half", iotest.HalfReader},
	{"data+err", iotest.DataErrReader},
	{"timeout", iotest.TimeoutReader},
}

// Call Read to accumulate the text of a file
func reads(buf *Reader, m int) string {
	var b [1000]byte
	nb := 0
	for {
		n, err := buf.Read(b[nb : nb+m])
		nb += n
		if err == io.EOF {
			break
		}
	}
	return string(b[0:nb])
}

type bufReader struct {
	name string
	fn   func(*Reader) string
}

var bufreaders = []bufReader{
	{"1", func(b *Reader) string { return reads(b, 1) }},
	{"2", func(b *Reader) string { return reads(b, 2) }},
	{"3", func(b *Reader) string { return reads(b, 3) }},
	{"4", func(b *Reader) string { return reads(b, 4) }},
	{"5", func(b *Reader) string { return reads(b, 5) }},
	{"7", func(b *Reader) string { return reads(b, 7) }},
}

var bufsizes = []int{
	0, minReadBufferSize, 23, 32, 46, 64, 93, 128, 1024, 4096,
}

func TestReader(t *testing.T) {
	var texts [31]string
	str := ""
	all := ""
	for i := 0; i < len(texts)-1; i++ {
		texts[i] = str + "\n"
		all += texts[i]
		str += string(i%26 + 'a')
	}
	texts[len(texts)-1] = all

	for h := 0; h < len(texts); h++ {
		text := texts[h]
		for i := 0; i < len(readMakers); i++ {
			for j := 0; j < len(bufreaders); j++ {
				for k := 0; k < len(bufsizes); k++ {
					readmaker := readMakers[i]
					bufreader := bufreaders[j]
					bufsize := bufsizes[k]
					read := readmaker.fn(strings.NewReader(text))
					buf := NewReaderSize(nopSeeker{read}, bufsize)
					s := bufreader.fn(buf)
					if s != text {
						t.Errorf("reader=%s fn=%s bufsize=%d want=%q got=%q",
							readmaker.name, bufreader.name, bufsize, text, s)
					}
				}
			}
		}
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	return 0, nil
}

// A StringReader delivers its data one string segment at a time via Read.
type StringReader struct {
	data []string
	step int
}

func (r *StringReader) Read(p []byte) (n int, err error) {
	if r.step < len(r.data) {
		s := r.data[r.step]
		n = copy(p, s)
		r.step++
	} else {
		err = io.EOF
	}
	return
}

var segmentList = [][]string{
	{},
	{""},
	{"日", "本語"},
	{"\u65e5", "\u672c", "\u8a9e"},
	{"\U000065e5", "\U0000672c", "\U00008a9e"},
	{"\xe6", "\x97\xa5\xe6", "\x9c\xac\xe8\xaa\x9e"},
	{"Hello", ", ", "World", "!"},
	{"Hello", ", ", "", "World", "!"},
}

func TestNewReaderSizeIdempotent(t *testing.T) {
	const BufSize = 1000
	b := NewReaderSize(strings.NewReader("hello world"), BufSize)
	// Does it recognize itself?
	b1 := NewReaderSize(b, BufSize)
	if b1 != b {
		t.Error("NewReaderSize did not detect underlying Reader")
	}
	// Does it wrap if existing buffer is too small?
	b2 := NewReaderSize(b, 2*BufSize)
	if b2 == b {
		t.Error("NewReaderSize did not enlarge buffer")
	}
}

type dataAndEOFReader string

func (r dataAndEOFReader) Read(p []byte) (int, error) {
	return copy(p, r), io.EOF
}

var testOutput = []byte("0123456789abcdefghijklmnopqrstuvwxy")
var testInput = []byte("012\n345\n678\n9ab\ncde\nfgh\nijk\nlmn\nopq\nrst\nuvw\nxy")
var testInputrn = []byte("012\r\n345\r\n678\r\n9ab\r\ncde\r\nfgh\r\nijk\r\nlmn\r\nopq\r\nrst\r\nuvw\r\nxy\r\n\n\r\n")

// TestReader wraps a []byte and returns reads of a specific length.
type testReader struct {
	data   []byte
	stride int
}

func (t *testReader) Read(buf []byte) (n int, err error) {
	n = t.stride
	if n > len(t.data) {
		n = len(t.data)
	}
	if n > len(buf) {
		n = len(buf)
	}
	copy(buf, t.data)
	t.data = t.data[n:]
	if len(t.data) == 0 {
		err = io.EOF
	}
	return
}

type readLineResult struct {
	line     []byte
	isPrefix bool
	err      error
}

var readLineNewlinesTests = []struct {
	input  string
	expect []readLineResult
}{
	{"012345678901234\r\n012345678901234\r\n", []readLineResult{
		{[]byte("012345678901234"), true, nil},
		{nil, false, nil},
		{[]byte("012345678901234"), true, nil},
		{nil, false, nil},
		{nil, false, io.EOF},
	}},
	{"0123456789012345\r012345678901234\r", []readLineResult{
		{[]byte("0123456789012345"), true, nil},
		{[]byte("\r012345678901234"), true, nil},
		{[]byte("\r"), false, nil},
		{nil, false, io.EOF},
	}},
}

func createTestInput(n int) []byte {
	input := make([]byte, n)
	for i := range input {
		// 101 and 251 are arbitrary prime numbers.
		// The idea is to create an input sequence
		// which doesn't repeat too frequently.
		input[i] = byte(i % 251)
		if i%101 == 0 {
			input[i] ^= byte(i / 101)
		}
	}
	return input
}

type errorWriterToTest struct {
	rn, wn     int
	rerr, werr error
	expected   error
}

func (r errorWriterToTest) Read(p []byte) (int, error) {
	return len(p) * r.rn, r.rerr
}

func (w errorWriterToTest) Write(p []byte) (int, error) {
	return len(p) * w.wn, w.werr
}

var errorWriterToTests = []errorWriterToTest{
	{1, 0, nil, io.ErrClosedPipe, io.ErrClosedPipe},
	{0, 1, io.ErrClosedPipe, nil, io.ErrClosedPipe},
	{0, 0, io.ErrUnexpectedEOF, io.ErrClosedPipe, io.ErrClosedPipe},
	{0, 1, io.EOF, nil, nil},
}

type errorReaderFromTest struct {
	rn, wn     int
	rerr, werr error
	expected   error
}

func (r errorReaderFromTest) Read(p []byte) (int, error) {
	return len(p) * r.rn, r.rerr
}

func (w errorReaderFromTest) Write(p []byte) (int, error) {
	return len(p) * w.wn, w.werr
}

var errorReaderFromTests = []errorReaderFromTest{
	{0, 1, io.EOF, nil, nil},
	{1, 1, io.EOF, nil, nil},
	{0, 1, io.ErrClosedPipe, nil, io.ErrClosedPipe},
	{0, 0, io.ErrClosedPipe, io.ErrShortWrite, io.ErrClosedPipe},
	{1, 0, nil, io.ErrShortWrite, io.ErrShortWrite},
}

// A writeCountingDiscard is like ioutil.Discard and counts the number of times
// Write is called on it.
type writeCountingDiscard int

func (w *writeCountingDiscard) Write(p []byte) (int, error) {
	*w++
	return len(p), nil
}

type negativeReader int

func (r *negativeReader) Read([]byte) (int, error) { return -1, nil }

func TestNegativeRead(t *testing.T) {
	// should panic with a description pointing at the reader, not at itself.
	// (should NOT panic with slice index error, for example.)
	b := NewReader(nopSeeker{new(negativeReader)})
	defer func() {
		switch err := recover().(type) {
		case nil:
			t.Fatal("read did not panic")
		case error:
			if !strings.Contains(err.Error(), "reader returned negative count from Read") {
				t.Fatalf("wrong panic: %v", err)
			}
		default:
			t.Fatalf("unexpected panic value: %T(%v)", err, err)
		}
	}()
	b.Read(make([]byte, 100))
}

var errFake = errors.New("fake error")

type errorThenGoodReader struct {
	didErr bool
	nread  int
}

func (r *errorThenGoodReader) Read(p []byte) (int, error) {
	r.nread++
	if !r.didErr {
		r.didErr = true
		return 0, errFake
	}
	return len(p), nil
}

func TestReaderClearError(t *testing.T) {
	r := &errorThenGoodReader{}
	b := NewReader(nopSeeker{r})
	buf := make([]byte, 1)
	if _, err := b.Read(nil); err != nil {
		t.Fatalf("1st nil Read = %v; want nil", err)
	}
	if _, err := b.Read(buf); err != errFake {
		t.Fatalf("1st Read = %v; want errFake", err)
	}
	if _, err := b.Read(nil); err != nil {
		t.Fatalf("2nd nil Read = %v; want nil", err)
	}
	if _, err := b.Read(buf); err != nil {
		t.Fatalf("3rd Read with buffer = %v; want nil", err)
	}
	if r.nread != 2 {
		t.Errorf("num reads = %d; want 2", r.nread)
	}
}

type emptyThenNonEmptyReader struct {
	r io.Reader
	n int
}

func (r *emptyThenNonEmptyReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return r.r.Read(p)
	}
	r.n--
	return 0, nil
}

func TestReadZero(t *testing.T) {
	for _, size := range []int{100, 2} {
		t.Run(fmt.Sprintf("bufsize=%d", size), func(t *testing.T) {
			r := io.MultiReader(strings.NewReader("abc"), &emptyThenNonEmptyReader{r: strings.NewReader("def"), n: 1})
			br := NewReaderSize(nopSeeker{r}, size)
			want := func(s string, wantErr error) {
				p := make([]byte, 50)
				n, err := br.Read(p)
				if err != wantErr || n != len(s) || string(p[:n]) != s {
					t.Fatalf("read(%d) = %q, %v, want %q, %v", len(p), string(p[:n]), err, s, wantErr)
				}
				t.Logf("read(%d) = %q, %v", len(p), string(p[:n]), err)
			}
			want("abc", nil)
			want("", nil)
			want("def", nil)
			want("", io.EOF)
		})
	}
}

// An onlyReader only implements io.Reader, no matter what other methods the underlying implementation may have.
type onlyReader struct {
	io.Reader
}

// An onlyWriter only implements io.Writer, no matter what other methods the underlying implementation may have.
type onlyWriter struct {
	io.Writer
}

// A scriptedReader is an io.Reader that executes its steps sequentially.
type scriptedReader []func(p []byte) (n int, err error)

func (sr *scriptedReader) Read(p []byte) (n int, err error) {
	if len(*sr) == 0 {
		panic("too many Read calls on scripted Reader. No steps remain.")
	}
	step := (*sr)[0]
	*sr = (*sr)[1:]
	return step(p)
}

func newScriptedReader(steps ...func(p []byte) (n int, err error)) io.Reader {
	sr := scriptedReader(steps)
	return &sr
}

func BenchmarkReaderEmpty(b *testing.B) {
	b.ReportAllocs()
	str := strings.Repeat("x", 16<<10)
	for i := 0; i < b.N; i++ {
		br := NewReader(strings.NewReader(str))
		n, err := io.Copy(ioutil.Discard, br)
		if err != nil {
			b.Fatal(err)
		}
		if n != int64(len(str)) {
			b.Fatal("wrong length")
		}
	}
}
