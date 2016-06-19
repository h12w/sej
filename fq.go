package fq

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	journalFileExt = ".jnl"
)

type (
	Writer struct {
		dir    string
		offset uint64
		file   *os.File
		w      *bufio.Writer
		mu     sync.Mutex
	}
	Reader struct {
		dir    string
		offset uint64
		file   *os.File
		r      *bufio.Reader
		mu     sync.Mutex
	}
)

func NewWriter(dir string) (*Writer, error) {
	var err error
	w := Writer{dir: dir}
	names, err := getJournalFiles(dir)
	if err != nil {
		return nil, err
	}
	if len(names) > 0 {
		//sort.Strings(names)
	} else {
		w.offset = 0
		w.file, err = os.Create(path.Join(dir, fmt.Sprintf("%016x"+journalFileExt, w.offset)))
		if err != nil {
			return nil, err
		}
		w.w = bufio.NewWriter(w.file)
	}
	return &w, nil
}

func (w *Writer) Append(msg []byte) (offset uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	size := len(msg)
	if size > math.MaxInt32 {
		return w.offset, errors.New("message is too long")
	}
	if err := w.writeMessage(msg); err != nil {
		return w.offset, err
	}
	return w.offset, nil
}

func (w *Writer) Flush(offset uint64) error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *Writer) Close() error {
	if err := w.Flush(w.offset); err != nil {
		return err
	}
	return w.file.Close()
}

func NewReader(dir string, offset uint64) (*Reader, error) {
	var err error
	reader := Reader{
		dir:    dir,
		offset: offset,
	}
	files, err := getJournalFiles(dir)
	if err != nil {
		return nil, err
	}
	i := sort.Search(len(files), func(i int) bool { return files[i].startOffset > offset })
	if i == 0 {
		return nil, errors.New("offset is too small")
	}
	file := &files[i-1]
	reader.file, err = os.Open(file.fileName)
	if err != nil {
		return nil, err
	}
	reader.r = bufio.NewReader(reader.file)
	if offset != file.startOffset {
		// search the first offset
	}
	return &reader, nil
}

func (r *Reader) Read() (msg []byte, offset uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	msg, err = r.readMessage()
	if err != nil {
		return nil, 0, err
	}
	return msg, r.offset, nil
}

func (r *Reader) Close() {
	r.file.Close()
}

type (
	journalFile struct {
		startOffset uint64
		fileName    string
	}
	journalFiles []journalFile
)

func getJournalFiles(dir string) (files journalFiles, _ error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	allNames, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	for _, name := range allNames {
		if path.Ext(name) != journalFileExt {
			continue
		}
		offset, err := strconv.ParseUint(strings.TrimSuffix(path.Base(name), journalFileExt), 16, 64)
		if err != nil {
			continue
		}
		files = append(files, journalFile{
			startOffset: offset,
			fileName:    path.Join(dir, name),
		})
	}
	sort.Sort(files)
	return files, nil
}

func (a journalFiles) Len() int           { return len(a) }
func (a journalFiles) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a journalFiles) Less(i, j int) bool { return a[i].startOffset < a[j].startOffset }
