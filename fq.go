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
	}
	Reader struct {
		dir    string
		offset uint64
		file   *os.File
		r      *bufio.Reader
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
		w.file, err = os.OpenFile(names[len(names)-1].fileName, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		if _, err := w.file.Seek(-4, 2); err != nil {
			return nil, err
		}
		size, err := readInt32(w.file)
		if err != nil {
			return nil, err
		}
		if _, err := w.file.Seek(-20-int64(size), 2); err != nil {
			return nil, err
		}
		offset, err := readUint64(w.file)
		if err != nil {
			return nil, err
		}
		w.offset = offset + 1
		if _, err := w.file.Seek(0, 2); err != nil {
			return nil, err
		}
		w.w = bufio.NewWriter(w.file)
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
		dir: dir,
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
	reader.offset = file.startOffset
	for reader.offset < offset {
		if _, err := reader.readMessage(); err != nil {
			return nil, err
		}
	}
	if reader.offset != offset {
		return nil, fmt.Errorf("fail to find offset %d", offset)
	}
	return &reader, nil
}

func (r *Reader) Read() (msg []byte, offset uint64, err error) {
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
