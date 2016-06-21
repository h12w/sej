package fq

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
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
		if _, err := w.file.Seek(0, os.SEEK_END); err != nil {
			return nil, err
		}
		_, offset, err := readMessageBackward(w.file)
		if err != nil {
			return nil, err
		}
		w.offset = offset + 1
	} else {
		w.file, err = os.Create(path.Join(dir, fmt.Sprintf("%016x"+journalFileExt, w.offset)))
		if err != nil {
			return nil, err
		}
	}
	w.w = bufio.NewWriter(w.file)
	return &w, nil
}

func (w *Writer) Append(msg []byte) (offset uint64, err error) {
	size := len(msg)
	if size > math.MaxInt32 {
		return w.offset, errors.New("message is too long")
	}
	if err := writeMessage(w.w, msg, w.offset); err != nil {
		return w.offset, err
	}
	w.offset++
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
		if _, err := reader.Read(); err != nil {
			return nil, err
		}
	}
	if reader.offset != offset {
		return nil, fmt.Errorf("fail to find offset %d", offset)
	}
	return &reader, nil
}

func (r *Reader) Read() (msg []byte, err error) {
	msg, offset, err := readMessage(r.r)
	if err != nil {
		return nil, err
	}
	if offset != r.offset {
		return nil, fmt.Errorf("offset is out of order: %d, %d", offset, r.offset)
	}
	r.offset++
	return msg, nil
}

func (r *Reader) Offset() uint64 {
	return r.offset
}

func (r *Reader) Close() {
	r.file.Close()
}
