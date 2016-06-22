package fq

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
)

type (
	Writer struct {
		dir         string
		offset      uint64
		w           *bufio.Writer
		file        *os.File
		fileSize    int
		maxFileSize int
	}
	Reader struct {
		dir          string
		offset       uint64
		r            *bufio.Reader
		file         *os.File
		journalFiles journalFiles
		journalIndex int
	}
)

func NewWriter(dir string, maxFileSize int) (*Writer, error) {
	var err error
	w := Writer{
		dir:         dir,
		maxFileSize: maxFileSize,
	}
	names, err := getJournalFiles(dir)
	if err != nil {
		return nil, err
	}
	if len(names) > 0 {
		journalFile := &names[len(names)-1]
		w.file, err = os.OpenFile(journalFile.fileName, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		fileSize, err := w.file.Seek(0, os.SEEK_END)
		if err != nil {
			return nil, err
		}
		if fileSize == 0 {
			w.offset = journalFile.startOffset
		} else {
			w.fileSize = int(fileSize)
			_, offset, err := readMessageBackward(w.file)
			if err != nil {
				return nil, err
			}
			w.offset = offset + 1
		}
	} else {
		w.file, err = createNewJournalFile(w.dir, w.offset)
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
	w.fileSize += metaSize + len(msg)
	if w.fileSize >= w.maxFileSize {
		if err := w.Close(); err != nil {
			return w.offset, err
		}
		w.file, err = createNewJournalFile(w.dir, w.offset)
		if err != nil {
			return w.offset, err
		}
		w.fileSize = 0
		w.w = bufio.NewWriter(w.file)
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
	files, err := getJournalFiles(dir)
	if err != nil {
		return nil, err
	}
	i := sort.Search(len(files), func(i int) bool { return files[i].startOffset > offset })
	if i == 0 {
		return nil, errors.New("offset is too small")
	}
	journalIndex := i - 1
	file := &files[journalIndex]
	reader := Reader{
		dir:          dir,
		journalFiles: files,
		journalIndex: journalIndex,
	}
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
	if err == io.EOF {
		if r.journalIndex < len(r.journalFiles)-1 && r.offset == r.journalFiles[r.journalIndex+1].startOffset {
			r.Close()
			r.journalIndex++
			journalFile := &r.journalFiles[r.journalIndex]
			r.file, err = os.Open(journalFile.fileName)
			if err != nil {
				return nil, err
			}
			r.r = bufio.NewReader(r.file)
			return r.Read()
		}
		return nil, err // TODO: watch and tail this file
	} else if err != nil {
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
