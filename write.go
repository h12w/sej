package fq

import (
	"bufio"
	"errors"
	"math"
	"os"
)

type Writer struct {
	dir         string
	offset      uint64
	w           *bufio.Writer
	file        *os.File
	fileSize    int
	segmentSize int
}

func NewWriter(dir string, segmentSize int) (*Writer, error) {
	var err error
	w := Writer{
		dir:         dir,
		segmentSize: segmentSize,
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

func (w *Writer) Append(msg []byte) error {
	size := len(msg)
	if size > math.MaxInt32 {
		return errors.New("message is too long")
	}
	if err := writeMessage(w.w, msg, w.offset); err != nil {
		return err
	}
	w.offset++
	w.fileSize += metaSize + len(msg)
	if w.fileSize >= w.segmentSize {
		if err := w.Close(); err != nil {
			return err
		}
		var err error
		w.file, err = createNewJournalFile(w.dir, w.offset)
		if err != nil {
			return err
		}
		w.fileSize = 0
		w.w = bufio.NewWriter(w.file)
	}
	return nil
}

func (w *Writer) Offset() uint64 {
	return w.offset
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
