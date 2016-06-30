package sej

import (
	"bufio"
	"errors"
	"math"
	"os"
)

type Writer struct {
	dir         string
	lock        *fileLock
	offset      uint64
	w           *bufio.Writer
	file        *os.File
	fileSize    int
	segmentSize int
}

func NewWriter(dir string, segmentSize int) (*Writer, error) {
	lock, err := openFileLock(dir + ".lck")
	if err != nil {
		return nil, err
	}
	names, err := openJournalDir(dir)
	if err != nil {
		return nil, err
	}
	journalFile := names.last()
	file, err := os.OpenFile(journalFile.fileName, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	fileSize, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}
	var offset uint64
	if fileSize == 0 {
		offset = journalFile.startOffset
	} else {
		_, offset, err = readMessageBackward(file)
		if err != nil {
			return nil, err
		}
		offset = offset + 1
	}
	return &Writer{
		dir:         dir,
		lock:        lock,
		file:        file,
		offset:      offset,
		segmentSize: segmentSize,
		fileSize:    int(fileSize),
		w:           bufio.NewWriter(file),
	}, nil
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
	if err := w.file.Close(); err != nil {
		return err
	}
	return w.lock.Close()
}
