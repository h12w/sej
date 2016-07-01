package sej

import (
	"bufio"
	"errors"
	"math"
	"os"
)

// Writer writes to segmented journal files
type Writer struct {
	dir         string
	lock        *fileLock
	offset      uint64
	w           *bufio.Writer
	file        *os.File
	fileSize    int
	segmentSize int
}

// NewWriter creates a new writer for writing to dir with file size at least segmentSize
func NewWriter(dir string, segmentSize int) (*Writer, error) {
	lock, err := openFileLock(dir + ".lck")
	if err != nil {
		return nil, err
	}
	names, err := openJournalDir(dir)
	if err != nil {
		lock.Close()
		return nil, err
	}
	journalFile := names.last()
	file, err := os.OpenFile(journalFile.fileName, os.O_RDWR, 0644)
	if err != nil {
		lock.Close()
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		lock.Close()
		file.Close()
		return nil, err
	}
	latestOffset, err := getLatestOffset(journalFile, file)
	if err != nil {
		lock.Close()
		file.Close()
		return nil, err
	}
	return &Writer{
		dir:         dir,
		lock:        lock,
		file:        file,
		offset:      latestOffset,
		segmentSize: segmentSize,
		fileSize:    int(stat.Size()),
		w:           bufio.NewWriter(file),
	}, nil
}

// Append appends a message to the journal
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

// Offset returns the latest offset of the journal
func (w *Writer) Offset() uint64 {
	return w.offset
}

func (w *Writer) flush() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close closes the writer, flushes the buffer and syncs the file to the hard drive
func (w *Writer) Close() error {
	if err := w.flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	return w.lock.Close()
}
