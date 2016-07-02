package sej

import (
	"bufio"
	"errors"
	"math"
	"os"
)

const (
	defaultBufferSize = 83886080
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
	file, err := openOrCreate(journalFile.fileName)
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
		w:           bufio.NewWriterSize(file, defaultBufferSize),
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
		w.file, err = openOrCreate(journalFileName(w.dir, w.offset))
		if err != nil {
			return err
		}
		w.fileSize = 0
		w.w = bufio.NewWriterSize(w.file, defaultBufferSize)
	}
	return nil
}

// Offset returns the latest offset of the journal
func (w *Writer) Offset() uint64 {
	return w.offset
}

// Flush writes any buffered data from memory to the underlying file
func (w *Writer) Flush() error {
	return w.w.Flush()
}

// Sync calls File.Sync of the current file
func (w *Writer) Sync() error {
	return w.file.Sync()
}

// Close closes the writer, flushes the buffer and syncs the file to the hard drive
func (w *Writer) Close() error {
	if err := w.w.Flush(); err != nil {
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

func openOrCreate(file string) (*os.File, error) {
	return os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0644)
}
