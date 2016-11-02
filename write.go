package sej

import (
	"bufio"
	"errors"
	"io"
	"math"
	"os"
	"sync"
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
	mu          sync.Mutex
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
	latestOffset, err := journalFile.LatestOffset()
	if err != nil {
		lock.Close()
		file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, os.SEEK_END); err != nil {
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
		w:           newBufferWriter(file),
	}, nil
}

// Append appends a message to the journal
func (w *Writer) Append(msg []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
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
		if err := w.closeFile(); err != nil {
			return err
		}
		var err error
		w.file, err = openOrCreate(journalFileName(w.dir, w.offset))
		if err != nil {
			return err
		}
		w.fileSize = 0
		w.w = newBufferWriter(w.file)
	}
	return nil
}

// Offset returns the latest offset of the journal
func (w *Writer) Offset() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// Flush writes any buffered data from memory to the underlying file
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Flush()
}

// Sync calls File.Sync of the current file
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

// Close closes the writer, flushes the buffer and syncs the file to the hard drive
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.closeFile(); err != nil {
		return err
	}
	return w.lock.Close()
}

func (w *Writer) closeFile() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	return nil
}

func openOrCreate(file string) (*os.File, error) {
	return os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
}

func newBufferWriter(w io.Writer) *bufio.Writer {
	return bufio.NewWriterSize(w, 4096)
}
