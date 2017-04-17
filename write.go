package sej

import (
	"bufio"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"os"
	"sync"
)

// Writer writes to segmented journal files
type Writer struct {
	dir     string
	dirLock *fileLock
	offset  uint64

	w       *bufio.Writer
	file    *os.File
	fileLen int

	err error
	mu  sync.Mutex

	SegmentSize int
}

// NewWriter creates a new writer for writing to dir/jnl with file size at least segmentSize
func NewWriter(dir string) (*Writer, error) {
	dir = JournalDirPath(dir)
	dirLock, err := openFileLock(dir + ".lck")
	if err != nil {
		return nil, err
	}
	names, err := OpenJournalDir(dir)
	if err != nil {
		dirLock.Close()
		return nil, err
	}
	journalFile := names.Last()
	file, err := openOrCreate(journalFile.FileName)
	if err != nil {
		dirLock.Close()
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		dirLock.Close()
		file.Close()
		return nil, err
	}
	latestOffset, err := journalFile.LastOffset()
	if err != nil {
		dirLock.Close()
		file.Close()
		if err != errMessageCorrupted {
			return nil, err
		}
		bad, lastMsg, fixErr := truncateCorruption(journalFile.FileName)
		return nil, &CorruptionError{
			File:      journalFile.FileName,
			Offset:    lastMsg.Offset + 1,
			Timestamp: lastMsg.Timestamp,
			Message:   bad,
			Err:       err,
			FixErr:    fixErr,
		}
	}
	if _, err := file.Seek(0, os.SEEK_END); err != nil {
		dirLock.Close()
		file.Close()
		return nil, err
	}
	return &Writer{
		dir:         dir,
		dirLock:     dirLock,
		file:        file,
		offset:      latestOffset,
		fileLen:     int(stat.Size()),
		w:           newBufferWriter(file),
		SegmentSize: 1024 * 1024 * 1024,
	}, nil
}

// Append appends a message to the journal
func (w *Writer) Append(msg Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil { // skip if an error already happens
		return w.err
	}
	if len(msg.Key) > math.MaxInt8 {
		return errors.New("key is too long")
	}
	if len(msg.Value) > math.MaxInt32 {
		return errors.New("value is too long")
	}
	msg.Offset = w.offset
	numWritten, err := msg.WriteTo(w.w)
	w.fileLen += int(numWritten)
	if err != nil {
		w.err = err
		return err
	}
	w.offset++
	if w.fileLen >= w.SegmentSize {
		if err := w.closeFile(); err != nil {
			w.err = err
			return err
		}
		var err error
		w.file, err = openOrCreate(journalFileName(w.dir, w.offset))
		if err != nil {
			w.err = err
			return err
		}
		w.fileLen = 0
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
	return w.dirLock.Close()
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

func truncateCorruption(file string) (bad []byte, lastMessage *Message, err error) {
	f, err := os.OpenFile(file, os.O_RDWR, 0644)
	if err != nil {
		return nil, &Message{}, err
	}
	defer f.Close()
	var msg Message
	var lastMsg Message
	for {
		n, err := msg.ReadFrom(f)
		if err != nil {
			switch err {
			case io.EOF, io.ErrUnexpectedEOF, errMessageCorrupted:
				offset, err := f.Seek(-n, io.SeekCurrent)
				if err != nil {
					return nil, &lastMsg, err
				}
				truncatedMsg, err := ioutil.ReadAll(f)
				if err != nil {
					return nil, &lastMsg, err
				}
				if err := f.Truncate(offset); err != nil {
					return nil, &lastMsg, err
				}
				return truncatedMsg, &lastMsg, nil
			default:
				return nil, &lastMsg, err
			}
		}
		lastMsg = msg
	}
}
