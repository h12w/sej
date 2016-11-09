package sej

import (
	"fmt"
	"io"
	"os"
	"time"
)

var (
	// NotifyTimeout is the timeout value in rare cases that the OS notification fails
	// to capture the file/directory change events
	NotifyTimeout = time.Hour
)

// Reader reads segmented journal files
type Reader struct {
	offset      uint64
	journalDir  *watchedJournalDir
	journalFile *JournalFile
	file        watchedReadSeekCloser
}
type watchedReadSeekCloser interface {
	readSeekCloser
	Watch() chan bool
}

// NewReader creates a reader for reading dir starting from offset
func NewReader(dir string, offset uint64) (*Reader, error) {
	r := Reader{}
	journalDir, err := openWatchedJournalDir(dir)
	if err != nil {
		return nil, err
	}
	journalFile, err := journalDir.Find(offset)
	if err != nil {
		return nil, err
	}
	if journalDir.IsLast(journalFile) {
		r.file, err = openWatchedFile(journalFile.fileName)
	} else {
		r.file, err = openDummyWatchedFile(journalFile.fileName)
	}
	if err != nil {
		return nil, err
	}
	r.offset = journalFile.startOffset
	r.journalFile = journalFile
	r.journalDir = journalDir
	for r.offset < offset {
		if _, err := r.Read(); err != nil {
			return nil, err
		}
	}
	if r.offset != offset {
		return nil, fmt.Errorf("fail to find offset %d", offset)
	}
	return &r, nil
}

// Read reads a message and increment the offset
func (r *Reader) Read() (msg []byte, err error) {
	var message *Message
	for {
		fileChanged, dirChanged := r.file.Watch(), r.journalDir.Watch()
		message, err = ReadMessage(r.file)
		if err == io.EOF {
			if r.journalDir.IsLast(r.journalFile) {
				select {
				case <-fileChanged:
					continue // read message again
				case <-dirChanged:
					if err := r.reopenFile(); err != nil {
						return nil, err
					}
				case <-time.After(NotifyTimeout):
					continue
				}
			} else if err := r.reopenFile(); err != nil {
				return nil, err
			}
			continue
		} else if err != nil {
			return nil, err
		}
		break
	}
	if message.Offset != r.offset {
		return nil, fmt.Errorf("offset is out of order, expect %d but got %d", r.offset, message.Offset)
	}
	r.offset++
	return message.Value, nil
}

func (r *Reader) reopenFile() error {
	journalFile, err := r.journalDir.Find(r.offset)
	if err != nil {
		return err
	}
	var newFile watchedReadSeekCloser
	if r.journalDir.IsLast(journalFile) {
		newFile, err = openWatchedFile(journalFile.fileName)
	} else {
		newFile, err = openDummyWatchedFile(journalFile.fileName)
	}
	if err != nil {
		return err
	}
	if err := r.file.Close(); err != nil {
		return err
	}
	r.file = newFile
	r.journalFile = journalFile
	return nil
}

// Offset returns the current offset of the reader
func (r *Reader) Offset() uint64 {
	return r.offset
}

// Close closes the reader
func (r *Reader) Close() error {
	err1 := r.journalDir.Close()
	err2 := r.file.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

type dummyWatchedFile struct {
	*os.File
}

func openDummyWatchedFile(file string) (*dummyWatchedFile, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	return &dummyWatchedFile{File: f}, nil
}

func (f *dummyWatchedFile) Watch() chan bool { return nil }
