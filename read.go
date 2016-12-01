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

	CheckCRC bool // whether or not to check CRC for each message
}
type watchedReadSeekCloser interface {
	readSeekCloser
	Watch() chan bool
}

// NewReader creates a reader for reading dir/jnl starting from offset
func NewReader(dir string, offset uint64) (*Reader, error) {
	dir = JournalDirPath(dir)
	r := Reader{
		CheckCRC: true,
	}
	journalDir, err := openWatchedJournalDir(dir)
	if err != nil {
		return nil, err
	}
	journalFile, err := journalDir.Find(offset)
	if err != nil {
		return nil, err
	}
	if journalDir.IsLast(journalFile) {
		r.file, err = openWatchedFile(journalFile.FileName)
	} else {
		r.file, err = openDummyWatchedFile(journalFile.FileName)
	}
	if err != nil {
		return nil, err
	}
	r.offset = journalFile.FirstOffset
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
func (r *Reader) Read() (message *Message, err error) {
	message = &Message{}
	for {
		fileChanged, dirChanged := r.file.Watch(), r.journalDir.Watch()
		n, err := message.ReadFrom(r.file)
		if err != nil {
			// rollback the reader
			if _, seekErr := r.file.Seek(-n, io.SeekCurrent); seekErr != nil {
				return nil, err
			}

			// unexpected io error
			switch err {
			case io.EOF, io.ErrUnexpectedEOF:
			default:
				return nil, err
			}

			// not the last one, open the next journal file
			if !r.journalDir.IsLast(r.journalFile) {
				if err := r.reopenFile(); err != nil {
					return nil, err
				}
				continue
			}

			// the last one, wait for any changes
			select {
			case <-dirChanged:
				if err := r.reopenFile(); err != nil {
					return nil, err
				}
			case <-fileChanged:
			case <-time.After(NotifyTimeout):
			}
			continue
		}
		break
	}

	// check offset
	if message.Offset != r.offset {
		return nil, fmt.Errorf("offset is out of order, expect %d but got %d", r.offset, message.Offset)
	}
	r.offset++

	if r.CheckCRC {
		if err := message.checkCRC(); err != nil {
			return message, err
		}
	}

	return message, nil
}

func (r *Reader) reopenFile() error {
	journalFile, err := r.journalDir.Find(r.offset)
	if err != nil {
		return err
	}
	var newFile watchedReadSeekCloser
	if r.journalDir.IsLast(journalFile) {
		newFile, err = openWatchedFile(journalFile.FileName)
	} else {
		newFile, err = openDummyWatchedFile(journalFile.FileName)
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
