package sej

import (
	"fmt"
	"io"
	"os"
)

// const (
// 	notifyTimeout = 10 * time.Millisecond
// )

// Reader reads segmented journal files
type Reader struct {
	offset      uint64
	file        io.ReadCloser
	journalDir  *watchedJournalDir
	journalFile *journalFile
	fileChanged chan bool
	dirChanged  chan bool
}

// NewReader creates a reader for reading dir starting from offset
func NewReader(dir string, offset uint64) (*Reader, error) {
	r := Reader{
		fileChanged: make(chan bool),
		dirChanged:  make(chan bool),
	}
	journalDir, err := openWatchedJournalDir(dir, r.dirChanged)
	if err != nil {
		return nil, err
	}
	journalFile, err := journalDir.Find(offset)
	if err != nil {
		return nil, err
	}
	if journalDir.IsLast(journalFile) {
		r.file, err = openWatchedFile(journalFile.fileName, r.fileChanged)
	} else {
		r.file, err = os.Open(journalFile.fileName)
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
	var offset uint64
	for {
		msg, offset, err = readMessage(r.file)
		if err == io.EOF {
			if r.journalDir.IsLast(r.journalFile) {
				select {
				case <-r.fileChanged:
					// fmt.Println("file changed")
					continue
				case <-r.dirChanged:
					// fmt.Println("dir changed")

					// case <-time.After(notifyTimeout):
					// 		fmt.Println("timeout")
					// 	continue
				}
			}
			if !r.journalDir.IsLast(r.journalFile) {
				if err := r.moveToNextFile(); err != nil {
					return nil, err
				}
			}
			continue
		} else if err != nil {
			return nil, err
		}
		break
	}
	if offset != r.offset {
		return nil, fmt.Errorf("offset is out of order, expect %d but got %d", r.offset, offset)
	}
	r.offset++
	return msg, nil
}

func (r *Reader) moveToNextFile() error {
	journalFile, err := r.journalDir.Find(r.offset)
	if err != nil {
		return err
	}
	var newFile io.ReadCloser
	if r.journalDir.IsLast(journalFile) {
		newFile, err = openWatchedFile(journalFile.fileName, r.fileChanged)
	} else {
		newFile, err = os.Open(journalFile.fileName)
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
