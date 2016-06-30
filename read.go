package sej

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

// const (
// 	notifyTimeout = 10 * time.Millisecond
// )

type Reader struct {
	offset      uint64
	r           *bufio.Reader
	file        io.ReadCloser
	journalDir  *watchedJournalDir
	journalFile *journalFile
	fileChanged chan bool
	dirChanged  chan bool
}

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
	r.r = bufio.NewReader(r.file)
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

func (r *Reader) Read() (msg []byte, err error) {
	var offset uint64
	for {
		msg, offset, err = readMessage(r.r)
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
			if err := r.moveToNextFile(); err != nil {
				return nil, err
			}
			continue
		} else if err != nil {
			return nil, err
		}
		break
	}
	if offset != r.offset {
		return nil, fmt.Errorf("offset is out of order: %d, %d", offset, r.offset)
	}
	r.offset++
	return msg, nil
}

func (r *Reader) moveToNextFile() error {
	journalFile, err := r.journalDir.Find(r.offset)
	if err != nil {
		return err
	}
	r.file.Close()
	if r.journalDir.IsLast(journalFile) {
		r.file, err = openWatchedFile(journalFile.fileName, r.fileChanged)
	} else {
		r.file, err = os.Open(journalFile.fileName)
	}
	if err != nil {
		return err
	}
	r.r = bufio.NewReader(r.file)
	r.journalFile = journalFile
	return nil
}

func (r *Reader) Offset() uint64 {
	return r.offset
}

func (r *Reader) Close() error {
	err1 := r.journalDir.Close()
	err2 := r.file.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
