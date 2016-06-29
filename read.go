package sej

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	notifyTimeout = 10 * time.Millisecond
)

type Reader struct {
	offset      uint64
	r           *bufio.Reader
	file        io.ReadCloser
	journalDir  *watchedJournalDir
	journalFile *journalFile
	changed     chan bool
}

func NewReader(dir string, offset uint64) (*Reader, error) {
	r := Reader{
		changed: make(chan bool),
	}
	journalDir, err := openWatchedJournalDir(dir, r.changed)
	if err != nil {
		return nil, err
	}
	journalFile, err := journalDir.find(offset)
	if err != nil {
		return nil, err
	}
	if journalDir.isLast(journalFile) {
		r.file, err = openWatchedFile(journalFile.fileName, r.changed)
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
			if r.journalDir.isLast(r.journalFile) {
				select {
				case <-r.changed:
					// fmt.Println("changed")
				case <-time.After(notifyTimeout):
					// fmt.Println("timeout")
				}
				continue
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
	journalFile, err := r.journalDir.find(r.offset)
	if err != nil {
		return err
	}
	r.closeFile()
	if r.journalDir.isLast(journalFile) {
		r.file, err = openWatchedFile(journalFile.fileName, r.changed)
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

func (r *Reader) Close() {
	r.journalDir.close()
	r.closeFile()
}

func (r *Reader) closeFile() {
	if r.file != nil {
		r.file.Close()
		r.file = nil
		r.r = nil
	}
}
