package sej

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"
)

type Reader struct {
	dir    string
	offset uint64
	r      *bufio.Reader
	file   *os.File
}

func NewReader(dir string, offset uint64) (*Reader, error) {
	files, err := getJournalFiles(dir)
	if err != nil {
		return nil, err
	}
	file, err := files.find(offset)
	if err != nil {
		return nil, err
	}
	reader := Reader{
		dir: dir,
	}
	if err := reader.openFile(file.fileName); err != nil {
		return nil, err
	}
	reader.offset = file.startOffset
	for reader.offset < offset {
		if _, err := reader.Read(); err != nil {
			return nil, err
		}
	}
	if reader.offset != offset {
		return nil, fmt.Errorf("fail to find offset %d", offset)
	}
	return &reader, nil
}

func (r *Reader) Read() (msg []byte, err error) {
	var offset uint64
	for {
		msg, offset, err = readMessage(r.r)
		if err == io.EOF {
			time.Sleep(10 * time.Millisecond)
			files, err := getJournalFiles(r.dir)
			if err != nil {
				return nil, err
			}
			journalFile, err := files.find(r.offset)
			if err != nil {
				return nil, err
			}
			if r.file.Name() == journalFile.fileName {
				if err := r.reopenFile(); err != nil {
					return nil, err
				}
				continue
			} else {
				r.closeFile()
				if err := r.openFile(journalFile.fileName); err != nil {
					return nil, err
				}
				continue
			}
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

func (r *Reader) Offset() uint64 {
	return r.offset
}

func (r *Reader) Close() {
	r.closeFile()
}

func (r *Reader) closeFile() {
	if r.file != nil {
		r.file.Close()
		r.file = nil
		r.r = nil
	}
}

func (r *Reader) openFile(name string) error {
	var err error
	r.file, err = os.Open(name)
	if err != nil {
		return err
	}
	r.r = bufio.NewReader(r.file)
	return nil
}

func (r *Reader) reopenFile() error {
	if err := reopenFile(r.file); err != nil {
		return err
	}
	r.r = bufio.NewReader(r.file)
	return nil
}

func reopenFile(file *os.File) error {
	fileName := file.Name()
	offset, err := file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	newFile, err := os.Open(fileName)
	if err != nil {
		return err
	}
	if _, err := newFile.Seek(offset, os.SEEK_SET); err != nil {
		newFile.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	*file = *newFile
	return nil
}
