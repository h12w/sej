package sej

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"
)

type Reader struct {
	dir         string
	offset      uint64
	r           *bufio.Reader
	file        io.ReadCloser
	journalDir  *watchedJournalDir
	journalFile *journalFile
}

func NewReader(dir string, offset uint64) (*Reader, error) {
	journalDir, err := openWatchedJournalDir(dir)
	if err != nil {
		return nil, err
	}
	journalFile, err := journalDir.find(offset)
	if err != nil {
		return nil, err
	}
	reader := Reader{
		dir: dir,
	}
	isLast, err := journalDir.isLast(journalFile)
	if err != nil {
		return nil, err
	}
	if isLast {
		reader.file, err = openTailFile(journalFile.fileName)
	} else {
		reader.file, err = os.Open(journalFile.fileName)
	}
	if err != nil {
		return nil, err
	}
	reader.r = bufio.NewReader(reader.file)
	reader.offset = journalFile.startOffset
	reader.journalFile = journalFile
	reader.journalDir = journalDir
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
			isLast, err := r.journalDir.isLast(r.journalFile)
			if err != nil {
				return nil, err
			}
			if isLast {
				time.Sleep(10 * time.Millisecond)
				continue // wait for append or new file
			}
			journalFile, err := r.journalDir.find(r.offset)
			if err != nil {
				return nil, err
			}
			r.closeFile()
			isLast, err = r.journalDir.isLast(r.journalFile)
			if err != nil {
				return nil, err
			}
			if isLast {
				r.file, err = openTailFile(journalFile.fileName)
			} else {
				r.file, err = os.Open(journalFile.fileName)
			}
			if err != nil {
				return nil, err
			}
			r.r = bufio.NewReader(r.file)
			r.journalFile = journalFile
			time.Sleep(10 * time.Millisecond)
			continue // try to read new file
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

type tailFile struct {
	*os.File
}

func openTailFile(file string) (*tailFile, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	return &tailFile{
		File: f,
	}, nil
}

func (f *tailFile) Read(p []byte) (n int, err error) {
	n, err = f.File.Read(p)
	if err == io.EOF && n == 0 {
		oldStat, err := f.File.Stat()
		if err != nil {
			return n, io.EOF
		}
		oldSize := oldStat.Size()
		fileName := f.File.Name()
		newFile, err := os.Open(fileName)
		if err != nil {
			return n, io.EOF
		}
		newStat, err := newFile.Stat()
		if err != nil {
			return n, io.EOF
		}
		newSize := newStat.Size()
		if newSize <= oldSize {
			newFile.Close()
			return n, io.EOF
		}
		if _, err := newFile.Seek(oldSize, os.SEEK_SET); err != nil {
			newFile.Close()
			return n, io.EOF
		}
		if err := f.File.Close(); err != nil {
			return n, io.EOF
		}
		f.File = newFile
		n, err = f.File.Read(p)
	} else if err != nil {
		return
	}
	return
}
