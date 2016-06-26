package fq

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

type Reader struct {
	dir          string
	offset       uint64
	r            *bufio.Reader
	file         *os.File
	journalFiles journalFiles
	journalIndex int
}

func NewReader(dir string, offset uint64) (*Reader, error) {
	files, err := getJournalFiles(dir)
	if err != nil {
		return nil, err
	}
	i := sort.Search(len(files), func(i int) bool { return files[i].startOffset > offset })
	if i == 0 {
		return nil, errors.New("offset is too small")
	}

	journalIndex := i - 1
	file := &files[journalIndex]
	reader := Reader{
		dir:          dir,
		journalFiles: files,
		journalIndex: journalIndex,
	}
	if err := reader.openFile(file.fileName); err != nil {
		return nil, err
	}
	reader.r = bufio.NewReader(reader.file)
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
	msg, offset, err := readMessage(r.r)
	if err == io.EOF {
		/*
			watcher, err := fsnotify.NewWatcher()
			if err != nil {
				return nil, err
			}
		*/
		files, err := getJournalFiles(r.dir)
		if err != nil {
			return nil, err
		}

		r.journalFiles = files
		if r.journalIndex < len(r.journalFiles)-1 && r.offset == r.journalFiles[r.journalIndex+1].startOffset {
			r.closeFile()
			r.journalIndex++
			journalFile := &r.journalFiles[r.journalIndex]
			if err := r.openFile(journalFile.fileName); err != nil {
				return nil, err
			}
			r.r = bufio.NewReader(r.file)
			return r.Read()
		}
		if err := r.reopenFile(); err != nil {
			return nil, err
		}
		msg, offset, err := readMessage(r.r)
		if err == io.EOF && offset == r.offset+1 {
			return msg, nil
		} else if err != nil {
			return nil, err
		}
		if offset != r.offset {
			return nil, fmt.Errorf("offset is out of order: %d, %d", offset, r.offset)
		}
		return msg, nil
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
	r.file.Close()
}

func (r *Reader) openFile(name string) error {
	var err error
	r.file, err = os.Open(name)
	if err != nil {
		return err
	}
	return nil
}

func (r *Reader) reopenFile() error {
	fileName := r.file.Name()
	fileOffset, err := r.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	r.closeFile()
	if err := r.openFile(fileName); err != nil {
		return err
	}
	if _, err := r.file.Seek(fileOffset, os.SEEK_SET); err != nil {
		return err
	}
	r.r = bufio.NewReader(r.file)
	return nil
}

/*
func (r *Reader) waitForFileAppend() error {
	r.watcher.Add(r.file.Name())
	defer r.watcher.Remove(r.file.Name())
	select {
	case event := <-r.watcher.Events:
		if event.Op&fsnotify.Write == fsnotify.Write {
			return r.reopenFile()
		}
	case err := <-r.watcher.Errors:
		return err
	}
	return nil
}
*/
