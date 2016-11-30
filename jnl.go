package sej

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type (
	// JournalFile represents a unopened journal file
	JournalFile struct {
		StartOffset uint64
		FileName    string
	}
	journalFiles []JournalFile
	JournalDir   struct {
		path  string
		files journalFiles
	}
)

const (
	journalExt = ".jnl"
)

func OpenJournalDir(dir string) (*JournalDir, error) {
	dirFile, err := openOrCreateDir(dir)
	if err != nil {
		return nil, err
	}
	defer dirFile.Close()
	allNames, err := dirFile.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	var files journalFiles
	for _, name := range allNames {
		if path.Ext(name) != journalExt {
			continue
		}
		journalFile, err := ParseJournalFileName(dir, name)
		if err != nil {
			continue
		}
		files = append(files, *journalFile)
	}
	if len(files) == 0 {
		f, err := openOrCreate(journalFileName(dir, 0))
		if err != nil {
			return nil, err
		}
		f.Close()
		return OpenJournalDir(dir)
	}
	sort.Sort(files)
	if len(files) == 0 {
		return nil, errors.New("no journal files found or created")
	}
	return &JournalDir{
		files: files,
		path:  dir,
	}, nil
}

func (a journalFiles) Len() int           { return len(a) }
func (a journalFiles) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a journalFiles) Less(i, j int) bool { return a[i].StartOffset < a[j].StartOffset }

func openOrCreateDir(dir string) (*os.File, error) {
	f, err := os.Open(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if nil != os.MkdirAll(dir, 0755) {
			return nil, err
		}
		f, err = os.Open(dir)
	}
	return f, err
}

func (d *JournalDir) Last() *JournalFile {
	return &d.files[len(d.files)-1]
}

func (d *JournalDir) First() *JournalFile {
	return &d.files[0]
}

func (d *JournalDir) isLast(f *JournalFile) bool {
	return d.files[len(d.files)-1].StartOffset == f.StartOffset
}

func (d *JournalDir) find(offset uint64) (*JournalFile, error) {
	for i := 0; i < len(d.files)-1; i++ {
		if d.files[i].StartOffset <= offset && offset < d.files[i+1].StartOffset {
			return &d.files[i], nil
		}
	}
	if len(d.files) == 1 && d.files[0].StartOffset <= offset {
		return &d.files[0], nil
	} else if d.files[len(d.files)-1].StartOffset <= offset {
		return &d.files[len(d.files)-1], nil
	}
	return nil, errors.New("offset is too small")
}

// TODO: use binary search as an optimization
/*
i := sort.Search(len(files), func(i int) bool { return files[i].startOffset > offset })
if i == 0 {
	return nil, errors.New("offset is too small")
}

journalIndex := i - 1
*/

func journalFileName(dir string, offset uint64) string {
	// maximum of 64-bit offset is 7fff,ffff,ffff,ffff
	return path.Join(dir, fmt.Sprintf("%016x"+journalExt, offset))
}

// ParseJournalFileName parses a journal file name and returns an JournalFile object
func ParseJournalFileName(dir, name string) (*JournalFile, error) {
	offset, err := strconv.ParseUint(strings.TrimSuffix(path.Base(name), journalExt), 16, 64)
	if err != nil {
		return nil, err
	}
	return &JournalFile{
		StartOffset: offset,
		FileName:    path.Join(dir, name),
	}, nil
}

func JournalDirPath(dir string) string {
	return path.Join(dir, "jnl")
}
