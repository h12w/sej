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
		FirstOffset uint64
		FileName    string
	}
	JournalFiles []JournalFile
	JournalDir   struct {
		path  string
		Files JournalFiles
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
	var files JournalFiles
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
		Files: files,
		path:  dir,
	}, nil
}

func (a JournalFiles) Len() int           { return len(a) }
func (a JournalFiles) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a JournalFiles) Less(i, j int) bool { return a[i].FirstOffset < a[j].FirstOffset }

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
	return &d.Files[len(d.Files)-1]
}

func (d *JournalDir) First() *JournalFile {
	return &d.Files[0]
}

func (d *JournalDir) isLast(f *JournalFile) bool {
	return d.Files[len(d.Files)-1].FirstOffset == f.FirstOffset
}

func (d *JournalDir) find(offset uint64) (*JournalFile, error) {
	if offset < d.Files[0].FirstOffset {
		// offset is too small, return the first journal file
		return &d.Files[0], nil
	}
	for i := 0; i < len(d.Files)-1; i++ {
		if d.Files[i].FirstOffset <= offset && offset < d.Files[i+1].FirstOffset {
			return &d.Files[i], nil
		}
	}
	// return the last journal file
	return &d.Files[len(d.Files)-1], nil
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
		FirstOffset: offset,
		FileName:    path.Join(dir, name),
	}, nil
}

func JournalDirPath(dir string) string {
	return path.Join(dir, "jnl")
}
