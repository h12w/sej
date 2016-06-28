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
	journalFile struct {
		startOffset uint64
		fileName    string
	}
	journalFiles []journalFile
)

const (
	journalFileExt = ".jnl"
)

func createNewJournalFile(dir string, offset uint64) (*os.File, error) {
	return os.Create(path.Join(dir, fmt.Sprintf("%016x"+journalFileExt, offset)))
}

func getJournalFiles(dir string) (files journalFiles, _ error) {
	f, err := openOrCreateDir(dir)
	if err != nil {
		return nil, err
	}
	allNames, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	for _, name := range allNames {
		if path.Ext(name) != journalFileExt {
			continue
		}
		offset, err := strconv.ParseUint(strings.TrimSuffix(path.Base(name), journalFileExt), 16, 64)
		if err != nil {
			continue
		}
		files = append(files, journalFile{
			startOffset: offset,
			fileName:    path.Join(dir, name),
		})
	}
	if len(files) == 0 {
		f, err := createNewJournalFile(dir, 0)
		if err != nil {
			return nil, err
		}
		f.Close()
		return getJournalFiles(dir)
	}
	sort.Sort(files)
	if len(files) == 0 {
		return nil, errors.New("no journal files found or created")
	}
	return files, nil
}

func (a journalFiles) Len() int           { return len(a) }
func (a journalFiles) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a journalFiles) Less(i, j int) bool { return a[i].startOffset < a[j].startOffset }

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

func (a journalFiles) isLast(f *journalFile) bool {
	return a[len(a)-1].startOffset == f.startOffset
}

func (a journalFiles) find(offset uint64) (*journalFile, error) {
	for i := 0; i < len(a)-1; i++ {
		if a[i].startOffset <= offset && offset < a[i+1].startOffset {
			return &a[i], nil
		}
	}
	if len(a) == 1 && a[0].startOffset <= offset {
		return &a[0], nil
	} else if a[len(a)-1].startOffset <= offset {
		return &a[len(a)-1], nil
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
