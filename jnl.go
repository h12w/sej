package fq

import (
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

func getJournalFiles(dir string) (files journalFiles, _ error) {
	f, err := os.Open(dir)
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
	sort.Sort(files)
	return files, nil
}

func (a journalFiles) Len() int           { return len(a) }
func (a journalFiles) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a journalFiles) Less(i, j int) bool { return a[i].startOffset < a[j].startOffset }
