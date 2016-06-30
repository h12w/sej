package sej

import (
	"io"
	"os"
	"sync"

	"gopkg.in/fsnotify.v1"
)

type watchedJournalDir struct {
	dir      *journalDir
	watcher  *fsnotify.Watcher
	modified bool
	err      error
	mu       sync.RWMutex
	wg       sync.WaitGroup
	changed  chan bool
}

func openWatchedJournalDir(dir string, changed chan bool) (*watchedJournalDir, error) {
	dirFile, err := openOrCreateDir(dir)
	if err != nil {
		return nil, err
	}
	dirFile.Close()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := watcher.Add(dir); err != nil {
		watcher.Close()
		return nil, err
	}
	d := &watchedJournalDir{
		watcher: watcher,
		changed: changed,
	}
	d.wg.Add(2)
	go d.watchEvent()
	go d.watchError()
	d.dir, err = openJournalDir(dir)
	if err != nil {
		watcher.Close()
		return nil, err
	}
	return d, nil
}

func (d *watchedJournalDir) find(offset uint64) (*journalFile, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.err != nil {
		return nil, d.err
	}
	if err := d.reload(); err != nil {
		d.err = err
		return nil, err
	}
	return d.dir.find(offset)
}

func (d *watchedJournalDir) isLast(f *journalFile) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.reload(); err != nil {
		d.err = err
		return false
	}
	return d.dir.isLast(f)
}

func (d *watchedJournalDir) watchEvent() {
	defer d.wg.Done()
	for event := range d.watcher.Events {
		if event.Op&(fsnotify.Create|fsnotify.Remove) > 0 {
			d.mu.Lock()
			d.modified = true
			d.mu.Unlock()
			select {
			case d.changed <- true:
			default:
			}
		}
	}
}

func (d *watchedJournalDir) watchError() {
	defer d.wg.Done()
	for err := range d.watcher.Errors {
		d.mu.Lock()
		d.err = err
		d.mu.Unlock()
	}
}

func (d *watchedJournalDir) reload() error {
	if !d.modified {
		return nil
	}
	journalDir, err := openJournalDir(d.dir.path)
	if err != nil {
		return err
	}
	d.dir = journalDir
	d.modified = false
	return nil
}

func (d *watchedJournalDir) close() error {
	d.watcher.Remove(d.dir.path)
	d.watcher.Close()
	d.wg.Wait()
	return nil
}

type watchedFile struct {
	file     *os.File
	watcher  *fsnotify.Watcher
	modified bool
	err      error
	mu       sync.RWMutex
	wg       sync.WaitGroup
	changed  chan bool
}

func openWatchedFile(name string, changed chan bool) (*watchedFile, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := watcher.Add(name); err != nil {
		watcher.Close()
		return nil, err
	}
	f := &watchedFile{
		watcher: watcher,
		changed: changed,
	}
	f.wg.Add(2)
	go f.watchEvent()
	go f.watchError()
	f.file, err = os.Open(name)
	if err != nil {
		watcher.Close()
		return nil, err
	}
	return f, nil
}

func (f *watchedFile) watchEvent() {
	defer f.wg.Done()
	for event := range f.watcher.Events {
		if event.Op&(fsnotify.Write) > 0 {
			f.modified = true
			select {
			case f.changed <- true:
			default:
			}
		}
	}
}

func (f *watchedFile) watchError() {
	defer f.wg.Done()
	for err := range f.watcher.Errors {
		f.mu.Lock()
		f.err = err
		f.mu.Unlock()
	}
}

func (f *watchedFile) reopen() error {
	oldStat, err := f.file.Stat()
	if err != nil {
		return err
	}
	oldSize := oldStat.Size()
	fileName := f.file.Name()
	newFile, err := os.Open(fileName)
	if err != nil {
		return err
	}
	if _, err := newFile.Seek(oldSize, os.SEEK_SET); err != nil {
		newFile.Close()
		return err
	}
	if err := f.file.Close(); err != nil {
		return err
	}
	f.file = newFile
	f.modified = false
	return nil
}

func (f *watchedFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	n, err = f.file.Read(p)
	if err == io.EOF && f.modified {
		if nil != f.reopen() {
			return n, err
		}
		return f.file.Read(p)
	}
	return n, err
}

func (f *watchedFile) Close() error {
	f.watcher.Remove(f.file.Name())
	f.watcher.Close()
	f.wg.Wait()
	return nil
}
