package sej

import (
	"io"
	"os"
	"sync"

	"gopkg.in/fsnotify.v1"
)

type watchedJournalDir struct {
	dir     *journalDir
	watcher *changeWatcher
}

func openWatchedJournalDir(dir string, changed chan bool) (*watchedJournalDir, error) {
	dirFile, err := openOrCreateDir(dir)
	if err != nil {
		return nil, err
	}
	dirFile.Close()
	watcher, err := newChangeWatcher(dir, fsnotify.Create|fsnotify.Remove, changed)
	if err != nil {
		return nil, err
	}
	journalDir, err := openJournalDir(dir)
	if err != nil {
		watcher.Close()
		return nil, err
	}
	return &watchedJournalDir{
		dir:     journalDir,
		watcher: watcher,
	}, nil
}

func (d *watchedJournalDir) Find(offset uint64) (*journalFile, error) {
	if err := d.watcher.Err(); err != nil {
		return nil, err
	}
	if err := d.watcher.Reset(d.reload); err != nil {
		return nil, err
	}
	return d.dir.find(offset)
}

func (d *watchedJournalDir) IsLast(f *journalFile) bool {
	if err := d.watcher.Err(); err != nil {
		return true
	}
	if err := d.watcher.Reset(d.reload); err != nil {
		return true
	}
	return d.dir.isLast(f)
}
func (d *watchedJournalDir) reload() error {
	journalDir, err := openJournalDir(d.dir.path)
	if err != nil {
		return err
	}
	d.dir = journalDir
	return nil
}

func (d *watchedJournalDir) Close() error {
	return d.watcher.Close()
}

type watchedFile struct {
	file    *os.File
	watcher *changeWatcher
}

func openWatchedFile(name string, changed chan bool) (*watchedFile, error) {
	watcher, err := newChangeWatcher(name, fsnotify.Write, changed)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(name)
	if err != nil {
		watcher.Close()
		return nil, err
	}
	return &watchedFile{
		file:    file,
		watcher: watcher,
	}, nil
}

func (f *watchedFile) Read(p []byte) (n int, err error) {
	if err := f.watcher.Err(); err != nil {
		return 0, err
	}
	n, err = f.file.Read(p)
	if err == io.EOF {
		if err := f.watcher.Reset(f.reopen); err != nil {
			return n, err
		}
		return f.file.Read(p)
	}
	return n, err
}
func (f *watchedFile) reopen() error {
	oldOffset, err := f.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	newFile, err := os.Open(f.file.Name())
	if _, err := newFile.Seek(oldOffset, os.SEEK_SET); err != nil {
		newFile.Close()
		return err
	}
	if err := f.file.Close(); err != nil {
		return err
	}
	f.file = newFile
	return nil
}

func (f *watchedFile) Close() error {
	err1 := f.file.Close()
	err2 := f.watcher.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

type changeWatcher struct {
	watcher   *fsnotify.Watcher
	watchedOp fsnotify.Op
	changed   bool
	err       error
	mu        sync.RWMutex
	wg        sync.WaitGroup
	changedCh chan bool
}

func newChangeWatcher(name string, op fsnotify.Op, changedCh chan bool) (*changeWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := watcher.Add(name); err != nil {
		watcher.Close()
		return nil, err
	}
	w := &changeWatcher{
		watcher:   watcher,
		watchedOp: fsnotify.Write,
		changedCh: changedCh,
	}
	w.wg.Add(2)
	go w.watchEvent()
	go w.watchError()
	return w, nil
}

func (f *changeWatcher) watchEvent() {
	defer f.wg.Done()
	for event := range f.watcher.Events {
		if event.Op&(f.watchedOp) > 0 {
			f.mu.Lock()
			f.changed = true
			f.mu.Unlock()
			select {
			case f.changedCh <- true:
			default:
			}
		}
	}
}

func (f *changeWatcher) watchError() {
	defer f.wg.Done()
	for err := range f.watcher.Errors {
		f.mu.Lock()
		f.err = err
		f.mu.Unlock()
	}
}

func (w *changeWatcher) Err() error {
	w.mu.RLock()
	err := w.err
	w.mu.RUnlock()
	return err
}

func (w *changeWatcher) Reset(update func() error) error {
	w.mu.Lock()
	if err := update(); err != nil {
		w.mu.Unlock()
		return err
	}
	w.changed = false
	w.mu.Unlock()
	return nil
}

func (f *changeWatcher) Close() error {
	f.watcher.Close()
	f.wg.Wait()
	return nil
}
