package sej

import (
	"sync"

	"gopkg.in/fsnotify.v1"
)

type watchedJournalDir struct {
	watcher *fsnotify.Watcher
	dirPath string
	dir     *journalDir
	err     error
	mu      sync.RWMutex
	wg      sync.WaitGroup
}

func openWatchedJournalDir(dir string) (*watchedJournalDir, error) {
	journalDir, err := openJournalDir(dir)
	if err != nil {
		return nil, err
	}
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
		dirPath: dir,
		dir:     journalDir,
	}
	d.wg.Add(2)
	go d.watchEvent()
	go d.watchError()
	return d, nil
}

func (d *watchedJournalDir) find(offset uint64) (*journalFile, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.err != nil {
		return nil, d.err
	}
	return d.dir.find(offset)
}

func (d *watchedJournalDir) isLast(f *journalFile) (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.err != nil {
		return false, d.err
	}
	return d.dir.isLast(f), nil
}

func (d *watchedJournalDir) watchEvent() {
	defer d.wg.Done()
	for event := range d.watcher.Events {
		if event.Op&(fsnotify.Create|fsnotify.Remove) > 0 {
			d.reload()
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

func (d *watchedJournalDir) reload() {
	d.mu.Lock()
	defer d.mu.Unlock()
	journalDir, err := openJournalDir(d.dirPath)
	if err != nil {
		d.err = err
	}
	d.dir = journalDir
}

func (d *watchedJournalDir) close() error {
	d.watcher.Remove(d.dirPath)
	d.watcher.Close()
	d.wg.Wait()
	return nil
}
