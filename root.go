package sej

import (
	"os"
	"path"
	"path/filepath"
	"time"
)

type (
	// OpenFunc callback
	OpenFunc func(journalDir string)
)

// WatchRootDir watches the directory and calls open only once for each shard
func WatchRootDir(rootDir string, watchInterval time.Duration, open OpenFunc) error {
	watcher := newDirWatcher(open)
	t := time.Now().UTC()
	for {
		if !dirExists(rootDir) {
			time.Sleep(watchInterval)
			continue
		}
		watcher.poll(rootDir)
		subDirs, err := filepath.Glob(path.Join(rootDir, "*"))
		if err != nil {
			return err
		}
		for _, subDir := range subDirs {
			watcher.poll(subDir)
		}
		if delay := watchInterval - time.Since(t); delay > 0 {
			time.Sleep(delay)
		}
		t = time.Now().UTC()
	}
}

type dirWatcher struct {
	dirs map[string]bool
	open OpenFunc
}

func newDirWatcher(open OpenFunc) dirWatcher {
	return dirWatcher{
		dirs: make(map[string]bool),
		open: open,
	}
}

func (w *dirWatcher) poll(dir string) {
	if w.dirs[dir] {
		return
	}
	if !dirExists(path.Join(dir, "jnl")) {
		return
	}

	// set the guard and go
	w.dirs[dir] = true
	w.open(dir)
}

func dirExists(dir string) bool {
	s, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		return false
	}
	return s.IsDir()
}
