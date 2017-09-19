package shard

import (
	"os"
	"path"
	"path/filepath"
	"time"
)

type (
	// OpenShardFunc callback
	OpenShardFunc func(string)
)

// WatchInterval defines how long the watch polls for a new shard
var WatchInterval = time.Minute

// Watch watches the directory and calls open only once for each shard
func Watch(rootDir string, open OpenShardFunc) error {
	watcher := newShardWatcher(open)
	t := time.Now().UTC()
	for {
		if !dirExists(rootDir) {
			time.Sleep(WatchInterval)
			continue
		}
		watcher.poll(rootDir)
		subDirs, err := filepath.Glob(path.Join(rootDir, "*"))
		if err != nil {
			return err
		}
		for _, subDir := range subDirs {
			shard, err := parseShardDir(rootDir, subDir)
			if err != nil {
				return err
			}
			watcher.poll(shard.Dir(rootDir))
		}
		if delay := WatchInterval - time.Since(t); delay > 0 {
			time.Sleep(delay)
		}
		t = time.Now().UTC()
	}
}

type shardWatcher struct {
	dirs map[string]bool
	open OpenShardFunc
}

func newShardWatcher(open OpenShardFunc) shardWatcher {
	return shardWatcher{
		dirs: make(map[string]bool),
		open: open,
	}
}

func (w *shardWatcher) poll(dir string) {
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
