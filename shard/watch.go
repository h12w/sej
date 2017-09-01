package shard

import (
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"
)

type (
	// Shard contains the shard info for opening
	Shard struct {
		Bit   uint   // number of bits that the shard index contains
		Index int    // shard index
		Dir   string // shard directory
	}
	// OpenShardFunc callback
	OpenShardFunc func(*Shard)
)

// WatchInterval defines how long the watch polls for a new shard
var WatchInterval = time.Minute

// Watch watches the directory and calls open only once for each shard
func Watch(dir string, open OpenShardFunc) error {
	watcher := newShardWatcher(dir, open)
	t := time.Now().UTC()
	for {
		watcher.poll(&Shard{Dir: dir})
		maskDirs, err := filepath.Glob(path.Join(dir, "shd", "*"))
		if err != nil {
			return err
		}
		for _, maskDir := range maskDirs {
			shardBit, err := strconv.ParseUint(path.Base(maskDir), 16, 8)
			if err != nil {
				return err
			}
			shardCount := int(1 << shardBit)
			for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
				watcher.poll(&Shard{Bit: uint(shardBit), Index: shardIndex, Dir: shardDir(dir, uint(shardBit), shardIndex)})
			}
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

func newShardWatcher(dir string, open OpenShardFunc) shardWatcher {
	return shardWatcher{
		dirs: make(map[string]bool),
		open: open,
	}
}

func (w *shardWatcher) poll(shard *Shard) {
	if w.dirs[shard.Dir] {
		return
	}
	if !dirExists(path.Join(shard.Dir, "jnl")) {
		return
	}

	// set the guard and go
	w.dirs[shard.Dir] = true
	w.open(shard)
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
