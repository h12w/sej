package shard

import (
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type (
	// OpenShardFunc callback
	OpenShardFunc func(string)
)

// WatchInterval defines how long the watch polls for a new shard
var WatchInterval = time.Minute

// Watch watches the directory and calls open only once for each shard
func Watch(dir string, open OpenShardFunc) error {
	watcher := newShardWatcher(open)
	t := time.Now().UTC()
	for {
		if !dirExists(dir) {
			time.Sleep(WatchInterval)
			continue
		}
		watcher.poll(dir)
		maskDirs, err := filepath.Glob(path.Join(dir, "*"))
		if err != nil {
			return err
		}
		for _, maskDir := range maskDirs {
			shard, err := parseShardDir(maskDir)
			if err != nil {
				return err
			}
			watcher.poll(shard.Dir(dir))
		}
		if delay := WatchInterval - time.Since(t); delay > 0 {
			time.Sleep(delay)
		}
		t = time.Now().UTC()
	}
}

func parseShardDir(dir string) (Shard, error) {
	base := path.Base(dir)
	parts := strings.Split(base, ".")
	switch len(parts) {
	case 1:
		return Shard{Prefix: parts[0]}, nil
	case 3:
		shardBit, err := strconv.ParseUint(parts[1], 16, 8)
		if err != nil {
			return Shard{}, errors.Wrap(err, "fail to parse shard bit")
		}
		shardIndex, err := strconv.ParseUint(parts[2], 16, 16)
		if err != nil {
			return Shard{}, errors.Wrap(err, "fail to parse shard index")
		}
		return Shard{
			Prefix: parts[0],
			Bit:    uint8(shardBit),
			Index:  int(shardIndex),
		}, nil
	default:
		return Shard{}, errors.New("fail to parse shard dir " + dir)
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
