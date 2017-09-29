package hub

import (
	"path"
	"regexp"
	"sync"

	"github.com/pkg/errors"
	"h12.me/sej"
)

type writers struct {
	dir string
	m   map[string]*sej.Writer
	mu  sync.Mutex
}

func newWriters(dir string) *writers {
	return &writers{
		dir: dir,
		m:   make(map[string]*sej.Writer),
	}
}

var (
	rxClientID   = regexp.MustCompile(`[0-9a-zA-Z_\-]`)
	rxJournalDir = regexp.MustCompile(`[0-9a-zA-Z_\-\.]`)
)

func (w *writers) Writer(clientID, journalDir string) (*sej.Writer, error) {
	if !rxClientID.MatchString(clientID) {
		return nil, errors.New("invalid clientID " + clientID)
	}
	if !rxJournalDir.MatchString(journalDir) {
		return nil, errors.New("invalid journalDir " + journalDir)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	key := clientID + "." + journalDir
	writer, ok := w.m[key]
	if !ok {
		var err error
		writer, err = sej.NewWriter(path.Join(w.dir, key))
		if err != nil {
			return nil, err
		}
		w.m[key] = writer
	}
	return writer, nil
}

func (w *writers) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for key, writer := range w.m {
		writer.Close()
		delete(w.m, key)
	}
	return nil
}
