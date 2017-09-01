package shard

import (
	"errors"
	"fmt"
	"path"

	"h12.me/sej"
)

type (
	// Writer is a meta writer of multiple sharded sej.Writers
	Writer struct {
		ws        []*sej.Writer
		shard     HashFunc
		shardMask uint16
	}
	// HashFunc gives a shard index based on a message (probably its key)
	HashFunc func(*sej.Message) uint16
)

// NewWriter creates a meta writer for writing to multiple shards under $dir/jnl/shd/$shardMask
// shardBit is the number of bits used in the shard index
// the number of shards is 1<<shardBit
// the shard mask is 1<<shardBit - 1
func NewWriter(dir string, shardBit uint, shardFunc HashFunc) (*Writer, error) {
	if shardBit > 10 {
		return nil, errors.New("shardBit should be no more than 10")
	}
	writer := Writer{
		ws:        make([]*sej.Writer, 1<<shardBit),
		shard:     shardFunc,
		shardMask: 1<<shardBit - 1,
	}
	for i := range writer.ws {
		var err error
		writer.ws[i], err = sej.NewWriter(shardDir(dir, writer.shardMask, i))
		if err != nil {
			writer.Close()
			return nil, err
		}
	}
	return &writer, nil
}

func shardDir(rootDir string, shardMask uint16, shardIndex int) string {
	dir := path.Join(rootDir, "shd", fmt.Sprintf("%03x", shardMask), fmt.Sprintf("%03x", shardIndex))
	return dir
}

// Append appends a message to a shard
func (w *Writer) Append(msg *sej.Message) error {
	return w.ws[int(w.shard(msg)&w.shardMask)].Append(msg)
}

// Flush flushes all shards
func (w *Writer) Flush() error {
	var es []error
	for _, w := range w.ws {
		if err := w.Flush(); err != nil {
			es = append(es, err)
		}
	}
	if len(es) > 0 {
		return errors.New(fmt.Sprint(es))
	}
	return nil
}

// Close closes all shards
func (w *Writer) Close() error {
	var es []error
	for i := range w.ws {
		if w.ws[i] != nil {
			if err := w.ws[i].Close(); err != nil {
				es = append(es, err)
			}
			w.ws[i] = nil
		}
	}
	if len(es) > 0 {
		return errors.New(fmt.Sprint(es))
	}
	return nil
}
