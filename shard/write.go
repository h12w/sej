package shard

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"h12.me/sej"
)

type (
	// Writer is a meta writer of multiple sharded sej.Writers
	Writer struct {
		ws        []sejWriterPtr
		shard     HashFunc
		shardMask uint16
	}
	sejWriterPtr struct {
		dir         string
		p           *sej.Writer
		mu          sync.Mutex
		initialized uint32
	}
	// HashFunc gives a shard index based on a message (probably its key)
	HashFunc func(*sej.Message) uint16
)

// NewWriter creates a meta writer for writing to multiple shards under $dir/jnl/shd/$shardMask
// shardBit is the number of bits used in the shard index
// the number of shards is 1<<shardBit
// the shard mask is 1<<shardBit - 1
func NewWriter(shardPath Path, shardFunc HashFunc) (*Writer, error) {
	if err := shardPath.check(); err != nil {
		return nil, err
	}
	if shardFunc == nil {
		shardFunc = dummyShardFunc // no sharding
	}
	writer := Writer{
		ws:        make([]sejWriterPtr, shardPath.shardCount()),
		shard:     shardFunc,
		shardMask: shardPath.shardMask(),
	}
	for i := range writer.ws {
		writer.ws[i] = sejWriterPtr{
			dir: shardPath.dir(i),
		}
	}
	return &writer, nil
}

func (w *sejWriterPtr) get() *sej.Writer {
	if atomic.LoadUint32(&w.initialized) == 1 {
		return w.p
	}
	return nil
}

func (w *sejWriterPtr) getOrOpen() (*sej.Writer, error) {
	if atomic.LoadUint32(&w.initialized) == 1 {
		return w.p, nil
	}

	// slow path
	w.mu.Lock()
	if w.initialized == 1 {
		w.mu.Unlock()
		return w.p, nil
	}
	var err error
	w.p, err = sej.NewWriter(w.dir)
	if err != nil {
		w.mu.Unlock()
		return nil, err
	}
	atomic.StoreUint32(&w.initialized, 1)
	w.mu.Unlock()

	return w.p, nil
}

func dummyShardFunc(*sej.Message) uint16 { return 0 }

func (w *Writer) SEJWriter(msg *sej.Message) (*sej.Writer, error) {
	return w.ws[int(w.shard(msg)&w.shardMask)].getOrOpen()
}

// Append appends a message to a shard
func (w *Writer) Append(msg *sej.Message) error {
	writer, err := w.SEJWriter(msg)
	if err != nil {
		return err
	}
	return writer.Append(msg)
}

// Flush flushes all opened shards
func (w *Writer) Flush() error {
	var es []error
	for i := range w.ws {
		writer := w.ws[i].get()
		if writer == nil {
			continue
		}
		if err := writer.Flush(); err != nil {
			es = append(es, err)
		}
	}
	if len(es) > 0 {
		return errors.New(fmt.Sprint(es))
	}
	return nil
}

// Close closes all opened shards
func (w *Writer) Close() error {
	var es []error
	for i := range w.ws {
		writer := w.ws[i].get()
		if writer == nil {
			continue
		}
		if err := writer.Close(); err != nil {
			es = append(es, err)
		}
	}
	if len(es) > 0 {
		return errors.New(fmt.Sprint(es))
	}
	return nil
}
