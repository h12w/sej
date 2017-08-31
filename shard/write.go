package shard

import (
	"errors"
	"fmt"
	"os"
	"path"

	"h12.me/sej"
)

type (
	Writer struct {
		ws        []*sej.Writer
		shard     ShardFunc
		shardMask uint32
	}
	ShardFunc func(*sej.Message) uint32
)

func NewWriter(dir string, shardBit uint, shardFunc ShardFunc) (*Writer, error) {
	if shardBit > 16 {
		return nil, errors.New("shardBit should be less than 16")
	}
	writer := Writer{
		ws:        make([]*sej.Writer, 1<<shardBit),
		shard:     shardFunc,
		shardMask: 1<<shardBit - 1,
	}
	for i := range writer.ws {
		var err error
		writer.ws[i], err = sej.NewWriter(shardDir(dir, i))
		if err != nil {
			writer.Close()
			return nil, err
		}
	}
	return &writer, nil
}

func shardDir(rootDir string, shardIndex int) string {
	dir := path.Join(rootDir, "shd", fmt.Sprintf("%02x", shardIndex))
	os.MkdirAll(dir, 0755)
	return dir
}

func (w *Writer) Append(msg sej.Message) error {
	return w.ws[int(w.shard(&msg)&w.shardMask)].Append(msg)
}

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

func (w *Writer) ShardCount() int {
	return len(w.ws)
}
