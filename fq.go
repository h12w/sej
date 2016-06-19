package fq

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
)

type (
	Writer struct {
		dir    string
		offset uint64
		f      *os.File
		buf    *bufio.Writer
	}
	Reader struct{}
)

func NewWriter(dir string) (*Writer, error) {
	var err error
	w := Writer{dir: dir}
	dirObj, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	names, err := dirObj.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	if len(names) > 0 {
		//sort.Strings(names)
	} else {
		w.offset = 0
		w.f, err = os.Create(path.Join(dir, fmt.Sprintf("%0000000000000000x.jnl", w.offset)))
		if err != nil {
			return nil, err
		}
		w.buf = bufio.NewWriter(w.f)
	}
	return &w, nil
}

func (w *Writer) Append(msg []byte) (offset uint64, err error) {
	//crc := crc32.ChecksumIEEE(msg)

	return 0, nil
}

func (w *Writer) writeUint64(uint64) {

}

func (w *Writer) Flush(offset uint64) error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *Writer) Close() {
	w.Flush(w.offset)
	w.f.Close()
}

func NewReader(dir string, offset uint64) (*Reader, error) {
	return &Reader{}, nil
}

func (r *Reader) Read() (msg []byte, err error) {
	return nil, io.EOF
}

func (r *Reader) Offset() uint64 {
	return 0
}

func (r *Reader) Close() {}
