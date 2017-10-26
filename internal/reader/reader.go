// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bufio implements buffered I/O.  It wraps an io.Reader or io.Writer
// object, creating another object (Reader or Writer) that also implements
// the interface but provides buffering and some help for textual I/O.
package reader

import (
	"errors"
	"io"
	"os"
)

const (
	defaultBufSize = 4096
)

// Buffered input.

// Reader implements buffering for an io.Reader object.
type Reader struct {
	buf  []byte
	rd   io.ReadSeeker // reader provided by the client
	r, w int           // buf read and write positions
	ar   int           // absolute read position
	err  error
}

const minReadBufferSize = 16

// NewReaderSize returns a new Reader whose buffer has at least the specified
// size. If the argument io.Reader is already a Reader with large enough
// size, it returns the underlying Reader.
func NewReaderSize(rd io.ReadSeeker, size int) *Reader {
	// Is it already a Reader?
	b, ok := rd.(*Reader)
	if ok && len(b.buf) >= size {
		return b
	}
	if size < minReadBufferSize {
		size = minReadBufferSize
	}
	r := new(Reader)
	r.reset(make([]byte, size), rd)
	return r
}

// NewReader returns a new Reader whose buffer has the default size.
func NewReader(rd io.ReadSeeker) *Reader {
	return NewReaderSize(rd, defaultBufSize)
}

func (b *Reader) reset(buf []byte, r io.ReadSeeker) {
	*b = Reader{
		buf: buf,
		rd:  r,
	}
}

var errNegativeRead = errors.New("bufio: reader returned negative count from Read")

func (b *Reader) readErr() error {
	err := b.err
	b.err = nil
	return err
}

// Read reads data into p.
// It returns the number of bytes read into p.
// The bytes are taken from at most one Read on the underlying Reader,
// hence n may be less than len(p).
// At EOF, the count will be zero and err will be io.EOF.
func (b *Reader) Read(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		return 0, b.readErr()
	}
	if b.r == b.w {
		if b.err != nil {
			return 0, b.readErr()
		}
		if len(p) >= len(b.buf) {
			// Large read, empty buffer.
			// Read directly into p to avoid copy.
			n, b.err = b.rd.Read(p)
			if n < 0 {
				panic(errNegativeRead)
			}
			b.ar += n
			return n, b.readErr()
		}
		// One read.
		// Do not use b.fill, which will loop.
		b.r = 0
		b.w = 0
		n, b.err = b.rd.Read(b.buf)
		if n < 0 {
			panic(errNegativeRead)
		}
		if n == 0 {
			return 0, b.readErr()
		}
		b.w += n
	}

	// copy as much as we can
	n = copy(p, b.buf[b.r:b.w])
	b.r += n
	b.ar += n
	return n, nil
}

func (b *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case os.SEEK_CUR:
		return b.seekCur(int(offset))
	case os.SEEK_SET:
		return b.seekSet(int(offset))
	default: // os.SEEK_END:
		panic("unsupported whence")
	}
}

func (b *Reader) seekSet(pos int) (int64, error) {
	if pos < 0 {
		return int64(b.ar), errors.New("negative position")
	}
	ar, aw := b.ar, b.aw()
	if ar <= pos && pos <= aw {
		roffset := pos - ar
		b.r += roffset
		b.ar += roffset
	} else {
		newPos, err := b.rd.Seek(int64(pos), os.SEEK_SET)
		if err != nil {
			b.ar = int(newPos)
			b.r = 0
			b.w = 0
			return int64(b.ar), err
		}
		b.r = 0
		b.w = 0
		b.ar = pos
	}
	return int64(b.ar), nil
}

func (b *Reader) seekCur(offset int) (int64, error) {
	return b.seekSet(b.ar + offset)
}

// aw is absolute write position, i.e. file position
func (b *Reader) aw() int {
	return b.ar + (b.w - b.r)
}
