package proto

import (
	"bytes"
	"encoding/gob"
	"io"
	"math"

	"github.com/pkg/errors"
	"h12.me/sej"
)

type (
	Request struct {
		Title    RequestTitle
		Header   interface{}
		Messages Messages
	}
	Messages []sej.Message

	RequestTitle struct {
		Verb     uint8
		ClientID string
	}
	RequestVerb uint8

	Put struct {
		JournalDir string
	}

	Get struct {
		JournalDir string
		Offset     uint64
	}

	Quit struct {
		JournalDir string
	}

	Response struct {
		Err string
	}
)

func init() {
	gob.Register(&Put{})
	gob.Register(&Get{})
	gob.Register(&Quit{})
}

const (
	QUIT RequestVerb = iota
	PUT
	GET
)

func (o *Request) WriteTo(w io.Writer) (n int64, err error)  { return gobWriteTo(w, o) }
func (o *Response) WriteTo(w io.Writer) (n int64, err error) { return gobWriteTo(w, o) }

func (o *Request) ReadFrom(r io.Reader) (n int64, err error)  { return gobReadFrom(r, o) }
func (o *Response) ReadFrom(r io.Reader) (n int64, err error) { return gobReadFrom(r, o) }

func gobReadFrom(r io.Reader, v interface{}) (n int64, err error) {
	var l uint32
	nn, err := readUint32(r, &l)
	n += int64(nn)
	if err != nil {
		return n, err
	}
	buf := make([]byte, l)
	nn, err = io.ReadAtLeast(r, buf, int(l))
	n += int64(nn)
	if err != nil {
		return n, err
	}
	return n, gob.NewDecoder(bytes.NewReader(buf)).Decode(v)
}

func gobWriteTo(w io.Writer, v interface{}) (n int64, err error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return 0, err
	}
	if buf.Len() > math.MaxUint32 {
		return 0, errors.Errorf("request size should be less than 2^32-1, got %d", buf.Len())
	}
	l := uint32(buf.Len())
	nn, err := writeUint32(w, l)
	n += int64(nn)
	if err != nil {
		return n, err
	}
	nn, err = w.Write(buf.Bytes())
	n += int64(nn)
	if err != nil {
		return n, err
	}
	return n, nil
}

func writeUint16(w io.Writer, i uint16) (int, error) {
	n, err := w.Write([]byte{byte(i >> 8), byte(i)})
	return n, err
}

func readUint16(r io.Reader, i *uint16) (int, error) {
	var b [2]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		return n, err
	}
	*i = uint16(b[0])<<8 | uint16(b[1])
	return n, nil
}

func writeUint32(w io.Writer, i uint32) (int, error) {
	return w.Write([]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
}

func readUint32(r io.Reader, i *uint32) (int, error) {
	var b [4]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		return n, err
	}
	*i = uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	return n, nil
}
