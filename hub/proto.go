//go:generate colf -b .. go proto.colf
//go:generate mv Colfer.go proto_auto.go
package hub

import (
	"encoding"
	"io"
	"math"

	"h12.me/sej"

	"github.com/pkg/errors"
)

type (
	// RequestVerb defines all possible requests
	// version is not neeeded because a new version of request
	// is a new request
	RequestVerb     uint8
	colferMarshaler interface {
		MarshalTo(buf []byte) int
		MarshalLen() (int, error)
	}
	colferUnmarshaler encoding.BinaryUnmarshaler
	colferObject      interface {
	}
	Request struct {
		Title  RequestTitle
		Header interface {
			io.WriterTo
			io.ReaderFrom
		}
		Messages []sej.Message
	}
)

const (
	QUIT RequestVerb = iota
	PUT
	GET
)

func (o *Request) WriteTo(w io.Writer) (n int64, err error) {
	if len(o.Messages) > math.MaxUint16 {
		return 0, errors.New("message count must be less than 65535")
	}

	nn, err := o.Title.WriteTo(w)
	n += nn
	if err != nil {
		return n, err
	}
	nn, err = o.Header.WriteTo(w)
	n += nn
	if err != nil {
		return n, err
	}

	if o.Title.Verb != uint8(PUT) {
		return n, nil
	}

	nn, err = writeUint16(w, uint16(len(o.Messages)))
	n += nn
	if err != nil {
		return n, err
	}
	smallBuf := make([]byte, 8)
	for i := range o.Messages {
		nn, err := sej.WriteMessage(w, smallBuf, &o.Messages[i])
		n += nn
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

func (o *Request) ReadFrom(r io.Reader) (n int64, err error) {
	nn, err := o.Title.ReadFrom(r)
	n += nn
	if err != nil {
		return n, err
	}
	switch RequestVerb(o.Title.Verb) {
	case QUIT:
		q := &Quit{}
		nn, err := q.ReadFrom(r)
		n += nn
		if err != nil {
			return n, err
		}
		o.Header = q
	case PUT:
		put := new(Put)
		nn, err := put.ReadFrom(r)
		n += nn
		if err != nil {
			return n, err
		}
		o.Header = put
		var length uint16
		nn, err = readUint16(r, &length)
		n += nn
		if err != nil {
			return n, err
		}

		for i := 0; i < int(length); i++ {
			var msg sej.Message
			nn, err := msg.ReadFrom(r)
			n += nn
			if err != nil {
				return n, err
			}
			o.Messages = append(o.Messages, msg)
		}
	default:
		return n, errors.Errorf("unknown request type %d", o.Title.Verb)
	}
	return n, nil
}

func (o *RequestTitle) WriteTo(w io.Writer) (n int64, err error) { return writeTo256(w, o) }
func (o *Put) WriteTo(w io.Writer) (n int64, err error)          { return writeTo256(w, o) }
func (o *Get) WriteTo(w io.Writer) (n int64, err error)          { return writeTo256(w, o) }
func (o *Quit) WriteTo(w io.Writer) (n int64, err error)         { return writeTo256(w, o) }
func (o *Response) WriteTo(w io.Writer) (n int64, err error)     { return writeTo256(w, o) }

func (o *RequestTitle) ReadFrom(r io.Reader) (n int64, err error) { return readFrom256(r, o) }
func (o *Put) ReadFrom(r io.Reader) (n int64, err error)          { return readFrom256(r, o) }
func (o *Get) ReadFrom(r io.Reader) (n int64, err error)          { return readFrom256(r, o) }
func (o *Quit) ReadFrom(r io.Reader) (n int64, err error)         { return readFrom256(r, o) }
func (o *Response) ReadFrom(r io.Reader) (n int64, err error)     { return readFrom256(r, o) }

func writeTo256(w io.Writer, o colferMarshaler) (int64, error) {
	l, err := o.MarshalLen()
	if err != nil {
		return 0, err
	}
	if l > math.MaxUint8 {
		return 0, errors.New("length out of range")
	}
	data := make([]byte, l+1)
	data[0] = uint8(l)
	o.MarshalTo(data[1:])
	nn, err := w.Write(data)
	return int64(nn), errors.Wrap(err, "fail to write data frame")
}

func readFrom256(r io.Reader, o colferUnmarshaler) (n int64, err error) {
	var l [1]byte
	nn, err := r.Read(l[:])
	n += int64(nn)
	if err != nil {
		return n, errors.Wrap(err, "fail to read data size")
	}
	buf := make([]byte, int(l[0]))
	nn, err = io.ReadAtLeast(r, buf, int(l[0]))
	n += int64(nn)
	if err != nil {
		return n, errors.Wrap(err, "fail to read data bytes")
	}
	return n, o.UnmarshalBinary(buf)
}

func writeUint16(w io.Writer, i uint16) (int64, error) {
	n, err := w.Write([]byte{byte(i >> 8), byte(i)})
	return int64(n), err
}

func readUint16(r io.Reader, i *uint16) (int64, error) {
	var b [2]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		return int64(n), err
	}
	*i = uint16(b[0])<<8 | uint16(b[1])
	return int64(n), nil
}
