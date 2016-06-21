package fq

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

func (w *Writer) writeMessage(msg []byte) error {
	if err := writeUint64(w.w, w.offset); err != nil {
		return err
	}
	if err := w.writeCRC(msg); err != nil {
		return err
	}
	size := int32(len(msg))
	if err := w.writeInt32(size); err != nil {
		return err
	}
	if _, err := w.w.Write(msg); err != nil {
		return err
	}
	if err := w.writeInt32(size); err != nil {
		return err
	}
	w.offset++
	return nil
}

func (r *Reader) readMessage() (msg []byte, _ error) {
	offset, err := readUint64(r.r)
	if err != nil {
		return nil, err
	}
	if offset != r.offset {
		return nil, fmt.Errorf("offset is out of order: %d, %d", offset, r.offset)
	}
	crc, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	size, err := readInt32(r.r)
	if err != nil {
		return nil, err
	}
	msg = make([]byte, int(size))
	n, err := io.ReadFull(r.r, msg)
	if err != nil {
		return nil, err
	}
	if n != int(size) {
		return nil, errors.New("message is truncated")
	}
	size2, err := readInt32(r.r)
	if err != nil {
		return nil, err
	}
	if size != size2 {
		return nil, errors.New("data corruption detected by size2")
	}
	if crc != crc32.ChecksumIEEE(msg) {
		return nil, errors.New("data corruption detected by CRC")
	}
	r.offset++
	return msg, nil
}

func writeUint64(w io.Writer, i uint64) error {
	_, err := w.Write([]byte{byte(i >> 56), byte(i >> 48), byte(i >> 40), byte(i >> 32), byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
	return err
}

func readUint64(r io.Reader) (uint64, error) {
	var b [8]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, err
	}
	if n != 8 {
		return 0, fmt.Errorf("uint64 is truncated (%d)", n)
	}
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7]), nil
}

func (w *Writer) writeInt32(i int32) error {
	_, err := w.w.Write([]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
	return err
}

func readInt32(r io.Reader) (int32, error) {
	var b [4]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, err
	}
	if n != 4 {
		return 0, fmt.Errorf("int32 is truncated (%d)", n)
	}
	return int32(b[0])<<24 | int32(b[1])<<16 | int32(b[2])<<8 | int32(b[3]), nil
}

func (w *Writer) writeUint32(i uint32) error {
	_, err := w.w.Write([]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
	return err
}

func (r *Reader) readUint32() (uint32, error) {
	var b [4]byte
	n, err := io.ReadFull(r.r, b[:])
	if err != nil {
		return 0, err
	}
	if n != 4 {
		return 0, fmt.Errorf("uint32 is truncated (%d)", n)
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]), nil
}

func (w *Writer) writeCRC(data []byte) error {
	return w.writeUint32(crc32.ChecksumIEEE(data))
}
