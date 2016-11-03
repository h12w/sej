package sej

import (
	"errors"
	"fmt"
	"hash/crc32"
	//"github.com/klauspost/crc32"
	"io"
	"os"
)

const (
	metaSize = 20
)

var (
	// ErrCorrupted is returned when the journal file is corrupted
	ErrCorrupted = errors.New("journal file is courrupted")
)

func writeMessage(w io.Writer, msg []byte, offset uint64) error {
	if err := writeUint64(w, offset); err != nil {
		return err
	}
	if err := writeCRC(w, msg); err != nil {
		return err
	}
	size := int32(len(msg))
	if err := writeInt32(w, size); err != nil {
		return err
	}
	if _, err := w.Write(msg); err != nil {
		return err
	}
	if err := writeInt32(w, size); err != nil {
		return err
	}
	return nil
}

type Message struct {
	Offset uint64
	CRC    uint32
	Value  []byte
}

type readSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

func ReadMessage(r io.ReadSeeker) (*Message, error) {
	offset, err := readUint64(r)
	if err != nil {
		return nil, err
	}
	crc, err := readUint32(r)
	if err != nil {
		r.Seek(-8, io.SeekCurrent)
		return nil, err
	}
	size, err := readInt32(r)
	if err != nil {
		r.Seek(-12, io.SeekCurrent)
		return nil, err
	}
	msg := make([]byte, int(size))
	n, err := io.ReadFull(r, msg)
	if err != nil {
		r.Seek(-16-int64(n), io.SeekCurrent)
		return nil, err
	}
	if n != int(size) {
		r.Seek(-16-int64(n), io.SeekCurrent)
		return nil, fmt.Errorf("message is truncated at %d", offset)
	}
	size2, err := readInt32(r)
	if err != nil && err != io.EOF {
		r.Seek(-16-int64(n), io.SeekCurrent)
		return nil, err
	}
	if size != size2 {
		r.Seek(-16-int64(n), io.SeekCurrent)
		return nil, fmt.Errorf("data corruption detected by size2 at %d", offset)
	}
	if crc != crc32.ChecksumIEEE(msg) {
		r.Seek(-16-int64(n), io.SeekCurrent)
		return nil, fmt.Errorf("data corruption detected by CRC at %d", offset)
	}
	return &Message{
		Offset: offset,
		CRC:    crc,
		Value:  msg,
	}, nil
}

func readMessageBackward(r io.ReadSeeker) (*Message, error) {
	if _, err := r.Seek(-4, os.SEEK_CUR); err != nil {
		return nil, err
	}
	size, err := readInt32(r)
	if err != nil {
		return nil, err
	}
	if _, err := r.Seek(-metaSize-int64(size), os.SEEK_CUR); err != nil {
		return nil, err
	}
	return ReadMessage(r)
}

func writeUint64(w io.Writer, i uint64) error {
	_, err := w.Write([]byte{byte(i >> 56), byte(i >> 48), byte(i >> 40), byte(i >> 32), byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
	return err
}

func readUint64(r io.ReadSeeker) (uint64, error) {
	var b [8]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		r.Seek(int64(-n), io.SeekCurrent)
		return 0, err
	}
	if n != 8 {
		return 0, fmt.Errorf("uint64 is truncated (%d)", n)
	}
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7]), nil
}

func writeInt32(w io.Writer, i int32) error {
	_, err := w.Write([]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
	return err
}

func readInt32(r io.ReadSeeker) (int32, error) {
	var b [4]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		r.Seek(int64(-n), io.SeekCurrent)
		return 0, err
	}
	if n != 4 {
		return 0, fmt.Errorf("int32 is truncated (%d)", n)
	}
	return int32(b[0])<<24 | int32(b[1])<<16 | int32(b[2])<<8 | int32(b[3]), nil
}

func writeUint32(w io.Writer, i uint32) error {
	_, err := w.Write([]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
	return err
}

func readUint32(r io.ReadSeeker) (uint32, error) {
	var b [4]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		r.Seek(int64(-n), io.SeekCurrent)
		return 0, err
	}
	if n != 4 {
		return 0, fmt.Errorf("uint32 is truncated (%d)", n)
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]), nil
}

func writeCRC(w io.Writer, data []byte) error {
	return writeUint32(w, crc32.ChecksumIEEE(data))
}

func (journalFile *JournalFile) LatestOffset() (uint64, error) {
	file, err := os.Open(journalFile.fileName)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	fileSize, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return 0, err
	}
	if fileSize == 0 {
		return journalFile.startOffset, nil
	}
	msg, err := readMessageBackward(file)
	if err != nil {
		return 0, ErrCorrupted
	}
	return msg.Offset + 1, nil
}
