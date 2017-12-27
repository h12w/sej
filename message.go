package sej

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

var (
	errMessageCorrupted   = errors.New("last message of the journal file is courrupted")
	errJournalFileIsEmpty = errors.New("the journal file is empty")
)

// Message in a segmented journal file
type Message struct {
	Offset    uint64
	Timestamp time.Time
	Type      byte
	Key       []byte
	Value     []byte
}

type readSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

func (m *Message) IsNull() bool {
	return m.Offset == 0 &&
		m.Timestamp.IsZero() &&
		m.Type == 0 &&
		m.Key == nil &&
		m.Value == nil
}

// WriteMessage writes the message
// buf should be at least 8 bytes and is used to avoid allocation
func WriteMessage(w io.Writer, buf []byte, m *Message) (int64, error) {
	cnt := int64(0) // total bytes written

	n, err := writeUint64(w, buf, m.Offset)
	cnt += int64(n)
	if err != nil {
		return cnt, err
	}

	ts := m.Timestamp
	var nano int64
	if ts.IsZero() {
		nano = math.MinInt64
	} else {
		nano = ts.UnixNano()
	}
	n, err = writeInt64(w, buf, nano)
	cnt += int64(n)
	if err != nil {
		return cnt, err
	}

	n, err = writeByte(w, buf, m.Type)
	cnt += int64(n)
	if err != nil {
		return cnt, err
	}

	n, err = writeInt8(w, buf, int8(len(m.Key)))
	cnt += int64(n)
	if err != nil {
		return cnt, err
	}

	n, err = w.Write(m.Key)
	cnt += int64(n)
	if err != nil {
		return cnt, err
	}

	n, err = writeInt32(w, buf, int32(len(m.Value)))
	cnt += int64(n)
	if err != nil {
		return cnt, err
	}

	n, err = w.Write(m.Value)
	cnt += int64(n)
	if err != nil {
		return cnt, err
	}

	n, err = writeInt32(w, buf, int32(cnt)+4)
	cnt += int64(n)
	if err != nil {
		return cnt, err
	}

	return cnt, nil
}

// ReadFrom reads a message from a io.ReadSeeker.
// When an error occurs, it will rollback the seeker and then returns the original error.
func (m *Message) ReadFrom(r io.Reader) (n int64, err error) {
	cnt := int64(0) // total bytes read

	nn, err := readUint64(r, &m.Offset)
	cnt += int64(nn)
	if err != nil {
		return cnt, err
	}

	var unixNano int64
	nn, err = readInt64(r, &unixNano)
	cnt += int64(nn)
	if err != nil {
		return cnt, err
	}
	if unixNano != math.MinInt64 {
		m.Timestamp = time.Unix(0, unixNano).UTC()
	} else {
		m.Timestamp = time.Time{}
	}

	nn, err = readByte(r, &m.Type)
	cnt += int64(nn)
	if err != nil {
		return cnt, err
	}

	var keyLen int8
	nn, err = readInt8(r, &keyLen)
	cnt += int64(nn)
	if err != nil {
		return cnt, err
	}
	if keyLen < 0 {
		return cnt, errMessageCorrupted
	}

	if keyLen > 0 {
		m.Key = make([]byte, int(keyLen))
		nn, err = io.ReadFull(r, m.Key)
		cnt += int64(nn)
		if err != nil {
			return cnt, err
		}
		if nn != int(keyLen) {
			return cnt, fmt.Errorf("message is truncated at %d", m.Offset)
		}
	}

	var valueLen int32
	nn, err = readInt32(r, &valueLen)
	cnt += int64(nn)
	if err != nil {
		return cnt, err
	}
	if valueLen < 0 {
		return cnt, errMessageCorrupted
	}

	m.Value = make([]byte, int(valueLen))
	nn, err = io.ReadFull(r, m.Value)
	cnt += int64(nn)
	if err != nil {
		return cnt, err
	}
	if nn != int(valueLen) {
		return cnt, fmt.Errorf("message is truncated at %d", m.Offset)
	}

	var size int32
	nn, err = readInt32(r, &size)
	cnt += int64(nn)
	if err != nil {
		return cnt, err
	}
	if int64(size) != cnt {
		return cnt, errMessageCorrupted
	}

	return cnt, nil
}

func readMessageBackward(r io.ReadSeeker) (*Message, error) {
	var size int32
	if _, err := r.Seek(-4, os.SEEK_CUR); err != nil {
		return nil, err
	}
	if _, err := readInt32(r, &size); err != nil {
		return nil, err
	}
	if _, err := r.Seek(-int64(size), os.SEEK_CUR); err != nil {
		return nil, err
	}
	var msg Message
	_, err := msg.ReadFrom(r)
	return &msg, err
}

func writeInt8(w io.Writer, buf []byte, i int8) (int, error) {
	buf[0] = byte(i)
	return w.Write(buf[:1])
}

func readInt8(r io.Reader, i *int8) (int, error) {
	var b [1]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		return n, err
	}
	*i = int8(b[0])
	return n, nil
}

func writeByte(w io.Writer, buf []byte, i byte) (int, error) {
	buf[0] = i
	return w.Write(buf[:1])
}

func readByte(r io.Reader, i *byte) (int, error) {
	var b [1]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		return n, err
	}
	*i = b[0]
	return n, nil
}

func writeInt64(w io.Writer, buf []byte, i int64) (int, error) {
	binary.BigEndian.PutUint64(buf, uint64(i))
	return w.Write(buf[:8])
}

func readInt64(r io.Reader, i *int64) (int, error) {
	var b [8]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		return n, err
	}
	*i = int64(b[0])<<56 | int64(b[1])<<48 | int64(b[2])<<40 | int64(b[3])<<32 |
		int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
	return n, nil
}

func writeUint64(w io.Writer, buf []byte, i uint64) (int, error) {
	binary.BigEndian.PutUint64(buf, i)
	return w.Write(buf[:8])
}

func readUint64(r io.Reader, i *uint64) (int, error) {
	var b [8]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		return n, err
	}
	*i = uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
	return n, nil
}

func writeInt32(w io.Writer, buf []byte, i int32) (int, error) {
	binary.BigEndian.PutUint32(buf, uint32(i))
	return w.Write(buf[:4])
}

func readInt32(r io.Reader, i *int32) (int, error) {
	var b [4]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		return n, err
	}
	*i = int32(b[0])<<24 | int32(b[1])<<16 | int32(b[2])<<8 | int32(b[3])
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

func (journalFile *JournalFile) FirstMessage() (*Message, error) {
	file, err := os.Open(journalFile.FileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	fileStat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if fileStat.Size() == 0 {
		return nil, errJournalFileIsEmpty
	}
	var msg Message
	if _, err := msg.ReadFrom(file); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (journalFile *JournalFile) LastMessage() (*Message, error) {
	file, err := os.Open(journalFile.FileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	fileSize, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}
	if fileSize == 0 {
		return nil, errJournalFileIsEmpty
	}
	msg, err := readMessageBackward(file)
	if err != nil {
		return nil, errMessageCorrupted
	}
	return msg, nil
}

// LatestOffset returns the offset after the last message in a journal file
func (journalFile *JournalFile) LastOffset() (uint64, error) {
	msg, err := journalFile.LastMessage()
	if err != nil {
		if err == errJournalFileIsEmpty {
			return journalFile.FirstOffset, nil
		}
		return 0, err
	}
	return msg.Offset + 1, nil
}

func (journalFile *JournalFile) LastReadableOffset() (uint64, error) {
	offset, err := journalFile.LastOffset()
	if err == nil {
		return offset, nil
	}
	oriErr := err

	f, err := os.Open(journalFile.FileName)
	if err != nil {
		return 0, oriErr
	}
	defer f.Close()
	var msg Message
	for {
		_, err := msg.ReadFrom(f)
		if err != nil {
			break
		}
		offset = msg.Offset
	}
	return offset, nil
}
