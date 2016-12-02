package sej

import (
	"io"
	"os"
	"path"
)

// Offset is used to manage a disk-persisted offset
type Offset struct {
	dir      *os.File
	file     string
	fileLock *fileLock
	value    uint64
}

// NewOffset creates a new Offset object persisted to dir/ofs/name.ofs
func NewOffset(dir, name string) (*Offset, error) {
	dir = OffsetDirPath(dir)
	_ = os.MkdirAll(dir, 0755)
	filePrefix := path.Join(dir, name)
	fileLock, err := openFileLock(filePrefix + ".lck")
	if err != nil {
		return nil, err
	}
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	o := &Offset{dir: d, file: filePrefix + ".ofs", fileLock: fileLock}
	f, err := openOrCreate(o.file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	o.value, err = ReadOffset(f)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return o, nil
}

// Value gets the current offset value
func (o *Offset) Value() uint64 {
	return o.value
}

// Commit saves and syncs the offset to disk
func (o *Offset) Commit(offset uint64) error {
	file := o.file + ".tmp"
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	if _, err := writeUint64(f, offset); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(file, o.file); err != nil {
		os.Remove(file)
		return err
	}
	if err := o.dir.Sync(); err != nil {
		return err
	}
	o.value = offset
	return nil
}

// Close closes opened resources
func (o *Offset) Close() error {
	o.fileLock.Close()
	return o.dir.Close()
}

func OffsetDirPath(dir string) string {
	return path.Join(dir, "ofs")
}

// ReadOffset reads the offset stored in an ofs file (r)
func ReadOffset(r io.ReadSeeker) (offset uint64, err error) {
	_, err = readUint64(r, &offset)
	return offset, err
}
