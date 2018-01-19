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

	Syncing bool
}

type DefaultOffset int

const (
	FirstOffset DefaultOffset = iota
	LastOffset
)

// NewOffset creates a new Offset object persisted to dir/ofs/name.ofs
func NewOffset(dir, name string, defaultOffset DefaultOffset) (*Offset, error) {
	jnlDir, dir := JournalDirPath(dir), OffsetDirPath(dir)
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
	if err == io.EOF {
		jd, err := OpenJournalDir(jnlDir)
		if err != nil {
			return nil, err
		}
		if defaultOffset == LastOffset {
			o.value, err = jd.Last().LastReadableOffset()
			if err != nil {
				return nil, err
			}
		} else {
			o.value = jd.First().FirstOffset
		}
	} else if err != nil {
		return nil, err
	}
	return o, nil
}

func OpenReadonlyOffset(dir, name string) (*Offset, error) {
	dir = OffsetDirPath(dir)
	filePrefix := path.Join(dir, name)
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	o := &Offset{dir: d, file: filePrefix + ".ofs"}
	f, err := os.Open(o.file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	o.value, err = ReadOffset(f)
	if err != nil {
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
	if o.fileLock == nil {
		panic("read only offset cannot be used for committing offset")
	}
	if offset == o.value {
		return nil
	}
	file := o.file + ".tmp"
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	if _, err := writeUint64(f, make([]byte, 8), offset); err != nil {
		f.Close()
		return err
	}
	if o.Syncing {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(file, o.file); err != nil {
		os.Remove(file)
		return err
	}
	if o.Syncing {
		if err := o.dir.Sync(); err != nil {
			return err
		}
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
