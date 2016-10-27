package sej

import (
	"errors"
	"os"
	"syscall"
)

var (
	// ErrLocked is returned when another writer has already gotten the lock
	ErrLocked = errors.New("file is already locked")
)

type fileLock struct {
	f *os.File
}

func openFileLock(name string) (*fileLock, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, ErrLocked
	}
	return &fileLock{
		f: f,
	}, nil
}

func (l *fileLock) Close() error {
	if l.f != nil {
		f := l.f
		l.f = nil
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
			return err
		}
		fileName := f.Name()
		if err := f.Close(); err != nil {
			return err
		}
		if err := os.Remove(fileName); err != nil {
			return err
		}
	}
	return nil
}
