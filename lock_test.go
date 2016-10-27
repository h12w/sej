package sej

import "testing"

func TestWriteLock(t *testing.T) {
	path := newTestPath(t)
	w1, err := NewWriter(path, defaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewWriter(path, defaultSegmentSize)
	if err != ErrLocked {
		t.Fatal("expect lock error but got nil")
	}
	w1.Close()
	w3, err := NewWriter(path, defaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	w3.Close()
}
