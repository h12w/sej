package sej

import "testing"

func TestWriteLock(t *testing.T) {
	path := newTestPath(t)
	w1, err := NewWriter(path, 9999)
	if err != nil {
		t.Fatal(err)
	}
	w2, err := NewWriter(path, 9999)
	if err != ErrLocked {
		t.Fatal("expect lock error but got nil")
	}
	w1.Close()
	w2, err = NewWriter(path, 9999)
	if err != nil {
		t.Fatal(err)
	}
	w2.Close()
}
