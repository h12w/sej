package sej

import "testing"

func TestWriteLock(t *testing.T) {
	path := newTestPath(t)
	w1, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewWriter(path)
	if err != ErrLocked {
		t.Fatal("expect lock error but got nil")
	}
	w1.Close()
	w3, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	w3.Close()
}

func TestOffsetLock(t *testing.T) {
	dir := newTestPath(t)
	name := "reader2"
	o1, err := NewOffset(dir, name)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewOffset(dir, name)
	if err != ErrLocked {
		t.Fatal("expect lock error but got nil")
	}
	o1.Close()
	o3, err := NewOffset(dir, name)
	if err != nil {
		t.Fatal(err)
	}
	o3.Close()
}
