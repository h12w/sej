package sej

import "testing"

func TestOffset(t *testing.T) {
	dir := newTestPath(t)
	name := "reader1"
	offset, err := NewOffset(dir, name)
	if err != nil {
		t.Fatal(err)
	}
	if val := offset.Value(); val != 0 {
		t.Fatalf("expect offset is 0 but got %d", val)
	}
	offset.Inc()
	if err := offset.Commit(); err != nil {
		t.Fatal(err)
	}
	if val := offset.Value(); val != 1 {
		t.Fatalf("expect offset is 1 but got %d", val)
	}
	offset.Close()

	// open again
	offset, err = NewOffset(dir, name)
	if err != nil {
		t.Fatal(err)
	}
	if val := offset.Value(); val != 1 {
		t.Fatalf("expect offset is 1 but got %d", val)
	}
	offset.Close()
}
