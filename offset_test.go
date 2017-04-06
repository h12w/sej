package sej

import "testing"

func TestOffset(t *testing.T) {
	dir := newTestPath(t)
	name := "reader1"
	offset, err := NewOffset(dir, name, FirstOffset)
	if err != nil {
		t.Fatal(err)
	}
	if val := offset.Value(); val != 0 {
		t.Fatalf("expect offset is 0 but got %d", val)
	}
	if err := offset.Commit(1); err != nil {
		t.Fatal(err)
	}
	if val := offset.Value(); val != 1 {
		t.Fatalf("expect offset is 1 but got %d", val)
	}
	offset.Close()

	// open again
	offset, err = NewOffset(dir, name, FirstOffset)
	if err != nil {
		t.Fatal(err)
	}
	if val := offset.Value(); val != 1 {
		t.Fatalf("expect offset is 1 but got %d", val)
	}
	offset.Close()
}

func TestLastOffset(t *testing.T) {
	dir := newTestPath(t)
	w := newTestWriter(t, dir)
	writeTestMessages(t, w, "a", "b")
	w.Close()
	offset, err := NewOffset(dir, "test", LastOffset)
	if err != nil {
		t.Fatal(err)
	}
	if offset.Value() != 2 {
		t.Fatal("should be 2, got ", offset.Value())
	}
}
