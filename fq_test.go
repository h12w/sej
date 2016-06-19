package fq

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

func TestAppendRead(t *testing.T) {
	path, err := ioutil.TempDir(".", "test-")
	if err != nil {
		t.Error(err)
		return
	}
	defer os.RemoveAll(path)
	w, err := NewWriter(path)
	if err != nil {
		t.Error(err)
		return
	}
	var messages []string
	for i := 0; i < 1000; i++ {
		messages = append(messages, strconv.Itoa(i))
	}
	for i, msg := range messages {
		offset, err := w.Append([]byte(msg))
		if err != nil {
			t.Error(err)
			return
		}
		if offset != uint64(i+1) {
			t.Errorf("expect offset %d, got %d", i+1, offset)
			return
		}
	}
	if err := w.Close(); err != nil {
		t.Error(err)
		return
	}
	r, err := NewReader(path, 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer r.Close()
	for i := range messages {
		msg, offset, err := r.Read()
		if err != nil {
			t.Error(err)
			return
		}
		if int(offset) != i+1 {
			t.Errorf("expect offset %d, got %d", i+1, offset)
			return
		}
		if string(msg) != messages[i] {
			t.Errorf("expect %s, got %s", messages[i], string(msg))
			return
		}
	}
}
