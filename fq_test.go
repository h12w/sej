package fq

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
)

func TestAppendRead(t *testing.T) {
	path, err := ioutil.TempDir(".", "test-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(path)
	w, err := NewWriter(path)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	var messages []string
	for i := 0; i < 1; i++ {
		messages = append(messages, strconv.Itoa(i))
	}
	for _, msg := range messages {
		if _, err := w.Append([]byte(msg)); err != nil {
			log.Fatal(err)
		}
	}
	w.Close()
	r, err := NewReader(path, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	for i := range messages {
		msg, err := r.Read()
		if err != nil {
			log.Fatal(err)
		}
		if string(msg) != messages[i] {
			t.Fatalf("expect %s, got %s", messages[i], string(msg))
		}
	}
}
