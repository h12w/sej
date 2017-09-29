package hub

import (
	"path"
	"runtime"
	"testing"
	"time"

	"appcoachs.net/x/log"
	"h12.me/sej"
)

func TestGET(t *testing.T) {
	tt := sej.Test{t}
	serverDir := tt.NewDir()
	const addr = "127.0.0.1:19001"
	s := Server{
		Addr:    addr,
		Dir:     serverDir,
		Timeout: time.Second,
		ErrChan: make(chan error),
		LogChan: make(chan string),
	}
	defer s.Close()
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		for err := range s.ErrChan {
			log.Error(err)
		}
	}()
	go func() {
		for line := range s.LogChan {
			t.Log("server log:", line)
		}
	}()
	runtime.Gosched()
	client := Client{
		Addr:       addr,
		ClientID:   "client",
		JournalDir: "blue.0.1",
		Timeout:    time.Second,
	}
	defer client.Close()
	dirOnHub := path.Join(serverDir, "client.blue.0.1")
	testMessageTexts := []string{"a", "b", "c", "d", "e"}
	messages := toMsgSlice(testMessageTexts)
	time.Sleep(time.Second)

	if err := client.Send(messages[:3]); err != nil {
		t.Fatal(err)
	}
	if err := client.Send(messages); err != nil {
		t.Fatal(err)
	}

	if err := client.Quit(); err != nil {
		t.Fatal(err)
	}
	tt.VerifyMessages(dirOnHub, testMessageTexts...)
}

func toMsgSlice(messages []string) []sej.Message {
	ms := make([]sej.Message, len(messages))
	for i := range ms {
		ms[i] = sej.Message{Offset: uint64(i), Value: []byte(messages[i])}
	}
	return ms
}
