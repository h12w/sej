package hub

import (
	"bytes"
	"fmt"
	"path"
	"testing"
	"time"

	"h12.me/sej"
)

func TestHub(t *testing.T) {
	tt := newHubTest(t)
	defer tt.Close()
	messages := []string{"a", "b", "c", "d", "e"}
	sejMessages := toMsgSlice(messages)
	// send first 3
	if err := tt.Send(sejMessages[:3]); err != nil {
		t.Fatal(err)
	}
	// send rest with duplicated messages
	if err := tt.Send(sejMessages); err != nil {
		t.Fatal(err)
	}
	tt.VerifyServerMessages(sejMessages)
	time.Sleep(time.Second)
}

func BenchmarkHub_1000_10(b *testing.B) {
	tt := newHubTest(b)
	defer tt.Close()

	const (
		batchSize = 1000
		batchNum  = 10
	)
	messages := make([]sej.Message, batchNum*batchSize)
	value := bytes.Repeat([]byte{'a'}, 100)
	now := time.Now().Truncate(time.Millisecond).UTC()
	for i := range messages {
		messages[i] = sej.Message{
			Timestamp: now,
			Key:       []byte("key-" + fmt.Sprintf("%09x", i)),
			Value:     value,
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for batch := 0; batch < batchNum; batch++ {
			if err := tt.Send(messages[batchNum : batchNum+batchSize]); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
}

type hubTest struct {
	sej.Test
	*Client
	*Server
}

func (h *hubTest) Close() {
	t := h.Test
	if err := h.Client.Quit(); err != nil {
		t.Fatal(err)
	}
	if err := h.Client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.Server.Close(); err != nil {
		t.Fatal(err)
	}
}

func newHubTest(t testing.TB) *hubTest {
	tt := sej.Test{t}
	serverDir := tt.NewDir()
	const addr = "127.0.0.1:19001"
	server := &Server{
		Addr:    addr,
		Dir:     serverDir,
		Timeout: time.Second,
		ErrChan: make(chan error, 1),
		LogChan: make(chan string, 1),
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		for err := range server.ErrChan {
			fmt.Println("server error", err)
		}
	}()
	go func() {
		for line := range server.LogChan {
			t.Log("server log:", line)
		}
	}()
	client := &Client{
		Addr:       addr,
		ClientID:   "client",
		JournalDir: "blue.0.1",
		Timeout:    time.Second,
	}
	return &hubTest{
		Test:   sej.Test{t},
		Server: server,
		Client: client,
	}
}

func (h *hubTest) clientDirOnHub() string {
	return path.Join(h.Server.Dir, h.Client.ClientID+"."+h.Client.JournalDir)
}

func (h *hubTest) VerifyServerMessages(messages []sej.Message) {
	h.VerifyMessages(h.clientDirOnHub(), messages)
}

func toMsgSlice(messages []string) []sej.Message {
	ms := make([]sej.Message, len(messages))
	now := time.Now().Truncate(time.Millisecond).UTC()
	for i := range ms {
		ms[i] = sej.Message{
			Timestamp: now,
			Offset:    uint64(i),
			Value:     []byte(messages[i]),
		}
	}
	return ms
}
