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

func BenchmarkHubBatch1000(b *testing.B) {
	tt := newHubTest(b)
	defer tt.Close()

	const batchSize = 1000
	batchNum := b.N
	now := time.Now().Truncate(time.Millisecond).UTC()
	value := bytes.Repeat([]byte{'a'}, 2000)
	messages := make([]sej.Message, batchSize*batchNum)
	for i := range messages {
		messages[i] = sej.Message{
			Offset:    uint64(i),
			Timestamp: now,
			Key:       []byte("key-" + fmt.Sprintf("%09x", i)),
			Value:     value,
		}
	}
	start := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := messages[batchSize*i : batchSize*(i+1)]
		if err := tt.Send(batch); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	fmt.Println("qps", float64(batchNum)*batchSize/time.Since(start).Seconds())
	tt.VerifyMessages(tt.clientDirOnHub(), messages)
}

type hubTest struct {
	dir string
	sej.Test
	*Client
	*Server
}

func (h *hubTest) Close() {
	t := h.Test
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
		Timeout: time.Second,
		// ErrChan: make(chan error, 1),
		LogChan: make(chan string, 1),
		Handler: NewJournalCopyHandler(serverDir),
	}
	go func() {
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
	}()
	// go func() {
	// 	for err := range server.ErrChan {
	// 		fmt.Println("server error", err)
	// 	}
	// }()
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
		dir:    serverDir,
		Test:   sej.Test{t},
		Server: server,
		Client: client,
	}
}

func (h *hubTest) clientDirOnHub() string {
	return path.Join(h.dir, h.Client.ClientID+"."+h.Client.JournalDir)
}

func (h *hubTest) VerifyServerMessages(messages []sej.Message) {
	h.VerifyMessages(h.clientDirOnHub(), messages)
}

func (h *hubTest) serverLatestOffset() uint64 {
	dir, err := sej.OpenJournalDir(sej.JournalDirPath(h.clientDirOnHub()))
	if err != nil {
		h.Fatal(err)
	}
	offset, err := dir.Last().LastOffset()
	if err != nil {
		h.Fatal(err)
	}
	return offset
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
