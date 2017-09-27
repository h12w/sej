package hub

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"h12.me/sej"
	"h12.me/sej/sejtest"
)

func TestGET(t *testing.T) {
	serverDir := sejtest.NewDir(t)
	// clientDir := sejtest.NewDir(t)
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
		if err, ok := err.(stackTracer); ok {
			fmt.Println("STACK TRACE")
			for _, f := range err.StackTrace() {
				fmt.Println(f)
			}
		}
		t.Fatal(err)
	}
	go func() {
		for err := range s.ErrChan {
			fmt.Println(err)
		}
	}()
	go func() {
		for line := range s.LogChan {
			fmt.Println(line)
		}
	}()
	runtime.Gosched()
	client := Client{
		Addr:       addr,
		ClientID:   "client",
		JournalDir: "blue",
		Timeout:    time.Second,
	}
	messages := []sej.Message{
		{
			Offset: 0,
			Value:  []byte("v0"),
		},
		{
			Offset: 1,
			Value:  []byte("v1"),
		},
	}
	time.Sleep(time.Second)
	if err := client.Send(messages); err != nil {
		if err, ok := err.(stackTracer); ok {
			fmt.Println("STACK TRACE")
			for _, f := range err.StackTrace() {
				fmt.Println(f)
			}
		}
		t.Fatal(err)
	}
}
