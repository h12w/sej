package hub

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"h12.me/sej"
)

type (
	Server struct {
		Addr    string
		Timeout time.Duration
		// ErrChan chan error
		LogChan chan string
		g       *grpc.Server
		Handler

		mu sync.Mutex
	}
	Handler interface {
		Put(ctx context.Context, req *PutRequest) (*PutResponse, error)
		Get(ctx context.Context, req *GetRequest) (*GetResponse, error)
	}
)

var errQuit = errors.New("quit")

func (s *Server) Start() error {
	s.mu.Lock()
	if s.g != nil {
		s.mu.Unlock()
		return nil
	}
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.log("listening to " + s.Addr)
	s.g = grpc.NewServer()
	RegisterHubServer(s.g, s)
	reflection.Register(s.g)
	g := s.g
	s.mu.Unlock()

	return g.Serve(l)
}

func (s *Server) log(format string, v ...interface{}) {
	if s.LogChan == nil {
		return
	}
	select {
	case s.LogChan <- fmt.Sprintf(format, v...):
	default:
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	if s.g != nil {
		s.g.GracefulStop()
		s.g = nil
	}
	s.mu.Unlock()
	return nil
}

type JournalCopyHandler struct {
	ws *writers
}

func NewJournalCopyHandler(dir string) *JournalCopyHandler {
	return &JournalCopyHandler{ws: newWriters(dir)}
}

func (h *JournalCopyHandler) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	writer, err := h.ws.Writer(req.ClientID, req.JournalDir)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get writer for client "+req.ClientID)
	}
	for _, msg := range req.Messages {
		if msg.Offset > writer.Offset() {
			return nil, errors.Wrapf(err, "offset out of order, msg: %d, writer %d", msg.Offset, writer.Offset())
		} else if msg.Offset < writer.Offset() { // redundant
			continue
		}
		if err := writer.Append(&sej.Message{
			Offset:    msg.Offset,
			Timestamp: time.Unix(0, msg.Timestamp).UTC(),
			Type:      byte(msg.Type),
			Key:       msg.Key,
			Value:     msg.Value,
		}); err != nil {
			return nil, err
		}
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	if err := writer.Sync(); err != nil {
		return nil, err
	}
	return &PutResponse{}, nil
}

func (h *JournalCopyHandler) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	return &GetResponse{}, nil
}
