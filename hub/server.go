package hub

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"h12.me/sej"
	"h12.me/sej/hub/proto"
)

type (
	Server struct {
		Addr    string
		Dir     string
		Timeout time.Duration
		ErrChan chan error
		LogChan chan string

		l  net.Listener
		mu sync.Mutex
	}
	Handler interface {
		Handle(msg *sej.Message) (uint64, error)
	}
)

var errQuit = errors.New("quit")

func (s *Server) Start() error {
	s.mu.Lock()
	l := s.l
	if l == nil {
		var err error
		l, err = net.Listen("tcp", s.Addr)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		s.l = l
	}
	s.mu.Unlock()
	s.log("listening to " + s.Addr)

	ws := newWriters(s.Dir)
	go func() {
		for {
			sock, err := l.Accept()
			if err != nil {
				s.mu.Lock()
				closed := (s.l == nil)
				s.mu.Unlock()
				if !closed {
					s.error(err)
				}
				break
			}
			go newSession(sock, ws, s).loop()
		}
	}()
	return nil
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
	var err error
	s.mu.Lock()
	if s.l != nil {
		err = s.l.Close()
		s.l = nil
	}
	s.mu.Unlock()
	return err
}

func (s *Server) error(err error) error {
	if s.ErrChan == nil {
		return err
	}
	select {
	case s.ErrChan <- err:
	default:
	}
	return err
}

type session struct {
	c  net.Conn
	ws *writers
	*Server
}

func newSession(c net.Conn, ws *writers, s *Server) *session {
	return &session{
		c:      c,
		ws:     ws,
		Server: s,
	}
}

func (s *session) loop() {
	defer func() {
		if err := s.c.Close(); err != nil {
			s.error(errors.Wrap(err, "fail to close client socket"))
		}
	}()
	rw := bufio.NewReadWriter(bufio.NewReader(s.c), bufio.NewWriter(s.c))
	defer rw.Flush()
	for {
		if err := s.serve(rw); err != nil {
			if err != errQuit {
				s.error(err)
			}
			break
		}
		if err := rw.Flush(); err != nil {
			s.error(err)
			break
		}
	}
}

func (s *session) serve(rw io.ReadWriter) error {
	var req proto.Request
	s.c.SetReadDeadline(time.Now().Add(time.Minute))
	if _, err := req.ReadFrom(rw); err != nil {
		return errors.Wrap(err, "fail to read request")
	}
	switch header := req.Header.(type) {
	case *proto.Quit:
		return s.serveQuit(rw, &req, header)
	case *proto.Put:
		return s.servePut(rw, &req, header)
	case *proto.Get:
		return s.serveError(rw, &req, errors.Errorf("unsupported request type %d", req.Title.Verb))
	default:
		return s.serveError(rw, &req, errors.Errorf("unknown request type %d", req.Title.Verb))
	}
	return nil
}

func (s *session) serveQuit(rw io.ReadWriter, req *proto.Request, quit *proto.Quit) error {
	s.writeResp(rw, req, &proto.Response{})
	return errQuit
}

func (s *session) servePut(rw io.ReadWriter, req *proto.Request, put *proto.Put) error {
	writer, err := s.ws.Writer(req.Title.ClientID, put.JournalDir)
	if err != nil {
		return errors.Wrap(err, "fail to get writer for client "+req.Title.ClientID)
	}
	for _, msg := range req.Messages {
		if msg.Offset > writer.Offset() {
			return errors.Wrapf(err, "offset out of order, msg: %d, writer %d", msg.Offset, writer.Offset())
		} else if msg.Offset < writer.Offset() { // redundant
			continue
		}
		if err := writer.Append(&msg); err != nil {
			return s.serveError(rw, req, err)
		}
	}
	if err := writer.Flush(); err != nil {
		return s.serveError(rw, req, err)
	}
	if err := writer.Sync(); err != nil {
		return s.serveError(rw, req, err)
	}
	s.writeResp(rw, req, &proto.Response{})
	return nil
}

func (s *session) serveError(rw io.ReadWriter, req *proto.Request, err error) error {
	s.writeResp(rw, req, &proto.Response{
		Err: err.Error(),
	})
	return err
}

func (s *session) writeResp(w io.Writer, req *proto.Request, resp *proto.Response) error {
	s.c.SetWriteDeadline(time.Now().Add(s.Timeout))
	if _, err := resp.WriteTo(w); err != nil {
		return errors.Wrap(err, "fail to write response to "+req.Title.ClientID)
	}
	return nil
}
