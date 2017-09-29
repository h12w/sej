package hub

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"h12.me/sej"
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
				// server close?
				break
			}
			go newSession(sock, ws, s).loop()
		}
	}()
	return nil
}

func (s *Server) log(format string, v ...interface{}) {
	if s.ErrChan == nil {
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
	// rw := bufio.NewReadWriter(bufio.NewReader(s.c), bufio.NewWriter(s.c))
	for {
		if err := s.serve(s.c); err != nil {
			if err != errQuit {
				s.error(err)
			}
			break
		}
		// if err := rw.Flush(); err != nil {
		// 	s.error(err)
		// 	break
		// }
	}
}

func (s *session) serve(rw io.ReadWriter) error {
	var req Request
	s.c.SetReadDeadline(time.Now().Add(time.Minute))
	if _, err := req.ReadFrom(rw); err != nil {
		return errors.Wrap(err, "fail to read request")
	}
	switch RequestType(req.Type) {
	case QUIT:
		return errQuit
	case PUT:
		return s.servePut(rw, &req)
	case GET:
		return s.serveError(rw, &req, errors.Errorf("unsupported request type %d", req.Type))
	default:
		return s.serveError(rw, &req, errors.Errorf("unknown request type %d", req.Type))
	}
	return nil
}

func (s *session) servePut(rw io.ReadWriter, req *Request) error {
	writer, err := s.ws.Writer(req.ClientID, req.JournalDir)
	if err != nil {
		return errors.Wrap(err, "fail to get writer for client "+req.ClientID)
	}
	var msg sej.Message
	count := int(req.Count)
	offset := req.Offset
	for offset < writer.Offset() {
		s.c.SetReadDeadline(time.Now().Add(s.Timeout))
		if _, err := msg.ReadFrom(rw); err != nil {
			return s.serveError(rw, req, err)
		}
		count--
		offset++
	}
	if req.Offset != writer.Offset() {
		if err != nil {
			return s.serveError(rw, req, errors.Errorf("offset mismatch: request.offset=%d, writer.offset=%d", req.Offset, writer.Offset()))
		}
	}

	for i := 0; i < count; i++ {
		s.c.SetReadDeadline(time.Now().Add(s.Timeout))
		if _, err := msg.ReadFrom(rw); err != nil {
			return s.serveError(rw, req, err)
		}
		msg.Offset = offset
		if err := writer.Append(&msg); err != nil {
			return s.serveError(rw, req, err)
		}
		offset++
	}
	if err := writer.Flush(); err != nil {
		return s.serveError(rw, req, err)
	}
	if err := writer.Sync(); err != nil {
		return s.serveError(rw, req, err)
	}
	s.writeResp(rw, req, &Response{
		RequestID: req.ID,
	})
	return nil
}

func (s *session) serveError(rw io.ReadWriter, req *Request, err error) error {
	s.writeResp(rw, req, &Response{
		RequestID: req.ID,
		Err:       err.Error(),
	})
	return err
}

func (s *session) writeResp(w io.Writer, req *Request, resp *Response) error {
	s.c.SetWriteDeadline(time.Now().Add(s.Timeout))
	if _, err := resp.WriteTo(w); err != nil {
		return errors.Wrap(err, "fail to write response to "+req.ClientID)
	}
	return nil
}
