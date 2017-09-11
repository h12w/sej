package wire

import (
	"bufio"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"h12.me/sej"
)

type (
	Server struct {
		Addr    string
		Timeout time.Duration
		ErrChan chan error
		LogChan chan string
	}
	Handler interface {
		Handle(msg *sej.Message) (uint64, error)
	}
)

func (s *Server) start() {
	c, err := net.Listen("tcp", s.Addr)
	if err != nil {
		panic(err)
	}
	defer c.Close()
	for {
		sock, err := c.Accept()
		if err != nil {
			// Error(err)
			continue
		}
		go newSession(sock, s.Timeout).loop()
	}
}

type session struct {
	c       net.Conn
	timeout time.Duration
	errChan chan error
}

func newSession(c net.Conn, timeout time.Duration) *session {
	return &session{
		c:       c,
		timeout: timeout,
	}
}

func (s *session) error(err error) {
	if s.errChan == nil {
		return
	}
	select {
	case s.errChan <- err:
	default:
	}
}

func (s *session) loop() {
	defer func() {
		if err := s.c.Close(); err != nil {
			s.error(errors.Wrap(err, "fail to close client socket"))
		}
	}()
	w := bufio.NewWriter(s.c)
	rw := bufio.NewReadWriter(bufio.NewReader(s.c), w)
	for {
		s.serve(rw)
		w.Flush()
	}
}

func (s *session) serve(rw io.ReadWriter) {
	var req Request
	s.c.SetReadDeadline(time.Now().Add(s.timeout))
	_, err := req.ReadFrom(rw)
	if err != nil {
		s.error(errors.Wrap(err, "fail to read request"))
		return
	}
	switch RequestType(req.Type) {
	case PUT:
		s.servePut(rw, &req)
	// case GET:
	default:
		s.serveError(rw, &req, errors.Wrapf(err, "unknown request type %d", req.Type))
	}
}

func (s *session) servePut(rw io.ReadWriter, req *Request) {
	var msg sej.Message
	for {
		s.c.SetReadDeadline(time.Now().Add(s.timeout))
		_, err := msg.ReadFrom(rw)
		if err != nil {
			s.serveError(rw, req, err)
			return
		}
		if msg.IsNull() {
			break
		}
		// TODO: write message to files
	}
	s.writeResp(rw, req, &Response{
		RequestID: req.ID,
	})
}

func (s *session) serveError(rw io.ReadWriter, req *Request, err error) {
	s.error(err)
	s.writeResp(rw, req, &Response{
		RequestID: req.ID,
		Err:       err.Error(),
	})
}

func (s *session) writeResp(w io.Writer, req *Request, resp *Response) {
	s.c.SetWriteDeadline(time.Now().Add(s.timeout))
	if _, err := resp.WriteTo(w); err != nil {
		s.error(errors.Wrap(err, "fail to write response to "+req.ClientID))
	}
}
