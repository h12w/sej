package hub

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
		Dir     string
		Timeout time.Duration
		ErrChan chan error
		LogChan chan string
	}
	Handler interface {
		Handle(msg *sej.Message) (uint64, error)
	}
)

func (s *Server) start() error {
	ws := newWriters(s.Dir)
	c, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	defer c.Close()
	for {
		sock, err := c.Accept()
		if err != nil {
			s.error(err)
			continue
		}
		go newSession(sock, ws, s).loop()
	}
}

func (s *Server) error(err error) {
	if s.ErrChan == nil {
		return
	}
	select {
	case s.ErrChan <- err:
	default:
	}
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
	w := bufio.NewWriter(s.c)
	rw := bufio.NewReadWriter(bufio.NewReader(s.c), w)
	for {
		s.serve(rw)
		w.Flush()
	}
}

func (s *session) serve(rw io.ReadWriter) {
	var req Request
	s.c.SetReadDeadline(time.Now().Add(s.Timeout))
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
	writer, err := s.ws.Writer(req.ClientID, req.JournalDir)
	if err != nil {
		s.error(errors.Wrap(err, "fail to get writer for client "+req.ClientID))
		return
	}
	var msg sej.Message
	for req.Offset < writer.Offset() {
		s.c.SetReadDeadline(time.Now().Add(s.Timeout))
		_, err := msg.ReadFrom(rw)
		if err != nil {
			s.serveError(rw, req, err)
			return
		}
	}
	if req.Offset != writer.Offset() {
		if err != nil {
			s.serveError(rw, req, errors.Errorf("offset mismatch: request.offset=%d, writer.offset=%d", req.Offset, writer.Offset()))
			return
		}
	}

	for {
		s.c.SetReadDeadline(time.Now().Add(s.Timeout))
		_, err := msg.ReadFrom(rw)
		if err != nil {
			s.serveError(rw, req, err)
			return
		}
		if msg.IsNull() {
			break
		}
		if err := writer.Append(&msg); err != nil {
			s.serveError(rw, req, err)
			return
		}
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
	s.c.SetWriteDeadline(time.Now().Add(s.Timeout))
	if _, err := resp.WriteTo(w); err != nil {
		s.error(errors.Wrap(err, "fail to write response to "+req.ClientID))
	}
}
