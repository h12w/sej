package hub

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"reflect"
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
	c      net.Conn
	enc    *gob.Encoder
	dec    *gob.Decoder
	outBuf *bufio.Writer
	ws     *writers
	*Server
}

func newSession(c net.Conn, ws *writers, s *Server) *session {
	w := bufio.NewWriter(c)
	enc := gob.NewEncoder(w)
	return &session{
		c:      c,
		enc:    enc,
		dec:    gob.NewDecoder(c),
		outBuf: w,
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
	defer s.outBuf.Flush()
	for {
		if err := s.serve(); err != nil {
			if err != errQuit {
				s.error(err)
			}
			break
		}
		if err := s.outBuf.Flush(); err != nil {
			s.error(err)
			break
		}
	}
}

func (s *session) serve() error {
	s.c.SetReadDeadline(time.Now().Add(time.Minute))
	var req proto.Request
	if err := s.dec.Decode(&req); err != nil {
		return errors.Wrap(err, "fail to read request")
	}
	switch body := req.Body.(type) {
	case *proto.Quit:
		return s.serveQuit(&req, body)
	case *proto.Put:
		return s.servePut(&req, body)
	case *proto.Get:
		return s.serveError(&req, errors.New("unsupported request type GET"))
	default:
		return s.serveError(&req, errors.Errorf("unknown request type %v", reflect.TypeOf(req.Body)))
	}
	return nil
}

func (s *session) serveQuit(req *proto.Request, quit *proto.Quit) error {
	s.writeResp(req, &proto.Response{})
	return errQuit
}

func (s *session) servePut(req *proto.Request, put *proto.Put) error {
	writer, err := s.ws.Writer(req.ClientID, put.JournalDir)
	if err != nil {
		return errors.Wrap(err, "fail to get writer for client "+req.ClientID)
	}
	for _, msg := range put.Messages {
		if msg.Offset > writer.Offset() {
			return errors.Wrapf(err, "offset out of order, msg: %d, writer %d", msg.Offset, writer.Offset())
		} else if msg.Offset < writer.Offset() { // redundant
			continue
		}
		if err := writer.Append(&msg); err != nil {
			return s.serveError(req, err)
		}
	}
	if err := writer.Flush(); err != nil {
		return s.serveError(req, err)
	}
	if err := writer.Sync(); err != nil {
		return s.serveError(req, err)
	}
	s.writeResp(req, &proto.Response{})
	return nil
}

func (s *session) serveError(req *proto.Request, err error) error {
	s.writeResp(req, &proto.Response{
		Err: err.Error(),
	})
	return err
}

func (s *session) writeResp(req *proto.Request, resp *proto.Response) error {
	s.c.SetWriteDeadline(time.Now().Add(s.Timeout))
	if err := s.enc.Encode(resp); err != nil {
		return errors.Wrap(err, "fail to write response to "+req.ClientID)
	}
	return nil
}
