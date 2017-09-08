package wire

import (
	"encoding/gob"
	"net"

	"github.com/pkg/errors"
	"h12.me/sej"
)

type (
	Server struct {
		Addr    string
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
		go newSession(sock).serve()
	}
}

type session struct {
	c       net.Conn
	errChan chan error
	dec     *gob.Decoder
}

func newSession(c net.Conn) *session {
	return &session{
		c:   c,
		dec: gob.NewDecoder(c),
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

func (s *session) serve() {
	defer func() {
		if err := s.c.Close(); err != nil {
			s.error(errors.Wrap(err, "fail to close client socket"))
		}
	}()
	// r := bufio.NewReader(s.c)
}
