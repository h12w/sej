package hub

import (
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"h12.me/sej"
	"h12.me/sej/hub/proto"
)

type Client struct {
	Addr       string
	ClientID   string
	JournalDir string
	Timeout    time.Duration

	conn net.Conn
	mu   sync.Mutex
}

func (c *Client) Quit() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}
	req := proto.Request{
		Title: proto.RequestTitle{
			Verb:     uint8(proto.QUIT),
			ClientID: c.ClientID,
		},
		Header: &proto.Quit{
			JournalDir: c.JournalDir,
		},
	}
	c.conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	if _, err := req.WriteTo(c.conn); err != nil {
		return err
	}

	var resp proto.Response
	c.conn.SetReadDeadline(time.Now().Add(c.Timeout))
	if _, err := resp.ReadFrom(c.conn); err != nil {
		c.close()
		return err
	}
	if resp.Err != "" {
		return errors.New(resp.Err)
	}
	return nil
}

func (c *Client) Send(messages []sej.Message) error {
	if len(messages) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		var err error
		c.conn, err = net.DialTimeout("tcp", c.Addr, c.Timeout)
		if err != nil {
			return errors.Wrap(err, "fail to connect to sej hub "+c.Addr)
		}
	}
	req := proto.Request{
		Title: proto.RequestTitle{
			Verb:     uint8(proto.PUT),
			ClientID: c.ClientID,
		},
		Header: &proto.Put{
			JournalDir: c.JournalDir,
		},
		Messages: messages,
	}
	c.conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	if _, err := req.WriteTo(c.conn); err != nil {
		return err
	}

	var resp proto.Response
	c.conn.SetReadDeadline(time.Now().Add(c.Timeout))
	if _, err := resp.ReadFrom(c.conn); err != nil {
		c.close()
		return err
	}
	if resp.Err != "" {
		return errors.New(resp.Err)
	}
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.close()
}

func (c *Client) close() error {
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return err
		}
		c.conn = nil
	}
	return nil
}
