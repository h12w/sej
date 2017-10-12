package hub

import (
	"bufio"
	"encoding/gob"
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

	enc    *gob.Encoder
	dec    *gob.Decoder
	outBuf *bufio.Writer
	conn   net.Conn
	mu     sync.Mutex
}

func (c *Client) Quit() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}
	req := proto.Request{
		ClientID: c.ClientID,
		Body: &proto.Quit{
			JournalDir: c.JournalDir,
		},
	}
	c.conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	if err := c.enc.Encode(&req); err != nil {
		return err
	}
	if err := c.outBuf.Flush(); err != nil {
		return err
	}

	var resp proto.Response
	c.conn.SetReadDeadline(time.Now().Add(c.Timeout))
	if err := c.dec.Decode(&resp); err != nil {
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
		c.outBuf = bufio.NewWriter(c.conn)
		c.enc = gob.NewEncoder(c.outBuf)
		c.dec = gob.NewDecoder(c.conn)
	}
	req := proto.Request{
		ClientID: c.ClientID,
		Body: &proto.Put{
			JournalDir: c.JournalDir,
			Messages:   messages,
		},
	}
	c.conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	if err := c.enc.Encode(&req); err != nil {
		return err
	}
	if err := c.outBuf.Flush(); err != nil {
		return err
	}

	var resp proto.Response
	c.conn.SetReadDeadline(time.Now().Add(c.Timeout))
	if err := c.dec.Decode(&resp); err != nil {
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
		c.dec = nil
		c.enc = nil
		c.outBuf = nil
	}
	return nil
}
