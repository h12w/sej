package hub

import (
	"bytes"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"h12.me/sej"
)

type Client struct {
	Addr       string
	ClientID   string
	JournalDir string
	Timeout    time.Duration

	serial uint64
	conn   net.Conn
	mu     sync.Mutex
}

func (c *Client) Send(messages []sej.Message) error {
	if len(messages) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.serial++

	if c.conn == nil {
		var err error
		c.conn, err = net.DialTimeout("tcp", c.Addr, c.Timeout)
		if err != nil {
			return errors.Wrap(err, "fail to connect to sej hub "+c.Addr)
		}
	}
	var buf bytes.Buffer
	req := Request{
		ID:         c.serial,
		Type:       uint8(PUT),
		ClientID:   c.ClientID,
		JournalDir: c.JournalDir,
		Offset:     messages[0].Offset,
	}
	if _, err := req.WriteTo(&buf); err != nil {
		return err
	}
	smallBuf := make([]byte, 8)
	for i := range messages {
		if _, err := sej.WriteMessage(&buf, smallBuf, &messages[i]); err != nil {
			return err
		}
	}
	c.conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		c.close()
		return err
	}

	var resp Response
	c.conn.SetReadDeadline(time.Now().Add(c.Timeout))
	if _, err := resp.ReadFrom(c.conn); err != nil {
		c.close()
		return err
	}
	if resp.RequestID != req.ID {
		c.close()
		return errors.New("request id mismatch")
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
