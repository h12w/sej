package hub

import (
	"bytes"
	"math"
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

func (c *Client) Quit() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.serial++

	if c.conn == nil {
		return nil
	}
	req := Request{
		ID:         c.serial,
		Type:       uint8(QUIT),
		ClientID:   c.ClientID,
		JournalDir: c.JournalDir,
	}
	c.conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	if _, err := req.WriteTo(c.conn); err != nil {
		return err
	}
	return nil
}

func (c *Client) Send(messages []sej.Message) error {
	if len(messages) == 0 {
		return nil
	} else if len(messages) > math.MaxUint16 {
		return errors.New("message count must be less than 65535")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.serial++

	if c.conn == nil {
		var err error
		// c.conn, err = net.DialTimeout("tcp", c.Addr, c.Timeout)
		c.conn, err = net.Dial("tcp", c.Addr)
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
		Count:      uint16(len(messages)),
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
