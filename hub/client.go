package hub

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"h12.io/sej"
)

type Client struct {
	Addr       string
	ClientID   string
	JournalDir string
	Timeout    time.Duration

	conn *grpc.ClientConn
	c    HubClient
	mu   sync.Mutex
}

func (c *Client) getClient() (HubClient, error) {
	c.mu.Lock()
	if c.c != nil {
		c.mu.Unlock()
		return c.c, nil
	}
	var err error
	c.conn, err = grpc.Dial(
		c.Addr,
		grpc.WithTimeout(c.Timeout),
		grpc.WithInsecure(),
	)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.c = NewHubClient(c.conn)
	client := c.c
	c.mu.Unlock()
	return client, nil
}

func (c *Client) Send(messages []sej.Message) error {
	if len(messages) == 0 {
		return nil
	}

	client, err := c.getClient()
	if err != nil {
		return err
	}

	ms := make([]*Message, len(messages))
	for i := range ms {
		ms[i] = &Message{
			Offset:    messages[i].Offset,
			Timestamp: messages[i].Timestamp.UnixNano(),
			Type:      uint32(messages[i].Type),
			Key:       messages[i].Key,
			Value:     messages[i].Value,
		}
	}

	_, err = client.Put(context.TODO(), &PutRequest{
		ClientID:   c.ClientID,
		JournalDir: c.JournalDir,
		Messages:   ms,
	})
	return err
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close()
}
