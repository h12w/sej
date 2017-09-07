package sejutil

import (
	"fmt"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"h12.me/sej"
)

type (
	Consumer struct {
		Dir           string
		Offset        string
		DefaultOffset sej.DefaultOffset
		Timeout       time.Duration
		Handler       Handler
		ErrChan       chan error
		LogChan       chan string

		stopChan chan chan bool
		offset   *sej.Offset
		scanner  *sej.Scanner
	}
	Handler interface {
		Handle(msg *sej.Message) (uint64, error)
	}
)

func (c *Consumer) log(format string, v ...interface{}) {
	if c.LogChan != nil {
		select {
		case c.LogChan <- fmt.Sprintf(format+"(%s, %s)", append(v, c.Dir, c.Offset)...):
		default:
		}
	}
}

func (c *Consumer) error(err error, format string, v ...interface{}) {
	if c.ErrChan != nil {
		select {
		case c.ErrChan <- errors.Wrapf(err, format+"(%s, %s)", append(v, c.Dir, c.Offset)...):
		default:
		}
	}
}

func (c *Consumer) Stop() error {
	stoppedChan := make(chan bool)
	c.stopChan <- stoppedChan
	<-stoppedChan
	return nil
}

func (c *Consumer) Start() (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.New(fmt.Sprint(e))
		}
	}()
	if err := c.init(); err != nil {
		return err
	}
	c.log("consumer started")

	var stoppedChan chan bool
consumerLoop:
	for {
		start := c.scanner.Offset()
		for c.scanner.Scan() {
			msg := c.scanner.Message()
			committed := c.handle(msg)
			if committed >= start {
				start = c.commit(committed + 1)
			}
		}
		committed := c.handle(nil)
		if committed >= start {
			start = c.commit(committed + 1)
		}
		if err := c.scanner.Err(); err != nil && err != sej.ErrTimeout {
			c.resetScanner() // reset scanner if journal is truncated
		}
		select {
		case stoppedChan = <-c.stopChan:
			break consumerLoop
		default:
		}
		runtime.Gosched()
	}
	c.close()

	c.log("consumer stopped")
	stoppedChan <- true
	return nil
}

func (c *Consumer) handle(msg *sej.Message) uint64 {
	for {
		committed, err := c.Handler.Handle(msg)
		if err != nil {
			c.error(err, "fail to handle message")
			time.Sleep(c.Timeout)
			continue
		}
		return committed
	}
}

func (c *Consumer) commit(offset uint64) uint64 {
	for {
		if err := c.offset.Commit(offset); err != nil {
			c.error(err, "")
			time.Sleep(c.Timeout)
			continue
		}
		return offset
	}
}

func (c *Consumer) resetScanner() {
	c.scanner.Close()
	for {
		var err error
		c.scanner, err = sej.NewScanner(c.Dir, c.offset.Value())
		if err != nil {
			c.error(err, "fail to reset scanner")
			time.Sleep(c.Timeout)
			continue
		}
		c.scanner.Timeout = c.Timeout
		c.log("scanner restarted at %d", c.offset.Value())
		return
	}
}

func (c *Consumer) init() error {
	var err error
	c.stopChan = make(chan chan bool, 1)
	c.offset, err = sej.NewOffset(c.Dir, c.Offset, c.DefaultOffset)
	if err != nil {
		return err
	}
	c.scanner, err = sej.NewScanner(c.Dir, c.offset.Value())
	if err != nil {
		return err
	}
	c.scanner.Timeout = c.Timeout
	return nil
}

func (c *Consumer) close() {
	c.scanner.Close()
	c.offset.Close()
}
