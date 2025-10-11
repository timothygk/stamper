package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/timothygk/stamper/internal/assert"
	"github.com/timothygk/stamper/internal/stamper"
	"github.com/timothygk/stamper/internal/timepkg"
)

type eventType int

const (
	eventTypeClientClosing eventType = 0
	eventTypeConnClosed    eventType = 1
	eventTypeSend          eventType = 2
	eventTypeRecv          eventType = 3
)

func (e eventType) String() string {
	switch e {
	case eventTypeClientClosing:
		return "client_closing"
	case eventTypeConnClosed:
		return "conn_closed"
	case eventTypeSend:
		return "send"
	case eventTypeRecv:
		return "recv"
	default:
		return "UNKNOWN"
	}
}

type event struct {
	etype        eventType
	serverIdx    int
	request      *stamper.Request  // for eventTypeSend
	recvEnvelope *stamper.Envelope // for eventTypeRecv
}

func (e *event) String() string {
	var requestBuf bytes.Buffer
	if e.request != nil {
		enc := json.NewEncoder(&requestBuf)
		enc.SetIndent("", "")
		enc.Encode(e.request)
		requestBuf.Truncate(requestBuf.Len() - 1)
	} else {
		requestBuf.WriteString("<nil>")
	}
	return fmt.Sprintf("etype:%s, serverIdx:%d, request:%s, recvEnvelope:%s",
		e.etype.String(),
		e.serverIdx,
		requestBuf.String(),
		e.recvEnvelope.JsonStr(),
	)
}

var ErrAnotherRequestInflight = errors.New("there's another request inflight")

type ClientConfig struct {
	ClientId      uint64
	ServerAddrs   []string
	RetryDuration time.Duration
}

type Client struct {
	config     ClientConfig
	tt         timepkg.Time
	encdec     stamper.EncoderDecoder
	createConn func(toAddr string) (net.Conn, error)
	events     chan event
	closing    chan struct{}
	closed     chan struct{}

	serverConns  []net.Conn
	viewId       uint64
	isInflight   atomic.Bool
	curRequestId uint64
	replyCh      chan *stamper.Reply
}

func NewClient(
	config ClientConfig,
	tt timepkg.Time,
	encdec stamper.EncoderDecoder,
	createConn func(toAddr string) (net.Conn, error),
) *Client {
	c := &Client{
		config:       config,
		tt:           tt,
		encdec:       encdec,
		createConn:   createConn,
		events:       make(chan event, 1000),
		closing:      make(chan struct{}),
		closed:       make(chan struct{}),
		serverConns:  make([]net.Conn, len(config.ServerAddrs)),
		viewId:       0,
		isInflight:   atomic.Bool{},
		curRequestId: 1,
		replyCh:      make(chan *stamper.Reply),
	}
	go c.loop()
	return c
}

func (c *Client) Close() error {
	close(c.closing)
	c.events <- event{etype: eventTypeClientClosing}
	<-c.closed
	return nil
}

func (c *Client) Request(body []byte) ([]byte, error) {
	if !c.isInflight.CompareAndSwap(false, true) {
		return nil, ErrAnotherRequestInflight
	}
	defer c.isInflight.Store(false)

	request := &stamper.Request{
		ClientId:    c.config.ClientId,
		RequestId:   c.curRequestId,
		RequestBody: body,
	}
	c.send(int(c.viewId%uint64(len(c.config.ServerAddrs))), request)

	retryTicker := c.tt.Tick(c.config.RetryDuration)

	// wait for reply...
	for {
		select {
		case <-retryTicker:
			// broadcast
			for i := range c.config.ServerAddrs {
				c.send(i, request)
			}
		case reply := <-c.replyCh:
			if reply.RequestId != c.curRequestId { // ignore old replies
				continue
			}
			if reply.Error == stamper.ProtocolErrorExpiredRequestId {
				c.viewId = reply.ViewId
				c.curRequestId += 1000 // TODO: just do a huge step?
				request = &stamper.Request{
					ClientId:    c.config.ClientId,
					RequestId:   c.curRequestId,
					RequestBody: body,
				}
				c.send(int(c.viewId%uint64(len(c.config.ServerAddrs))), request)
				continue
			}

			c.curRequestId++
			if reply.Error != stamper.ProtocolErrorNone {
				return nil, reply.Error
			}
			return reply.ResponseBody, nil
		case <-c.closing:
			return nil, io.EOF
		}
	}
}

func (c *Client) loop() {
EventLoop:
	for e := range c.events {
		// fmt.Printf("Event %s\n", e.String())
		switch e.etype {
		case eventTypeClientClosing:
			for _, conn := range c.serverConns {
				if conn != nil {
					conn.Close()
				}
			}
			break EventLoop // stop processing
		case eventTypeConnClosed:
			err := c.serverConns[e.serverIdx].Close()
			if err != nil {
				// TODO: log
			}
			c.serverConns[e.serverIdx] = nil // dereference
		case eventTypeSend:
			if e.request.RequestId != c.curRequestId {
				continue
			}

			// get conn
			conn, err := c.getConn(e.serverIdx)
			if err != nil {
				// TODO: log
				return
			}

			// send through the network
			err = c.encdec.Encode(conn, &stamper.Envelope{
				Cmd:     stamper.CmdTypeRequest,
				Payload: e.request,
			})
			if err != nil {
				// TODO: log
				return
			}
		case eventTypeRecv:
			assert.Assertf(e.recvEnvelope.Cmd == stamper.CmdTypeReply, "eventTypeRecv should receive CmdTypeReply, got: %s", e.recvEnvelope.Cmd.String())
			reply, ok := e.recvEnvelope.Payload.(*stamper.Reply)
			assert.Assert(ok, "Should be able to upcast Payload to *Reply type")
			assert.Assert(reply != nil, "Reply should not be nil")

			if reply.Error == stamper.ProtocolErrorReplicaNotReady ||
				reply.Error == stamper.ProtocolErrorNotPrimaryNode {
				continue // keep retrying
			}
			if reply.Error == stamper.ProtocolErrorExpiredRequestId {
				c.replyCh <- reply // stop retrying, let the request thread handle it
				continue
			}

			c.replyCh <- reply
		}
	}

	close(c.closed)
}

func (c *Client) send(serverIdx int, request *stamper.Request) {
	c.events <- event{
		etype:     eventTypeSend,
		serverIdx: serverIdx,
		request:   request,
	}
}

func (c *Client) listen(serverIdx int, conn net.Conn) {
	defer func() {
		c.events <- event{
			etype:     eventTypeConnClosed,
			serverIdx: serverIdx,
		} // handled by the Run() goroutine
	}()
	for {
		select {
		case <-c.closing:
			return
		default:
			envelope, err := c.encdec.Decode(conn)
			if err != nil {
				// TODO: log
				return
			}
			c.events <- event{
				etype:        eventTypeRecv,
				serverIdx:    serverIdx,
				recvEnvelope: envelope,
			}
		}
	}
}

func (c *Client) getConn(serverIdx int) (net.Conn, error) {
	if c.serverConns[serverIdx] == nil {
		conn, err := c.createConn(c.config.ServerAddrs[serverIdx])
		if err != nil {
			return nil, err
		}
		c.serverConns[serverIdx] = conn
		go c.listen(serverIdx, conn)
	}
	return c.serverConns[serverIdx], nil
}
