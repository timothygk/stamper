package stamper

import (
	"net"
	"sync"

	"github.com/timothygk/stamper/internal/assert"
)

type connType int

const (
	connTypeUnknown connType = 0
	connTypeReplica connType = 1
	connTypeClient  connType = 2
)

type wrappedconn struct {
	net.Conn
	conntype connType
	nodeId   int
	clientId uint64
}

type connEventType int

const (
	connEventTypeClosing       connEventType = 0
	connEventTypeListenConn    connEventType = 1
	connEventTypeAddConn       connEventType = 2
	connEventTypeConnClosed    connEventType = 3
	connEventTypeSendToReplica connEventType = 4
	connEventTypeSendToClient  connEventType = 5
)

type connEvent struct {
	etype      connEventType
	conn       *wrappedconn
	toNodeId   int
	toClientId uint64
	envelope   *Envelope
}

type connManager struct {
	replicaAddrs []string
	createConn   func(toAddr string) (net.Conn, error)
	encdec       EncoderDecoder
	onEnvelope   func(*Envelope)
	replicaConns []*wrappedconn          // allow reusing replica conns to send msg
	clientConns  map[uint64]*wrappedconn // allow reusing client conns to send msg
	connset      map[*wrappedconn]struct{}
	events       chan connEvent
	loopWg       sync.WaitGroup
	listenWg     sync.WaitGroup
	closing      chan struct{}
}

func newConnManager(
	replicaAddrs []string,
	createConn func(string) (net.Conn, error),
	encdec EncoderDecoder,
	onEnvelope func(*Envelope),
) *connManager {
	cm := &connManager{
		replicaAddrs: replicaAddrs,
		createConn:   createConn,
		encdec:       encdec,
		onEnvelope:   onEnvelope,
		replicaConns: make([]*wrappedconn, len(replicaAddrs)),
		clientConns:  make(map[uint64]*wrappedconn),
		connset:      make(map[*wrappedconn]struct{}),
		events:       make(chan connEvent, 1000),
		closing:      make(chan struct{}),
	}
	cm.loopWg.Go(cm.loop)
	return cm
}

func (cm *connManager) Close() error {
	close(cm.closing)
	cm.events <- connEvent{etype: connEventTypeClosing}
	cm.listenWg.Wait()
	cm.loopWg.Wait()
	return nil
}

func (cm *connManager) loop() {
EventLoop:
	for e := range cm.events {
		switch e.etype {
		case connEventTypeClosing:
			for conn := range cm.connset {
				if conn != nil {
					conn.Close()
				}
			}
			break EventLoop

		case connEventTypeListenConn:
			cm.connset[e.conn] = struct{}{}
			cm.listenWg.Go(func() { cm.listen(e.conn) })

		case connEventTypeAddConn:
			if e.conn.conntype == connTypeReplica {
				cm.replicaConns[e.conn.nodeId] = e.conn
			}
			if e.conn.conntype == connTypeClient {
				cm.clientConns[e.conn.clientId] = e.conn
			}

		case connEventTypeConnClosed:
			e.conn.Close()
			if e.conn.conntype == connTypeReplica && cm.replicaConns[e.conn.nodeId] == e.conn {
				cm.replicaConns[e.conn.nodeId] = nil
			}
			if e.conn.conntype == connTypeClient {
				if currentconn := cm.clientConns[e.conn.clientId]; currentconn == e.conn {
					delete(cm.clientConns, e.conn.clientId)
				}
			}
			delete(cm.connset, e.conn)

		case connEventTypeSendToReplica:
			if cm.replicaConns[e.toNodeId] == nil {
				conn, err := cm.createConn(cm.replicaAddrs[e.toNodeId])
				if err != nil {
					// TODO: log this
					continue
				}
				wconn := &wrappedconn{
					Conn:     conn,
					conntype: connTypeReplica,
					nodeId:   e.toNodeId,
				}
				cm.connset[wconn] = struct{}{}
				cm.listenWg.Go(func() { cm.listen(wconn) })
				cm.replicaConns[e.toNodeId] = wconn
			}

			// TODO: add write timeout
			err := cm.encdec.Encode(cm.replicaConns[e.toNodeId], e.envelope)
			if err != nil {
				// TODO: log this
				cm.replicaConns[e.toNodeId].Close()
				delete(cm.connset, cm.replicaConns[e.toNodeId])
				cm.replicaConns[e.toNodeId] = nil
				continue
			}

		case connEventTypeSendToClient:
			if conn := cm.clientConns[e.toClientId]; conn != nil {
				err := cm.encdec.Encode(conn, e.envelope)
				if err != nil {
					// TODO: log this
					conn.Close()
					delete(cm.connset, conn)
					delete(cm.clientConns, e.toClientId)
					continue
				}
			}

		default:
			assert.Assert(false, "Unreached state")
		}
	}
}

func (cm *connManager) accept(conn net.Conn) {
	wconn := &wrappedconn{Conn: conn, conntype: connTypeUnknown}
	cm.events <- connEvent{
		etype: connEventTypeListenConn,
		conn:  wconn,
	}
}

func (cm *connManager) sendTo(toNodeId int, envelope *Envelope) {
	cm.events <- connEvent{
		etype:    connEventTypeSendToReplica,
		toNodeId: toNodeId,
		envelope: envelope,
	}
}

func (cm *connManager) sendReply(clientId uint64, envelope *Envelope) {
	cm.events <- connEvent{
		etype:      connEventTypeSendToClient,
		toClientId: clientId,
		envelope:   envelope,
	}
}

func (cm *connManager) listen(conn *wrappedconn) {
	defer func() {
		cm.events <- connEvent{
			etype: connEventTypeConnClosed,
			conn:  conn,
		}
	}()
	for {
		select {
		case <-cm.closing:
			return
		default:
			envelope, err := cm.encdec.Decode(conn.Conn)
			if err != nil {
				return
			}

			assert.Assert(envelope != nil, "envelope should not be nil")

			if conn.conntype == connTypeUnknown {
				if envelope.Cmd == CmdTypeRequest {
					request, ok := envelope.Payload.(*Request)
					assert.Assert(ok, "should be able to cast envelope payload to *Request type")
					conn.conntype = connTypeClient
					conn.clientId = request.ClientId
					cm.events <- connEvent{
						etype: connEventTypeAddConn,
						conn:  conn,
					}
				} else if envelope.FromNodeId >= 0 {
					conn.conntype = connTypeReplica
					conn.nodeId = envelope.FromNodeId
					cm.events <- connEvent{
						etype: connEventTypeAddConn,
						conn:  conn,
					}
				}
			}

			cm.onEnvelope(envelope)
		}
	}
}
