package stamper

import (
	"crypto/sha256"
	"fmt"
	"math/rand/v2"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/timothygk/stamper/internal/assert"
	"github.com/timothygk/stamper/internal/timepkg"
)

type replicaStatus int

const (
	replicaStatusNormal     replicaStatus = 1
	replicaStatusViewChange replicaStatus = 2
	replicaStatusRecovering replicaStatus = 3
)

type logEntry struct {
	LogId     uint64
	ClientId  uint64
	RequestId uint64
	Body      []byte
}

type clientTable struct {
	requestId uint64 // last request-number
	reply     *Reply
}

type eventType int

const (
	eventTypeReplicaClosing eventType = 0
	eventTypeSend           eventType = 1
	eventTypeCommitDelay    eventType = 2
	eventTypeMsgRecv        eventType = 100
)

func (e eventType) String() string {
	switch e {
	case eventTypeReplicaClosing:
		return "replica_closing"
	case eventTypeSend:
		return "send"
	case eventTypeCommitDelay:
		return "commit_delay"
	case eventTypeMsgRecv:
		return "msg_recv"
	default:
		return "UNKNOWN"
	}
}

type event struct {
	etype    eventType
	envelope *Envelope
	conn     net.Conn
	nodeId   int
	commitId uint64
}

func (e *event) String() string {
	return fmt.Sprintf(
		"etype:%s, envelope:%s, conn:%p, nodeId:%d, commitId:%d",
		e.etype.String(),
		e.envelope.JsonStr(),
		e.conn,
		e.nodeId,
		e.commitId,
	)
}

type ReplicaConfig struct {
	SendRetryDuration   time.Duration
	CommitDelayDuration time.Duration
	NodeId              int
	ServerAddrs         []string // configurations, currently it is static
}

type Replica struct {
	// config
	config ReplicaConfig

	// time
	tt timepkg.Time

	// network/communication related
	encdec      EncoderDecoder
	clientConns map[uint64]net.Conn // map of client id -> connection
	events      chan event
	connCloseWg sync.WaitGroup
	closing     chan struct{}
	closed      chan struct{}
	createConn  func(toAddr string) (net.Conn, error)
	envelopeIdR *rand.Rand

	// states
	viewId    uint64
	status    replicaStatus
	lastLogId uint64
	logs      []logEntry
	commitId  uint64
	clients   map[uint64]*clientTable
	upcall    func([]byte) []byte

	// internal states
	waitAck           map[uint64]struct{} // set of envelopeId
	waitingPrepares   map[uint64]*Prepare // logId -> prepare request
	lastPreparedLogId []uint64            // node -> last prepared log id
	maxSentCommitId   uint64
}

// NewReplica create a new replica
func NewReplica(
	config ReplicaConfig,
	tt timepkg.Time,
	encdec EncoderDecoder,
	createConn func(string) (net.Conn, error),
	r *rand.Rand,
	upcall func([]byte) []byte,
) *Replica {
	replica := &Replica{
		config:            config,
		tt:                tt,
		encdec:            encdec,
		clientConns:       make(map[uint64]net.Conn),
		events:            make(chan event, 1000),
		closing:           make(chan struct{}),
		closed:            make(chan struct{}),
		createConn:        createConn,
		envelopeIdR:       r,
		viewId:            0,
		status:            replicaStatusNormal, // TODO: should start with recovering??
		lastLogId:         0,
		logs:              make([]logEntry, 0, 1000),
		commitId:          0,
		clients:           make(map[uint64]*clientTable),
		upcall:            upcall,
		waitAck:           make(map[uint64]struct{}),
		waitingPrepares:   make(map[uint64]*Prepare),
		lastPreparedLogId: make([]uint64, len(config.ServerAddrs)),
		maxSentCommitId:   0,
	}
	go replica.loop()
	return replica
}

// GetState get the replica state to compare with another state
func (r *Replica) GetState() string {
	h := sha256.New()
	for i := range r.logs {
		h.Write(r.logs[i].Body)
	}
	return fmt.Sprintf("status:%d, viewId:%d, commitId:%d, lastLogId:%d, loghash:%x",
		r.status,
		r.viewId,
		r.commitId,
		r.lastLogId,
		h.Sum(nil),
	)
}

// Accept accept a network connection
func (r *Replica) Accept(conn net.Conn) {
	// fmt.Printf("Accepted connection %p from %s\n", conn, conn.RemoteAddr().String())
	r.connCloseWg.Go(func() {
		defer conn.Close()
		for {
			select {
			case <-r.closing:
				return
			default:
				envelope, err := r.encdec.Decode(conn)
				if err != nil {
					return
				}
				assert.Assert(envelope != nil, "envelope should not be nil")
				r.updateConn(envelope, conn)
				r.events <- event{
					etype:    eventTypeMsgRecv,
					envelope: envelope,
					conn:     conn,
				}
			}
		}
	})
}

// Close closes the handler and all its states
func (r *Replica) Close() error {
	close(r.closing)
	r.events <- event{etype: eventTypeReplicaClosing}
	r.connCloseWg.Wait()
	clear(r.clientConns)
	<-r.closed
	return nil
}

func (r *Replica) loop() {
EventLoop:
	for e := range r.events {
		// fmt.Printf("Event %s\n", e.String())
		switch e.etype {
		case eventTypeReplicaClosing:
			break EventLoop
		case eventTypeSend:
			r.handleSend(e.nodeId, e.envelope)
		case eventTypeCommitDelay:
			r.handleCommitDelay(e.commitId)
		case eventTypeMsgRecv:
			r.handleMsgRecv(e.conn, e.envelope)
		default:
			assert.Assertf(false, "Unknown event type %d", e.etype)
		}
	}
	close(r.closed)
}

func (r *Replica) newEnvelope(cmd CmdType, payload any) *Envelope {
	return &Envelope{
		Cmd:        cmd,
		EnvelopeId: r.envelopeIdR.Uint64(),
		Payload:    payload,
	}
}

func (r *Replica) updateConn(envelope *Envelope, conn net.Conn) {
	if envelope.Cmd == CmdTypeRequest {
		request, ok := envelope.Payload.(*Request)
		assert.Assert(ok, "should be able to cast envelope payload to *Request type")
		r.clientConns[request.ClientId] = conn // update to the latest connection seen
	}
}

func (r *Replica) sendReply(clientId uint64, reply *Reply) {
	conn := r.clientConns[clientId]
	if conn != nil {
		envelope := r.newEnvelope(CmdTypeReply, reply)
		err := r.encdec.Encode(conn, envelope)
		if err != nil {
			// on failure, client should ask for the reply again
			delete(r.clientConns, clientId)
			conn.Close()
		}
	}
}

func (r *Replica) sendTo(nodeId int, cmd CmdType, payload any) {
	// create envlope & wait for ack
	envelope := r.newEnvelope(cmd, payload)
	r.waitAck[envelope.EnvelopeId] = struct{}{}
	r.events <- event{
		etype:    eventTypeSend,
		envelope: envelope,
		conn:     nil,
		nodeId:   nodeId,
	}
}

func (r *Replica) broadcast(cmd CmdType, payload any) {
	for nodeId := range r.config.ServerAddrs {
		if nodeId != r.config.NodeId {
			r.sendTo(nodeId, cmd, payload)
		}
	}
}

func (r *Replica) handleSend(nodeId int, envelope *Envelope) {
	assert.Assertf(nodeId != r.config.NodeId, "Should not send payload to the same node, node_id:%d\n", nodeId)
	assert.Assert(envelope != nil, "Should not send with nil envelope")

	// check if it is ack-ed
	if _, ok := r.waitAck[envelope.EnvelopeId]; !ok {
		return
	}

	// defer to retry
	defer r.tt.AfterFunc(r.config.SendRetryDuration, func() {
		r.events <- event{
			etype:    eventTypeSend,
			envelope: envelope,
			conn:     nil,
			nodeId:   nodeId,
		}
	})

	conn, err := r.createConn(r.config.ServerAddrs[nodeId])
	if err != nil {
		// TODO: log this
		return
	}

	err = r.encdec.Encode(conn, envelope)
	if err != nil {
		// TODO: log this
		conn.Close()
		return
	}

	go func() {
		// poor man simple accept logic to wait for ack
		defer conn.Close()
		envelope, err := r.encdec.Decode(conn)
		if err != nil {
			// TODO: log
			return
		}
		r.events <- event{
			etype:    eventTypeMsgRecv,
			envelope: envelope,
		}
	}()
}

func (r *Replica) handleCommitDelay(commitId uint64) {
	assert.Assert(commitId <= r.commitId, "On commit, the src commit id should be before or equal to current commit id")

	if r.maxSentCommitId >= commitId {
		return
	}

	// broadcast the latest commit rather than the requested commit id
	r.broadcast(CmdTypeCommit, &Commit{
		ViewId:   r.viewId,
		CommitId: r.commitId,
	})
	r.maxSentCommitId = r.commitId
}

func (r *Replica) handleMsgRecv(conn net.Conn, envelope *Envelope) {
	assert.Assert(envelope != nil, "envelope should not be nil")

	switch envelope.Cmd {
	case CmdTypeRequest:
		request, ok := envelope.Payload.(*Request)
		assert.Assert(ok, "should be able to cast envelope payload to *Request type")
		r.clientConns[request.ClientId] = conn // add clientId -> connection mapping
		r.handleRequest(request)
		return // continue to skip sending ack
	case CmdTypePrepare:
		prepare, ok := envelope.Payload.(*Prepare)
		assert.Assert(ok, "should be able to cast envelope payload to *Prepare type")
		r.handlePrepare(prepare)
	case CmdTypePrepareOk:
		prepareOk, ok := envelope.Payload.(*PrepareOk)
		assert.Assert(ok, "should be able to cast envelope payload to *PrepareOk type")
		r.handlePrepareOk(prepareOk)
	case CmdTypeReply:
		assert.Assert(false, "reply should not be sent to a server node")
	case CmdTypeCommit:
		commit, ok := envelope.Payload.(*Commit)
		assert.Assert(ok, "should be able to cast envelope payload to *Commit type")
		r.handleCommit(commit)
	case CmdTypeAck:
		ack, ok := envelope.Payload.(*Ack)
		assert.Assert(ok, "should be able to cast envelope payload to *Ack type")
		delete(r.waitAck, ack.EnvelopeId)
		return // continue to skip sending ack
	default:
		assert.Assertf(false, "Unknown envelope cmd %d", envelope.Cmd)
	}

	// fire and forget the ack
	err := r.encdec.Encode(conn, r.newEnvelope(CmdTypeAck, &Ack{envelope.EnvelopeId}))
	if err != nil {
		// TODO: log this error
	}
}

func (r *Replica) handleRequest(request *Request) {
	assert.Assert(request != nil, "request should not be nil")

	if r.status != replicaStatusNormal {
		// replica is not ready to accept request
		r.sendReply(request.ClientId, &Reply{
			ViewId:    r.viewId,
			RequestId: request.RequestId,
			Error:     ProtocolErrorReplicaNotReady,
		})
		return
	}

	if r.primaryNode() != r.config.NodeId {
		// not the primary node
		r.sendReply(request.ClientId, &Reply{
			ViewId:    r.viewId,
			RequestId: request.RequestId,
			Error:     ProtocolErrorNotPrimaryNode,
		})
		return
	}

	// process from client table
	clientInfo := r.clients[request.ClientId]
	if clientInfo != nil {
		if request.RequestId < clientInfo.requestId {
			r.sendReply(request.ClientId, &Reply{
				ViewId:    r.viewId,
				RequestId: request.RequestId,
				Error:     ProtocolErrorExpiredRequestId,
			})
			return // drop the request
		}
		if request.RequestId == clientInfo.requestId {
			if clientInfo.reply != nil {
				// send reply
				r.sendReply(request.ClientId, clientInfo.reply)
			}
			return
		}

		clientInfo.requestId = request.RequestId
		clientInfo.reply = nil
	}

	// advance log and add to client table
	r.appendLog(request)

	// add to quorum
	toCommitId := r.quorumAddPrepareOk(r.lastLogId, r.config.NodeId)
	assert.Assert(toCommitId <= r.commitId, "Primary quorumAddPrepareOk should not commit anything")

	// broadcast prepare request
	r.broadcast(CmdTypePrepare, &Prepare{
		ViewId:        r.viewId,
		LogId:         r.lastLogId,
		CommitId:      r.commitId,
		ClientRequest: request,
	})
	if r.maxSentCommitId < r.commitId {
		r.maxSentCommitId = r.commitId // commitId is piggybacked on prepare msg
	}
}

func (r *Replica) handlePrepare(prepare *Prepare) {
	assert.Assert(prepare != nil, "prepare should not be nil")

	if r.lastLogId >= prepare.LogId {
		// already existed
		return
	}

	// backup to apply commit from h.lastLogId + 1 until prepare.CommitId
	r.doCommit(prepare.CommitId, false)

	if r.lastLogId+1 < prepare.LogId {
		r.waitingPrepares[prepare.LogId] = prepare
		return
	}

	// advance log and add to client table
	r.appendLog(prepare.ClientRequest)

	nextPrepare, ok := r.waitingPrepares[r.lastLogId+1]
	for ok {
		// advance log and add to client table
		r.appendLog(nextPrepare.ClientRequest)
		delete(r.waitingPrepares, nextPrepare.LogId)
		nextPrepare, ok = r.waitingPrepares[r.lastLogId+1]
	}

	// send PrepareOk to primary node
	primaryNodeId := r.primaryNode()
	r.sendTo(primaryNodeId, CmdTypePrepareOk, &PrepareOk{
		ViewId: r.viewId,
		LogId:  r.lastLogId,
		NodeId: r.config.NodeId,
	})
}

func (r *Replica) handlePrepareOk(prepareOk *PrepareOk) {
	assert.Assert(prepareOk != nil, "prepareOk should not be nil")

	if r.commitId >= prepareOk.LogId {
		return // committed, skip this log id
	}

	toCommitId := r.quorumAddPrepareOk(prepareOk.LogId, prepareOk.NodeId)
	r.doCommit(toCommitId, true)
	r.tt.AfterFunc(r.config.CommitDelayDuration, func() {
		r.events <- event{
			etype:    eventTypeCommitDelay,
			commitId: r.commitId,
		}
	})
}

func (r *Replica) handleCommit(commit *Commit) {
	assert.Assert(commit != nil, "commit should not be nil")

	r.doCommit(commit.CommitId, false)
}

func (r *Replica) primaryNode() int {
	return int(r.viewId) % len(r.config.ServerAddrs)
}

func (r *Replica) appendLog(request *Request) {
	r.lastLogId++
	r.logs = append(r.logs, logEntry{
		LogId:     r.lastLogId,
		ClientId:  request.ClientId,
		RequestId: request.RequestId,
		Body:      request.RequestBody,
	})
	r.clients[request.ClientId] = &clientTable{
		requestId: request.RequestId,
		reply:     nil,
	}
}

func (r *Replica) quorumAddPrepareOk(logId uint64, nodeId int) uint64 {
	if r.lastPreparedLogId[nodeId] < logId {
		r.lastPreparedLogId[nodeId] = logId
	}
	logIds := make([]uint64, 0, len(r.lastPreparedLogId))
	logIds = append(logIds, r.lastPreparedLogId...)
	sort.Slice(logIds, func(i, j int) bool { return logIds[i] > logIds[j] }) // desc

	threshold := (len(r.config.ServerAddrs))/2 + 1
	return logIds[threshold-1]
}

func (r *Replica) doCommit(toCommitId uint64, sendReply bool) {
	for logId := r.commitId + 1; logId <= toCommitId; logId++ {
		index := logId - 1
		// upcall & commit
		assert.Assertf(r.logs[index].LogId == logId, "Logs array should be ordered correctly, expected %d, found %d", logId, r.logs[index].LogId)
		responseBody := r.upcall(r.logs[index].Body)
		r.commitId = logId
		// update client table
		reply := &Reply{
			ViewId:       r.viewId,
			RequestId:    r.logs[index].RequestId,
			ResponseBody: responseBody,
		}
		r.clients[r.logs[index].ClientId] = &clientTable{
			requestId: r.logs[index].RequestId,
			reply:     reply,
		}
		if sendReply {
			r.sendReply(r.logs[index].ClientId, reply)
		}
	}
}
